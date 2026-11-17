import os
import subprocess
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta, date
import requests
from flask import Flask, jsonify, render_template_string

app = Flask(__name__)

# ==================== 全局核心开关 ====================
WEEKLY_MODE = False       # False = 📅 当日(0DTE)模式 | True = 📆 全周(WEEKLY 5到期日聚合)模式
LIVE_FLOW_MODE = True     # True = StrikeGEX动态盘口流向(增量账本稳态版) | False = 传统静态(Call正/Put负)
NOISE_REDUCTION = True    # True = 过滤远端噪音，聚焦ATM核心博弈区

TICKERS = {
    "SPX": {"cboe": "_SPX", "yf": "^GSPC", "step": 5.0},
    "XSP": {"cboe": "_XSP", "yf": "^XSP",  "step": 1.0},
    "SPY": {"cboe": "SPY",  "yf": "SPY",   "step": 1.0},
    "QQQ": {"cboe": "QQQ",  "yf": "QQQ",   "step": 1.0},
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Cache-Control": "no-cache",
}

# ==================== 内存持久化状态池 (防闪烁/防回退/秒切周日模式) ====================
RAW_CHAIN_CACHE = {}      # 缓存经过防倒退校验的CBOE期权链 (含未来5个到期日全周数据)
CHAIN_VOL_STAMP = {}      # 记录每个标的最大累计成交量，拦截落后的CDN旧节点
FLOW_LEDGER = {}          # 增量订单流账本: {opt_sym: {"vol": float, "signed_vol": float}}
EMA_GEX_CACHE = {}        # 行权价级别EMA平滑缓存
SPOT_CACHE = {}           # 持久化最新实时现价(永不回退15分钟延迟价)
RENDER_CACHE = {}         # 前端秒读缓存
BASELINE_MEMORY = {}      # 破位与小火箭预警记忆
LAST_UPDATE_STR = "同步中..."
CACHE_LOCK = threading.Lock()


# ==================== 0. 启动自检：自动清理占用5000端口的旧幽灵进程 ====================
def kill_stale_port_5000():
    if os.name != "nt":
        return
    my_pid = os.getpid()
    try:
        out = subprocess.check_output("netstat -ano | findstr :5000", shell=True, text=True)
        killed = set()
        for line in out.strip().splitlines():
            parts = line.split()
            if len(parts) >= 5 and "LISTENING" in parts:
                pid = int(parts[-1])
                if pid != my_pid and pid > 0 and pid not in killed:
                    subprocess.run(f"taskkill /F /PID {pid}", shell=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    killed.add(pid)
        if killed:
            print(f"🧹 已自动清理后台残留的旧端口进程 PID: {killed}，防止双进程画面闪烁！")
            time.sleep(0.5)
    except Exception:
        pass


def get_us_eastern_now():
    return datetime.now(timezone.utc) - timedelta(hours=5)


def yymmdd_to_days_diff(yymmdd_str, today_date):
    try:
        y = 2000 + int(yymmdd_str[0:2])
        m = int(yymmdd_str[2:4])
        d = int(yymmdd_str[4:6])
        return max(0, (date(y, m, d) - today_date).days)
    except Exception:
        return 1


# ==================== 1. 实时现价抓取 (带持久化防回退锁) ====================
def fetch_single_spot(key, yf_sym):
    try:
        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{yf_sym}?interval=1m&range=1d"
        r = requests.get(url, headers=HEADERS, timeout=2.5)
        if r.status_code == 200:
            meta = r.json()["chart"]["result"][0]["meta"]
            price = float(meta.get("regularMarketPrice") or meta.get("previousClose") or 0)
            prev = float(meta.get("chartPreviousClose") or meta.get("previousClose") or price)
            if price > 0:
                pct = ((price - prev) / prev * 100.0) if prev > 0 else 0.0
                return key, price, pct
    except Exception:
        pass
    return key, None, None


def update_all_spots():
    with ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(fetch_single_spot, k, cfg["yf"]) for k, cfg in TICKERS.items()]
        for f in futures:
            k, price, pct = f.result()
            if price and price > 0:
                SPOT_CACHE[k] = {"spot": price, "pct": pct}

    if "SPX" in SPOT_CACHE and "XSP" not in SPOT_CACHE:
        SPOT_CACHE["XSP"] = {"spot": round(SPOT_CACHE["SPX"]["spot"] / 10.0, 2), "pct": SPOT_CACHE["SPX"]["pct"]}
    elif "XSP" in SPOT_CACHE and "SPX" not in SPOT_CACHE:
        SPOT_CACHE["SPX"] = {"spot": round(SPOT_CACHE["XSP"]["spot"] * 10.0, 2), "pct": SPOT_CACHE["XSP"]["pct"]}


# ==================== 2. Black-Scholes Gamma 补偿 ====================
def bs_live_gamma(spot, strike, iv, t_years):
    if spot <= 0 or strike <= 0 or iv <= 0 or t_years <= 0:
        return 0.0
    try:
        sqrt_t = math.sqrt(t_years)
        d1 = (math.log(spot / strike) + 0.5 * (iv * iv) * t_years) / (iv * sqrt_t)
        pdf_d1 = math.exp(-0.5 * (d1 * d1)) * 0.3989422804
        return pdf_d1 / (spot * iv * sqrt_t)
    except Exception:
        return 0.0


# ==================== 3. CBOE 期权链拉取 (预缓存未来5个到期日以支持周/日秒切) ====================
def fetch_cboe_raw_chain(ticker_key):
    cfg = TICKERS[ticker_key]
    cboe_sym = cfg["cboe"]
    ts_bucket = int(time.time() // 5)
    urls = [
        f"https://cdn.cboe.com/api/global/delayed_quotes/options/{cboe_sym}.json?_={ts_bucket}",
        f"https://cdn.cboe.com/api/global/delayed_quotes/options/{cboe_sym.replace('_', '')}.json?_={ts_bucket}",
    ]

    raw_json = None
    for url in urls:
        try:
            r = requests.get(url, headers=HEADERS, timeout=(2.5, 4.5))
            if r.status_code == 200:
                raw_json = r.json()
                break
        except Exception:
            continue

    if not raw_json or "data" not in raw_json:
        return False

    data_block = raw_json["data"]
    cboe_spot = float(data_block.get("current_price") or 0)
    if ticker_key not in SPOT_CACHE and cboe_spot > 0:
        SPOT_CACHE[ticker_key] = {"spot": cboe_spot, "pct": 0.0}

    spot = SPOT_CACHE.get(ticker_key, {}).get("spot") or cboe_spot
    if spot <= 0:
        return False

    options = data_block.get("options", [])
    if not options:
        return False

    est_now = get_us_eastern_now()
    today_yymmdd = est_now.strftime("%y%m%d")
    max_dist = spot * 0.065

    filtered_opts = []
    avail_dates = set()
    total_chain_vol = 0.0

    for opt in options:
        sym = opt.get("option", "")
        if len(sym) < 16:
            continue
        opt_type = sym[-9]
        if opt_type not in ("C", "P"):
            continue
        yymmdd = sym[-15:-9]
        if yymmdd < today_yymmdd:
            continue
        try:
            strike = int(sym[-8:]) * 0.001
        except ValueError:
            continue

        if abs(strike - spot) > max_dist:
            continue

        vol = float(opt.get("volume", 0) or 0)
        total_chain_vol += vol
        avail_dates.add(yymmdd)
        filtered_opts.append((sym, yymmdd, opt_type, strike, opt))

    if not avail_dates:
        return False

    # 🔒 CDN 边缘节点防倒退锁
    prev_max_vol = CHAIN_VOL_STAMP.get(ticker_key, 0.0)
    if total_chain_vol < prev_max_vol * 0.998 and ticker_key in RAW_CHAIN_CACHE:
        return True
    CHAIN_VOL_STAMP[ticker_key] = max(prev_max_vol, total_chain_vol)

    sorted_dates = sorted(list(avail_dates))
    target_d = sorted_dates[0]
    next_d = sorted_dates[1] if len(sorted_dates) > 1 else target_d
    # 截取未来最多 5 个到期日 (涵盖当周全部交易日及周五OPEX)
    week_dates = sorted_dates[:5]
    week_dates_set = set(week_dates)

    week_opts = [item for item in filtered_opts if item[1] in week_dates_set]

    # 更新全周期权增量订单流账本
    update_flow_ledger(week_opts, spot)

    first_lbl = f"{target_d[2:4]}-{target_d[4:6]}"
    last_d = week_dates[-1]
    last_lbl = f"{last_d[2:4]}-{last_d[4:6]}"

    RAW_CHAIN_CACHE[ticker_key] = {
        "cboe_spot": cboe_spot,
        "target_d": target_d,
        "next_d": next_d,
        "week_dates": week_dates,
        "day_label": f"{first_lbl} 0DTE",
        "week_label": f"{first_lbl}~{last_lbl} 周" if last_d != target_d else f"{first_lbl} 周",
        "week_opts": week_opts,
    }
    return True


# ==================== 4. 增量订单流账本 ====================
def calc_contract_flow_sign(opt_type, strike, spot, opt):
    static_sign = 1.0 if opt_type == "C" else -1.0
    bid = float(opt.get("bid", 0) or 0)
    ask = float(opt.get("ask", 0) or 0)
    last_p = float(opt.get("last_trade_price", 0) or 0)
    prev_close = float(opt.get("prev_day_close", 0) or 0)

    quote_sign = static_sign
    if ask > bid and bid > 0 and last_p > 0:
        spread = max(ask - bid, 0.01)
        ask_pos = max(0.0, min(1.0, (last_p - bid) / spread))
        quote_sign = 1.0 - 2.0 * ask_pos

    mom_sign = static_sign
    if last_p > 0 and prev_close > 0:
        chg = (last_p - prev_close) / max(prev_close, 0.05)
        if chg > 0.12:
            mom_sign = -0.85
        elif chg < -0.12:
            mom_sign = 0.85

    return 0.60 * mom_sign + 0.40 * quote_sign


def update_flow_ledger(week_opts, spot):
    for sym, yymmdd, opt_type, strike, opt in week_opts:
        vol = float(opt.get("volume", 0) or 0)
        cur_sign = calc_contract_flow_sign(opt_type, strike, spot, opt)

        rec = FLOW_LEDGER.get(sym)
        if rec is None:
            FLOW_LEDGER[sym] = {
                "vol": vol,
                "signed_vol": vol * cur_sign,
            }
        else:
            delta_vol = vol - rec["vol"]
            if delta_vol > 0:
                rec["signed_vol"] += delta_vol * cur_sign
                rec["vol"] = vol
            else:
                rec["signed_vol"] = 0.97 * rec["signed_vol"] + 0.03 * (vol * cur_sign)


# ==================== 5. 内存级 GEX 计算引擎 (支持当日/全周双模秒切) ====================
def compute_ticker_gex(ticker_key, use_ema=True):
    raw = RAW_CHAIN_CACHE.get(ticker_key)
    if not raw:
        return None

    cfg = TICKERS[ticker_key]
    step = cfg["step"]
    live_info = SPOT_CACHE.get(ticker_key, {})
    spot = live_info.get("spot") or raw["cboe_spot"]
    pct_change = live_info.get("pct", 0.0)
    if spot <= 0:
        return None

    target_d = raw["target_d"]
    next_d = raw["next_d"]
    est_now = get_us_eastern_now()
    today_date = est_now.date()
    hours_left = max(1.0, (16 - est_now.hour) + (15 - est_now.minute) / 60.0) if est_now.hour < 16 else 4.0
    t_0dte = max(hours_left / (252.0 * 6.5), 1.0 / (252.0 * 24.0))

    strike_buckets = {}
    vol_weight = 1.35 if NOISE_REDUCTION else 0.75

    for sym, yymmdd, opt_type, strike, opt in raw["week_opts"]:
        # ==================== 当日 (0DTE) vs 全周 (WEEKLY) 过滤与权重 ====================
        if not WEEKLY_MODE:
            # 📅 当日模式：仅聚焦当日0DTE与次日近端缓冲
            if yymmdd not in (target_d, next_d):
                continue
            date_factor = 1.0 if yymmdd == target_d else 0.25
        else:
            # 📆 全周模式：聚合当周全部 5 个到期日，按到期远近温和赋权
            days_diff = yymmdd_to_days_diff(yymmdd, today_date)
            date_factor = 1.0 if days_diff == 0 else max(0.55, 0.92 ** days_diff)

        grid_strike = round(round(strike / step) * step, 2)

        oi = float(opt.get("open_interest", 0) or 0)
        vol = float(opt.get("volume", 0) or 0)
        iv = float(opt.get("iv", 0) or 0.18)
        if iv > 3.0:
            iv = iv * 0.01
        raw_gamma = float(opt.get("gamma", 0) or 0)

        if yymmdd == target_d:
            t_exp = t_0dte
        else:
            days_diff = max(1, yymmdd_to_days_diff(yymmdd, today_date))
            t_exp = t_0dte + (days_diff / 252.0)

        gamma = bs_live_gamma(spot, strike, max(iv, 0.05), t_exp) if raw_gamma <= 1e-6 else raw_gamma

        # 标准单点美元 Gamma ($M)
        unit_gex = gamma * spot * 0.000001 * date_factor
        static_sign = 1.0 if opt_type == "C" else -1.0

        if not LIVE_FLOW_MODE:
            eff_qty = oi + vol * vol_weight
            gex_m = unit_gex * eff_qty * static_sign
            flow_score = static_sign * vol
        else:
            # 全周模式下隔夜 OI 底仓权重更高(0.65)，当日模式下日内订单流权重更高(0.40)
            base_oi_keep = 0.60 if WEEKLY_MODE else 0.40
            oi_keep = base_oi_keep if NOISE_REDUCTION else (base_oi_keep + 0.20)
            oi_gex = unit_gex * (oi * oi_keep) * static_sign

            ledger_rec = FLOW_LEDGER.get(sym)
            signed_vol = ledger_rec["signed_vol"] if ledger_rec else (vol * static_sign)

            dist_pct = abs(strike - spot) / spot
            flow_boost = 1.85 if dist_pct <= 0.018 else 1.15
            vol_gex = unit_gex * (signed_vol * vol_weight * flow_boost)

            gex_m = oi_gex + vol_gex
            flow_score = signed_vol

        b = strike_buckets.get(grid_strike)
        if b is None:
            b = {
                "strike": grid_strike, "net_gex": 0.0,
                "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0,
                "flow_weighted": 0.0, "vol_total": 0.0,
            }
            strike_buckets[grid_strike] = b

        b["net_gex"] += gex_m
        b["flow_weighted"] += flow_score
        b["vol_total"] += vol
        if opt_type == "C":
            b["call_oi"] += int(oi)
            b["call_vol"] += int(vol)
        else:
            b["put_oi"] += int(oi)
            b["put_vol"] += int(vol)

    center_strike = round(round(spot / step) * step, 2)
    ladder_strikes = [round(center_strike + i * step, 2) for i in range(12, -13, -1)]

    rows = []
    for s in ladder_strikes:
        b = strike_buckets.get(s)
        raw_val = b["net_gex"] if b else 0.0

        # 独立 EMA 键（包含 WEEKLY_MODE，确保日/周切换互不干扰）
        ema_key = (ticker_key, s, WEEKLY_MODE, LIVE_FLOW_MODE, NOISE_REDUCTION)
        if use_ema and ema_key in EMA_GEX_CACHE:
            smoothed_val = 0.75 * EMA_GEX_CACHE[ema_key] + 0.25 * raw_val
        else:
            smoothed_val = raw_val
        EMA_GEX_CACHE[ema_key] = smoothed_val

        if b:
            avg_flow = (b["flow_weighted"] / b["vol_total"]) if b["vol_total"] > 0 else 0.0
            rows.append({
                "strike": s, "net_gex": smoothed_val,
                "call_oi": b["call_oi"], "put_oi": b["put_oi"],
                "call_vol": b["call_vol"], "put_vol": b["put_vol"],
                "avg_flow": round(avg_flow, 2),
            })
        else:
            rows.append({
                "strike": s, "net_gex": 0.0,
                "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0, "avg_flow": 0.0,
            })

    pos_rows = [r for r in rows if r["net_gex"] > 0]
    neg_rows = [r for r in rows if r["net_gex"] < 0]

    call_wall = max(pos_rows, key=lambda x: x["net_gex"])["strike"] if pos_rows else center_strike + step * 3
    put_wall = min(neg_rows, key=lambda x: x["net_gex"])["strike"] if neg_rows else center_strike - step * 3
    king_node = max(rows, key=lambda x: abs(x["net_gex"]))["strike"] if rows else center_strike

    max_pos_val = max((r["net_gex"] for r in pos_rows), default=0.0)
    min_neg_val = min((r["net_gex"] for r in neg_rows), default=0.0)

    zero_flip = center_strike
    min_diff = 1e9
    for i in range(len(rows) - 1):
        g1, g2 = rows[i]["net_gex"], rows[i + 1]["net_gex"]
        if (g1 >= 0 and g2 < 0) or (g1 < 0 and g2 >= 0):
            cand = rows[i]["strike"] if abs(g1) < abs(g2) else rows[i + 1]["strike"]
            if abs(cand - spot) < min_diff:
                min_diff = abs(cand - spot)
                zero_flip = cand

    sorted_by_abs = sorted(rows, key=lambda x: abs(x["net_gex"]), reverse=True)
    star_strikes = {r["strike"] for r in sorted_by_abs[:3] if abs(r["net_gex"]) > 0}
    max_abs_gex = max((abs(r["net_gex"]) for r in rows), default=1.0) or 1.0

    for r in rows:
        s = r["strike"]
        r["is_spot"] = (abs(s - center_strike) < 1e-4)
        r["is_king"] = (abs(s - king_node) < 1e-4)
        r["is_call_wall"] = (abs(s - call_wall) < 1e-4)
        r["is_put_wall"] = (abs(s - put_wall) < 1e-4)
        r["is_flip"] = (abs(s - zero_flip) < 1e-4)
        r["is_star"] = (s in star_strikes)
        r["intensity"] = round(min(1.0, abs(r["net_gex"]) / max_abs_gex), 3)

    alerts = []
    base_key = (ticker_key, WEEKLY_MODE)
    prev_state = BASELINE_MEMORY.get(base_key)
    mode_tag = "全周" if WEEKLY_MODE else "日内"
    if prev_state:
        if call_wall > prev_state["call_wall"] and spot > prev_state["spot"]:
            alerts.append(f"🚀 {ticker_key} ({mode_tag}) 小火箭逼空：正Gamma阻力墙上移至 {call_wall}！")
        if spot < prev_state["put_wall"] and prev_state["spot"] >= prev_state["put_wall"]:
            alerts.append(f"⚠️ {ticker_key} ({mode_tag}) 破位警报：跌穿 {prev_state['put_wall']} 负Gamma触发位！")

    BASELINE_MEMORY[base_key] = {"spot": spot, "call_wall": call_wall, "put_wall": put_wall}

    return {
        "ticker": ticker_key,
        "exp": raw["week_label"] if WEEKLY_MODE else raw["day_label"],
        "spot": spot,
        "pct_change": pct_change,
        "zero_flip": zero_flip,
        "call_wall": call_wall,
        "put_wall": put_wall,
        "king_node": king_node,
        "max_pos_val": max_pos_val,
        "min_neg_val": min_neg_val,
        "rows": rows,
        "alerts": alerts,
    }


def rebuild_render_cache(use_ema=True):
    global LAST_UPDATE_STR
    new_render = {}
    for k in ["SPX", "XSP", "SPY", "QQQ"]:
        res = compute_ticker_gex(k, use_ema=use_ema)
        if res:
            new_render[k] = res
        elif k in RENDER_CACHE:
            new_render[k] = RENDER_CACHE[k]
    with CACHE_LOCK:
        RENDER_CACHE.update(new_render)
        LAST_UPDATE_STR = datetime.now().strftime("%H:%M:%S")


def background_sync_worker():
    while True:
        try:
            update_all_spots()
            with ThreadPoolExecutor(max_workers=4) as ex:
                list(ex.map(fetch_cboe_raw_chain, ["SPX", "XSP", "SPY", "QQQ"]))
            rebuild_render_cache(use_ema=True)
        except Exception as e:
            print("Background sync warning:", e)
        time.sleep(5.0)


# ==================== 6. Flask 极速路由 ====================
@app.route("/api/gex")
def api_gex():
    with CACHE_LOCK:
        data_copy = dict(RENDER_CACHE)
        ts = LAST_UPDATE_STR

    all_alerts = []
    for k, v in data_copy.items():
        all_alerts.extend(v.get("alerts", []))

    return jsonify({
        "timestamp": ts,
        "ready": len(data_copy) > 0,
        "weekly_mode": WEEKLY_MODE,
        "live_flow_mode": LIVE_FLOW_MODE,
        "noise_reduction": NOISE_REDUCTION,
        "data": data_copy,
        "alerts": all_alerts,
    })


@app.route("/api/toggle_weekly", methods=["POST"])
def toggle_weekly():
    global WEEKLY_MODE
    WEEKLY_MODE = not WEEKLY_MODE
    rebuild_render_cache(use_ema=False)
    return jsonify({"status": "ok", "weekly_mode": WEEKLY_MODE})


@app.route("/api/toggle_flow", methods=["POST"])
def toggle_flow():
    global LIVE_FLOW_MODE
    LIVE_FLOW_MODE = not LIVE_FLOW_MODE
    rebuild_render_cache(use_ema=False)
    return jsonify({"status": "ok", "live_flow_mode": LIVE_FLOW_MODE})


@app.route("/api/toggle_noise", methods=["POST"])
def toggle_noise():
    global NOISE_REDUCTION
    NOISE_REDUCTION = not NOISE_REDUCTION
    rebuild_render_cache(use_ema=False)
    return jsonify({"status": "ok", "noise_reduction": NOISE_REDUCTION})


# ==================== 7. 前端可视化界面 ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GEX 实时量化指挥舱 (StrikeGEX Live Flow Pro)</title>
<style>
  * { box-sizing: border-box; margin: 0; padding: 0; }
  body {
    background: #0b0e17;
    color: #f8fafc;
    font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Consolas, monospace;
    padding: 8px;
    overflow-x: hidden;
  }
  .topbar {
    display: flex;
    flex-wrap: wrap;
    justify-content: space-between;
    align-items: center;
    background: #131826;
    border: 1px solid #262f45;
    border-radius: 8px;
    padding: 8px 14px;
    margin-bottom: 10px;
    gap: 8px;
  }
  .brand {
    display: flex;
    align-items: center;
    gap: 10px;
    font-weight: 800;
    font-size: 15px;
    color: #e2e8f0;
  }
  .badge-live {
    background: #064e3b;
    color: #34d399;
    border: 1px solid #059669;
    padding: 2px 8px;
    border-radius: 4px;
    font-size: 11px;
  }
  .controls {
    display: flex;
    flex-wrap: wrap;
    align-items: center;
    gap: 8px;
  }
  .btn {
    border: 1px solid #334155;
    background: #1e293b;
    color: #e2e8f0;
    padding: 5px 12px;
    border-radius: 5px;
    font-size: 12px;
    font-weight: 700;
    cursor: pointer;
    transition: all 0.12s;
  }
  .btn.active-day {
    background: #10b981;
    color: #052e16;
    border-color: #34d399;
    box-shadow: 0 0 10px rgba(16, 185, 129, 0.35);
  }
  .btn.active-week {
    background: #8b5cf6;
    color: #ffffff;
    border-color: #a78bfa;
    box-shadow: 0 0 10px rgba(139, 92, 246, 0.45);
  }
  .btn.active-flow {
    background: #f59e0b;
    color: #0f172a;
    border-color: #fbbf24;
    box-shadow: 0 0 10px rgba(245, 158, 11, 0.4);
  }
  .btn.active-sub {
    background: #0284c7;
    color: #fff;
    border-color: #38bdf8;
  }
  .grid-container {
    display: grid;
    grid-template-columns: repeat(4, 1fr);
    gap: 8px;
  }
  @media (max-width: 900px) {
    .grid-container { grid-template-columns: repeat(2, 1fr); }
  }
  .col-card {
    background: #111624;
    border: 1px solid #222b40;
    border-radius: 8px;
    overflow: hidden;
    display: flex;
    flex-direction: column;
  }
  .col-header {
    padding: 8px 10px;
    background: #161d2f;
    border-bottom: 1px solid #262f45;
  }
  .title-row {
    display: flex;
    justify-content: space-between;
    align-items: baseline;
    font-weight: 800;
    font-size: 14px;
    margin-bottom: 6px;
  }
  .pct-tag {
    font-size: 11px;
    font-weight: 600;
    margin-left: 4px;
  }
  .pills-row {
    display: flex;
    gap: 6px;
    flex-wrap: wrap;
  }
  .pill {
    font-size: 11px;
    font-weight: 700;
    padding: 2px 6px;
    border-radius: 4px;
    background: #1e293b;
  }
  .pill-flip { color: #60a5fa; border: 1px solid #1d4ed8; }
  .pill-call { color: #facc15; border: 1px solid #a16207; }
  .pill-put  { color: #c084fc; border: 1px solid #6b21a8; }

  .ladder-body { display: flex; flex-direction: column; }
  .strike-row {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 4px 10px;
    font-size: 13px;
    font-weight: 700;
    border-bottom: 1px solid rgba(255,255,255,0.04);
    cursor: crosshair;
  }
  .strike-row.spot-row {
    outline: 2px solid #38bdf8;
    z-index: 2;
  }
  .strike-left {
    display: flex;
    align-items: center;
    gap: 4px;
  }
  .m-tag {
    font-size: 10px;
    background: rgba(15, 23, 42, 0.8);
    color: #38bdf8;
    padding: 0px 4px;
    border-radius: 3px;
    border: 1px solid #38bdf8;
  }
  .col-footer {
    display: flex;
    justify-content: space-between;
    align-items: center;
    padding: 6px 10px;
    font-size: 12px;
    font-weight: 800;
    background: linear-gradient(90deg, #581c87 0%, #1e293b 50%, #ca8a04 100%);
    border-top: 1px solid #334155;
  }
  .loading-box {
    grid-column: 1 / -1;
    text-align: center;
    padding: 60px 20px;
    color: #94a3b8;
    font-size: 15px;
    font-weight: 700;
  }
  #tooltip {
    position: fixed;
    display: none;
    background: rgba(15, 23, 42, 0.96);
    border: 1px solid #475569;
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 12px;
    color: #f1f5f9;
    z-index: 9999;
    pointer-events: none;
    box-shadow: 0 8px 20px rgba(0,0,0,0.6);
    min-width: 220px;
  }
  .alert-banner {
    display: none;
    justify-content: space-between;
    align-items: center;
    background: #7c2d12;
    border: 1px solid #f97316;
    color: #ffedd5;
    padding: 7px 12px;
    border-radius: 6px;
    margin-bottom: 8px;
    font-size: 13px;
    font-weight: 700;
  }
</style>
</head>
<body>

<div class="topbar">
  <div class="brand">
    <span>⚡ XSP / SPX GEX 指挥中心</span>
    <span class="badge-live" id="clockTag">LIVE</span>
  </div>
  <div class="controls">
    <button id="weekBtn" class="btn active-day" onclick="toggleMode('/api/toggle_weekly')">📅 周期: 当日 (0DTE)</button>
    <button id="flowBtn" class="btn active-flow" onclick="toggleMode('/api/toggle_flow')">⚡ LIVE FLOW: ON (盘口动态)</button>
    <button id="noiseBtn" class="btn active-sub" onclick="toggleMode('/api/toggle_noise')">🧹 降噪: ON</button>
    <button id="soundBtn" class="btn" onclick="toggleSound()">🔊 语音: OFF</button>
  </div>
</div>

<div id="alertBox" class="alert-banner">
  <span id="alertText"></span>
  <span style="cursor:pointer; padding:0 6px;" onclick="document.getElementById('alertBox').style.display='none'">✖</span>
</div>
<div id="grid" class="grid-container">
  <div class="loading-box">⚡ 正在同步 CBOE 四标的当日/全周期权链稳态快照 (约 1~2 秒)...</div>
</div>
<div id="tooltip"></div>

<script>
let soundEnabled = false;
let isFetching = false;
let lastSpokenAlert = "";

function formatGex(val) {
  const absV = Math.abs(val);
  const sign = val > 0 ? "+" : (val < 0 ? "-" : "");
  if (absV >= 1.0) return `${sign}$${absV.toFixed(1)}M`;
  if (absV >= 0.001) return `${sign}$${(absV * 1000).toFixed(0)}K`;
  return "$0";
}

function getRowStyle(gex, intensity) {
  if (gex >= 0) {
    if (intensity > 0.55) {
      return `background: linear-gradient(90deg, rgba(234,179,8,0.88), rgba(250,204,21,0.95)); color: #0f172a;`;
    } else if (intensity > 0.22) {
      return `background: rgba(21, 128, 61, ${0.45 + intensity * 0.45}); color: #f8fafc;`;
    }
    return `background: rgba(20, 83, 45, 0.35); color: #cbd5e1;`;
  } else {
    if (intensity > 0.55) {
      return `background: linear-gradient(90deg, rgba(107,33,168,0.92), rgba(126,34,206,0.96)); color: #ffffff;`;
    } else if (intensity > 0.20) {
      return `background: rgba(88, 28, 135, ${0.45 + intensity * 0.45}); color: #f1f5f9;`;
    }
    return `background: rgba(59, 7, 100, 0.35); color: #94a3b8;`;
  }
}

async function toggleMode(endpoint) {
  await fetch(endpoint, { method: "POST" });
  fetchGexData(true);
}

function toggleSound() {
  soundEnabled = !soundEnabled;
  const btn = document.getElementById("soundBtn");
  btn.innerText = soundEnabled ? "🔊 语音: ON" : "🔊 语音: OFF";
  btn.className = soundEnabled ? "btn active-sub" : "btn";
}

function speakAlert(text) {
  if (!soundEnabled || !("speechSynthesis" in window) || text === lastSpokenAlert) return;
  lastSpokenAlert = text;
  const utter = new SpeechSynthesisUtterance(text);
  utter.lang = "zh-CN";
  utter.rate = 1.15;
  window.speechSynthesis.speak(utter);
}

function showTooltip(e, ticker, row) {
  const tt = document.getElementById("tooltip");
  let flowDesc = "⚪ 盘口均衡";
  if (row.avg_flow > 0.12) flowDesc = "🟡 抄底/平仓护盘 (Bid主动卖盘)";
  else if (row.avg_flow < -0.12) flowDesc = "🟣 空头扫货加速 (Ask主动买盘)";

  tt.innerHTML = `
    <div style="font-weight:800; color:#38bdf8; margin-bottom:4px;">${ticker} Strike ${row.strike}</div>
    <div>Net GEX: <b>${formatGex(row.net_gex)}</b></div>
    <div>盘口流向: <b>${flowDesc}</b></div>
    <hr style="border:0; border-top:1px solid #334155; margin:4px 0;">
    <div>Call OI: ${row.call_oi.toLocaleString()} | Vol: ${row.call_vol.toLocaleString()}</div>
    <div>Put OI: ${row.put_oi.toLocaleString()} | Vol: ${row.put_vol.toLocaleString()}</div>
  `;
  tt.style.display = "block";
  tt.style.left = Math.min(e.clientX + 14, window.innerWidth - 230) + "px";
  tt.style.top = Math.min(e.clientY + 14, window.innerHeight - 130) + "px";
}

function hideTooltip() {
  document.getElementById("tooltip").style.display = "none";
}

async function fetchGexData(force = false) {
  if (isFetching && !force) return;
  isFetching = true;
  try {
    const res = await fetch("/api/gex");
    const json = await res.json();

    document.getElementById("clockTag").innerText = `LIVE ${json.timestamp}`;

    const weekBtn = document.getElementById("weekBtn");
    weekBtn.innerText = json.weekly_mode ? "📆 周期: 全周 (WEEKLY)" : "📅 周期: 当日 (0DTE)";
    weekBtn.className = json.weekly_mode ? "btn active-week" : "btn active-day";

    const flowBtn = document.getElementById("flowBtn");
    flowBtn.innerText = json.live_flow_mode ? "⚡ LIVE FLOW: ON (盘口动态)" : "📊 STATIC OI (传统静态)";
    flowBtn.className = json.live_flow_mode ? "btn active-flow" : "btn";

    const noiseBtn = document.getElementById("noiseBtn");
    noiseBtn.innerText = json.noise_reduction ? "🧹 降噪: ON" : "🧹 降噪: OFF";
    noiseBtn.className = json.noise_reduction ? "btn active-sub" : "btn";

    if (!json.ready) return;

    const alertBox = document.getElementById("alertBox");
    const alertText = document.getElementById("alertText");
    if (json.alerts && json.alerts.length > 0) {
      alertBox.style.display = "flex";
      alertText.innerText = json.alerts.join("  |  ");
      speakAlert(json.alerts[0]);
    }

    const order = ["SPX", "XSP", "SPY", "QQQ"];
    let html = "";

    for (const sym of order) {
      const d = json.data[sym];
      if (!d) continue;

      const pctColor = d.pct_change >= 0 ? "#4ade80" : "#f87171";
      const pctSign = d.pct_change >= 0 ? "+" : "";

      let rowsHtml = "";
      for (const r of d.rows) {
        const style = getRowStyle(r.net_gex, r.intensity);
        const spotClass = r.is_spot ? "strike-row spot-row" : "strike-row";
        const icons = [
          r.is_spot ? `<span class="m-tag">M</span>` : "",
          r.is_king ? "👑" : "",
          r.is_call_wall ? "▲" : "",
          r.is_put_wall ? "▼" : "",
          r.is_flip ? "◆" : ""
        ].join(" ");
        const star = r.is_star ? "*" : "";
        const rowJson = JSON.stringify(r).replace(/"/g, '&quot;');

        rowsHtml += `
          <div class="${spotClass}" style="${style}"
               onmousemove="showTooltip(event, '${sym}', ${rowJson})"
               onmouseleave="hideTooltip()">
            <div class="strike-left">
              <span>${r.strike}</span>
              <span>${icons}</span>
            </div>
            <div>${formatGex(r.net_gex)}${star}</div>
          </div>
        `;
      }

      html += `
        <div class="col-card">
          <div class="col-header">
            <div class="title-row">
              <span>${sym} (${d.exp})</span>
              <span>
                $${d.spot.toFixed(2)}
                <span class="pct-tag" style="color:${pctColor}">${pctSign}${d.pct_change.toFixed(2)}%</span>
              </span>
            </div>
            <div class="pills-row">
              <span class="pill pill-flip">◆${d.zero_flip}</span>
              <span class="pill pill-call">▲${d.call_wall}</span>
              <span class="pill pill-put">▼${d.put_wall}</span>
            </div>
          </div>
          <div class="ladder-body">${rowsHtml}</div>
          <div class="col-footer">
            <span>${formatGex(d.min_neg_val)}</span>
            <span style="font-size:10px; color:#cbd5e1;">MAX -GEX / +GEX</span>
            <span>${formatGex(d.max_pos_val)}</span>
          </div>
        </div>
      `;
    }

    if (html) {
      document.getElementById("grid").innerHTML = html;
    }
  } catch (err) {
    console.error("GEX refresh error:", err);
  } finally {
    isFetching = false;
  }
}

fetchGexData();
setInterval(fetchGexData, 2500);
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)


if __name__ == "__main__":
    kill_stale_port_5000()
    t = threading.Thread(target=background_sync_worker, daemon=True)
    t.start()
    print("=========================================================")
    print("🚀 XSP/SPX GEX 指挥中心 (含日/周双模秒切 + 稳态Live Flow) 已启动")
    print("👉 请在浏览器打开: http://127.0.0.1:5000")
    print("=========================================================")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
