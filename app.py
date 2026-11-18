import os
import subprocess
import math
import time
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta, date
import requests
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

# ==================== 全局核心开关 ====================
SELECTED_EXP = "AUTO"     # "AUTO" = 美东16:15收盘后自动跳下一交易日 | 或指定如 "261118"
WEEKLY_MODE = False       # False = 📅 单日视图 | True = 📆 本周OPEX视图 (后台始终双周期并行计算)
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

# ==================== 内存持久化状态池 ====================
RAW_CHAIN_CACHE = {}      # 缓存CBOE原始期权链
CHAIN_VOL_STAMP = {}      # CDN防倒退校验戳
FLOW_LEDGER = {}          # 增量订单流账本: {opt_sym: {"vol": float, "signed_vol": float}}
EMA_GEX_CACHE = {}        # 行权价级别EMA平滑缓存
SPOT_CACHE = {}           # 实时现价缓存
RENDER_CACHE = {}         # 前端秒读缓存
INTRADAY_ANCHOR = {}      # 盘中主力移仓对比基准
ALERT_COOLDOWN = {}       # 智能语音冷却锁
AVAIL_DATES_LIST = []
ACTIVE_AUTO_DATE = ""
LAST_UPDATE_STR = "同步中..."
CACHE_LOCK = threading.Lock()


# ==================== 0. 启动自检：清理旧端口进程 ====================
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
            print(f"🧹 已自动清理后台残留的旧端口进程 PID: {killed}")
            time.sleep(0.5)
    except Exception:
        pass


def get_us_eastern_now():
    return datetime.now(timezone.utc) - timedelta(hours=5)


def is_after_market_close(est_now=None):
    if est_now is None:
        est_now = get_us_eastern_now()
    return (est_now.hour > 16) or (est_now.hour == 16 and est_now.minute >= 15)


def yymmdd_to_date(yymmdd_str):
    try:
        return date(2000 + int(yymmdd_str[0:2]), int(yymmdd_str[2:4]), int(yymmdd_str[4:6]))
    except Exception:
        return date.today()


def yymmdd_to_days_diff(yymmdd_str, today_date):
    return max(0, (yymmdd_to_date(yymmdd_str) - today_date).days)


def format_yymmdd_label(yymmdd_str, today_yymmdd, after_close):
    d_obj = yymmdd_to_date(yymmdd_str)
    wk_names = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    wk_str = wk_names[d_obj.weekday()]
    mm_dd = f"{yymmdd_str[2:4]}-{yymmdd_str[4:6]}"
    if yymmdd_str == today_yymmdd:
        return f"{mm_dd} {wk_str} (今日收盘)" if after_close else f"{mm_dd} {wk_str} (今日0DTE)"
    if d_obj.weekday() == 4:
        return f"{mm_dd} {wk_str} (OPEX结算)"
    return f"{mm_dd} {wk_str}"


def fmt_gex_str(val):
    abs_v = abs(val)
    sign = "+" if val > 0 else ("-" if val < 0 else "")
    if abs_v >= 1.0:
        return f"{sign}${abs_v:.1f}M"
    if abs_v >= 0.001:
        return f"{sign}${abs_v * 1000:.0f}K"
    return "$0"


# ==================== 1. 实时现价抓取 ====================
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


# ==================== 3. CBOE 期权链拉取 ====================
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

    prev_max_vol = CHAIN_VOL_STAMP.get(ticker_key, 0.0)
    if total_chain_vol < prev_max_vol * 0.998 and ticker_key in RAW_CHAIN_CACHE:
        return True
    CHAIN_VOL_STAMP[ticker_key] = max(prev_max_vol, total_chain_vol)

    sorted_dates = sorted(list(avail_dates))[:8]
    kept_set = set(sorted_dates)
    week_opts = [item for item in filtered_opts if item[1] in kept_set]

    update_flow_ledger(week_opts, spot)

    RAW_CHAIN_CACHE[ticker_key] = {
        "cboe_spot": cboe_spot,
        "avail_dates": sorted_dates,
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


# ==================== 5. 业内标准 OPEX 周周期解析器 ====================
def resolve_active_dates(avail_dates, est_now):
    today_yymmdd = est_now.strftime("%y%m%d")
    after_close = is_after_market_close(est_now)

    if SELECTED_EXP != "AUTO" and SELECTED_EXP in avail_dates:
        target_d = SELECTED_EXP
    else:
        if after_close:
            future_dates = [d for d in avail_dates if d > today_yymmdd]
            target_d = future_dates[0] if future_dates else avail_dates[0]
        else:
            target_d = avail_dates[0]

    target_date_obj = yymmdd_to_date(target_d)
    wkday = target_date_obj.weekday()

    if wkday < 4:
        this_friday = target_date_obj + timedelta(days=(4 - wkday))
        friday_yymmdd = this_friday.strftime("%y%m%d")
        week_slice = [d for d in avail_dates if target_d <= d <= friday_yymmdd]
        if not week_slice:
            week_slice = [target_d]
        week_tag = "本周"
    else:
        next_friday = target_date_obj + timedelta(days=7)
        next_fri_yymmdd = next_friday.strftime("%y%m%d")
        week_slice = [d for d in avail_dates if target_d <= d <= next_fri_yymmdd]
        if not week_slice:
            week_slice = [target_d]
        week_tag = "跨周"

    first_lbl = f"{target_d[2:4]}-{target_d[4:6]}"
    last_d = week_slice[-1] if week_slice else target_d
    last_lbl = f"{last_d[2:4]}-{last_d[4:6]}"

    if WEEKLY_MODE:
        exp_label = f"{first_lbl}~{last_lbl} {week_tag}" if last_d != target_d else f"{first_lbl} {week_tag}"
    else:
        if target_d == today_yymmdd:
            exp_label = f"{first_lbl} 收盘" if after_close else f"{first_lbl} 0DTE"
        else:
            exp_label = f"{first_lbl}"

    return target_d, week_slice, exp_label


def extract_profile_nodes(ladder_strikes, gex_map, spot, step):
    pos_s = [s for s in ladder_strikes if gex_map.get(s, 0.0) > 0]
    neg_s = [s for s in ladder_strikes if gex_map.get(s, 0.0) < 0]
    center_s = round(round(spot / step) * step, 2)

    cw = max(pos_s, key=lambda s: gex_map[s]) if pos_s else round(center_s + step * 3, 2)
    pw = min(neg_s, key=lambda s: gex_map[s]) if neg_s else round(center_s - step * 3, 2)
    king = max(ladder_strikes, key=lambda s: abs(gex_map.get(s, 0.0))) if ladder_strikes else center_s

    max_pos = max((gex_map[s] for s in pos_s), default=0.0)
    min_neg = min((gex_map[s] for s in neg_s), default=0.0)
    max_abs = max((abs(gex_map.get(s, 0.0)) for s in ladder_strikes), default=1.0) or 1.0

    flip = center_s
    min_dist = 1e9
    for i in range(len(ladder_strikes) - 1):
        s1, s2 = ladder_strikes[i], ladder_strikes[i + 1]
        g1, g2 = gex_map.get(s1, 0.0), gex_map.get(s2, 0.0)
        if (g1 >= 0 and g2 < 0) or (g1 < 0 and g2 >= 0):
            cand = s1 if abs(g1) < abs(g2) else s2
            if abs(cand - spot) < min_dist:
                min_dist = abs(cand - spot)
                flip = cand

    top_pos = set(sorted(pos_s, key=lambda s: gex_map[s], reverse=True)[:3])
    top_neg = set(sorted(neg_s, key=lambda s: gex_map[s])[:3])
    top_abs = set(sorted(ladder_strikes, key=lambda s: abs(gex_map.get(s, 0.0)), reverse=True)[:3])

    return {
        "cw": cw, "pw": pw, "king": king, "flip": flip,
        "max_pos": max_pos, "min_neg": min_neg, "max_abs": max_abs,
        "top_pos": top_pos, "top_neg": top_neg, "top_abs": top_abs,
    }


# ==================== 6. Top-1 唯一主靶心收敛 + 盘中移仓追踪引擎 ====================
def analyze_dynamic_resonance_and_shifts(ticker_key, target_d, spot, step, ladder_strikes, day_map, week_map, day_meta, week_meta):
    """
    严格去伪存真算法：
    - 每列上方最多只评出 1 个【多头1号共振主靶心 🎯】
    - 每列下方最多只评出 1 个【空头1号共振主靶心 🎯】
    - 每列最多 1 个【最强护盘铁底 🛡️】与 1 个【最大日黄周紫背离墙 ⚠️】
    - 次级共振点位仅在鼠标悬停 Tooltip 中显示详情，绝不污染主界面图标
    """
    now_ts = time.time()
    dynamic_alerts = []
    strike_notes = {s: [] for s in ladder_strikes}
    strike_icons = {s: "" for s in ladder_strikes}

    def trigger_voice_once(key_id, msg, cooldown=180):
        last_t = ALERT_COOLDOWN.get(key_id, 0)
        if now_ts - last_t > cooldown:
            ALERT_COOLDOWN[key_id] = now_ts
            dynamic_alerts.append(msg)

    # --- A. 盘中主力移仓基准比对 ---
    anchor_key = (ticker_key, target_d)
    anchor = INTRADAY_ANCHOR.get(anchor_key)
    if anchor is None:
        INTRADAY_ANCHOR[anchor_key] = {
            "time": now_ts,
            "spot": spot,
            "cw": day_meta["cw"],
            "pw": day_meta["pw"],
            "gex_map": dict(day_map),
        }
        anchor = INTRADAY_ANCHOR[anchor_key]
    else:
        if day_meta["cw"] > anchor["cw"] and spot > anchor["spot"]:
            trigger_voice_once(
                f"{ticker_key}_CW_UP_{day_meta['cw']}",
                f"🚀 {ticker_key} 主力向上移仓逼空：日内正Gamma目标墙从 {anchor['cw']} 上移至 {day_meta['cw']}！"
            )
            anchor["cw"] = day_meta["cw"]
        if day_meta["pw"] < anchor["pw"] and spot < anchor["spot"]:
            trigger_voice_once(
                f"{ticker_key}_PW_DN_{day_meta['pw']}",
                f"⚠️ {ticker_key} 空头向下移仓砸盘：日内负Gamma触发墙从 {anchor['pw']} 下移至 {day_meta['pw']}！"
            )
            anchor["pw"] = day_meta["pw"]

    # --- B. 候选池打分：选出全列唯一的 Top-1 上方主靶心 & Top-1 下方主靶心 ---
    pos_res_candidates = []  # (score, strike)
    neg_res_candidates = []  # (score, strike)
    shield_candidates = []   # (score, strike) 下方卖Put护盘铁底
    trap_candidates = []     # (score, strike) 上方日黄周紫诱多墙
    shift_candidates = []    # (score, strike) 盘中大幅移仓

    for s in ladder_strikes:
        d_val = day_map.get(s, 0.0)
        w_val = week_map.get(s, 0.0)
        d_norm = abs(d_val) / day_meta["max_abs"]
        w_norm = abs(w_val) / week_meta["max_abs"]
        old_d_val = anchor["gex_map"].get(s, d_val)

        # 1. 同向正 Gamma 共振评分
        if d_val > 0 and w_val > 0 and d_norm > 0.20 and w_norm > 0.20:
            wall_boost = 1.35 if (s == day_meta["cw"] or s == week_meta["cw"]) else 1.0
            score = math.sqrt(d_norm * w_norm) * wall_boost
            if s >= spot - step * 0.5:
                pos_res_candidates.append((score, s))
                strike_notes[s].append(f"多头双周期共振 (单日{fmt_gex_str(d_val)} | 全周{fmt_gex_str(w_val)})")
            else:
                shield_candidates.append((score * 1.2, s))
                strike_notes[s].append(f"双周期正GEX联合护盘铁底 (单日{fmt_gex_str(d_val)} | 全周{fmt_gex_str(w_val)})")

        # 2. 同向负 Gamma 共振评分
        elif d_val < 0 and w_val < 0 and d_norm > 0.20 and w_norm > 0.20:
            wall_boost = 1.35 if (s == day_meta["pw"] or s == week_meta["pw"]) else 1.0
            score = math.sqrt(d_norm * w_norm) * wall_boost
            neg_res_candidates.append((score, s))
            strike_notes[s].append(f"空头双周期共振靶心 (单日{fmt_gex_str(d_val)} | 全周{fmt_gex_str(w_val)})")

        # 3. 日黄周紫背离
        elif d_val > 0 and w_val < 0:
            if s < spot and (s == day_meta["cw"] or d_norm > 0.60):
                # 现价下方单日巨量正Gamma铁底 (如 QQQ 650)
                shield_candidates.append((d_norm * 1.3, s))
                strike_notes[s].append(f"🛡️ 日内卖Put急跌铁底：单日巨量正GEX({fmt_gex_str(d_val)})对冲全周负压({fmt_gex_str(w_val)})，空单优先止盈")
            elif s >= spot and d_norm > 0.20 and w_norm > 0.25:
                trap_score = math.sqrt(d_norm * w_norm)
                trap_candidates.append((trap_score, s))
                strike_notes[s].append(f"⚠️ 日黄周紫诱多墙：单日正GEX({fmt_gex_str(d_val)})被全周负GEX({fmt_gex_str(w_val)})吞噬，防假突破受阻")
            elif s < spot and d_norm > 0.15 and w_norm > 0.25:
                strike_notes[s].append(f"⚠️ 脆弱多头薄垫：单日薄垫({fmt_gex_str(d_val)})但全周翻紫({fmt_gex_str(w_val)})，失守易加速")

        # 4. 日紫周黄背离 (全周机构下方黄金护垫，如 QQQ 648~649)
        elif d_val <= 0 and w_val > 0 and s < spot and w_norm > 0.30:
            shield_candidates.append((w_norm, s))
            strike_notes[s].append(f"🛡️ 全周机构黄金护垫：全周沉淀厚实正GEX({fmt_gex_str(w_val)})，插针至此易获买盘托底")

        # 5. 盘中移仓检测
        diff_norm = abs(d_val - old_d_val) / day_meta["max_abs"]
        if old_d_val > 0 and d_val < 0 and diff_norm > 0.30:
            shift_candidates.append((diff_norm * 1.5, s))
            strike_notes[s].append(f"🔄 盘中支撑蒸发：从正GEX({fmt_gex_str(old_d_val)})被空头砸穿翻紫({fmt_gex_str(d_val)})")
        elif diff_norm > 0.45:
            shift_candidates.append((diff_norm, s))
            dir_str = "主力增仓强化" if abs(d_val) > abs(old_d_val) else "主力平仓撤退"
            strike_notes[s].append(f"🔄 盘中移仓异动：{dir_str} ({fmt_gex_str(old_d_val)} ➔ {fmt_gex_str(d_val)})")

    # --- C. 严格唯一图标授勋 (Top-1 Lock) ---
    # 1) 唯一上方多头共振主靶心 🎯
    if pos_res_candidates:
        best_pos_score, best_pos_s = max(pos_res_candidates, key=lambda x: x[0])
        if best_pos_score >= 0.32:
            strike_icons[best_pos_s] = "🎯"
            strike_notes[best_pos_s].insert(0, "🎯【全列唯一多头1号共振主靶心】")

    # 2) 唯一空头负Gamma共振主靶心 🎯
    if neg_res_candidates:
        best_neg_score, best_neg_s = max(neg_res_candidates, key=lambda x: x[0])
        if best_neg_score >= 0.32:
            strike_icons[best_neg_s] = "🎯"
            strike_notes[best_neg_s].insert(0, "🎯【全列唯一空头1号共振主靶心】")

    # 3) 唯一最强下方护盘铁底 🛡️ (不覆盖已有的 🎯)
    if shield_candidates:
        best_sh_score, best_sh_s = max(shield_candidates, key=lambda x: x[0])
        if best_sh_score >= 0.35 and not strike_icons[best_sh_s]:
            strike_icons[best_sh_s] = "🛡️"

    # 4) 唯一最显著上方日黄周紫背离墙 ⚠️
    if trap_candidates:
        best_tr_score, best_tr_s = max(trap_candidates, key=lambda x: x[0])
        if best_tr_score >= 0.28 and not strike_icons[best_tr_s]:
            strike_icons[best_tr_s] = "⚠️"

    # 5) 唯一最剧烈盘中移仓点 🔄
    if shift_candidates:
        best_shf_score, best_shf_s = max(shift_candidates, key=lambda x: x[0])
        if not strike_icons[best_shf_s]:
            strike_icons[best_shf_s] = "🔄"

    for s in ladder_strikes:
        anchor["gex_map"][s] = 0.98 * anchor["gex_map"].get(s, day_map.get(s, 0.0)) + 0.02 * day_map.get(s, 0.0)

    # --- D. 生成顶部实时 Tactical HUD 作战指令条 ---
    center_s = round(round(spot / step) * step, 2)
    below_3 = [round(center_s - i * step, 2) for i in (1, 2, 3)]
    vacuum_below = all((day_map.get(s, 0.0) < 0 and week_map.get(s, 0.0) < 0) for s in below_3)

    cur_d_gex = day_map.get(center_s, 0.0)
    cur_w_gex = week_map.get(center_s, 0.0)
    top_neg_target = max(neg_res_candidates, key=lambda x: x[0])[1] if neg_res_candidates else day_meta["pw"]
    top_pos_target = max(pos_res_candidates, key=lambda x: x[0])[1] if pos_res_candidates else day_meta["cw"]

    if cur_d_gex > 0 and cur_w_gex < 0 and abs(cur_w_gex) / week_meta["max_abs"] > 0.22 and spot >= day_meta["flip"]:
        hud = {
            "text": f"🟠 触及 {center_s}⚠️ 日黄周紫诱多墙，受阻优先伏击Put看向 {top_neg_target}🎯",
            "color": "#f59e0b"
        }
        trigger_voice_once(f"{ticker_key}_DIV_TRAP_{center_s}", f"🎯 {ticker_key} 触及 {center_s} 日黄周紫背离压制区，提防假突破！")

    elif abs(spot - top_neg_target) <= step * 0.6:
        hud = {
            "text": f"🎯 抵达 {top_neg_target}▼ 唯一空头1号主靶心！注意分批止盈Put，严防反抽",
            "color": "#facc15"
        }
        trigger_voice_once(f"{ticker_key}_PW_HIT_{top_neg_target}", f"🎯 {ticker_key} 抵达 {top_neg_target} 唯一空头主靶心，注意止盈！")

    elif spot < day_meta["flip"] and cur_d_gex > 0 and (center_s == day_meta["cw"] or cur_d_gex / day_meta["max_abs"] > 0.65):
        hud = {
            "text": f"🛡️ 触及 {center_s}👑 日内巨量正GEX护盘铁底({fmt_gex_str(cur_d_gex)})！果断止盈空单",
            "color": "#facc15"
        }
        trigger_voice_once(f"{ticker_key}_IRON_BOT_{center_s}", f"🛡️ {ticker_key} 触及 {center_s} 日内正Gamma铁底，建议空单止盈！")

    elif spot < day_meta["flip"] and spot < week_meta["flip"] and vacuum_below:
        hud = {
            "text": f"🟣 翻转轴({day_meta['flip']}◆)下方负Gamma真空滑梯！唯一主靶心直指 {top_neg_target}🎯",
            "color": "#c084fc"
        }

    elif spot >= day_meta["flip"] and spot >= week_meta["flip"]:
        hud = {
            "text": f"🟢 站稳双翻转轴({day_meta['flip']}◆)之上，多头1号主靶心看向 {top_pos_target}🎯",
            "color": "#10b981"
        }

    else:
        hud = {
            "text": f"⚪ 震荡拉锯：上方翻转轴 {max(day_meta['flip'], week_meta['flip'])}◆ | 下方唯一主靶心 {top_neg_target}🎯",
            "color": "#94a3b8"
        }

    strike_tags = {
        s: {"icon": strike_icons[s], "note": "；".join(strike_notes[s])}
        for s in ladder_strikes
    }
    return strike_tags, hud, dynamic_alerts


# ==================== 7. 内存级双周期并行 GEX 计算引擎 ====================
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

    est_now = get_us_eastern_now()
    today_date = est_now.date()
    avail_dates = raw["avail_dates"]
    target_d, week_slice, exp_label = resolve_active_dates(avail_dates, est_now)
    week_slice_set = set(week_slice)
    last_week_d = week_slice[-1] if week_slice else target_d

    hours_left = max(1.0, (16 - est_now.hour) + (15 - est_now.minute) / 60.0) if est_now.hour < 16 else 1.5
    t_0dte = max(hours_left / (252.0 * 6.5), 1.0 / (252.0 * 24.0))

    day_buckets = {}
    week_buckets = {}
    vol_weight = 1.35 if NOISE_REDUCTION else 0.75

    for sym, yymmdd, opt_type, strike, opt in raw["week_opts"]:
        if yymmdd not in week_slice_set and yymmdd != target_d:
            continue

        grid_strike = round(round(strike / step) * step, 2)
        oi = float(opt.get("open_interest", 0) or 0)
        vol = float(opt.get("volume", 0) or 0)
        iv = float(opt.get("iv", 0) or 0.18)
        if iv > 3.0:
            iv = iv * 0.01
        raw_gamma = float(opt.get("gamma", 0) or 0)

        days_diff = yymmdd_to_days_diff(yymmdd, today_date)
        t_exp = t_0dte if days_diff == 0 else (days_diff * 6.5 + hours_left) / (252.0 * 6.5)
        gamma = bs_live_gamma(spot, strike, max(iv, 0.05), t_exp) if raw_gamma <= 1e-6 else raw_gamma

        base_unit_gex = gamma * spot * 0.000001
        static_sign = 1.0 if opt_type == "C" else -1.0
        ledger_rec = FLOW_LEDGER.get(sym)
        signed_vol = ledger_rec["signed_vol"] if ledger_rec else (vol * static_sign)
        dist_pct = abs(strike - spot) / spot
        flow_boost = 1.85 if dist_pct <= 0.018 else 1.15

        if yymmdd == target_d:
            if not LIVE_FLOW_MODE:
                d_gex = base_unit_gex * (oi + vol * vol_weight) * static_sign
                d_flow = static_sign * vol
            else:
                oi_keep_d = 0.42 if NOISE_REDUCTION else 0.62
                d_gex = base_unit_gex * (oi * oi_keep_d * static_sign + signed_vol * vol_weight * flow_boost)
                d_flow = signed_vol

            bd = day_buckets.get(grid_strike)
            if bd is None:
                bd = {"net_gex": 0.0, "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0, "flow_w": 0.0, "vol_t": 0.0}
                day_buckets[grid_strike] = bd
            bd["net_gex"] += d_gex
            bd["flow_w"] += d_flow
            bd["vol_t"] += vol
            if opt_type == "C":
                bd["call_oi"] += int(oi)
                bd["call_vol"] += int(vol)
            else:
                bd["put_oi"] += int(oi)
                bd["put_vol"] += int(vol)

        if yymmdd in week_slice_set:
            w_factor = 1.0 if (yymmdd == target_d or yymmdd == last_week_d) else 0.85
            if not LIVE_FLOW_MODE:
                w_gex = base_unit_gex * w_factor * (oi + vol * vol_weight) * static_sign
                w_flow = static_sign * vol
            else:
                oi_keep_w = 0.65 if NOISE_REDUCTION else 0.85
                w_gex = base_unit_gex * w_factor * (oi * oi_keep_w * static_sign + signed_vol * vol_weight * flow_boost)
                w_flow = signed_vol

            bw = week_buckets.get(grid_strike)
            if bw is None:
                bw = {"net_gex": 0.0, "call_oi": 0, "put_oi": 0, "call_vol": 0, "put_vol": 0, "flow_w": 0.0, "vol_t": 0.0}
                week_buckets[grid_strike] = bw
            bw["net_gex"] += w_gex
            bw["flow_w"] += w_flow
            bw["vol_t"] += vol
            if opt_type == "C":
                bw["call_oi"] += int(oi)
                bw["call_vol"] += int(vol)
            else:
                bw["put_oi"] += int(oi)
                bw["put_vol"] += int(vol)

    center_strike = round(round(spot / step) * step, 2)
    ladder_strikes = [round(center_strike + i * step, 2) for i in range(12, -13, -1)]

    day_gex_map = {}
    week_gex_map = {}
    for s in ladder_strikes:
        raw_d = day_buckets[s]["net_gex"] if s in day_buckets else 0.0
        raw_w = week_buckets[s]["net_gex"] if s in week_buckets else 0.0

        ema_d_key = (ticker_key, s, target_d, "DAY", LIVE_FLOW_MODE, NOISE_REDUCTION)
        ema_w_key = (ticker_key, s, target_d, "WEEK", LIVE_FLOW_MODE, NOISE_REDUCTION)

        if use_ema and ema_d_key in EMA_GEX_CACHE:
            day_gex_map[s] = 0.75 * EMA_GEX_CACHE[ema_d_key] + 0.25 * raw_d
        else:
            day_gex_map[s] = raw_d
        EMA_GEX_CACHE[ema_d_key] = day_gex_map[s]

        if use_ema and ema_w_key in EMA_GEX_CACHE:
            week_gex_map[s] = 0.75 * EMA_GEX_CACHE[ema_w_key] + 0.25 * raw_w
        else:
            week_gex_map[s] = raw_w
        EMA_GEX_CACHE[ema_w_key] = week_gex_map[s]

    day_meta = extract_profile_nodes(ladder_strikes, day_gex_map, spot, step)
    week_meta = extract_profile_nodes(ladder_strikes, week_gex_map, spot, step)

    strike_tags, hud, dynamic_alerts = analyze_dynamic_resonance_and_shifts(
        ticker_key, target_d, spot, step, ladder_strikes, day_gex_map, week_gex_map, day_meta, week_meta
    )

    active_buckets = week_buckets if WEEKLY_MODE else day_buckets
    active_map = week_gex_map if WEEKLY_MODE else day_gex_map
    active_meta = week_meta if WEEKLY_MODE else day_meta

    rows = []
    for s in ladder_strikes:
        b = active_buckets.get(s)
        val = active_map.get(s, 0.0)
        d_val = day_gex_map.get(s, 0.0)
        w_val = week_gex_map.get(s, 0.0)
        tag_info = strike_tags.get(s, {"icon": "", "note": ""})

        avg_flow = (b["flow_w"] / b["vol_t"]) if (b and b["vol_t"] > 0) else 0.0
        rows.append({
            "strike": s,
            "net_gex": val,
            "day_gex": d_val,
            "week_gex": w_val,
            "call_oi": b["call_oi"] if b else 0,
            "put_oi": b["put_oi"] if b else 0,
            "call_vol": b["call_vol"] if b else 0,
            "put_vol": b["put_vol"] if b else 0,
            "avg_flow": round(avg_flow, 2),
            "tac_icon": tag_info["icon"],
            "tac_note": tag_info["note"],
            "is_spot": (abs(s - center_strike) < 1e-4),
            "is_king": (abs(s - active_meta["king"]) < 1e-4),
            "is_call_wall": (abs(s - active_meta["cw"]) < 1e-4),
            "is_put_wall": (abs(s - active_meta["pw"]) < 1e-4),
            "is_flip": (abs(s - active_meta["flip"]) < 1e-4),
            "is_star": (s in active_meta["top_abs"] and abs(val) > 0),
            "intensity": round(min(1.0, abs(val) / active_meta["max_abs"]), 3),
        })

    return {
        "ticker": ticker_key,
        "exp": exp_label,
        "target_d": target_d,
        "spot": spot,
        "pct_change": pct_change,
        "zero_flip": active_meta["flip"],
        "call_wall": active_meta["cw"],
        "put_wall": active_meta["pw"],
        "king_node": active_meta["king"],
        "max_pos_val": active_meta["max_pos"],
        "min_neg_val": active_meta["min_neg"],
        "hud": hud,
        "rows": rows,
        "alerts": dynamic_alerts,
    }


def rebuild_render_cache(use_ema=True):
    global LAST_UPDATE_STR, AVAIL_DATES_LIST, ACTIVE_AUTO_DATE
    new_render = {}
    est_now = get_us_eastern_now()
    today_yymmdd = est_now.strftime("%y%m%d")
    after_close = is_after_market_close(est_now)

    sample_raw = RAW_CHAIN_CACHE.get("SPY") or RAW_CHAIN_CACHE.get("SPX") or next(iter(RAW_CHAIN_CACHE.values()), None)
    if sample_raw:
        avail = sample_raw["avail_dates"]
        auto_d, _, _ = resolve_active_dates(avail, est_now)
        ACTIVE_AUTO_DATE = f"{auto_d[2:4]}-{auto_d[4:6]}"
        AVAIL_DATES_LIST = [
            {"val": d, "label": format_yymmdd_label(d, today_yymmdd, after_close)}
            for d in avail
        ]

    for k in ["SPX", "XSP", "SPY", "QQQ"]:
        res = compute_ticker_gex(k, use_ema=use_ema)
        if res:
            new_render[k] = res
        elif k in RENDER_CACHE:
            new_render[k] = RENDER_CACHE[k]

    with CACHE_LOCK:
        RENDER_CACHE.update(new_render)
        LAST_UPDATE_STR = est_now.strftime("%H:%M:%S EST")


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


# ==================== 8. Flask 极速路由 ====================
@app.route("/api/gex")
def api_gex():
    with CACHE_LOCK:
        data_copy = dict(RENDER_CACHE)
        ts = LAST_UPDATE_STR
        avail_copy = list(AVAIL_DATES_LIST)
        auto_d = ACTIVE_AUTO_DATE

    all_alerts = []
    for k, v in data_copy.items():
        all_alerts.extend(v.get("alerts", []))

    return jsonify({
        "timestamp": ts,
        "ready": len(data_copy) > 0,
        "selected_exp": SELECTED_EXP,
        "active_auto_date": auto_d,
        "avail_dates": avail_copy,
        "weekly_mode": WEEKLY_MODE,
        "live_flow_mode": LIVE_FLOW_MODE,
        "noise_reduction": NOISE_REDUCTION,
        "data": data_copy,
        "alerts": all_alerts,
    })


@app.route("/api/set_date", methods=["POST"])
def set_date():
    global SELECTED_EXP, WEEKLY_MODE
    req = request.get_json(silent=True) or {}
    exp_val = req.get("exp", "AUTO")
    SELECTED_EXP = exp_val
    if exp_val != "AUTO":
        WEEKLY_MODE = False
    rebuild_render_cache(use_ema=False)
    return jsonify({"status": "ok", "selected_exp": SELECTED_EXP, "weekly_mode": WEEKLY_MODE})


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


# ==================== 9. 前端可视化界面 ====================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>GEX 实时量化指挥舱 (Top-1 唯一主靶心版)</title>
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
  .date-select {
    background: #0f172a;
    color: #38bdf8;
    border: 1px solid #0284c7;
    padding: 5px 10px;
    border-radius: 5px;
    font-size: 12px;
    font-weight: 800;
    cursor: pointer;
    outline: none;
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
    margin-bottom: 6px;
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

  .tactical-hud {
    font-size: 11px;
    font-weight: 700;
    padding: 4px 7px;
    border-radius: 4px;
    background: rgba(15, 23, 42, 0.85);
    border-left: 3px solid #38bdf8;
    line-height: 1.35;
  }

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
  .tac-dot {
    font-size: 12px;
    filter: drop-shadow(0 0 3px rgba(0,0,0,0.8));
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
    max-width: 310px;
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
    <select id="dateSelect" class="date-select" onchange="changeExpDate(this.value)">
      <option value="AUTO">🤖 自动换日 (16:15跳次日)</option>
    </select>
    <button id="weekBtn" class="btn active-day" onclick="toggleMode('/api/toggle_weekly')">📅 周期: 单日 (DAY)</button>
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
  <div class="loading-box">⚡ 正在锁定各列 Top-1 唯一双周期主靶心 (约 1~2 秒)...</div>
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

async function changeExpDate(expVal) {
  await fetch("/api/set_date", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ exp: expVal })
  });
  fetchGexData(true);
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

  const tacHtml = row.tac_note
    ? `<div style="margin-top:5px; padding:4px 6px; background:#1e293b; border-left:3px solid #f59e0b; color:#fde68a; font-weight:700; line-height:1.35;">${row.tac_note}</div>`
    : "";

  tt.innerHTML = `
    <div style="font-weight:800; color:#38bdf8; margin-bottom:4px;">${ticker} Strike ${row.strike}</div>
    <div>单日 GEX: <b>${formatGex(row.day_gex)}</b> | 本周 OPEX: <b>${formatGex(row.week_gex)}</b></div>
    <div>盘口流向: <b>${flowDesc}</b></div>
    <hr style="border:0; border-top:1px solid #334155; margin:4px 0;">
    <div>Call OI: ${row.call_oi.toLocaleString()} | Vol: ${row.call_vol.toLocaleString()}</div>
    <div>Put OI: ${row.put_oi.toLocaleString()} | Vol: ${row.put_vol.toLocaleString()}</div>
    ${tacHtml}
  `;
  tt.style.display = "block";
  tt.style.left = Math.min(e.clientX + 14, window.innerWidth - 320) + "px";
  tt.style.top = Math.min(e.clientY + 14, window.innerHeight - 180) + "px";
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

    const sel = document.getElementById("dateSelect");
    if (json.avail_dates && json.avail_dates.length > 0) {
      const autoText = json.active_auto_date ? `🤖 自动换日 (当前: ${json.active_auto_date})` : "🤖 自动换日 (16:15跳次日)";
      let optsHtml = `<option value="AUTO">${autoText}</option>`;
      for (const item of json.avail_dates) {
        const selectedAttr = (json.selected_exp === item.val) ? "selected" : "";
        optsHtml += `<option value="${item.val}" ${selectedAttr}>📅 ${item.label}</option>`;
      }
      if (sel.innerHTML !== optsHtml) {
        sel.innerHTML = optsHtml;
        sel.value = json.selected_exp || "AUTO";
      }
    }

    const weekBtn = document.getElementById("weekBtn");
    weekBtn.innerText = json.weekly_mode ? "📆 周期: 本周 (至周五OPEX)" : "📅 周期: 单日 (DAY)";
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
      const hud = d.hud || { text: "动态扫描中...", color: "#38bdf8" };

      let rowsHtml = "";
      for (const r of d.rows) {
        const style = getRowStyle(r.net_gex, r.intensity);
        const spotClass = r.is_spot ? "strike-row spot-row" : "strike-row";
        const icons = [
          r.is_spot ? `<span class="m-tag">M</span>` : "",
          r.is_king ? "👑" : "",
          r.is_call_wall ? "▲" : "",
          r.is_put_wall ? "▼" : "",
          r.is_flip ? "◆" : "",
          r.tac_icon ? `<span class="tac-dot">${r.tac_icon}</span>` : ""
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
            <div class="tactical-hud" style="border-left-color:${hud.color}; color:${hud.color};">
              ${hud.text}
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
    print("🚀 XSP/SPX GEX 指挥中心 (Top-1 唯一主靶心收敛版) 已启动")
    print("👉 请在浏览器打开: http://127.0.0.1:5000")
    print("=========================================================")
    app.run(host="0.0.0.0", port=5000, debug=False, threaded=True)
