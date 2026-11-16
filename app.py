import os, re, json, time, math, socket, platform, subprocess, threading, datetime
from collections import deque
import requests
from flask import Flask, jsonify, Response

app = Flask(__name__)

CBOE_URLS = {
    "SPX": "https://cdn.cboe.com/api/global/delayed_quotes/options/_SPX.json",
    "XSP": "https://cdn.cboe.com/api/global/delayed_quotes/options/_XSP.json",
    "SPY": "https://cdn.cboe.com/api/global/delayed_quotes/options/SPY.json",
    "QQQ": "https://cdn.cboe.com/api/global/delayed_quotes/options/QQQ.json",
}
HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
OPT_REGEX = re.compile(r"^([A-Z_]+?)(\d{6})([CP])(\d{8})$")
CACHE_FILE = "gex_auto_cache.json"
ROLLING_HISTORY = deque(maxlen=60)

STATE = {
    "mode": "next",
    "skew_adj": True,
    "last_update": "正在同步 CBOE 期权链...",
    "baseline_time": "自动锚定中...",
    "rolling_time": "积累滑动窗口中...",
    "trading_day": "",
    "rth_locked": False,
    "public_url": "云端/公网模式就绪",
    "lan_url": "",
    "baseline_data": {},
    "current_data": {},
    "alerts": [],
    "xsp_advice": {}
}

def get_est_now():
    return datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(hours=5)

def get_lan_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return f"http://{ip}:5050"
    except Exception:
        return "http://127.0.0.1:5050"

def generate_cloud_24h_package():
    cloud_dir = "cloud_24h_deploy"
    try:
        os.makedirs(cloud_dir, exist_ok=True)
        old_docker = os.path.join(cloud_dir, "Dockerfile")
        if os.path.exists(old_docker):
            os.remove(old_docker)
        with open(__file__, "r", encoding="utf-8") as f:
            self_code = f.read()
        with open(os.path.join(cloud_dir, "app.py"), "w", encoding="utf-8") as f:
            f.write(self_code)
        with open(os.path.join(cloud_dir, "requirements.txt"), "w", encoding="utf-8") as f:
            f.write("flask\nrequests\ngunicorn\n")
        print(f"☁️ 已生成免Docker纯Python云端包: ./{cloud_dir}/ (仅含 app.py 与 requirements.txt)")
    except Exception as e:
        print(f"[Cloud Gen Warn] {e}")

def start_public_tunnel():
    ext_url = os.environ.get("RENDER_EXTERNAL_URL")
    if ext_url or os.environ.get("RENDER") or os.environ.get("PORT"):
        STATE["public_url"] = ext_url or "24小时云端服务器运行中 (直接收藏当前网址)"
        return
    STATE["lan_url"] = get_lan_ip()
    is_win = platform.system().lower().startswith("win")
    bin_name = "cloudflared.exe" if is_win else "cloudflared"
    if not os.path.exists(bin_name):
        try:
            print("🌐 正在下载本机临时公网组件 cloudflared...")
            dl_url = (
                "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-windows-amd64.exe"
                if is_win else
                "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-linux-amd64"
            )
            r = requests.get(dl_url, stream=True, timeout=60)
            with open(bin_name, "wb") as f:
                for chunk in r.iter_content(chunk_size=65536):
                    if chunk: f.write(chunk)
            if not is_win: os.chmod(bin_name, 0o755)
        except Exception:
            STATE["public_url"] = STATE["lan_url"]
            return
    try:
        cmd = [bin_name if is_win else f"./{bin_name}", "tunnel", "--url", "http://127.0.0.1:5050"]
        proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding="utf-8", errors="ignore")
        url_pat = re.compile(r"(https://[a-zA-Z0-9\-]+\.trycloudflare\.com)")
        for line in proc.stdout:
            m = url_pat.search(line)
            if m:
                STATE["public_url"] = m.group(1)
                print(f"\n📱 手机流量(4G/5G)临时公网网址: {STATE['public_url']}\n")
                break
    except Exception:
        STATE["public_url"] = STATE["lan_url"]

def save_baseline_cache():
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump({
                "trading_day": STATE["trading_day"], "rth_locked": STATE["rth_locked"],
                "baseline_time": STATE["baseline_time"], "baseline_data": STATE["baseline_data"]
            }, f)
    except Exception:
        pass

def load_baseline_cache():
    if not os.path.exists(CACHE_FILE): return
    try:
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        est_today = get_est_now().strftime("%Y-%m-%d")
        if data.get("trading_day") == est_today and data.get("baseline_data"):
            b_data = data["baseline_data"]
            for tk, item in b_data.items():
                if "strike_map" in item:
                    item["strike_map"] = {float(k): float(v) for k, v in item["strike_map"].items()}
            STATE.update({
                "trading_day": est_today, "rth_locked": bool(data.get("rth_locked", False)),
                "baseline_time": data.get("baseline_time", "已恢复"), "baseline_data": b_data
            })
    except Exception:
        pass

def bs_gamma_fallback(spot, strike, iv, dte_days):
    t = max(dte_days, 0.25) / 365.0
    sigma = iv if (iv and 0.03 < iv < 3.0) else 0.18
    if spot <= 0 or strike <= 0: return 0.0
    d1 = (math.log(spot / strike) + 0.5 * (sigma ** 2) * t) / (sigma * math.sqrt(t))
    return (math.exp(-0.5 * (d1 ** 2)) / math.sqrt(2.0 * math.pi)) / (spot * sigma * math.sqrt(t))

def fetch_cboe_gex(ticker, url, strike_step, mode="next", skew_adj=True, num_strikes=28):
    resp = requests.get(url, headers=HEADERS, timeout=12)
    resp.raise_for_status()
    payload = resp.json().get("data", {})
    spot = float(payload.get("current_price") or payload.get("close") or 0)
    options = payload.get("options", [])
    if spot <= 0 or not options: raise ValueError(f"{ticker} 数据为空")

    est_now = get_est_now()
    today_str = est_now.strftime("%y%m%d")
    is_after_close = (est_now.hour >= 16)

    all_exps = sorted({m.group(2) for opt in options if (m := OPT_REGEX.match(opt.get("option", ""))) and m.group(2) >= today_str})
    if not all_exps:
        all_exps = sorted({m.group(2) for opt in options if (m := OPT_REGEX.match(opt.get("option", "")))})[-2:]
    valid_exps = [e for e in all_exps if not (is_after_close and e == today_str)] or all_exps

    if mode == "weekly":
        target_exps = set(valid_exps[:5])
        exp_label = f"全周({len(target_exps)}期)"
    else:
        target_exps = {valid_exps[0]}
        e0 = valid_exps[0]
        exp_label = f"{e0[2:4]}-{e0[4:]}"

    chain_group = {}
    for opt in options:
        m = OPT_REGEX.match(opt.get("option", ""))
        if not m or m.group(2) not in target_exps: continue
        exp_str, opt_type = m.group(2), m.group(3)
        strike = int(m.group(4)) / 1000.0
        if strike_step >= 5 and (strike % 5 != 0): continue

        key = (exp_str, strike)
        if key not in chain_group:
            chain_group[key] = {
                "C_oi": 0.0, "C_vol": 0.0, "C_g": 0.0, "C_iv": 0.0,
                "P_oi": 0.0, "P_vol": 0.0, "P_g": 0.0, "P_iv": 0.0, "P_bid": 0.0, "P_ask": 0.0, "P_last": 0.0
            }
        rg, riv = float(opt.get("gamma") or 0.0), float(opt.get("iv") or 0.0)
        chain_group[key][f"{opt_type}_oi"] += float(opt.get("open_interest") or 0.0)
        chain_group[key][f"{opt_type}_vol"] += float(opt.get("volume") or 0.0)
        if opt_type == "P":
            chain_group[key]["P_bid"] = float(opt.get("bid") or 0.0)
            chain_group[key]["P_ask"] = float(opt.get("ask") or 0.0)
            chain_group[key]["P_last"] = float(opt.get("last_trade_price") or 0.0)
        if rg > 0: chain_group[key][f"{opt_type}_g"] = rg
        if riv > 0: chain_group[key][f"{opt_type}_iv"] = riv

    raw_strike_data = {}
    for (exp_str, strike), d in chain_group.items():
        exp_dt = datetime.datetime.strptime("20" + exp_str, "%Y%m%d").date()
        dte_days = max((exp_dt - est_now.date()).days, 0.35)
        ug = (d["C_g"] or d["P_g"]) if strike >= spot else (d["P_g"] or d["C_g"])
        riv = (d["C_iv"] or d["P_iv"]) if strike >= spot else (d["P_iv"] or d["C_iv"])
        bs_g = bs_gamma_fallback(spot, strike, riv, dte_days)
        if ug <= 0 or ug > bs_g * 3.0: ug = bs_g

        c_eff = d["C_oi"] + d["C_vol"] * 0.30
        p_eff = d["P_oi"] + d["P_vol"] * 0.30
        p_flow_mult = 0.55 if (skew_adj and d["P_ask"] > d["P_bid"] > 0 and 0 < d["P_last"] <= 0.5 * (d["P_bid"] + d["P_ask"])) else 1.0

        if strike not in raw_strike_data:
            raw_strike_data[strike] = {"c_gex": 0.0, "p_gex": 0.0}
        scale = 2.5 if ticker == "SPX" else (10.0 if ticker == "XSP" else 1.0)
        factor = ug * 100.0 * spot * scale / 1e6
        raw_strike_data[strike]["c_gex"] += c_eff * factor
        raw_strike_data[strike]["p_gex"] += (p_eff * p_flow_mult) * factor

    all_strikes = sorted(raw_strike_data.keys())
    atm = min(all_strikes, key=lambda k: abs(k - spot)) if all_strikes else round(spot / strike_step) * strike_step
    sel = sorted([k for k in all_strikes if abs(k - atm) <= strike_step * (num_strikes // 2)], reverse=True)

    strike_map = {}
    if skew_adj and sel:
        tot_c = sum(raw_strike_data[k]["c_gex"] for k in sel)
        tot_p = sum(raw_strike_data[k]["p_gex"] for k in sel)
        global_bal = min(max((tot_c / tot_p) * 0.92, 0.32), 1.0) if tot_p > tot_c * 1.15 else 1.0
        temp_gex = {k: raw_strike_data[k]["c_gex"] - raw_strike_data[k]["p_gex"] * global_bal for k in sel}
        asc_sel = sorted(sel)
        for i, k in enumerate(asc_sel):
            win = asc_sel[max(0, i - 2):min(len(asc_sel), i + 3)]
            local_mean = sum(temp_gex[w] for w in win) / len(win)
            strike_map[k] = (0.55 * temp_gex[k] + 0.45 * (temp_gex[k] - local_mean * 0.85)) if (k < spot and temp_gex[k] < 0 and local_mean < 0) else temp_gex[k]
    else:
        strike_map = {k: raw_strike_data[k]["c_gex"] - raw_strike_data[k]["p_gex"] for k in sel}

    pos = {k: strike_map[k] for k in sel if strike_map[k] > 0.005}
    neg = {k: strike_map[k] for k in sel if strike_map[k] < -0.005}
    max_pos = max(pos, key=pos.get) if pos else atm
    max_neg = min(neg, key=neg.get) if neg else atm
    king = max(sel, key=lambda k: abs(strike_map[k])) if sel else atm

    up_pos = {k: v for k, v in pos.items() if k >= spot}
    dn_pos = {k: v for k, v in pos.items() if k < spot}
    dn_neg = {k: v for k, v in neg.items() if k <= spot}

    cw = max(up_pos, key=up_pos.get) if up_pos else max([k for k in sel if k >= spot], default=atm)
    below_cands = [k for k in sel if k <= spot - strike_step]
    pw = max(dn_pos, key=dn_pos.get) if dn_pos else (max(below_cands, key=lambda k: strike_map[k]) if below_cands else atm - strike_step * 2)
    pt = min(dn_neg, key=dn_neg.get) if dn_neg else max_neg

    flip = atm
    for i in range(len(sel) - 1):
        k1, k2 = sel[i], sel[i + 1]
        if (strike_map[k1] >= 0) != (strike_map[k2] >= 0):
            if abs(k1 - spot) < abs(flip - spot) or flip == atm: flip = k1

    rows = []
    mp_val = pos.get(max_pos, 1.0) or 1.0
    mn_val = abs(neg.get(max_neg, -1.0)) or 1.0

    for k in sel:
        v = strike_map[k]
        b = [sym for cond, sym in [(k == king, "👑"), (k == cw, "▲"), (k == pt, "▼"), (k == flip, "◆"), (k == atm, "M")] if cond]
        r = (v / mp_val) if v >= 0 else (abs(v) / mn_val)
        cls = ("gex-king-pos" if r >= 0.65 else ("gex-mid-pos" if r >= 0.25 else "gex-low-pos")) if v >= 0 else ("gex-king-neg" if r >= 0.65 else ("gex-mid-neg" if r >= 0.25 else "gex-low-neg"))
        star = "*" if (k == max_pos or k == max_neg) else ""
        av = abs(v)
        v_str = f"{'+' if v >= 0 else '-'}${av:.1f}M{star}" if av >= 1.0 else f"{'+' if v >= 0 else '-'}${av*1000:.0f}K{star}"
        xsp_eq = k / 10.0 if ticker == "SPX" else k
        pos_rel = "above" if k > spot + strike_step * 0.3 else ("below" if k < spot - strike_step * 0.3 else "at")
        rows.append({
            "strike": f"{k:.0f}" if strike_step >= 5 else f"{k:.1f}" if ticker == "XSP" else f"{k:.0f}",
            "raw_strike": k, "xsp_eq": f"{xsp_eq:.1f}", "pos_rel": pos_rel,
            "gex": round(v, 2), "gex_str": v_str, "badges": b, "color_cls": cls, "is_spot": k == atm
        })

    return {
        "ticker": ticker, "spot": spot, "exp": exp_label, "atm_strike": atm,
        "call_wall": cw, "put_wall": pw, "put_trigger": pt, "flip_strike": flip,
        "strike_map": strike_map, "rows": rows
    }

def check_reference_shifts(curr, ref_data, ref_label, alerts_dict):
    if not ref_data or "SPX" not in ref_data or "SPX" not in curr: return
    spx, spy, xsp, b_spx = curr["SPX"], curr.get("SPY"), curr.get("XSP"), ref_data["SPX"]
    spot, cw, pw, pt, flip = spx["spot"], spx["call_wall"], spx["put_wall"], spx["put_trigger"], spx["flip_strike"]
    xsp_spot = spot / 10.0
    if spx["strike_map"].get(pw, 0) <= 0 and xsp and xsp["strike_map"].get(xsp["put_wall"], 0) > 0:
        pw = xsp["put_wall"] * 10.0

    old_spot, old_cw = b_spx["spot"], b_spx["call_wall"]
    old_v, now_v = b_spx["strike_map"].get(old_cw, 0), spx["strike_map"].get(old_cw, 0)

    if old_cw > spot and old_v > 3.0 and now_v < old_v * 0.62:
        aid = f"NODE_GONE_{old_cw:.0f}"
        alerts_dict[aid] = {
            "id": aid, "level": "danger", "sound": "alarm",
            "voice": f"警报！对比{ref_label}，上方正Gamma节点 {old_cw/10:.1f} 撤单消失，请下调 XSP Call 止盈目标至 {cw/10:.1f}",
            "title": f"🚨【信号1：上方节点蒸发 ({ref_label}对比)】SPX {old_cw:.0f} (XSP {old_cw/10:.1f})",
            "desc": f"上方正GEX节点从 +${old_v:.1f}M 骤降至 +${now_v:.1f}M！立即将 XSP Call 止盈位下调至 {cw/10:.1f} (▲)！"
        }
    old_pw_v, now_pw_v = b_spx["strike_map"].get(pw, 0), spx["strike_map"].get(pw, 0)
    if old_pw_v > 0 and now_pw_v > old_pw_v * 1.25 and now_pw_v > 5.0:
        aid = f"CHOP_WALL_{pw:.0f}"
        alerts_dict[aid] = {
            "id": aid, "level": "warning", "sound": "beep",
            "voice": f"注意！对比{ref_label}，下方 {pw/10:.1f} 正Gamma铁底增厚，进入横盘钉死模式",
            "title": f"🧱【信号2：底部铁墙增厚 ({ref_label}对比)】SPX {pw:.0f} (XSP {pw/10:.1f}) 增至 +${now_pw_v:.1f}M",
            "desc": f"下方护城河增厚，指数易被钉在 XSP {pw/10:.1f}~{cw/10:.1f} 磨时间！0DTE 离场防磨损，周五周权依托 {pw/10:.1f} 持有。"
        }
    if cw < old_cw:
        aid = f"CEIL_LOW_{cw:.0f}"
        alerts_dict[aid] = {
            "id": aid, "level": "warning", "sound": "beep",
            "voice": f"提示！对比{ref_label}，上方第一阻力轨下移至 XSP {cw/10:.1f}，反抽触及请先减仓",
            "title": f"📉【信号3：阻力上轨下移 ({ref_label}对比)】▲ 从 {old_cw:.0f} 下移至 {cw:.0f} (XSP {cw/10:.1f})",
            "desc": f"日内第一反弹天花板已降至 XSP {cw/10:.1f}，多单反抽触及必须先减仓 30%~50%！"
        }
    reclaimed_flip = (old_spot < b_spx["flip_strike"] and spot >= flip)
    spy_reclaimed = (spy and "SPY" in ref_data and ref_data["SPY"]["spot"] < ref_data["SPY"]["flip_strike"] and spy["spot"] >= spy["flip_strike"])
    neg_squeeze = (spot > old_spot + 7.5 and spx["strike_map"].get(spx["atm_strike"], 0) < -1.5)
    if reclaimed_flip or spy_reclaimed or neg_squeeze:
        rec_strike = math.ceil(xsp_spot) + 1
        aid = f"ROCKET_SQUEEZE_{int(xsp_spot)}"
        alerts_dict[aid] = {
            "id": aid, "level": "rocket", "sound": "rocket",
            "voice": f"小火箭逼空点火信号！指数冲破负Gamma加速区，可顺势关注 XSP {rec_strike} Call，第一止盈看 {cw/10:.1f}",
            "title": f"🚀【信号4：小火箭负GEX逼空点火 ({ref_label}触发)】现价突破 XSP {xsp_spot:.1f}",
            "desc": f"做市商被迫追买期货助涨！👉 XSP 操作：右侧顺势关注 XSP {rec_strike}C，第一止盈看 XSP {cw/10:.1f} (▲)！"
        }
    if old_spot > pt and spot <= pt - 2.0:
        aid = f"FLUSH_BREAK_{pt:.0f}"
        alerts_dict[aid] = {
            "id": aid, "level": "danger", "sound": "alarm",
            "voice": f"严重警报！指数跌穿下方破位红线 XSP {pt/10:.1f}，多单请严格执行止损",
            "title": f"🌊【信号5：铁底坍塌破位 ({ref_label}触发)】跌穿 SPX {pt:.0f} (XSP {pt/10:.1f} ▼)",
            "desc": f"指数放量跌穿 ▼ 破位点！👉 XSP 操作：日内多单无条件止损，反手关注 XSP {math.floor(xsp_spot)} Put！"
        }

def refresh_all_now():
    steps = {"SPX": 5, "XSP": 1, "SPY": 1, "QQQ": 1}
    new_data = {}
    for tk, url in CBOE_URLS.items():
        try:
            new_data[tk] = fetch_cboe_gex(tk, url, steps[tk], mode=STATE["mode"], skew_adj=STATE["skew_adj"], num_strikes=28)
        except Exception as e:
            print(f"[Warn] {tk}: {e}")
    if not new_data or "SPX" not in new_data: return

    est_now = get_est_now()
    today_str, time_str = est_now.strftime("%Y-%m-%d"), est_now.strftime("%H:%M:%S")
    is_rth = (est_now.hour > 9 or (est_now.hour == 9 and est_now.minute >= 30)) and (est_now.hour < 16)

    if not STATE["baseline_data"] or STATE["trading_day"] != today_str:
        STATE.update({"trading_day": today_str, "rth_locked": is_rth, "baseline_data": new_data, "baseline_time": f"{time_str} (自动锚定)"})
        save_baseline_cache()
    elif is_rth and not STATE["rth_locked"]:
        STATE.update({"rth_locked": True, "baseline_data": new_data, "baseline_time": f"{time_str} (9:30开盘自动锁定)"})
        ROLLING_HISTORY.clear()
        save_baseline_cache()

    now_ts = time.time()
    if not ROLLING_HISTORY or (now_ts - ROLLING_HISTORY[-1]["ts"] >= 60):
        ROLLING_HISTORY.append({"ts": now_ts, "time": time_str, "data": new_data})
    rolling_snap = ROLLING_HISTORY[0]
    for s in ROLLING_HISTORY:
        if now_ts - s["ts"] >= 900: rolling_snap = s
    STATE["rolling_time"] = f"{rolling_snap['time']} (滑动窗口)"

    alerts_dict = {}
    check_reference_shifts(new_data, STATE["baseline_data"], "今日基准", alerts_dict)
    check_reference_shifts(new_data, rolling_snap["data"], "15分钟前", alerts_dict)

    spx, xsp = new_data["SPX"], new_data.get("XSP")
    spot, cw, pw, pt, flip = spx["spot"], spx["call_wall"], spx["put_wall"], spx["put_trigger"], spx["flip_strike"]
    xsp_spot = spot / 10.0
    if spx["strike_map"].get(pw, 0) <= 0 and xsp and xsp["strike_map"].get(xsp["put_wall"], 0) > 0:
        pw = xsp["put_wall"] * 10.0

    if spot <= pt:
        regime, a0, awk = f"🌊 负Gamma破位区 (处于 ▼ {pt:.0f})", f"反抽不破 XSP {flip/10:.1f} (◆) 偏空，勿接刀 0DTE Call。", f"周权 XSP Call 等站稳 {pw/10:.1f} 上方再操作。"
    elif spot >= flip:
        regime, a0, awk = f"📈 正Gamma偏多区 (站上 ◆ {flip:.0f})", f"回踩 XSP {pw/10:.1f} 企稳短多，触及 {cw/10:.1f} (▲) 止盈。", f"周权 760C 持有，冲高进入 XSP {cw/10:.1f} 减半仓锁利。"
    else:
        regime, a0, awk = f"⚖️ 多空拉锯修复区 ({spx['exp']})", f"支撑看 XSP {pw/10:.1f}，反弹目标看 {flip/10:.1f} (◆) 与 {cw/10:.1f} (▲)。", f"紧盯能否收复 XSP {flip/10:.1f} (◆)，冲高至 {cw/10:.1f} 减仓。"

    STATE.update({
        "current_data": new_data, "last_update": time_str, "alerts": list(alerts_dict.values()),
        "xsp_advice": {
            "xsp_spot": f"{xsp_spot:.2f}", "spx_spot": f"{spot:.2f}", "regime": regime,
            "xsp_support": f"{pw/10:.1f} (SPX {pw:.0f})", "xsp_resistance": f"{cw/10:.1f} (SPX {cw:.0f} ▲)",
            "xsp_breakdown": f"{pt/10:.1f} (SPX {pt:.0f} ▼)", "xsp_flip": f"{flip/10:.1f} (SPX {flip:.0f} ◆)",
            "action_0dte": a0, "action_weekly": awk
        }
    })

def background_updater():
    load_baseline_cache()
    last_self_ping = 0
    while True:
        refresh_all_now()
        ext_url = os.environ.get("RENDER_EXTERNAL_URL")
        if ext_url and (time.time() - last_self_ping >= 600):
            try:
                requests.get(f"{ext_url.rstrip('/')}/api/ping", timeout=8)
                last_self_ping = time.time()
            except Exception:
                pass
        time.sleep(20)

HTML_LINES = [
    '<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1.0,maximum-scale=1.0,user-scalable=no">',
    '<title>XSP / SPX 实时 GEX 作战指挥舱</title><style>',
    'body{background:#0b0f17;color:#e2e8f0;font-family:-apple-system,BlinkMacSystemFont,"Microsoft YaHei",sans-serif;margin:0;padding:8px}',
    '.top-bar{display:flex;justify-content:space-between;align-items:center;background:#141b2d;padding:8px 12px;border-radius:8px;border:1px solid #232f46;margin-bottom:8px;flex-wrap:wrap;gap:6px}',
    '.btn{background:#2563eb;color:#fff;border:none;padding:6px 9px;border-radius:5px;cursor:pointer;font-size:11.5px;font-weight:600;margin:1px}',
    '.btn-sec{background:#334155}.btn-gold{background:#b45309}.btn-green{background:#059669}.btn-rocket{background:linear-gradient(90deg,#7e22ce,#ec4899)}',
    '.legend-bar{display:flex;align-items:center;gap:5px;flex-wrap:wrap;background:#111827;padding:6px 8px;border-radius:8px;border:1px solid #1f2937;margin-bottom:8px;font-size:11.5px}',
    '.lg-badge{padding:2px 6px;border-radius:4px;cursor:pointer;font-weight:600;border:1px solid rgba(255,255,255,0.12)}',
    '.drawer{display:none;background:#111827;border:1px solid #38bdf8;border-radius:8px;padding:10px;margin-bottom:10px;font-size:11.5px;line-height:1.45}',
    '.drawer-grid{display:grid;grid-template-columns:1fr 1fr;gap:10px}',
    '.d-table{width:100%;border-collapse:collapse}.d-table th,.d-table td{border:1px solid #263147;padding:5px;text-align:left}.d-table th{background:#1e293b;color:#38bdf8}',
    '.alert-box{padding:8px 12px;border-radius:6px;margin-bottom:8px;font-size:12.5px;cursor:pointer}',
    '.alert-danger{background:rgba(220,38,38,0.22);border-left:4px solid #ef4444;color:#fecaca}',
    '.alert-warning{background:rgba(234,179,8,0.22);border-left:4px solid #eab308;color:#fef08a}',
    '.alert-rocket{background:linear-gradient(90deg,rgba(126,34,206,0.35),rgba(236,72,153,0.25));border-left:4px solid #f472b6;color:#fdf2f8}',
    '.cmd-panel{background:#111827;border:1px solid #374151;border-radius:8px;padding:10px;margin-bottom:10px;display:grid;grid-template-columns:1.25fr 2fr;gap:10px}',
    '.levels-grid{display:grid;grid-template-columns:1fr 1fr;gap:6px;font-size:12px}',
    '.lvl-card{background:#1e293b;padding:6px 8px;border-radius:6px;cursor:pointer}.lvl-label{color:#94a3b8;font-size:10.5px}.lvl-val{font-size:13.5px;font-weight:700;margin-top:2px}',
    '.advice-box{font-size:12.5px;line-height:1.55;background:#1e293b;padding:8px 12px;border-radius:6px;border-left:4px solid #38bdf8}',
    '.grid-4{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:8px}',
    '.col-card{background:#131824;border:1px solid #222b3d;border-radius:8px;overflow:hidden}',
    '.col-hdr{padding:7px;background:#192132;border-bottom:1px solid #263147}',
    '.col-title{display:flex;justify-content:space-between;font-weight:700;font-size:13px;white-space:nowrap}',
    '.pills{display:flex;gap:3px;flex-wrap:wrap;margin-top:5px;font-size:10.5px;font-weight:600}',
    '.pill{padding:1px 4px;border-radius:3px;background:#0f172a;border:1px solid #334155;cursor:pointer}',
    '.pill-up{color:#facc15}.pill-dn{color:#c084fc}.pill-fl{color:#60a5fa}',
    'table.gex-tbl{width:100%;border-collapse:collapse;font-size:11.5px;font-variant-numeric:tabular-nums}',
    'table.gex-tbl td{padding:3.5px 6px;border-bottom:1px solid rgba(255,255,255,0.04);cursor:pointer;white-space:nowrap}',
    '.strike-col{font-weight:600;color:#cbd5e1}.gex-col{text-align:right;font-weight:700}',
    '.gex-king-pos{background:#eab308;color:#0f172a}.gex-mid-pos{background:#2f855a;color:#f0fff4}.gex-low-pos{background:#1b4332;color:#a7f3d0}',
    '.gex-king-neg{background:#6b21a8;color:#f3e8ff}.gex-mid-neg{background:#4c1d95;color:#ddd6fe}.gex-low-neg{background:#1e293b;color:#94a3b8}',
    '.spot-row{outline:2px solid #38bdf8}',
    '#floating-tip{position:fixed;display:none;z-index:9999;max-width:340px;background:rgba(15,23,42,0.98);color:#f8fafc;border:1px solid #38bdf8;border-radius:8px;padding:10px;font-size:12px;line-height:1.5;box-shadow:0 10px 25px rgba(0,0,0,0.8)}',
    '#floating-tip .tip-title{font-weight:700;color:#38bdf8;font-size:13px;margin-bottom:4px;border-bottom:1px solid #334155;padding-bottom:3px}',
    '#floating-tip .tip-act{color:#fde047;font-weight:600;margin-top:4px}',
    '#modal-overlay,#qr-overlay{position:fixed;top:0;left:0;width:100%;height:100%;background:rgba(0,0,0,0.75);display:none;justify-content:center;align-items:center;z-index:10000}',
    '#modal-card,#qr-card{background:#111827;border:2px solid #f472b6;border-radius:12px;max-width:460px;width:90%;padding:18px;box-shadow:0 0 40px rgba(0,0,0,0.8)}',
    '@media (max-width:920px){',
    '  .cmd-panel{grid-template-columns:1fr}.drawer-grid{grid-template-columns:1fr}',
    '  .grid-4{grid-template-columns:repeat(2,minmax(0,1fr));gap:6px}',
    '  table.gex-tbl{font-size:11px}table.gex-tbl td{padding:3px 4px}',
    '  .col-title{font-size:12px}#floating-tip{left:4%!important;top:auto!important;bottom:14px!important;width:86%!important;max-width:none}',
    '}',
    '.force-1col{grid-template-columns:1fr!important}',
    '</style></head><body onclick="hideTip()">',
    '<div id="floating-tip" onclick="hideTip()"></div>',
    '<div id="modal-overlay" onclick="closeModal()"><div id="modal-card" onclick="event.stopPropagation()">',
    '<div id="m-title" style="font-size:16.5px;font-weight:800;color:#f472b6;margin-bottom:8px;"></div>',
    '<div id="m-desc" style="font-size:13.5px;line-height:1.55;color:#e2e8f0;margin-bottom:14px;white-space:pre-line;"></div>',
    '<div style="display:flex;justify-content:flex-end;gap:8px;">',
    '<button class="btn btn-sec" onclick="replayVoice()">🔊 重播语音</button>',
    '<button class="btn" onclick="closeModal()">✅ 我已收到</button></div></div></div>',
    '<div id="qr-overlay" onclick="closeQr()"><div id="qr-card" style="border-color:#10b981;text-align:center;" onclick="event.stopPropagation()">',
    '<div style="font-size:17px;font-weight:800;color:#34d399;margin-bottom:6px;">📱 手机扫码随时看盘 (支持iPhone双列)</div>',
    '<img id="qr-img" src="" style="width:190px;height:190px;background:#fff;padding:6px;border-radius:8px;margin-bottom:8px;"/>',
    '<div style="background:#1e293b;padding:7px;border-radius:6px;font-size:11.5px;word-break:break-all;color:#38bdf8;margin-bottom:6px;" id="pub-url-txt"></div>',
    '<div style="font-size:11px;color:#94a3b8;margin-bottom:12px;" id="lan-url-txt"></div>',
    '<button class="btn btn-green" onclick="closeQr()">✅ 完成</button></div></div>',
    '<div class="top-bar"><div><b style="font-size:16px;">⚡ XSP / SPX 全自动 GEX 指挥舱</b><div id="meta-info" style="font-size:11px;color:#4ade80;margin-top:2px;">同步中...</div></div>',
    '<div><button class="btn btn-green" onclick="openQr()">📱 手机扫码</button>',
    '<button class="btn btn-rocket" onclick="testRocketAlert()">🚀 测试小火箭</button>',
    '<button class="btn btn-sec" onclick="toggleAudio()" id="audio-btn">🔊 语音:开</button>',
    '<button class="btn btn-gold" onclick="toggleDrawer()">📖 字典&铁律</button>',
    '<button class="btn btn-sec" onclick="toggleCols()" id="col-btn">📱 双列/单列</button>',
    '<button class="btn btn-sec" onclick="toggleSkew()" id="skew-btn">⚖️ 去噪:开</button>',
    '<button class="btn btn-sec" onclick="toggleMode()">🔄 单日/全周</button></div></div>',
    '<div class="legend-bar"><span style="color:#94a3b8;font-weight:600;">💡 点击/悬停色块看XSP指令：</span>',
    '<span class="lg-badge gex-king-pos" onclick="event.stopPropagation();showTip(event,\'KING_POS\')">🟨 亮黄墙+GEX*</span>',
    '<span class="lg-badge gex-mid-pos" onclick="event.stopPropagation();showTip(event,\'MID_POS\')">🟩 青绿缓冲</span>',
    '<span class="lg-badge gex-king-neg" onclick="event.stopPropagation();showTip(event,\'KING_NEG\')">🟪 深紫核爆-GEX*</span>',
    '<span class="lg-badge" style="background:#1e293b;color:#facc15" onclick="event.stopPropagation();showTip(event,\'CROWN\')">👑 皇冠</span>',
    '<span class="lg-badge" style="background:#1e293b;color:#facc15" onclick="event.stopPropagation();showTip(event,\'UP_WALL\')">▲ 上轨阻力</span>',
    '<span class="lg-badge" style="background:#1e293b;color:#c084fc" onclick="event.stopPropagation();showTip(event,\'DN_TRIG\')">▼ 破位红线</span>',
    '<span class="lg-badge" style="background:#1e293b;color:#60a5fa" onclick="event.stopPropagation();showTip(event,\'FLIP\')">◆ 翻转点</span></div>',
    '<div class="drawer" id="guide-drawer"><div class="drawer-grid"><div><b style="color:#facc15;">一、StrikeGEX 标记拆解与 XSP 实盘字典</b>',
    '<table class="d-table" style="margin-top:4px;"><tr><th>标记</th><th>含义</th><th>XSP 傻瓜操作指令</th></tr>',
    '<tr><td><b style="color:#eab308">🟨 亮黄+*</b></td><td>最大正GEX铁墙</td><td>跌到下方黄墙<b>严禁追Put</b>；涨到上方黄墙<b>多单止盈</b></td></tr>',
    '<tr><td><b style="color:#c084fc">🟪 深紫+👑</b></td><td>最大负GEX核爆区</td><td>跌入紫区<b>顺势买Put</b>；向上冲破紫区<b>追Call博小火箭逼空</b></td></tr>',
    '<tr><td><b style="color:#facc15">▲ 上三角</b></td><td>上方第一阻力轨</td><td>XSP Call <b>第一刚性减仓/止盈位</b>(触及先平30%~50%)</td></tr>',
    '<tr><td><b style="color:#c084fc">▼ 下三角</b></td><td>下方破位触发点</td><td>XSP Call <b>绝对硬止损线</b>；实体跌穿无条件砍多单</td></tr>',
    '<tr><td><b style="color:#60a5fa">◆ 菱形</b></td><td>多空翻转分界线</td><td>◆上方高抛低吸；◆下方反弹皆为反抽，<b>勿死拿远端虚值Call</b></td></tr></table></div>',
    '<div><b style="color:#38bdf8;">二、六大盘中异动信号与 XSP 应对铁律</b>',
    '<table class="d-table" style="margin-top:4px;"><tr><th>信号</th><th>特征</th><th>XSP 应对铁律</th></tr>',
    '<tr><td><b>🚨 1.节点蒸发</b></td><td>上方正GEX缩水超40%</td><td><b>立刻下调Call止盈至最近▲</b>，0DTE远端Call趁反抽清仓</td></tr>',
    '<tr><td><b>🧱 2.铁墙增厚</b></td><td>下方黄墙增厚超25%</td><td>横盘绞肉机！<b>0DTE立刻离场</b>；周权Call依托铁底持有</td></tr>',
    '<tr><td><b>📉 3.上轨下移</b></td><td>顶部▲向下移动</td><td><b>多单反抽到新▲位置必须先减仓</b>，不可盲目格局</td></tr>',
    '<tr><td><b>🚀 4.小火箭逼空</b></td><td>冲破◆或深紫负GEX区</td><td>做市商追买期货！<b>顺势买XSP浅虚值Call</b>，拿到▲止盈</td></tr>',
    '<tr><td><b>🌊 5.铁底破位</b></td><td>跌穿黄墙并击穿▼</td><td>触发负Gamma瀑布！<b>XSP多单无条件止损</b>，反手买Put</td></tr></table></div></div></div>',
    '<div id="alerts-container"></div>',
    '<div class="cmd-panel" id="cmd-panel" style="display:none;"><div class="levels-grid">',
    '<div class="lvl-card" onclick="event.stopPropagation();showTip(event,\'SPOT_M\')"><div class="lvl-label">XSP 实时折算点位</div><div class="lvl-val" id="v-spot" style="color:#38bdf8;">-</div></div>',
    '<div class="lvl-card" onclick="event.stopPropagation();showTip(event,\'FLIP\')"><div class="lvl-label">多空翻转点 (◆ Flip)</div><div class="lvl-val" id="v-flip" style="color:#60a5fa;">-</div></div>',
    '<div class="lvl-card" onclick="event.stopPropagation();showTip(event,\'UP_WALL\')"><div class="lvl-label">上方第一阻力 (▲ Call Wall)</div><div class="lvl-val" id="v-cw" style="color:#facc15;">-</div></div>',
    '<div class="lvl-card" onclick="event.stopPropagation();showTip(event,\'PW_CARD\')"><div class="lvl-label">下方支撑墙 (Put Support)</div><div class="lvl-val" id="v-pw" style="color:#4ade80;">-</div></div></div>',
    '<div class="advice-box"><div style="font-weight:700;color:#38bdf8;margin-bottom:3px;" id="v-regime">-</div>',
    '<div><b>🎯 短线(0DTE)：</b><span id="v-0dte">-</span></div>',
    '<div style="margin-top:3px;"><b>📅 周五(周权)：</b><span id="v-weekly">-</span></div>',
    '<div style="margin-top:3px;font-size:11px;color:#94a3b8;">🚨 破位红线 (▼ Put Trigger)：<span id="v-pt" style="color:#f87171;font-weight:700;">-</span></div></div></div>',
    '<div class="grid-4" id="gex-grid"></div><script>',
    'let audioEnabled=true,seenAlerts=new Set(),lastVoiceText="",lastAdvice={},pubUrl="",lanUrl="";',
    'function playTone(type){if(!audioEnabled)return;try{const ctx=new (window.AudioContext||window.webkitAudioContext)();',
    'const notes=type==="rocket"?[523.25,659.25,783.99,1046.5]:(type==="alarm"?[880,440,880,440]:[660,880]);',
    'notes.forEach((freq,idx)=>{const osc=ctx.createOscillator(),gain=ctx.createGain();',
    'osc.type=type==="alarm"?"sawtooth":"triangle";osc.frequency.value=freq;',
    'gain.gain.setValueAtTime(0.18,ctx.currentTime+idx*0.14);gain.gain.exponentialRampToValueAtTime(0.001,ctx.currentTime+(idx+1)*0.14);',
    'osc.connect(gain);gain.connect(ctx.destination);osc.start(ctx.currentTime+idx*0.14);osc.stop(ctx.currentTime+(idx+1)*0.14);});}catch(e){}}',
    'function speakText(txt){if(!audioEnabled||!txt)return;lastVoiceText=txt;try{window.speechSynthesis.cancel();',
    'const u=new SpeechSynthesisUtterance(txt);u.lang="zh-CN";u.rate=1.08;window.speechSynthesis.speak(u);}catch(e){}}',
    'function replayVoice(){if(lastVoiceText)speakText(lastVoiceText);}',
    'function triggerPopupAlert(a){if(!audioEnabled)return;document.getElementById("m-title").innerText=a.title;',
    'document.getElementById("m-desc").innerText=a.desc;document.getElementById("modal-card").style.borderColor=a.level==="rocket"?"#f472b6":(a.level==="danger"?"#ef4444":"#eab308");',
    'document.getElementById("modal-overlay").style.display="flex";playTone(a.sound||"rocket");speakText(a.voice||a.title);}',
    'function closeModal(){document.getElementById("modal-overlay").style.display="none";}',
    'function openQr(){document.getElementById("qr-overlay").style.display="flex";',
    'const target=pubUrl.startsWith("http")?pubUrl:window.location.href;',
    'document.getElementById("qr-img").src=`https://api.qrserver.com/v1/create-qr-code/?size=200x200&data=${encodeURIComponent(target)}`;',
    'document.getElementById("pub-url-txt").innerText=`🌐 手机公网网址: ${target}`;',
    'document.getElementById("lan-url-txt").innerText=`🏠 同WiFi局域网: ${lanUrl}`;}',
    'function closeQr(){document.getElementById("qr-overlay").style.display="none";}',
    'function toggleAudio(){audioEnabled=!audioEnabled;document.getElementById("audio-btn").innerText=`🔊 语音:${audioEnabled?"开":"静"}`;if(audioEnabled)speakText("语音已开启");}',
    'function toggleCols(){document.getElementById("gex-grid").classList.toggle("force-1col");}',
    'function testRocketAlert(){const sp=lastAdvice.xsp_spot||"741.7",cw=lastAdvice.xsp_resistance||"746.0 (SPX 7460 ▲)",fl=lastAdvice.xsp_flip||"741.0",st=Math.ceil(parseFloat(sp)||742)+1;',
    'triggerPopupAlert({level:"rocket",sound:"rocket",title:`🚀【小火箭逼空点火信号】突破 XSP ${sp}`,',
    'desc:`多头冲破翻转线(${fl})与负Gamma加速区，做市商被迫追买期货助涨！\\n\\n👉 0DTE指令：顺势关注 XSP ${st} Call，第一止盈看 ${cw}！\\n👉 周五760C指令：冲高进入 ${cw} 区间先减仓 30%~50% 锁利！`,',
    'voice:`小火箭逼空点火信号！指数冲破负Gamma加速带，短线可顺势关注 XSP ${st} Call，第一止盈看 ${cw.split(" ")[0]}`});}',
    'const TIP_DICT={',
    'KING_POS:{t:"🟨 亮黄最大正GEX铁墙 (+GEX*)",d:"做市商严格执行【逢低买、逢高卖】对冲。",a:"👉 XSP操作：跌到下方黄墙严禁追Put；涨到上方黄墙多单分批止盈！"},',
    'MID_POS:{t:"🟩 青绿次级正GEX缓冲区",d:"波动率被压缩，价格经过此处易减速停顿。",a:"👉 XSP操作：可作为顺势单第一减仓位或回踩观察位。"},',
    'KING_NEG:{t:"🟪 深紫最大负GEX核爆区 (-GEX*)",d:"做市商被迫【越跌越卖、越涨越买】顺势助涨助跌！",a:"👉 XSP操作：向下跌穿买Put；向上冲破追Call吃小火箭逼空段！"},',
    'CROWN:{t:"👑 皇冠标记 (全场最大敞口)",d:"该品种单档Gamma绝对值最大的主战场。",a:"👉 XSP操作：黄皇冠高抛低吸；紫皇冠突破顺势追单。"},',
    'UP_WALL:{t:"▲ 橙色上三角 (上方第一阻力轨)",d:"现价上方正Gamma最强的多头目标天花板。",a:"👉 XSP操作：XSP Call 第一止盈减仓位！反抽触及先平30%~50%。"},',
    'DN_TRIG:{t:"▼ 紫色下三角 (破位瀑布红线)",d:"下方负Gamma核爆临界点，失守将转为顺势砸盘。",a:"👉 XSP操作：XSP Call 绝对硬止损线！实体跌穿无条件止损。"},',
    'FLIP:{t:"◆ 蓝紫菱形 (多空翻转点)",d:"正Gamma震荡与负Gamma单边的分界线。",a:"👉 XSP操作：站稳◆上方回调接Call；压在◆下方反弹皆为反抽！"},',
    'SPOT_M:{t:"M 现价锚定格",d:"当前价格所在的贴身肉搏行权价。",a:"👉 XSP操作：日内买卖期权只选 M 上下 2~4 点以内的合约。"},',
    'PW_CARD:{t:"🛡️ 下方支撑墙",d:"现价下方承接力最强的正GEX铁底。",a:"👉 XSP操作：回踩企稳不破时是平Put或博反抽买点。"}',
    '};',
    'function showTip(e,key){const box=document.getElementById("floating-tip"),it=TIP_DICT[key];if(!it)return;',
    'box.innerHTML=`<div class="tip-title">${it.t}</div><div>${it.d}</div><div class="tip-act">${it.a}</div>`;box.style.display="block";moveTip(e);}',
    'function showRowTip(e,tk,strike,xspEq,gexStr,cls,posRel,badges){const box=document.getElementById("floating-tip"),isPos=cls.includes("pos"),isKing=cls.includes("king");',
    'let title=`${tk} ${strike} (折合 XSP ${xspEq}) | ${gexStr}`;',
    'let desc=isPos?(isKing?"🟨 亮黄最大正GEX铁墙：阻力与支撑极强。":"🟩 正GEX缓冲带：抑制波动。"):(isKing?"🟪 深紫最大负GEX核爆区：突破或跌破时波动剧烈放大！":"🟪 负GEX加速带：易加速滑动。");',
    'let act=posRel==="above"?(isPos?`👉 在现价【上方】：属反弹阻力墙，涨到 XSP ${xspEq} 附近分批止盈！`:`👉 在现价【上方】：属负GEX空头回补带，冲破 XSP ${xspEq} 将触发🚀小火箭逼空！`):(posRel==="below"?(isPos?`👉 在现价【下方】：属正GEX护盘铁底，回踩 XSP ${xspEq} 严禁追Put！`:`👉 在现价【下方】：属负GEX助跌雷区，跌穿 XSP ${xspEq} 警惕瀑布杀跌！`):`👉 当前现价 M 肉搏区：严格带好止损！`);',
    'box.innerHTML=`<div class="tip-title">${title}</div><div>${desc}</div><div class="tip-act">${act}</div>`;box.style.display="block";moveTip(e);}',
    'function moveTip(e){if(!e||window.innerWidth<=920)return;const box=document.getElementById("floating-tip");let x=e.clientX+14,y=e.clientY+14;',
    'if(x+350>window.innerWidth)x=e.clientX-355;if(y+200>window.innerHeight)y=e.clientY-190;box.style.left=x+"px";box.style.top=y+"px";}',
    'function hideTip(){document.getElementById("floating-tip").style.display="none";}',
    'function toggleDrawer(){const d=document.getElementById("guide-drawer");d.style.display=d.style.display==="block"?"none":"block";}',
    'async function loadData(){const res=await fetch("/api/gex"),d=await res.json();',
    'pubUrl=d.public_url||"";lanUrl=d.lan_url||"";',
    'document.getElementById("skew-btn").innerText=`⚖️ 去噪:${d.skew_adj?"开":"关"}`;',
    'document.getElementById("meta-info").innerText=`🌐 ${pubUrl} | 刷新:${d.last_update} | 基准:${d.baseline_time}`;',
    'const alerts=d.alerts||[];',
    'document.getElementById("alerts-container").innerHTML=alerts.map(a=>`<div class="alert-box alert-${a.level}" onclick=\'triggerPopupAlert(${JSON.stringify(a)})\'><b>${a.title} (点击重播)</b><br>${a.desc}</div>`).join("");',
    'alerts.forEach(a=>{if(!seenAlerts.has(a.id)){seenAlerts.add(a.id);triggerPopupAlert(a);}});',
    'if(d.xsp_advice&&d.xsp_advice.xsp_spot){lastAdvice=d.xsp_advice;document.getElementById("cmd-panel").style.display="grid";',
    'document.getElementById("v-spot").innerText=`${d.xsp_advice.xsp_spot} (SPX ${d.xsp_advice.spx_spot})`;',
    'document.getElementById("v-flip").innerText=d.xsp_advice.xsp_flip;document.getElementById("v-cw").innerText=d.xsp_advice.xsp_resistance;',
    'document.getElementById("v-pw").innerText=d.xsp_advice.xsp_support;document.getElementById("v-pt").innerText=d.xsp_advice.xsp_breakdown;',
    'document.getElementById("v-regime").innerText=d.xsp_advice.regime;document.getElementById("v-0dte").innerText=d.xsp_advice.action_0dte;',
    'document.getElementById("v-weekly").innerText=d.xsp_advice.action_weekly;}',
    'const order=["SPX","XSP","SPY","QQQ"];document.getElementById("gex-grid").innerHTML=order.map(tk=>{const it=d.current_data[tk];',
    'if(!it)return`<div class="col-card"><div class="col-hdr">${tk} 同步中...</div></div>`;',
    'const rHtml=it.rows.map(r=>{const bStr=(r.badges||[]).join("");',
    'return`<tr class="${r.color_cls} ${r.is_spot?"spot-row":""}" onclick="event.stopPropagation();showRowTip(event,\'${tk}\',\'${r.strike}\',\'${r.xsp_eq}\',\'${r.gex_str}\',\'${r.color_cls}\',\'${r.pos_rel}\',\'${bStr}\')" onmousemove="showRowTip(event,\'${tk}\',\'${r.strike}\',\'${r.xsp_eq}\',\'${r.gex_str}\',\'${r.color_cls}\',\'${r.pos_rel}\',\'${bStr}\')"><td class="strike-col">${r.strike}<span style="font-size:10px;margin-left:2px;">${bStr}</span></td><td class="gex-col">${r.gex_str}</td></tr>`;}).join("");',
    'return`<div class="col-card"><div class="col-hdr"><div class="col-title"><span>${tk} (${it.exp})</span><span>$${it.spot.toFixed(1)}</span></div>',
    '<div class="pills"><span class="pill pill-fl" onclick="event.stopPropagation();showTip(event,\'FLIP\')">◆${it.flip_strike}</span>',
    '<span class="pill pill-up" onclick="event.stopPropagation();showTip(event,\'UP_WALL\')">▲${it.call_wall}</span>',
    '<span class="pill pill-dn" onclick="event.stopPropagation();showTip(event,\'DN_TRIG\')">▼${it.put_trigger}</span></div></div>',
    '<table class="gex-tbl"><tbody>${rHtml}</tbody></table></div>`;}).join("");}',
    'async function toggleMode(){await fetch("/api/toggle_mode",{method:"POST"});loadData();}',
    'async function toggleSkew(){await fetch("/api/toggle_skew",{method:"POST"});loadData();}',
    'loadData();setInterval(loadData,15000);</script></body></html>'
]
HTML_PAGE = "\n".join(HTML_LINES)

@app.route("/")
def index():
    return Response(HTML_PAGE, mimetype="text/html")

@app.route("/api/gex")
def api_gex():
    return jsonify(STATE)

@app.route("/api/ping")
def api_ping():
    return "ok", 200

@app.route("/api/toggle_mode", methods=["POST"])
def api_toggle_mode():
    STATE["mode"] = "weekly" if STATE["mode"] == "next" else "next"
    ROLLING_HISTORY.clear()
    STATE["baseline_data"] = {}
    refresh_all_now()
    return jsonify({"status": "ok", "mode": STATE["mode"]})

@app.route("/api/toggle_skew", methods=["POST"])
def api_toggle_skew():
    STATE["skew_adj"] = not STATE["skew_adj"]
    ROLLING_HISTORY.clear()
    STATE["baseline_data"] = {}
    refresh_all_now()
    return jsonify({"status": "ok", "skew_adj": STATE["skew_adj"]})

if __name__ == "__main__":
    generate_cloud_24h_package()
    t1 = threading.Thread(target=background_updater, daemon=True)
    t1.start()
    t2 = threading.Thread(target=start_public_tunnel, daemon=True)
    t2.start()
    port = int(os.environ.get("PORT", 5050))
    print(f"✅ iPhone双列+免Docker云端版已启动！本机请访问: http://127.0.0.1:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
# === END ===