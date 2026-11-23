import os
import time
import math
import json
import hmac
import hashlib
import threading
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify, Response, render_template_string

app = Flask(__name__)

STATE_FILE = "vaultskimmer_v4_2_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

BULL_UNIVERSE = [
    "SUI_USDT", "PEPE_USDT", "ARB_USDT", "FET_USDT", "DOGE_USDT",
    "SOL_USDT", "XRP_USDT", "ADA_USDT", "NEAR_USDT", "AVAX_USDT",
    "LINK_USDT", "DOT_USDT", "OP_USDT", "WIF_USDT", "RENDER_USDT", "BNB_USDT"
]

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=24,
    pool_maxsize=24,
    max_retries=Retry(total=2, backoff_factor=0.15, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)

PAIR_META_CACHE = {}
BULL_RADAR_CACHE = {}


def fmt_px(val):
    v = float(val or 0.0)
    if v == 0:
        return "$0.0000"
    if v < 0.0001:
        return f"${v:.8f}"
    if v < 0.01:
        return f"${v:.6f}"
    return f"${v:.4f}"


def floor_prec(val, prec):
    factor = 10 ** int(max(0, prec))
    return math.floor(float(val) * factor) / factor


DEFAULT_STATE = {
    "mode": "PAPER",
    "slot_size_usdt": 10.0,
    "escape_dump_pct": 3.5,         # Step 1: If coin drops -3.5%, sells into USDT to prevent -15% stuck bags
    "reentry_bounce_pct": 1.0,      # Step 2: Tracks bottom; on +1.0% bounce, tops up from Reserve -> Full $10 Re-Buy!
    "recovery_pump_pct": 2.20,      # Step 3: Waits for +2.20% net recovery pump -> Refunds Reserve + Locks Excess!
    "skim_profit_pct": 1.00,        # Normal Spot Skim Target (+1.00% net / +1.40% pump with 0.40% Spot fee)
    "trail_pullback_pct": 0.22,     # Trailing Peak Rider: sells if price pulls back -0.22% off peak
    "spot_fee_buffer_pct": 0.40,    # Gate.io Spot round-trip taker fee (0.20% buy + 0.20% sell)
    "live_starting_deposit": 20.00, # Anchors True Net Profit to your real Gate.io Spot deposit
    "api_key": os.environ.get("GATE_API_KEY", "").strip(),
    "api_secret": os.environ.get("GATE_API_SECRET", "").strip(),
    "api_connected": False,
    "api_status_msg": "Not Connected — Paste Gate.io Spot Keys Below for Live Mode",
    "live_spot_usdt": 0.0,
    "paper_usdt_balance": 100.0,
    "paper_starting_bankroll": 100.0,
    "spot_positions": {},
    "radar": [],
    "stats": {
        "paper_skims": 0,
        "paper_profit_usdt": 0.0,
        "live_skims": 0,
        "live_profit_usdt": 0.0
    },
    "skim_ledger": [],
    "logs": [],
    "cycle_count": 0,
    "last_scan_ms": 0,
    "last_update": "--:--:--",
    "last_worker_heartbeat": 0.0
}

state_lock = threading.Lock()
cycle_lock = threading.Lock()
worker_started = False
STATE = json.loads(json.dumps(DEFAULT_STATE))


def add_log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    STATE["logs"].insert(0, f"[{ts}] {msg}")
    STATE["logs"] = STATE["logs"][:120]


def load_state():
    global STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                sk = (saved.get("api_key") or "").strip()
                ss = (saved.get("api_secret") or "").strip()
                STATE.update(saved)
                if sk:
                    STATE["api_key"] = sk
                if ss:
                    STATE["api_secret"] = ss
                STATE.setdefault("escape_dump_pct", 3.5)
                STATE.setdefault("reentry_bounce_pct", 1.0)
                STATE.setdefault("recovery_pump_pct", 2.20)
                STATE.setdefault("skim_profit_pct", 1.00)
                STATE.setdefault("live_starting_deposit", 20.00)
                for p in STATE.get("spot_positions", {}).values():
                    p.setdefault("phase", "ACTIVE_LONG")
                    p.setdefault("in_recovery", False)
                    p.setdefault("borrowed_from_reserve_usdt", 0.0)
                    p.setdefault("bottom_watch_price", float(p.get("last_price", 0.0)))
        except Exception as e:
            add_log(f"State load notice: {e}")

    ek = os.environ.get("GATE_API_KEY", "").strip()
    es = os.environ.get("GATE_API_SECRET", "").strip()
    if ek:
        STATE["api_key"] = ek
    if es:
        STATE["api_secret"] = es


def save_state():
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(STATE, f, indent=2)
        os.replace(tmp, STATE_FILE)
    except Exception:
        pass


def gate_sign(method, url, query_string="", body_string="", custom_key=None, custom_secret=None):
    key = (custom_key if custom_key else STATE.get("api_key", "")).strip()
    secret = (custom_secret if custom_secret else STATE.get("api_secret", "")).strip()
    t = str(int(time.time()))
    hashed_payload = hashlib.sha512(body_string.encode("utf-8")).hexdigest()
    s = f"{method}\n{url}\n{query_string}\n{hashed_payload}\n{t}"
    sign = hmac.new(secret.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": key,
        "Timestamp": t,
        "SIGN": sign
    }


def fetch_spot_tickers(pairs):
    r = HTTP_SESSION.get(f"{GATE_HOST}{GATE_PREFIX}/spot/tickers", timeout=6)
    r.raise_for_status()
    wanted = set(pairs)
    out = {}
    for t in r.json():
        cp = t.get("currency_pair")
        if cp in wanted:
            last = float(t.get("last") or 0.0)
            if last > 0:
                out[cp] = {
                    "last": last,
                    "change_24h": float(t.get("change_percentage") or 0.0)
                }
    return out


def get_pair_meta(pair):
    now = time.time()
    cached = PAIR_META_CACHE.get(pair)
    if not cached or (now - cached["ts"] > 3600):
        r = HTTP_SESSION.get(f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs/{pair}", timeout=5)
        if r.status_code == 200:
            info = r.json()
            cached = {
                "amount_precision": int(info.get("amount_precision") or 2),
                "min_quote_amount": float(info.get("min_quote_amount") or 1.0),
                "ts": now
            }
        else:
            cached = {"amount_precision": 2, "min_quote_amount": 1.0, "ts": now}
        PAIR_META_CACHE[pair] = cached
    return cached


def fetch_spot_accounts(custom_key=None, custom_secret=None):
    url = f"{GATE_PREFIX}/spot/accounts"
    headers = gate_sign("GET", url, "", "", custom_key, custom_secret)
    r = HTTP_SESSION.get(f"{GATE_HOST}{url}", headers=headers, timeout=5)
    if r.status_code != 200:
        raise ValueError(f"Gate.io Spot HTTP {r.status_code}: {r.text[:80]}")
    balances = {}
    for item in r.json():
        cur = item.get("currency", "").upper()
        avail = float(item.get("available") or 0.0)
        if avail > 0:
            balances[cur] = avail
    return balances


def fetch_spot_usdt_balance(custom_key=None, custom_secret=None):
    bals = fetch_spot_accounts(custom_key, custom_secret)
    return float(bals.get("USDT", 0.0))


def place_spot_market_buy(pair, usdt_amount):
    """Gate.io v4 Spot Market Buy: amount is in QUOTE currency (USDT)."""
    url = f"{GATE_PREFIX}/spot/orders"
    body = {
        "currency_pair": pair,
        "account": "spot",
        "type": "market",
        "side": "buy",
        "amount": f"{round(float(usdt_amount), 2):.2f}",
        "time_in_force": "ioc"
    }
    body_str = json.dumps(body)
    headers = gate_sign("POST", url, "", body_str)
    r = HTTP_SESSION.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=7)
    return r.status_code, r.json()


def place_spot_market_sell(pair, coin_qty):
    """Gate.io v4 Spot Market Sell: amount is in BASE currency (coin quantity)."""
    meta = get_pair_meta(pair)
    prec = meta["amount_precision"]
    base_coin = pair.split("_")[0]

    try:
        bals = fetch_spot_accounts()
        avail_coin = float(bals.get(base_coin, 0.0))
        sell_qty = min(float(coin_qty), avail_coin * 0.9995) if avail_coin > 0 else float(coin_qty)
    except Exception:
        sell_qty = float(coin_qty)

    clean_qty = floor_prec(sell_qty, prec)
    if clean_qty <= 0:
        return 400, {"message": f"Zero sellable balance for {base_coin}"}

    qty_str = f"{clean_qty:.{prec}f}"
    url = f"{GATE_PREFIX}/spot/orders"
    body = {
        "currency_pair": pair,
        "account": "spot",
        "type": "market",
        "side": "sell",
        "amount": qty_str,
        "time_in_force": "ioc"
    }
    body_str = json.dumps(body)
    headers = gate_sign("POST", url, "", body_str)
    r = HTTP_SESSION.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=7)
    return r.status_code, r.json()


def calc_rsi(closes, period=14):
    if len(closes) < period + 1:
        return 50.0
    gains, losses = [], []
    for i in range(1, len(closes)):
        d = closes[i] - closes[i - 1]
        gains.append(max(0.0, d))
        losses.append(max(0.0, -d))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
    if avg_l == 0:
        return 100.0
    rs = avg_g / avg_l
    return 100.0 - (100.0 / (1.0 + rs))


def compute_bull_score(pair, tk):
    now = time.time()
    cached = BULL_RADAR_CACHE.get(pair)
    if cached and (now - cached["ts"] < 14.0):
        cached["price"] = tk["last"]
        return cached

    price = tk["last"]
    rsi_14 = 50.0
    bid_buy_pct = 50.0
    lower_wick = False

    try:
        c_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/spot/candlesticks",
            params={"currency_pair": pair, "interval": "5m", "limit": 25},
            timeout=3.5
        )
        if c_resp.status_code == 200:
            raw = c_resp.json()
            closes = [float(c[2]) for c in raw if len(c) >= 6]
            highs = [float(c[3]) for c in raw if len(c) >= 6]
            lows = [float(c[4]) for c in raw if len(c) >= 6]
            if len(closes) >= 16:
                closes[-1] = price
                rsi_14 = calc_rsi(closes, 14)
                c_rng = max(1e-12, highs[-1] - lows[-1])
                lower_wick = ((price - lows[-1]) / c_rng) >= 0.35
    except Exception:
        pass

    try:
        ob_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/spot/order_book",
            params={"currency_pair": pair, "limit": 15},
            timeout=3.0
        )
        if ob_resp.status_code == 200:
            ob = ob_resp.json()
            bids = sum(float(b[0]) * float(b[1]) for b in ob.get("bids", []))
            asks = sum(float(a[0]) * float(a[1]) for a in ob.get("asks", []))
            if (bids + asks) > 0:
                bid_buy_pct = (bids / (bids + asks)) * 100.0
    except Exception:
        pass

    if rsi_14 > 72:
        s_rsi = 5.0
    elif 28 <= rsi_14 <= 58:
        s_rsi = 38.0
    else:
        s_rsi = 25.0

    s_ob = max(4.0, min(35.0, ((bid_buy_pct - 35.0) / 35.0) * 35.0))
    s_bounce = (15.0 if lower_wick else 6.0) + (12.0 if tk["change_24h"] > 0 else 5.0)
    bull_score = int(round(max(1.0, min(99.0, s_rsi + s_ob + s_bounce))))

    if bull_score >= 74:
        sig = "SNIPE BULL BOUNCE"
    elif bull_score >= 66:
        sig = "BULLISH DEMAND"
    elif rsi_14 > 72:
        sig = "OVERBOUGHT — WAIT"
    else:
        sig = "NEUTRAL"

    res = {
        "pair": pair,
        "price": price,
        "change_24h": round(tk["change_24h"], 2),
        "bull_score": bull_score,
        "rsi_14": round(rsi_14, 1),
        "bid_buy_pct": round(bid_buy_pct, 1),
        "signal": sig,
        "ts": now
    }
    BULL_RADAR_CACHE[pair] = res
    return res


def run_bull_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            mode = STATE.get("mode", "PAPER")
            escape_dump_pct = float(STATE.get("escape_dump_pct", 3.5))
            reentry_bounce_pct = float(STATE.get("reentry_bounce_pct", 1.0))
            recovery_pump_pct = float(STATE.get("recovery_pump_pct", 2.20))
            skim_pct = float(STATE.get("skim_profit_pct", 1.00))
            trail_pullback = float(STATE.get("trail_pullback_pct", 0.22))
            fee_buf = float(STATE.get("spot_fee_buffer_pct", 0.40))
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))
            active_pairs = list(STATE["spot_positions"].keys())

        if has_keys:
            try:
                spot_usdt = fetch_spot_usdt_balance()
                with state_lock:
                    STATE["api_connected"] = True
                    STATE["live_spot_usdt"] = spot_usdt
                    STATE["api_status_msg"] = f"Gate.io Spot Connected (Available: ${spot_usdt:.2f} USDT)"
            except Exception as e:
                with state_lock:
                    STATE["api_connected"] = False
                    STATE["api_status_msg"] = f"API Notice: {str(e)[:55]}"

        all_pairs = list(set(BULL_UNIVERSE + active_pairs))
        try:
            tk_map = fetch_spot_tickers(all_pairs)
        except Exception:
            return

        radar_list = []
        for pair in BULL_UNIVERSE:
            tk = tk_map.get(pair)
            if tk:
                radar_list.append(compute_bull_score(pair, tk))
        radar_list.sort(key=lambda x: x["bull_score"], reverse=True)

        for pair in list(STATE["spot_positions"].keys()):
            with state_lock:
                pos = STATE["spot_positions"].get(pair)
            if not pos or pair not in tk_map:
                continue

            live_px = tk_map[pair]["last"]
            pos["last_price"] = live_px
            phase = pos.get("phase", "ACTIVE_LONG")
            margin_usdt = float(pos["margin_usdt"])
            base_slot = float(pos.get("base_slot_usdt") or 10.0)

            if phase == "STEPPED_OUT_HOLD":
                bottom_px = float(pos.get("bottom_watch_price") or live_px)
                if live_px < bottom_px:
                    bottom_px = live_px
                    pos["bottom_watch_price"] = bottom_px

                reentry_trigger_px = bottom_px * (1.0 + (reentry_bounce_pct / 100.0))
                pos["reentry_trigger_price"] = reentry_trigger_px
                bounce_from_bottom_pct = ((live_px - bottom_px) / bottom_px) * 100.0

                pos["coin_move_pct"] = round(bounce_from_bottom_pct, 2)
                pos["pnl_pct"] = 0.0
                pos["pnl_usdt"] = 0.0

                shortfall_needed = max(0.0, round(base_slot - margin_usdt, 4))

                if live_px >= reentry_trigger_px:
                    rec_req_pump = recovery_pump_pct + fee_buf
                    new_target_px = live_px * (1.0 + (rec_req_pump / 100.0))

                    if mode == "PAPER":
                        with state_lock:
                            avail_res = float(STATE.get("paper_usdt_balance", 0.0))
                            patch_from_res = min(avail_res, shortfall_needed)
                            restored_margin = round(margin_usdt + patch_from_res, 4)
                            STATE["paper_usdt_balance"] = round(avail_res - patch_from_res, 4)

                            pos["phase"] = "ACTIVE_LONG"
                            pos["in_recovery"] = True
                            pos["margin_usdt"] = restored_margin
                            pos["coin_qty"] = restored_margin / live_px
                            pos["current_equity_usdt"] = restored_margin
                            pos["borrowed_from_reserve_usdt"] = round(
                                float(pos.get("borrowed_from_reserve_usdt", 0.0)) + patch_from_res, 4
                            )
                            pos["entry_price"] = live_px
                            pos["target_sell_price"] = new_target_px
                            pos["escape_stop_price"] = live_px * (1.0 - (escape_dump_pct / 100.0))
                            pos["trailing_active"] = False
                            pos["highest_price"] = live_px
                            pos["status"] = (
                                f"🎯 Restored to ${restored_margin:.2f} (+${patch_from_res:.2f} from Reserve) @ {fmt_px(live_px)} | "
                                f"Target +{rec_req_pump:.2f}% ({fmt_px(new_target_px)})"
                            )
                            add_log(
                                f"🎯 [FULL ${restored_margin:.2f} RE-BUY][{pair}] Bounced +{bounce_from_bottom_pct:.2f}% off bottom! "
                                f"Topped up +${patch_from_res:.2f} (Total Debt: ${pos['borrowed_from_reserve_usdt']:.2f}) @ {fmt_px(live_px)}"
                            )
                    elif mode == "LIVE" and has_keys:
                        avail_spot = float(STATE.get("live_spot_usdt", 0.0))
                        patch_from_res = min(avail_spot, shortfall_needed)
                        buy_usdt = round(min(avail_spot, margin_usdt + patch_from_res), 2)
                        if buy_usdt >= 1.0:
                            code, resp = place_spot_market_buy(pair, buy_usdt)
                            if code in (200, 201):
                                filled_qty = float(resp.get("filled_amount") or (buy_usdt / live_px))
                                avg_px = float(resp.get("avg_deal_price") or live_px)
                                if avg_px <= 0:
                                    avg_px = live_px
                                new_target_px = avg_px * (1.0 + (rec_req_pump / 100.0))
                                with state_lock:
                                    STATE["live_spot_usdt"] = max(0.0, avail_spot - buy_usdt)
                                    pos["phase"] = "ACTIVE_LONG"
                                    pos["in_recovery"] = True
                                    pos["coin_qty"] = filled_qty
                                    pos["margin_usdt"] = buy_usdt
                                    pos["current_equity_usdt"] = buy_usdt
                                    pos["borrowed_from_reserve_usdt"] = round(
                                        float(pos.get("borrowed_from_reserve_usdt", 0.0)) + patch_from_res, 4
                                    )
                                    pos["entry_price"] = avg_px
                                    pos["target_sell_price"] = new_target_px
                                    pos["escape_stop_price"] = avg_px * (1.0 - (escape_dump_pct / 100.0))
                                    pos["trailing_active"] = False
                                    pos["highest_price"] = avg_px
                                    pos["status"] = f"🎯 Live Re-Bought (${buy_usdt:.2f}) @ {fmt_px(avg_px)} | Target +{rec_req_pump:.2f}%"
                                    add_log(
                                        f"🎯 [LIVE SPOT RE-BUY][{pair}] Bounced +{bounce_from_bottom_pct:.2f}% off bottom -> "
                                        f"Bought ${buy_usdt:.2f} @ {fmt_px(avg_px)} (Debt: ${pos['borrowed_from_reserve_usdt']:.2f})"
                                    )
                else:
                    need_bounce = max(0.0, reentry_bounce_pct - bounce_from_bottom_pct)
                    pos["status"] = (
                        f"🛡️ Holding ${margin_usdt:.2f} USDT (Adds +${shortfall_needed:.2f} -> ${base_slot:.2f}) | "
                        f"Bottom: {fmt_px(bottom_px)} | Re-Buys on +{need_bounce:.2f}% bounce @ {fmt_px(reentry_trigger_px)}"
                    )
                continue

            entry_px = float(pos["entry_price"])
            borrowed_now = float(pos.get("borrowed_from_reserve_usdt", 0.0))
            in_rec = bool(pos.get("in_recovery", False)) or (borrowed_now > 0.01)
            pos["in_recovery"] = in_rec

            active_skim_goal_pct = recovery_pump_pct if in_rec else skim_pct
            required_pump_pct = active_skim_goal_pct + fee_buf

            target_sell_px = entry_px * (1.0 + (required_pump_pct / 100.0))
            escape_stop_px = entry_px * (1.0 - (escape_dump_pct / 100.0))
            pos["target_sell_price"] = target_sell_px
            pos["escape_stop_price"] = escape_stop_px

            coin_move_pct = ((live_px - entry_px) / entry_px) * 100.0
            net_pnl_pct = coin_move_pct - fee_buf
            net_pnl_usdt = margin_usdt * (net_pnl_pct / 100.0)

            pos["coin_move_pct"] = round(coin_move_pct, 2)
            pos["pnl_pct"] = round(net_pnl_pct, 2)
            pos["pnl_usdt"] = round(net_pnl_usdt, 4)
            pos["current_equity_usdt"] = round(margin_usdt + net_pnl_usdt, 4)

            if coin_move_pct <= -escape_dump_pct:
                held_cash = round(margin_usdt * (1.0 - ((escape_dump_pct + (fee_buf / 2.0)) / 100.0)), 4)
                reentry_px = live_px * (1.0 + (reentry_bounce_pct / 100.0))
                shortfall = max(0.0, round(base_slot - held_cash, 4))

                if mode == "PAPER":
                    with state_lock:
                        pos["phase"] = "STEPPED_OUT_HOLD"
                        pos["margin_usdt"] = held_cash
                        pos["current_equity_usdt"] = held_cash
                        pos["bottom_watch_price"] = live_px
                        pos["reentry_trigger_price"] = reentry_px
                        pos["step_outs_count"] = int(pos.get("step_outs_count", 0)) + 1
                        pos["pnl_pct"] = 0.0
                        pos["pnl_usdt"] = 0.0
                        pos["status"] = (
                            f"🛡️ Stepped Out @ {fmt_px(live_px)} (${held_cash:.2f} held) | "
                            f"Waits for +{reentry_bounce_pct}% bounce, adds +${shortfall:.2f} -> ${base_slot:.2f}"
                        )
                        add_log(
                            f"🛡️ [-{escape_dump_pct}% DUMP SHIELD][{pair}] Dropped {coin_move_pct:.2f}% to {fmt_px(live_px)}! "
                            f"Sold into USDT (${held_cash:.2f}) — Waiting for +{reentry_bounce_pct}% bottom bounce."
                        )
                    continue
                elif mode == "LIVE" and has_keys:
                    code, resp = place_spot_market_sell(pair, float(pos.get("coin_qty", 0.0)))
                    if code in (200, 201):
                        real_usdt = float(resp.get("filled_total") or held_cash)
                        with state_lock:
                            STATE["live_spot_usdt"] = round(STATE.get("live_spot_usdt", 0.0) + real_usdt, 4)
                            pos["phase"] = "STEPPED_OUT_HOLD"
                            pos["coin_qty"] = 0.0
                            pos["margin_usdt"] = round(real_usdt, 4)
                            pos["current_equity_usdt"] = round(real_usdt, 4)
                            pos["bottom_watch_price"] = live_px
                            pos["reentry_trigger_price"] = reentry_px
                            pos["step_outs_count"] = int(pos.get("step_outs_count", 0)) + 1
                            pos["pnl_pct"] = 0.0
                            pos["pnl_usdt"] = 0.0
                            pos["status"] = f"🛡️ Live Stepped Out @ {fmt_px(live_px)} | Re-Buys full ${base_slot:.2f} on +{reentry_bounce_pct}% bounce"
                            add_log(
                                f"🛡️ [LIVE -{escape_dump_pct}% DUMP SHIELD][{pair}] Sold Spot @ {fmt_px(live_px)} (${real_usdt:.2f} USDT safe)!"
                            )
                        continue

            should_skim_and_rebuy = False
            if live_px >= target_sell_px:
                if not pos.get("trailing_active"):
                    pos["trailing_active"] = True
                    pos["highest_price"] = live_px
                    pos["status"] = f"🚀 +{required_pump_pct:.2f}% Target Hit! Trailing Peak Rider Armed (+{net_pnl_pct:.2f}%)"
                else:
                    if live_px > float(pos.get("highest_price", live_px)):
                        pos["highest_price"] = live_px
                    high_watermark = float(pos["highest_price"])
                    pullback_floor_px = max(target_sell_px, high_watermark * (1.0 - (trail_pullback / 100.0)))
                    if live_px <= pullback_floor_px:
                        should_skim_and_rebuy = True
                    else:
                        pos["status"] = f"🟢 Riding Green Pump (+{net_pnl_pct:.2f}%) | Lock Floor @ {fmt_px(pullback_floor_px)}"
            else:
                if pos.get("trailing_active") and net_pnl_pct >= active_skim_goal_pct:
                    should_skim_and_rebuy = True
                else:
                    pos["trailing_active"] = False
                    need_pump = max(0.0, ((target_sell_px - live_px) / live_px) * 100.0)
                    mode_tag = f"+{recovery_pump_pct}% Recovery (Debt: ${borrowed_now:.2f})" if in_rec else f"+{skim_pct}% Skim"
                    pos["status"] = (
                        f"Need +{need_pump:.2f}% pump for {mode_tag} | "
                        f"-{escape_dump_pct}% Shield @ {fmt_px(escape_stop_px)}"
                    )

            if should_skim_and_rebuy:
                gross_gain_usdt = max(0.02, round(margin_usdt * (max(active_skim_goal_pct, net_pnl_pct) / 100.0), 4))
                borrowed_res = float(pos.get("borrowed_from_reserve_usdt", 0.0))

                if borrowed_res > 0.0:
                    reserve_refund = round(min(gross_gain_usdt, borrowed_res), 4)
                    remaining_debt = max(0.0, round(borrowed_res - reserve_refund, 4))
                    pure_vault_profit = max(0.0, round(gross_gain_usdt - reserve_refund, 4))
                else:
                    reserve_refund = 0.0
                    remaining_debt = 0.0
                    pure_vault_profit = gross_gain_usdt

                still_in_recovery = remaining_debt > 0.01
                next_goal = recovery_pump_pct if still_in_recovery else skim_pct
                new_target_px = live_px * (1.0 + ((next_goal + fee_buf) / 100.0))

                if mode == "PAPER":
                    with state_lock:
                        if pair not in STATE["spot_positions"]:
                            continue
                        if reserve_refund > 0:
                            STATE["paper_usdt_balance"] = round(
                                STATE.get("paper_usdt_balance", 0.0) + reserve_refund, 4
                            )
                        STATE["stats"]["paper_skims"] += 1
                        STATE["stats"]["paper_profit_usdt"] = round(
                            STATE["stats"]["paper_profit_usdt"] + pure_vault_profit, 4
                        )
                        pos["slot_skims"] = int(pos.get("slot_skims", 0)) + 1
                        pos["slot_profit_usdt"] = round(float(pos.get("slot_profit_usdt", 0.0)) + pure_vault_profit, 4)

                        profit_tag = f"+${pure_vault_profit:.4f}" if pure_vault_profit > 0 else f"Refunded ${reserve_refund:.2f}"
                        STATE["skim_ledger"].insert(0, {
                            "time": datetime.utcnow().strftime("%H:%M:%S"),
                            "pair": f"{pair} (DEMO #{pos['slot_skims']})",
                            "pump": f"+{coin_move_pct:.2f}%",
                            "buy_to_sell": f"{fmt_px(entry_px)} → {fmt_px(live_px)}",
                            "profit": profit_tag
                        })
                        add_log(
                            f"🔒💎 [BULL SKIM WON][{pair}] Pumped +{coin_move_pct:.2f}% | "
                            f"Reserve Refund: +${reserve_refund:.2f} (Debt Left: ${remaining_debt:.2f}) | "
                            f"Net Vault Profit: +${pure_vault_profit:.4f}"
                        )

                        pos["phase"] = "ACTIVE_LONG"
                        pos["in_recovery"] = still_in_recovery
                        pos["borrowed_from_reserve_usdt"] = remaining_debt
                        pos["margin_usdt"] = base_slot
                        pos["coin_qty"] = base_slot / live_px
                        pos["current_equity_usdt"] = base_slot
                        pos["entry_price"] = live_px
                        pos["target_sell_price"] = new_target_px
                        pos["escape_stop_price"] = live_px * (1.0 - (escape_dump_pct / 100.0))
                        pos["coin_move_pct"] = 0.0
                        pos["pnl_pct"] = -fee_buf
                        pos["pnl_usdt"] = round(-base_slot * (fee_buf / 100.0), 4)
                        pos["trailing_active"] = False
                        pos["highest_price"] = live_px
                        pos["status"] = f"Cycle #{pos['slot_skims']} Done — Re-Bought ${base_slot:.2f} @ {fmt_px(live_px)}"

                elif mode == "LIVE" and has_keys:
                    code_s, resp_s = place_spot_market_sell(pair, float(pos.get("coin_qty", 0.0)))
                    if code_s in (200, 201):
                        code_b, resp_b = place_spot_market_buy(pair, base_slot)
                        new_qty = float(resp_b.get("filled_amount") or (base_slot / live_px)) if code_b in (200, 201) else 0.0
                        new_entry = float(resp_b.get("avg_deal_price") or live_px) if code_b in (200, 201) else live_px
                        if new_entry <= 0:
                            new_entry = live_px
                        new_target_px = new_entry * (1.0 + ((next_goal + fee_buf) / 100.0))
                        with state_lock:
                            STATE["stats"]["live_skims"] += 1
                            STATE["stats"]["live_profit_usdt"] = round(
                                STATE["stats"]["live_profit_usdt"] + pure_vault_profit, 4
                            )
                            pos["slot_skims"] = int(pos.get("slot_skims", 0)) + 1
                            pos["slot_profit_usdt"] = round(float(pos.get("slot_profit_usdt", 0.0)) + pure_vault_profit, 4)
                            profit_tag = f"+${pure_vault_profit:.4f}" if pure_vault_profit > 0 else f"Refunded ${reserve_refund:.2f}"
                            STATE["skim_ledger"].insert(0, {
                                "time": datetime.utcnow().strftime("%H:%M:%S"),
                                "pair": f"{pair} (LIVE #{pos['slot_skims']})",
                                "pump": f"+{coin_move_pct:.2f}%",
                                "buy_to_sell": f"{fmt_px(entry_px)} → {fmt_px(live_px)}",
                                "profit": profit_tag
                            })
                            pos["phase"] = "ACTIVE_LONG"
                            pos["in_recovery"] = still_in_recovery
                            pos["borrowed_from_reserve_usdt"] = remaining_debt
                            pos["coin_qty"] = new_qty
                            pos["margin_usdt"] = base_slot
                            pos["current_equity_usdt"] = base_slot
                            pos["entry_price"] = new_entry
                            pos["target_sell_price"] = new_target_px
                            pos["escape_stop_price"] = new_entry * (1.0 - (escape_dump_pct / 100.0))
                            pos["trailing_active"] = False
                            pos["highest_price"] = new_entry
                            add_log(
                                f"🔒💎 [LIVE SPOT SKIM][{pair}] Refunded ${reserve_refund:.2f} (Debt: ${remaining_debt:.2f}) | "
                                f"Net Vault Profit: +${pure_vault_profit:.4f} | Re-Bought ${base_slot:.2f} @ {fmt_px(new_entry)}"
                            )

        with state_lock:
            STATE["radar"] = radar_list[:12]
            STATE["skim_ledger"] = STATE["skim_ledger"][:50]
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            STATE["last_worker_heartbeat"] = time.time()
            save_state()

    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("🟢 VaultSkimmer v4.2 PRO Online — Bullish Spot Skimmer + Carry-Over Recovery + -3.5% Dump Shield.")
    while True:
        run_bull_cycle(force_manual=False)
        time.sleep(3.5)


def ensure_worker_alive():
    global worker_started
    now = time.time()
    if not worker_started or (now - STATE.get("last_worker_heartbeat", 0) > 14):
        worker_started = True
        STATE["last_worker_heartbeat"] = now
        threading.Thread(target=background_worker, daemon=True).start()


@app.before_request
def check_worker():
    ensure_worker_alive()


# =============================================================================
# PWA MOBILE INSTALLER ENDPOINTS (GREEN BULLISH ICON)
# =============================================================================
@app.route("/manifest.json")
def pwa_manifest():
    return jsonify({
        "name": "VaultSkimmer v4.2 PRO",
        "short_name": "VaultSkimmer",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#040807",
        "theme_color": "#00f5a0",
        "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}]
    })


@app.route("/icon.svg")
def pwa_icon():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
      <rect width="512" height="512" rx="115" fill="#061410"/>
      <circle cx="256" cy="256" r="210" fill="none" stroke="#00f5a0" stroke-width="18"/>
      <path d="M150 325 L245 230 L300 285 L380 185" fill="none" stroke="#00f5a0" stroke-width="36" stroke-linecap="round"/>
      <polygon points="395,170 310,170 395,255" fill="#00d9f5"/>
    </svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/sw.js")
def pwa_sw():
    return Response("self.addEventListener('fetch', e => {});", mimetype="application/javascript")


# =============================================================================
# API ENDPOINTS
# =============================================================================
@app.route("/api/state")
def api_state():
    if STATE["cycle_count"] == 0:
        run_bull_cycle(force_manual=False)
    with state_lock:
        safe = STATE.copy()
        safe["api_key"] = ""
        safe["api_secret"] = ""
        return jsonify(safe)


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    run_bull_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/toggle_mode", methods=["POST"])
def api_toggle_mode():
    with state_lock:
        STATE["mode"] = "LIVE" if STATE.get("mode") == "PAPER" else "PAPER"
        add_log(f"🔀 Switched VaultSkimmer Mode to: {STATE['mode']}")
        save_state()
    run_bull_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/sync_live_positions", methods=["POST"])
def api_sync_live_positions():
    if not STATE.get("api_key") or not STATE.get("api_secret"):
        return jsonify({"ok": False, "error": "Sync your Gate.io API Keys first!"}), 400
    try:
        bals = fetch_spot_accounts()
        candidate_pairs = [f"{cur}_USDT" for cur in bals.keys() if cur not in ("USDT", "USD", "GT")]
        tk_map = fetch_spot_tickers(candidate_pairs) if candidate_pairs else {}
        imported = []
        with state_lock:
            fee_buf = float(STATE.get("spot_fee_buffer_pct", 0.40))
            req_pump = float(STATE.get("skim_profit_pct", 1.0)) + fee_buf
            esc_pct = float(STATE.get("escape_dump_pct", 3.5))
            for cur, qty in bals.items():
                if cur in ("USDT", "USD", "GT"):
                    continue
                pair = f"{cur}_USDT"
                if pair not in tk_map:
                    continue
                px = tk_map[pair]["last"]
                val = round(qty * px, 4)
                if val < 2.0:
                    continue
                STATE["spot_positions"][pair] = {
                    "pair": pair,
                    "is_live": True,
                    "phase": "ACTIVE_LONG",
                    "in_recovery": False,
                    "borrowed_from_reserve_usdt": 0.0,
                    "coin_qty": qty,
                    "base_slot_usdt": val,
                    "margin_usdt": val,
                    "current_equity_usdt": val,
                    "entry_price": px,
                    "last_price": px,
                    "target_sell_price": px * (1.0 + (req_pump / 100.0)),
                    "escape_stop_price": px * (1.0 - (esc_pct / 100.0)),
                    "bottom_watch_price": px,
                    "reentry_trigger_price": px,
                    "step_outs_count": 0,
                    "slot_skims": 0,
                    "slot_profit_usdt": 0.0,
                    "coin_move_pct": 0.0,
                    "pnl_pct": 0.0,
                    "pnl_usdt": 0.0,
                    "trailing_active": False,
                    "highest_price": px,
                    "status": f"Synced Live Spot Bag (${val:.2f})"
                }
                imported.append(pair)
            if imported:
                STATE["mode"] = "LIVE"
                add_log(f"📥 Synced {len(imported)} live Gate.io Spot bag(s): {', '.join(imported)}")
                save_state()
        run_bull_cycle(force_manual=True)
        return jsonify({"ok": True, "imported": imported})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/clear_demo_only", methods=["POST"])
def api_clear_demo_only():
    d = request.json or {}
    raw_amt = d.get("amount_usdt", STATE.get("paper_starting_bankroll", 100.0))
    try:
        new_bal = max(10.0, float(raw_amt))
    except Exception:
        new_bal = 100.0

    with state_lock:
        to_remove = [k for k, v in STATE["spot_positions"].items() if not v.get("is_live")]
        for k in to_remove:
            STATE["spot_positions"].pop(k, None)

        STATE["paper_usdt_balance"] = round(new_bal, 4)
        STATE["paper_starting_bankroll"] = round(new_bal, 4)
        STATE["stats"]["paper_skims"] = 0
        STATE["stats"]["paper_profit_usdt"] = 0.0
        STATE["skim_ledger"] = [item for item in STATE.get("skim_ledger", []) if "LIVE" in item.get("pair", "")]
        add_log(f"🧹 Cleared Demo & Active Demo Coins! Virtual Bankroll reset to ${new_bal:.2f}.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/close_all_active_coins", methods=["POST"])
def api_close_all_active_coins():
    with state_lock:
        refund_total = 0.0
        to_remove = [k for k, v in STATE["spot_positions"].items() if not v.get("is_live")]
        for k in to_remove:
            pos = STATE["spot_positions"].pop(k, None)
            if pos:
                refund_total += float(pos.get("current_equity_usdt", pos.get("margin_usdt", 0.0)))
        STATE["paper_usdt_balance"] = round(STATE.get("paper_usdt_balance", 0.0) + refund_total, 4)
        add_log(f"♻️ Closed all active Demo coins & refunded ${refund_total:.2f} to Reserve Cash.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_profit_vault", methods=["POST"])
def api_reset_profit_vault():
    with state_lock:
        STATE["stats"]["paper_skims"] = 0
        STATE["stats"]["paper_profit_usdt"] = 0.0
        STATE["stats"]["live_skims"] = 0
        STATE["stats"]["live_profit_usdt"] = 0.0
        STATE["skim_ledger"] = []
        add_log("🧹 Reset Locked Profit Vault & Skim Ledger to $0.0000.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_true_net_profit", methods=["POST"])
def api_reset_true_net_profit():
    """Resets True Net Profit to $0.0000 by re-baselining Live Deposit to current True Net Worth."""
    with state_lock:
        mode = STATE.get("mode", "PAPER")
        if mode == "LIVE":
            sum_eq = sum(float(p.get("current_equity_usdt", 0.0)) for p in STATE.get("spot_positions", {}).values())
            free_cash = float(STATE.get("live_spot_usdt", 0.0))
            new_dep = round(sum_eq + free_cash, 4)
            STATE["live_starting_deposit"] = new_dep
            STATE["stats"]["live_skims"] = 0
            STATE["stats"]["live_profit_usdt"] = 0.0
            add_log(f"🔄 Reset True Net Profit to $0.0000 (New Live Deposit Baseline: ${new_dep:.4f}).")
        else:
            STATE["stats"]["paper_skims"] = 0
            STATE["stats"]["paper_profit_usdt"] = 0.0
            add_log("🔄 Reset Demo True Net Profit to $0.0000.")
        save_state()
    return jsonify({"ok": True, "live_starting_deposit": STATE.get("live_starting_deposit", 20.0)})


@app.route("/api/manual_long", methods=["POST"])
def api_manual_long():
    d = request.json or {}
    raw_pair = (d.get("pair") or "").strip().upper().replace("/", "_").replace("-", "_")
    if not raw_pair:
        return jsonify({"ok": False, "error": "Enter a Token Symbol first (e.g. SUI, PEPE, ARB)"}), 400
    pair = raw_pair if "_" in raw_pair else f"{raw_pair}_USDT"

    raw_amt = d.get("amount_usdt")
    custom_size = float(raw_amt) if (raw_amt and float(raw_amt) >= 1.0) else float(STATE.get("slot_size_usdt", 10.0))

    with state_lock:
        if pair in STATE["spot_positions"]:
            return jsonify({"ok": False, "error": f"{pair} is already active!"}), 400
        mode = STATE.get("mode", "PAPER")

    try:
        tk_map = fetch_spot_tickers([pair])
        if pair not in tk_map:
            return jsonify({"ok": False, "error": f"Spot pair {pair} not found on Gate.io"}), 400
        c_price = tk_map[pair]["last"]
        fee_buf = float(STATE.get("spot_fee_buffer_pct", 0.40))
        req_pump = float(STATE.get("skim_profit_pct", 1.0)) + fee_buf
        esc_pct = float(STATE.get("escape_dump_pct", 3.5))
        target_px = c_price * (1.0 + (req_pump / 100.0))
        esc_px = c_price * (1.0 - (esc_pct / 100.0))

        if mode == "PAPER":
            with state_lock:
                avail = float(STATE.get("paper_usdt_balance", 100.0))
                if avail < custom_size:
                    return jsonify({"ok": False, "error": f"Insufficient Free Reserve USDT (${avail:.2f})"}), 400
                STATE["paper_usdt_balance"] = round(avail - custom_size, 4)
                STATE["spot_positions"][pair] = {
                    "pair": pair,
                    "is_live": False,
                    "phase": "ACTIVE_LONG",
                    "in_recovery": False,
                    "borrowed_from_reserve_usdt": 0.0,
                    "coin_qty": custom_size / c_price,
                    "base_slot_usdt": custom_size,
                    "margin_usdt": custom_size,
                    "current_equity_usdt": custom_size,
                    "entry_price": c_price,
                    "last_price": c_price,
                    "target_sell_price": target_px,
                    "escape_stop_price": esc_px,
                    "bottom_watch_price": c_price,
                    "reentry_trigger_price": c_price,
                    "step_outs_count": 0,
                    "slot_skims": 0,
                    "slot_profit_usdt": 0.0,
                    "coin_move_pct": 0.0,
                    "pnl_pct": -fee_buf,
                    "pnl_usdt": round(-custom_size * (fee_buf / 100.0), 4),
                    "trailing_active": False,
                    "highest_price": c_price,
                    "status": f"Armed: Skims @ {fmt_px(target_px)} | -{esc_pct}% Shield @ {fmt_px(esc_px)}"
                }
                add_log(
                    f"🚀 [DEMO SPOT BUY][{pair}] Deployed ${custom_size:.2f} @ {fmt_px(c_price)} "
                    f"(Target: {fmt_px(target_px)} | -{esc_pct}% Shield: {fmt_px(esc_px)})"
                )
                save_state()
            return jsonify({"ok": True})
        else:
            if not STATE.get("api_key") or not STATE.get("api_secret"):
                return jsonify({"ok": False, "error": "Paste & Sync your Gate.io Spot API Keys first!"}), 400
            avail_spot = fetch_spot_usdt_balance()
            if avail_spot < custom_size:
                return jsonify({
                    "ok": False,
                    "error": f"Insufficient Gate.io Spot USDT (Available: ${avail_spot:.2f})"
                }), 400
            code, resp = place_spot_market_buy(pair, custom_size)
            if code in (200, 201):
                filled_qty = float(resp.get("filled_amount") or (custom_size / c_price))
                avg_px = float(resp.get("avg_deal_price") or c_price)
                if avg_px <= 0:
                    avg_px = c_price
                target_px = avg_px * (1.0 + (req_pump / 100.0))
                esc_px = avg_px * (1.0 - (esc_pct / 100.0))
                with state_lock:
                    STATE["live_spot_usdt"] = max(0.0, avail_spot - custom_size)
                    STATE["spot_positions"][pair] = {
                        "pair": pair,
                        "is_live": True,
                        "phase": "ACTIVE_LONG",
                        "in_recovery": False,
                        "borrowed_from_reserve_usdt": 0.0,
                        "coin_qty": filled_qty,
                        "base_slot_usdt": custom_size,
                        "margin_usdt": custom_size,
                        "current_equity_usdt": custom_size,
                        "entry_price": avg_px,
                        "last_price": avg_px,
                        "target_sell_price": target_px,
                        "escape_stop_price": esc_px,
                        "bottom_watch_price": avg_px,
                        "reentry_trigger_price": avg_px,
                        "step_outs_count": 0,
                        "slot_skims": 0,
                        "slot_profit_usdt": 0.0,
                        "coin_move_pct": 0.0,
                        "pnl_pct": -fee_buf,
                        "pnl_usdt": round(-custom_size * (fee_buf / 100.0), 4),
                        "trailing_active": False,
                        "highest_price": avg_px,
                        "status": f"LIVE Spot Loop Active | -{esc_pct}% Dump Shield Armed"
                    }
                    add_log(f"🚀 [LIVE SPOT BUY][{pair}] Bought ${custom_size:.2f} @ {fmt_px(avg_px)}")
                    save_state()
                return jsonify({"ok": True})
            return jsonify({"ok": False, "error": str(resp)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 400


@app.route("/api/step_out_or_resnipe_now", methods=["POST"])
def api_step_out_or_resnipe_now():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["spot_positions"].get(pair)
        if not pos:
            return jsonify({"ok": False, "error": "Position not found"}), 400
        mode = STATE.get("mode", "PAPER")
        phase = pos.get("phase", "ACTIVE_LONG")
        live_px = float(pos["last_price"])
        base_slot = float(pos.get("base_slot_usdt", 10.0))
        margin_usdt = float(pos.get("margin_usdt", 10.0))
        reentry_bounce = float(STATE.get("reentry_bounce_pct", 1.0))
        rec_pump = float(STATE.get("recovery_pump_pct", 2.20))
        esc_pct = float(STATE.get("escape_dump_pct", 3.5))
        fee_buf = float(STATE.get("spot_fee_buffer_pct", 0.40))

    if phase == "ACTIVE_LONG":
        real_eq = float(pos.get("current_equity_usdt", margin_usdt))
        if mode == "LIVE" and pos.get("is_live") and STATE.get("api_key"):
            code, resp = place_spot_market_sell(pair, float(pos.get("coin_qty", 0.0)))
            if code in (200, 201):
                real_eq = float(resp.get("filled_total") or real_eq)
        with state_lock:
            re_px = live_px * (1.0 + (reentry_bounce / 100.0))
            pos["phase"] = "STEPPED_OUT_HOLD"
            pos["coin_qty"] = 0.0
            pos["margin_usdt"] = round(real_eq, 4)
            pos["current_equity_usdt"] = round(real_eq, 4)
            pos["bottom_watch_price"] = live_px
            pos["reentry_trigger_price"] = re_px
            pos["step_outs_count"] = int(pos.get("step_outs_count", 0)) + 1
            pos["pnl_pct"] = 0.0
            pos["pnl_usdt"] = 0.0
            pos["status"] = f"🛡️ Stepped Out @ {fmt_px(live_px)} (${real_eq:.2f}) | Re-Buys full ${base_slot:.2f} on +{reentry_bounce}% bounce"
            add_log(f"🛡️ [MANUAL STEP-OUT][{pair}] Sold Spot @ {fmt_px(live_px)} (${real_eq:.2f} USDT held) -> Watching bottom!")
            save_state()
        return jsonify({"ok": True})
    else:
        req_pump = rec_pump + fee_buf
        new_target = live_px * (1.0 + (req_pump / 100.0))
        shortfall = max(0.0, round(base_slot - margin_usdt, 4))

        if mode == "LIVE" and pos.get("is_live") and STATE.get("api_key"):
            avail_spot = float(STATE.get("live_spot_usdt", 0.0))
            patch = min(avail_spot, shortfall)
            buy_usdt = round(min(avail_spot, margin_usdt + patch), 2)
            code, resp = place_spot_market_buy(pair, buy_usdt)
            if code not in (200, 201):
                return jsonify({"ok": False, "error": str(resp)}), 400
            filled_qty = float(resp.get("filled_amount") or (buy_usdt / live_px))
            avg_px = float(resp.get("avg_deal_price") or live_px)
            if avg_px <= 0:
                avg_px = live_px
            new_target = avg_px * (1.0 + (req_pump / 100.0))
            with state_lock:
                pos["coin_qty"] = filled_qty
                pos["margin_usdt"] = buy_usdt
                pos["current_equity_usdt"] = buy_usdt
                pos["entry_price"] = avg_px
                pos["borrowed_from_reserve_usdt"] = round(float(pos.get("borrowed_from_reserve_usdt", 0.0)) + patch, 4)
        else:
            with state_lock:
                avail_res = float(STATE.get("paper_usdt_balance", 0.0))
                patch = min(avail_res, shortfall)
                restored_margin = round(margin_usdt + patch, 4)
                STATE["paper_usdt_balance"] = round(avail_res - patch, 4)
                pos["coin_qty"] = restored_margin / live_px
                pos["margin_usdt"] = restored_margin
                pos["current_equity_usdt"] = restored_margin
                pos["entry_price"] = live_px
                pos["borrowed_from_reserve_usdt"] = round(float(pos.get("borrowed_from_reserve_usdt", 0.0)) + patch, 4)

        with state_lock:
            pos["phase"] = "ACTIVE_LONG"
            pos["in_recovery"] = True
            pos["target_sell_price"] = new_target
            pos["escape_stop_price"] = pos["entry_price"] * (1.0 - (esc_pct / 100.0))
            pos["trailing_active"] = False
            pos["highest_price"] = pos["entry_price"]
            pos["status"] = f"🎯 Restored to ${pos['margin_usdt']:.2f} @ {fmt_px(pos['entry_price'])} | Target +{req_pump:.2f}%"
            add_log(
                f"🎯 [MANUAL FULL RE-BUY][{pair}] Topped up to ${pos['margin_usdt']:.2f} from Reserve @ {fmt_px(pos['entry_price'])}"
            )
            save_state()
        return jsonify({"ok": True})


@app.route("/api/close_spot_slot", methods=["POST"])
def api_close_spot_slot():
    pair = (request.json or {}).get("pair", "")
    with state_lock:
        pos = STATE["spot_positions"].pop(pair, None)
        if pos:
            if not pos.get("is_live"):
                refund = float(pos.get("current_equity_usdt", pos.get("margin_usdt", 0.0)))
                STATE["paper_usdt_balance"] = round(STATE.get("paper_usdt_balance", 0.0) + refund, 4)
                add_log(f"✖ Closed {pair} slot & returned ${refund:.2f} to Reserve Cash.")
            elif STATE.get("api_key") and pos.get("phase") == "ACTIVE_LONG":
                place_spot_market_sell(pair, float(pos.get("coin_qty", 0.0)))
                add_log(f"✖ Sold & Closed LIVE {pair} Spot position on Gate.io.")
            save_state()
    return jsonify({"ok": True})


@app.route("/api/update_config", methods=["POST"])
def api_update_config():
    d = request.json or {}
    with state_lock:
        if d.get("slot_size_usdt"):
            STATE["slot_size_usdt"] = max(1.0, float(d["slot_size_usdt"]))
        if d.get("skim_profit_pct"):
            STATE["skim_profit_pct"] = max(0.2, float(d["skim_profit_pct"]))
        if d.get("escape_dump_pct"):
            STATE["escape_dump_pct"] = max(0.5, float(d["escape_dump_pct"]))
        if d.get("reentry_bounce_pct"):
            STATE["reentry_bounce_pct"] = max(0.2, float(d["reentry_bounce_pct"]))
        if d.get("recovery_pump_pct"):
            STATE["recovery_pump_pct"] = max(0.5, float(d["recovery_pump_pct"]))
        if d.get("live_starting_deposit"):
            STATE["live_starting_deposit"] = max(1.0, float(d["live_starting_deposit"]))
        add_log(
            f"⚙️ Saved v4.2 Spot Settings: Dump Shield=-{STATE['escape_dump_pct']}% | "
            f"Bounce Re-Buy=+{STATE['reentry_bounce_pct']}% | Recovery=+{STATE['recovery_pump_pct']}% | "
            f"Skim=+{STATE['skim_profit_pct']}% | Live Deposit=${STATE['live_starting_deposit']:.2f}"
        )
        save_state()
    run_bull_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/sync_keys", methods=["POST"])
def api_sync_keys():
    d = request.json or {}
    rk = (d.get("api_key") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")
    rs = (d.get("api_secret") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")
    with state_lock:
        if rk and "Saved" not in rk:
            STATE["api_key"] = rk
        if rs and "Saved" not in rs:
            STATE["api_secret"] = rs
        ak = STATE.get("api_key", "")
        asec = STATE.get("api_secret", "")
    if not ak or not asec:
        return jsonify({"ok": False, "error": "Paste both Gate.io API Key and Secret"}), 400
    try:
        u = fetch_spot_usdt_balance(ak, asec)
        with state_lock:
            STATE["api_connected"] = True
            STATE["live_spot_usdt"] = u
            STATE["api_status_msg"] = f"Verified! Gate.io Spot USDT: ${u:.2f}"
            add_log(f"🔗 Gate.io v4 Spot API Verified! Available USDT: ${u:.2f}")
            save_state()
        return jsonify({"ok": True, "usdt_balance": u})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)[:80]}), 400


# =============================================================================
# PEAK INSTITUTIONAL OBSIDIAN-GLASS UI — VAULTSKIMMER v4.2 PRO (BULL EDITION)
# =============================================================================
HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#00f5a0" />
  <link rel="manifest" href="/manifest.json" />
  <link rel="icon" type="image/svg+xml" href="/icon.svg" />
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700;800;900&family=JetBrains+Mono:wght@500;700;800&display=swap" rel="stylesheet" />
  <title>VaultSkimmer v4.2 PRO | Institutional Spot Bull Terminal</title>
  <style>
    :root {
      --bg: #040807;
      --surface: rgba(11, 20, 18, 0.78);
      --border: rgba(255, 255, 255, 0.08);
      --border-glow: rgba(0, 245, 160, 0.35);
      --neon: #00f5a0;
      --neon-soft: rgba(0, 245, 160, 0.13);
      --cyan: #00d9f5;
      --crimson: #ff2a5f;
      --crimson-soft: rgba(255, 42, 95, 0.14);
      --amber: #ffb020;
      --text: #f4f8f7;
      --muted: #8a9b96;
      --mono: 'JetBrains Mono', ui-monospace, SFMono-Regular, monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }

    body {
      background-color: var(--bg);
      background-image:
        radial-gradient(ellipse 80% 50% at 50% -10%, rgba(0, 245, 160, 0.15), transparent 65%),
        radial-gradient(ellipse 60% 40% at 90% 10%, rgba(0, 217, 245, 0.10), transparent 60%),
        linear-gradient(rgba(255,255,255,0.015) 1px, transparent 1px),
        linear-gradient(90deg, rgba(255,255,255,0.015) 1px, transparent 1px);
      background-size: 100% 100%, 100% 100%, 36px 36px, 36px 36px;
      color: var(--text);
      padding: 18px 16px 36px;
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
    }

    ::-webkit-scrollbar { width: 6px; height: 6px; }
    ::-webkit-scrollbar-track { background: rgba(255,255,255,0.02); }
    ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.14); border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: var(--neon); }

    .wrap { max-width: 1280px; margin: 0 auto; }

    .hero {
      background: linear-gradient(135deg, rgba(8, 28, 22, 0.88), rgba(9, 16, 20, 0.92));
      backdrop-filter: blur(20px);
      -webkit-backdrop-filter: blur(20px);
      border: 1px solid var(--border-glow);
      border-radius: 20px;
      padding: 20px 24px;
      margin-bottom: 16px;
      box-shadow: 0 20px 50px rgba(0, 0, 0, 0.65), inset 0 1px 0 rgba(255,255,255,0.08);
    }
    .hero-top { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 14px; }
    .logo { font-size: 1.55rem; font-weight: 900; display: flex; align-items: center; gap: 10px; letter-spacing: -0.4px; }
    .badge {
      background: linear-gradient(135deg, var(--neon), var(--cyan));
      color: #031c15; font-size: 0.68rem; font-weight: 900; padding: 4px 11px; border-radius: 99px;
      letter-spacing: 0.6px; text-transform: uppercase; box-shadow: 0 0 18px rgba(0, 245, 160, 0.35);
    }
    .sub { color: var(--muted); font-size: 0.8rem; margin-top: 5px; font-weight: 500; }

    .btn-row { display: flex; flex-wrap: wrap; gap: 8px; }
    .btn {
      cursor: pointer; border: none; border-radius: 11px; padding: 10px 15px;
      font-weight: 800; font-size: 0.76rem; letter-spacing: 0.2px;
      transition: transform 0.12s ease, box-shadow 0.2s ease, filter 0.15s ease;
      display: inline-flex; align-items: center; justify-content: center; gap: 6px;
    }
    .btn:hover { filter: brightness(1.08); transform: translateY(-1px); }
    .btn:active { transform: scale(0.97); }
    .btn-bull { background: linear-gradient(135deg, var(--neon), var(--cyan)); color: #031c15; box-shadow: 0 4px 18px rgba(0, 245, 160, 0.28); }
    .btn-crimson { background: linear-gradient(135deg, var(--crimson), #d90429); color: #fff; box-shadow: 0 4px 18px rgba(255, 42, 95, 0.3); }
    .btn-amber { background: linear-gradient(135deg, #ffb020, #f59e0b); color: #1a1200; }
    .btn-outline { background: rgba(255, 255, 255, 0.04); border: 1px solid rgba(255, 255, 255, 0.14); color: #e2e8f0; }
    .btn-outline:hover { border-color: var(--neon); color: #fff; background: var(--neon-soft); }

    .strip {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      margin-top: 14px; padding-top: 13px; border-top: 1px solid rgba(255,255,255,0.06);
      font-size: 0.76rem; color: var(--muted); gap: 12px;
    }
    .pulse-dot {
      display: inline-block; width: 8px; height: 8px; border-radius: 50%;
      background: var(--neon); box-shadow: 0 0 10px var(--neon); margin-right: 6px;
      animation: pulse 1.8s infinite ease-in-out;
    }
    @keyframes pulse { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }

    .top-panels { display: grid; grid-template-columns: 1.35fr 1fr; gap: 14px; margin-bottom: 16px; }
    @media (max-width: 920px) { .top-panels { grid-template-columns: 1fr; } }

    .panel-card {
      background: var(--surface);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 16px 18px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
    }
    .sec-title {
      font-size: 0.76rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.8px;
      color: #cbd5e1; margin-bottom: 12px; display: flex; justify-content: space-between;
      align-items: center; flex-wrap: wrap; gap: 8px;
    }
    .api-grid { display: grid; grid-template-columns: 1fr 1fr auto; gap: 10px; align-items: end; }
    @media (max-width: 640px) { .api-grid { grid-template-columns: 1fr; } }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(235px, 1fr)); gap: 14px; margin-bottom: 16px; }
    .card {
      background: var(--surface);
      backdrop-filter: blur(16px);
      border: 1px solid var(--border);
      border-radius: 16px;
      padding: 18px 20px;
      box-shadow: 0 10px 30px rgba(0, 0, 0, 0.35);
      transition: border-color 0.2s ease;
    }
    .card:hover { border-color: rgba(255, 255, 255, 0.14); }
    .kpi-card { position: relative; overflow: hidden; }
    .kpi-card::before {
      content: ""; position: absolute; top: 0; left: 0; right: 0; height: 2px;
      background: linear-gradient(90deg, transparent, rgba(255,255,255,0.2), transparent);
    }
    .kpi-vault { border-color: rgba(0, 245, 160, 0.35); background: linear-gradient(180deg, rgba(0, 245, 160, 0.06), var(--surface)); }
    .kpi-vault::before { background: linear-gradient(90deg, transparent, var(--neon), transparent); }
    .lbl { font-size: 0.68rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.8px; color: var(--muted); display: flex; justify-content: space-between; align-items: center; }
    .val { font-family: var(--mono); font-size: 1.65rem; font-weight: 800; margin: 8px 0 6px; letter-spacing: -0.5px; }
    .c-crimson { color: var(--crimson); }
    .c-neon { color: var(--neon); }
    .c-cyan { color: var(--cyan); }
    .c-amber { color: var(--amber); }

    .grid-main { display: grid; grid-template-columns: 1.18fr 1fr; gap: 16px; margin-bottom: 16px; }
    @media (max-width: 960px) { .grid-main { grid-template-columns: 1fr; } }

    .pos-card {
      background: linear-gradient(180deg, rgba(16, 32, 27, 0.9), rgba(9, 17, 15, 0.95));
      border: 1px solid rgba(255, 255, 255, 0.09);
      border-radius: 15px;
      padding: 15px 16px;
      margin-bottom: 12px;
      transition: border-color 0.2s ease;
    }
    .pos-card:hover { border-color: rgba(0, 245, 160, 0.35); }
    .step-pill { background: rgba(0, 217, 245, 0.12); border: 1px solid rgba(0, 217, 245, 0.45); color: var(--cyan); font-size: 0.67rem; font-weight: 800; padding: 3px 9px; border-radius: 99px; }
    .bull-pill { background: var(--neon-soft); border: 1px solid rgba(0, 245, 160, 0.45); color: var(--neon); font-size: 0.67rem; font-weight: 800; padding: 3px 9px; border-radius: 99px; }

    .metrics-box {
      background: rgba(4, 9, 8, 0.65); border: 1px solid rgba(255, 255, 255, 0.06);
      border-radius: 11px; padding: 10px 12px; margin: 10px 0;
      display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.76rem;
    }
    .metrics-box strong { font-family: var(--mono); }

    .track { height: 7px; background: rgba(255,255,255,0.05); border-radius: 99px; overflow: hidden; margin: 8px 0; }
    .fill { height: 100%; width: 0%; background: linear-gradient(90deg, var(--cyan), var(--neon)); border-radius: 99px; transition: width 0.4s cubic-bezier(0.22, 1, 0.36, 1); }

    .btn-step {
      background: var(--neon-soft); border: 1px solid rgba(0, 245, 160, 0.45); color: var(--neon);
      padding: 7px 12px; border-radius: 9px; font-size: 0.72rem; font-weight: 800; cursor: pointer;
    }
    .btn-step:hover { background: rgba(0, 245, 160, 0.22); }
    .btn-del {
      background: var(--crimson-soft); border: 1px solid rgba(255, 42, 95, 0.45); color: #fda4af;
      padding: 7px 12px; border-radius: 9px; font-size: 0.72rem; font-weight: 800; cursor: pointer;
    }
    .btn-del:hover { background: rgba(255, 42, 95, 0.25); color: #fff; }

    .chip-bar { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 12px; }
    .chip {
      background: rgba(255,255,255,0.04); border: 1px solid rgba(255,255,255,0.1);
      color: #cbd5e1; font-size: 0.7rem; font-weight: 800; padding: 5px 11px; border-radius: 99px; cursor: pointer;
      transition: all 0.15s ease;
    }
    .chip:hover { border-color: var(--neon); color: #fff; background: var(--neon-soft); }

    .input-grid-2 { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    .input-grid-3 { display: grid; grid-template-columns: repeat(auto-fit, minmax(125px, 1fr)); gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.69rem; color: var(--muted); margin-bottom: 5px; font-weight: 700; letter-spacing: 0.2px; }
    input {
      width: 100%; background: rgba(4, 9, 8, 0.8); border: 1px solid rgba(255,255,255,0.1);
      color: var(--text); padding: 10px 12px; border-radius: 10px; font-size: 0.83rem; font-weight: 700;
      font-family: var(--mono); transition: border-color 0.15s ease, box-shadow 0.15s ease;
    }
    input:focus { outline: none; border-color: var(--neon); box-shadow: 0 0 0 3px rgba(0, 245, 160, 0.14); }

    table { width: 100%; border-collapse: collapse; font-size: 0.77rem; }
    th, td { padding: 9px 8px; text-align: left; border-bottom: 1px solid rgba(255,255,255,0.05); font-family: var(--mono); }
    th { font-family: 'Inter', sans-serif; color: var(--muted); font-size: 0.66rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.6px; }
    tr:hover td { background: rgba(255,255,255,0.02); }
    .logs {
      background: rgba(3, 7, 6, 0.9); border: 1px solid rgba(255,255,255,0.07);
      border-radius: 12px; padding: 12px 14px; height: 220px; overflow-y: auto;
      font-family: var(--mono); font-size: 0.73rem; color: #d4e4df; line-height: 1.65;
    }
  </style>
</head>
<body>
<div class="wrap">

  <!-- HERO HEADER -->
  <div class="hero">
    <div class="hero-top">
      <div>
        <div class="logo">🟢 VAULTSKIMMER <span class="badge">v4.2 PRO BULL SPOT</span></div>
        <div class="sub">Profits When Market Goes UP • Carry-Over Recovery • -3.5% Dump Escape Shield + Bottom-Bounce Re-Buy</div>
      </div>
      <div class="btn-row">
        <button class="btn btn-bull" onclick="installApp()" id="installBtn">📲 Install App</button>
        <button class="btn btn-amber" onclick="toggleMode()" id="modeBtn">🔀 Mode: PAPER</button>
        <button class="btn btn-outline" onclick="syncLiveBags()">📥 Sync Gate.io Spot Bags</button>
        <button class="btn btn-outline" onclick="manualScan()">⚡ Sweep</button>
      </div>
    </div>
    <div class="strip">
      <div style="font-weight:700; color:var(--text);"><span class="pulse-dot"></span><span id="apiStatus">Paper Simulation Active</span></div>
      <div>Shield: <strong>-<span id="ruleEsc">3.5</span>% Dump Out → +<span id="ruleBounce">1.0</span>% Bottom Top-Up → +<span id="ruleRec">2.2</span>% Carry-Over Recovery</strong></div>
      <div>Sweep: <strong id="lastUpdate" style="font-family:var(--mono);color:#fff;">--:--:--</strong> (<strong class="c-neon" id="scanMs">0</strong>ms | #<span id="cycleNum">0</span>)</div>
    </div>
  </div>

  <!-- API KEY SYNC & DEMO RESET PANELS -->
  <div class="top-panels">
    <div class="panel-card">
      <div class="sec-title">
        <span>🔑 Gate.io v4 Spot API Connection</span>
        <span id="apiRightBadge" style="font-size:0.69rem;color:var(--amber);">Enable "Spot Trade (Read & Write)"</span>
      </div>
      <div class="api-grid">
        <div>
          <label>Gate.io v4 API Key</label>
          <input type="password" id="apiKeyInput" placeholder="Paste API Key..." autocomplete="off" />
        </div>
        <div>
          <label>Gate.io v4 API Secret</label>
          <input type="password" id="apiSecretInput" placeholder="Paste API Secret..." autocomplete="off" />
        </div>
        <button class="btn btn-bull" style="height:40px;" onclick="syncApiKeys()" id="syncBtn">🔗 Sync Keys</button>
      </div>
    </div>

    <div class="panel-card">
      <div class="sec-title">
        <span>💰 Reset Demo & Active Coins</span>
        <button class="btn-del" style="padding:4px 9px;font-size:0.66rem;" onclick="resetProfitVault()">🧹 Zero Vault</button>
      </div>
      <div style="display:grid; grid-template-columns: 1fr auto auto; gap:8px; align-items:end;">
        <div>
          <label>Demo Starting USDT ($)</label>
          <input type="number" id="resetUsdtInput" step="10" value="100" />
        </div>
        <button class="btn btn-crimson" style="height:40px;" onclick="clearDemoOnly()">🧹 Clear Demo</button>
        <button class="btn btn-outline" style="height:40px;" onclick="closeAllActiveCoins()">♻️ Close Coins</button>
      </div>
    </div>
  </div>

  <!-- 4 SCOREBOARD CARDS -->
  <div class="grid-4">
    <div class="card kpi-card">
      <div class="lbl">1. Active Spot Capital</div>
      <div class="val c-neon" id="deployedVal">$0.00</div>
      <div class="sub">Active Loops: <strong id="posCount" style="color:#fff;">0</strong></div>
    </div>
    <div class="card kpi-card">
      <div class="lbl">2. Reserve Cash (Top-Up Pool)</div>
      <div class="val c-amber" id="freeUsdt">$100.00</div>
      <div class="sub">Tops Up Step-Outs to Full Principal</div>
    </div>
    <div class="card kpi-card kpi-vault">
      <div class="lbl">
        <span>3. 🔒 True Net Profit</span>
        <button class="btn-del" style="padding:3px 8px;font-size:0.64rem;" onclick="resetTrueNetProfit()">🔄 Reset</button>
      </div>
      <div class="val c-neon" id="profitVault">+$0.0000</div>
      <div class="sub">Wins: <strong id="skimCount" style="color:#fff;">0</strong> | Deposit: $<span id="depLbl">20.00</span></div>
    </div>
    <div class="card kpi-card">
      <div class="lbl">4. True Total Net Worth</div>
      <div class="val c-cyan" id="totalPort">$100.0000</div>
      <div class="sub">Shield: <strong>-<span id="cardEsc">3.5</span>% / +<span id="cardBounce">1.0</span>% / +<span id="cardRec">2.2</span>%</strong></div>
    </div>
  </div>

  <!-- MAIN WORKSPACE -->
  <div class="grid-main">
    <div class="card">
      <div class="sec-title">
        <span>🟢 Active Spot Long Loops (-3.5% Dump Escape Shield)</span>
        <span style="font-size:0.7rem;color:var(--cyan);">Carry-Over Recovery Active</span>
      </div>
      <div id="positionsBox"></div>
    </div>

    <div>
      <!-- MANUAL LONG COMMAND DECK -->
      <div class="card" style="margin-bottom:16px;">
        <div class="sec-title">➕ Launch Protected Spot Long Loop</div>
        <div class="chip-bar">
          <span class="chip" onclick="fillPair('SUI_USDT')">SUI</span>
          <span class="chip" onclick="fillPair('PEPE_USDT')">PEPE</span>
          <span class="chip" onclick="fillPair('ARB_USDT')">ARB</span>
          <span class="chip" onclick="fillPair('FET_USDT')">FET</span>
          <span class="chip" onclick="fillPair('DOGE_USDT')">DOGE</span>
          <span class="chip" onclick="fillPair('SOL_USDT')">SOL</span>
          <span class="chip" onclick="fillPair('AVAX_USDT')">AVAX</span>
          <span class="chip" onclick="fillPair('DOT_USDT')">DOT</span>
          <span class="chip" onclick="fillPair('WIF_USDT')">WIF</span>
          <span class="chip" onclick="fillPair('NEAR_USDT')">NEAR</span>
          <span class="chip" onclick="fillPair('XRP_USDT')">XRP</span>
          <span class="chip" onclick="fillPair('ADA_USDT')">ADA</span>
        </div>
        <div class="input-grid-2">
          <div>
            <label>Token Symbol</label>
            <input type="text" id="customPairInput" placeholder="SUI, PEPE, ARB" />
          </div>
          <div>
            <label>Buy Amount (USDT)</label>
            <input type="number" id="customAmtInput" step="5" value="10" />
          </div>
        </div>
        <button class="btn btn-bull" style="width:100%;padding:12px;" onclick="launchCustomLong()">🟢 Launch Protected Spot Buy Loop Now</button>
      </div>

      <!-- ENGINE PARAMETERS -->
      <div class="card">
        <div class="sec-title">⚙️ Dump Shield & Bull Skim Parameters</div>
        <div class="input-grid-3">
          <div>
            <label>Escape Dump (%)</label>
            <input type="number" id="cfgEsc" step="0.1" value="3.5" />
          </div>
          <div>
            <label>Bottom Bounce (%)</label>
            <input type="number" id="cfgBounce" step="0.1" value="1.0" />
          </div>
          <div>
            <label>Recovery Pump (%)</label>
            <input type="number" id="cfgRec" step="0.1" value="2.2" />
          </div>
        </div>
        <div class="input-grid-3">
          <div>
            <label>Default Buy ($)</label>
            <input type="number" id="cfgSize" step="5" value="10" />
          </div>
          <div>
            <label>Normal Skim (%)</label>
            <input type="number" id="cfgTarget" step="0.05" value="1.0" />
          </div>
          <div>
            <label>Live Deposit ($)</label>
            <input type="number" id="cfgDeposit" step="0.01" value="20.00" />
          </div>
        </div>
        <button class="btn btn-bull" style="width:100%;padding:12px;" onclick="saveConfig()">💾 Save v4.2 Spot Engine Settings</button>
      </div>
    </div>
  </div>

  <!-- BOTTOM WORKSPACE -->
  <div class="grid-main">
    <div class="card">
      <div class="sec-title">
        <span>📡 16-Market Bullish Breakout Radar</span>
        <span style="font-size:0.7rem;color:var(--muted);">Tap Buy to Launch</span>
      </div>
      <div style="overflow-x:auto; max-height:260px;">
        <table>
          <thead>
            <tr>
              <th>Pair</th>
              <th>Live Price (24h)</th>
              <th>Bull Score</th>
              <th>5m RSI</th>
              <th>Buyers %</th>
              <th>Action</th>
            </tr>
          </thead>
          <tbody id="radarBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">🔒 Verified Bull Skim & Refund Ledger</div>
      <div style="max-height:260px; overflow-y:auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Pair</th>
              <th>Pump</th>
              <th>Bought → Sold</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody id="ledgerBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title">📡 Real-Time Spot Execution & Recovery Telemetry</div>
    <div class="logs" id="logBox"></div>
  </div>

</div>

<script>
  let deferredPrompt = null;
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
  });

  async function installApp() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      deferredPrompt = null;
    } else {
      alert("To install VaultSkimmer Pro on your phone: Tap your browser menu (⋮) and select 'Install app' or 'Add to Home screen'.");
    }
  }

  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  function fillPair(p) {
    document.getElementById('customPairInput').value = p;
  }

  async function syncApiKeys() {
    const btn = document.getElementById('syncBtn');
    const k = document.getElementById('apiKeyInput').value.trim();
    const s = document.getElementById('apiSecretInput').value.trim();
    btn.innerText = 'Syncing...';
    const r = await fetch('/api/sync_keys', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ api_key: k, api_secret: s })
    });
    const d = await r.json();
    if (d.ok) {
      document.getElementById('apiKeyInput').value = '';
      document.getElementById('apiSecretInput').value = '';
      btn.innerText = '✅ Synced';
    } else {
      alert('API Notice: ' + (d.error || 'Check keys'));
      btn.innerText = '❌ Retry';
    }
    await refresh();
    setTimeout(() => { btn.innerText = '🔗 Sync Keys'; }, 2500);
  }

  async function syncLiveBags() {
    const r = await fetch('/api/sync_live_positions', { method: 'POST' });
    const d = await r.json();
    if (d.ok) {
      alert(d.imported.length ? ('Synced Live Spot Bags: ' + d.imported.join(', ')) : 'No Spot coin holdings >= $2.00 found on Gate.io.');
      await refresh();
    } else {
      alert('Notice: ' + d.error);
    }
  }

  async function clearDemoOnly() {
    const amt = document.getElementById('resetUsdtInput').value || 100;
    await fetch('/api/clear_demo_only', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ amount_usdt: amt })
    });
    await refresh();
  }

  async function closeAllActiveCoins() {
    await fetch('/api/close_all_active_coins', { method: 'POST' });
    await refresh();
  }

  async function resetProfitVault() {
    await fetch('/api/reset_profit_vault', { method: 'POST' });
    await refresh();
  }

  async function resetTrueNetProfit() {
    const r = await fetch('/api/reset_true_net_profit', { method: 'POST' });
    const d = await r.json();
    if (d.ok && d.live_starting_deposit !== undefined) {
      document.getElementById('cfgDeposit').value = Number(d.live_starting_deposit).toFixed(2);
    }
    await refresh();
  }

  async function launchCustomLong(pairOverride) {
    const pair = pairOverride || document.getElementById('customPairInput').value.trim();
    const amt = document.getElementById('customAmtInput').value || 10;
    if (!pair) {
      alert('Select or type a token symbol first.');
      return;
    }
    const r = await fetch('/api/manual_long', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair, amount_usdt: amt })
    });
    const d = await r.json();
    if (!d.ok) alert(d.error || 'Could not launch buy');
    await refresh();
  }

  async function stepOutOrResnipe(pair) {
    const r = await fetch('/api/step_out_or_resnipe_now', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    const d = await r.json();
    if (!d.ok) alert(d.error);
    await refresh();
  }

  async function closeSlot(pair) {
    await fetch('/api/close_spot_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    await refresh();
  }

  async function toggleMode() {
    await fetch('/api/toggle_mode', { method: 'POST' });
    await refresh();
  }

  async function manualScan() {
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
  }

  async function saveConfig() {
    await fetch('/api/update_config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        slot_size_usdt: document.getElementById('cfgSize').value,
        skim_profit_pct: document.getElementById('cfgTarget').value,
        escape_dump_pct: document.getElementById('cfgEsc').value,
        reentry_bounce_pct: document.getElementById('cfgBounce').value,
        recovery_pump_pct: document.getElementById('cfgRec').value,
        live_starting_deposit: document.getElementById('cfgDeposit').value
      })
    });
    await refresh();
  }

  let cfgLoaded = false;
  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      if (!cfgLoaded) {
        document.getElementById('cfgSize').value = s.slot_size_usdt || 10;
        document.getElementById('customAmtInput').value = s.slot_size_usdt || 10;
        document.getElementById('cfgTarget').value = s.skim_profit_pct || 1.0;
        document.getElementById('cfgEsc').value = s.escape_dump_pct || 3.5;
        document.getElementById('cfgBounce').value = s.reentry_bounce_pct || 1.0;
        document.getElementById('cfgRec').value = s.recovery_pump_pct || 2.2;
        document.getElementById('cfgDeposit').value = s.live_starting_deposit || 20.00;
        document.getElementById('resetUsdtInput').value = s.paper_starting_bankroll || 100;
        cfgLoaded = true;
      }

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('cycleNum').innerText = s.cycle_count;

      const escVal = Number(s.escape_dump_pct || 3.5).toFixed(1);
      const bounceVal = Number(s.reentry_bounce_pct || 1.0).toFixed(1);
      const recVal = Number(s.recovery_pump_pct || 2.2).toFixed(1);
      const depVal = Number(s.live_starting_deposit || 20.00);

      document.getElementById('ruleEsc').innerText = escVal;
      document.getElementById('ruleBounce').innerText = bounceVal;
      document.getElementById('ruleRec').innerText = recVal;
      document.getElementById('cardEsc').innerText = escVal;
      document.getElementById('cardBounce').innerText = bounceVal;
      document.getElementById('cardRec').innerText = recVal;
      document.getElementById('depLbl').innerText = depVal.toFixed(2);

      const modeBtn = document.getElementById('modeBtn');
      modeBtn.innerText = s.mode === 'LIVE' ? '🔀 Mode: LIVE SPOT' : '🔀 Mode: PAPER';
      modeBtn.className = s.mode === 'LIVE' ? 'btn btn-bull' : 'btn btn-amber';

      document.getElementById('apiStatus').innerText = s.api_status_msg || '';
      document.getElementById('apiRightBadge').innerText = s.api_connected ? '✅ Gate.io Spot Connected' : 'Enable "Spot Trade (Read & Write)"';

      const posMap = s.spot_positions || {};
      const keys = Object.keys(posMap);
      let sumEq = 0;
      keys.forEach(k => { sumEq += Number(posMap[k].current_equity_usdt || 0); });

      const freeCash = s.mode === 'LIVE' ? Number(s.live_spot_usdt || 0) : Number(s.paper_usdt_balance || 0);
      const trackedProf = s.mode === 'LIVE' ? Number(s.stats.live_profit_usdt || 0) : Number(s.stats.paper_profit_usdt || 0);
      const skims = s.mode === 'LIVE' ? (s.stats.live_skims || 0) : (s.stats.paper_skims || 0);

      document.getElementById('deployedVal').innerText = '$' + sumEq.toFixed(2);
      document.getElementById('posCount').innerText = keys.length;
      document.getElementById('freeUsdt').innerText = '$' + freeCash.toFixed(2);
      document.getElementById('skimCount').innerText = skims;

      const trueNetWorth = s.mode === 'LIVE' ? (sumEq + freeCash) : (sumEq + freeCash + trackedProf);
      document.getElementById('totalPort').innerText = '$' + trueNetWorth.toFixed(4);

      const displayProfit = s.mode === 'LIVE' ? (trueNetWorth - depVal) : trackedProf;
      const profEl = document.getElementById('profitVault');
      profEl.innerText = (displayProfit >= 0 ? '+$' : '-$') + Math.abs(displayProfit).toFixed(4);
      profEl.style.color = displayProfit >= 0 ? 'var(--neon)' : 'var(--crimson)';

      const pBox = document.getElementById('positionsBox');
      if (keys.length === 0) {
        pBox.innerHTML = `<div style="text-align:center;color:var(--muted);padding:32px;font-size:0.84rem;">0 Active Coins — Launch a Spot coin on the right or tap "📥 Sync Gate.io Spot Bags" above.</div>`;
      } else {
        pBox.innerHTML = keys.map(k => {
          const p = posMap[k];
          const phase = p.phase || 'ACTIVE_LONG';
          const pnl = Number(p.pnl_pct || 0);
          const pnlSign = pnl >= 0 ? '+' : '';
          const pnlUsdt = Number(p.pnl_usdt || 0);
          const slotSkims = Number(p.slot_skims || 0);
          const slotProf = Number(p.slot_profit_usdt || 0);
          const borrowed = Number(p.borrowed_from_reserve_usdt || 0);
          const goalPct = p.in_recovery ? Number(s.recovery_pump_pct || 2.2) : Number(s.skim_profit_pct || 1.0);
          const prog = phase === 'STEPPED_OUT_HOLD' ? 50 : Math.max(0, Math.min(100, (pnl / goalPct) * 100));

          const phaseBadge = phase === 'STEPPED_OUT_HOLD'
            ? `<span class="step-pill" style="border-color:var(--amber);color:var(--amber);">🛡️ HOLDING $${Number(p.margin_usdt).toFixed(2)} (RE-BUYS $${Number(p.base_slot_usdt || 10).toFixed(2)})</span>`
            : (p.in_recovery
                ? `<span class="step-pill">🎯 RECOVERY (DEBT: $${borrowed.toFixed(2)})</span>`
                : `<span class="bull-pill">$${Number(p.margin_usdt).toFixed(2)} Spot</span>`);

          const actionBtn = phase === 'STEPPED_OUT_HOLD'
            ? `<button class="btn-step" onclick="stepOutOrResnipe('${p.pair}')">🎯 Top-Up & Re-Buy Now</button>`
            : `<button class="btn-step" onclick="stepOutOrResnipe('${p.pair}')">🛡️ Step Out to USDT Now</button>`;

          return `
            <div class="pos-card">
              <div style="display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:6px;">
                <div style="display:flex;align-items:center;gap:6px;flex-wrap:wrap;">
                  <strong style="font-size:1.02rem;letter-spacing:-0.2px;">${p.pair}</strong>
                  ${phaseBadge}
                  <span class="step-pill" style="border-color:rgba(0,245,160,0.45);color:var(--neon);background:var(--neon-soft);">Won: +$${slotProf.toFixed(2)} (${slotSkims})</span>
                </div>
                <div style="font-family:var(--mono);font-weight:800;font-size:0.9rem;color:${pnl >= 0 ? 'var(--neon)' : 'var(--amber)'}">
                  ${phase === 'STEPPED_OUT_HOLD' ? 'SAFE IN USDT' : `${pnlSign}${pnl.toFixed(2)}% (${pnlSign}$${pnlUsdt.toFixed(2)})`}
                </div>
              </div>

              <div class="metrics-box">
                <div>${phase === 'STEPPED_OUT_HOLD' ? `Bottom Watched: <strong>${fmtPx(p.bottom_watch_price)}</strong>` : `Entry Floor: <strong>${fmtPx(p.entry_price)}</strong>`}</div>
                <div>${phase === 'STEPPED_OUT_HOLD' ? `Re-Buy Trigger: <strong class="c-amber">${fmtPx(p.reentry_trigger_price)}</strong>` : `Sell Skim Target: <strong class="c-neon">${fmtPx(p.target_sell_price)}</strong>`}</div>
                <div>Live Price: <strong>${fmtPx(p.last_price)}</strong></div>
                <div>${phase === 'STEPPED_OUT_HOLD' ? `Reserve Patch: <strong class="c-cyan">+$${Math.max(0, Number(p.base_slot_usdt || 10) - Number(p.margin_usdt)).toFixed(2)}</strong>` : `-${escVal}% Shield: <strong class="c-crimson">${fmtPx(p.escape_stop_price)}</strong>`}</div>
              </div>

              <div style="display:flex;justify-content:space-between;font-size:0.71rem;color:var(--muted);">
                <span>${p.status}</span>
                <strong class="c-neon">${phase === 'STEPPED_OUT_HOLD' ? 'Watching Bottom' : `${prog.toFixed(0)}% to Skim`}</strong>
              </div>
              <div class="track"><div class="fill" style="width:${prog.toFixed(1)}%"></div></div>

              <div style="display:flex;justify-content:space-between;align-items:center;margin-top:10px;">
                ${actionBtn}
                <button class="btn-del" onclick="closeSlot('${p.pair}')">✖ Sell & Close</button>
              </div>
            </div>
          `;
        }).join('');
      }

      const rb = document.getElementById('radarBody');
      rb.innerHTML = (s.radar || []).map(item => {
        const chgSign = item.change_24h >= 0 ? '+' : '';
        return `
          <tr>
            <td><strong>${item.pair}</strong></td>
            <td>${fmtPx(item.price)} <span style="color:${item.change_24h < 0 ? 'var(--crimson)' : 'var(--neon)'}">(${chgSign}${item.change_24h}%)</span></td>
            <td><span class="bull-pill">${item.bull_score}/100</span></td>
            <td>${item.rsi_14}</td>
            <td>${item.bid_buy_pct}%</td>
            <td><button class="btn btn-outline" style="padding:4px 10px;font-size:0.69rem;" onclick="launchCustomLong('${item.pair}')">🟢 Buy</button></td>
          </tr>
        `;
      }).join('');

      const lb = document.getElementById('ledgerBody');
      if (s.skim_ledger && s.skim_ledger.length > 0) {
        lb.innerHTML = s.skim_ledger.map(t => `
          <tr>
            <td>${t.time}</td>
            <td><strong>${t.pair}</strong></td>
            <td class="c-neon"><strong>${t.pump}</strong></td>
            <td>${t.buy_to_sell}</td>
            <td class="c-neon"><strong>${t.profit}</strong></td>
          </tr>
        `).join('');
      } else {
        lb.innerHTML = `<tr><td colspan="5" style="text-align:center;color:var(--muted);padding:18px;">Waiting for next green-candle skim...</td></tr>`;
      }

      document.getElementById('logBox').innerHTML = (s.logs || []).join('<br/>');
    } catch (e) {}
  }

  setInterval(refresh, 2500);
  refresh();
</script>
</body>
</html>
"""


@app.route("/")
def index():
    return render_template_string(HTML_UI)


if __name__ == "__main__":
    ensure_worker_alive()
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
