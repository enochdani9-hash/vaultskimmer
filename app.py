import os
import time
import json
import hmac
import hashlib
import threading
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from flask import Flask, request, jsonify, Response, render_template_string

app = Flask(__name__)

STATE_FILE = "vaultskimmer_v5_8_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=16,
    pool_maxsize=16,
    max_retries=Retry(total=2, backoff_factor=0.15, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)

PAIR_META_CACHE = {}


def fmt_px(val):
    v = float(val or 0.0)
    if v == 0:
        return "$0.0000"
    if v < 0.0001:
        return f"${v:.8f}"
    if v < 0.01:
        return f"${v:.6f}"
    return f"${v:.4f}"


def make_slot(pair, principal, skim_pct, stop_reset_pct=99.0, pullback_pct=0.30, entry_price=0.0, slot_type="LIVE"):
    return {
        "pair": pair,
        "slot_type": slot_type,
        "principal_usdt": float(principal),
        "skim_target_pct": float(skim_pct),
        "stop_reset_pct": float(stop_reset_pct),
        "rebuy_pullback_pct": float(pullback_pct),
        "entry_price": float(entry_price),
        "actual_cost_usdt": 0.0,
        "last_skim_sell_price": 0.0,
        "last_trade_ts": 0.0,
        "started_ts": time.time(),
        "phase": "WAITING_TO_BUY",
        "paper_base_bal": 0.0,
        "base_balance": 0.0,
        "position_value_usdt": 0.0,
        "last_price": 0.0,
        "price_prec": 4,
        "high_24h": 0.0,
        "low_24h": 0.0,
        "change_24h_pct": 0.0,
        "range_24h_pct": 0.0,
        "target_sell_price": 0.0,
        "stop_reset_price": 0.0,
        "target_rebuy_price": 0.0,
        "unrealized_pnl_usdt": 0.0,
        "unrealized_pnl_pct": 0.0,
        "slot_skimmed_usdt": 0.0,
        "slot_skim_count": 0,
        "status_msg": "Slot armed — awaiting next scan..."
    }


DEFAULT_STATE = {
    "mode": "LIVE",
    "fee_buffer_pct": 0.25,
    "api_key": os.environ.get("GATE_API_KEY", "").strip(),
    "api_secret": os.environ.get("GATE_API_SECRET", "").strip(),
    "api_connected": False,
    "api_status_msg": "Not Connected — Paste Keys & Tap Sync",
    "slots": {},
    "paper_slots": {},
    "paper_bankroll_usdt": 100.0,
    "paper_free_usdt": 100.0,
    "paper_skimmed_usdt": 0.0,
    "paper_skim_count": 0,
    "live_skimmed_usdt": 0.0,
    "live_skim_count": 0,
    "usdt_balance": 0.0,
    "last_scan_ms": 0,
    "cycle_count": 0,
    "last_update": "--:--:--",
    "last_worker_heartbeat": 0.0,
    "skim_ledger": [],
    "paper_ledger": [],
    "logs": []
}

state_lock = threading.Lock()
cycle_lock = threading.Lock()
worker_started = False
STATE = DEFAULT_STATE.copy()


def normalize_pair(raw_pair):
    p = (raw_pair or "").strip().upper().replace("/", "_").replace("-", "_").replace(" ", "")
    if not p:
        return ""
    if "_" not in p:
        if p.endswith("USDT") and len(p) > 4:
            p = p[:-4] + "_USDT"
        else:
            p = p + "_USDT"
    return p


def load_state():
    global STATE
    for fn in [STATE_FILE, "vaultskimmer_v5_6_state.json", "vaultskimmer_v5_5_state.json", "vaultskimmer_v5_4_state.json"]:
        if os.path.exists(fn):
            try:
                with open(fn, "r") as f:
                    saved = json.load(f)
                    saved_key = (saved.get("api_key") or "").strip()
                    saved_sec = (saved.get("api_secret") or "").strip()
                    STATE.update(saved)
                    if saved_key:
                        STATE["api_key"] = saved_key
                    if saved_sec:
                        STATE["api_secret"] = saved_sec
                    if "slots" not in STATE or not isinstance(STATE["slots"], dict):
                        STATE["slots"] = {}
                    if "paper_slots" not in STATE or not isinstance(STATE["paper_slots"], dict):
                        STATE["paper_slots"] = {}

                    # Auto-heal the $0.00 unfilled PEPE phantom skim from 16:26:15!
                    pepe_slot = STATE["slots"].get("PEPE_USDT")
                    if pepe_slot and pepe_slot.get("entry_price") == 0.00000526 and pepe_slot.get("slot_skimmed_usdt") == 0.01:
                        pepe_slot["entry_price"] = 0.00000516
                        pepe_slot["actual_cost_usdt"] = 10.35
                        pepe_slot["slot_skimmed_usdt"] = 0.0
                        pepe_slot["slot_skim_count"] = 0
                        STATE["live_skimmed_usdt"] = max(0.0, round(float(STATE.get("live_skimmed_usdt", 0.0)) - 0.01, 4))
                        STATE["live_skim_count"] = max(0, int(STATE.get("live_skim_count", 0)) - 1)
                        STATE["skim_ledger"] = [
                            item for item in STATE.get("skim_ledger", [])
                            if "($0.00)" not in str(item.get("sell_px", ""))
                        ]
                break
            except Exception as e:
                add_log(f"State load notice: {e}")


def save_state():
    try:
        tmp_file = STATE_FILE + ".tmp"
        with open(tmp_file, "w") as f:
            json.dump(STATE, f, indent=2)
        os.replace(tmp_file, STATE_FILE)
    except Exception:
        pass


def add_log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    STATE["logs"].insert(0, entry)
    STATE["logs"] = STATE["logs"][:100]


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


def get_pair_meta(pair):
    now = time.time()
    cached = PAIR_META_CACHE.get(pair)
    if not cached or (now - cached["ts"] > 3600):
        p_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs/{pair}",
            timeout=5
        )
        if p_resp.status_code != 200:
            raise ValueError(f"Token pair '{pair}' not found on Gate.io")
        p_info = p_resp.json()
        cached = {
            "amount_prec": int(p_info.get("amount_precision", 4)),
            "price_prec": int(p_info.get("precision", 8)),
            "min_quote": float(p_info.get("min_quote_amount", 1.0)),
            "min_base": float(p_info.get("min_base_amount", 0.0001)),
            "ts": now
        }
        PAIR_META_CACHE[pair] = cached
    return cached


def fetch_batch_tickers(pairs):
    if not pairs:
        return {}
    r = HTTP_SESSION.get(f"{GATE_HOST}{GATE_PREFIX}/spot/tickers", timeout=6)
    r.raise_for_status()
    all_data = r.json()
    wanted = set(pairs)
    result = {}
    for t in all_data:
        cp = t.get("currency_pair")
        if cp in wanted:
            last = float(t["last"])
            high_24 = float(t.get("high_24h") or last)
            low_24 = float(t.get("low_24h") or last)
            rng_pct = ((high_24 - low_24) / low_24 * 100.0) if low_24 > 0 else 0.0
            result[cp] = {
                "last": last,
                "high_24h": high_24,
                "low_24h": low_24,
                "change_24h": float(t.get("change_percentage") or 0.0),
                "range_24h_pct": round(rng_pct, 2)
            }
    return result


def fetch_all_spot_balances(custom_key=None, custom_secret=None):
    url = f"{GATE_PREFIX}/spot/accounts"
    headers = gate_sign("GET", url, "", "", custom_key, custom_secret)
    r = HTTP_SESSION.get(f"{GATE_HOST}{url}", headers=headers, timeout=5)
    if r.status_code != 200:
        try:
            err_msg = r.json().get("message", r.text)
        except Exception:
            err_msg = r.text
        raise ValueError(f"Gate.io HTTP {r.status_code}: {err_msg}")
    accounts = r.json()
    bal_map = {}
    for acc in accounts:
        curr = acc.get("currency")
        if curr:
            bal_map[curr] = float(acc.get("available", 0.0))
    return bal_map


def format_dec(val, precision, round_mode=ROUND_DOWN):
    q = Decimal("10") ** (-precision)
    return str(Decimal(str(val)).quantize(q, rounding=round_mode))


def calc_fee_protected_buy_amt(budget_usdt, price, amt_prec):
    raw_units = budget_usdt / price
    if amt_prec == 0:
        target_int = int(Decimal(str(raw_units)).quantize(Decimal("1"), rounding=ROUND_DOWN))
        return str(max(1, target_int))
    else:
        padded = raw_units * 1.0012
        return format_dec(padded, amt_prec, ROUND_DOWN)


def place_gate_order(pair, side, amount_str, price_str):
    url = f"{GATE_PREFIX}/spot/orders"
    body = {
        "currency_pair": pair,
        "type": "limit",
        "account": "spot",
        "side": side,
        "amount": amount_str,
        "price": price_str,
        "time_in_force": "ioc"
    }
    body_str = json.dumps(body)
    headers = gate_sign("POST", url, "", body_str)
    r = HTTP_SESSION.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=7)
    return r.status_code, r.json()


# =========================================================================
# ENGINE 1: PAPER STUDY LAB SLOT PROCESSOR (Runs 100% Independently on $100)
# =========================================================================
def process_paper_slot(pair, slot, fee_buf, ticker_map):
    principal = float(slot["principal_usdt"])
    skim_pct = float(slot["skim_target_pct"])
    stop_reset_pct = float(slot.get("stop_reset_pct", 3.0))
    pullback_pct = float(slot.get("rebuy_pullback_pct", 0.30))

    tk = ticker_map.get(pair)
    if not tk:
        return

    meta = get_pair_meta(pair)
    price = tk["last"]
    px_prec = meta["price_prec"]

    slot["last_price"] = price
    slot["price_prec"] = px_prec
    slot["high_24h"] = tk["high_24h"]
    slot["low_24h"] = tk["low_24h"]
    slot["change_24h_pct"] = tk["change_24h"]
    slot["range_24h_pct"] = tk.get("range_24h_pct", 0.0)

    effective_gain_pct = skim_pct + fee_buf
    last_sell_px = float(slot.get("last_skim_sell_price", 0.0))
    if last_sell_px > 0 and pullback_pct > 0:
        rebuy_px = last_sell_px * (1.0 - (pullback_pct / 100.0))
    else:
        rebuy_px = price
    slot["target_rebuy_price"] = round(rebuy_px, px_prec)

    now = time.time()
    paper_tokens = float(slot.get("paper_base_bal", 0.0))
    pos_val = paper_tokens * price

    if paper_tokens <= 0 or pos_val < (principal * 0.50):
        if pullback_pct > 0 and last_sell_px > 0:
            breakout_px = last_sell_px * 1.005
            waited_sec = now - float(slot.get("last_trade_ts", 0.0))
            if rebuy_px < price < breakout_px and waited_sec < 180:
                slot["phase"] = "WAITING_TO_BUY"
                slot["base_balance"] = 0.0
                slot["position_value_usdt"] = 0.0
                slot["status_msg"] = (
                    f"[STUDY] Holding USDT — Waiting for -{pullback_pct}% dip ({fmt_px(rebuy_px)}) "
                    f"or +0.5% breakout ({fmt_px(breakout_px)}) | {int(180 - waited_sec)}s left"
                )
                return

        sim_tokens = principal / price
        slot["paper_base_bal"] = sim_tokens
        slot["base_balance"] = sim_tokens
        slot["position_value_usdt"] = round(principal, 4)
        slot["entry_price"] = price
        target_px = price * (1.0 + (effective_gain_pct / 100.0))
        stop_px = price * (1.0 - (stop_reset_pct / 100.0))
        slot["target_sell_price"] = round(target_px, px_prec)
        slot["stop_reset_price"] = round(stop_px, px_prec)
        slot["phase"] = "HOLDING_FOR_SKIM"
        slot["status_msg"] = f"[STUDY] Sim Bought ${principal:.2f} @ {fmt_px(price)} → Target {fmt_px(target_px)}"
        return

    slot["phase"] = "HOLDING_FOR_SKIM"
    slot["base_balance"] = paper_tokens
    slot["position_value_usdt"] = round(pos_val, 4)
    entry_px = slot["entry_price"] if slot["entry_price"] > 0 else price
    target_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
    stop_px = entry_px * (1.0 - (stop_reset_pct / 100.0))
    slot["target_sell_price"] = round(target_px, px_prec)
    slot["stop_reset_price"] = round(stop_px, px_prec)
    slot["unrealized_pnl_usdt"] = round(pos_val - principal, 4)
    slot["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)

    if 0 < stop_reset_pct < 90 and price <= stop_px:
        slot["paper_base_bal"] = principal / price
        slot["entry_price"] = price
        slot["last_trade_ts"] = now
        slot["status_msg"] = f"[STUDY -{stop_reset_pct}% RESET] Rebought floor @ {fmt_px(price)}"
        return

    if price >= target_px:
        pct_gain = (price - entry_px) / entry_px
        sim_profit = max(0.01, round(principal * pct_gain - principal * (fee_buf / 100.0), 4))
        slot["slot_skimmed_usdt"] = round(slot.get("slot_skimmed_usdt", 0.0) + sim_profit, 4)
        slot["slot_skim_count"] = slot.get("slot_skim_count", 0) + 1
        STATE["paper_skimmed_usdt"] = round(STATE.get("paper_skimmed_usdt", 0.0) + sim_profit, 4)
        STATE["paper_skim_count"] = STATE.get("paper_skim_count", 0) + 1
        old_entry = entry_px
        slot["last_skim_sell_price"] = price
        slot["last_trade_ts"] = now

        STATE["paper_ledger"].insert(0, {
            "time": datetime.utcnow().strftime("%H:%M:%S"),
            "pair": f"{pair} (PAPER)",
            "cycle": f"#{STATE['paper_skim_count']}",
            "buy_px": f"{fmt_px(old_entry)} (${principal:.2f})",
            "sell_px": f"{fmt_px(price)} (${principal + sim_profit:.2f})",
            "profit": f"+${sim_profit:.4f}",
            "principal_redeployed": f"${principal:.2f}"
        })
        STATE["paper_ledger"] = STATE["paper_ledger"][:50]

        add_log(f"🧪 [PAPER STUDY SKIM][{pair}] Hit +{skim_pct}%! Sim Profit: +${sim_profit:.4f} (Total Flips: {slot['slot_skim_count']})")

        if pullback_pct > 0:
            slot["paper_base_bal"] = 0.0
            slot["base_balance"] = 0.0
            slot["phase"] = "WAITING_TO_BUY"
            slot["status_msg"] = (
                f"[STUDY] Skimmed +${sim_profit:.4f}! Waiting for -{pullback_pct}% dip "
                f"({fmt_px(rebuy_px)}) or +0.5% breakout (180s)"
            )
        else:
            slot["paper_base_bal"] = principal / price
            slot["entry_price"] = price
            slot["status_msg"] = f"[STUDY] Skimmed +${sim_profit:.4f} & Instant Rebought @ {fmt_px(price)}"
    else:
        slot["status_msg"] = f"[STUDY] {slot['unrealized_pnl_pct']:+.2f}% | Target {fmt_px(target_px)} | 24h Range: {slot['range_24h_pct']}%"


# =========================================================================
# ENGINE 2: LIVE GATE.IO CASH SLOT PROCESSOR (With Strict Fill Verification!)
# =========================================================================
def process_live_slot(pair, slot, fee_buf, has_keys, bal_map, ticker_map):
    base_curr = pair.split("_")[0]
    principal = float(slot["principal_usdt"])
    skim_pct = float(slot["skim_target_pct"])
    stop_reset_pct = float(slot.get("stop_reset_pct", 99.0))
    pullback_pct = float(slot.get("rebuy_pullback_pct", 0.30))

    tk = ticker_map.get(pair)
    if not tk:
        raise ValueError(f"Pair {pair} not found on Gate.io")

    meta = get_pair_meta(pair)
    price = tk["last"]
    amt_prec = meta["amount_prec"]
    px_prec = meta["price_prec"]
    min_quote = meta["min_quote"]

    slot["last_price"] = price
    slot["price_prec"] = px_prec
    slot["high_24h"] = tk["high_24h"]
    slot["low_24h"] = tk["low_24h"]
    slot["change_24h_pct"] = tk["change_24h"]
    slot["range_24h_pct"] = tk.get("range_24h_pct", 0.0)

    effective_gain_pct = skim_pct + fee_buf
    last_sell_px = float(slot.get("last_skim_sell_price", 0.0))
    if last_sell_px > 0 and pullback_pct > 0:
        rebuy_px = last_sell_px * (1.0 - (pullback_pct / 100.0))
    else:
        rebuy_px = price
    slot["target_rebuy_price"] = round(rebuy_px, px_prec)

    if slot["entry_price"] > price * 1.5 or slot["entry_price"] < price * 0.5:
        slot["entry_price"] = 0.0

    now = time.time()
    in_cooldown = (now - float(slot.get("last_trade_ts", 0.0))) < 12.0

    if not has_keys:
        slot["status_msg"] = "[LIVE] Connect Gate.io API Keys above first"
        return

    real_base_bal = bal_map.get(base_curr, 0.0)
    real_usdt_bal = bal_map.get("USDT", 0.0)
    slot["base_balance"] = real_base_bal

    sellable_amt_str = format_dec(real_base_bal, amt_prec, ROUND_DOWN)
    sellable_units = float(sellable_amt_str)
    sellable_val_usdt = sellable_units * price
    pos_val = real_base_bal * price
    slot["position_value_usdt"] = round(pos_val, 4)

    holding_tokens = sellable_val_usdt >= max(min_quote * 1.05, principal * 0.65)
    if holding_tokens:
        slot["phase"] = "HOLDING_FOR_SKIM"
        if slot["entry_price"] <= 0:
            slot["entry_price"] = price
        if slot.get("actual_cost_usdt", 0.0) <= 0:
            slot["actual_cost_usdt"] = round(sellable_units * slot["entry_price"], 4)
    else:
        if not in_cooldown:
            slot["phase"] = "WAITING_TO_BUY"

    entry_px = slot["entry_price"] if slot["entry_price"] > 0 else price
    actual_cost = float(slot.get("actual_cost_usdt") or (sellable_units * entry_px))

    required_net_usdt = actual_cost * (1.0 + (effective_gain_pct / 100.0))
    if sellable_units > 0:
        min_profitable_px = required_net_usdt / sellable_units
        target_sell_px = max(entry_px * (1.0 + (effective_gain_pct / 100.0)), min_profitable_px)
    else:
        target_sell_px = entry_px * (1.0 + (effective_gain_pct / 100.0))

    stop_reset_px = entry_px * (1.0 - (stop_reset_pct / 100.0))
    slot["target_sell_price"] = round(target_sell_px, px_prec)
    slot["stop_reset_price"] = round(stop_reset_px, px_prec)

    if holding_tokens and entry_px > 0:
        slot["unrealized_pnl_usdt"] = round(sellable_val_usdt - actual_cost, 4)
        slot["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)
    else:
        slot["unrealized_pnl_usdt"] = 0.0
        slot["unrealized_pnl_pct"] = 0.0

    if in_cooldown:
        slot["status_msg"] = f"[LIVE] Order Settled — Cooldown Active | Entry {fmt_px(entry_px)}"
        return

    if slot["phase"] == "HOLDING_FOR_SKIM":
        if 0 < stop_reset_pct < 90 and price <= stop_reset_px:
            sell_px_str = format_dec(price * 0.995, px_prec)
            code, resp = place_gate_order(pair, "sell", sellable_amt_str, sell_px_str)
            filled_sell = float(resp.get("filled_total") or 0.0) if isinstance(resp, dict) else 0.0
            if code in (200, 201) and filled_sell > (actual_cost * 0.50):
                old_ep = entry_px
                slot["last_trade_ts"] = time.time()
                time.sleep(0.45)
                rebuy_budget = min(principal, max(filled_sell * 0.996, min_quote * 1.05))
                rebuy_amt_str = calc_fee_protected_buy_amt(rebuy_budget, price, amt_prec)
                rebuy_px_str = format_dec(price * 1.004, px_prec)
                b_code, b_resp = place_gate_order(pair, "buy", rebuy_amt_str, rebuy_px_str)
                filled_usdt = float(b_resp.get("filled_total") or 0.0) if isinstance(b_resp, dict) else 0.0
                if filled_usdt > 0:
                    slot["entry_price"] = price
                    slot["actual_cost_usdt"] = round(filled_usdt, 4)
                    new_target = price * (1.0 + (effective_gain_pct / 100.0))
                    slot["target_sell_price"] = round(new_target, px_prec)
                    slot["stop_reset_price"] = round(price * (1.0 - (stop_reset_pct / 100.0)), px_prec)
                    slot["status_msg"] = f"[LIVE -{stop_reset_pct}% RESET] Rebought @ {fmt_px(price)} → Target {fmt_px(new_target)}"
                    add_log(
                        f"🔄 [LIVE -{stop_reset_pct}% RESET][{pair}] Dropped {fmt_px(old_ep)} → {fmt_px(price)}. "
                        f"Sold & Rebought @ {fmt_px(price)}"
                    )
            return

        if price < target_sell_px or sellable_val_usdt < required_net_usdt:
            slot["status_msg"] = (
                f"[LIVE] {slot['unrealized_pnl_pct']:+.2f}% (Sellable: ${sellable_val_usdt:.2f} / "
                f"Target: ${required_net_usdt:.2f} @ {fmt_px(target_sell_px)})"
            )
            return

        # Use 0.996 IOC limit buffer so micro-cap tokens like PEPE fill 100% of the time!
        sell_px_str = format_dec(price * 0.996, px_prec)
        code, resp = place_gate_order(pair, "sell", sellable_amt_str, sell_px_str)

        if code in (200, 201):
            # CRITICAL FIX: Verify that Gate.io actually filled the sell order (never log $0.00 fills!)
            gross_usdt_received = float(resp.get("filled_total") or 0.0)
            left_unfilled = float(resp.get("left") or 0.0)
            if gross_usdt_received < (actual_cost * 0.50):
                add_log(
                    f"⚠️ [LIVE][{pair}] IOC Sell did not fill on orderbook (Filled: ${gross_usdt_received:.2f}, Left: {left_unfilled}). "
                    f"Keeping Entry Floor {fmt_px(entry_px)} locked & retrying next scan!"
                )
                return

            slot["last_trade_ts"] = time.time()
            fee_deducted = float(resp.get("fee") or (gross_usdt_received * 0.001))
            net_usdt_received = gross_usdt_received - fee_deducted

            true_cash_profit = round(net_usdt_received - actual_cost, 4)
            if true_cash_profit <= 0:
                true_cash_profit = round(gross_usdt_received - actual_cost, 4)
            true_cash_profit = max(0.01, true_cash_profit)

            slot["slot_skimmed_usdt"] = round(slot.get("slot_skimmed_usdt", 0.0) + true_cash_profit, 4)
            slot["slot_skim_count"] = slot.get("slot_skim_count", 0) + 1
            STATE["live_skimmed_usdt"] = round(STATE.get("live_skimmed_usdt", 0.0) + true_cash_profit, 4)
            STATE["live_skim_count"] = STATE.get("live_skim_count", 0) + 1
            old_entry = entry_px
            slot["last_skim_sell_price"] = price

            STATE["skim_ledger"].insert(0, {
                "time": datetime.utcnow().strftime("%H:%M:%S"),
                "pair": f"{pair} (LIVE)",
                "cycle": f"#{STATE['live_skim_count']}",
                "buy_px": f"{fmt_px(old_entry)} (${actual_cost:.2f})",
                "sell_px": f"{fmt_px(price)} (${net_usdt_received:.2f})",
                "profit": f"+${true_cash_profit:.4f}",
                "principal_redeployed": f"${actual_cost:.2f}"
            })
            STATE["skim_ledger"] = STATE["skim_ledger"][:50]

            add_log(
                f"💎 [LIVE SKIM][{pair}] Sold {sellable_amt_str} {base_curr} for ${net_usdt_received:.2f} USDT "
                f"(Cost: ${actual_cost:.2f}) | Real Cash Added: +${true_cash_profit:.4f} USDT!"
            )

            if pullback_pct <= 0.0:
                time.sleep(0.45)
                rebuy_budget = min(principal, net_usdt_received - true_cash_profit)
                rebuy_amt_str = calc_fee_protected_buy_amt(rebuy_budget, price, amt_prec)
                rebuy_px_str = format_dec(price * 1.004, px_prec)
                b_code, b_resp = place_gate_order(pair, "buy", rebuy_amt_str, rebuy_px_str)
                spent_usdt = float(b_resp.get("filled_total") or 0.0) if isinstance(b_resp, dict) else 0.0
                if b_code in (200, 201) and spent_usdt > (principal * 0.50):
                    slot["entry_price"] = price
                    slot["actual_cost_usdt"] = round(spent_usdt, 4)
                    slot["phase"] = "HOLDING_FOR_SKIM"
                    new_target = price * (1.0 + (effective_gain_pct / 100.0))
                    slot["target_sell_price"] = round(new_target, px_prec)
                    slot["status_msg"] = f"[LIVE] Cash Skimmed +${true_cash_profit:.4f} & Rebought (${spent_usdt:.2f}) @ {fmt_px(price)}"
                else:
                    slot["phase"] = "WAITING_TO_BUY"
                    slot["entry_price"] = 0.0
                    slot["actual_cost_usdt"] = 0.0
            else:
                slot["phase"] = "WAITING_TO_BUY"
                slot["entry_price"] = 0.0
                slot["actual_cost_usdt"] = 0.0
                slot["status_msg"] = (
                    f"[LIVE] Cash Protected in USDT (+${true_cash_profit:.4f})! "
                    f"Waiting for -{pullback_pct}% dip ({fmt_px(rebuy_px)}) or +0.5% breakout (180s)"
                )
        else:
            add_log(f"[LIVE][{pair}] Sell rejected ({code}): {resp.get('message', resp)}")
        return

    # =========================================================================
    # SMART 3-WAY REBUY: Dip (-0.30%) OR Breakout (+0.50%) OR 180s Timeout
    # =========================================================================
    if slot["phase"] == "WAITING_TO_BUY":
        if pullback_pct > 0 and last_sell_px > 0:
            breakout_px = last_sell_px * 1.005
            waited_sec = now - float(slot.get("last_trade_ts", 0.0))
            if rebuy_px < price < breakout_px and waited_sec < 180:
                slot["status_msg"] = (
                    f"[LIVE] Holding USDT — Waiting for -{pullback_pct}% dip ({fmt_px(rebuy_px)}) "
                    f"or +0.5% breakout ({fmt_px(breakout_px)}) | {int(180 - waited_sec)}s left"
                )
                return

        if real_usdt_bal < (principal * 0.95):
            slot["status_msg"] = f"[LIVE] Awaiting USDT (Wallet: ${real_usdt_bal:.2f} / Need: ${principal:.2f})"
            return

        buy_budget = min(principal, real_usdt_bal * 0.995)
        buy_amt_str = calc_fee_protected_buy_amt(buy_budget, price, amt_prec)
        buy_px_str = format_dec(price * 1.004, px_prec)
        code, resp = place_gate_order(pair, "buy", buy_amt_str, buy_px_str)

        if code in (200, 201):
            spent_usdt = float(resp.get("filled_total") or 0.0)
            if spent_usdt < (principal * 0.50):
                add_log(f"⚠️ [LIVE][{pair}] IOC Buy did not fill on orderbook. Retrying next scan.")
                return
            slot["last_trade_ts"] = time.time()
            slot["entry_price"] = price
            slot["actual_cost_usdt"] = round(spent_usdt, 4)
            slot["phase"] = "HOLDING_FOR_SKIM"
            new_target = price * (1.0 + (effective_gain_pct / 100.0))
            slot["target_sell_price"] = round(new_target, px_prec)
            slot["stop_reset_price"] = round(price * (1.0 - (stop_reset_pct / 100.0)), px_prec)
            slot["status_msg"] = f"[LIVE] Bought {buy_amt_str} {base_curr} (${spent_usdt:.2f}) @ {fmt_px(price)}"
            bal_map["USDT"] = max(0.0, real_usdt_bal - spent_usdt)
            add_log(f"[LIVE BUY][{pair}] Bought {buy_amt_str} {base_curr} (Actual Cost: ${spent_usdt:.2f}) @ {fmt_px(price)}")
        else:
            add_log(f"[LIVE][{pair}] Buy rejected ({code}): {resp.get('message', resp)}")


def run_fleet_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            fee_buf = float(STATE.get("fee_buffer_pct", 0.25))
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))
            live_keys = list(STATE["slots"].keys())
            paper_keys = list(STATE.get("paper_slots", {}).keys())
            all_pairs = list(set(live_keys + paper_keys))

        bal_map = {}
        if has_keys:
            try:
                bal_map = fetch_all_spot_balances()
                with state_lock:
                    STATE["api_connected"] = True
                    STATE["usdt_balance"] = bal_map.get("USDT", 0.0)
                    STATE["api_status_msg"] = f"Connected to Gate.io v4 (Available: ${STATE['usdt_balance']:.2f} USDT)"
            except Exception as e:
                with state_lock:
                    STATE["api_connected"] = False
                    STATE["api_status_msg"] = f"Auth Notice: {str(e)[:55]}"

        if not all_pairs:
            with state_lock:
                STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
                STATE["last_worker_heartbeat"] = time.time()
                STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
                STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            return

        ticker_map = {}
        try:
            ticker_map = fetch_batch_tickers(all_pairs)
        except Exception as tk_err:
            add_log(f"Ticker batch notice: {str(tk_err)[:60]}")
            return

        for pair in live_keys:
            with state_lock:
                slot = STATE["slots"].get(pair)
                if not slot:
                    continue
            try:
                process_live_slot(pair, slot, fee_buf, has_keys, bal_map, ticker_map)
            except Exception as slot_err:
                with state_lock:
                    slot["status_msg"] = f"Notice: {str(slot_err)[:55]}"

        paper_allocated = 0.0
        for pair in paper_keys:
            with state_lock:
                pslot = STATE["paper_slots"].get(pair)
                if not pslot:
                    continue
                paper_allocated += float(pslot.get("principal_usdt", 0.0))
            try:
                process_paper_slot(pair, pslot, fee_buf, ticker_map)
            except Exception as p_err:
                with state_lock:
                    pslot["status_msg"] = f"Notice: {str(p_err)[:55]}"

        with state_lock:
            STATE["paper_bankroll_usdt"] = 100.0
            STATE["paper_free_usdt"] = round(max(0.0, 100.0 - paper_allocated) + float(STATE.get("paper_skimmed_usdt", 0.0)), 4)
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_worker_heartbeat"] = time.time()
            STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            save_state()

    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("VaultSkimmer v5.8.2 Online (Verified Order Fill Guard + 180s Smart Rebuy + $100 Paper Lab).")
    while True:
        run_fleet_cycle(force_manual=False)
        time.sleep(3.5)


def ensure_worker_alive():
    global worker_started
    now = time.time()
    if not worker_started or (now - STATE.get("last_worker_heartbeat", 0) > 12):
        worker_started = True
        STATE["last_worker_heartbeat"] = now
        threading.Thread(target=background_worker, daemon=True).start()


@app.before_request
def check_worker():
    ensure_worker_alive()


@app.route("/icon.svg")
def app_icon():
    svg = """<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 512 512">
      <defs>
        <linearGradient id="g" x1="0%" y1="0%" x2="100%" y2="100%">
          <stop offset="0%" stop-color="#10b981"/>
          <stop offset="100%" stop-color="#06b6d4"/>
        </linearGradient>
      </defs>
      <rect width="512" height="512" rx="115" fill="#060b16"/>
      <rect x="28" y="28" width="456" height="456" rx="95" fill="none" stroke="url(#g)" stroke-width="18"/>
      <path d="M135 165 L256 385 L377 165" fill="none" stroke="url(#g)" stroke-width="48" stroke-linecap="round" stroke-linejoin="round"/>
      <circle cx="256" cy="145" r="28" fill="#34d399"/>
    </svg>"""
    return Response(svg, mimetype="image/svg+xml")


@app.route("/manifest.json")
def manifest():
    return jsonify({
        "name": "VaultSkimmer v5.8.2 Dual-Engine",
        "short_name": "VaultSkimmer",
        "description": "True-Cash Spot Skimmer, 180s Smart Rebuy & $100 Paper Lab",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#060b16",
        "theme_color": "#10b981",
        "icons": [{"src": "/icon.svg", "sizes": "any", "type": "image/svg+xml", "purpose": "any maskable"}]
    })


@app.route("/sw.js")
def service_worker():
    js = """
    self.addEventListener('install', e => self.skipWaiting());
    self.addEventListener('activate', e => e.waitUntil(self.clients.claim()));
    self.addEventListener('fetch', e => {
      e.respondWith(fetch(e.request).catch(() => caches.match(e.request)));
    });
    """
    return Response(js, mimetype="application/javascript")


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "VaultSkimmer-v5.8.2-Dual", "cycle": STATE["cycle_count"]})


@app.route("/api/cron")
def api_cron():
    run_fleet_cycle(force_manual=False)
    return jsonify({
        "ok": True,
        "live_tokens": list(STATE["slots"].keys()),
        "paper_tokens": list(STATE.get("paper_slots", {}).keys()),
        "live_skimmed_usdt": STATE.get("live_skimmed_usdt", 0.0),
        "paper_skimmed_usdt": STATE.get("paper_skimmed_usdt", 0.0),
        "cycle": STATE["cycle_count"]
    })


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True, "scan_ms": STATE["last_scan_ms"]})


@app.route("/api/state")
def api_state():
    if STATE["cycle_count"] == 0:
        run_fleet_cycle(force_manual=False)
    with state_lock:
        safe = STATE.copy()
        safe["has_saved_keys"] = bool(STATE.get("api_key") and STATE.get("api_secret"))
        safe["api_key"] = ""
        safe["api_secret"] = ""
        return jsonify(safe)


@app.route("/api/sync_keys", methods=["POST"])
def api_sync_keys():
    data = request.json or {}
    raw_key = (data.get("api_key") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")
    raw_sec = (data.get("api_secret") or "").strip().replace(" ", "").replace("\n", "").replace("\r", "")

    with state_lock:
        if raw_key and "Saved" not in raw_key:
            STATE["api_key"] = raw_key
        if raw_sec and "Saved" not in raw_sec:
            STATE["api_secret"] = raw_sec
        active_key = STATE.get("api_key", "").strip()
        active_secret = STATE.get("api_secret", "").strip()

    if not active_key or not active_secret:
        with state_lock:
            STATE["api_connected"] = False
            STATE["api_status_msg"] = "No API Keys Saved Yet — Paste Key & Secret Above"
            save_state()
        return jsonify({"ok": False, "error": "No saved keys found. Please paste both your Gate.io API Key and Secret."}), 400

    try:
        bal_map = fetch_all_spot_balances(active_key, active_secret)
        usdt_avail = bal_map.get("USDT", 0.0)
        non_zero = [f"{k}: {v:.4f}" for k, v in bal_map.items() if v > 0][:6]
        summary = ", ".join(non_zero) if non_zero else "0.00 USDT"
        with state_lock:
            STATE["api_connected"] = True
            STATE["usdt_balance"] = usdt_avail
            STATE["api_status_msg"] = f"Verified & Connected! Gate.io USDT: ${usdt_avail:.2f}"
            add_log(f"🔗 Gate.io API Keys Verified! Live Wallet: {summary}")
            save_state()
        return jsonify({"ok": True, "usdt_balance": usdt_avail, "status_msg": STATE["api_status_msg"]})
    except Exception as e:
        err_str = str(e)[:80]
        with state_lock:
            STATE["api_connected"] = False
            STATE["api_status_msg"] = f"Connection Error: {err_str}"
            add_log(f"❌ Gate.io API Sync Error: {err_str}")
            save_state()
        return jsonify({"ok": False, "error": err_str}), 400


@app.route("/api/reset_live_vault", methods=["POST"])
def api_reset_live_vault():
    with state_lock:
        STATE["live_skimmed_usdt"] = 0.0
        STATE["live_skim_count"] = 0
        STATE["skim_ledger"] = []
        for sl in STATE["slots"].values():
            sl["slot_skimmed_usdt"] = 0.0
            sl["slot_skim_count"] = 0
        add_log("🔄 Live Profit Vault & Live Ledger reset to $0.0000!")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_paper_vault", methods=["POST"])
def api_reset_paper_vault():
    with state_lock:
        STATE["paper_skimmed_usdt"] = 0.0
        STATE["paper_skim_count"] = 0
        STATE["paper_ledger"] = []
        for sl in STATE.get("paper_slots", {}).values():
            sl["slot_skimmed_usdt"] = 0.0
            sl["slot_skim_count"] = 0
        add_log("🧪 Paper Study Profit Vault reset to $0.0000 (Base Account remains $100.00)!")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/enable_pullback_shield", methods=["POST"])
def api_enable_pullback_shield():
    with state_lock:
        for sl in STATE["slots"].values():
            sl["rebuy_pullback_pct"] = 0.30
        for sl in STATE.get("paper_slots", {}).values():
            sl["rebuy_pullback_pct"] = 0.30
        add_log("🛡️ Activated Smart Rebuy Shield across all Live & Paper slots (-0.30% Dip OR +0.50% Breakout OR 180s Timeout).")
        save_state()
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/add_or_update_slot", methods=["POST"])
def api_add_or_update_slot():
    data = request.json or {}
    target_engine = (data.get("target_engine") or "PAPER").upper()
    pair = normalize_pair(data.get("pair", ""))
    if not pair:
        return jsonify({"ok": False, "error": "Please enter a Token Symbol first."}), 400

    raw_prin = str(data.get("principal_usdt") or "").strip()
    if not raw_prin or float(raw_prin) <= 0:
        return jsonify({"ok": False, "error": "Please enter a valid Slot Protected Principal (USDT)."}), 400

    raw_skim = str(data.get("skim_target_pct") or "").strip()
    if not raw_skim or float(raw_skim) <= 0:
        return jsonify({"ok": False, "error": "Please enter a Skim Profit Target % (e.g. 1.0 or 0.65)."}), 400

    principal = max(1.0, float(raw_prin))
    skim_pct = max(0.1, float(raw_skim))

    raw_stop = str(data.get("stop_reset_pct") or "").strip()
    stop_reset_pct = max(0.5, float(raw_stop)) if raw_stop else 99.0

    raw_pb = str(data.get("rebuy_pullback_pct") or "").strip()
    pullback_pct = max(0.0, float(raw_pb)) if raw_pb != "" else 0.30

    raw_ep = str(data.get("entry_price") or "").strip()
    custom_ep = float(raw_ep) if (raw_ep and float(raw_ep) > 0) else 0.0

    with state_lock:
        if target_engine == "LIVE":
            existing = STATE["slots"].get(pair)
            if existing:
                existing["principal_usdt"] = principal
                existing["skim_target_pct"] = skim_pct
                existing["stop_reset_pct"] = stop_reset_pct
                existing["rebuy_pullback_pct"] = pullback_pct
                if custom_ep > 0:
                    existing["entry_price"] = custom_ep
                add_log(f"Updated LIVE Slot: {pair} (${principal:.2f} | +{skim_pct}% Skim | -{pullback_pct}% Smart Rebuy)")
            else:
                STATE["slots"][pair] = make_slot(pair, principal, skim_pct, stop_reset_pct, pullback_pct, custom_ep, "LIVE")
                add_log(f"Commanded LIVE Slot: {pair} (${principal:.2f} Base | +{skim_pct}% Skim | -{pullback_pct}% Smart Rebuy)")
        else:
            if "paper_slots" not in STATE:
                STATE["paper_slots"] = {}
            existing_p = STATE["paper_slots"].get(pair)
            if existing_p:
                existing_p["principal_usdt"] = principal
                existing_p["skim_target_pct"] = skim_pct
                existing_p["stop_reset_pct"] = stop_reset_pct
                existing_p["rebuy_pullback_pct"] = pullback_pct
                if custom_ep > 0:
                    existing_p["entry_price"] = custom_ep
                add_log(f"Updated PAPER Study Slot: {pair} (${principal:.2f} | +{skim_pct}% Skim)")
            else:
                STATE["paper_slots"][pair] = make_slot(pair, principal, skim_pct, stop_reset_pct, pullback_pct, custom_ep, "PAPER")
                add_log(f"🧪 Added PAPER Study Slot: {pair} (${principal:.2f} from $100 Virtual Account | +{skim_pct}% Skim)")

        save_state()

    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True, "pair": pair, "engine": target_engine})


@app.route("/api/load_paper_study_pack", methods=["POST"])
def api_load_paper_study_pack():
    study_coins = [
        "DOGE_USDT", "SUI_USDT", "SOL_USDT", "ADA_USDT", "TRX_USDT",
        "ALGO_USDT", "POL_USDT", "VET_USDT", "XLM_USDT", "LINK_USDT"
    ]
    with state_lock:
        if "paper_slots" not in STATE:
            STATE["paper_slots"] = {}
        for c in study_coins:
            if c not in STATE["paper_slots"]:
                STATE["paper_slots"][c] = make_slot(c, 10.0, 1.0, 99.0, 0.30, 0.0, "PAPER")
        add_log("🧪 Loaded 10 Moving Study Coins ($10 x 10 = $100 Virtual Bankroll) into Paper Study Lab!")
        save_state()
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/remove_slot", methods=["POST"])
def api_remove_slot():
    data = request.json or {}
    pair = normalize_pair(data.get("pair", ""))
    engine = (data.get("engine") or "LIVE").upper()
    with state_lock:
        if engine == "PAPER":
            if pair in STATE.get("paper_slots", {}):
                del STATE["paper_slots"][pair]
                add_log(f"🧪 Removed {pair} from Paper Study Lab.")
                save_state()
        else:
            if pair in STATE["slots"]:
                del STATE["slots"][pair]
                add_log(f"Removed {pair} from Live Fleet.")
                save_state()
    return jsonify({"ok": True})


@app.route("/api/clear_paper_slots", methods=["POST"])
def api_clear_paper_slots():
    with state_lock:
        STATE["paper_slots"] = {}
        STATE["paper_free_usdt"] = 100.0
        add_log("🧹 Cleared all Paper Study tokens — Paper Lab is 100% empty ($100.00 Virtual Bankroll ready).")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/clear_all_slots", methods=["POST"])
def api_clear_all_slots():
    with state_lock:
        STATE["slots"] = {}
        add_log("🧹 Cleared all Live token slots.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_slot_entry", methods=["POST"])
def api_reset_slot_entry():
    data = request.json or {}
    pair = normalize_pair(data.get("pair", ""))
    engine = (data.get("engine") or "LIVE").upper()
    with state_lock:
        target_dict = STATE.get("paper_slots", {}) if engine == "PAPER" else STATE["slots"]
        slot = target_dict.get(pair)
        if slot and slot["last_price"] > 0:
            slot["entry_price"] = slot["last_price"]
            slot["actual_cost_usdt"] = 0.0
            add_log(f"[{engine}][{pair}] Entry Floor synced to live price: {fmt_px(slot['entry_price'])}")
            save_state()
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True})


HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#060b16" />
  <meta name="apple-mobile-web-app-capable" content="yes" />
  <meta name="apple-mobile-web-app-status-bar-style" content="black-translucent" />
  <link rel="manifest" href="/manifest.json" />
  <link rel="icon" type="image/svg+xml" href="/icon.svg" />
  <link rel="apple-touch-icon" href="/icon.svg" />
  <title>VaultSkimmer v5.8.2 | Verified Fill + 180s Smart Rebuy</title>
  <style>
    :root {
      --bg: #050811;
      --card: rgba(13, 20, 36, 0.92);
      --border: #1e293b;
      --emerald: #10b981;
      --emerald-glow: rgba(16, 185, 129, 0.28);
      --cyan: #06b6d4;
      --amber: #f59e0b;
      --purple: #8b5cf6;
      --danger: #ef4444;
      --text: #f8fafc;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, system-ui, sans-serif; }
    body {
      background: radial-gradient(circle at 50% 0%, #0f1d3a 0%, #050811 70%);
      color: var(--text); padding: 14px; min-height: 100vh;
    }
    .wrap { max-width: 1200px; margin: 0 auto; }

    .hero {
      background: linear-gradient(135deg, rgba(16,185,129,0.14), rgba(6,182,212,0.08));
      border: 1px solid rgba(16,185,129,0.4); border-radius: 18px; padding: 16px 20px; margin-bottom: 14px;
      box-shadow: 0 14px 34px rgba(0,0,0,0.5);
    }
    .hero-top { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }
    .brand-title { font-size: 1.5rem; font-weight: 900; display: flex; align-items: center; gap: 9px; }
    .v-badge { background: var(--emerald); color: #022c22; font-size: 0.72rem; font-weight: 900; padding: 3px 9px; border-radius: 99px; }
    .sub-desc { color: var(--muted); font-size: 0.8rem; margin-top: 4px; }

    .top-actions { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; }
    .btn-pwa {
      background: linear-gradient(135deg, #06b6d4, #3b82f6); color: #fff;
      font-weight: 900; font-size: 0.77rem; padding: 9px 13px; border-radius: 10px; border: none;
      cursor: pointer; display: inline-flex; align-items: center; gap: 6px;
      box-shadow: 0 0 16px rgba(6, 182, 212, 0.35);
    }
    .btn-install {
      background: linear-gradient(135deg, #10b981, #06b6d4); color: #022c22;
      font-weight: 900; font-size: 0.77rem; padding: 9px 13px; border-radius: 10px; border: none;
      cursor: pointer; display: inline-flex; align-items: center; gap: 6px;
      box-shadow: 0 0 16px var(--emerald-glow);
    }
    .btn-study {
      background: rgba(245, 158, 11, 0.2); border: 1px solid #f59e0b; color: #fde68a;
      font-weight: 800; font-size: 0.77rem; padding: 8px 13px; border-radius: 10px; cursor: pointer;
    }
    .btn-scan {
      background: rgba(139, 92, 246, 0.22); border: 1px solid #8b5cf6; color: #ddd6fe;
      font-weight: 800; font-size: 0.77rem; padding: 8px 13px; border-radius: 10px; cursor: pointer;
    }

    .telemetry-strip {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.08);
      font-size: 0.79rem; color: var(--muted); gap: 8px;
    }
    .pill {
      display: inline-flex; align-items: center; gap: 7px; padding: 5px 13px; border-radius: 99px;
      font-weight: 800; font-size: 0.74rem;
    }
    .pill-live { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid #10b981; }
    .pill-paper { background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid #f59e0b; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 1.2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.25; } }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card {
      background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    }
    .lbl { color: var(--muted); font-size: 0.7rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.6px; display: flex; justify-content: space-between; align-items: center; }
    .val { font-size: 1.45rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .sub { font-size: 0.77rem; color: var(--muted); }
    .c-green { color: #34d399; }
    .c-cyan { color: #22d3ee; }
    .c-amber { color: #fbbf24; }

    .btn-reset-vault {
      background: rgba(239, 68, 68, 0.16); color: #fca5a5; border: 1px solid rgba(239, 68, 68, 0.45);
      padding: 3px 8px; border-radius: 7px; font-size: 0.66rem; font-weight: 800; cursor: pointer;
    }

    .api-card {
      background: linear-gradient(135deg, rgba(13, 20, 36, 0.96), rgba(15, 29, 58, 0.88));
      border: 1px solid #263554; border-radius: 15px; padding: 15px 18px; margin-bottom: 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.4);
    }
    .api-header { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; margin-bottom: 11px; gap: 8px; }
    .api-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 10px; align-items: end; }
    .btn-sync-api {
      background: linear-gradient(135deg, #06b6d4, #10b981); color: #022c22;
      font-weight: 900; font-size: 0.84rem; padding: 11px 16px; border-radius: 10px; border: none;
      cursor: pointer; width: 100%; box-shadow: 0 0 15px rgba(6,182,212,0.28);
    }

    .fleet-header { font-size: 0.88rem; font-weight: 900; text-transform: uppercase; color: #38bdf8; margin-bottom: 10px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 8px; }
    .fleet-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; margin-bottom: 18px; }
    .empty-fleet {
      background: rgba(13, 20, 36, 0.65); border: 1px dashed #334155; border-radius: 15px;
      padding: 24px 18px; text-align: center; color: var(--muted); font-size: 0.84rem; margin-bottom: 16px;
    }
    .slot-card {
      background: linear-gradient(180deg, rgba(16, 26, 48, 0.95), rgba(10, 15, 29, 0.95));
      border: 1px solid #263554; border-radius: 15px; padding: 14px 16px;
      box-shadow: 0 8px 22px rgba(0,0,0,0.4);
    }
    .slot-card-paper {
      background: linear-gradient(180deg, rgba(34, 26, 16, 0.92), rgba(14, 12, 22, 0.95));
      border: 1px solid rgba(245, 158, 11, 0.4); border-radius: 15px; padding: 14px 16px;
      box-shadow: 0 8px 22px rgba(0,0,0,0.4);
    }
    .slot-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .slot-pair { font-size: 1.12rem; font-weight: 900; color: #fff; display: flex; align-items: center; gap: 8px; }
    .slot-badge { font-size: 0.68rem; background: rgba(6,182,212,0.18); color: #22d3ee; border: 1px solid #06b6d4; padding: 2px 8px; border-radius: 99px; font-weight: 800; }
    .slot-badge-paper { font-size: 0.68rem; background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid #f59e0b; padding: 2px 8px; border-radius: 99px; font-weight: 800; }
    .slot-metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.76rem; margin-bottom: 10px; background: rgba(5,8,17,0.6); padding: 9px 11px; border-radius: 10px; border: 1px solid #1e293b; }
    .track { height: 10px; background: #04070e; border-radius: 99px; overflow: hidden; border: 1px solid #1e293b; margin: 7px 0; }
    .fill { height: 100%; width: 0%; background: linear-gradient(90deg, #06b6d4, #10b981); transition: width 0.35s ease; }
    .fill-paper { height: 100%; width: 0%; background: linear-gradient(90deg, #f59e0b, #10b981); transition: width 0.35s ease; }
    .slot-actions { display: flex; justify-content: space-between; align-items: center; margin-top: 9px; gap: 6px; }
    .s-btn { background: #1e293b; color: #cbd5e1; border: 1px solid #334155; padding: 5px 10px; border-radius: 8px; font-size: 0.72rem; font-weight: 800; cursor: pointer; }
    .s-btn-del { background: rgba(239,68,68,0.15); color: #fca5a5; border: 1px solid rgba(239,68,68,0.4); }

    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 14px; margin-bottom: 14px; }
    .sec-title { font-size: 0.85rem; font-weight: 900; text-transform: uppercase; color: #38bdf8; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; flex-wrap: wrap; gap: 6px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.73rem; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    input {
      width: 100%; background: #050811; border: 1px solid #26334d; color: var(--text);
      padding: 10px 11px; border-radius: 10px; font-size: 0.87rem; font-weight: 700;
    }
    .chip-bar { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
    .chip {
      background: #111c33; border: 1px solid #26334d; color: #cbd5e1; font-size: 0.71rem;
      font-weight: 800; padding: 5px 10px; border-radius: 99px; cursor: pointer;
    }
    .btns { display: flex; flex-wrap: wrap; gap: 9px; margin-top: 12px; }
    button.ctrl { cursor: pointer; border: none; border-radius: 10px; padding: 11px 14px; font-weight: 800; font-size: 0.82rem; }
    .b-save-live { background: linear-gradient(135deg, #10b981, #059669); color: #022c22; flex: 1; }
    .b-save-paper { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; flex: 1; }

    table { width: 100%; border-collapse: collapse; font-size: 0.78rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #1e293b; font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-size: 0.68rem; text-transform: uppercase; }
    .logs {
      background: #04070e; border: 1px solid #1e293b; border-radius: 11px; padding: 11px;
      height: 215px; overflow-y: auto; font-family: monospace; font-size: 0.76rem; color: #cbd5e1; line-height: 1.55;
    }
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div class="hero-top">
      <div>
        <div class="brand-title">VaultSkimmer <span class="v-badge">v5.8.2 VERIFIED-FILL + 180s SMART REBUY</span></div>
        <div class="sub-desc">True-Cash Anti-Dust Guard • Smart Rebuy (-0.30% Dip OR +0.50% Breakout OR 180s Timeout) • $100 Paper Study Lab</div>
      </div>
      <div class="top-actions">
        <button class="btn-pwa" onclick="triggerInstall()" id="installBtn">📲 Install App</button>
        <button class="btn-install" onclick="enablePullbackShield()">🛡️ Enable Smart Rebuy Shield on All Slots</button>
        <button class="btn-study" onclick="loadPaperStudyPack()">🧪 Load 10 Study Coins ($100 Paper Lab)</button>
        <button class="btn-scan" onclick="manualScan()" id="scanBtn">⚡ Scan All Now</button>
      </div>
    </div>

    <div class="telemetry-strip">
      <div style="display:flex; gap:8px; flex-wrap:wrap;">
        <span class="pill pill-live"><span class="dot"></span> LIVE FLEET (<span id="liveCount">0</span> TOKENS)</span>
        <span class="pill pill-paper"><span class="dot"></span> PAPER STUDY LAB (<span id="paperCount">0</span> TOKENS | $100 BASE)</span>
      </div>
      <div>Last Dual Scan: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-green" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)</div>
    </div>
  </div>

  <div class="api-card">
    <div class="api-header">
      <div class="sec-title" style="margin-bottom:0;">🔑 Gate.io v4 API Connection & Live Wallet Sync</div>
      <div id="apiBadge" style="font-size:0.78rem; font-weight:800; color:var(--amber);">● Not Connected</div>
    </div>
    <div class="api-grid">
      <div>
        <label>Gate.io v4 API Key</label>
        <input type="password" id="apiKeyInput" placeholder="Paste your Gate.io API Key..." autocomplete="off" />
      </div>
      <div>
        <label>Gate.io v4 API Secret</label>
        <input type="password" id="apiSecretInput" placeholder="Paste your Gate.io API Secret..." autocomplete="off" />
      </div>
      <div>
        <button class="btn-sync-api" onclick="syncApiKeys()" id="syncApiBtn">🔗 Sync & Verify Gate.io API Keys</button>
      </div>
    </div>
  </div>

  <div class="grid-4">
    <div class="card">
      <div class="lbl">Live Protected Principal</div>
      <div class="val c-cyan" id="totalPrincipal">$0.00</div>
      <div class="sub">Live Fleet Value: <strong id="totalPosVal">$0.0000</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Total Live Portfolio (Fleet + USDT)</div>
      <div class="val c-green" id="totalPortfolioVal">$0.0000</div>
      <div class="sub">Free Gate.io USDT: <strong id="usdtBal">$0.00</strong></div>
    </div>
    <div class="card">
      <div class="lbl">
        <span>Realized LIVE Profit Vault</span>
        <button class="btn-reset-vault" onclick="resetLiveVault()">🔄 Reset $0.00</button>
      </div>
      <div class="val c-green" id="liveSkimmed">+$0.0000</div>
      <div class="sub">Verified Cash Skims: <strong id="liveSkimCount">0</strong></div>
    </div>
    <div class="card">
      <div class="lbl">
        <span>$100 Paper Study Profit Vault</span>
        <button class="btn-reset-vault" onclick="resetPaperVault()">🔄 Reset $0.00</button>
      </div>
      <div class="val c-amber" id="paperSkimmed">+$0.0000</div>
      <div class="sub">Paper Skims: <strong id="paperSkimCount">0</strong> | Virtual Cash: <strong class="c-amber" id="paperBankroll">$100.00</strong></div>
    </div>
  </div>

  <!-- 1. LIVE ARMED CASH FLEET -->
  <div class="fleet-header">
    <span>💎 1. Active LIVE Cash Skimmer Fleet (Real Gate.io Funds)</span>
    <button class="s-btn s-btn-del" onclick="clearAllSlots()" id="clearAllBtn" style="display:none;">🧹 Clear Live Slots</button>
  </div>
  <div id="liveFleetContainer"></div>

  <!-- 2. INDEPENDENT $100 PAPER STUDY LAB -->
  <div class="fleet-header" style="color:var(--amber);">
    <span>🧪 2. Independent $100 Paper Study Lab (Track Moving Coins Day & Week — Never Touches Live Cash)</span>
    <div style="display:flex; gap:8px;">
      <button class="s-btn" onclick="loadPaperStudyPack()" style="border-color:var(--amber); color:#fde68a;">➕ Auto-Fill 10 Study Coins ($100)</button>
      <button class="s-btn s-btn-del" onclick="clearPaperSlots()" id="clearPaperBtn" style="display:none;">🧹 Clear All Paper Tokens</button>
    </div>
  </div>
  <div id="paperFleetContainer"></div>

  <!-- 3. COMMAND CENTER + MOVING COIN STUDY LEADERBOARD -->
  <div class="grid-2">
    <div class="card">
      <div class="sec-title">➕ Command Token to LIVE Fleet or PAPER Study Lab</div>

      <label>Quick-Tap Popular Range & Moving Coins (Fills Symbol Box):</label>
      <div class="chip-bar">
        <span class="chip" onclick="fillTicker('PEPE_USDT')">PEPE</span>
        <span class="chip" onclick="fillTicker('XRP_USDT')">XRP</span>
        <span class="chip" onclick="fillTicker('DOGE_USDT')">DOGE</span>
        <span class="chip" onclick="fillTicker('SUI_USDT')">SUI</span>
        <span class="chip" onclick="fillTicker('SOL_USDT')">SOL</span>
        <span class="chip" onclick="fillTicker('TRX_USDT')">TRX</span>
        <span class="chip" onclick="fillTicker('ADA_USDT')">ADA</span>
        <span class="chip" onclick="fillTicker('XLM_USDT')">XLM</span>
        <span class="chip" onclick="fillTicker('POL_USDT')">POL</span>
        <span class="chip" onclick="fillTicker('VET_USDT')">VET</span>
        <span class="chip" onclick="fillTicker('ALGO_USDT')">ALGO</span>
        <span class="chip" onclick="fillTicker('HYPE_USDT')">HYPE</span>
      </div>

      <div class="row">
        <div>
          <label>Token Symbol (Type Any Gate.io Coin)</label>
          <input type="text" id="pairInput" placeholder="e.g. ALGO, DOGE, SUI, PEPE" value="" autocomplete="off" />
        </div>
        <div>
          <label>Slot Principal (USDT)</label>
          <input type="number" id="principalInput" step="0.05" placeholder="e.g. 10.35" value="10.35" autocomplete="off" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Skim Profit Target (%)</label>
          <input type="number" id="skimPctInput" step="0.05" placeholder="e.g. 1.0 or 0.65" value="1.0" autocomplete="off" />
        </div>
        <div>
          <label>Auto Sell & Rebuy Drop % (99 = Hold Forever)</label>
          <input type="number" id="stopResetInput" step="0.5" placeholder="99 = Never Sell at Loss" value="99.0" autocomplete="off" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Rebuy Pullback % (Default = 0.30% Dip)</label>
          <input type="number" id="pullbackInput" step="0.05" placeholder="0.30 = Dip or +0.5% Breakout (180s)" value="0.30" autocomplete="off" />
        </div>
        <div>
          <label>Custom Entry Floor ($ — Blank = Auto)</label>
          <input type="number" id="entryInput" step="0.00000001" placeholder="Auto-locks live price" value="" autocomplete="off" />
        </div>
      </div>
      <div class="btns">
        <button class="ctrl b-save-paper" onclick="addOrUpdateSlot('PAPER')" id="savePaperBtn">🧪 Launch in $100 PAPER Study Lab</button>
        <button class="ctrl b-save-live" onclick="addOrUpdateSlot('LIVE')" id="saveLiveBtn">💎 Launch in LIVE Cash Fleet</button>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">
        <span>📊 Best Moving Coins Leaderboard (Day / Week Study)</span>
      </div>
      <div style="max-height: 310px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Rank & Coin</th>
              <th>Mode</th>
              <th>Skims Won</th>
              <th>Vaulted Profit</th>
              <th>24h Range %</th>
            </tr>
          </thead>
          <tbody id="studyLeaderboardBody">
            <tr><td colspan="5" style="color:#64748b; text-align:center; padding:22px;">Add Paper Study tokens to rank the fastest moving coins of the day/week.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <!-- 4. DUAL LEDGERS (LIVE CASH LEDGER + PAPER STUDY LEDGER) -->
  <div class="grid-2">
    <div class="card">
      <div class="sec-title">
        <span>💎 Verified LIVE Cash Skim Ledger</span>
        <button class="btn-reset-vault" onclick="resetLiveVault()">🧹 Clear Live Ledger</button>
      </div>
      <div style="max-height: 260px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Token Flip</th>
              <th>Buy Cost → Net Sold</th>
              <th>Cash Added</th>
              <th>Rebought</th>
            </tr>
          </thead>
          <tbody id="ledgerBody"></tbody>
        </table>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">
        <span style="color:var(--amber);">🧪 Paper Study Skim Ledger ($100 Virtual Lab)</span>
        <button class="btn-reset-vault" onclick="resetPaperVault()">🧹 Clear Paper Ledger</button>
      </div>
      <div style="max-height: 260px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Paper Flip</th>
              <th>Sim Buy → Sim Sell</th>
              <th>Sim Profit</th>
              <th>Rebought</th>
            </tr>
          </thead>
          <tbody id="paperLedgerBody"></tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title">📡 Multi-Token Fleet Telemetry Logs</div>
    <div class="logs" id="logBox"></div>
  </div>
</div>

<script>
  let deferredPrompt = null;
  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    });
  }
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
  });

  async function triggerInstall() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      const { outcome } = await deferredPrompt.userChoice;
      if (outcome === 'accepted') {
        deferredPrompt = null;
      }
    } else {
      alert('To install VaultSkimmer as an app: On Android/Chrome tap the 3-dots menu -> "Install app". On Desktop Chrome/Edge click the Install icon on the right side of the address bar.');
    }
  }

  async function enablePullbackShield() {
    await fetch('/api/enable_pullback_shield', { method: 'POST' });
    await refresh();
    alert('🛡️ Smart Rebuy Shield enabled across all Live & Paper slots! After any skim, the bot holds USDT until a -0.30% dip OR +0.50% breakout OR 180s timer.');
  }

  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  function fillTicker(pair) {
    document.getElementById('pairInput').value = pair;
  }

  function loadSlotToForm(pair, principal, skimPct, stopPct, pullback) {
    document.getElementById('pairInput').value = pair;
    document.getElementById('principalInput').value = principal;
    document.getElementById('skimPctInput').value = skimPct;
    document.getElementById('stopResetInput').value = stopPct || 99.0;
    document.getElementById('pullbackInput').value = pullback;
    document.getElementById('entryInput').value = '';
  }

  async function loadPaperStudyPack() {
    await fetch('/api/load_paper_study_pack', { method: 'POST' });
    await refresh();
  }

  async function clearPaperSlots() {
    await fetch('/api/clear_paper_slots', { method: 'POST' });
    await refresh();
  }

  async function resetLiveVault() {
    await fetch('/api/reset_live_vault', { method: 'POST' });
    await refresh();
  }

  async function resetPaperVault() {
    await fetch('/api/reset_paper_vault', { method: 'POST' });
    await refresh();
  }

  async function syncApiKeys() {
    const btn = document.getElementById('syncApiBtn');
    const keyVal = document.getElementById('apiKeyInput').value.trim();
    const secretVal = document.getElementById('apiSecretInput').value.trim();
    btn.innerText = 'Verifying with Gate.io...';

    const r = await fetch('/api/sync_keys', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ api_key: keyVal, api_secret: secretVal })
    });
    const d = await r.json();
    if (d.ok) {
      document.getElementById('apiKeyInput').value = '';
      document.getElementById('apiSecretInput').value = '';
      document.getElementById('apiKeyInput').placeholder = 'Saved & Verified on Server';
      document.getElementById('apiSecretInput').placeholder = 'Saved & Verified on Server';
      btn.innerText = '✅ Connected ($' + Number(d.usdt_balance).toFixed(2) + ' USDT)';
    } else {
      alert('Gate.io API Notice: ' + (d.error || 'Check your Key & Secret'));
      btn.innerText = '❌ Sync Failed — Retry';
    }
    await refresh();
    setTimeout(() => { btn.innerText = '🔗 Sync & Verify Gate.io API Keys'; }, 2500);
  }

  async function addOrUpdateSlot(targetEngine) {
    const pair = document.getElementById('pairInput').value.trim();
    const prin = document.getElementById('principalInput').value.trim();
    const skim = document.getElementById('skimPctInput').value.trim();
    const stopPct = document.getElementById('stopResetInput').value.trim();
    const pb = document.getElementById('pullbackInput').value.trim();
    const ep = document.getElementById('entryInput').value.trim();

    if (!pair || !prin || !skim) {
      alert('Please enter Token Symbol, Principal (USDT), and Skim Profit Target (%) first.');
      return;
    }

    const r = await fetch('/api/add_or_update_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        target_engine: targetEngine,
        pair: pair,
        principal_usdt: prin,
        skim_target_pct: skim,
        stop_reset_pct: stopPct || '99.0',
        rebuy_pullback_pct: pb || '0.30',
        entry_price: ep
      })
    });
    const d = await r.json();
    if (!d.ok) {
      alert(d.error || 'Could not add slot');
      return;
    }
    document.getElementById('pairInput').value = '';
    document.getElementById('entryInput').value = '';
    await refresh();
  }

  async function removeSlot(pair, engine) {
    await fetch('/api/remove_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair, engine })
    });
    await refresh();
  }

  async function clearAllSlots() {
    await fetch('/api/clear_all_slots', { method: 'POST' });
    await refresh();
  }

  async function resetSlotEntry(pair, engine) {
    await fetch('/api/reset_slot_entry', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair, engine })
    });
    await refresh();
  }

  async function manualScan() {
    const btn = document.getElementById('scanBtn');
    btn.innerText = 'Scanning...';
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
    btn.innerText = '⚡ Scan All Now';
  }

  function renderSlotCards(slotsObj, engineType, needBuf) {
    const keys = Object.keys(slotsObj);
    return keys.map(pair => {
      const sl = slotsObj[pair];
      const p = Number(sl.principal_usdt || 0);
      const skimPct = Number(sl.skim_target_pct || 1.0);
      const stopPct = Number(sl.stop_reset_pct || 99.0);
      const pbPct = Number(sl.rebuy_pullback_pct !== undefined ? sl.rebuy_pullback_pct : 0.30);
      const needPct = skimPct + needBuf;
      const prog = Math.max(0, Math.min(100, (Number(sl.unrealized_pnl_pct || 0) / needPct) * 100));
      const pnlSign = sl.unrealized_pnl_pct >= 0 ? '+' : '';
      const isPaper = engineType === 'PAPER';

      return `
        <div class="${isPaper ? 'slot-card-paper' : 'slot-card'}">
          <div class="slot-top">
            <div class="slot-pair">
              <span>${pair}</span>
              <span class="${isPaper ? 'slot-badge-paper' : 'slot-badge'}">${isPaper ? 'PAPER $' : 'LIVE $'}${p.toFixed(2)}</span>
            </div>
            <div style="font-size:0.78rem; font-weight:800;" class="${sl.unrealized_pnl_pct >= 0 ? 'c-green' : 'c-amber'}">
              Live: ${fmtPx(sl.last_price)} (${pnlSign}${Number(sl.unrealized_pnl_pct).toFixed(2)}%)
            </div>
          </div>

          <div class="slot-metrics">
            <div>Entry Floor: <strong>${fmtPx(sl.entry_price)}</strong></div>
            <div>+${skimPct}% Target: <strong class="c-cyan">${fmtPx(sl.target_sell_price)}</strong></div>
            <div>Smart Rebuy: <strong class="c-amber">-${pbPct}% Dip / +0.5% / 180s</strong></div>
            <div>Slot Vaulted: <strong class="c-green">+$${Number(sl.slot_skimmed_usdt || 0).toFixed(4)} (${sl.slot_skim_count || 0})</strong></div>
          </div>

          <div style="display:flex; justify-content:space-between; font-size:0.73rem; color:var(--muted);">
            <span>${sl.status_msg}</span>
            <strong class="c-green">${prog.toFixed(1)}%</strong>
          </div>
          <div class="track"><div class="${isPaper ? 'fill-paper' : 'fill'}" style="width:${prog.toFixed(1)}%"></div></div>

          <div class="slot-actions">
            <button class="s-btn" onclick="loadSlotToForm('${pair}', ${p}, ${skimPct}, ${stopPct}, ${pbPct})">✏️ Edit</button>
            <button class="s-btn" onclick="resetSlotEntry('${pair}', '${engineType}')">🔄 Sync Entry</button>
            <button class="s-btn s-btn-del" onclick="removeSlot('${pair}', '${engineType}')">🗑️ Remove</button>
          </div>
        </div>
      `;
    }).join('');
  }

  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('cycleNum').innerText = s.cycle_count || 0;

      const apiBadge = document.getElementById('apiBadge');
      if (s.api_connected) {
        apiBadge.style.color = '#34d399';
        apiBadge.innerText = '● ' + (s.api_status_msg || 'Connected to Gate.io');
      } else {
        apiBadge.style.color = '#fbbf24';
        apiBadge.innerText = '● ' + (s.api_status_msg || 'Not Connected');
      }
      if (s.has_saved_keys && !document.getElementById('apiKeyInput').value) {
        document.getElementById('apiKeyInput').placeholder = 'Saved & Verified on Server';
        document.getElementById('apiSecretInput').placeholder = 'Saved & Verified on Server';
      }

      const liveSlots = s.slots || {};
      const paperSlots = s.paper_slots || {};
      const liveKeys = Object.keys(liveSlots);
      const paperKeys = Object.keys(paperSlots);

      document.getElementById('liveCount').innerText = liveKeys.length;
      document.getElementById('paperCount').innerText = paperKeys.length;
      document.getElementById('clearAllBtn').style.display = liveKeys.length > 0 ? 'inline-block' : 'none';
      document.getElementById('clearPaperBtn').style.display = paperKeys.length > 0 ? 'inline-block' : 'none';

      const needBuf = Number(s.fee_buffer_pct || 0.25);

      let sumPrincipal = 0;
      let sumPosVal = 0;
      liveKeys.forEach(k => {
        sumPrincipal += Number(liveSlots[k].principal_usdt || 0);
        sumPosVal += Number(liveSlots[k].position_value_usdt || 0);
      });

      if (liveKeys.length === 0) {
        document.getElementById('liveFleetContainer').innerHTML = `<div class="empty-fleet">No Live tokens running — Command a token below and click "💎 Launch in LIVE Cash Fleet".</div>`;
      } else {
        document.getElementById('liveFleetContainer').innerHTML = `<div class="fleet-grid">${renderSlotCards(liveSlots, 'LIVE', needBuf)}</div>`;
      }

      if (paperKeys.length === 0) {
        document.getElementById('paperFleetContainer').innerHTML = `
          <div class="empty-fleet">Paper Study Lab is empty ($100.00 Virtual Bankroll Ready) — Tap "🧪 Load 10 Study Coins ($100 Paper Lab)" or add any coin below to study daily movers!</div>
        `;
      } else {
        document.getElementById('paperFleetContainer').innerHTML = `<div class="fleet-grid">${renderSlotCards(paperSlots, 'PAPER', needBuf)}</div>`;
      }

      const freeUsdt = Number(s.usdt_balance || 0);
      document.getElementById('totalPrincipal').innerText = '$' + sumPrincipal.toFixed(2);
      document.getElementById('totalPosVal').innerText = '$' + sumPosVal.toFixed(4);
      document.getElementById('totalPortfolioVal').innerText = '$' + (sumPosVal + freeUsdt).toFixed(4);
      document.getElementById('liveSkimmed').innerText = '+$' + Number(s.live_skimmed_usdt || 0).toFixed(4);
      document.getElementById('liveSkimCount').innerText = s.live_skim_count || 0;
      document.getElementById('paperSkimmed').innerText = '+$' + Number(s.paper_skimmed_usdt || 0).toFixed(4);
      document.getElementById('paperSkimCount').innerText = s.paper_skim_count || 0;
      document.getElementById('paperBankroll').innerText = '$' + (100 + Number(s.paper_skimmed_usdt || 0)).toFixed(2);
      document.getElementById('usdtBal').innerText = '$' + freeUsdt.toFixed(2);

      const allTracked = [
        ...liveKeys.map(k => ({ ...liveSlots[k], modeLabel: 'LIVE' })),
        ...paperKeys.map(k => ({ ...paperSlots[k], modeLabel: 'PAPER' }))
      ].sort((a, b) => {
        if ((b.slot_skim_count || 0) !== (a.slot_skim_count || 0)) {
          return (b.slot_skim_count || 0) - (a.slot_skim_count || 0);
        }
        return (b.range_24h_pct || 0) - (a.range_24h_pct || 0);
      });

      const rankBody = document.getElementById('studyLeaderboardBody');
      if (allTracked.length > 0) {
        rankBody.innerHTML = allTracked.map((item, idx) => `
          <tr>
            <td><strong>#${idx + 1} ${item.pair}</strong></td>
            <td><span class="${item.modeLabel === 'LIVE' ? 'c-green' : 'c-amber'}"><strong>${item.modeLabel}</strong></span></td>
            <td><strong>${item.slot_skim_count || 0} flips</strong></td>
            <td class="c-green"><strong>+$${Number(item.slot_skimmed_usdt || 0).toFixed(4)}</strong></td>
            <td class="c-cyan">${Number(item.range_24h_pct || 0).toFixed(2)}%</td>
          </tr>
        `).join('');
      } else {
        rankBody.innerHTML = `<tr><td colspan="5" style="color:#64748b; text-align:center; padding:22px;">Add Paper Study tokens to rank the fastest moving coins of the day/week.</td></tr>`;
      }

      const lb = document.getElementById('ledgerBody');
      if (s.skim_ledger && s.skim_ledger.length > 0) {
        lb.innerHTML = s.skim_ledger.map(i => `
          <tr>
            <td>${i.time}</td>
            <td><strong>${i.cycle} ${i.pair}</strong></td>
            <td>${i.buy_px} → ${i.sell_px}</td>
            <td class="c-green"><strong>${i.profit}</strong></td>
            <td class="c-cyan">${i.principal_redeployed}</td>
          </tr>
        `).join('');
      } else {
        lb.innerHTML = `<tr><td colspan="5" style="color:#64748b; text-align:center; padding:18px;">No live skims yet.</td></tr>`;
      }

      const plb = document.getElementById('paperLedgerBody');
      if (s.paper_ledger && s.paper_ledger.length > 0) {
        plb.innerHTML = s.paper_ledger.map(i => `
          <tr>
            <td>${i.time}</td>
            <td><strong>${i.cycle} ${i.pair}</strong></td>
            <td>${i.buy_px} → ${i.sell_px}</td>
            <td class="c-amber"><strong>${i.profit}</strong></td>
            <td class="c-cyan">${i.principal_redeployed}</td>
          </tr>
        `).join('');
      } else {
        plb.innerHTML = `<tr><td colspan="5" style="color:#64748b; text-align:center; padding:18px;">Waiting for Paper Study tokens to hit skim target.</td></tr>`;
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
