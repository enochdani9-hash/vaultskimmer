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

STATE_FILE = "vaultskimmer_v5_2_clean.json"
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


def make_slot(pair, principal, skim_pct, pullback_pct=0.0, entry_price=0.0):
    return {
        "pair": pair,
        "principal_usdt": float(principal),
        "actual_cost_usdt": 0.0,
        "skim_target_pct": float(skim_pct),
        "rebuy_pullback_pct": float(pullback_pct),
        "entry_price": float(entry_price),
        "last_skim_sell_price": 0.0,
        "phase": "WAITING_TO_BUY",
        "paper_base_bal": 0.0,
        "base_balance": 0.0,
        "position_value_usdt": 0.0,
        "last_price": 0.0,
        "price_prec": 4,
        "high_24h": 0.0,
        "low_24h": 0.0,
        "change_24h_pct": 0.0,
        "target_sell_price": 0.0,
        "target_rebuy_price": 0.0,
        "unrealized_pnl_usdt": 0.0,
        "unrealized_pnl_pct": 0.0,
        "slot_skimmed_usdt": 0.0,
        "slot_skim_count": 0,
        "status_msg": "Slot armed — awaiting next scan..."
    }


# 100% BLANK INITIAL STATE — Nothing runs or loads until commanded by user
DEFAULT_STATE = {
    "mode": "PAPER",  # "PAPER" or "LIVE"
    "fee_buffer_pct": 0.40,  # 0.20% buy fee + 0.20% sell fee round-trip buffer
    "api_key": os.environ.get("GATE_API_KEY", ""),
    "api_secret": os.environ.get("GATE_API_SECRET", ""),
    "api_connected": False,
    "api_status_msg": "Not Connected — Paste Keys & Tap Sync",
    "slots": {},  # Completely empty until user adds a token
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
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                STATE.update(saved)
                # Ensure round-trip fee buffer is at least 0.40% for Gate.io spot
                if float(STATE.get("fee_buffer_pct", 0.0)) < 0.40:
                    STATE["fee_buffer_pct"] = 0.40
                if "slots" not in STATE or not isinstance(STATE["slots"], dict):
                    STATE["slots"] = {}
                for sl in STATE["slots"].values():
                    if "actual_cost_usdt" not in sl:
                        sl["actual_cost_usdt"] = 0.0
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
    key = (custom_key if custom_key is not None else STATE.get("api_key", "")).strip()
    secret = (custom_secret if custom_secret is not None else STATE.get("api_secret", "")).strip()
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
            result[cp] = {
                "last": last,
                "high_24h": float(t.get("high_24h") or last),
                "low_24h": float(t.get("low_24h") or last),
                "change_24h": float(t.get("change_percentage") or 0.0)
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


def format_dec(val, precision):
    q = Decimal("10") ** (-precision)
    return str(Decimal(str(val)).quantize(q, rounding=ROUND_DOWN))


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


def process_single_slot(pair, slot, mode, fee_buf, has_keys, bal_map, ticker_map):
    base_curr = pair.split("_")[0]
    principal = float(slot["principal_usdt"])
    skim_pct = float(slot["skim_target_pct"])
    pullback_pct = float(slot.get("rebuy_pullback_pct", 0.0))

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

    # Round-trip fee buffer (0.40% default) ensures full skim_pct net profit after buy+sell fees
    effective_gain_pct = skim_pct + fee_buf
    last_sell_px = float(slot.get("last_skim_sell_price", 0.0))
    if last_sell_px > 0 and pullback_pct > 0:
        rebuy_px = last_sell_px * (1.0 - (pullback_pct / 100.0))
    else:
        rebuy_px = price
    slot["target_rebuy_price"] = round(rebuy_px, px_prec)

    if slot["entry_price"] > price * 2.5 or slot["entry_price"] < price * 0.2:
        slot["entry_price"] = 0.0

    # =========================================================================
    # PAPER SIMULATION MODE (Simulates exact Gate.io lot-size precision too)
    # =========================================================================
    if mode == "PAPER":
        paper_tokens = float(slot.get("paper_base_bal", 0.0))
        pos_val = paper_tokens * price

        if paper_tokens <= 0 or pos_val < (principal * 0.25):
            if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
                slot["phase"] = "WAITING_TO_BUY"
                slot["base_balance"] = 0.0
                slot["position_value_usdt"] = 0.0
                slot["status_msg"] = f"[PAPER] Waiting for -{pullback_pct}% pullback to {fmt_px(rebuy_px)}"
                return

            sim_tokens = float(format_dec(principal / price, amt_prec))
            if sim_tokens <= 0:
                sim_tokens = principal / price
            if slot["entry_price"] <= 0:
                slot["entry_price"] = price
            actual_cost = round(sim_tokens * slot["entry_price"], 4)
            slot["paper_base_bal"] = sim_tokens
            slot["base_balance"] = sim_tokens
            slot["actual_cost_usdt"] = actual_cost
            slot["position_value_usdt"] = round(sim_tokens * price, 4)
            target_px = slot["entry_price"] * (1.0 + (effective_gain_pct / 100.0))
            slot["target_sell_price"] = round(target_px, px_prec)
            slot["phase"] = "HOLDING_FOR_SKIM"
            slot["unrealized_pnl_usdt"] = 0.0
            slot["unrealized_pnl_pct"] = 0.0
            slot["status_msg"] = f"[PAPER] Bought ${actual_cost:.2f} @ {fmt_px(slot['entry_price'])} → Target {fmt_px(target_px)}"
            add_log(f"[PAPER][{pair}] Deployed ${actual_cost:.2f} @ {fmt_px(price)}")
            return

        slot["phase"] = "HOLDING_FOR_SKIM"
        slot["base_balance"] = paper_tokens
        slot["position_value_usdt"] = round(pos_val, 4)
        entry_px = slot["entry_price"] if slot["entry_price"] > 0 else price
        actual_cost = float(slot.get("actual_cost_usdt") or 0.0)
        if actual_cost <= 0:
            actual_cost = round(paper_tokens * entry_px, 4)
            slot["actual_cost_usdt"] = actual_cost

        target_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
        slot["target_sell_price"] = round(target_px, px_prec)

        slot["unrealized_pnl_usdt"] = round(pos_val - actual_cost, 4)
        slot["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)

        if price >= target_px:
            gross_val = paper_tokens * price
            net_val = gross_val * (1.0 - (fee_buf / 100.0))
            sim_profit = max(0.01, round(net_val - actual_cost, 4))

            slot["slot_skimmed_usdt"] = round(slot.get("slot_skimmed_usdt", 0.0) + sim_profit, 4)
            slot["slot_skim_count"] = slot.get("slot_skim_count", 0) + 1
            STATE["paper_skimmed_usdt"] = round(STATE.get("paper_skimmed_usdt", 0.0) + sim_profit, 4)
            STATE["paper_skim_count"] = STATE.get("paper_skim_count", 0) + 1
            old_entry = entry_px
            slot["last_skim_sell_price"] = price

            STATE["skim_ledger"].insert(0, {
                "time": datetime.utcnow().strftime("%H:%M:%S"),
                "pair": f"{pair} (SIM)",
                "cycle": f"#{STATE['paper_skim_count']}",
                "buy_px": fmt_px(old_entry),
                "sell_px": fmt_px(price),
                "profit": f"+${sim_profit:.4f}",
                "principal_redeployed": f"${actual_cost:.2f}"
            })
            STATE["skim_ledger"] = STATE["skim_ledger"][:50]

            if pullback_pct <= 0:
                new_tokens = float(format_dec(principal / price, amt_prec))
                if new_tokens <= 0:
                    new_tokens = principal / price
                new_cost = round(new_tokens * price, 4)
                slot["paper_base_bal"] = new_tokens
                slot["base_balance"] = new_tokens
                slot["actual_cost_usdt"] = new_cost
                slot["entry_price"] = price
                new_target = price * (1.0 + (effective_gain_pct / 100.0))
                slot["target_sell_price"] = round(new_target, px_prec)
                slot["position_value_usdt"] = new_cost
                slot["status_msg"] = f"[PAPER] Skimmed +${sim_profit:.4f} & Rebought @ {fmt_px(price)}"
                add_log(f"[PAPER SKIM][{pair}] Vaulted +${sim_profit:.4f} & Rebought ${new_cost:.2f} @ {fmt_px(price)}!")
            else:
                slot["paper_base_bal"] = 0.0
                slot["base_balance"] = 0.0
                slot["phase"] = "WAITING_TO_BUY"
                slot["status_msg"] = f"[PAPER] Skimmed +${sim_profit:.4f}! Waiting for pullback."
        else:
            if price < entry_px:
                slot["status_msg"] = f"[PAPER] Capital Protected | Live {fmt_px(price)} < Entry {fmt_px(entry_px)}"
            else:
                slot["status_msg"] = f"[PAPER] Climbing ({slot['unrealized_pnl_pct']:+.2f}%) → Target {fmt_px(target_px)}"
        return

    # =========================================================================
    # LIVE GATE.IO CASH MODE (TRUE ACTUAL-COST PROFIT & LOT-ROUNDING FIX)
    # =========================================================================
    if not has_keys:
        slot["status_msg"] = "[LIVE] Connect Gate.io API Keys above first"
        return

    real_base_bal = bal_map.get(base_curr, 0.0)
    real_usdt_bal = bal_map.get("USDT", 0.0)
    slot["base_balance"] = real_base_bal
    pos_val = real_base_bal * price
    slot["position_value_usdt"] = round(pos_val, 4)

    # Use lot-truncated quantity * entry_price as true cost basis so lot rounding never eats profit!
    sellable_qty = float(format_dec(real_base_bal, amt_prec)) if real_base_bal > 0 else 0.0
    holding_tokens = pos_val >= max(min_quote * 1.05, principal * 0.40) and sellable_qty > 0

    if holding_tokens:
        slot["phase"] = "HOLDING_FOR_SKIM"
        if slot["entry_price"] <= 0:
            slot["entry_price"] = price
            add_log(f"[LIVE][{pair}] Locked Entry Floor @ {fmt_px(price)}")
    else:
        slot["phase"] = "WAITING_TO_BUY"

    entry_px = slot["entry_price"] if slot["entry_price"] > 0 else price
    target_sell_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
    slot["target_sell_price"] = round(target_sell_px, px_prec)

    if holding_tokens and entry_px > 0:
        # True cost of the exact coins held in the slot (fixes the $10.76 vs $11.00 discrepancy)
        actual_cost = round(sellable_qty * entry_px, 4)
        slot["actual_cost_usdt"] = actual_cost
        slot["unrealized_pnl_usdt"] = round((sellable_qty * price) - actual_cost, 4)
        slot["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)
    else:
        slot["actual_cost_usdt"] = 0.0
        slot["unrealized_pnl_usdt"] = 0.0
        slot["unrealized_pnl_pct"] = 0.0

    if slot["phase"] == "HOLDING_FOR_SKIM":
        if price < target_sell_px:
            if price < entry_px:
                slot["status_msg"] = f"[LIVE] Capital Protected | Live {fmt_px(price)} < Entry {fmt_px(entry_px)}"
            else:
                slot["status_msg"] = f"[LIVE] Climbing ({slot['unrealized_pnl_pct']:+.2f}%) → Target {fmt_px(target_sell_px)}"
            return

        sell_amt_str = format_dec(real_base_bal, amt_prec)
        sell_qty_num = float(sell_amt_str)
        sell_px_str = format_dec(price * 0.998, px_prec)
        code, resp = place_gate_order(pair, "sell", sell_amt_str, sell_px_str)

        if code in (200, 201):
            # Read exact USDT proceeds from Gate.io response if available
            filled_quote = float(resp.get("filled_total") or 0.0)
            fill_fee_usdt = float(resp.get("fee") or 0.0) if resp.get("fee_currency") == "USDT" else 0.0
            gross_proceeds = filled_quote if filled_quote > 0 else (sell_qty_num * price)

            # True entry cost of the sold coins (including 0.20% buy-side fee)
            true_entry_cost = (sell_qty_num * entry_px) * 1.002
            if fill_fee_usdt > 0:
                net_proceeds = gross_proceeds - fill_fee_usdt
            else:
                net_proceeds = gross_proceeds * 0.998  # 0.20% sell-side fee

            # True net profit above the actual coin cost (never eaten by lot-size rounding!)
            skimmed_profit = max(0.01, round(net_proceeds - true_entry_cost, 4))

            slot["slot_skimmed_usdt"] = round(slot.get("slot_skimmed_usdt", 0.0) + skimmed_profit, 4)
            slot["slot_skim_count"] = slot.get("slot_skim_count", 0) + 1
            STATE["live_skimmed_usdt"] = round(STATE.get("live_skimmed_usdt", 0.0) + skimmed_profit, 4)
            STATE["live_skim_count"] = STATE.get("live_skim_count", 0) + 1
            old_entry = slot["entry_price"]
            slot["last_skim_sell_price"] = price

            # Reinvest the exact principal (leaving the skimmed profit untouched in your USDT balance!)
            reinvest_budget = min(principal, max(min_quote * 1.02, net_proceeds - skimmed_profit))

            STATE["skim_ledger"].insert(0, {
                "time": datetime.utcnow().strftime("%H:%M:%S"),
                "pair": f"{pair} (LIVE)",
                "cycle": f"#{STATE['live_skim_count']}",
                "buy_px": fmt_px(old_entry),
                "sell_px": fmt_px(price),
                "profit": f"+${skimmed_profit:.4f}",
                "principal_redeployed": f"${reinvest_budget:.2f}"
            })
            STATE["skim_ledger"] = STATE["skim_ledger"][:50]

            add_log(
                f"[LIVE SKIM][{pair}] Sold {sell_amt_str} {base_curr} @ {fmt_px(price)} "
                f"(Cost: ${true_entry_cost:.2f} → Net: ${net_proceeds:.2f}) | Vaulted +${skimmed_profit:.4f} USDT!"
            )

            if pullback_pct <= 0.0:
                time.sleep(0.3)
                rebuy_amt = reinvest_budget / price
                rebuy_amt_str = format_dec(rebuy_amt, amt_prec)
                rebuy_px_str = format_dec(price * 1.002, px_prec)
                b_code, b_resp = place_gate_order(pair, "buy", rebuy_amt_str, rebuy_px_str)
                if b_code in (200, 201):
                    actual_fill_px = float(b_resp.get("fill_price") or 0.0)
                    if actual_fill_px <= 0:
                        filled_tot = float(b_resp.get("filled_total") or 0.0)
                        filled_amt = float(b_resp.get("filled_amount") or rebuy_amt_str or 0.0)
                        actual_fill_px = (filled_tot / filled_amt) if (filled_tot > 0 and filled_amt > 0) else price

                    slot["entry_price"] = actual_fill_px
                    slot["actual_cost_usdt"] = round(float(rebuy_amt_str) * actual_fill_px, 4)
                    slot["phase"] = "HOLDING_FOR_SKIM"
                    new_target = actual_fill_px * (1.0 + (effective_gain_pct / 100.0))
                    slot["target_sell_price"] = round(new_target, px_prec)
                    slot["status_msg"] = f"[LIVE] Skimmed +${skimmed_profit:.4f} & Rebought @ {fmt_px(actual_fill_px)}"
                    add_log(f"[LIVE REBUY][{pair}] Bought {rebuy_amt_str} {base_curr} (${slot['actual_cost_usdt']:.2f}) @ {fmt_px(actual_fill_px)}")
                else:
                    slot["phase"] = "WAITING_TO_BUY"
            else:
                slot["phase"] = "WAITING_TO_BUY"
        else:
            add_log(f"[LIVE][{pair}] Sell rejected ({code}): {resp.get('message', resp)}")
        return

    if slot["phase"] == "WAITING_TO_BUY":
        if real_usdt_bal < min(principal, min_quote * 1.05):
            slot["status_msg"] = f"[LIVE] Awaiting USDT (Wallet: ${real_usdt_bal:.2f} / Need: ${principal:.2f})"
            return

        if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
            slot["status_msg"] = f"[LIVE] Waiting for pullback to {fmt_px(rebuy_px)}"
            return

        usable_usdt = min(principal, real_usdt_bal * 0.996)
        if usable_usdt < min_quote:
            slot["status_msg"] = f"[LIVE] Awaiting USDT (Wallet: ${real_usdt_bal:.2f} / Min: ${min_quote:.2f})"
            return

        buy_amt = usable_usdt / price
        buy_amt_str = format_dec(buy_amt, amt_prec)
        buy_px_str = format_dec(price * 1.002, px_prec)
        code, resp = place_gate_order(pair, "buy", buy_amt_str, buy_px_str)

        if code in (200, 201):
            actual_fill_px = float(resp.get("fill_price") or 0.0)
            filled_tot = float(resp.get("filled_total") or 0.0)
            filled_amt = float(resp.get("filled_amount") or buy_amt_str or 0.0)
            if actual_fill_px <= 0:
                actual_fill_px = (filled_tot / filled_amt) if (filled_tot > 0 and filled_amt > 0) else price

            actual_spent = filled_tot if filled_tot > 0 else ( float(buy_amt_str) * actual_fill_px )
            slot["entry_price"] = actual_fill_px
            slot["actual_cost_usdt"] = round(actual_spent, 4)
            slot["phase"] = "HOLDING_FOR_SKIM"
            new_target = actual_fill_px * (1.0 + (effective_gain_pct / 100.0))
            slot["target_sell_price"] = round(new_target, px_prec)
            slot["status_msg"] = f"[LIVE] Bought ${actual_spent:.2f} @ {fmt_px(actual_fill_px)} → Target {fmt_px(new_target)}"
            bal_map["USDT"] = max(0.0, real_usdt_bal - actual_spent)
            add_log(f"[LIVE BUY][{pair}] Bought {buy_amt_str} {base_curr} (Actual Cost: ${actual_spent:.2f}) @ {fmt_px(actual_fill_px)}")
        else:
            add_log(f"[LIVE][{pair}] Buy rejected ({code}): {resp.get('message', resp)}")


def run_fleet_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            mode = STATE.get("mode", "PAPER")
            fee_buf = float(STATE.get("fee_buffer_pct", 0.40))
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))
            slot_keys = list(STATE["slots"].keys())

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
                    STATE["api_status_msg"] = f"Auth Error: {str(e)[:55]}"
                if mode == "LIVE":
                    add_log(f"Gate.io API Key Error: {str(e)[:60]}")

        if not slot_keys:
            with state_lock:
                STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
                STATE["last_worker_heartbeat"] = time.time()
                STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
                STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            return

        ticker_map = {}
        try:
            ticker_map = fetch_batch_tickers(slot_keys)
        except Exception as tk_err:
            add_log(f"Ticker batch notice: {str(tk_err)[:60]}")
            return

        for pair in slot_keys:
            with state_lock:
                slot = STATE["slots"].get(pair)
                if not slot:
                    continue
            try:
                process_single_slot(pair, slot, mode, fee_buf, has_keys, bal_map, ticker_map)
            except Exception as slot_err:
                with state_lock:
                    slot["status_msg"] = f"Notice: {str(slot_err)[:55]}"

        with state_lock:
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_worker_heartbeat"] = time.time()
            STATE["last_scan_ms"] = int((time.time() - t0) * 1000)
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")
            save_state()

    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("VaultSkimmer v5.3 Precision Engine Ready — True Cost & Round-Trip Fee Buffer Active.")
    while True:
        run_fleet_cycle(force_manual=False)
        time.sleep(3.0)


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
        "name": "VaultSkimmer v5.3 Command Fleet",
        "short_name": "VaultSkimmer",
        "description": "True-Cost Precision Multi-Token Spot Skimmer",
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
    return jsonify({"ok": True, "service": "VaultSkimmer-v5.3", "cycle": STATE["cycle_count"]})


@app.route("/api/cron")
def api_cron():
    run_fleet_cycle(force_manual=False)
    return jsonify({
        "ok": True,
        "mode": STATE["mode"],
        "active_tokens": list(STATE["slots"].keys()),
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
    new_key = (data.get("api_key") or "").strip()
    new_secret = (data.get("api_secret") or "").strip()

    with state_lock:
        if new_key:
            STATE["api_key"] = new_key
        if new_secret:
            STATE["api_secret"] = new_secret
        active_key = STATE.get("api_key", "")
        active_secret = STATE.get("api_secret", "")

    if not active_key or not active_secret:
        with state_lock:
            STATE["api_connected"] = False
            STATE["api_status_msg"] = "Missing API Key or Secret — Please paste both."
            save_state()
        return jsonify({"ok": False, "error": "Please enter both your Gate.io API Key and Secret."}), 400

    try:
        bal_map = fetch_all_spot_balances(active_key, active_secret)
        usdt_avail = bal_map.get("USDT", 0.0)
        non_zero = [f"{k}: {v:.4f}" for k, v in bal_map.items() if v > 0][:6]
        summary = ", ".join(non_zero) if non_zero else "0.00 USDT"
        with state_lock:
            STATE["api_connected"] = True
            STATE["usdt_balance"] = usdt_avail
            STATE["api_status_msg"] = f"Verified & Connected! Gate.io USDT: ${usdt_avail:.2f}"
            add_log(f"🔗 Gate.io API Keys Synced & Verified! Balances: {summary}")
            save_state()
        return jsonify({"ok": True, "usdt_balance": usdt_avail, "status_msg": STATE["api_status_msg"]})
    except Exception as e:
        err_str = str(e)[:80]
        with state_lock:
            STATE["api_connected"] = False
            STATE["api_status_msg"] = f"Connection Failed: {err_str}"
            add_log(f"❌ Gate.io API Sync Failed: {err_str}")
            save_state()
        return jsonify({"ok": False, "error": err_str}), 400


@app.route("/api/add_or_update_slot", methods=["POST"])
def api_add_or_update_slot():
    data = request.json or {}
    pair = normalize_pair(data.get("pair", ""))
    if not pair:
        return jsonify({"ok": False, "error": "Please enter a Token Symbol first."}), 400

    raw_prin = str(data.get("principal_usdt") or "").strip()
    if not raw_prin or float(raw_prin) <= 0:
        return jsonify({"ok": False, "error": "Please enter a valid Slot Protected Principal (USDT)."}), 400

    raw_skim = str(data.get("skim_target_pct") or "").strip()
    if not raw_skim or float(raw_skim) <= 0:
        return jsonify({"ok": False, "error": "Please enter a Skim Profit Target % (e.g. 1)."}), 400

    principal = max(1.0, float(raw_prin))
    skim_pct = max(0.1, float(raw_skim))
    raw_pb = str(data.get("rebuy_pullback_pct") or "").strip()
    pullback_pct = max(0.0, float(raw_pb)) if raw_pb else 0.0

    raw_ep = str(data.get("entry_price") or "").strip()
    custom_ep = float(raw_ep) if (raw_ep and float(raw_ep) > 0) else 0.0

    with state_lock:
        existing = STATE["slots"].get(pair)
        if existing:
            if existing["principal_usdt"] != principal and STATE["mode"] == "PAPER":
                existing["paper_base_bal"] = 0.0
                existing["entry_price"] = custom_ep
            existing["principal_usdt"] = principal
            existing["skim_target_pct"] = skim_pct
            existing["rebuy_pullback_pct"] = pullback_pct
            if custom_ep > 0:
                existing["entry_price"] = custom_ep
            add_log(f"Updated Command Slot: {pair} | Principal: ${principal:.2f} | Skim: +{skim_pct}%")
        else:
            STATE["slots"][pair] = make_slot(pair, principal, skim_pct, pullback_pct, custom_ep)
            add_log(f"Commanded New Token Slot: {pair} (${principal:.2f} Base @ +{skim_pct}% Skim)")

        save_state()

    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True, "pair": pair})


@app.route("/api/remove_slot", methods=["POST"])
def api_remove_slot():
    data = request.json or {}
    pair = normalize_pair(data.get("pair", ""))
    with state_lock:
        if pair in STATE["slots"]:
            del STATE["slots"][pair]
            add_log(f"Removed {pair} from fleet ({len(STATE['slots'])} active slots remaining).")
            save_state()
    return jsonify({"ok": True})


@app.route("/api/clear_all_slots", methods=["POST"])
def api_clear_all_slots():
    with state_lock:
        STATE["slots"] = {}
        add_log("🧹 Cleared all token slots — System is now 100% empty and standing by.")
        save_state()
    return jsonify({"ok": True})


@app.route("/api/reset_slot_entry", methods=["POST"])
def api_reset_slot_entry():
    data = request.json or {}
    pair = normalize_pair(data.get("pair", ""))
    with state_lock:
        slot = STATE["slots"].get(pair)
        if slot and slot["last_price"] > 0:
            slot["entry_price"] = slot["last_price"]
            if STATE["mode"] == "PAPER":
                slot["paper_base_bal"] = slot["principal_usdt"] / slot["last_price"]
            add_log(f"[{pair}] Entry Floor synced to live price: {fmt_px(slot['entry_price'])}")
            save_state()
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True})


@app.route("/api/toggle_mode", methods=["POST"])
def api_toggle_mode():
    with state_lock:
        new_mode = "LIVE" if STATE.get("mode") == "PAPER" else "PAPER"
        STATE["mode"] = new_mode
        for sl in STATE["slots"].values():
            sl["entry_price"] = 0.0
            sl["actual_cost_usdt"] = 0.0
            sl["slot_skimmed_usdt"] = 0.0
            sl["slot_skim_count"] = 0
            sl["unrealized_pnl_pct"] = 0.0
            sl["unrealized_pnl_usdt"] = 0.0
        add_log(f"Switched Fleet Mode to: {new_mode} (Entry floors reset for clean {new_mode} execution)")
        save_state()
    run_fleet_cycle(force_manual=True)
    return jsonify({"ok": True, "mode": STATE["mode"]})


HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#060b16" />
  <link rel="manifest" href="/manifest.json" />
  <link rel="icon" type="image/svg+xml" href="/icon.svg" />
  <title>VaultSkimmer v5.3 | True-Cost Precision Engine</title>
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
    .wrap { max-width: 1180px; margin: 0 auto; }

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
    .btn-install {
      background: linear-gradient(135deg, #10b981, #06b6d4); color: #022c22;
      font-weight: 900; font-size: 0.79rem; padding: 9px 14px; border-radius: 10px; border: none;
      cursor: pointer; display: inline-flex; align-items: center; gap: 6px;
      box-shadow: 0 0 16px var(--emerald-glow);
    }
    .btn-scan {
      background: rgba(139, 92, 246, 0.22); border: 1px solid #8b5cf6; color: #ddd6fe;
      font-weight: 800; font-size: 0.78rem; padding: 8px 13px; border-radius: 10px; cursor: pointer;
    }

    .telemetry-strip {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      margin-top: 12px; padding-top: 12px; border-top: 1px solid rgba(255,255,255,0.08);
      font-size: 0.79rem; color: var(--muted); gap: 8px;
    }
    .pill {
      display: inline-flex; align-items: center; gap: 7px; padding: 5px 13px; border-radius: 99px;
      font-weight: 800; font-size: 0.75rem;
    }
    .pill-live { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid #10b981; }
    .pill-paper { background: rgba(6,182,212,0.18); color: #22d3ee; border: 1px solid #06b6d4; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 1.2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.25; } }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card {
      background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    }
    .lbl { color: var(--muted); font-size: 0.7rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.6px; }
    .val { font-size: 1.45rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .sub { font-size: 0.77rem; color: var(--muted); }
    .c-green { color: #34d399; }
    .c-cyan { color: #22d3ee; }
    .c-amber { color: #fbbf24; }

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
    .fleet-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(320px, 1fr)); gap: 12px; margin-bottom: 16px; }
    .empty-fleet {
      background: rgba(13, 20, 36, 0.65); border: 1px dashed #334155; border-radius: 15px;
      padding: 28px 18px; text-align: center; color: var(--muted); font-size: 0.85rem; margin-bottom: 16px;
    }
    .slot-card {
      background: linear-gradient(180deg, rgba(16, 26, 48, 0.95), rgba(10, 15, 29, 0.95));
      border: 1px solid #263554; border-radius: 15px; padding: 14px 16px;
      box-shadow: 0 8px 22px rgba(0,0,0,0.4);
    }
    .slot-top { display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px; }
    .slot-pair { font-size: 1.12rem; font-weight: 900; color: #fff; display: flex; align-items: center; gap: 8px; }
    .slot-badge { font-size: 0.68rem; background: rgba(6,182,212,0.18); color: #22d3ee; border: 1px solid #06b6d4; padding: 2px 8px; border-radius: 99px; font-weight: 800; }
    .slot-metrics { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; font-size: 0.76rem; margin-bottom: 10px; background: rgba(5,8,17,0.6); padding: 9px 11px; border-radius: 10px; border: 1px solid #1e293b; }
    .track { height: 10px; background: #04070e; border-radius: 99px; overflow: hidden; border: 1px solid #1e293b; margin: 7px 0; }
    .fill { height: 100%; width: 0%; background: linear-gradient(90deg, #06b6d4, #10b981); transition: width 0.35s ease; }
    .slot-actions { display: flex; justify-content: space-between; align-items: center; margin-top: 9px; gap: 6px; }
    .s-btn { background: #1e293b; color: #cbd5e1; border: 1px solid #334155; padding: 5px 10px; border-radius: 8px; font-size: 0.72rem; font-weight: 800; cursor: pointer; }
    .s-btn-del { background: rgba(239,68,68,0.15); color: #fca5a5; border: 1px solid rgba(239,68,68,0.4); }

    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 14px; margin-bottom: 14px; }
    .sec-title { font-size: 0.85rem; font-weight: 900; text-transform: uppercase; color: #38bdf8; margin-bottom: 12px; }
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
    button.ctrl { cursor: pointer; border: none; border-radius: 10px; padding: 11px 14px; font-weight: 800; font-size: 0.83rem; }
    .b-save { background: linear-gradient(135deg, #10b981, #059669); color: #022c22; flex: 1; }
    .b-arm { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; flex: 1; }

    table { width: 100%; border-collapse: collapse; font-size: 0.79rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #1e293b; font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-size: 0.69rem; text-transform: uppercase; }
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
        <div class="brand-title">VaultSkimmer <span class="v-badge">v5.3 TRUE-COST</span></div>
        <div class="sub-desc">True Lot-Cost Profit Calculation • 0.40% Round-Trip Fee Buffer • Independent +1% Net Skim & Auto-Rebuy</div>
      </div>
      <div class="top-actions">
        <button class="btn-install" onclick="triggerInstall()">📲 Install App</button>
        <button class="btn-scan" onclick="manualScan()" id="scanBtn">⚡ Scan All Now</button>
      </div>
    </div>

    <div class="telemetry-strip">
      <div>
        <span id="modePill" class="pill pill-paper"><span class="dot"></span> <span id="modeText">PAPER SIM FLEET</span></span>
      </div>
      <div>Active Tokens Running: <strong class="c-cyan" id="activeCount">0</strong></div>
      <div>Last Fleet Scan: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-green" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)</div>
    </div>
  </div>

  <!-- DEDICATED GATE.IO API KEY CONNECTION PANEL -->
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
      <div class="lbl">Total Active Cost (All Slots)</div>
      <div class="val c-cyan" id="totalPrincipal">$0.00</div>
      <div class="sub">Live Fleet Value: <strong id="totalPosVal">$0.0000</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Active Tokens in Fleet</div>
      <div class="val" id="fleetListText">None</div>
      <div class="sub">Empty until you command a token</div>
    </div>
    <div class="card">
      <div class="lbl">Realized LIVE Cash Profit Vault</div>
      <div class="val c-green" id="liveSkimmed">+$0.0000</div>
      <div class="sub">Live Skims Won: <strong id="liveSkimCount">0</strong> | Gate.io USDT: <strong id="usdtBal">$0.00</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Paper Sim Profit Tracker</div>
      <div class="val c-amber" id="paperSkimmed">+$0.0000</div>
      <div class="sub">Paper Skims Won: <strong id="paperSkimCount">0</strong></div>
    </div>
  </div>

  <div class="fleet-header">
    <span>🚀 Active Multi-Token Skimmer Fleet</span>
    <button class="s-btn s-btn-del" onclick="clearAllSlots()" id="clearAllBtn" style="display:none;">🧹 Clear All Slots</button>
  </div>
  <div id="fleetContainer">
    <div class="empty-fleet">No tokens running — Everything is 100% empty until you command a token below.</div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="sec-title">➕ Command New Token Slot / Edit Existing Slot</div>

      <label>Tap a Ticker to Fill Symbol Box (Does Not Auto-Run Until You Command):</label>
      <div class="chip-bar">
        <span class="chip" onclick="fillTicker('SUI_USDT')">SUI</span>
        <span class="chip" onclick="fillTicker('QNT_USDT')">QNT</span>
        <span class="chip" onclick="fillTicker('SOL_USDT')">SOL</span>
        <span class="chip" onclick="fillTicker('HYPE_USDT')">HYPE</span>
        <span class="chip" onclick="fillTicker('PEPE_USDT')">PEPE</span>
        <span class="chip" onclick="fillTicker('TAO_USDT')">TAO</span>
        <span class="chip" onclick="fillTicker('LINK_USDT')">LINK</span>
        <span class="chip" onclick="fillTicker('DOGE_USDT')">DOGE</span>
        <span class="chip" onclick="fillTicker('AAVE_USDT')">AAVE</span>
        <span class="chip" onclick="fillTicker('NEAR_USDT')">NEAR</span>
      </div>

      <div class="row">
        <div>
          <label>Token Symbol (Type Any Gate.io Coin)</label>
          <input type="text" id="pairInput" placeholder="e.g. SUI, QNT, PEPE, BTC" value="" autocomplete="off" />
        </div>
        <div>
          <label>Slot Protected Principal (USDT)</label>
          <input type="number" id="principalInput" step="0.5" placeholder="Enter USDT amount..." value="" autocomplete="off" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Skim Profit Target (%)</label>
          <input type="number" id="skimPctInput" step="0.1" placeholder="Enter % (e.g. 1.0)" value="" autocomplete="off" />
        </div>
        <div>
          <label>Rebuy Pullback % (Optional — Blank = 0%)</label>
          <input type="number" id="pullbackInput" step="0.05" placeholder="0 = Instant Rebuy" value="" autocomplete="off" />
        </div>
      </div>
      <div class="row">
        <div style="grid-column: span 2;">
          <label>Custom Entry Floor ($ — Leave Blank to Auto-Lock Live Price)</label>
          <input type="number" id="entryInput" step="0.00000001" placeholder="Leave blank for automatic live entry price" value="" autocomplete="off" />
        </div>
      </div>
      <div class="btns">
        <button class="ctrl b-save" onclick="addOrUpdateSlot()" id="saveBtn">➕ Command & Launch Token Slot</button>
        <button class="ctrl b-arm" onclick="toggleMode()">🔀 Switch Fleet PAPER / LIVE</button>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">💎 Unified Multi-Token +1% Skim Ledger</div>
      <div style="max-height: 310px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Token Flip</th>
              <th>Buy → Sell</th>
              <th>Net Profit</th>
              <th>Rebought</th>
            </tr>
          </thead>
          <tbody id="ledgerBody">
            <tr><td colspan="5" style="color:#64748b; text-align:center; padding:22px;">Waiting for commanded tokens to hit target above Entry Floor.</td></tr>
          </tbody>
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
    window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js').catch(() => {}));
  }
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
  });

  function fmtPx(val) {
    const v = Number(val || 0);
    if (v === 0) return '$0.0000';
    if (v < 0.0001) return '$' + v.toFixed(8);
    if (v < 0.01) return '$' + v.toFixed(6);
    return '$' + v.toFixed(4);
  }

  async function triggerInstall() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      deferredPrompt = null;
    } else {
      alert('App is already installed or available via your browser menu!');
    }
  }

  function fillTicker(pair) {
    document.getElementById('pairInput').value = pair;
  }

  function loadSlotToForm(pair, principal, skimPct, pullback) {
    document.getElementById('pairInput').value = pair;
    document.getElementById('principalInput').value = principal;
    document.getElementById('skimPctInput').value = skimPct;
    document.getElementById('pullbackInput').value = pullback;
    document.getElementById('entryInput').value = '';
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
      alert('Gate.io API Error: ' + (d.error || 'Check your Key & Secret'));
      btn.innerText = '❌ Sync Failed — Retry';
    }
    await refresh();
    setTimeout(() => { btn.innerText = '🔗 Sync & Verify Gate.io API Keys'; }, 2500);
  }

  async function addOrUpdateSlot() {
    const pair = document.getElementById('pairInput').value.trim();
    const prin = document.getElementById('principalInput').value.trim();
    const skim = document.getElementById('skimPctInput').value.trim();
    const pb = document.getElementById('pullbackInput').value.trim();
    const ep = document.getElementById('entryInput').value.trim();

    if (!pair || !prin || !skim) {
      alert('Please enter Token Symbol, Protected Principal (USDT), and Skim Profit Target (%) first.');
      return;
    }

    const btn = document.getElementById('saveBtn');
    btn.innerText = 'Commanding Slot...';
    const r = await fetch('/api/add_or_update_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({
        pair: pair,
        principal_usdt: prin,
        skim_target_pct: skim,
        rebuy_pullback_pct: pb,
        entry_price: ep
      })
    });
    const d = await r.json();
    if (!d.ok) {
      alert(d.error || 'Could not add slot');
      btn.innerText = '➕ Command & Launch Token Slot';
      return;
    }
    document.getElementById('pairInput').value = '';
    document.getElementById('principalInput').value = '';
    document.getElementById('skimPctInput').value = '';
    document.getElementById('pullbackInput').value = '';
    document.getElementById('entryInput').value = '';
    await refresh();
    btn.innerText = '✅ Slot Launched!';
    setTimeout(() => { btn.innerText = '➕ Command & Launch Token Slot'; }, 1000);
  }

  async function removeSlot(pair) {
    await fetch('/api/remove_slot', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ pair })
    });
    await refresh();
  }

  async function clearAllSlots() {
    await fetch('/api/clear_all_slots', { method: 'POST' });
    await refresh();
  }

  async function resetSlotEntry(pair) {
    await fetch('/api/reset_slot_entry', {
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
    const btn = document.getElementById('scanBtn');
    btn.innerText = 'Scanning...';
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
    btn.innerText = '⚡ Scan All Now';
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
        document.getElementById('apiKeyInput').placeholder = 'Saved on Server (Paste new to replace)';
        document.getElementById('apiSecretInput').placeholder = 'Saved on Server (Paste new to replace)';
      }

      const slots = s.slots || {};
      const keys = Object.keys(slots);
      document.getElementById('activeCount').innerText = keys.length;
      document.getElementById('fleetListText').innerText = keys.length > 0 ? keys.map(k => k.split('_')[0]).join(', ') : 'None';
      document.getElementById('clearAllBtn').style.display = keys.length > 0 ? 'inline-block' : 'none';

      const pill = document.getElementById('modePill');
      const modeText = document.getElementById('modeText');
      if (s.mode === 'LIVE') {
        pill.className = 'pill pill-live';
        modeText.innerText = 'LIVE ARMED FLEET (' + keys.length + ' TOKENS)';
      } else {
        pill.className = 'pill pill-paper';
        modeText.innerText = 'PAPER SIM FLEET (' + keys.length + ' TOKENS)';
      }

      let sumActualCost = 0;
      let sumPosVal = 0;
      const needBuf = Number(s.fee_buffer_pct || 0.40);

      if (keys.length === 0) {
        document.getElementById('fleetContainer').innerHTML = `
          <div class="empty-fleet">No tokens running — Everything is 100% empty until you command a token below.</div>
        `;
      } else {
        const cardsHtml = keys.map(pair => {
          const sl = slots[pair];
          const p = Number(sl.principal_usdt || 0);
          const cost = Number(sl.actual_cost_usdt || p);
          const pos = Number(sl.position_value_usdt || 0);
          const pnlUsd = Number(sl.unrealized_pnl_usdt || 0);
          const skimPct = Number(sl.skim_target_pct || 1.0);
          sumActualCost += cost;
          sumPosVal += pos;

          const needPct = skimPct + needBuf;
          const prog = Math.max(0, Math.min(100, (Number(sl.unrealized_pnl_pct || 0) / needPct) * 100));
          const pnlSign = sl.unrealized_pnl_pct >= 0 ? '+' : '';
          const usdSign = pnlUsd >= 0 ? '+$' : '-$';

          return `
            <div class="slot-card">
              <div class="slot-top">
                <div class="slot-pair">
                  <span>${pair}</span>
                  <span class="slot-badge">Cost: $${cost.toFixed(2)}</span>
                </div>
                <div style="font-size:0.78rem; font-weight:800;" class="${sl.unrealized_pnl_pct >= 0 ? 'c-green' : 'c-amber'}">
                  Live: ${fmtPx(sl.last_price)} (${pnlSign}${Number(sl.unrealized_pnl_pct).toFixed(2)}%)
                </div>
              </div>

              <div class="slot-metrics">
                <div>Entry Floor: <strong>${fmtPx(sl.entry_price)}</strong></div>
                <div>+${skimPct}% Net Target: <strong class="c-cyan">${fmtPx(sl.target_sell_price)}</strong></div>
                <div>Value: <strong>$${pos.toFixed(4)} (${usdSign}${Math.abs(pnlUsd).toFixed(4)})</strong></div>
                <div>Slot Vaulted: <strong class="c-green">+$${Number(sl.slot_skimmed_usdt || 0).toFixed(4)} (${sl.slot_skim_count || 0})</strong></div>
              </div>

              <div style="display:flex; justify-content:space-between; font-size:0.73rem; color:var(--muted);">
                <span>${sl.status_msg}</span>
                <strong class="c-green">${prog.toFixed(1)}%</strong>
              </div>
              <div class="track"><div class="fill" style="width:${prog.toFixed(1)}%"></div></div>

              <div class="slot-actions">
                <button class="s-btn" onclick="loadSlotToForm('${pair}', ${p}, ${skimPct}, ${sl.rebuy_pullback_pct || 0})">✏️ Edit Slot</button>
                <button class="s-btn" onclick="resetSlotEntry('${pair}')">🔄 Sync Entry</button>
                <button class="s-btn s-btn-del" onclick="removeSlot('${pair}')">🗑️ Remove</button>
              </div>
            </div>
          `;
        }).join('');
        document.getElementById('fleetContainer').innerHTML = `<div class="fleet-grid">${cardsHtml}</div>`;
      }

      document.getElementById('totalPrincipal').innerText = '$' + sumActualCost.toFixed(2);
      document.getElementById('totalPosVal').innerText = '$' + sumPosVal.toFixed(4);
      document.getElementById('liveSkimmed').innerText = '+$' + Number(s.live_skimmed_usdt || 0).toFixed(4);
      document.getElementById('liveSkimCount').innerText = s.live_skim_count || 0;
      document.getElementById('paperSkimmed').innerText = '+$' + Number(s.paper_skimmed_usdt || 0).toFixed(4);
      document.getElementById('paperSkimCount').innerText = s.paper_skim_count || 0;
      document.getElementById('usdtBal').innerText = '$' + Number(s.usdt_balance).toFixed(2);

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
