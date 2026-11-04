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

STATE_FILE = "vaultskimmer_v4_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=12,
    pool_maxsize=12,
    max_retries=Retry(total=2, backoff_factor=0.15, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)

PAIR_META_CACHE = {}

DEFAULT_STATE = {
    "mode": "PAPER",  # "PAPER" (Active Paper Sim) or "LIVE" (Real Gate.io Orders)
    "pair": "SUI_USDT",
    "principal_usdt": 10.0,
    "skim_target_pct": 1.0,
    "fee_buffer_pct": 0.20,
    "rebuy_pullback_pct": 0.0,
    "entry_price": 0.0,
    "last_skim_sell_price": 0.0,
    "phase": "WAITING_TO_BUY",
    "api_key": os.environ.get("GATE_API_KEY", ""),
    "api_secret": os.environ.get("GATE_API_SECRET", ""),
    # Live & Paper Separate/Unified Trackers
    "paper_base_bal": 0.0,
    "paper_usdt_vault": 0.0,
    "total_skimmed_usdt": 0.0,
    "skim_count": 0,
    "last_price": 0.0,
    "high_24h": 0.0,
    "low_24h": 0.0,
    "change_24h_pct": 0.0,
    "base_balance": 0.0,
    "usdt_balance": 0.0,
    "position_value_usdt": 0.0,
    "target_sell_price": 0.0,
    "target_rebuy_price": 0.0,
    "unrealized_pnl_usdt": 0.0,
    "unrealized_pnl_pct": 0.0,
    "status_msg": "Booting VaultSkimmer v4.0 Engine...",
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
    p = (raw_pair or "SUI_USDT").strip().upper().replace("/", "_").replace("-", "_").replace(" ", "")
    if not p:
        return "SUI_USDT"
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
                STATE["pair"] = normalize_pair(STATE.get("pair", "SUI_USDT"))
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
    STATE["logs"] = STATE["logs"][:90]


def gate_sign(method, url, query_string="", body_string=""):
    key = STATE.get("api_key", "").strip()
    secret = STATE.get("api_secret", "").strip()
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


def fetch_market_snapshot(pair):
    r = HTTP_SESSION.get(
        f"{GATE_HOST}{GATE_PREFIX}/spot/tickers",
        params={"currency_pair": pair},
        timeout=5
    )
    if r.status_code != 200:
        raise ValueError(f"Token pair '{pair}' not found on Gate.io")
    data = r.json()
    if not data or not isinstance(data, list):
        raise ValueError(f"Token pair '{pair}' not found on Gate.io")

    t = data[0]
    last_price = float(t["last"])
    high_24h = float(t.get("high_24h") or last_price)
    low_24h = float(t.get("low_24h") or last_price)
    change_24h = float(t.get("change_percentage") or 0.0)

    now = time.time()
    cached = PAIR_META_CACHE.get(pair)
    if not cached or (now - cached["ts"] > 3600):
        p_resp = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs/{pair}",
            timeout=5
        )
        p_info = p_resp.json() if p_resp.status_code == 200 else {}
        cached = {
            "amount_prec": int(p_info.get("amount_precision", 4)),
            "price_prec": int(p_info.get("precision", 4)),
            "min_quote": float(p_info.get("min_quote_amount", 1.0)),
            "min_base": float(p_info.get("min_base_amount", 0.0001)),
            "ts": now
        }
        PAIR_META_CACHE[pair] = cached

    return (
        last_price,
        high_24h,
        low_24h,
        change_24h,
        cached["amount_prec"],
        cached["price_prec"],
        cached["min_quote"],
        cached["min_base"]
    )


def fetch_spot_balances(base_curr):
    url = f"{GATE_PREFIX}/spot/accounts"
    headers = gate_sign("GET", url, "", "")
    r = HTTP_SESSION.get(f"{GATE_HOST}{url}", headers=headers, timeout=5)
    r.raise_for_status()
    accounts = r.json()
    base_avail = 0.0
    usdt_avail = 0.0
    for acc in accounts:
        curr = acc.get("currency")
        if curr == base_curr:
            base_avail = float(acc.get("available", 0.0))
        elif curr == "USDT":
            usdt_avail = float(acc.get("available", 0.0))
    return base_avail, usdt_avail


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


def run_skim_cycle(force_manual=False):
    if not cycle_lock.acquire(blocking=force_manual):
        return

    t0 = time.time()
    try:
        with state_lock:
            pair = normalize_pair(STATE["pair"])
            STATE["pair"] = pair
            base_curr = pair.split("_")[0]
            principal = float(STATE["principal_usdt"])
            skim_pct = float(STATE["skim_target_pct"])
            fee_buf = float(STATE["fee_buffer_pct"])
            pullback_pct = float(STATE.get("rebuy_pullback_pct", 0.0))
            mode = STATE.get("mode", "PAPER")
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))

        price, high_24, low_24, chg_24, amt_prec, px_prec, min_quote, min_base = fetch_market_snapshot(pair)

        real_base_bal, real_usdt_bal = (0.0, 0.0)
        if has_keys:
            try:
                real_base_bal, real_usdt_bal = fetch_spot_balances(base_curr)
            except Exception as api_err:
                if mode == "LIVE":
                    add_log(f"Gate.io Balance Auth Error: {str(api_err)[:60]}")

        with state_lock:
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_worker_heartbeat"] = time.time()
            STATE["last_price"] = price
            STATE["high_24h"] = high_24
            STATE["low_24h"] = low_24
            STATE["change_24h_pct"] = chg_24

            effective_gain_pct = skim_pct + fee_buf
            last_sell_px = STATE.get("last_skim_sell_price", 0.0)
            if last_sell_px > 0 and pullback_pct > 0:
                rebuy_px = last_sell_px * (1.0 - (pullback_pct / 100.0))
            else:
                rebuy_px = price
            STATE["target_rebuy_price"] = round(rebuy_px, px_prec)

            ms = int((time.time() - t0) * 1000)
            STATE["last_scan_ms"] = ms
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")

            # =========================================================================
            # MODE 1: ACTIVE PAPER SIMULATION (Works immediately with $0 risk!)
            # =========================================================================
            if mode == "PAPER":
                STATE["usdt_balance"] = real_usdt_bal if has_keys else principal
                paper_tokens = float(STATE.get("paper_base_bal", 0.0))
                pos_val = paper_tokens * price

                # Auto-corrected Entry Floor if user accidentally typed a huge number (e.g. 10 on a $2 coin)
                if STATE["entry_price"] > price * 2.5 or STATE["entry_price"] < price * 0.2:
                    STATE["entry_price"] = 0.0

                # Paper Auto-Buy initial position or Rebuy after skim
                if paper_tokens <= 0 or pos_val < (principal * 0.25):
                    if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
                        STATE["phase"] = "WAITING_TO_BUY"
                        STATE["base_balance"] = 0.0
                        STATE["position_value_usdt"] = 0.0
                        STATE["status_msg"] = (
                            f"[PAPER SIM] Waiting for -{pullback_pct}% pullback to ${rebuy_px:.4f} (Live: ${price:.4f})"
                        )
                        save_state()
                        return

                    sim_tokens = principal / price
                    STATE["paper_base_bal"] = sim_tokens
                    STATE["base_balance"] = sim_tokens
                    STATE["position_value_usdt"] = round(principal, 4)
                    if STATE["entry_price"] <= 0:
                        STATE["entry_price"] = price
                    target_px = STATE["entry_price"] * (1.0 + (effective_gain_pct / 100.0))
                    STATE["target_sell_price"] = round(target_px, px_prec)
                    STATE["phase"] = "HOLDING_FOR_SKIM"
                    STATE["unrealized_pnl_usdt"] = 0.0
                    STATE["unrealized_pnl_pct"] = 0.0
                    STATE["status_msg"] = (
                        f"[PAPER SIM] Deployed ${principal:.2f} in {base_curr} @ ${ STATE['entry_price']:.4f} "
                        f"→ Target +1% Skim @ ${target_px:.4f}"
                    )
                    add_log(
                        f"[PAPER SIM] Bought {sim_tokens:.4f} {base_curr} (${principal:.2f}) @ ${price:.4f} "
                        f"| Target Sell: ${target_px:.4f}"
                    )
                    save_state()
                    return

                # Holding Paper Position: Check for +1% Skim
                STATE["phase"] = "HOLDING_FOR_SKIM"
                STATE["base_balance"] = paper_tokens
                STATE["position_value_usdt"] = round(pos_val, 4)
                entry_px = STATE["entry_price"] if STATE["entry_price"] > 0 else price
                target_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
                STATE["target_sell_price"] = round(target_px, px_prec)

                STATE["unrealized_pnl_usdt"] = round(pos_val - principal, 4)
                STATE["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)

                if price >= target_px:
                    gross_val = paper_tokens * price
                    net_val = gross_val * (1.0 - (fee_buf / 100.0))
                    sim_profit = max(0.01, round(net_val - principal, 4))

                    STATE["total_skimmed_usdt"] = round(STATE["total_skimmed_usdt"] + sim_profit, 4)
                    STATE["skim_count"] += 1
                    old_entry = entry_px
                    STATE["last_skim_sell_price"] = price

                    STATE["skim_ledger"].insert(0, {
                        "time": datetime.utcnow().strftime("%H:%M:%S"),
                        "pair": f"{pair} (SIM)",
                        "cycle": f"#{STATE['skim_count']}",
                        "buy_px": f"${old_entry:.4f}",
                        "sell_px": f"${price:.4f}",
                        "profit": f"+${sim_profit:.4f}",
                        "principal_redeployed": f"${principal:.2f}"
                    })
                    STATE["skim_ledger"] = STATE["skim_ledger"][:40]

                    if pullback_pct <= 0:
                        new_tokens = principal / price
                        STATE["paper_base_bal"] = new_tokens
                        STATE["base_balance"] = new_tokens
                        STATE["entry_price"] = price
                        new_target = price * (1.0 + (effective_gain_pct / 100.0))
                        STATE["target_sell_price"] = round(new_target, px_prec)
                        STATE["position_value_usdt"] = round(principal, 4)
                        STATE["status_msg"] = (
                            f"[PAPER SIM] SKIMMED +${sim_profit:.4f} & REBOUGHT ${principal:.2f} @ ${price:.4f} "
                            f"→ Next Target ${new_target:.4f}"
                        )
                        add_log(
                            f"[PAPER SKIM #{STATE['skim_count']}] Sold @ ${price:.4f} (+${sim_profit:.4f} Profit) "
                            f"& Instantly Rebought ${principal:.2f} @ ${price:.4f}!"
                        )
                    else:
                        STATE["paper_base_bal"] = 0.0
                        STATE["base_balance"] = 0.0
                        STATE["phase"] = "WAITING_TO_BUY"
                        STATE["status_msg"] = (
                            f"[PAPER SIM] Skimmed +${sim_profit:.4f}! Waiting for -{pullback_pct}% pullback."
                        )
                else:
                    if price < entry_px:
                        STATE["status_msg"] = (
                            f"[PAPER SIM] Holding (Capital Protected) | Live ${price:.4f} < Entry ${entry_px:.4f} "
                            f"(Target ${target_px:.4f})"
                        )
                    else:
                        STATE["status_msg"] = (
                            f"[PAPER SIM] Climbing ({STATE['unrealized_pnl_pct']:+.2f}%) | "
                            f"Entry ${entry_px:.4f} → Skim Target ${target_px:.4f}"
                        )
                save_state()
                return

            # =========================================================================
            # MODE 2: LIVE ARMED TRADING ON GATE.IO
            # =========================================================================
            if not has_keys:
                STATE["status_msg"] = "LIVE ARMED - Paste Gate.io API Key & Secret to Trade Live"
                save_state()
                return

            STATE["base_balance"] = real_base_bal
            STATE["usdt_balance"] = real_usdt_bal
            pos_val = real_base_bal * price
            STATE["position_value_usdt"] = round(pos_val, 4)

            holding_tokens = pos_val >= max(min_quote * 1.05, principal * 0.40)
            if holding_tokens:
                STATE["phase"] = "HOLDING_FOR_SKIM"
                if STATE["entry_price"] <= 0 or STATE["entry_price"] > price * 2.5:
                    STATE["entry_price"] = price
                    add_log(f"[LIVE] Locked Entry Floor @ ${price:.4f} for {pair}")
            else:
                STATE["phase"] = "WAITING_TO_BUY"

            entry_px = STATE["entry_price"] if STATE["entry_price"] > 0 else price
            target_sell_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
            STATE["target_sell_price"] = round(target_sell_px, px_prec)

            if holding_tokens and entry_px > 0:
                STATE["unrealized_pnl_usdt"] = round(pos_val - principal, 4)
                STATE["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)
            else:
                STATE["unrealized_pnl_usdt"] = 0.0
                STATE["unrealized_pnl_pct"] = 0.0

            if STATE["phase"] == "HOLDING_FOR_SKIM":
                if price < target_sell_px:
                    if price < entry_px:
                        STATE["status_msg"] = (
                            f"[LIVE] Capital Protected Hold | Live ${price:.4f} < Entry Floor ${entry_px:.4f}"
                        )
                    else:
                        STATE["status_msg"] = (
                            f"[LIVE] Climbing ({STATE['unrealized_pnl_pct']:+.2f}%) | "
                            f"Entry ${entry_px:.4f} → Target ${target_sell_px:.4f}"
                        )
                    save_state()
                    return

                sell_amt_str = format_dec(real_base_bal, amt_prec)
                sell_px_str = format_dec(price * 0.998, px_prec)
                code, resp = place_gate_order(pair, "sell", sell_amt_str, sell_px_str)

                if code in (200, 201):
                    gross_proceeds = real_base_bal * price
                    net_proceeds = gross_proceeds * (1.0 - (fee_buf / 100.0))
                    skimmed_profit = max(0.01, round(net_proceeds - principal, 4))

                    STATE["total_skimmed_usdt"] = round(STATE["total_skimmed_usdt"] + skimmed_profit, 4)
                    STATE["skim_count"] += 1
                    old_entry = STATE["entry_price"]
                    STATE["last_skim_sell_price"] = price

                    STATE["skim_ledger"].insert(0, {
                        "time": datetime.utcnow().strftime("%H:%M:%S"),
                        "pair": pair,
                        "cycle": f"#{STATE['skim_count']}",
                        "buy_px": f"${old_entry:.4f}",
                        "sell_px": f"${price:.4f}",
                        "profit": f"+${skimmed_profit:.4f}",
                        "principal_redeployed": f"${principal:.2f}"
                    })
                    STATE["skim_ledger"] = STATE["skim_ledger"][:40]

                    add_log(
                        f"[LIVE SKIM #{STATE['skim_count']}] Sold {sell_amt_str} {base_curr} @ ${price:.4f} "
                        f"| Profit Vaulted: +${skimmed_profit:.4f} USDT"
                    )

                    if pullback_pct <= 0.0:
                        time.sleep(0.35)
                        rebuy_amt = principal / price
                        rebuy_amt_str = format_dec(rebuy_amt, amt_prec)
                        rebuy_px_str = format_dec(price * 1.002, px_prec)
                        b_code, b_resp = place_gate_order(pair, "buy", rebuy_amt_str, rebuy_px_str)
                        if b_code in (200, 201):
                            STATE["entry_price"] = price
                            STATE["phase"] = "HOLDING_FOR_SKIM"
                            new_target = price * (1.0 + (effective_gain_pct / 100.0))
                            STATE["target_sell_price"] = round(new_target, px_prec)
                            STATE["status_msg"] = (
                                f"[LIVE] Skimmed +${skimmed_profit:.4f} & Rebought ${principal:.2f} @ ${price:.4f}"
                            )
                            add_log(f"[LIVE AUTO-REBUY] Redeployed ${principal:.2f} @ ${price:.4f}")
                        else:
                            STATE["phase"] = "WAITING_TO_BUY"
                            add_log(f"[LIVE] Rebuy queued ({b_code}): {b_resp.get('message', '')}")
                    else:
                        STATE["phase"] = "WAITING_TO_BUY"
                else:
                    add_log(f"[LIVE] Sell rejected ({code}): {resp.get('message', resp)}")
                save_state()
                return

            if STATE["phase"] == "WAITING_TO_BUY":
                if real_usdt_bal < principal:
                    STATE["status_msg"] = (
                        f"[LIVE] Insufficient USDT: Have ${real_usdt_bal:.2f} USDT, Need ${principal:.2f} Principal"
                    )
                    save_state()
                    return

                if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
                    STATE["status_msg"] = f"[LIVE] Waiting for Rebuy Pullback to ${rebuy_px:.4f}"
                    save_state()
                    return

                buy_amt = principal / price
                buy_amt_str = format_dec(buy_amt, amt_prec)
                buy_px_str = format_dec(price * 1.002, px_prec)
                code, resp = place_gate_order(pair, "buy", buy_amt_str, buy_px_str)

                if code in (200, 201):
                    STATE["entry_price"] = price
                    STATE["phase"] = "HOLDING_FOR_SKIM"
                    new_target = price * (1.0 + (effective_gain_pct / 100.0))
                    STATE["target_sell_price"] = round(new_target, px_prec)
                    STATE["status_msg"] = (
                        f"[LIVE] Bought ${principal:.2f} {base_curr} @ ${price:.4f} → Target ${new_target:.4f}"
                    )
                    add_log(f"[LIVE BUY] Bought {buy_amt_str} {base_curr} (${principal:.2f}) @ ${price:.4f}")
                else:
                    add_log(f"[LIVE] Buy rejected ({code}): {resp.get('message', resp)}")
                save_state()

    except Exception as e:
        with state_lock:
            STATE["status_msg"] = f"Alert: {str(e)[:70]}"
            add_log(f"Engine notice: {str(e)[:90]}")
    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("VaultSkimmer v4.0 Self-Healing Worker Started (Paper Sim & Live Trading Ready).")
    while True:
        run_skim_cycle(force_manual=False)
        time.sleep(3.0)


def ensure_worker_alive():
    """Guarantees the background thread is alive inside Gunicorn worker processes."""
    global worker_started
    now = time.time()
    if not worker_started or (now - STATE.get("last_worker_heartbeat", 0) > 12):
        worker_started = True
        STATE["last_worker_heartbeat"] = now
        t = threading.Thread(target=background_worker, daemon=True)
        t.start()


@app.before_request
def check_worker():
    ensure_worker_alive()


# =========================================================================
# PWA AUTO-INSTALL ENDPOINTS
# =========================================================================
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
        "name": "VaultSkimmer v4.0 Peak Engine",
        "short_name": "VaultSkimmer",
        "description": "Any-Token +1% Spot Skimmer & Auto-Rebuy Engine",
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


# =========================================================================
# API ENDPOINTS
# =========================================================================
@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "VaultSkimmer-v4", "cycle": STATE["cycle_count"]})


@app.route("/api/cron")
def api_cron():
    run_skim_cycle(force_manual=False)
    return jsonify({
        "ok": True,
        "mode": STATE["mode"],
        "pair": STATE["pair"],
        "principal_usdt": STATE["principal_usdt"],
        "entry_price": STATE["entry_price"],
        "last_price": STATE["last_price"],
        "target_sell_price": STATE["target_sell_price"],
        "total_skimmed_usdt": STATE["total_skimmed_usdt"],
        "cycle": STATE["cycle_count"]
    })


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    run_skim_cycle(force_manual=True)
    return jsonify({"ok": True, "scan_ms": STATE["last_scan_ms"]})


@app.route("/api/state")
def api_state():
    if STATE["cycle_count"] == 0:
        run_skim_cycle(force_manual=False)
    with state_lock:
        safe = STATE.copy()
        safe["api_key"] = "Saved on server" if STATE.get("api_key") else ""
        safe["api_secret"] = "Saved on server" if STATE.get("api_secret") else ""
        return jsonify(safe)


@app.route("/api/config", methods=["POST"])
def api_config():
    data = request.json or {}
    with state_lock:
        if "pair" in data and data["pair"]:
            new_pair = normalize_pair(data["pair"])
            if new_pair != STATE["pair"]:
                STATE["pair"] = new_pair
                STATE["entry_price"] = 0.0
                STATE["paper_base_bal"] = 0.0
                STATE["last_skim_sell_price"] = 0.0
                add_log(f"Token switched to {new_pair} (Entry Floor & Paper position reset).")

        if "principal_usdt" in data and data["principal_usdt"] != "":
            new_p = max(1.0, float(data["principal_usdt"]))
            if new_p != STATE["principal_usdt"]:
                STATE["principal_usdt"] = new_p
                if STATE["mode"] == "PAPER":
                    STATE["paper_base_bal"] = 0.0  # Re-deploy paper position with new principal
                    STATE["entry_price"] = 0.0

        if "skim_target_pct" in data and data["skim_target_pct"] != "":
            STATE["skim_target_pct"] = max(0.1, float(data["skim_target_pct"]))

        if "rebuy_pullback_pct" in data and data["rebuy_pullback_pct"] != "":
            STATE["rebuy_pullback_pct"] = max(0.0, float(data["rebuy_pullback_pct"]))

        if "entry_price" in data:
            raw_ep = str(data["entry_price"]).strip()
            if raw_ep == "" or float(raw_ep) <= 0:
                STATE["entry_price"] = 0.0
            else:
                STATE["entry_price"] = float(raw_ep)

        if data.get("api_key") and data["api_key"] != "Saved on server":
            STATE["api_key"] = data["api_key"].strip()
        if data.get("api_secret") and data["api_secret"] != "Saved on server":
            STATE["api_secret"] = data["api_secret"].strip()

        save_state()
        add_log(
            f"Saved: {STATE['pair']} | Principal: ${STATE['principal_usdt']:.2f} "
            f"| Skim: +{STATE['skim_target_pct']}%"
        )

    run_skim_cycle(force_manual=True)
    return jsonify({"ok": True, "principal_usdt": STATE["principal_usdt"], "pair": STATE["pair"]})


@app.route("/api/toggle_mode", methods=["POST"])
def api_toggle_mode():
    with state_lock:
        STATE["mode"] = "LIVE" if STATE.get("mode") == "PAPER" else "PAPER"
        add_log(f"Switched Engine Mode to: {STATE['mode']}")
        save_state()
    run_skim_cycle(force_manual=True)
    return jsonify({"ok": True, "mode": STATE["mode"]})


@app.route("/api/reset_entry", methods=["POST"])
def api_reset_entry():
    with state_lock:
        STATE["entry_price"] = STATE["last_price"] if STATE["last_price"] > 0 else 0.0
        if STATE["mode"] == "PAPER" and STATE["last_price"] > 0:
            STATE["paper_base_bal"] = STATE["principal_usdt"] / STATE["last_price"]
        add_log(f"Entry Floor synced to live price: ${STATE['entry_price']:.4f}")
        save_state()
    run_skim_cycle(force_manual=True)
    return jsonify({"ok": True, "entry_price": STATE["entry_price"]})


HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no" />
  <meta name="theme-color" content="#060b16" />
  <link rel="manifest" href="/manifest.json" />
  <link rel="icon" type="image/svg+xml" href="/icon.svg" />
  <title>VaultSkimmer v4.0 | Any-Token +1% Profit Engine</title>
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
      --text: #f8fafc;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', -apple-system, system-ui, sans-serif; }
    body {
      background: radial-gradient(circle at 50% 0%, #0f1d3a 0%, #050811 70%);
      color: var(--text); padding: 14px; min-height: 100vh;
    }
    .wrap { max-width: 1140px; margin: 0 auto; }

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

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card {
      background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35);
    }
    .lbl { color: var(--muted); font-size: 0.7rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.6px; }
    .val { font-size: 1.48rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .sub { font-size: 0.77rem; color: var(--muted); }
    .c-green { color: #34d399; }
    .c-cyan { color: #22d3ee; }
    .c-amber { color: #fbbf24; }

    .bar-card { background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 18px; margin-bottom: 14px; }
    .bar-top { display: flex; justify-content: space-between; align-items: center; font-size: 0.82rem; font-weight: 800; margin-bottom: 9px; flex-wrap: wrap; gap: 6px; }
    .track { height: 13px; background: #04070e; border-radius: 99px; overflow: hidden; border: 1px solid #1e293b; }
    .fill {
      height: 100%; width: 0%; background: linear-gradient(90deg, #06b6d4, #10b981);
      transition: width 0.35s ease; box-shadow: 0 0 14px var(--emerald);
    }

    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(335px, 1fr)); gap: 14px; margin-bottom: 14px; }
    .sec-title { font-size: 0.85rem; font-weight: 900; text-transform: uppercase; color: #38bdf8; margin-bottom: 12px; display: flex; justify-content: space-between; align-items: center; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.73rem; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    input {
      width: 100%; background: #050811; border: 1px solid #26334d; color: var(--text);
      padding: 10px 11px; border-radius: 10px; font-size: 0.87rem; font-weight: 700;
    }
    input:focus { outline: none; border-color: var(--cyan); }

    .chip-bar { display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 10px; }
    .chip {
      background: #111c33; border: 1px solid #26334d; color: #cbd5e1; font-size: 0.71rem;
      font-weight: 700; padding: 4px 9px; border-radius: 99px; cursor: pointer;
    }
    .chip:hover { border-color: var(--cyan); color: #fff; }

    .btns { display: flex; flex-wrap: wrap; gap: 9px; margin-top: 12px; }
    button.ctrl { cursor: pointer; border: none; border-radius: 10px; padding: 11px 14px; font-weight: 800; font-size: 0.83rem; }
    .b-save { background: linear-gradient(135deg, #10b981, #059669); color: #022c22; flex: 1; }
    .b-arm { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; flex: 1; }
    .b-lock { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; width: 100%; }

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
        <div class="brand-title">VaultSkimmer <span class="v-badge">v4.0 ANY-TOKEN + PAPER SIM</span></div>
        <div class="sub-desc">Type Any Token on Gate.io • Live Paper Sim & Real Spot Execution • +1% Skim & Instant Principal Auto-Rebuy</div>
      </div>
      <div class="top-actions">
        <button class="btn-install" id="installBtn" onclick="triggerInstall()">📲 Install App</button>
        <button class="btn-scan" onclick="manualScan()" id="scanBtn">⚡ Scan Now</button>
      </div>
    </div>

    <div class="telemetry-strip">
      <div>
        <span id="modePill" class="pill pill-paper"><span class="dot"></span> <span id="modeText">PAPER SIM ACTIVE</span></span>
        <span style="margin-left:8px;">Phase: <strong class="c-cyan" id="phaseText">HOLDING</strong></span>
      </div>
      <div>
        24h Range (<strong id="pairBadge">SUI_USDT</strong>): <strong id="low24">$0.00</strong> – <strong id="high24">$0.00</strong> (<span id="chg24">0.00%</span>)
      </div>
      <div>
        Last Scan: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-green" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)
      </div>
    </div>
  </div>

  <div class="grid-4">
    <div class="card">
      <div class="lbl">Protected Principal (Active Base)</div>
      <div class="val c-cyan" id="principalVal">$10.00</div>
      <div class="sub">Position Value: <strong id="posVal">$0.0000</strong> (<span id="baseBal">0.00</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Entry Floor → +1% Skim Target</div>
      <div class="val" id="entryToTarget">$0.00 → $0.00</div>
      <div class="sub">Live Price: <strong class="c-amber" id="livePrice">$0.0000</strong> (<span id="pnlPct">0.00%</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Next +1% Skim Payout</div>
      <div class="val c-amber" id="expectedSkim">+$0.1000</div>
      <div class="sub">Rebuy Mode: <strong id="rebuyMode">Instant @ New Price</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Realized USDT Profit Vault</div>
      <div class="val c-green" id="totalSkimmed">+$0.0000</div>
      <div class="sub">1% Skims Won: <strong id="skimCount">0</strong> | Wallet USDT: <strong id="usdtBal">$0.00</strong></div>
    </div>
  </div>

  <div class="bar-card">
    <div class="bar-top">
      <span id="statusBanner">Syncing Live Gate.io Feed...</span>
      <span id="progressLabel" class="c-green">0.0% to +1% Skim</span>
    </div>
    <div class="track"><div class="fill" id="progressBar"></div></div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="sec-title">
        <span>⚙️ Any-Token, Principal & Auto-Rebuy Controls</span>
      </div>

      <label>Quick-Pick Popular Tokens or Type ANY Gate.io Token Below:</label>
      <div class="chip-bar">
        <span class="chip" onclick="pickPair('SUI_USDT')">SUI</span>
        <span class="chip" onclick="pickPair('QNT_USDT')">QNT</span>
        <span class="chip" onclick="pickPair('PEPE_USDT')">PEPE</span>
        <span class="chip" onclick="pickPair('SOL_USDT')">SOL</span>
        <span class="chip" onclick="pickPair('ETH_USDT')">ETH</span>
        <span class="chip" onclick="pickPair('BTC_USDT')">BTC</span>
        <span class="chip" onclick="pickPair('DOGE_USDT')">DOGE</span>
        <span class="chip" onclick="pickPair('XRP_USDT')">XRP</span>
      </div>

      <div class="row">
        <div>
          <label>Type Any Token (e.g. SUI, QNT, PEPE, ADA_USDT)</label>
          <input type="text" id="pairInput" list="tokenSuggestions" placeholder="Type any token..." value="SUI_USDT" />
          <datalist id="tokenSuggestions">
            <option value="SUI_USDT"></option>
            <option value="QNT_USDT"></option>
            <option value="PEPE_USDT"></option>
            <option value="SOL_USDT"></option>
            <option value="ETH_USDT"></option>
            <option value="BTC_USDT"></option>
            <option value="DOGE_USDT"></option>
            <option value="XRP_USDT"></option>
            <option value="ADA_USDT"></option>
            <option value="AVAX_USDT"></option>
            <option value="LINK_USDT"></option>
            <option value="WIF_USDT"></option>
          </datalist>
        </div>
        <div>
          <label>Protected Principal (USDT)</label>
          <input type="number" id="principalInput" step="0.5" value="10" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Skim Profit Target (%)</label>
          <input type="number" id="skimPctInput" step="0.1" value="1.0" />
        </div>
        <div>
          <label>Rebuy Pullback % (0 = Instant)</label>
          <input type="number" id="pullbackInput" step="0.05" value="0.0" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Custom Entry Floor ($ - Leave Blank for Auto)</label>
          <input type="number" id="entryInput" step="0.0001" placeholder="Auto-locks live price" />
        </div>
        <div>
          <label>Gate.io v4 API Key (For Live Mode)</label>
          <input type="password" id="apiKeyInput" placeholder="Saved on server" />
        </div>
      </div>
      <div class="row">
        <div style="grid-column: span 2;">
          <label>Gate.io v4 API Secret (For Live Mode)</label>
          <input type="password" id="apiSecretInput" placeholder="Saved on server" />
        </div>
      </div>
      <div class="btns">
        <button class="ctrl b-save" onclick="saveSettings()" id="saveBtn">💾 Save & Sync Now</button>
        <button class="ctrl b-arm" onclick="toggleMode()" id="armBtn">🔀 Switch PAPER / LIVE</button>
        <button class="ctrl b-lock" onclick="resetEntry()">🔄 Sync Entry Floor to Current Live Price</button>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">
        <span>💎 Verified +1% Skim & Auto-Rebuy Ledger</span>
      </div>
      <div style="max-height: 310px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Flip</th>
              <th>Buy → +1% Sell</th>
              <th>Profit</th>
              <th>Rebought</th>
            </tr>
          </thead>
          <tbody id="ledgerBody">
            <tr><td colspan="5" style="color:#64748b; text-align:center; padding:22px;">No flips logged yet — Paper & Live +1% skims appear here automatically.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title"><span>📡 Live Engine Telemetry Logs</span></div>
    <div class="logs" id="logBox"></div>
  </div>
</div>

<script>
  let initialized = false;
  let deferredPrompt = null;

  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => navigator.serviceWorker.register('/sw.js').catch(() => {}));
  }
  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
  });

  async function triggerInstall() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      deferredPrompt = null;
    } else {
      alert('In Chrome/Edge/Brave: Click the Install icon in the right side of your address bar, or tap Menu (⋮) -> Install App.');
    }
  }

  function pickPair(p) {
    document.getElementById('pairInput').value = p;
    document.getElementById('entryInput').value = '';
    saveSettings();
  }

  async function manualScan() {
    const btn = document.getElementById('scanBtn');
    btn.innerText = 'Scanning...';
    await fetch('/api/scan_now', { method: 'POST' });
    await refresh();
    btn.innerText = '⚡ Scan Now';
  }

  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('cycleNum').innerText = s.cycle_count || 0;
      document.getElementById('phaseText').innerText = s.phase;
      document.getElementById('pairBadge').innerText = s.pair;
      document.getElementById('low24').innerText = '$' + Number(s.low_24h || 0).toFixed(4);
      document.getElementById('high24').innerText = '$' + Number(s.high_24h || 0).toFixed(4);
      const chg = Number(s.change_24h_pct || 0);
      document.getElementById('chg24').innerText = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';

      const pill = document.getElementById('modePill');
      const modeText = document.getElementById('modeText');
      if (s.mode === 'LIVE') {
        pill.className = 'pill pill-live';
        modeText.innerText = 'LIVE ARMED (' + s.pair + ')';
      } else {
        pill.className = 'pill pill-paper';
        modeText.innerText = 'PAPER SIM ACTIVE (' + s.pair + ')';
      }

      document.getElementById('principalVal').innerText = '$' + Number(s.principal_usdt).toFixed(2);
      document.getElementById('posVal').innerText = '$' + Number(s.position_value_usdt).toFixed(4);
      document.getElementById('baseBal').innerText = Number(s.base_balance).toFixed(4) + ' ' + s.pair.split('_')[0];
      document.getElementById('entryToTarget').innerText = '$' + Number(s.entry_price).toFixed(4) + ' → $' + Number(s.target_sell_price).toFixed(4);
      document.getElementById('livePrice').innerText = '$' + Number(s.last_price).toFixed(4);
      document.getElementById('pnlPct').innerText = (s.unrealized_pnl_pct >= 0 ? '+' : '') + Number(s.unrealized_pnl_pct).toFixed(2) + '%';

      const expProfit = Number(s.principal_usdt) * (Number(s.skim_target_pct) / 100.0);
      document.getElementById('expectedSkim').innerText = '+$' + expProfit.toFixed(4);
      document.getElementById('rebuyMode').innerText = Number(s.rebuy_pullback_pct) <= 0
        ? 'Instant @ New Price'
        : '-' + s.rebuy_pullback_pct + '% Pullback ($' + Number(s.target_rebuy_price).toFixed(4) + ')';

      document.getElementById('totalSkimmed').innerText = '+$' + Number(s.total_skimmed_usdt).toFixed(4);
      document.getElementById('skimCount').innerText = s.skim_count;
      document.getElementById('usdtBal').innerText = '$' + Number(s.usdt_balance).toFixed(2);
      document.getElementById('statusBanner').innerText = s.status_msg;

      const needPct = Number(s.skim_target_pct) + Number(s.fee_buffer_pct);
      const pct = Math.max(0, Math.min(100, (s.unrealized_pnl_pct / needPct) * 100));
      document.getElementById('progressBar').style.width = pct.toFixed(1) + '%';
      document.getElementById('progressLabel').innerText = pct.toFixed(1) + '% to +' + s.skim_target_pct + '% Skim';

      if (!initialized) {
        document.getElementById('pairInput').value = s.pair;
        document.getElementById('principalInput').value = s.principal_usdt;
        document.getElementById('skimPctInput').value = s.skim_target_pct;
        document.getElementById('pullbackInput').value = s.rebuy_pullback_pct;
        if (s.entry_price > 0) document.getElementById('entryInput').value = s.entry_price;
        initialized = true;
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
      }

      document.getElementById('logBox').innerHTML = (s.logs || []).join('<br/>');
    } catch (e) {}
  }

  async function saveSettings() {
    const btn = document.getElementById('saveBtn');
    btn.innerText = 'Syncing...';
    const payload = {
      pair: document.getElementById('pairInput').value,
      principal_usdt: parseFloat(document.getElementById('principalInput').value),
      skim_target_pct: parseFloat(document.getElementById('skimPctInput').value),
      rebuy_pullback_pct: parseFloat(document.getElementById('pullbackInput').value),
      entry_price: document.getElementById('entryInput').value,
      api_key: document.getElementById('apiKeyInput').value,
      api_secret: document.getElementById('apiSecretInput').value
    };
    await fetch('/api/config', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify(payload)
    });
    await refresh();
    btn.innerText = '✅ Synced!';
    setTimeout(() => { btn.innerText = '💾 Save & Sync Now'; }, 1000);
  }

  async function toggleMode() {
    await fetch('/api/toggle_mode', { method: 'POST' });
    await refresh();
  }

  async function resetEntry() {
    const r = await fetch('/api/reset_entry', { method: 'POST' });
    const d = await r.json();
    if (d.entry_price) document.getElementById('entryInput').value = d.entry_price;
    await refresh();
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
