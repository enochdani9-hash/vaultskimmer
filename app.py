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

STATE_FILE = "vaultskimmer_v3_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

# Persistent high-speed HTTP Session (reuses SSL/TCP connections for ultra-smooth cycles)
HTTP_SESSION = requests.Session()
adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    max_retries=Retry(total=2, backoff_factor=0.2, status_forcelist=[502, 503, 504])
)
HTTP_SESSION.mount("https://", adapter)

# Cache pair precision metadata so we don't re-fetch static pair rules every 3 seconds
PAIR_META_CACHE = {}

DEFAULT_STATE = {
    "armed": False,
    "pair": "SUI_USDT",
    "principal_usdt": 25.0,
    "skim_target_pct": 1.0,
    "fee_buffer_pct": 0.20,
    "rebuy_pullback_pct": 0.0,
    "entry_price": 0.0,
    "last_skim_sell_price": 0.0,
    "phase": "WAITING_TO_BUY",
    "api_key": os.environ.get("GATE_API_KEY", ""),
    "api_secret": os.environ.get("GATE_API_SECRET", ""),
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
    "status_msg": "STANDBY - Configure Principal & Arm Engine",
    "last_scan_ms": 0,
    "cycle_count": 0,
    "last_update": "--:--:--",
    "skim_ledger": [],
    "logs": []
}

state_lock = threading.Lock()
cycle_lock = threading.Lock()  # Prevents overlapping cycles between background worker & cron
STATE = DEFAULT_STATE.copy()


def load_state():
    global STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                STATE.update(saved)
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
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"Pair {pair} not found on Gate.io")
    t = data[0]
    last_price = float(t["last"])
    high_24h = float(t.get("high_24h", last_price))
    low_24h = float(t.get("low_24h", last_price))
    change_24h = float(t.get("change_percentage", 0.0))

    now = time.time()
    cached = PAIR_META_CACHE.get(pair)
    if not cached or (now - cached["ts"] > 3600):
        p_info = HTTP_SESSION.get(
            f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs/{pair}",
            timeout=5
        ).json()
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
        return  # Skip if another cycle is already mid-flight

    t0 = time.time()
    try:
        with state_lock:
            pair = STATE["pair"].strip().upper()
            base_curr = pair.split("_")[0]
            principal = float(STATE["principal_usdt"])
            skim_pct = float(STATE["skim_target_pct"])
            fee_buf = float(STATE["fee_buffer_pct"])
            pullback_pct = float(STATE.get("rebuy_pullback_pct", 0.0))
            armed = bool(STATE["armed"])
            has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))

        price, high_24, low_24, chg_24, amt_prec, px_prec, min_quote, min_base = fetch_market_snapshot(pair)
        base_bal, usdt_bal = (0.0, 0.0)
        if has_keys:
            base_bal, usdt_bal = fetch_spot_balances(base_curr)

        with state_lock:
            STATE["cycle_count"] = STATE.get("cycle_count", 0) + 1
            STATE["last_price"] = price
            STATE["high_24h"] = high_24
            STATE["low_24h"] = low_24
            STATE["change_24h_pct"] = chg_24
            STATE["base_balance"] = base_bal
            STATE["usdt_balance"] = usdt_bal

            pos_val = base_bal * price
            STATE["position_value_usdt"] = round(pos_val, 4)

            holding_tokens = pos_val >= max(min_quote * 1.05, principal * 0.40)
            if holding_tokens:
                STATE["phase"] = "HOLDING_FOR_SKIM"
                if STATE["entry_price"] <= 0:
                    STATE["entry_price"] = price
                    add_log(f"Detected {base_curr} balance: Entry Floor auto-locked @ ${price:.4f}")
            else:
                STATE["phase"] = "WAITING_TO_BUY"

            entry_px = STATE["entry_price"] if STATE["entry_price"] > 0 else price
            effective_gain_pct = skim_pct + fee_buf
            target_sell_px = entry_px * (1.0 + (effective_gain_pct / 100.0))
            STATE["target_sell_price"] = round(target_sell_px, px_prec)

            last_sell_px = STATE.get("last_skim_sell_price", 0.0)
            if last_sell_px > 0 and pullback_pct > 0:
                rebuy_px = last_sell_px * (1.0 - (pullback_pct / 100.0))
            else:
                rebuy_px = price
            STATE["target_rebuy_price"] = round(rebuy_px, px_prec)

            if holding_tokens and entry_px > 0:
                STATE["unrealized_pnl_usdt"] = round(pos_val - principal, 4)
                STATE["unrealized_pnl_pct"] = round(((price - entry_px) / entry_px) * 100.0, 3)
            else:
                STATE["unrealized_pnl_usdt"] = 0.0
                STATE["unrealized_pnl_pct"] = 0.0

            ms = int((time.time() - t0) * 1000)
            STATE["last_scan_ms"] = ms
            STATE["last_update"] = datetime.utcnow().strftime("%H:%M:%S")

            if not armed:
                STATE["status_msg"] = f"PAPER STANDBY | {pair} Live @ ${price:.4f} ({ms}ms)"
                save_state()
                return

            if not has_keys:
                STATE["status_msg"] = "ARMED - Waiting for Gate.io API Key & Secret"
                save_state()
                return

            # =========================================================================
            # PHASE A: HOLDING TOKENS -> SELL 100% AT +1% ABOVE ENTRY FLOOR
            # =========================================================================
            if STATE["phase"] == "HOLDING_FOR_SKIM":
                if price < target_sell_px:
                    if price < entry_px:
                        STATE["status_msg"] = (
                            f"CAPITAL PROTECTED HOLD | Live ${price:.4f} < Entry Floor ${entry_px:.4f} "
                            f"(Target ${target_sell_px:.4f})"
                        )
                    else:
                        STATE["status_msg"] = (
                            f"CLIMBING (+{STATE['unrealized_pnl_pct']:.2f}%) | Entry ${entry_px:.4f} → "
                            f"+{skim_pct}% Target ${target_sell_px:.4f}"
                        )
                    save_state()
                    return

                sell_amt_str = format_dec(base_bal, amt_prec)
                sell_px_str = format_dec(price * 0.998, px_prec)
                code, resp = place_gate_order(pair, "sell", sell_amt_str, sell_px_str)

                if code in (200, 201):
                    gross_proceeds = base_bal * price
                    net_proceeds = gross_proceeds * (1.0 - (fee_buf / 100.0))
                    skimmed_profit = max(0.01, round(net_proceeds - principal, 4))

                    STATE["total_skimmed_usdt"] = round(STATE["total_skimmed_usdt"] + skimmed_profit, 4)
                    STATE["skim_count"] += 1
                    old_entry = STATE["entry_price"]
                    STATE["last_skim_sell_price"] = price

                    ledger_entry = {
                        "time": datetime.utcnow().strftime("%H:%M:%S"),
                        "pair": pair,
                        "cycle": f"#{STATE['skim_count']}",
                        "buy_px": f"${old_entry:.4f}",
                        "sell_px": f"${price:.4f}",
                        "profit": f"+${skimmed_profit:.4f}",
                        "principal_redeployed": f"${principal:.2f}"
                    }
                    STATE["skim_ledger"].insert(0, ledger_entry)
                    STATE["skim_ledger"] = STATE["skim_ledger"][:40]

                    add_log(
                        f"SKIM #{STATE['skim_count']} WON (+{skim_pct}%): Sold {sell_amt_str} {base_curr} @ ${price:.4f} "
                        f"| Vaulted +${skimmed_profit:.4f} USDT!"
                    )

                    # Instant Auto-Rebuy of Principal at the new price if pullback == 0
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
                                f"SKIMMED +${skimmed_profit:.4f} & REBOUGHT ${principal:.2f} @ ${price:.4f} "
                                f"→ Next Target ${new_target:.4f}"
                            )
                            add_log(
                                f"AUTO-REBUY LOCKED: Redeployed ${principal:.2f} Principal @ ${price:.4f} "
                                f"| Next +{skim_pct}% Skim @ ${new_target:.4f}"
                            )
                        else:
                            STATE["phase"] = "WAITING_TO_BUY"
                            add_log(f"Rebuy queued for next cycle ({b_code}): {b_resp.get('message', '')}")
                    else:
                        STATE["phase"] = "WAITING_TO_BUY"
                        STATE["status_msg"] = (
                            f"SKIMMED +${skimmed_profit:.4f}! Waiting for -{pullback_pct}% pullback "
                            f"to ${rebuy_px:.4f}"
                        )
                else:
                    add_log(f"Skim sell notice ({code}): {resp.get('message', resp)}")

                save_state()
                return

            # =========================================================================
            # PHASE B: AUTO-BUY / REBUY EXACT PROTECTED PRINCIPAL ($C)
            # =========================================================================
            if STATE["phase"] == "WAITING_TO_BUY":
                if usdt_bal < principal:
                    STATE["status_msg"] = (
                        f"WAITING FOR USDT: Available ${usdt_bal:.2f} USDT < Protected Principal ${principal:.2f}"
                    )
                    save_state()
                    return

                if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
                    STATE["status_msg"] = (
                        f"WAITING FOR REBUY PULLBACK | Live ${price:.4f} → Rebuy Trigger ${rebuy_px:.4f}"
                    )
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
                        f"DEPLOYED ${principal:.2f} IN {base_curr} @ ${price:.4f} → Target ${new_target:.4f}"
                    )
                    add_log(
                        f"PRINCIPAL DEPLOYED: Bought {buy_amt_str} {base_curr} (${principal:.2f}) @ ${price:.4f} "
                        f"| Skim Target: ${new_target:.4f} (+{skim_pct}%)"
                    )
                else:
                    add_log(f"Buy order notice ({code}): {resp.get('message', resp)}")

                save_state()

    except Exception as e:
        with state_lock:
            STATE["status_msg"] = f"Syncing Node: {str(e)[:60]}"
            add_log(f"Network notice: {str(e)[:85]}")
    finally:
        cycle_lock.release()


def background_worker():
    load_state()
    add_log("VaultSkimmer v3.0 Peak Engine Online: Persistent Session + Auto-Rebuy Active.")
    while True:
        run_skim_cycle(force_manual=False)
        time.sleep(3.5)


threading.Thread(target=background_worker, daemon=True).start()


# =========================================================================
# PWA AUTO-INSTALL ENDPOINTS (Manifest, Service Worker, Native Vector Icon)
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
        "name": "VaultSkimmer v3.0 Peak Engine",
        "short_name": "VaultSkimmer",
        "description": "Capital-First +1% Spot Skimmer & Auto-Rebuy Engine",
        "start_url": "/",
        "display": "standalone",
        "background_color": "#060b16",
        "theme_color": "#10b981",
        "orientation": "portrait-primary",
        "icons": [
            {
                "src": "/icon.svg",
                "sizes": "any",
                "type": "image/svg+xml",
                "purpose": "any maskable"
            }
        ]
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
# REST API & CRON ENDPOINTS
# =========================================================================
@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "VaultSkimmer-v3-Peak", "ts": time.time()})


@app.route("/api/cron")
def api_cron():
    run_skim_cycle(force_manual=False)
    return jsonify({
        "ok": True,
        "armed": STATE["armed"],
        "phase": STATE["phase"],
        "pair": STATE["pair"],
        "entry_price": STATE["entry_price"],
        "last_price": STATE["last_price"],
        "target_sell_price": STATE["target_sell_price"],
        "total_skimmed_usdt": STATE["total_skimmed_usdt"],
        "scan_ms": STATE["last_scan_ms"]
    })


@app.route("/api/scan_now", methods=["POST"])
def api_scan_now():
    run_skim_cycle(force_manual=True)
    return jsonify({"ok": True, "scan_ms": STATE["last_scan_ms"]})


@app.route("/api/state")
def api_state():
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
            new_pair = data["pair"].strip().upper()
            if new_pair != STATE["pair"]:
                STATE["pair"] = new_pair
                STATE["entry_price"] = 0.0
                STATE["last_skim_sell_price"] = 0.0
                add_log(f"Switched active pair to {new_pair} (Entry floor reset).")
        if "principal_usdt" in data:
            STATE["principal_usdt"] = max(2.0, float(data["principal_usdt"]))
        if "skim_target_pct" in data:
            STATE["skim_target_pct"] = max(0.2, float(data["skim_target_pct"]))
        if "rebuy_pullback_pct" in data:
            STATE["rebuy_pullback_pct"] = max(0.0, float(data["rebuy_pullback_pct"]))
        if "entry_price" in data and data["entry_price"] != "":
            val = float(data["entry_price"])
            if val > 0:
                STATE["entry_price"] = val
        if data.get("api_key") and data["api_key"] != "Saved on server":
            STATE["api_key"] = data["api_key"].strip()
        if data.get("api_secret") and data["api_secret"] != "Saved on server":
            STATE["api_secret"] = data["api_secret"].strip()

        save_state()
        add_log(
            f"Synced Settings: {STATE['pair']} | Principal: ${STATE['principal_usdt']:.2f} "
            f"| Skim: +{STATE['skim_target_pct']}%"
        )
    run_skim_cycle(force_manual=False)
    return jsonify({"ok": True})


@app.route("/api/toggle_arm", methods=["POST"])
def api_toggle_arm():
    with state_lock:
        STATE["armed"] = not STATE["armed"]
        mode = "LIVE ARMED" if STATE["armed"] else "PAPER STANDBY"
        add_log(f"Engine mode switched to: {mode}")
        save_state()
    run_skim_cycle(force_manual=False)
    return jsonify({"ok": True, "armed": STATE["armed"]})


@app.route("/api/lock_current_entry", methods=["POST"])
def api_lock_entry():
    with state_lock:
        if STATE["last_price"] > 0:
            STATE["entry_price"] = STATE["last_price"]
            add_log(f"Entry Floor manually locked @ ${STATE['entry_price']:.4f}")
            save_state()
    run_skim_cycle(force_manual=False)
    return jsonify({"ok": True, "entry_price": STATE["entry_price"]})


# =========================================================================
# PEAK UPGRADED UI WITH NATIVE 1-CLICK AUTO-INSTALL APP BUTTON
# =========================================================================
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
  <title>VaultSkimmer v3.0 | Peak Capital-First Skim Engine</title>
  <style>
    :root {
      --bg: #050811;
      --card: rgba(13, 20, 36, 0.88);
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

    /* Top Header & Auto-Install Bar */
    .hero {
      background: linear-gradient(135deg, rgba(16,185,129,0.14), rgba(6,182,212,0.08));
      border: 1px solid rgba(16,185,129,0.4); border-radius: 18px; padding: 18px 20px; margin-bottom: 14px;
      box-shadow: 0 14px 34px rgba(0,0,0,0.5); backdrop-filter: blur(12px);
    }
    .hero-top { display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center; gap: 12px; }
    .brand-row { display: flex; align-items: center; gap: 12px; flex-wrap: wrap; }
    .brand-title { font-size: 1.55rem; font-weight: 900; letter-spacing: -0.5px; display: flex; align-items: center; gap: 9px; }
    .v-badge { background: var(--emerald); color: #022c22; font-size: 0.72rem; font-weight: 900; padding: 3px 9px; border-radius: 99px; }
    .sub-desc { color: var(--muted); font-size: 0.81rem; margin-top: 5px; }

    .top-actions { display: flex; align-items: center; gap: 10px; flex-wrap: wrap; }
    .btn-install {
      background: linear-gradient(135deg, #10b981, #06b6d4); color: #022c22;
      font-weight: 900; font-size: 0.8rem; padding: 9px 15px; border-radius: 11px; border: none;
      cursor: pointer; display: inline-flex; align-items: center; gap: 7px;
      box-shadow: 0 0 18px var(--emerald-glow); transition: transform 0.15s ease;
    }
    .btn-install:hover { transform: scale(1.03); }
    .btn-scan {
      background: rgba(139, 92, 246, 0.2); border: 1px solid #8b5cf6; color: #ddd6fe;
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
    .pill-live { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid #10b981; box-shadow: 0 0 12px var(--emerald-glow); }
    .pill-paper { background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid #f59e0b; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 1.2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.25; } }

    /* KPI Grid */
    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 12px; margin-bottom: 14px; }
    .card {
      background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 16px;
      box-shadow: 0 8px 24px rgba(0,0,0,0.35); backdrop-filter: blur(8px);
    }
    .lbl { color: var(--muted); font-size: 0.7rem; font-weight: 800; text-transform: uppercase; letter-spacing: 0.6px; }
    .val { font-size: 1.48rem; font-weight: 900; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .sub { font-size: 0.77rem; color: var(--muted); }
    .c-green { color: #34d399; }
    .c-cyan { color: #22d3ee; }
    .c-amber { color: #fbbf24; }

    /* Progress & Live Trajectory */
    .bar-card { background: var(--card); border: 1px solid var(--border); border-radius: 15px; padding: 15px 18px; margin-bottom: 14px; }
    .bar-top { display: flex; justify-content: space-between; align-items: center; font-size: 0.82rem; font-weight: 800; margin-bottom: 9px; flex-wrap: wrap; gap: 6px; }
    .track { height: 13px; background: #04070e; border-radius: 99px; overflow: hidden; border: 1px solid #1e293b; }
    .fill {
      height: 100%; width: 0%; background: linear-gradient(90deg, #06b6d4, #10b981);
      transition: width 0.35s cubic-bezier(0.4, 0, 0.2, 1); box-shadow: 0 0 14px var(--emerald);
    }

    /* Controls & Ledger Grid */
    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(335px, 1fr)); gap: 14px; margin-bottom: 14px; }
    .sec-title { font-size: 0.85rem; font-weight: 900; text-transform: uppercase; letter-spacing: 0.5px; color: #38bdf8; margin-bottom: 13px; display: flex; justify-content: space-between; align-items: center; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 10px; margin-bottom: 10px; }
    label { display: block; font-size: 0.73rem; color: var(--muted); margin-bottom: 4px; font-weight: 700; }
    input, select {
      width: 100%; background: #050811; border: 1px solid #26334d; color: var(--text);
      padding: 10px 11px; border-radius: 10px; font-size: 0.87rem; font-weight: 700;
    }
    input:focus, select:focus { outline: none; border-color: var(--cyan); }
    .btns { display: flex; flex-wrap: wrap; gap: 9px; margin-top: 12px; }
    button.ctrl { cursor: pointer; border: none; border-radius: 10px; padding: 11px 14px; font-weight: 800; font-size: 0.83rem; transition: all 0.15s ease; }
    .b-save { background: linear-gradient(135deg, #10b981, #059669); color: #022c22; flex: 1; }
    .b-arm { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; flex: 1; }
    .b-lock { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; width: 100%; }

    table { width: 100%; border-collapse: collapse; font-size: 0.79rem; }
    th, td { padding: 8px 6px; text-align: left; border-bottom: 1px solid #1e293b; font-variant-numeric: tabular-nums; }
    th { color: var(--muted); font-size: 0.69rem; text-transform: uppercase; }
    .logs {
      background: #04070e; border: 1px solid #1e293b; border-radius: 11px; padding: 11px;
      height: 215px; overflow-y: auto; font-family: 'JetBrains Mono', monospace; font-size: 0.76rem; color: #cbd5e1; line-height: 1.55;
    }

    /* Install Modal for iOS / Browsers */
    #installModal {
      display: none; position: fixed; inset: 0; background: rgba(0,0,0,0.75); z-index: 999;
      align-items: center; justify-content: center; padding: 18px;
    }
    .modal-box {
      background: #0f172a; border: 1px solid #10b981; border-radius: 16px; max-width: 400px; width: 100%;
      padding: 20px; text-align: center; box-shadow: 0 20px 50px rgba(0,0,0,0.7);
    }
    .modal-box h3 { font-size: 1.1rem; margin-bottom: 10px; color: #34d399; }
    .modal-box p { font-size: 0.85rem; color: #cbd5e1; line-height: 1.5; margin-bottom: 16px; }
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div class="hero-top">
      <div>
        <div class="brand-row">
          <div class="brand-title">VaultSkimmer <span class="v-badge">v3.0 PEAK</span></div>
        </div>
        <div class="sub-desc">Capital-First +1% Spot Skimmer • 100% Flip & Instant Principal Auto-Rebuy • Persistent Low-Latency Pool</div>
      </div>
      <div class="top-actions">
        <button class="btn-install" id="installBtn" onclick="triggerInstall()">📲 Install App</button>
        <button class="btn-scan" onclick="manualScan()" id="scanBtn">⚡ Scan Now</button>
      </div>
    </div>

    <div class="telemetry-strip">
      <div>
        <span id="modePill" class="pill pill-paper"><span class="dot"></span> <span id="modeText">PAPER STANDBY</span></span>
        <span style="margin-left:8px;">Phase: <strong class="c-cyan" id="phaseText">WAITING</strong></span>
      </div>
      <div>
        24h Range: <strong id="low24">$0.00</strong> – <strong id="high24">$0.00</strong> (<span id="chg24">0.00%</span>)
      </div>
      <div>
        Last Scan: <strong id="lastUpdate">--:--:--</strong> (<strong class="c-green" id="scanMs">0</strong>ms | Cycle #<span id="cycleNum">0</span>)
      </div>
    </div>
  </div>

  <div class="grid-4">
    <div class="card">
      <div class="lbl">Protected Principal (Active Base)</div>
      <div class="val c-cyan" id="principalVal">$25.00</div>
      <div class="sub">Live Token Value: <strong id="posVal">$0.0000</strong> (<span id="baseBal">0.00</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Entry Floor → +1% Skim Target</div>
      <div class="val" id="entryToTarget">$0.00 → $0.00</div>
      <div class="sub">Live Price: <strong class="c-amber" id="livePrice">$0.0000</strong> (<span id="pnlPct">0.00%</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Next +1% Skim Payout</div>
      <div class="val c-amber" id="expectedSkim">+$0.2500</div>
      <div class="sub">Rebuy Mode: <strong id="rebuyMode">Instant @ New Price</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Realized USDT Profit Vault</div>
      <div class="val c-green" id="totalSkimmed">+$0.0000</div>
      <div class="sub">1% Skims Won: <strong id="skimCount">0</strong> | Free USDT: <strong id="usdtBal">$0.00</strong></div>
    </div>
  </div>

  <div class="bar-card">
    <div class="bar-top">
      <span id="statusBanner">Connecting to Persistent Gate.io Spot Engine...</span>
      <span id="progressLabel" class="c-green">0.0% to +1% Skim</span>
    </div>
    <div class="track"><div class="fill" id="progressBar"></div></div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="sec-title">
        <span>⚙️ Capital-First & Auto-Rebuy Controls</span>
      </div>
      <div class="row">
        <div>
          <label>Spot Pair (Gate.io)</label>
          <select id="pairInput">
            <option value="SUI_USDT">SUI_USDT (Fast 1% Waves)</option>
            <option value="QNT_USDT">QNT_USDT (High Beta)</option>
            <option value="ETH_USDT">ETH_USDT (Major)</option>
            <option value="BTC_USDT">BTC_USDT (Major)</option>
            <option value="SOL_USDT">SOL_USDT (High Velocity)</option>
          </select>
        </div>
        <div>
          <label>Protected Principal (USDT)</label>
          <input type="number" id="principalInput" step="1" value="25" />
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
          <label>Locked Entry Floor ($)</label>
          <input type="number" id="entryInput" step="0.0001" placeholder="Auto-locks on buy" />
        </div>
        <div>
          <label>Gate.io v4 API Key</label>
          <input type="password" id="apiKeyInput" placeholder="Saved on server" />
        </div>
      </div>
      <div class="row">
        <div style="grid-column: span 2;">
          <label>Gate.io v4 API Secret</label>
          <input type="password" id="apiSecretInput" placeholder="Saved on server" />
        </div>
      </div>
      <div class="btns">
        <button class="ctrl b-save" onclick="saveSettings()" id="saveBtn">💾 Save & Sync</button>
        <button class="ctrl b-arm" onclick="toggleArm()" id="armBtn">⚡ Arm / Disarm Bot</button>
        <button class="ctrl b-lock" onclick="lockCurrentEntry()">🔒 Lock Current Price as Entry Floor</button>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">
        <span>💎 Verified +1% Skim & Rebuy Ledger</span>
      </div>
      <div style="max-height: 295px; overflow-y: auto;">
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
            <tr><td colspan="5" style="color:#64748b; text-align:center; padding:22px;">No flips logged yet — waiting for +1% move above Entry Floor.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title"><span>📡 High-Speed Engine Telemetry Logs</span></div>
    <div class="logs" id="logBox"></div>
  </div>
</div>

<!-- Fallback Install Guide Modal -->
<div id="installModal" onclick="this.style.display='none'">
  <div class="modal-box" onclick="event.stopPropagation()">
    <h3>📲 Install VaultSkimmer App</h3>
    <p id="installModalText">
      To install VaultSkimmer as a fullscreen native app:<br/><br/>
      <strong>Android / Chrome:</strong> Tap the browser menu (⋮) and select <strong>"Install app"</strong> or <strong>"Add to Home screen"</strong>.<br/><br/>
      <strong>iPhone / Safari:</strong> Tap the <strong>Share</strong> button at the bottom and tap <strong>"Add to Home Screen"</strong>.
    </p>
    <button class="btn-install" style="width:100%; justify-content:center;" onclick="document.getElementById('installModal').style.display='none'">Got It</button>
  </div>
</div>

<script>
  let initialized = false;
  let deferredPrompt = null;

  // Register Service Worker for Native App Installation
  if ('serviceWorker' in navigator) {
    window.addEventListener('load', () => {
      navigator.serviceWorker.register('/sw.js').catch(() => {});
    });
  }

  window.addEventListener('beforeinstallprompt', (e) => {
    e.preventDefault();
    deferredPrompt = e;
    const btn = document.getElementById('installBtn');
    btn.innerHTML = '📲 Auto-Install App';
  });

  window.addEventListener('appinstalled', () => {
    deferredPrompt = null;
    document.getElementById('installBtn').innerHTML = '✅ App Installed';
  });

  async function triggerInstall() {
    if (deferredPrompt) {
      deferredPrompt.prompt();
      const { outcome } = await deferredPrompt.userChoice;
      if (outcome === 'accepted') {
        document.getElementById('installBtn').innerHTML = '✅ Installed';
      }
      deferredPrompt = null;
    } else {
      document.getElementById('installModal').style.display = 'flex';
    }
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
      document.getElementById('low24').innerText = '$' + Number(s.low_24h || 0).toFixed(4);
      document.getElementById('high24').innerText = '$' + Number(s.high_24h || 0).toFixed(4);
      const chg = Number(s.change_24h_pct || 0);
      document.getElementById('chg24').innerText = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';

      const pill = document.getElementById('modePill');
      const modeText = document.getElementById('modeText');
      if (s.armed) {
        pill.className = 'pill pill-live';
        modeText.innerText = 'LIVE ARMED (' + s.pair + ')';
      } else {
        pill.className = 'pill pill-paper';
        modeText.innerText = 'PAPER STANDBY';
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
            <td><strong>${i.cycle} (${i.pair})</strong></td>
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
    btn.innerText = 'Saving...';
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
    btn.innerText = '✅ Saved!';
    setTimeout(() => { btn.innerText = '💾 Save & Sync'; }, 1200);
  }

  async function toggleArm() {
    await fetch('/api/toggle_arm', { method: 'POST' });
    refresh();
  }

  async function lockCurrentEntry() {
    const r = await fetch('/api/lock_current_entry', { method: 'POST' });
    const d = await r.json();
    if (d.entry_price) document.getElementById('entryInput').value = d.entry_price;
    refresh();
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
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
