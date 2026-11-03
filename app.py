import os
import time
import json
import hmac
import hashlib
import threading
from datetime import datetime
from decimal import Decimal, ROUND_DOWN
import requests
from flask import Flask, request, jsonify, render_template_string

app = Flask(__name__)

STATE_FILE = "vaultskimmer_v2_state.json"
GATE_HOST = "https://api.gateio.ws"
GATE_PREFIX = "/api/v4"

DEFAULT_STATE = {
    "armed": False,
    "pair": "SUI_USDT",
    "principal_usdt": 25.0,
    "skim_target_pct": 1.0,
    "fee_buffer_pct": 0.20,
    "rebuy_pullback_pct": 0.0,  # 0.0 = Instant rebuy at new price; >0 = wait for micro-pullback
    "entry_price": 0.0,
    "last_skim_sell_price": 0.0,
    "phase": "WAITING_TO_BUY",  # WAITING_TO_BUY or HOLDING_FOR_SKIM
    "api_key": os.environ.get("GATE_API_KEY", ""),
    "api_secret": os.environ.get("GATE_API_SECRET", ""),
    "total_skimmed_usdt": 0.0,
    "skim_count": 0,
    "last_price": 0.0,
    "base_balance": 0.0,
    "usdt_balance": 0.0,
    "position_value_usdt": 0.0,
    "target_sell_price": 0.0,
    "target_rebuy_price": 0.0,
    "unrealized_pnl_usdt": 0.0,
    "unrealized_pnl_pct": 0.0,
    "status_msg": "STANDBY - Set Principal & Arm VaultSkimmer",
    "last_scan_ms": 0,
    "last_update": "--:--:--",
    "skim_ledger": [],
    "logs": []
}

state_lock = threading.Lock()
STATE = DEFAULT_STATE.copy()


def load_state():
    global STATE
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                saved = json.load(f)
                STATE.update(saved)
        except Exception as e:
            add_log(f"State load warning: {e}")


def save_state():
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(STATE, f, indent=2)
    except Exception:
        pass


def add_log(msg):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    entry = f"[{ts}] {msg}"
    STATE["logs"].insert(0, entry)
    STATE["logs"] = STATE["logs"][:80]


def gate_sign(method, url, query_string="", body_string=""):
    key = STATE.get("api_key", "").strip()
    secret = STATE.get("api_secret", "").strip()
    t = str(int(time.time()))
    m = hashlib.sha512()
    m.update(body_string.encode("utf-8"))
    hashed_payload = m.hexdigest()
    s = f"{method}\n{url}\n{query_string}\n{hashed_payload}\n{t}"
    sign = hmac.new(secret.encode("utf-8"), s.encode("utf-8"), hashlib.sha512).hexdigest()
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "KEY": key,
        "Timestamp": t,
        "SIGN": sign
    }


def fetch_ticker_and_pair_info(pair):
    r = requests.get(
        f"{GATE_HOST}{GATE_PREFIX}/spot/tickers",
        params={"currency_pair": pair},
        timeout=6
    )
    r.raise_for_status()
    data = r.json()
    if not data:
        raise ValueError(f"Pair {pair} not found on Gate.io")
    last_price = float(data[0]["last"])

    p_info = requests.get(
        f"{GATE_HOST}{GATE_PREFIX}/spot/currency_pairs/{pair}",
        timeout=6
    ).json()
    amount_prec = int(p_info.get("amount_precision", 4))
    price_prec = int(p_info.get("precision", 4))
    min_quote = float(p_info.get("min_quote_amount", 1.0))
    min_base = float(p_info.get("min_base_amount", 0.0001))
    return last_price, amount_prec, price_prec, min_quote, min_base


def fetch_spot_balances(base_curr):
    url = f"{GATE_PREFIX}/spot/accounts"
    headers = gate_sign("GET", url, "", "")
    r = requests.get(f"{GATE_HOST}{url}", headers=headers, timeout=6)
    r.raise_for_status()
    accounts = r.json()
    base_avail = 0.0
    usdt_avail = 0.0
    for acc in accounts:
        if acc.get("currency") == base_curr:
            base_avail = float(acc.get("available", 0.0))
        elif acc.get("currency") == "USDT":
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
    r = requests.post(f"{GATE_HOST}{url}", headers=headers, data=body_str, timeout=8)
    return r.status_code, r.json()


def run_skim_cycle():
    t0 = time.time()
    with state_lock:
        pair = STATE["pair"].strip().upper()
        base_curr = pair.split("_")[0]
        principal = float(STATE["principal_usdt"])
        skim_pct = float(STATE["skim_target_pct"])
        fee_buf = float(STATE["fee_buffer_pct"])
        pullback_pct = float(STATE.get("rebuy_pullback_pct", 0.0))
        armed = bool(STATE["armed"])
        has_keys = bool(STATE.get("api_key") and STATE.get("api_secret"))

    try:
        price, amt_prec, px_prec, min_quote, min_base = fetch_ticker_and_pair_info(pair)
        base_bal, usdt_bal = (0.0, 0.0)
        if has_keys:
            base_bal, usdt_bal = fetch_spot_balances(base_curr)

        with state_lock:
            STATE["last_price"] = price
            STATE["base_balance"] = base_bal
            STATE["usdt_balance"] = usdt_bal

            pos_val = base_bal * price
            STATE["position_value_usdt"] = round(pos_val, 4)

            # Determine active phase from live token balance
            holding_tokens = pos_val >= max(min_quote * 1.1, principal * 0.40)
            if holding_tokens:
                STATE["phase"] = "HOLDING_FOR_SKIM"
                if STATE["entry_price"] <= 0:
                    STATE["entry_price"] = price
                    add_log(f"Existing {base_curr} detected: Locked Entry Floor at ${price:.4f}")
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
                STATE["status_msg"] = f"PAPER STANDBY | {pair} @ ${price:.4f} ({ms}ms)"
                save_state()
                return

            if not has_keys:
                STATE["status_msg"] = "ARMED - Paste Gate.io API Keys to Execute"
                save_state()
                return

            # =========================================================================
            # PHASE A: HOLDING TOKENS -> WAIT FOR +1% ABOVE ENTRY FLOOR, THEN SELL 100%
            # =========================================================================
            if STATE["phase"] == "HOLDING_FOR_SKIM":
                if price < target_sell_px:
                    if price < entry_px:
                        STATE["status_msg"] = (
                            f"HOLDING (CAPITAL PROTECTED) | Live ${price:.4f} < Entry Floor ${entry_px:.4f} "
                            f"(Waiting for ${target_sell_px:.4f})"
                        )
                    else:
                        STATE["status_msg"] = (
                            f"CLIMBING (+{STATE['unrealized_pnl_pct']:.2f}%) | Entry ${entry_px:.4f} -> "
                            f"+1% Skim Target ${target_sell_px:.4f}"
                        )
                    save_state()
                    return

                # +1% Target Hit! Sell 100% of base_bal to lock in the 1% gain cleanly
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
                    STATE["skim_ledger"] = STATE["skim_ledger"][:35]

                    add_log(
                        f"SKIM #{STATE['skim_count']} HIT (+1%): Sold {sell_amt_str} {base_curr} @ ${price:.4f} "
                        f"| Skimmed +${skimmed_profit:.4f} USDT to Vault!"
                    )

                    # Instant Auto-Rebuy with ONLY the Protected Principal if pullback == 0
                    if pullback_pct <= 0.0:
                        time.sleep(0.4)
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
                                f"| Next +1% Target: ${new_target:.4f}"
                            )
                            add_log(
                                f"AUTO-REBUY COMPLETE: Deployed ${principal:.2f} Principal @ ${price:.4f}. "
                                f"New +1% Skim Target locked at ${new_target:.4f}"
                            )
                        else:
                            STATE["phase"] = "WAITING_TO_BUY"
                            add_log(f"Auto-rebuy queued for next tick ({b_code}): {b_resp.get('message', '')}")
                    else:
                        STATE["phase"] = "WAITING_TO_BUY"
                        STATE["status_msg"] = (
                            f"SKIMMED +${skimmed_profit:.4f}! Waiting for -{pullback_pct}% pullback "
                            f"to ${rebuy_px:.4f} to rebuy ${principal:.2f}"
                        )
                else:
                    add_log(f"Skim sell rejected ({code}): {resp.get('message', resp)}")

                save_state()
                return

            # =========================================================================
            # PHASE B: WAITING TO BUY / AUTO-REBUY ONLY THE PROTECTED PRINCIPAL ($C)
            # =========================================================================
            if STATE["phase"] == "WAITING_TO_BUY":
                if usdt_bal < principal:
                    STATE["status_msg"] = (
                        f"AWAITING USDT FOR REBUY: Have ${usdt_bal:.2f} USDT, Need ${principal:.2f} Principal"
                    )
                    save_state()
                    return

                if pullback_pct > 0 and last_sell_px > 0 and price > rebuy_px:
                    STATE["status_msg"] = (
                        f"WAITING FOR REBUY PULLBACK | Live ${price:.4f} -> Target Rebuy ${rebuy_px:.4f}"
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
                        f"BOUGHT ${principal:.2f} {base_curr} @ ${price:.4f} | Waiting for +1% (${new_target:.4f})"
                    )
                    add_log(
                        f"ENTRY LOCKED: Bought {buy_amt_str} {base_curr} (${principal:.2f}) @ ${price:.4f} "
                        f"| Will skim at ${new_target:.4f} (+{skim_pct}%)"
                    )
                else:
                    add_log(f"Entry buy warning ({code}): {resp.get('message', resp)}")

                save_state()

    except Exception as e:
        with state_lock:
            STATE["status_msg"] = f"Check warning: {str(e)[:65]}"
            add_log(f"Cycle alert: {str(e)[:90]}")


def background_worker():
    load_state()
    add_log("VaultSkimmer v2.0 Online: 100% Sell + Principal Auto-Rebuy Loop Ready.")
    while True:
        run_skim_cycle()
        time.sleep(4)


threading.Thread(target=background_worker, daemon=True).start()


@app.route("/ping")
def ping():
    return jsonify({"ok": True, "service": "VaultSkimmer-v2", "ts": time.time()})


@app.route("/api/cron")
def api_cron():
    run_skim_cycle()
    return jsonify({
        "ok": True,
        "armed": STATE["armed"],
        "phase": STATE["phase"],
        "pair": STATE["pair"],
        "entry_price": STATE["entry_price"],
        "last_price": STATE["last_price"],
        "target_sell_price": STATE["target_sell_price"],
        "total_skimmed_usdt": STATE["total_skimmed_usdt"]
    })


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
                add_log(f"Switched pair to {new_pair} (Entry floor reset for new asset).")
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
            f"Synced: {STATE['pair']} | Principal: ${STATE['principal_usdt']:.2f} "
            f"| Skim: +{STATE['skim_target_pct']}% | Rebuy Pullback: {STATE['rebuy_pullback_pct']}%"
        )
    run_skim_cycle()
    return jsonify({"ok": True})


@app.route("/api/toggle_arm", methods=["POST"])
def api_toggle_arm():
    with state_lock:
        STATE["armed"] = not STATE["armed"]
        mode = "LIVE ARMED" if STATE["armed"] else "PAPER STANDBY"
        add_log(f"Engine switched to: {mode}")
        save_state()
    run_skim_cycle()
    return jsonify({"ok": True, "armed": STATE["armed"]})


@app.route("/api/lock_current_entry", methods=["POST"])
def api_lock_entry():
    with state_lock:
        if STATE["last_price"] > 0:
            STATE["entry_price"] = STATE["last_price"]
            add_log(f"Entry Floor locked at live price: ${STATE['entry_price']:.4f}")
            save_state()
    run_skim_cycle()
    return jsonify({"ok": True, "entry_price": STATE["entry_price"]})


HTML_UI = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>VaultSkimmer v2.0 | +1% Skim & Auto-Rebuy Engine</title>
  <style>
    :root {
      --bg: #060913;
      --card: #0d1323;
      --border: #1e293b;
      --emerald: #10b981;
      --cyan: #06b6d4;
      --amber: #f59e0b;
      --text: #f8fafc;
      --muted: #94a3b8;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: 'Inter', system-ui, sans-serif; }
    body { background: radial-gradient(circle at top, #0f172a 0%, #05070f 100%); color: var(--text); padding: 16px; min-height: 100vh; }
    .wrap { max-width: 1140px; margin: 0 auto; }
    .hero {
      display: flex; flex-wrap: wrap; justify-content: space-between; align-items: center;
      background: linear-gradient(135deg, rgba(16,185,129,0.14), rgba(6,182,212,0.08));
      border: 1px solid rgba(16,185,129,0.4); border-radius: 16px; padding: 18px 22px; margin-bottom: 16px;
      box-shadow: 0 12px 32px rgba(0,0,0,0.45);
    }
    .hero h1 { font-size: 1.5rem; font-weight: 800; display: flex; align-items: center; gap: 10px; }
    .tag { background: var(--emerald); color: #022c22; font-size: 0.72rem; font-weight: 900; padding: 3px 9px; border-radius: 99px; }
    .hero p { color: var(--muted); font-size: 0.83rem; margin-top: 4px; }
    .pill {
      display: inline-flex; align-items: center; gap: 8px; padding: 6px 14px; border-radius: 99px;
      font-weight: 800; font-size: 0.78rem; margin-bottom: 5px;
    }
    .pill-live { background: rgba(16,185,129,0.2); color: #34d399; border: 1px solid #10b981; }
    .pill-paper { background: rgba(245,158,11,0.18); color: #fbbf24; border: 1px solid #f59e0b; }
    .dot { width: 8px; height: 8px; border-radius: 50%; background: currentColor; animation: pulse 1.3s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.3; } }

    .grid-4 { display: grid; grid-template-columns: repeat(auto-fit, minmax(230px, 1fr)); gap: 14px; margin-bottom: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 16px; }
    .lbl { color: var(--muted); font-size: 0.72rem; font-weight: 700; text-transform: uppercase; letter-spacing: 0.6px; }
    .val { font-size: 1.5rem; font-weight: 800; margin: 6px 0 4px; font-variant-numeric: tabular-nums; }
    .sub { font-size: 0.78rem; color: var(--muted); }
    .c-green { color: #34d399; }
    .c-cyan { color: #22d3ee; }
    .c-amber { color: #fbbf24; }

    .bar-card { background: var(--card); border: 1px solid var(--border); border-radius: 14px; padding: 15px 18px; margin-bottom: 16px; }
    .bar-top { display: flex; justify-content: space-between; font-size: 0.83rem; font-weight: 700; margin-bottom: 9px; }
    .track { height: 12px; background: #050811; border-radius: 99px; overflow: hidden; border: 1px solid #1e293b; }
    .fill { height: 100%; width: 0%; background: linear-gradient(90deg, #06b6d4, #10b981); transition: width 0.4s ease; }

    .grid-2 { display: grid; grid-template-columns: repeat(auto-fit, minmax(340px, 1fr)); gap: 16px; margin-bottom: 16px; }
    .sec-title { font-size: 0.88rem; font-weight: 800; text-transform: uppercase; color: #38bdf8; margin-bottom: 14px; }
    .row { display: grid; grid-template-columns: 1fr 1fr; gap: 12px; margin-bottom: 12px; }
    label { display: block; font-size: 0.75rem; color: var(--muted); margin-bottom: 5px; font-weight: 600; }
    input, select {
      width: 100%; background: #060913; border: 1px solid #26334d; color: var(--text);
      padding: 10px 12px; border-radius: 10px; font-size: 0.88rem; font-weight: 600;
    }
    .btns { display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }
    button { cursor: pointer; border: none; border-radius: 10px; padding: 11px 15px; font-weight: 800; font-size: 0.84rem; }
    .b-save { background: linear-gradient(135deg, #10b981, #059669); color: #022c22; flex: 1; }
    .b-arm { background: linear-gradient(135deg, #f59e0b, #d97706); color: #1e1b4b; flex: 1; }
    .b-lock { background: #1e293b; color: #e2e8f0; border: 1px solid #334155; }

    table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
    th, td { padding: 9px 7px; text-align: left; border-bottom: 1px solid #1e293b; }
    th { color: var(--muted); font-size: 0.7rem; text-transform: uppercase; }
    .logs {
      background: #05070f; border: 1px solid #1e293b; border-radius: 10px; padding: 12px;
      height: 220px; overflow-y: auto; font-family: monospace; font-size: 0.77rem; color: #cbd5e1; line-height: 1.55;
    }
  </style>
</head>
<body>
<div class="wrap">
  <div class="hero">
    <div>
      <h1>VaultSkimmer <span class="tag">v2.0 AUTO-REBUY LOOP</span></h1>
      <p>Buys with Protected Principal • Sells 100% at +1% Above Entry • Vaults 1% Profit & Immediately Rebuys</p>
    </div>
    <div style="text-align:right; font-size:0.8rem; color:var(--muted);">
      <div id="modePill" class="pill pill-paper"><span class="dot"></span> <span id="modeText">PAPER STANDBY</span></div>
      <div>Cycle: <strong id="lastUpdate">--:--:--</strong> (<span id="scanMs">0</span>ms) | Phase: <strong class="c-cyan" id="phaseText">WAITING</strong></div>
    </div>
  </div>

  <div class="grid-4">
    <div class="card">
      <div class="lbl">Protected Principal (Redeployed)</div>
      <div class="val c-cyan" id="principalVal">$25.00</div>
      <div class="sub">Active Position: <strong id="posVal">$0.0000</strong> (<span id="baseBal">0.00</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Current Entry → +1% Sell Trigger</div>
      <div class="val" id="entryToTarget">$0.00 → $0.00</div>
      <div class="sub">Live Price: <strong class="c-amber" id="livePrice">$0.0000</strong> (<span id="pnlPct">0.00%</span>)</div>
    </div>
    <div class="card">
      <div class="lbl">Expected Skim on Current Wave</div>
      <div class="val c-amber" id="expectedSkim">+$0.2500</div>
      <div class="sub">Rebuy Mode: <strong id="rebuyMode">Instant @ New Price</strong></div>
    </div>
    <div class="card">
      <div class="lbl">Realized USDT Profit Vault</div>
      <div class="val c-green" id="totalSkimmed">+$0.0000</div>
      <div class="sub">1% Flips Won: <strong id="skimCount">0</strong> | Free USDT: <strong id="usdtBal">$0.00</strong></div>
    </div>
  </div>

  <div class="bar-card">
    <div class="bar-top">
      <span id="statusBanner">Syncing with Gate.io Spot Engine...</span>
      <span id="progressLabel">0% to +1% Skim</span>
    </div>
    <div class="track"><div class="fill" id="progressBar"></div></div>
  </div>

  <div class="grid-2">
    <div class="card">
      <div class="sec-title">⚙️ Principal, +1% Skim & Auto-Rebuy Settings</div>
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
          <label>Rebuy Pullback % (0 = Instant Rebuy)</label>
          <input type="number" id="pullbackInput" step="0.05" value="0.0" />
        </div>
      </div>
      <div class="row">
        <div>
          <label>Current Entry Floor ($)</label>
          <input type="number" id="entryInput" step="0.0001" placeholder="Auto-updates on every buy" />
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
        <button class="b-save" onclick="saveSettings()">💾 Save & Sync Loop</button>
        <button class="b-arm" onclick="toggleArm()">⚡ Arm / Disarm Live Bot</button>
        <button class="b-lock" onclick="lockCurrentEntry()">🔒 Set Live Price as Entry</button>
      </div>
    </div>

    <div class="card">
      <div class="sec-title">💎 Completed +1% Skim & Rebuy Ledger</div>
      <div style="max-height: 290px; overflow-y: auto;">
        <table>
          <thead>
            <tr>
              <th>Time</th>
              <th>Flip</th>
              <th>Buy → +1% Sell</th>
              <th>Vaulted Profit</th>
              <th>Rebought</th>
            </tr>
          </thead>
          <tbody id="ledgerBody">
            <tr><td colspan="5" style="color:#64748b; text-align:center; padding:20px;">No flips yet — waiting for +1% move above Entry Floor.</td></tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>

  <div class="card">
    <div class="sec-title">📡 Live Auto-Rebuy & Skim Telemetry</div>
    <div class="logs" id="logBox"></div>
  </div>
</div>

<script>
  let initialized = false;

  async function refresh() {
    try {
      const r = await fetch('/api/state');
      const s = await r.json();

      document.getElementById('lastUpdate').innerText = s.last_update;
      document.getElementById('scanMs').innerText = s.last_scan_ms;
      document.getElementById('phaseText').innerText = s.phase;

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
      document.getElementById('progressLabel').innerText = pct.toFixed(1) + '% to +1% Skim';

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
    refresh();
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

  setInterval(refresh, 3000);  
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
