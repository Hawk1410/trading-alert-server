# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.15
# DEPLOYED: 2026-04-19
# NOTES:
# - ✅ FIXED logging visibility (flush=True)
# - ✅ Added webhook hit logging
# - ✅ Added giveback debug prints
# - ✅ No strategy logic changes
# =========================

print("🔥🔥🔥 MAIN.PY v3.15 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# ⚙️ CONFIG
# =========================
MAX_OPEN_TRADES = 7
MIN_TREND = 0.20
MIN_MOM = 0.05
TRADE_SIZE_GBP = 100

ENABLE_GIVEBACK_EXIT = True
PROTECT_PROFIT_THRESHOLD = 0.2
GIVEBACK_RATIO = 0.5


def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🧠 DEBUG BUFFER
# =========================
DEBUG_SIGNALS = []
MAX_DEBUG_SIGNALS = 50


def add_debug_signal(signal):
    DEBUG_SIGNALS.append(signal)
    if len(DEBUG_SIGNALS) > MAX_DEBUG_SIGNALS:
        DEBUG_SIGNALS.pop(0)


# =========================
# 🧠 TIER CLASSIFICATION
# =========================
def classify_trade(momentum, trend):
    abs_mom = abs(momentum)
    abs_trend = abs(trend)

    if abs_mom >= 0.8 and abs_trend >= 0.25:
        return "A", "A+"
    if abs_mom >= 0.6 and abs_trend >= 0.2:
        return "A", "A"
    if abs_mom >= 0.3 and abs_trend >= 0.1:
        return "B", "B"

    return "C", "C"


# =========================
# 🏠 ROUTES
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200


@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


@app.route("/version", methods=["GET"])
def version():
    return jsonify({
        "version": "v3.15",
        "status": "running"
    })


@app.route("/debug_signals", methods=["GET"])
def debug_signals():
    return jsonify({
        "count": len(DEBUG_SIGNALS),
        "signals": DEBUG_SIGNALS
    })


# =========================
# 🚀 WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print("📩 WEBHOOK HIT", flush=True)

        data = request.get_json(force=True)

        symbol = data.get("symbol")
        decision = data.get("decision_model")
        price = float(data.get("price", 0))
        momentum = float(data.get("momentum_strength", 0))
        trend = float(data.get("trend_strength", 0))
        alignment = data.get("trend_alignment")
        data_version = data.get("data_version", "V3_UNKNOWN")

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        tier, subtier = classify_trade(momentum, trend)

        hold_reason = None

        # =========================
        # 🔥 ENTRY FILTER
        # =========================
        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        elif abs_trend < MIN_TREND:
            hold_reason = "not_strong_trend"

        elif abs_mom < MIN_MOM:
            hold_reason = "momentum_too_weak"

        if hold_reason is None and tier != "A":
            hold_reason = "low_quality"

        action = "OPEN" if hold_reason is None else "BLOCKED"

        print(f"📊 SIGNAL: {symbol} | {decision} | mom={momentum:.3f} | trend={trend:.3f} | {tier}/{subtier}", flush=True)

        conn = get_db()
        cur = conn.cursor()

        # =========================
        # 🚀 ENTRY
        # =========================
        if action == "OPEN":

            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            total_open = cur.fetchone()[0]

            if total_open >= MAX_OPEN_TRADES:
                print(f"⚠️ MAX OPEN TRADES REACHED ({total_open})", flush=True)

            else:
                cur.execute("""
                    SELECT COUNT(*) FROM bot_trades
                    WHERE symbol=%s AND status='OPEN'
                """, (symbol,))
                exists = cur.fetchone()[0]

                if exists == 0:
                    cur.execute("""
                        INSERT INTO bot_trades (
                            symbol, direction, entry_price,
                            status, opened_at, tier, data_version,
                            entry_momentum, entry_trend,
                            entry_tier, entry_subtier,
                            peak_pnl_percent
                        )
                        VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s)
                    """, (
                        symbol, decision, price,
                        tier, data_version,
                        momentum, trend,
                        tier, subtier,
                        0
                    ))

                    print(f"🚀 OPEN: {symbol} | {subtier}", flush=True)

                else:
                    print(f"⚠️ SKIPPED (already open): {symbol}", flush=True)

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason} | {subtier}", flush=True)

        # =========================
        # 🧠 EXIT ENGINE
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent
            FROM bot_trades
            WHERE status='OPEN'
            AND symbol = %s
        """, (symbol,))

        open_trades = cur.fetchall()

        for tid, sym, direction, entry_price, opened_at, peak_pnl in open_trades:

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - price) / entry_price)

            if abs(pnl) > 0.1:
                continue

            pnl_percent = pnl * 100

            # Update peak
            if pnl_percent > (peak_pnl or 0):
                cur.execute("""
                    UPDATE bot_trades
                    SET peak_pnl_percent = %s
                    WHERE id = %s
                """, (pnl_percent, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            # =========================
            # 💰 GIVEBACK ENGINE
            # =========================
            if ENABLE_GIVEBACK_EXIT and peak_pnl:

                if peak_pnl >= PROTECT_PROFIT_THRESHOLD:

                    giveback_level = peak_pnl * GIVEBACK_RATIO

                    print(f"📈 {sym} peak={peak_pnl:.3f}% | current={pnl_percent:.3f}% | giveback={giveback_level:.3f}%", flush=True)

                    if pnl_percent < giveback_level:
                        close_reason = "giveback_exit"
                        print(f"💡 GIVEBACK TRIGGERED: {sym}", flush=True)

            # =========================
            # FALLBACK EXITS
            # =========================
            if not close_reason:

                if pnl < -0.004:
                    close_reason = "hard_stop"

                elif pnl > 0.004 and mins < 10:
                    close_reason = "quick_profit"

                elif pnl > 0 and abs_mom < 0.1:
                    close_reason = "momentum_drop"

                elif pnl > 0 and alignment != "aligned":
                    close_reason = "trend_flip"

                elif mins > 20 and abs(pnl) < 0.001:
                    close_reason = "no_follow_through"

                elif mins > 60:
                    close_reason = "time_cut"

            if close_reason:
                pnl_gbp = (pnl_percent / 100) * TRADE_SIZE_GBP

                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        close_price=%s,
                        pnl_percent=%s,
                        pnl_gbp=%s,
                        trade_size_gbp=%s,
                        close_reason=%s,
                        exit_momentum=%s,
                        exit_trend=%s
                    WHERE id=%s
                """, (
                    price,
                    pnl_percent,
                    pnl_gbp,
                    TRADE_SIZE_GBP,
                    close_reason,
                    momentum,
                    trend,
                    tid
                ))

                print(f"💰 CLOSED: {sym} | {close_reason} | {round(pnl_percent,3)}%", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
