# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.10.2
# DEPLOYED: 2026-04-17
# NOTES:
# - Added startup print (debug)
# - Added /version endpoint (critical test)
# - NO logic changes
# =========================

print("🔥🔥🔥 MAIN.PY IS DEFINITELY RUNNING 🔥🔥🔥")

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

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
# ⚙️ CONFIG
# =========================
MAX_OPEN_TRADES = 7
MIN_TREND = 0.20
MIN_MOM = 0.05


def get_db():
    return psycopg2.connect(DATABASE_URL)


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
        "version": "v3.10.2",
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
        data = request.get_json(force=True)

        symbol = data.get("symbol")
        decision = data.get("decision_model")
        price = float(data.get("price", 0))
        momentum = float(data.get("momentum_strength", 0))
        trend = float(data.get("trend_strength", 0))
        alignment = data.get("trend_alignment")

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        tier, subtier = classify_trade(momentum, trend)

        hold_reason = None

        # ENTRY FILTER
        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        elif abs_trend < MIN_TREND:
            hold_reason = "not_strong_trend"

        elif abs_mom < MIN_MOM:
            hold_reason = "momentum_too_weak"

        # TIER FILTER
        if hold_reason is None and tier != "A":
            hold_reason = "low_quality"

        action = "OPEN" if hold_reason is None else "BLOCKED"

        debug_payload = {
            "time": now.isoformat(),
            "symbol": symbol,
            "decision": decision,
            "momentum": momentum,
            "trend": trend,
            "tier": tier,
            "subtier": subtier,
            "hold_reason": hold_reason,
            "action": action
        }

        add_debug_signal(debug_payload)

        print(f"{action}: {symbol} | {subtier} | {hold_reason}")

        return jsonify(debug_payload), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
