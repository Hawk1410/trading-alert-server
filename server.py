from flask import Flask, request, jsonify
import os
import psycopg2
import traceback
from datetime import datetime

app = Flask(__name__)

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

# === V2 EXECUTION FILTERS ===
ALLOWED_MODELS = ["trend_model_v1"]

ALLOWED_VWAP_BUCKETS = ["0.2-0.5", "0.5-1"]
REQUIRE_TREND_ALIGNMENT = True

# NEW 🔥
MIN_MOMENTUM = 0.5
MIN_ATR = 0.2
MIN_TREND_STRENGTH = 0.5

# === DATABASE ===
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# === CORE DECISION ENGINE V2 ===
def should_take_trade(data):
    model = data.get("model_version")
    vwap_bucket = data.get("vwap_distance_bucket")
    trend_alignment = data.get("trend_alignment")

    momentum = float(data.get("momentum_strength", 0))
    atr = float(data.get("atr", 0))
    trend_strength = float(data.get("trend_strength", 0))

    # 1. Model filter
    if model not in ALLOWED_MODELS:
        return False, "model_not_allowed"

    # 2. VWAP filter
    if vwap_bucket not in ALLOWED_VWAP_BUCKETS:
        return False, "bad_vwap_distance"

    # 3. Trend alignment
    if REQUIRE_TREND_ALIGNMENT and trend_alignment != "aligned":
        return False, "not_trend_aligned"

    # 4. Momentum filter 🔥
    if momentum < MIN_MOMENTUM:
        return False, "low_momentum"

    # 5. Volatility filter 🔥
    if atr < MIN_ATR:
        return False, "low_volatility"

    # 6. Chop filter 🔥
    if trend_strength < MIN_TREND_STRENGTH:
        return False, "weak_trend"

    return True, "v2_pass"

# === WEBHOOK ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json

        model_version = data.get("model_version")
        decision_model = data.get("decision_model")
        symbol = data.get("symbol")
        price = float(data.get("price", 0))

        trade_taken, hold_reason = should_take_trade(data)

        conn = get_db_connection()
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO signal_history (
                id,
                created_at,
                model_version,
                decision_model,
                symbol,
                entry_price,
                trade_taken,
                hold_reason,
                vwap_distance_bucket,
                trend_alignment,
                momentum_strength,
                atr,
                trend_strength
            )
            VALUES (
                gen_random_uuid(),
                NOW(),
                %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s, %s
            )
        """, (
            model_version,
            decision_model,
            symbol,
            price,
            trade_taken,
            hold_reason,
            data.get("vwap_distance_bucket"),
            data.get("trend_alignment"),
            data.get("momentum_strength"),
            data.get("atr"),
            data.get("trend_strength")
        ))

        conn.commit()
        cur.close()
        conn.close()

        if trade_taken and decision_model in ["LONG", "SHORT"]:
            return jsonify({
                "action": decision_model,
                "symbol": symbol,
                "entry_price": price,
                "stop_loss": STOP_LOSS,
                "take_profit": TAKE_PROFIT
            })

        return jsonify({
            "action": "HOLD",
            "reason": hold_reason
        })

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500


@app.route("/")
def home():
    return "Trend V2 running 🚀"
