from flask import Flask, request, jsonify
import os
import psycopg2
import traceback
from datetime import datetime

app = Flask(__name__)

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

# === EXECUTION FILTERS (NEW 🔥) ===
ALLOWED_MODELS = ["trend_model_v1"]  # ONLY trade best model

ALLOWED_VWAP_BUCKETS = ["0.2-0.5", "0.5-1"]  # sweet spot
REQUIRE_TREND_ALIGNMENT = True

# === DATABASE ===
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# === CORE DECISION ENGINE ===
def should_take_trade(data):
    model = data.get("model_version")
    vwap_bucket = data.get("vwap_distance_bucket")
    trend_alignment = data.get("trend_alignment")

    # 1. Model filter
    if model not in ALLOWED_MODELS:
        return False, "model_not_allowed"

    # 2. VWAP filter
    if vwap_bucket not in ALLOWED_VWAP_BUCKETS:
        return False, "bad_vwap_distance"

    # 3. Trend alignment filter
    if REQUIRE_TREND_ALIGNMENT and trend_alignment != "aligned":
        return False, "not_trend_aligned"

    return True, "passed_all_filters"

# === MAIN WEBHOOK ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json

        model_version = data.get("model_version")
        decision_model = data.get("decision_model")
        symbol = data.get("symbol")
        price = float(data.get("price", 0))

        # === FILTER DECISION ===
        trade_taken, hold_reason = should_take_trade(data)

        # === DB INSERT ===
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
                trend_alignment
            )
            VALUES (
                gen_random_uuid(),
                NOW(),
                %s, %s, %s, %s,
                %s, %s, %s, %s
            )
        """, (
            model_version,
            decision_model,
            symbol,
            price,
            trade_taken,
            hold_reason,
            data.get("vwap_distance_bucket"),
            data.get("trend_alignment")
        ))

        conn.commit()
        cur.close()
        conn.close()

        # === EXECUTION RESPONSE ===
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


# === HEALTH CHECK ===
@app.route("/")
def home():
    return "Bot is running 🚀"
    
