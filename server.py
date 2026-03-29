from flask import Flask, request, jsonify
import os
import psycopg2
import traceback
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

# === DB CONNECTION ===
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# === WEBHOOK ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json

        print("📩 Incoming signal:", data)

        symbol = data.get("symbol")
        price = float(data.get("price", 0))
        decision_model = data.get("decision_model")
        model_version = data.get("model_version")

        trend_alignment = data.get("trend_alignment")
        momentum = float(data.get("momentum", 0))

        # 🔥 FIXED SAFE TREND STRENGTH
        trend_strength_raw = data.get("trend_strength")

        try:
            trend_strength = float(trend_strength_raw)
        except:
            trend_strength = None

        conn = get_db_connection()
        cur = conn.cursor()

        # === FILTER LOGIC ===
        take_trade = False

        if decision_model in ["LONG", "SHORT"]:
            
            # ✅ alignment must be good
            if trend_alignment == "aligned":

                # ✅ momentum must confirm
                if momentum > 0:

                    # ✅ FIXED trend strength logic
                    if trend_strength is None or trend_strength > 0:
                        take_trade = True

        # === INSERT SIGNAL ===
        cur.execute("""
            INSERT INTO signal_history (
                symbol,
                price,
                decision_model,
                model_version,
                trend_alignment,
                momentum,
                trend_strength,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            price,
            decision_model,
            model_version,
            trend_alignment,
            momentum,
            trend_strength,
            datetime.utcnow()
        ))

        # === CREATE TRADE ===
        if take_trade:
            cur.execute("""
                INSERT INTO bot_trades (
                    symbol,
                    direction,
                    entry_price,
                    status,
                    opened_at
                )
                VALUES (%s,%s,%s,'OPEN',%s)
            """, (
                symbol,
                decision_model,
                price,
                datetime.utcnow()
            ))

            print("✅ TRADE OPENED")

        else:
            print("⛔ FILTERED OUT")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"})

    except Exception as e:
        print("❌ ERROR:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/")
def home():
    return "🚀 Trading bot is live!"
