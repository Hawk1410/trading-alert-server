from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

MIN_MOMENTUM = 0.001
MIN_TREND_STRENGTH = 0.0001

def log(msg):
    print(msg, flush=True)

def get_connection():
    return psycopg2.connect(DATABASE_URL)

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    log(f"Incoming signal: {data}")

    try:
        symbol = data.get("symbol")
        decision = data.get("decision_model")
        price = float(data.get("price", 0))

        trend_alignment = data.get("trend_alignment")
        momentum_strength = float(data.get("momentum_strength", 0))
        trend_strength = float(data.get("trend_strength", 0))
        model_version = data.get("model_version", "unknown")
        vwap_bucket = data.get("vwap_distance_bucket")

        # === FILTERS ===
        if decision == "NONE":
            log("FILTERED - no decision")
            return jsonify({"status": "filtered"})

        if abs(momentum_strength) < MIN_MOMENTUM:
            log(f"FILTERED - weak momentum {momentum_strength}")
            return jsonify({"status": "filtered"})

        if abs(trend_strength) < MIN_TREND_STRENGTH:
            log(f"FILTERED - weak trend {trend_strength}")
            return jsonify({"status": "filtered"})

        strategy_type = "trend" if trend_alignment == "aligned" else "counter"

        conn = get_connection()
        cur = conn.cursor()

        # === CHECK EXISTING OPEN TRADE ===
        cur.execute("""
            SELECT id FROM bot_trades
            WHERE symbol = %s AND status = 'OPEN'
        """, (symbol,))
        existing = cur.fetchone()

        if existing:
            log("Trade already open")
            cur.close()
            conn.close()
            return jsonify({"status": "exists"})

        # === INSERT TRADE ===
        cur.execute("""
            INSERT INTO bot_trades (
                symbol,
                direction,
                entry_price,
                status,
                opened_at,
                strategy_type,
                model_version,
                trend_alignment,
                momentum_strength,
                trend_strength,
                vwap_bucket
            ) VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            decision,
            price,
            datetime.utcnow(),
            strategy_type,
            model_version,
            trend_alignment,
            momentum_strength,
            trend_strength,
            vwap_bucket
        ))

        conn.commit()
        cur.close()
        conn.close()

        log(f"TRADE OPENED: {symbol} {decision} ({strategy_type})")

        return jsonify({"status": "opened"})

    except Exception as e:
        log(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 200  # <- IMPORTANT (prevents webhook death)


@app.route("/check-trades", methods=["GET"])
def check_trades():
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT id, symbol, direction, entry_price
            FROM bot_trades
            WHERE status = 'OPEN'
        """)

        trades = cur.fetchall()

        for trade in trades:
            trade_id, symbol, direction, entry_price = trade

            current_price = entry_price  # placeholder

            pnl = ((current_price - entry_price) / entry_price) * 100
            if direction == "SHORT":
                pnl = -pnl

            if pnl <= -STOP_LOSS or pnl >= TAKE_PROFIT:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = %s,
                        pnl_percent = %s
                    WHERE id = %s
                """, (datetime.utcnow(), pnl, trade_id))

                log(f"CLOSED TRADE {trade_id} | {pnl:.2f}%")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "checked"})

    except Exception as e:
        log(f"ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 200


@app.route("/")
def home():
    return "Bot is running"
