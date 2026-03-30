from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

MIN_MOMENTUM = 0.001
MIN_TREND_STRENGTH = 0.0001

DATA_VERSION = "v2_final"


def log(msg):
    print(msg, flush=True)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def get_live_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        response = requests.get(url, timeout=2)
        data = response.json()
        return float(data["price"])
    except Exception as e:
        log(f"PRICE FETCH ERROR for {symbol}: {e}")
        return None


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    log(f"📩 DATA_V2 PAYLOAD: {data}")

    try:
        # ================================
        # INPUTS (STRICT + SAFE)
        # ================================
        symbol = data.get("symbol")
        decision = data.get("decision_model", "NONE")
        price = float(data.get("price", 0))

        trend_alignment = data.get("trend_alignment", "unknown")
        momentum_strength = float(data.get("momentum_strength", 0))
        trend_strength = float(data.get("trend_strength", 0))
        vwap_bucket = data.get("vwap_distance_bucket", "unknown")

        model_version = data.get("model_version", "unknown")

        conn = get_connection()
        cur = conn.cursor()

        # ================================
        # HOLD REASON ENGINE (FINAL)
        # ================================
        hold_reason = None

        if decision == "NONE":
            hold_reason = "no_decision"

        elif abs(momentum_strength) < MIN_MOMENTUM:
            hold_reason = f"weak_momentum"

        elif abs(trend_strength) < MIN_TREND_STRENGTH:
            hold_reason = f"weak_trend"

        elif trend_alignment != "aligned":
            hold_reason = "counter_trend"

        # ================================
        # TRADE FLAG
        # ================================
        trade_taken = False

        # ================================
        # CLOSE EXISTING TRADES
        # ================================
        cur.execute("""
            SELECT id, symbol, direction, entry_price
            FROM bot_trades
            WHERE status = 'OPEN'
            AND data_version = %s
        """, (DATA_VERSION,))

        open_trades = cur.fetchall()

        for trade in open_trades:
            trade_id, trade_symbol, direction, entry_price = trade

            live_price = get_live_price(trade_symbol)
            if live_price is None:
                continue

            pnl = ((live_price - entry_price) / entry_price) * 100
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

                log(f"💥 CLOSED TRADE {trade_id} | {pnl:.2f}%")

        conn.commit()

        # ================================
        # FILTER (NO TRADE)
        # ================================
        if hold_reason is not None:
            log(f"🛑 FILTERED: {hold_reason}")

            cur.execute("""
                INSERT INTO signal_history_v2 (
                    symbol, price, decision_model, trade_taken,
                    trend_alignment, momentum_strength, trend_strength,
                    vwap_distance_bucket, hold_reason,
                    model_version, data_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, price, decision, False,
                trend_alignment, momentum_strength, trend_strength,
                vwap_bucket, hold_reason,
                model_version, DATA_VERSION
            ))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"status": "filtered"})

        # ================================
        # CHECK EXISTING TRADE
        # ================================
        cur.execute("""
            SELECT id FROM bot_trades
            WHERE symbol = %s AND status = 'OPEN'
            AND data_version = %s
        """, (symbol, DATA_VERSION))

        if cur.fetchone():
            log("⚠️ Trade already open")

            cur.execute("""
                INSERT INTO signal_history_v2 (
                    symbol, price, decision_model, trade_taken,
                    trend_alignment, momentum_strength, trend_strength,
                    vwap_distance_bucket, hold_reason,
                    model_version, data_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, price, decision, False,
                trend_alignment, momentum_strength, trend_strength,
                vwap_bucket, "trade_exists",
                model_version, DATA_VERSION
            ))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"status": "exists"})

        # ================================
        # OPEN TRADE
        # ================================
        strategy_type = "trend" if trend_alignment == "aligned" else "counter"

        cur.execute("""
            INSERT INTO bot_trades (
                symbol, direction, entry_price, status, opened_at,
                strategy_type, model_version, trend_alignment,
                momentum_strength, trend_strength,
                vwap_bucket, data_version
            ) VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol, decision, price, datetime.utcnow(),
            strategy_type, model_version, trend_alignment,
            momentum_strength, trend_strength,
            vwap_bucket, DATA_VERSION
        ))

        trade_taken = True

        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, price, decision_model, trade_taken,
                trend_alignment, momentum_strength, trend_strength,
                vwap_distance_bucket, hold_reason,
                model_version, data_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol, price, decision, True,
            trend_alignment, momentum_strength, trend_strength,
            vwap_bucket, "trade_opened",
            model_version, DATA_VERSION
        ))

        conn.commit()
        cur.close()
        conn.close()

        log(f"🚀 TRADE OPENED: {symbol} {decision}")

        return jsonify({"status": "opened"})

    except Exception as e:
        log(f"❌ ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 200


@app.route("/")
def home():
    return "Data V2 Bot Running"
