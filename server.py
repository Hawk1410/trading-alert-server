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

DATA_VERSION = "v2_clean"


def log(msg):
    print(msg, flush=True)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


# 🔥 FETCH REAL PRICE FROM BINANCE
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
    log(f"📩 Incoming signal: {data}")

    try:
        symbol = data.get("symbol")
        decision = data.get("decision_model")
        signal_price = float(data.get("price", 0))

        trend_alignment = data.get("trend_alignment")
        momentum_strength = float(data.get("momentum_strength", 0))
        trend_strength = float(data.get("trend_strength", 0))
        model_version = data.get("model_version", "unknown")
        vwap_bucket = data.get("vwap_distance_bucket")

        conn = get_connection()
        cur = conn.cursor()

        # ================================
        # 🧠 HOLD REASON LOGIC
        # ================================
        hold_reason = None

        if decision == "NONE":
            hold_reason = "no_decision"

        elif abs(momentum_strength) < MIN_MOMENTUM:
            hold_reason = f"weak_momentum ({momentum_strength:.6f})"

        elif abs(trend_strength) < MIN_TREND_STRENGTH:
            hold_reason = f"weak_trend ({trend_strength:.6f})"

        elif trend_alignment != "aligned":
            hold_reason = "counter_trend"

        # ================================
        # 📊 LOG SIGNAL (FIXED + EXPANDED)
        # ================================
        try:
            cur.execute("""
                INSERT INTO signal_history (
                    symbol,
                    created_at,
                    price,
                    decision,
                    distance_from,
                    momentum_strength,
                    trend_strength,
                    trend_alignment,
                    hold_reason
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol,
                datetime.utcnow(),
                signal_price,
                decision,
                vwap_bucket,
                momentum_strength,
                trend_strength,
                trend_alignment,
                hold_reason
            ))
            conn.commit()

        except Exception as e:
            log(f"❌ SIGNAL LOG ERROR: {e}")

        # ================================
        # 🔄 CLOSE EXISTING TRADES
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
        # 🚫 FILTERS (WITH LOGGING)
        # ================================
        if hold_reason is not None:
            log(f"🛑 FILTERED: {hold_reason}")
            cur.close()
            conn.close()
            return jsonify({"status": "filtered", "reason": hold_reason})

        # ================================
        # 📌 STRATEGY TYPE
        # ================================
        strategy_type = "trend" if trend_alignment == "aligned" else "counter"

        # ================================
        # 🔍 CHECK EXISTING TRADE
        # ================================
        cur.execute("""
            SELECT id FROM bot_trades
            WHERE symbol = %s AND status = 'OPEN'
            AND data_version = %s
        """, (symbol, DATA_VERSION))

        existing = cur.fetchone()

        if existing:
            log("⚠️ Trade already open")
            cur.close()
            conn.close()
            return jsonify({"status": "exists"})

        # ================================
        # 🚀 OPEN TRADE
        # ================================
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
                vwap_bucket,
                data_version
            ) VALUES (%s, %s, %s, 'OPEN', %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            decision,
            signal_price,
            datetime.utcnow(),
            strategy_type,
            model_version,
            trend_alignment,
            momentum_strength,
            trend_strength,
            vwap_bucket,
            DATA_VERSION
        ))

        conn.commit()
        cur.close()
        conn.close()

        log(f"🚀 TRADE OPENED: {symbol} {decision} ({strategy_type})")

        return jsonify({"status": "opened"})

    except Exception as e:
        log(f"❌ ERROR: {str(e)}")
        return jsonify({"error": str(e)}), 200


@app.route("/")
def home():
    return "Bot is running"
