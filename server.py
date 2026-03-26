from flask import Flask, request, jsonify
import os
import psycopg2
import requests
import traceback
import uuid
from datetime import datetime

app = Flask(__name__)

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8
MIN_ATR = 0

# 🔥 FILTERS
MIN_DISTANCE = 0.5
MIN_CONFLUENCE = 2


# ================================
# DB CONNECTION
# ================================
def get_db_connection():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
        connect_timeout=10,
    )


# ================================
# TELEGRAM
# ================================
def send_telegram(msg):
    try:
        token = os.getenv("TELEGRAM_TOKEN")
        chat_id = os.getenv("TELEGRAM_CHAT_ID")

        if not token or not chat_id:
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        requests.post(url, json={
            "chat_id": chat_id,
            "text": msg
        }, timeout=5)

    except Exception as e:
        print("Telegram error:", e)


# ================================
# SESSION DETECTOR
# ================================
def get_session():
    hour = datetime.utcnow().hour

    if 0 <= hour < 8:
        return "asia"
    elif 8 <= hour < 16:
        return "london"
    else:
        return "ny"


# ================================
# VWAP BUCKET
# ================================
def get_vwap_bucket(distance):
    d = abs(distance)

    if d < 0.2:
        return "0-0.2"
    elif d < 0.5:
        return "0.2-0.5"
    elif d < 1:
        return "0.5-1"
    else:
        return "1+"


# ================================
# FUTURE PRICE UPDATER
# ================================
def update_future_prices(cursor, current_price):
    try:
        cursor.execute("""
            UPDATE signal_history
            SET price_after_3_candles = %s
            WHERE price_after_3_candles IS NULL
            AND created_at <= NOW() - INTERVAL '15 minutes'
        """, (current_price,))

        cursor.execute("""
            UPDATE signal_history
            SET price_after_5_candles = %s
            WHERE price_after_5_candles IS NULL
            AND created_at <= NOW() - INTERVAL '25 minutes'
        """, (current_price,))

        cursor.execute("""
            UPDATE signal_history
            SET price_after_10_candles = %s
            WHERE price_after_10_candles IS NULL
            AND created_at <= NOW() - INTERVAL '50 minutes'
        """, (current_price,))

    except Exception as e:
        print("Future update error:", e)


# ================================
# ENSURE SYMBOL ROW EXISTS
# ================================
def ensure_symbol_row(cursor, symbol):
    cursor.execute("""
        INSERT INTO bot_state (symbol, trade_open)
        VALUES (%s, FALSE)
        ON CONFLICT (symbol) DO NOTHING
    """, (symbol,))


# ================================
# WEBHOOK
# ================================
@app.route("/webhook", methods=["POST"])
def webhook():
    conn = None
    cursor = None

    try:
        data = request.get_json(force=True)

        symbol = data["symbol"]
        price = float(data["price"])
        vwap = float(data["vwap"])
        atr = float(data["atr"])

        momentum = data.get("momentum")
        vwap_trend = data.get("vwap_trend")
        timeframe = data.get("timeframe")

        volume = float(data.get("volume", 0))
        candle_time = int(float(data.get("candle_time", 0)))

        distance = ((price - vwap) / vwap) * 100
        distance_abs = price - vwap

        signal_id = str(uuid.uuid4())
        session = get_session()
        spread_proxy = atr
        vwap_bucket = get_vwap_bucket(distance)

        # ================================
        # CONFLUENCE
        # ================================
        confluence_score = 0

        if momentum == "up":
            confluence_score += 1
        if vwap_trend == "up":
            confluence_score += 1

        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        update_future_prices(cursor, price)

        # ================================
        # ENSURE SYMBOL EXISTS
        # ================================
        ensure_symbol_row(cursor, symbol)

        # ================================
        # GET 15m TREND
        # ================================
        cursor.execute("""
            SELECT vwap_trend
            FROM signal_history
            WHERE symbol = %s AND timeframe = '15'
            ORDER BY created_at DESC
            LIMIT 1
        """, (symbol,))
        result = cursor.fetchone()
        trend_15m = result[0] if result else None

        # ================================
        # DECISION
        # ================================
        decision_model = "HOLD"

        if atr > MIN_ATR:

            if (
                distance < -MIN_DISTANCE and
                momentum == "up" and
                trend_15m == "up"
            ):
                decision_model = "LONG"
                confluence_score += 1

            elif (
                distance > MIN_DISTANCE and
                momentum == "down" and
                trend_15m == "down"
            ):
                decision_model = "SHORT"
                confluence_score += 1

        # ================================
        # ALIGNMENT
        # ================================
        if trend_15m is None:
            trend_alignment = "unknown"
        elif (
            (momentum == "up" and trend_15m == "up") or
            (momentum == "down" and trend_15m == "down")
        ):
            trend_alignment = "aligned"
        else:
            trend_alignment = "counter"

        entry_signal_strength = confluence_score * 25
        market_regime = "trend" if atr > 0.5 else "range"

        trade_taken = False

        # ================================
        # FINAL FILTER
        # ================================
        valid_trade = (
            decision_model != "HOLD" and
            abs(distance) >= MIN_DISTANCE and
            confluence_score >= MIN_CONFLUENCE and
            trend_alignment in ["aligned", "unknown"]
        )

        # ================================
        # TRADE LOGIC (PER SYMBOL)
        # ================================
        if timeframe == "5" and valid_trade:

            cursor.execute("""
                SELECT trade_open
                FROM bot_state
                WHERE symbol = %s
            """, (symbol,))
            state = cursor.fetchone()

            trade_open = state[0] if state else False

            if not trade_open:

                if decision_model == "LONG":
                    stop_price = price * (1 - STOP_LOSS / 100)
                    target_price = price * (1 + TAKE_PROFIT / 100)

                else:  # SHORT
                    stop_price = price * (1 + STOP_LOSS / 100)
                    target_price = price * (1 - TAKE_PROFIT / 100)

                cursor.execute("""
                    UPDATE bot_state
                    SET trade_open = TRUE,
                        direction = %s,
                        entry_price = %s,
                        stop_price = %s,
                        target_price = %s,
                        opened_at = NOW()
                    WHERE symbol = %s
                """, (
                    decision_model,
                    price,
                    stop_price,
                    target_price,
                    symbol
                ))

                trade_taken = True

                send_telegram(f"{decision_model} {symbol} @ {price}")

        # ================================
        # SAVE SIGNAL
        # ================================
        cursor.execute("""
            INSERT INTO signal_history (
                symbol, price, vwap, distance_from_vwap_pct, distance_abs,
                atr, volume, decision, decision_model, is_extreme,
                momentum, vwap_trend, timeframe, candle_time,
                signal_id, session, spread_proxy, vwap_distance_bucket,
                entry_signal_strength, trade_taken, confluence_score,
                trend_alignment, market_regime
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol, price, vwap, distance, distance_abs,
            atr, volume, "SIGNAL", decision_model, distance < -1.5,
            momentum, vwap_trend, timeframe, candle_time,
            signal_id, session, spread_proxy, vwap_bucket,
            entry_signal_strength, trade_taken, confluence_score,
            trend_alignment, market_regime
        ))

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("ERROR:", str(e))
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
            
