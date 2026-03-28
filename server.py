from flask import Flask, request, jsonify
import os
import psycopg2
import requests
import traceback
import uuid
import json
from datetime import datetime

app = Flask(__name__)

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8
MIN_ATR = 0


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

        requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": msg},
            timeout=5
        )
    except Exception as e:
        print("Telegram error:", e)


# ================================
# HELPERS
# ================================
def get_session():
    hour = datetime.utcnow().hour
    if hour < 8:
        return "asia"
    elif hour < 16:
        return "london"
    else:
        return "ny"


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
# ADAPTIVE RULES
# ================================
def get_adaptive_rules():
    try:
        return json.loads(os.getenv("ADAPTIVE_RULES", "{}"))
    except:
        return {}


def adaptive_model(signal):
    rules = get_adaptive_rules()

    if "allowed_sessions" in rules:
        if signal.get("session") not in rules["allowed_sessions"]:
            return "HOLD"

    if "allowed_directions" in rules:
        if signal["base_decision"] not in rules["allowed_directions"]:
            return "HOLD"

    if "min_confluence" in rules:
        if signal["confluence_score"] < rules["min_confluence"]:
            return "HOLD"

    return signal["base_decision"]


# ================================
# 🔥 EXPLORATORY MODELS
# ================================
def exploratory_model_v1(signal):

    if (
        signal["distance"] < -0.3 and
        signal["momentum"] == "up"
    ):
        return "LONG"

    if (
        signal["distance"] > 0.3 and
        signal["momentum"] == "down"
    ):
        return "SHORT"

    return "HOLD"


def exploratory_model_v2_safe(signal):

    d = abs(signal["distance"])

    if not (0.2 <= d <= 1):
        return "HOLD"

    if (
        signal["distance"] < 0 and
        signal["momentum"] == "up"
    ):
        return "LONG"

    if (
        signal["distance"] > 0 and
        signal["momentum"] == "down"
    ):
        return "SHORT"

    return "HOLD"


def exploratory_model_v3_aggressive(signal):

    d = abs(signal["distance"])

    if not (0.2 <= d <= 1):
        return "HOLD"

    if signal["trend_alignment"] != "counter":
        return "HOLD"

    if (
        signal["distance"] < 0 and
        signal["momentum"] == "up"
    ):
        return "LONG"

    if (
        signal["distance"] > 0 and
        signal["momentum"] == "down"
    ):
        return "SHORT"

    return "HOLD"


# ================================
# CLOSE TRADES
# ================================
def close_open_trades(cursor, symbol, price):

    cursor.execute("""
        SELECT id, direction, stop_price, target_price
        FROM bot_trades
        WHERE symbol = %s AND status = 'OPEN'
    """, (symbol,))

    trades = cursor.fetchall()

    for trade_id, direction, stop, target in trades:

        close_reason = None

        if direction == "LONG":
            if price <= stop:
                close_reason = "STOP_LOSS"
            elif price >= target:
                close_reason = "TAKE_PROFIT"

        elif direction == "SHORT":
            if price >= stop:
                close_reason = "STOP_LOSS"
            elif price <= target:
                close_reason = "TAKE_PROFIT"

        if close_reason:
            cursor.execute("""
                UPDATE bot_trades
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_price = %s,
                    close_reason = %s
                WHERE id = %s
            """, (price, close_reason, trade_id))

            send_telegram(f"{symbol} {direction} CLOSED ({close_reason}) @ {price}")


# ================================
# FUTURE PRICE UPDATER
# ================================
def update_future_prices(cursor, current_price):
    try:
        cursor.execute("""
            UPDATE signal_history
            SET price_after_5_candles = %s
            WHERE price_after_5_candles IS NULL
            AND created_at <= NOW() - INTERVAL '25 minutes'
        """, (current_price,))

        cursor.execute("""
            UPDATE signal_history
            SET is_win = CASE
                WHEN decision_model = 'LONG' AND price_after_5_candles > price THEN TRUE
                WHEN decision_model = 'SHORT' AND price_after_5_candles < price THEN TRUE
                WHEN decision_model IN ('LONG','SHORT') THEN FALSE
                ELSE NULL
            END
            WHERE price_after_5_candles IS NOT NULL
            AND is_win IS NULL
        """)

    except Exception as e:
        print("Future update error:", e)


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
        vwap_bucket = get_vwap_bucket(distance)

        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        close_open_trades(cursor, symbol, price)
        update_future_prices(cursor, price)

        cursor.execute("""
            SELECT vwap_trend
            FROM signal_history
            WHERE symbol = %s AND timeframe = '15'
            ORDER BY created_at DESC
            LIMIT 1
        """, (symbol,))
        result = cursor.fetchone()
        trend_15m = result[0] if result else None

        if trend_15m is None:
            trend_alignment = "unknown"
        elif (
            (momentum == "up" and trend_15m == "up") or
            (momentum == "down" and trend_15m == "down")
        ):
            trend_alignment = "aligned"
        else:
            trend_alignment = "counter"

        signal = {
            "base_decision": "HOLD",
            "trend_alignment": trend_alignment,
            "confluence_score": 0,
            "session": session,
            "distance": distance,
            "momentum": momentum
        }

        models = {
            "adaptive_v1": adaptive_model(signal),
            "exploratory_v1": exploratory_model_v1(signal),
            "exploratory_v2_safe": exploratory_model_v2_safe(signal),
            "exploratory_v3_aggressive": exploratory_model_v3_aggressive(signal)
        }

        # 🚀 EXECUTION = v2 SAFE
        trade_taken = False
        decision_model = models["exploratory_v2_safe"]

        if timeframe == "5" and decision_model != "HOLD":

            cursor.execute("""
                SELECT COUNT(*)
                FROM bot_trades
                WHERE symbol = %s
                AND direction = %s
                AND status = 'OPEN'
            """, (symbol, decision_model))

            exists = cursor.fetchone()[0]

            if exists == 0:

                if decision_model == "LONG":
                    stop_price = price * (1 - STOP_LOSS / 100)
                    target_price = price * (1 + TAKE_PROFIT / 100)
                else:
                    stop_price = price * (1 + STOP_LOSS / 100)
                    target_price = price * (1 - TAKE_PROFIT / 100)

                cursor.execute("""
                    INSERT INTO bot_trades (
                        id, symbol, direction, entry_price,
                        stop_price, target_price, status, opened_at
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, 'OPEN', NOW())
                """, (
                    str(uuid.uuid4()),
                    symbol,
                    decision_model,
                    price,
                    stop_price,
                    target_price
                ))

                trade_taken = True
                send_telegram(f"{decision_model} {symbol} @ {price}")

        # SAVE ALL MODELS
        for model_name, decision in models.items():
            cursor.execute("""
                INSERT INTO signal_history (
                    symbol, price, vwap, distance_from_vwap_pct, distance_abs,
                    atr, volume, decision, decision_model,
                    momentum, vwap_trend, timeframe, candle_time,
                    signal_id, session, vwap_distance_bucket,
                    trade_taken, trend_alignment, model_version
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, price, vwap, distance, distance_abs,
                atr, volume, "SIGNAL", decision,
                momentum, vwap_trend, timeframe, candle_time,
                signal_id, session, vwap_bucket,
                trade_taken if model_name == "exploratory_v2_safe" else False,
                trend_alignment, model_name
            ))

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print(traceback.format_exc())
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
