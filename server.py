from flask import Flask, request, jsonify
import os
import psycopg2
import requests
import traceback

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
            print("⚠️ Telegram not configured")
            return

        url = f"https://api.telegram.org/bot{token}/sendMessage"

        requests.post(url, json={
            "chat_id": chat_id,
            "text": msg
        }, timeout=5)

    except Exception as e:
        print("Telegram error:", e)


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
        print(traceback.format_exc())


# ================================
# HEALTH
# ================================
@app.route("/")
def home():
    return "Trading bot running"


@app.route("/health")
def health():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.fetchone()
        cur.close()
        conn.close()
        return jsonify({"status": "ok"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


@app.route("/test")
def test():
    send_telegram("🔥 BOT TEST WORKING")
    return {"status": "ok"}


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
        distance_abs = price - vwap  # 🔥 NEW

        # ================================
        # DB
        # ================================
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        # Update old rows
        try:
            update_future_prices(cursor, price)
        except:
            pass

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
        # MODEL DECISION
        # ================================
        decision_model = "HOLD"

        if atr > MIN_ATR:

            if (
                distance < -0.5
                and momentum == "up"
                and trend_15m == "up"
            ):
                decision_model = "LONG"

            elif (
                distance > 0.5
                and momentum == "down"
                and trend_15m == "down"
            ):
                decision_model = "SHORT"

        # ================================
        # SAVE SIGNAL (FULL DATASET)
        # ================================
        cursor.execute("""
            INSERT INTO signal_history
            (symbol, price, vwap, distance_from_vwap_pct, distance_abs, atr, volume, decision, decision_model, is_extreme, momentum, vwap_trend, timeframe, candle_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            price,
            vwap,
            distance,
            distance_abs,
            atr,
            volume,
            "SIGNAL",
            decision_model,
            distance < -1.5,
            momentum,
            vwap_trend,
            timeframe,
            candle_time
        ))

        # ================================
        # ONLY TRADE ON 5m
        # ================================
        if timeframe != "5":
            return jsonify({"status": "logged_only"}), 200

        decision = decision_model

        # ================================
        # LOAD STATE
        # ================================
        cursor.execute("""
            SELECT trade_open, direction, entry_price, stop_price, target_price, opened_at, symbol
            FROM bot_state WHERE id = 1
        """)
        state = cursor.fetchone()

        trade_open, direction, entry_price, stop_price, target_price, opened_at, state_symbol = state

        # ================================
        # ENTRY
        # ================================
        if not trade_open and decision == "LONG":

            stop_price = price * (1 - STOP_LOSS / 100)
            target_price = price * (1 + TAKE_PROFIT / 100)

            cursor.execute("""
                UPDATE bot_state
                SET trade_open = TRUE,
                    direction = %s,
                    entry_price = %s,
                    stop_price = %s,
                    target_price = %s,
                    opened_at = NOW(),
                    symbol = %s
                WHERE id = 1
            """, ("LONG", price, stop_price, target_price, symbol))

            send_telegram(f"🚀 LONG {symbol} @ {price}")

        # ================================
        # MANAGEMENT
        # ================================
        elif trade_open:

            if symbol != state_symbol:
                return jsonify({"status": "ignored"}), 200

            close_trade = False
            result = None

            if direction == "LONG":
                if price <= float(stop_price):
                    result = "LOSS"
                    close_trade = True
                elif price >= float(target_price):
                    result = "WIN"
                    close_trade = True

            if close_trade:
                pnl_pct = ((price - float(entry_price)) / float(entry_price)) * 100

                cursor.execute("""
                    INSERT INTO trade_history
                    (symbol, direction, entry_price, stop_price, target_price, exit_price, result, pnl_pct, opened_at, closed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                """, (
                    state_symbol,
                    direction,
                    float(entry_price),
                    float(stop_price),
                    float(target_price),
                    price,
                    result,
                    pnl_pct,
                    opened_at
                ))

                send_telegram(f"✅ {result} {state_symbol} ({round(pnl_pct,2)}%)")

                cursor.execute("""
                    UPDATE bot_state
                    SET trade_open = FALSE,
                        direction = NULL,
                        entry_price = NULL,
                        stop_price = NULL,
                        target_price = NULL,
                        opened_at = NULL,
                        symbol = NULL
                    WHERE id = 1
                """)

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
