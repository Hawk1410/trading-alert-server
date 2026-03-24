from flask import Flask, request, jsonify
import os
import psycopg2
import requests

app = Flask(__name__)

# === STRATEGY CONFIG ===
LONG_THRESHOLD = 999  # testing mode (change later)
SHORT_THRESHOLD = 999  # for data collection

EXTREME_THRESHOLD = -1.5

STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

MIN_ATR = 0  # testing mode (IMPORTANT)


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


# ================================
# TEST TELEGRAM
# ================================
@app.route("/test")
def test():
    send_telegram("🔥 BOT TEST WORKING")
    return {"status": "ok"}


# ================================
# DAILY REPORT
# ================================
@app.route("/daily-report")
def daily_report():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*),
                   ROUND(AVG(pnl_pct), 3),
                   ROUND(AVG(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) * 100, 1)
            FROM trade_history
            WHERE opened_at >= NOW() - INTERVAL '24 hours'
        """)

        trades, avg_pnl, win_rate = cursor.fetchone()

        msg = (
            f"📊 DAILY REPORT\n"
            f"Trades: {trades}\n"
            f"Win rate: {win_rate}%\n"
            f"Avg PnL: {avg_pnl}%"
        )

        send_telegram(msg)

        cursor.close()
        conn.close()

        return jsonify({"status": "sent"})

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

        distance = ((price - vwap) / vwap) * 100

        decision = "HOLD"
        reason = ""

        # ================================
        # DECISION LOGIC (LONG + SHORT DATA)
        # ================================
        if atr <= MIN_ATR:
            reason = "ATR too low"

        elif distance < 0:
            decision = "LONG"
            reason = "Below VWAP"

        elif distance > 0:
            decision = "SHORT"
            reason = "Above VWAP"

        is_extreme = distance < EXTREME_THRESHOLD

        # ================================
        # SAVE SIGNAL (NOW WITH ATR + DATA)
        # ================================
        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute("""
            INSERT INTO signal_history
            (symbol, price, vwap, distance_from_vwap_pct, atr, decision, is_extreme, momentum, vwap_trend)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            price,
            vwap,
            distance,
            atr,
            decision,
            is_extreme,
            momentum,
            vwap_trend
        ))

        # ================================
        # LOAD STATE
        # ================================
        cursor.execute("""
            SELECT trade_open, direction, entry_price, stop_price, target_price, opened_at, symbol, entry_distance
            FROM bot_state WHERE id = 1
        """)
        state = cursor.fetchone()

        trade_open, direction, entry_price, stop_price, target_price, opened_at, state_symbol, entry_distance = state

        # ================================
        # ENTRY (ONLY LONG FOR NOW)
        # ================================
        if not trade_open and decision == "LONG":

            entry_price = price
            entry_distance = distance

            stop_price = entry_price * (1 - STOP_LOSS / 100)
            target_price = entry_price * (1 + TAKE_PROFIT / 100)

            cursor.execute("""
                UPDATE bot_state
                SET trade_open = TRUE,
                    direction = %s,
                    entry_price = %s,
                    stop_price = %s,
                    target_price = %s,
                    opened_at = NOW(),
                    symbol = %s,
                    entry_distance = %s
                WHERE id = 1
            """, ("LONG", entry_price, stop_price, target_price, symbol, entry_distance))

            print("🚀 TRADE OPENED:", symbol)

            send_telegram(
                f"🚀 TRADE OPENED\n"
                f"{symbol}\n"
                f"Entry: {entry_price}\n"
                f"Distance: {round(entry_distance, 3)}%\n"
                f"{'🔥 EXTREME' if is_extreme else '🟡 NORMAL'}"
            )

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
                    (symbol, direction, entry_price, stop_price, target_price, exit_price, result, pnl_pct, opened_at, closed_at, entry_distance)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                """, (
                    state_symbol,
                    direction,
                    float(entry_price),
                    float(stop_price),
                    float(target_price),
                    price,
                    result,
                    pnl_pct,
                    opened_at,
                    entry_distance
                ))

                print("✅ TRADE CLOSED:", result)

                send_telegram(
                    f"✅ TRADE CLOSED\n"
                    f"{state_symbol}\n"
                    f"Result: {result}\n"
                    f"PnL: {round(pnl_pct, 3)}%"
                )

                cursor.execute("""
                    UPDATE bot_state
                    SET trade_open = FALSE,
                        direction = NULL,
                        entry_price = NULL,
                        stop_price = NULL,
                        target_price = NULL,
                        opened_at = NULL,
                        symbol = NULL,
                        entry_distance = NULL
                    WHERE id = 1
                """)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("ERROR:", str(e))
        return jsonify({"error": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
