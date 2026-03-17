from flask import Flask, request, jsonify
import os
import psycopg2

app = Flask(__name__)

LONG_THRESHOLD = -0.7
SHORT_THRESHOLD = 0.6
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8
MIN_ATR = 80


def get_db_connection():
    return psycopg2.connect(
        os.getenv("DATABASE_URL"),
        sslmode="require",
        connect_timeout=10,
    )


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
        return jsonify({"status": "ok", "database": "connected"})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


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

        distance = ((price - vwap) / vwap) * 100

        decision = "HOLD"
        if distance < LONG_THRESHOLD and atr > MIN_ATR:
            decision = "LONG"
        elif distance > SHORT_THRESHOLD and atr > MIN_ATR:
            decision = "SHORT"

        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        # Store current signal
        cursor.execute(
            """
            INSERT INTO signal_history
            (symbol, price, vwap, distance_from_vwap_pct, decision)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (symbol, price, vwap, distance, decision),
        )
        new_signal_id = cursor.fetchone()[0]

        print("\n==============================")
        print("Signal received")
        print("Symbol:", symbol)
        print("Price:", price)
        print("VWAP:", vwap)
        print("ATR:", atr)
        print("Distance from VWAP:", round(distance, 3), "%")
        print("Decision:", decision)
        print("Signal ID:", new_signal_id)

        # Fill forward-price columns for earlier signals
        cursor.execute(
            """
            UPDATE signal_history
            SET price_after_3_candles = %s
            WHERE id = %s AND price_after_3_candles IS NULL
            """,
            (price, new_signal_id - 3),
        )

        cursor.execute(
            """
            UPDATE signal_history
            SET price_after_5_candles = %s
            WHERE id = %s AND price_after_5_candles IS NULL
            """,
            (price, new_signal_id - 5),
        )

        cursor.execute(
            """
            UPDATE signal_history
            SET price_after_10_candles = %s
            WHERE id = %s AND price_after_10_candles IS NULL
            """,
            (price, new_signal
