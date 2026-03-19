from flask import Flask, request, jsonify
import os
import psycopg2

app = Flask(__name__)

LONG_THRESHOLD = -1.2
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

        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        # Store signal
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

        # Forward fill future prices
        cursor.execute(
            "UPDATE signal_history SET price_after_3_candles = %s WHERE id = %s AND price_after_3_candles IS NULL",
            (price, new_signal_id - 3),
        )
        cursor.execute(
            "UPDATE signal_history SET price_after_5_candles = %s WHERE id = %s AND price_after_5_candles IS NULL",
            (price, new_signal_id - 5),
        )
        cursor.execute(
            "UPDATE signal_history SET price_after_10_candles = %s WHERE id = %s AND price_after_10_candles IS NULL",
            (price, new_signal_id - 10),
        )

        # Load bot state
        cursor.execute(
            "SELECT trade_open, direction, entry_price, stop_price, target_price, opened_at, symbol FROM bot_state WHERE id = 1"
        )
        state = cursor.fetchone()

        if state is None:
            return jsonify({"status": "error", "message": "bot_state row missing"}), 500

        trade_open, direction, entry_price, stop_price, target_price, opened_at, state_symbol = state

        # ENTRY
        if not trade_open and decision == "LONG":
            entry_price = price
            stop_price = entry_price * (1 - STOP_LOSS / 100)
            target_price = entry_price * (1 + TAKE_PROFIT / 100)

            cursor.execute(
                """
                UPDATE bot_state
                SET trade_open = TRUE,
                    direction = %s,
                    entry_price = %s,
                    stop_price = %s,
                    target_price = %s,
                    opened_at = NOW(),
                    symbol = %s
                WHERE id = 1
                """,
                ("LONG", entry_price, stop_price, target_price, symbol),
            )

            print("\nTRADE OPENED")
            print("Entry:", entry_price)

        # MANAGEMENT
        elif trade_open:
            print("\nTrade open:", direction)

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

                cursor.execute(
                    """
                    INSERT INTO trade_history
                    (symbol, direction, entry_price, stop_price, target_price, exit_price, result, pnl_pct, opened_at, closed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """,
                    (
                        state_symbol,
                        direction,
                        float(entry_price),
                        float(stop_price),
                        float(target_price),
                        price,
                        result,
                        pnl_pct,
                        opened_at,
                    ),
                )

                cursor.execute(
                    """
                    UPDATE bot_state
                    SET trade_open = FALSE,
                        direction = NULL,
                        entry_price = NULL,
                        stop_price = NULL,
                        target_price = NULL,
                        opened_at = NULL,
                        symbol = NULL
                    WHERE id = 1
                    """
                )

                print("\nTRADE CLOSED")
                print("Result:", result)
                print("PnL:", round(pnl_pct, 3))

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("ERROR:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
