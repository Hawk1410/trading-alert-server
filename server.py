from flask import Flask, request, jsonify
import os
import psycopg2

app = Flask(__name__)

# === STRATEGY CONFIG ===
LONG_THRESHOLD = -1.3
EXTREME_THRESHOLD = -1.5

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


@app.route("/stats")
def stats():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT COUNT(*),
                   ROUND(AVG(pnl_pct), 3),
                   ROUND(AVG(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) * 100, 1)
            FROM trade_history
        """)
        trades, avg_pnl, win_rate = cursor.fetchone()

        cursor.execute("""
            SELECT COUNT(*),
                   ROUND(AVG(pnl_pct), 3),
                   ROUND(AVG(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) * 100, 1)
            FROM trade_history
            WHERE opened_at >= NOW() - INTERVAL '24 hours'
        """)
        trades_24h, avg_pnl_24h, win_rate_24h = cursor.fetchone()

        cursor.execute("""
            SELECT COUNT(*),
                   COUNT(*) FILTER (WHERE decision = 'LONG'),
                   COUNT(*) FILTER (WHERE decision = 'HOLD')
            FROM signal_history
        """)
        total_signals, long_signals, hold_signals = cursor.fetchone()

        conversion_rate = round((long_signals / total_signals) * 100, 2) if total_signals else 0

        cursor.execute("""
            SELECT ROUND(MIN(distance_from_vwap_pct), 3),
                   ROUND(MAX(distance_from_vwap_pct), 3),
                   ROUND(AVG(distance_from_vwap_pct), 3)
            FROM signal_history
        """)
        min_dist, max_dist, avg_dist = cursor.fetchone()

        cursor.execute("""
            SELECT ROUND(AVG(distance_from_vwap_pct), 3),
                   ROUND(MIN(distance_from_vwap_pct), 3),
                   ROUND(MAX(distance_from_vwap_pct), 3)
            FROM signal_history
            WHERE decision = 'LONG'
        """)
        trade_avg_dist, trade_min_dist, trade_max_dist = cursor.fetchone()

        cursor.execute("""
            SELECT symbol, price, vwap, distance_from_vwap_pct, decision, created_at
            FROM signal_history
            ORDER BY created_at DESC
            LIMIT 1
        """)
        last_signal = cursor.fetchone()

        cursor.execute("""
            SELECT symbol, result, pnl_pct, entry_price, exit_price, opened_at
            FROM trade_history
            ORDER BY opened_at DESC
            LIMIT 5
        """)
        recent_trades = cursor.fetchall()

        cursor.execute("""
            SELECT trade_open, direction, entry_price, stop_price, target_price, symbol
            FROM bot_state WHERE id = 1
        """)
        state = cursor.fetchone()

        cursor.close()
        conn.close()

        return jsonify({
            "performance_all_time": {
                "trades": trades,
                "avg_pnl": avg_pnl,
                "win_rate": win_rate
            },
            "performance_24h": {
                "trades": trades_24h,
                "avg_pnl": avg_pnl_24h,
                "win_rate": win_rate_24h
            },
            "signals": {
                "total": total_signals,
                "long": long_signals,
                "hold": hold_signals
            },
            "conversion": {"rate_pct": conversion_rate},
            "vwap_stats": {
                "min_distance": min_dist,
                "max_distance": max_dist,
                "avg_distance": avg_dist
            },
            "trade_distance": {
                "avg": trade_avg_dist,
                "min": trade_min_dist,
                "max": trade_max_dist
            },
            "last_signal": last_signal,
            "recent_trades": recent_trades,
            "current_trade": {
                "open": state[0],
                "direction": state[1],
                "entry": state[2],
                "stop": state[3],
                "target": state[4],
                "symbol": state[5]
            }
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

        if atr <= MIN_ATR:
            reason = "ATR too low"
        elif distance >= LONG_THRESHOLD:
            reason = "Not far enough from VWAP"
        else:
            decision = "LONG"
            reason = "Valid VWAP deviation"

        # 🔥 Detect extreme setups
        is_extreme = distance < EXTREME_THRESHOLD

        conn = get_db_connection()
        conn.autocommit = True
        cursor = conn.cursor()

        cursor.execute(
            """
            INSERT INTO signal_history
            (symbol, price, vwap, distance_from_vwap_pct, decision)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id
            """,
            (symbol, price, vwap, distance, decision),
        )

        print("\n==============================")
        print("Signal:", symbol, "| Distance:", round(distance, 3), "% | Decision:", decision)

        if is_extreme:
            print("🔥 EXTREME SETUP DETECTED")

        # Load state
        cursor.execute("""
            SELECT trade_open, direction, entry_price, stop_price, target_price, opened_at, symbol
            FROM bot_state WHERE id = 1
        """)
        state = cursor.fetchone()

        if state is None:
            return jsonify({"status": "error"}), 500

        trade_open, direction, entry_price, stop_price, target_price, opened_at, state_symbol = state

        # ENTRY
        if not trade_open and decision == "LONG":

            if is_extreme:
                print("🔥 EXTREME TRADE ENTRY")
            else:
                print("🟡 NORMAL TRADE ENTRY")

            entry_price = price
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
                    symbol = %s
                WHERE id = 1
            """, ("LONG", entry_price, stop_price, target_price, symbol))

            print("🚀 TRADE OPENED:", symbol, entry_price)

        # MANAGEMENT
        elif trade_open:
            if symbol != state_symbol:
                print(f"⚠️ Ignoring {symbol} update for active {state_symbol} trade")
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

                if pnl_pct < -5:
                    print("🚨 Abnormal PnL detected, skipping trade record")
                else:
                    cursor.execute("""
                        INSERT INTO trade_history
                        (symbol, direction, entry_price, stop_price, target_price, exit_price, result, pnl_pct, opened_at, closed_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                    """, (
                        state_symbol, direction,
                        float(entry_price), float(stop_price),
                        float(target_price), price,
                        result, pnl_pct, opened_at
                    ))

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

                print("✅ TRADE CLOSED:", result, round(pnl_pct, 3))

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("ERROR:", str(e))
        return jsonify({"status": "error", "message": str(e)}), 500

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
