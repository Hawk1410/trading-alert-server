from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🚀 WEBHOOK (V2.5 EXECUTION ENGINE)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📩 PAYLOAD:", data)

    symbol = data.get("symbol")
    decision = data.get("decision_model")
    price = data.get("price")
    momentum = data.get("momentum_strength")
    trend = data.get("trend_strength")
    alignment = data.get("trend_alignment")
    data_version = data.get("data_version")

    conn = get_db()
    cur = conn.cursor()

    try:
        hold_reason = None

        # =========================
        # 🚫 FILTER LOGIC
        # =========================
        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        # =========================
        # 🚫 PREVENT STACKING (MAX 2)
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT COUNT(*) FROM bot_trades
                WHERE symbol = %s AND status = 'OPEN'
            """, (symbol,))
            open_count = cur.fetchone()[0]

            if open_count >= 2:
                hold_reason = "trade_exists"

        # =========================
        # 🧠 SAVE SIGNAL
        # =========================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol,
                decision_model,
                momentum_strength,
                trend_strength,
                trend_alignment,
                price,
                data_version,
                hold_reason,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol,
            decision,
            momentum,
            trend,
            alignment,
            price,
            data_version,
            hold_reason
        ))

        # =========================
        # ✅ EXECUTE TRADE (WITH TP/SL)
        # =========================
        if hold_reason is None:

            if decision == "LONG":
                stop_price = price * (1 - 0.004)
                target_price = price * (1 + 0.008)

            elif decision == "SHORT":
                stop_price = price * (1 + 0.004)
                target_price = price * (1 - 0.008)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol,
                    direction,
                    entry_price,
                    stop_price,
                    target_price,
                    status,
                    data_version,
                    opened_at
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW())
            """, (
                symbol,
                decision,
                price,
                stop_price,
                target_price,
                data_version
            ))

            print(f"🚀 TRADE OPENED: {symbol} {decision} @ {price} | TP: {target_price} | SL: {stop_price}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "processed"}), 200


# =========================
# 🔥 SYSTEM SNAPSHOT + HEALTH
# =========================
@app.route("/system_snapshot", methods=["GET"])
def system_snapshot():
    conn = get_db()
    cur = conn.cursor()

    # ===== MASTER =====
    cur.execute("""
        SELECT
            symbol,
            COUNT(*) AS trades,
            ROUND(AVG(pnl_percent), 3) AS avg_pnl,
            ROUND(SUM(pnl_percent), 3) AS total_pnl,
            ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate
        FROM bot_trades
        WHERE status = 'CLOSED'
        GROUP BY symbol
        ORDER BY total_pnl DESC;
    """)
    master = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # ===== OPEN =====
    cur.execute("""
        SELECT symbol, direction, entry_price, stop_price, target_price, opened_at
        FROM bot_trades
        WHERE status = 'OPEN'
        ORDER BY opened_at DESC;
    """)
    open_trades = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # ===== RECENT =====
    cur.execute("""
        SELECT symbol, pnl_percent, opened_at, closed_at
        FROM bot_trades
        WHERE status = 'CLOSED'
        ORDER BY closed_at DESC
        LIMIT 50;
    """)
    recent = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # ===== HEALTH =====
    cur.execute("SELECT MAX(created_at) FROM signal_history_v2")
    last_signal = cur.fetchone()[0]

    cur.execute("SELECT MAX(opened_at) FROM bot_trades")
    last_trade = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM signal_history_v2
        WHERE created_at > NOW() - INTERVAL '1 hour'
    """)
    signals_1h = cur.fetchone()[0]

    cur.execute("""
        SELECT COUNT(*) FROM bot_trades
        WHERE opened_at > NOW() - INTERVAL '1 hour'
    """)
    trades_1h = cur.fetchone()[0]

    alert = None
    if signals_1h > 50 and trades_1h == 0:
        alert = "⚠️ SIGNALS ACTIVE BUT NO TRADES → CHECK EXECUTION"

    health = {
        "last_signal_time": str(last_signal),
        "last_trade_time": str(last_trade),
        "signals_last_hour": signals_1h,
        "trades_last_hour": trades_1h,
        "alert": alert
    }

    cur.close()
    conn.close()

    return jsonify({
        "health": health,
        "master": master,
        "open_trades": open_trades,
        "recent_trades": recent
    })


# =========================
# HEALTH CHECK
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀"
