from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🏠 HEALTH ROUTES
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


# =========================
# 📊 LIGHT SNAPSHOT (SAFE)
# =========================
@app.route("/system_snapshot", methods=["GET"])
def system_snapshot():
    try:
        conn = get_db()
        cur = conn.cursor()

        cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status = 'OPEN'")
        open_trades = cur.fetchone()[0]

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

        cur.close()
        conn.close()

        return jsonify({
            "status": "ok",
            "open_trades": open_trades,
            "signals_last_hour": signals_1h,
            "trades_last_hour": trades_1h
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# 🧠 FULL SNAPSHOT (GOD MODE)
# =========================
@app.route("/system_snapshot_full", methods=["GET"])
def system_snapshot_full():
    conn = get_db()
    cur = conn.cursor()

    try:
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
            ORDER BY total_pnl DESC
        """)
        master = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.execute("""
            SELECT
                sh.signal_quality,
                COUNT(*) AS trades,
                ROUND(AVG(bt.pnl_percent), 3) AS avg_pnl,
                ROUND(SUM(bt.pnl_percent), 3) AS total_pnl,
                ROUND(AVG(CASE WHEN bt.pnl_percent > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate
            FROM bot_trades bt
            JOIN signal_history_v2 sh
              ON bt.symbol = sh.symbol
             AND ABS(EXTRACT(EPOCH FROM (bt.opened_at - sh.created_at))) < 60
            WHERE bt.status = 'CLOSED'
            GROUP BY sh.signal_quality
            ORDER BY total_pnl DESC
        """)
        tier_performance = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.execute("""
            SELECT
                ROUND(sh.momentum_strength::numeric, 2) AS momentum,
                COUNT(*) AS trades,
                ROUND(AVG(bt.pnl_percent), 3) AS avg_pnl
            FROM bot_trades bt
            JOIN signal_history_v2 sh
              ON bt.symbol = sh.symbol
             AND ABS(EXTRACT(EPOCH FROM (bt.opened_at - sh.created_at))) < 60
            WHERE bt.status = 'CLOSED'
            GROUP BY momentum
            ORDER BY trades DESC
            LIMIT 20
        """)
        momentum = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.execute("""
            SELECT hold_reason, COUNT(*) as count
            FROM signal_history_v2
            WHERE created_at > NOW() - INTERVAL '6 hours'
            AND hold_reason IS NOT NULL
            GROUP BY hold_reason
            ORDER BY count DESC
        """)
        holds = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.execute("""
            SELECT symbol, pnl_percent, opened_at, closed_at
            FROM bot_trades
            WHERE status = 'CLOSED'
            ORDER BY closed_at DESC
            LIMIT 20
        """)
        recent_trades = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.execute("""
            SELECT symbol, direction, entry_price, stop_price, target_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)
        open_trades = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            "health": {
                "last_signal_time": str(last_signal),
                "last_trade_time": str(last_trade),
                "signals_last_hour": signals_1h,
                "trades_last_hour": trades_1h
            },
            "master": master,
            "tier_performance": tier_performance,
            "momentum_performance": momentum,
            "hold_reasons": holds,
            "open_trades": open_trades,
            "recent_trades": recent_trades
        })

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (UPDATED v2.7)
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

    now = datetime.utcnow()

    try:
        abs_mom = abs(momentum) if momentum else 0
        abs_trend = abs(trend) if trend else 0

        trend_quality = "strong" if abs_trend > 0.25 else "medium" if abs_trend > 0.12 else "weak"
        momentum_state = "impulse" if abs_mom > 0.45 else "build" if abs_mom > 0.2 else "weak"

        score = (
            (2 if abs_mom > 0.45 else 1 if abs_mom > 0.2 else 0) +
            (2 if abs_trend > 0.25 else 1 if abs_trend > 0.12 else 0) +
            (1 if alignment == "aligned" else 0)
        )

        signal_quality = "A+" if score >= 5 else "B" if score >= 3 else "C"

        # EXIT ENGINE (unchanged)
        cur.execute("""
            SELECT id, symbol, direction, entry_price,
                   stop_price, target_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)

        for trade_id, t_symbol, direction, entry, stop, target, opened in cur.fetchall():
            if t_symbol != symbol:
                continue

            duration = (now - opened).total_seconds()
            pnl = ((price - entry) / entry * 100) if direction == "LONG" else ((entry - price) / entry * 100)

            close = False

            if (direction == "LONG" and (price >= target or price <= stop)) or \
               (direction == "SHORT" and (price <= target or price >= stop)):
                close = True

            if not close and duration > 21600 and pnl < 0.2:
                close = True

            if not close and duration > 43200:
                close = True

            if close:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        pnl_percent = %s
                    WHERE id = %s
                """, (pnl, trade_id))

        # ENTRY FILTER
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        # ✅ NEW FILTER (MOST IMPORTANT)
        elif abs_trend < 0.12:
            hold_reason = "weak_trend"

        if hold_reason is None:
            cur.execute("""
                SELECT entry_price, opened_at
                FROM bot_trades
                WHERE symbol = %s AND status = 'OPEN'
                ORDER BY opened_at ASC
            """, (symbol,))
            existing = cur.fetchall()

            if len(existing) >= 2:
                hold_reason = "trade_exists"
            elif len(existing) == 1:
                first_entry, first_time = existing[0]
                if now - first_time < timedelta(minutes=20):
                    hold_reason = "too_soon"
                elif (decision == "LONG" and price >= first_entry) or \
                     (decision == "SHORT" and price <= first_entry):
                    hold_reason = "no_better_price"

        # SAVE SIGNAL
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason,
                signal_quality, trend_quality, momentum_state, signal_score, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason,
            signal_quality, trend_quality, momentum_state, score
        ))

        # EXECUTE TRADE
        if hold_reason is None:
            stop_price = price * (0.996 if decision == "LONG" else 1.004)
            target_price = price * (1.008 if decision == "LONG" else 0.992)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    stop_price, target_price,
                    status, data_version, opened_at
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW())
            """, (
                symbol, decision, price,
                stop_price, target_price, data_version
            ))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "processed"}), 200
