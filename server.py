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
# 📊 LIGHT SNAPSHOT
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
# 🧠 FULL SNAPSHOT (PRO MODE)
# =========================
@app.route("/system_snapshot_full", methods=["GET"])
def system_snapshot_full():
    try:
        conn = get_db()
        cur = conn.cursor()

        # HEALTH
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

        # MASTER PERFORMANCE
        cur.execute("""
            SELECT symbol,
                   COUNT(*) trades,
                   ROUND(AVG(pnl_percent),3) avg_pnl,
                   ROUND(SUM(pnl_percent),3) total_pnl
            FROM bot_trades
            WHERE status = 'CLOSED'
            GROUP BY symbol
            ORDER BY total_pnl DESC
        """)
        master = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        # TREND BUCKET PERFORMANCE (NEW 🔥)
        cur.execute("""
            SELECT
                CASE
                    WHEN ABS(sh.trend_strength) < 0.12 THEN 'weak'
                    WHEN ABS(sh.trend_strength) < 0.25 THEN 'medium'
                    ELSE 'strong'
                END AS trend_bucket,
                COUNT(*) trades,
                ROUND(AVG(bt.pnl_percent),3) avg_pnl,
                ROUND(SUM(bt.pnl_percent),3) total_pnl
            FROM bot_trades bt
            JOIN signal_history_v2 sh
              ON bt.symbol = sh.symbol
             AND ABS(EXTRACT(EPOCH FROM (bt.opened_at - sh.created_at))) < 60
            WHERE bt.status = 'CLOSED'
            GROUP BY trend_bucket
        """)
        trend_buckets = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        # TIER PERFORMANCE
        cur.execute("""
            SELECT sh.signal_quality,
                   COUNT(*) trades,
                   ROUND(AVG(bt.pnl_percent),3) avg_pnl,
                   ROUND(SUM(bt.pnl_percent),3) total_pnl
            FROM bot_trades bt
            JOIN signal_history_v2 sh
              ON bt.symbol = sh.symbol
             AND ABS(EXTRACT(EPOCH FROM (bt.opened_at - sh.created_at))) < 60
            WHERE bt.status = 'CLOSED'
            GROUP BY sh.signal_quality
        """)
        tiers = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            "health": {
                "last_signal": str(last_signal),
                "last_trade": str(last_trade),
                "signals_1h": signals_1h,
                "trades_1h": trades_1h
            },
            "master": master,
            "trend_buckets": trend_buckets,
            "tiers": tiers
        }), 200

    except Exception as e:
        print("❌ SNAPSHOT ERROR:", e)
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (UPDATED FILTERS)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json

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

        score = (
            (2 if abs_mom > 0.45 else 1 if abs_mom > 0.2 else 0) +
            (2 if abs_trend > 0.25 else 1 if abs_trend > 0.12 else 0) +
            (1 if alignment == "aligned" else 0)
        )

        signal_quality = "A+" if score >= 5 else "B" if score >= 3 else "C"

        hold_reason = None

        # BASE FILTERS
        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"
        elif alignment != "aligned":
            hold_reason = "counter_trend"
        elif abs_trend < 0.12:
            hold_reason = "weak_trend"

        # 🔥 HARD B FILTER
        if hold_reason is None and signal_quality == "B":
            if abs_trend < 0.25:
                hold_reason = "blocked_B_weak_trend"
            elif abs_mom < 0.45:
                hold_reason = "blocked_B_weak_momentum"

        # SAVE SIGNAL
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason,
                signal_quality, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason, signal_quality
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
