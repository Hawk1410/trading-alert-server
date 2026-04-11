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
    conn = get_db()
    cur = conn.cursor()

    try:
        # -------------------------
        # HEALTH
        # -------------------------
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

        # -------------------------
        # MASTER PERFORMANCE
        # -------------------------
        cur.execute("""
            SELECT
                symbol,
                COUNT(*) trades,
                ROUND(AVG(pnl_percent), 3) avg_pnl,
                ROUND(SUM(pnl_percent), 3) total_pnl,
                ROUND(AVG((pnl_percent > 0)::int)::numeric, 3) winrate
            FROM bot_trades
            WHERE status = 'CLOSED'
            GROUP BY symbol
            ORDER BY total_pnl DESC
        """)
        master = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # -------------------------
        # TIER PERFORMANCE (CLEAN)
        # -------------------------
        cur.execute("""
            SELECT
                COALESCE(sh.signal_quality, 'UNKNOWN') signal_quality,
                COUNT(*) trades,
                ROUND(AVG(bt.pnl_percent), 3) avg_pnl,
                ROUND(SUM(bt.pnl_percent), 3) total_pnl,
                ROUND(AVG((bt.pnl_percent > 0)::int)::numeric, 3) winrate
            FROM bot_trades bt
            JOIN signal_history_v2 sh
              ON bt.symbol = sh.symbol
             AND ABS(EXTRACT(EPOCH FROM (bt.opened_at - sh.created_at))) < 60
            WHERE bt.status = 'CLOSED'
            GROUP BY 1
            ORDER BY total_pnl DESC
        """)
        tier = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # -------------------------
        # HOLD PERFORMANCE (🔥 KEY)
        # -------------------------
        cur.execute("""
            SELECT
                hold_reason,
                COUNT(*) count
            FROM signal_history_v2
            WHERE hold_reason IS NOT NULL
            GROUP BY hold_reason
            ORDER BY count DESC
        """)
        holds = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # -------------------------
        # RECENT PNL (SYSTEM HEALTH)
        # -------------------------
        cur.execute("""
            SELECT
                ROUND(SUM(pnl_percent), 3)
            FROM bot_trades
            WHERE status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL '1 hour'
        """)
        pnl_1h = cur.fetchone()[0] or 0

        cur.execute("""
            SELECT
                ROUND(SUM(pnl_percent), 3)
            FROM bot_trades
            WHERE status = 'CLOSED'
              AND closed_at > NOW() - INTERVAL '4 hours'
        """)
        pnl_4h = cur.fetchone()[0] or 0

        # -------------------------
        # TRADE DURATION
        # -------------------------
        cur.execute("""
            SELECT
                ROUND(AVG(EXTRACT(EPOCH FROM (closed_at - opened_at))/60),2)
            FROM bot_trades
            WHERE status = 'CLOSED'
        """)
        avg_duration = cur.fetchone()[0]

        # -------------------------
        # OPEN TRADES
        # -------------------------
        cur.execute("""
            SELECT symbol, direction, entry_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)
        open_trades = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        # -------------------------
        # RECENT TRADES
        # -------------------------
        cur.execute("""
            SELECT symbol, pnl_percent, opened_at, closed_at
            FROM bot_trades
            WHERE status = 'CLOSED'
            ORDER BY closed_at DESC
            LIMIT 20
        """)
        recent = [dict(zip([d[0] for d in cur.description], r)) for r in cur.fetchall()]

        cur.close()
        conn.close()

        return jsonify({
            "health": {
                "last_signal": str(last_signal),
                "last_trade": str(last_trade),
                "signals_1h": signals_1h,
                "trades_1h": trades_1h,
                "pnl_1h": pnl_1h,
                "pnl_4h": pnl_4h,
                "avg_trade_duration_min": avg_duration
            },
            "master_performance": master,
            "tier_performance": tier,
            "hold_distribution": holds,
            "open_trades": open_trades,
            "recent_trades": recent
        })

    except Exception as e:
        print("❌ SNAPSHOT ERROR:", e)
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (UNCHANGED CORE LOGIC)
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

        trend_quality = "strong" if abs_trend > 0.25 else "medium" if abs_trend > 0.12 else "weak"
        momentum_state = "impulse" if abs_mom > 0.45 else "build" if abs_mom > 0.2 else "weak"

        score = (
            (2 if abs_mom > 0.45 else 1 if abs_mom > 0.2 else 0) +
            (2 if abs_trend > 0.25 else 1 if abs_trend > 0.12 else 0) +
            (1 if alignment == "aligned" else 0)
        )

        signal_quality = "A+" if score >= 5 else "B" if score >= 3 else "C"

        # EXIT ENGINE (same)
        cur.execute("SELECT id, symbol, direction, entry_price, stop_price, target_price, opened_at FROM bot_trades WHERE status='OPEN'")
        for trade_id, t_symbol, direction, entry, stop, target, opened in cur.fetchall():
            if t_symbol != symbol:
                continue

            pnl = ((price - entry) / entry * 100) if direction == "LONG" else ((entry - price) / entry * 100)
            duration = (now - opened).total_seconds()

            close = False

            if (direction == "LONG" and (price >= target or price <= stop)) or \
               (direction == "SHORT" and (price <= target or price >= stop)):
                close = True

            if not close and duration > 21600 and pnl < 0.2:
                close = True

            if not close and duration > 43200:
                close = True

            if close:
                cur.execute("UPDATE bot_trades SET status='CLOSED', closed_at=NOW(), pnl_percent=%s WHERE id=%s", (pnl, trade_id))

        # ENTRY FILTERS
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"
        elif alignment != "aligned":
            hold_reason = "counter_trend"
        elif abs_trend < 0.12:
            hold_reason = "weak_trend"

        if hold_reason is None and signal_quality == "B":
            if abs_trend < 0.25:
                hold_reason = "weak_B_trend"
            elif abs_mom < 0.35:
                hold_reason = "weak_B_momentum"

        # EXISTING TRADE CHECK
        if hold_reason is None:
            cur.execute("SELECT entry_price, opened_at FROM bot_trades WHERE symbol=%s AND status='OPEN'", (symbol,))
            existing = cur.fetchall()

            if len(existing) >= 2:
                hold_reason = "trade_exists"
            elif len(existing) == 1:
                entry_price, opened_at = existing[0]
                if now - opened_at < timedelta(minutes=20):
                    hold_reason = "too_soon"

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

        # EXECUTE
        if hold_reason is None:
            stop_price = price * (0.996 if decision == "LONG" else 1.004)
            target_price = price * (1.008 if decision == "LONG" else 0.992)

            cur.execute("""
                INSERT INTO bot_trades (symbol, direction, entry_price, stop_price, target_price, status, data_version, opened_at)
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW())
            """, (symbol, decision, price, stop_price, target_price, data_version))

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "processed"}), 200
