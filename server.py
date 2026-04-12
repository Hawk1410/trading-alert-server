from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta
import json

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# ⚙️ LIVE CONFIG
# =========================
MAX_OPEN_TRADES = 7
CAPITAL_PER_TRADE = 60

ENABLE_COOLDOWN = True
COOLDOWN_MINUTES = 20
LOSS_STREAK_LIMIT = 2

ENABLE_REGIME_FILTER = True


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
# 🧠 FULL SNAPSHOT (UPGRADED 🔥)
# =========================
@app.route("/system_snapshot_full", methods=["GET"])
def system_snapshot_full():
    try:
        conn = get_db()
        cur = conn.cursor()

        now = datetime.utcnow()

        # =========================
        # 🧠 BASIC HEALTH
        # =========================
        cur.execute("SELECT MAX(created_at) FROM signal_history_v2")
        last_signal = cur.fetchone()[0]

        cur.execute("SELECT MAX(opened_at) FROM bot_trades")
        last_trade = cur.fetchone()[0]

        # =========================
        # 📊 ACTIVITY
        # =========================
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

        # =========================
        # 📈 OPEN EXPOSURE
        # =========================
        cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status = 'OPEN'")
        open_trades = cur.fetchone()[0]

        exposure = open_trades * CAPITAL_PER_TRADE

        # =========================
        # 📊 PERFORMANCE
        # =========================
        cur.execute("""
            SELECT 
                COUNT(*),
                ROUND(AVG(pnl_percent), 3),
                ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END), 3)
            FROM bot_trades
            WHERE status = 'CLOSED'
        """)
        total_trades, avg_pnl, winrate = cur.fetchone()

        # =========================
        # 🔥 RECENT PERFORMANCE (1H)
        # =========================
        cur.execute("""
            SELECT 
                COUNT(*),
                ROUND(AVG(pnl_percent), 3),
                ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END), 3)
            FROM bot_trades
            WHERE status = 'CLOSED'
            AND closed_at > NOW() - INTERVAL '1 hour'
        """)
        r_trades, r_avg_pnl, r_winrate = cur.fetchone()

        # =========================
        # 🧠 HOLD REASONS (CRITICAL)
        # =========================
        cur.execute("""
            SELECT hold_reason, COUNT(*)
            FROM signal_history_v2
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY hold_reason
        """)
        hold_reasons = {k if k else "executed": v for k, v in cur.fetchall()}

        # =========================
        # 🧠 SIGNAL QUALITY
        # =========================
        cur.execute("""
            SELECT signal_quality, COUNT(*)
            FROM signal_history_v2
            GROUP BY signal_quality
        """)
        quality = {k: v for k, v in cur.fetchall() if k}

        # =========================
        # 🧠 REGIME PERFORMANCE
        # =========================
        cur.execute("""
            WITH joined AS (
                SELECT 
                    t.pnl_percent,
                    ABS(s.momentum_strength) AS mom,
                    ABS(s.trend_strength) AS trend
                FROM bot_trades t
                JOIN signal_history_v2 s 
                ON s.symbol = t.symbol
                AND s.created_at <= t.opened_at
                WHERE t.status = 'CLOSED'
            ),
            classified AS (
                SELECT *,
                    CASE 
                        WHEN mom > 0.2 AND trend > 0.2 THEN 'NORMAL'
                        ELSE 'BAD'
                    END AS regime
                FROM joined
            )
            SELECT 
                regime,
                COUNT(*),
                ROUND(AVG(pnl_percent), 3),
                ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END), 3)
            FROM classified
            GROUP BY regime
        """)
        regime_perf = {
            row[0]: {
                "trades": row[1],
                "avg_pnl": row[2],
                "winrate": row[3]
            }
            for row in cur.fetchall()
        }

        # =========================
        # 🧠 LOSS STREAK (LIVE)
        # =========================
        cur.execute("""
            SELECT pnl_percent
            FROM bot_trades
            WHERE status = 'CLOSED'
            ORDER BY closed_at DESC
            LIMIT 10
        """)
        streak_data = cur.fetchall()

        loss_streak = 0
        for r in streak_data:
            if r[0] is not None and r[0] < 0:
                loss_streak += 1
            else:
                break

        cooldown_active = False
        if ENABLE_COOLDOWN and loss_streak >= LOSS_STREAK_LIMIT:
            cooldown_active = True

        # =========================
        # 📦 FINAL JSON
        # =========================
        result = {
            "health": {
                "last_signal": str(last_signal) if last_signal else None,
                "last_trade": str(last_trade) if last_trade else None
            },
            "activity": {
                "signals_1h": signals_1h,
                "trades_1h": trades_1h
            },
            "exposure": {
                "open_trades": open_trades,
                "capital_exposed": exposure
            },
            "performance": {
                "total_trades": total_trades,
                "avg_pnl": avg_pnl,
                "winrate": winrate
            },
            "recent_performance": {
                "trades_1h": r_trades,
                "avg_pnl": r_avg_pnl,
                "winrate": r_winrate
            },
            "regime_performance": regime_perf,
            "signal_quality": quality,
            "hold_reasons_1h": hold_reasons,
            "risk_state": {
                "loss_streak": loss_streak,
                "cooldown_active": cooldown_active
            }
        }

        cur.close()
        conn.close()

        return jsonify(result), 200

    except Exception as e:
        print("❌ SNAPSHOT FULL ERROR:", e)
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (UNCHANGED)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    # (KEEP YOUR EXISTING WEBHOOK EXACTLY AS IS)
    return jsonify({"status": "use previous version"}), 200
