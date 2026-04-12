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
# 🧠 FULL SNAPSHOT
# =========================
@app.route("/system_snapshot_full", methods=["GET"])
def system_snapshot_full():
    try:
        conn = get_db()
        cur = conn.cursor()

        now = datetime.utcnow()

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

        cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status = 'OPEN'")
        open_trades = cur.fetchone()[0]

        exposure = open_trades * CAPITAL_PER_TRADE

        cur.execute("""
            SELECT 
                COUNT(*),
                ROUND(AVG(pnl_percent), 3),
                ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END), 3)
            FROM bot_trades
            WHERE status = 'CLOSED'
        """)
        total_trades, avg_pnl, winrate = cur.fetchone()

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

        cur.execute("""
            SELECT hold_reason, COUNT(*)
            FROM signal_history_v2
            WHERE created_at > NOW() - INTERVAL '1 hour'
            GROUP BY hold_reason
        """)
        hold_reasons = {k if k else "executed": v for k, v in cur.fetchall()}

        cur.execute("""
            SELECT signal_quality, COUNT(*)
            FROM signal_history_v2
            GROUP BY signal_quality
        """)
        quality = {k: v for k, v in cur.fetchall() if k}

        # LOSS STREAK
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
# 🚀 WEBHOOK (FIXED 🔥)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        symbol = data.get("symbol")
        decision = data.get("decision_model")
        price = float(data.get("price", 0))
        momentum = float(data.get("momentum_strength", 0))
        trend = float(data.get("trend_strength", 0))
        alignment = data.get("trend_alignment")
        data_version = data.get("data_version")

        # =========================
        # 🔥 DECISION FIX
        # =========================
        if decision is not None:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        if decision is None:
            print(f"⚠️ NO DECISION: {symbol}")

        conn = get_db()
        cur = conn.cursor()
        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        hold_reason = None

        # =========================
        # 🧠 COOLDOWN
        # =========================
        if ENABLE_COOLDOWN:
            cur.execute("""
                SELECT pnl_percent, closed_at
                FROM bot_trades
                WHERE status = 'CLOSED'
                ORDER BY closed_at DESC
                LIMIT %s
            """, (LOSS_STREAK_LIMIT,))
            recent = cur.fetchall()

            if len(recent) == LOSS_STREAK_LIMIT:
                losses = all(r[0] < 0 for r in recent if r[0] is not None)

                if losses:
                    last_closed_time = recent[0][1]
                    minutes_since = (now - last_closed_time).total_seconds() / 60

                    if minutes_since < COOLDOWN_MINUTES:
                        hold_reason = "cooldown_active"

        # =========================
        # ENTRY FILTER
        # =========================
        if hold_reason is None:

            if decision not in ["LONG", "SHORT"]:
                hold_reason = "no_decision"

            elif alignment != "aligned":
                hold_reason = "counter_trend"

            elif abs_trend < 0.15:
                hold_reason = "not_strong_trend"

            elif abs_mom < 0.15:
                hold_reason = "momentum_too_weak"

            elif abs_mom > 2.5:
                hold_reason = "extreme_momentum"

        # =========================
        # SAVE SIGNAL
        # =========================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason
        ))

        # =========================
        # OPEN TRADE
        # =========================
        if hold_reason is None:
            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    status, data_version, opened_at
                )
                VALUES (%s,%s,%s,'OPEN',%s,NOW())
            """, (
                symbol, decision, price, data_version
            ))

            print(f"🚀 TRADE OPENED: {symbol}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
