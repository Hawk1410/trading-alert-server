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
CAPITAL_PER_TRADE = 60  # £60 per trade

# 🧠 COOLDOWN CONFIG
ENABLE_COOLDOWN = True
COOLDOWN_MINUTES = 20
LOSS_STREAK_LIMIT = 2


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
            SELECT signal_quality, COUNT(*)
            FROM signal_history_v2
            GROUP BY signal_quality
        """)
        quality = {k: v for k, v in cur.fetchall() if k is not None}

        cur.close()
        conn.close()

        return jsonify({
            "health": {
                "last_signal": str(last_signal) if last_signal else None,
                "last_trade": str(last_trade) if last_trade else None,
                "signals_1h": signals_1h,
                "trades_1h": trades_1h
            },
            "signal_quality_distribution": quality
        }), 200

    except Exception as e:
        print("❌ SNAPSHOT FULL ERROR:", e)
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (LIVE CONFIG)
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

        conn = get_db()
        cur = conn.cursor()
        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        # =========================
        # 🧠 COOLDOWN CHECK (NEW)
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
                        print(f"⛔ COOLDOWN ACTIVE ({LOSS_STREAK_LIMIT} losses, {round(minutes_since,1)} mins elapsed)")
                        hold_reason = "cooldown_active"
                    else:
                        hold_reason = None
                else:
                    hold_reason = None
            else:
                hold_reason = None
        else:
            hold_reason = None

        # =========================
        # 🧠 SIGNAL SCORING
        # =========================
        score = (
            (2 if abs_mom > 0.45 else 1 if abs_mom > 0.2 else 0) +
            (2 if abs_trend > 0.25 else 1 if abs_trend > 0.12 else 0) +
            (1 if alignment == "aligned" else 0)
        )

        signal_quality = "A+" if score >= 5 else "B" if score >= 3 else "C"

        # =========================
        # 🔥 ENTRY FILTER
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
        # 🔒 GLOBAL TRADE LIMIT
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT COUNT(*) FROM bot_trades WHERE status = 'OPEN'
            """)
            open_count = cur.fetchone()[0]

            if open_count >= MAX_OPEN_TRADES:
                hold_reason = "max_open_trades_reached"

        # =========================
        # 🔁 EXISTING TRADE LOGIC
        # =========================
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

        # =========================
        # 💾 SAVE SIGNAL
        # =========================
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

        # =========================
        # 🚀 OPEN TRADE
        # =========================
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

            print(f"🚀 TRADE OPENED: {symbol}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        # =========================
        # 🧠 SMART EXIT ENGINE
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)
        open_trades = cur.fetchall()

        for trade_id, sym, direction, entry_price, opened_at in open_trades:

            if sym != symbol:
                continue

            pnl = (
                (price - entry_price) / entry_price
                if direction == "LONG"
                else (entry_price - price) / entry_price
            )

            time_open = (now - opened_at).total_seconds() / 60

            close_reason = None

            if time_open > 60:
                close_reason = "time_cut"

            elif pnl > 0.005:
                close_reason = "quick_profit"

            elif pnl > 0 and abs_mom < 0.1:
                close_reason = "momentum_drop"

            elif pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"

            if close_reason:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        pnl_percent = %s,
                        close_reason = %s
                    WHERE id = %s
                """, (pnl * 100, close_reason, trade_id))

                print(f"💰 CLOSED: {sym} | {close_reason} | {round(pnl*100, 3)}%")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
