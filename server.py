# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.3
# DEPLOYED: 2026-04-12
# NOTES:
# - Stacking toggle added (OFF by default)
# - Exit engine v2 preserved
# - Full system integrity maintained
# =========================

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

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
ENABLE_STACKING = False  # 🔥 NEW


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
# 🚀 WEBHOOK (FULL SYSTEM)
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
        if decision:
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
                if all(r[0] < 0 for r in recent if r[0] is not None):
                    last_time = recent[0][1]
                    mins = (now - last_time).total_seconds() / 60
                    if mins < COOLDOWN_MINUTES:
                        hold_reason = "cooldown_active"

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
        # 🔒 GLOBAL LIMIT
        # =========================
        if hold_reason is None:
            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            if cur.fetchone()[0] >= MAX_OPEN_TRADES:
                hold_reason = "max_open_trades"

        # =========================
        # 🔁 DUPLICATE / STACKING CONTROL
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT entry_price, opened_at
                FROM bot_trades
                WHERE symbol=%s AND status='OPEN'
                ORDER BY opened_at ASC
            """, (symbol,))
            existing = cur.fetchall()

            if ENABLE_STACKING:
                # 🟡 OLD BEHAVIOUR (max 2)
                if len(existing) >= 2:
                    hold_reason = "too_many_positions"

                elif len(existing) == 1:
                    first_price, first_time = existing[0]

                    if now - first_time < timedelta(minutes=20):
                        hold_reason = "too_soon"

                    elif (decision == "LONG" and price >= first_price) or \
                         (decision == "SHORT" and price <= first_price):
                        hold_reason = "no_better_price"

            else:
                # 🔴 STRICT MODE (1 trade only)
                if len(existing) >= 1:
                    hold_reason = "stacking_disabled"

        # =========================
        # 🧠 REGIME FILTER
        # =========================
        if hold_reason is None and ENABLE_REGIME_FILTER:
            if 0.2 < abs_mom < 0.45 and abs_trend > 0.25:
                hold_reason = "bad_regime"

        # =========================
        # 💾 SAVE SIGNAL
        # =========================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason
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

            print(f"🚀 OPEN: {symbol}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        # =========================
        # 🧠 EXIT ENGINE (v2)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at
            FROM bot_trades
            WHERE status='OPEN'
        """)
        open_trades = cur.fetchall()

        for tid, sym, direction, entry_price, opened_at in open_trades:

            if sym != symbol:
                continue

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - price) / entry_price)

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if pnl < -0.004:
                close_reason = "hard_stop"

            elif pnl > 0.004 and mins < 10:
                close_reason = "quick_profit"

            elif pnl > 0 and abs_mom < 0.1:
                close_reason = "momentum_drop"

            elif pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"

            elif mins > 20 and abs(pnl) < 0.001:
                close_reason = "no_follow_through"

            elif ENABLE_REGIME_FILTER and abs_trend < 0.15:
                close_reason = "regime_exit"

            elif mins > 60:
                close_reason = "time_cut"

            if close_reason:
                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        pnl_percent=%s,
                        close_reason=%s
                    WHERE id=%s
                """, (pnl * 100, close_reason, tid))

                print(f"💰 CLOSED: {sym} | {close_reason} | {round(pnl*100,3)}%")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
