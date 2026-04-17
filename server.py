# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.10
# DEPLOYED: 2026-04-17
# NOTES:
# - Removed coin filter (disabled)
# - Added A/B/C + A+ tier system
# - Hard filter: ONLY A trades allowed
# - Added trade_quality + trade_subtier tracking
# - Restored JSON debug output
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

ENABLE_MARKET_FILTER = True
MARKET_WINDOW_MINUTES = 15
MARKET_MIN_TRADES = 3
MARKET_MAX_WINRATE = 0.34
MARKET_MAX_AVG_PNL = -0.1
MARKET_COOLDOWN_MINUTES = 20

ENABLE_REGIME_FILTER = False
ENABLE_STACKING = False

ENABLE_EARLY_TREND = False
ENABLE_MOMENTUM_CAP = False
ENABLE_SWEET_SPOT = False
ENABLE_SMART_STACKING = False

# ❌ DISABLED
ENABLE_ADAPTIVE_COIN_FILTER = False

MIN_TREND = 0.20
MIN_MOM = 0.05

def get_db():
    return psycopg2.connect(DATABASE_URL)

@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200

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

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        conn = get_db()
        cur = conn.cursor()
        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        hold_reason = None

        # =========================
        # 🧠 TIER SYSTEM
        # =========================
        if abs_mom >= 1.0 and abs_trend >= 0.3:
            trade_subtier = "A+"
            trade_quality = "A"
        elif abs_mom >= 0.6 and abs_trend >= 0.2:
            trade_subtier = "A"
            trade_quality = "A"
        elif abs_mom >= 0.3 and abs_trend >= 0.15:
            trade_subtier = "B"
            trade_quality = "B"
        else:
            trade_subtier = "C"
            trade_quality = "C"

        # =========================
        # 🧠 MARKET FILTER
        # =========================
        if ENABLE_MARKET_FILTER:
            window_start = now - timedelta(minutes=MARKET_WINDOW_MINUTES)

            cur.execute("""
                SELECT 
                    COUNT(*),
                    AVG(pnl_percent),
                    AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END)
                FROM bot_trades
                WHERE status = 'CLOSED'
                AND closed_at >= %s
            """, (window_start,))

            count, avg_pnl, winrate = cur.fetchone()

            if count and count >= MARKET_MIN_TRADES:
                if (winrate is not None and winrate <= MARKET_MAX_WINRATE) or \
                   (avg_pnl is not None and avg_pnl <= MARKET_MAX_AVG_PNL):

                    cur.execute("""
                        SELECT MAX(closed_at)
                        FROM bot_trades
                        WHERE status = 'CLOSED'
                        AND closed_at >= %s
                    """, (window_start,))
                    last_event = cur.fetchone()[0]

                    if last_event:
                        mins = (now - last_event).total_seconds() / 60
                        if mins < MARKET_COOLDOWN_MINUTES:
                            hold_reason = "market_danger"

        # =========================
        # 🧠 COOLDOWN
        # =========================
        if hold_reason is None and ENABLE_COOLDOWN:
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

            elif abs_trend < MIN_TREND:
                hold_reason = "not_strong_trend"

            elif abs_mom < MIN_MOM:
                hold_reason = "momentum_too_weak"

            elif abs_mom > 2.5:
                hold_reason = "extreme_momentum"

        # =========================
        # 🚫 HARD QUALITY FILTER
        # =========================
        if hold_reason is None:
            if trade_quality != "A":
                hold_reason = "low_quality"

        # =========================
        # 🔒 GLOBAL LIMIT
        # =========================
        if hold_reason is None:
            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            if cur.fetchone()[0] >= MAX_OPEN_TRADES:
                hold_reason = "max_open_trades"

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
        action = "BLOCKED"

        if hold_reason is None:
            stop_price = price * (0.996 if decision == "LONG" else 1.004)
            target_price = price * (1.008 if decision == "LONG" else 0.992)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    stop_price, target_price,
                    status, data_version, opened_at,
                    peak_pnl_percent,
                    entry_momentum,
                    entry_trend
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW(),0,%s,%s)
            """, (
                symbol, decision, price,
                stop_price, target_price, data_version,
                momentum, trend
            ))

            action = "OPEN"
            print(f"🚀 OPEN: {symbol}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        conn.commit()
        cur.close()
        conn.close()

        # =========================
        # 📦 DEBUG JSON OUTPUT
        # =========================
        return jsonify({
            "symbol": symbol,
            "decision": decision,
            "momentum": momentum,
            "trend": trend,
            "tier": trade_quality,
            "subtier": trade_subtier,
            "hold_reason": hold_reason,
            "action": action
        }), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
