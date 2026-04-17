# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.10.1
# DEPLOYED: 2026-04-17
# NOTES:
# - Added tier system (A / B / C + A+)
# - Disabled coin filter
# - Added rolling debug signal buffer
# - Added /debug_signals endpoint
# - JSON output improved for fast analysis
# =========================

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# 🧠 DEBUG BUFFER
# =========================
DEBUG_SIGNALS = []
MAX_DEBUG_SIGNALS = 50


def add_debug_signal(signal):
    DEBUG_SIGNALS.append(signal)
    if len(DEBUG_SIGNALS) > MAX_DEBUG_SIGNALS:
        DEBUG_SIGNALS.pop(0)


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

ENABLE_STACKING = False
ENABLE_REGIME_FILTER = False

ENABLE_MOMENTUM_CAP = False  # OFF
ENABLE_ADAPTIVE_COIN_FILTER = False  # ❌ DISABLED

MIN_TREND = 0.20
MIN_MOM = 0.05


def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🧠 TIER CLASSIFICATION
# =========================
def classify_trade(momentum, trend):
    abs_mom = abs(momentum)
    abs_trend = abs(trend)

    # A+
    if abs_mom >= 0.8 and abs_trend >= 0.25:
        return "A", "A+"

    # A
    if abs_mom >= 0.6 and abs_trend >= 0.2:
        return "A", "A"

    # B
    if abs_mom >= 0.3 and abs_trend >= 0.1:
        return "B", "B"

    # C
    return "C", "C"


@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200


@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


# =========================
# 🧠 DEBUG ENDPOINT
# =========================
@app.route("/debug_signals", methods=["GET"])
def debug_signals():
    return jsonify({
        "count": len(DEBUG_SIGNALS),
        "signals": DEBUG_SIGNALS
    })


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

        # =========================
        # 🧠 TIER LOGIC
        # =========================
        tier, subtier = classify_trade(momentum, trend)

        hold_reason = None

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

        # =========================
        # 🧠 TIER FILTER (CORE CHANGE)
        # =========================
        if hold_reason is None:
            if tier != "A":
                hold_reason = "low_quality"

        # =========================
        # 🔒 GLOBAL LIMIT
        # =========================
        if hold_reason is None:
            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            if cur.fetchone()[0] >= MAX_OPEN_TRADES:
                hold_reason = "max_open_trades"

        # =========================
        # 🔁 STACKING
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT id FROM bot_trades
                WHERE symbol=%s AND status='OPEN'
            """, (symbol,))
            if cur.fetchone():
                hold_reason = "stacking_disabled"

        # =========================
        # 🚀 OPEN TRADE
        # =========================
        action = "BLOCKED"

        if hold_reason is None:
            action = "OPEN"

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

            print(f"🚀 OPEN: {symbol} | {subtier}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason} | {subtier}")

        # =========================
        # 🧠 DEBUG JSON
        # =========================
        debug_payload = {
            "time": now.isoformat(),
            "symbol": symbol,
            "decision": decision,
            "price": price,
            "momentum": momentum,
            "trend": trend,
            "tier": tier,
            "subtier": subtier,
            "hold_reason": hold_reason,
            "action": action
        }

        add_debug_signal(debug_payload)

        # =========================
        # 🧠 EXIT ENGINE (UNCHANGED)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent
            FROM bot_trades
            WHERE status='OPEN'
        """)
        open_trades = cur.fetchall()

        for tid, sym, direction, entry_price, opened_at, peak_pnl in open_trades:

            if sym != symbol:
                continue

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - price) / entry_price)

            if pnl * 100 > (peak_pnl or 0):
                cur.execute("""
                    UPDATE bot_trades
                    SET peak_pnl_percent = %s
                    WHERE id = %s
                """, (pnl * 100, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if pnl < -0.004:
                close_reason = "hard_stop"

            elif pnl > 0.004 and mins < 10:
                close_reason = "quick_profit"

            elif pnl > 0 and abs(momentum) < 0.1:
                close_reason = "momentum_drop"

            elif pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"

            elif mins > 20 and abs(pnl) < 0.001:
                close_reason = "no_follow_through"

            elif mins > 60:
                close_reason = "time_cut"

            if close_reason:
                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        pnl_percent=%s,
                        close_reason=%s,
                        exit_momentum=%s,
                        exit_trend=%s
                    WHERE id=%s
                """, (pnl * 100, close_reason, momentum, trend, tid))

                print(f"💰 CLOSED: {sym} | {close_reason} | {round(pnl*100,3)}%")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify(debug_payload), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
