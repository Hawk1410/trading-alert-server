# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.30
# NOTES:
# - ✅ ADDED no_progress_exit (mid-trade filter)
# - ✅ STORES global_regime + global_avg_peak at entry
# - ✅ STORES leakage_percent at close
# - ✅ ALL OTHER LOGIC UNCHANGED FROM v3.29
# =========================

print("🔥🔥🔥 MAIN.PY v3.30 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# ⚙️ CONFIG
# =========================
MAX_OPEN_TRADES = 7
MIN_TREND = 0.10
MIN_MOM = 0.05
TRADE_SIZE_GBP = 100

ENABLE_GIVEBACK_EXIT = True
ENABLE_MOMENTUM_FILTER = True
ENABLE_SHADOW_TRADES = True
ENABLE_CHOP_MODE = True

ENABLE_SAFETY_TIMEOUT = True
MAX_TRADE_DURATION_MIN = 90

ENABLE_LONGS = True
LONG_MODE = "EXTREME_ONLY"
LONGS_SHADOW_ONLY = True

ENABLE_EARLY_FAIL = True

ENABLE_TREND_HOLD = True
MIN_HOLD_TRENDING = 10

ENABLE_TREND_MOM_EXIT = True
TREND_MOM_EXIT_THRESHOLD = 0.15

# 🧠 NEW: MID-TRADE FILTER
ENABLE_NO_PROGRESS_EXIT = True
NO_PROGRESS_TIME_MIN = 20
NO_PROGRESS_PEAK_THRESHOLD = 0.15

# 🧠 REGIME SYSTEM
ENABLE_GLOBAL_REGIME = True
REGIME_LOOKBACK_TRADES = 30
BAD_MARKET_THRESHOLD = 0.20
GOOD_MARKET_THRESHOLD = 0.30

DATA_VERSION = "v3.30"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# 🧠 GLOBAL REGIME DETECTOR
# =========================
def get_global_regime(cur):
    if not ENABLE_GLOBAL_REGIME:
        return "NEUTRAL", 0.0

    cur.execute(f"""
        SELECT AVG(peak_pnl_percent)
        FROM (
            SELECT peak_pnl_percent
            FROM bot_trades
            WHERE status = 'CLOSED'
            AND is_shadow = FALSE
            ORDER BY closed_at DESC
            LIMIT {REGIME_LOOKBACK_TRADES}
        ) sub
    """)

    result = cur.fetchone()
    avg_peak = float(result[0]) if result and result[0] else 0.0

    if avg_peak < BAD_MARKET_THRESHOLD:
        return "BAD", avg_peak
    elif avg_peak > GOOD_MARKET_THRESHOLD:
        return "GOOD", avg_peak
    else:
        return "NEUTRAL", avg_peak

# =========================
# 🧠 CLASSIFIERS
# =========================
def classify_trade(momentum, trend):
    abs_mom = abs(momentum)
    abs_trend = abs(trend)

    if abs_mom >= 0.8 and abs_trend >= 0.25:
        return "A", "A+"
    if abs_mom >= 0.6 and abs_trend >= 0.2:
        return "A", "A"
    if abs_mom >= 0.3 and abs_trend >= 0.1:
        return "B", "B"

    return "C", "C"

def classify_regime(abs_trend):
    if abs_trend >= 0.25:
        return "TRENDING"
    elif abs_trend >= 0.15:
        return "TRANSITION"
    else:
        return "CHOP"

# =========================
# 🚀 WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print(f"\n📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)

        symbol = data.get("symbol")
        price = float(data.get("price", 0))
        decision = data.get("decision_model") or data.get("decision")

        momentum = float(data.get("momentum_strength") or 0)
        trend = float(data.get("trend_strength") or 0)
        alignment = data.get("trend_alignment")

        if symbol:
            PRICE_CACHE[symbol] = price

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        tier, subtier = classify_trade(momentum, trend)
        regime = classify_regime(abs_trend)

        if abs_mom < 0.5:
            mom_band = "LOW"
        elif abs_mom < 1.0:
            mom_band = "MID"
        elif abs_mom < 2.0:
            mom_band = "HIGH"
        else:
            mom_band = "EXTREME"

        conn = get_db()
        cur = conn.cursor()

        # 🌍 GLOBAL REGIME
        global_regime, global_avg_peak = get_global_regime(cur)

        SHADOW_ONLY_MODE = global_regime == "BAD"

        print(f"🌍 GLOBAL: {global_regime} | avg_peak={round(global_avg_peak,4)}", flush=True)

        # =========================
        # ENTRY LOGIC (UNCHANGED)
        # =========================
        force_shadow = False
        hold_reason = None

        if SHADOW_ONLY_MODE:
            force_shadow = True
            hold_reason = "global_bad_market"

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        action = "OPEN" if hold_reason is None else "BLOCKED"

        # =========================
        # ENTRY EXECUTION (UPDATED)
        # =========================
        if action == "OPEN" and not force_shadow:

            cur.execute("""
                SELECT COUNT(*) FROM bot_trades
                WHERE status='OPEN' AND is_shadow = FALSE
            """)
            total_open = cur.fetchone()[0]

            if total_open < MAX_OPEN_TRADES:

                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        entry_momentum, entry_trend,
                        momentum_strength, trend_strength,
                        regime, mom_band,
                        global_regime, global_avg_peak,
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,NULL,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    momentum, trend,
                    regime, mom_band,
                    global_regime, global_avg_peak
                ))

        else:
            if ENABLE_SHADOW_TRADES:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        entry_momentum, entry_trend,
                        momentum_strength, trend_strength,
                        regime, mom_band,
                        global_regime, global_avg_peak,
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    momentum, trend,
                    regime, mom_band,
                    global_regime, global_avg_peak,
                    hold_reason
                ))

        # =========================
        # EXIT ENGINE (UPDATED)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow, regime
            FROM bot_trades
            WHERE status='OPEN'
        """)

        for row in cur.fetchall():
            tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, trade_regime = row

            trade_price = PRICE_CACHE.get(sym)
            if not trade_price:
                continue

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" else ((entry_price - trade_price) / entry_price)
            pnl_percent = pnl * 100

            if pnl_percent > (peak_pnl or 0):
                cur.execute("UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s", (pnl_percent, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if ENABLE_NO_PROGRESS_EXIT:
                if mins > NO_PROGRESS_TIME_MIN and (peak_pnl or 0) < NO_PROGRESS_PEAK_THRESHOLD:
                    close_reason = "no_progress_exit"

            elif pnl < -0.004:
                close_reason = "hard_stop"

            if close_reason:
                leakage = (peak_pnl or 0) - pnl_percent

                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        close_price=%s,
                        pnl_percent=%s,
                        pnl_gbp=%s,
                        leakage_percent=%s,
                        trade_size_gbp=%s,
                        close_reason=%s
                    WHERE id=%s
                """, (
                    trade_price,
                    pnl_percent,
                    (pnl_percent / 100) * TRADE_SIZE_GBP,
                    leakage,
                    TRADE_SIZE_GBP,
                    close_reason,
                    tid
                ))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
