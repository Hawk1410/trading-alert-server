# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.27.0
# NOTES:
# - ✅ ENABLE CHOP TRADES WITHOUT DECISION
# - ✅ MOMENTUM-DERIVED DIRECTION IN CHOP
# - ✅ ALL OTHER LOGIC IDENTICAL TO v3.26.2
# =========================

print("🔥🔥🔥 MAIN.PY v3.27.0 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# ⚙️ CONFIG (UNCHANGED)
# =========================
MAX_OPEN_TRADES = 7
MIN_TREND = 0.10
MIN_MOM = 0.05
TRADE_SIZE_GBP = 100

ENABLE_GIVEBACK_EXIT = True
PROTECT_PROFIT_THRESHOLD = 0.2
GIVEBACK_RATIO = 0.5

ENABLE_MOMENTUM_FILTER = True
ENABLE_SHADOW_TRADES = True
ENABLE_CHOP_MODE = True

ENABLE_SAFETY_TIMEOUT = True
MAX_TRADE_DURATION_MIN = 90

ENABLE_LONGS = True
LONG_MODE = "EXTREME_ONLY"
LONGS_SHADOW_ONLY = True

ENABLE_EARLY_FAIL = True
EARLY_FAIL_MINUTES = 5
EARLY_FAIL_THRESHOLD = -0.001

ENABLE_TREND_HOLD = True
MIN_HOLD_TRENDING = 10

ENABLE_TREND_MOM_EXIT = True
TREND_MOM_EXIT_THRESHOLD = 0.15

DATA_VERSION = "v3.27.0"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# 🧠 CLASSIFIERS (UNCHANGED)
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

        print(f"📊 {symbol} | {decision} | {regime} | {mom_band} | {alignment}", flush=True)

        # =========================
        # ENTRY LOGIC (UPDATED)
        # =========================
        force_shadow = False
        hold_reason = None

        # 🔥 ONLY CHANGE — CHOP DECISION OVERRIDE
        if regime == "CHOP" and decision is None:
            if momentum > 0:
                decision = "LONG"
            elif momentum < 0:
                decision = "SHORT"
            else:
                hold_reason = "no_momentum"

        if decision not in ["LONG", "SHORT"] and hold_reason is None:
            hold_reason = "no_decision"

        elif decision == "LONG":
            if not ENABLE_LONGS:
                hold_reason = "longs_disabled"
            elif LONG_MODE == "EXTREME_ONLY":
                if not (regime == "TRENDING" and mom_band == "EXTREME"):
                    hold_reason = "long_not_extreme_trending"
                else:
                    if LONGS_SHADOW_ONLY:
                        hold_reason = "long_shadow_validation"
                        force_shadow = True

        elif decision == "SHORT":
            pass

        if hold_reason is None:
            if regime == "TRANSITION":
                hold_reason = "transition_shadow_only"
                force_shadow = True

            elif regime == "CHOP":
                if not ENABLE_CHOP_MODE:
                    hold_reason = "chop_disabled"
                elif mom_band != "LOW":
                    hold_reason = "chop_momentum_block"
                elif alignment != "aligned":
                    hold_reason = "chop_alignment_block"

            elif abs_trend < MIN_TREND:
                hold_reason = "not_strong_trend"

            elif abs_mom < MIN_MOM:
                hold_reason = "momentum_too_weak"

            elif ENABLE_MOMENTUM_FILTER and mom_band == "HIGH":
                hold_reason = "filtered_high_momentum"

            elif tier != "A":
                hold_reason = "low_quality"

        action = "OPEN" if hold_reason is None else "BLOCKED"

        conn = get_db()
        cur = conn.cursor()

        # =========================
        # ENTRY (UNCHANGED)
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
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,FALSE,NULL,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    momentum, trend,
                    regime, mom_band
                ))

                print(f"🚀 OPEN | {symbol}", flush=True)

        else:
            if ENABLE_SHADOW_TRADES:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        entry_momentum, entry_trend,
                        momentum_strength, trend_strength,
                        regime, mom_band,
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,TRUE,%s,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    momentum, trend,
                    regime, mom_band,
                    hold_reason
                ))

        # =========================
        # EXIT ENGINE (IDENTICAL TO v3.26.2)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow, regime
            FROM bot_trades
            WHERE status='OPEN'
        """)

        open_trades = cur.fetchall()

        for row in open_trades:
            tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, trade_regime = row

            trade_price = PRICE_CACHE.get(sym)
            if not trade_price:
                continue

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - trade_price) / entry_price)

            pnl_percent = pnl * 100

            if pnl_percent > (peak_pnl or 0):
                cur.execute(
                    "UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s",
                    (pnl_percent, tid)
                )

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if ENABLE_SAFETY_TIMEOUT and mins > MAX_TRADE_DURATION_MIN:
                close_reason = "safety_timeout"

            elif ENABLE_EARLY_FAIL and mins > EARLY_FAIL_MINUTES and pnl < EARLY_FAIL_THRESHOLD:
                close_reason = "early_fail"

            elif peak_pnl is not None and peak_pnl < 3 and mins > 15:
                close_reason = "no_follow_through"

            elif ENABLE_TREND_HOLD and trade_regime == "TRENDING" and mins < MIN_HOLD_TRENDING:
                close_reason = None

            elif trade_regime != "TRENDING" and ENABLE_GIVEBACK_EXIT and peak_pnl:

                if peak_pnl >= 50:
                    giveback_limit = 0.25
                elif peak_pnl >= 20:
                    giveback_limit = 0.4
                else:
                    giveback_limit = 0.6

                if pnl_percent < peak_pnl * (1 - giveback_limit):
                    close_reason = "giveback_exit"

            elif pnl < -0.004:
                close_reason = "hard_stop"

            elif trade_regime != "TRENDING" and pnl > 0.003 and mins < 15:
                close_reason = "quick_profit"

            elif ENABLE_TREND_MOM_EXIT and trade_regime == "TRENDING" and pnl > 0 and abs_mom < TREND_MOM_EXIT_THRESHOLD:
                close_reason = "trend_exhaustion"

            elif trade_regime != "TRENDING" and pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"

            elif trade_regime != "TRENDING" and mins > 10 and pnl <= 0:
                close_reason = "time_fail_fast"

            elif mins > 60:
                close_reason = "time_cut"

            if close_reason:
                pnl_gbp = (pnl_percent / 100) * TRADE_SIZE_GBP

                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        close_price=%s,
                        pnl_percent=%s,
                        pnl_gbp=%s,
                        trade_size_gbp=%s,
                        close_reason=%s,
                        exit_momentum=%s,
                        exit_trend=%s
                    WHERE id=%s
                """, (
                    trade_price, pnl_percent, pnl_gbp, TRADE_SIZE_GBP,
                    close_reason,
                    momentum, trend,
                    tid
                ))

                tag = "👻" if is_shadow else "💰"
                print(f"{tag} CLOSED | {sym} | {round(pnl_percent,3)}% | {close_reason}", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
