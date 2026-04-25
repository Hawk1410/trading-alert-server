# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.17.2
# DEPLOYED: 2026-04-25
# NOTES:
# - ✅ FIXED: Shadow trades no longer count toward open trade limits
# - ✅ FIXED: Symbol duplicate check ignores shadow trades
# - ✅ No strategy changes
# =========================

print("🔥🔥🔥 MAIN.PY v3.17.2 RUNNING 🔥🔥🔥", flush=True)

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
PROTECT_PROFIT_THRESHOLD = 0.2
GIVEBACK_RATIO = 0.5

ENABLE_MOMENTUM_FILTER = True
ENABLE_SHADOW_TRADES = True

DATA_VERSION = "v3.17.2"


def get_db():
    return psycopg2.connect(DATABASE_URL)


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


def classify_market(regime, mom_band):
    if regime == "TRENDING":
        return "TRENDING_OVEREXTENDED" if mom_band in ["HIGH", "EXTREME"] else "TRENDING_CLEAN"
    if regime == "TRANSITION":
        return "TRANSITION_OVEREXTENDED" if mom_band in ["HIGH", "EXTREME"] else "TRANSITION_CLEAN"
    return "CHOP"


# =========================
# 🚀 WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print(f"📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)

        symbol = data.get("symbol")
        price = float(data.get("price", 0))
        decision = data.get("decision_model") or data.get("decision")

        momentum = float(data.get("momentum_strength") or 0)
        trend = float(data.get("trend_strength") or 0)

        alignment = data.get("trend_alignment")

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        tier, subtier = classify_trade(momentum, trend)

        structure_score = round(abs_mom * abs_trend, 3)
        structure_bucket = "HIGH_STRUCT" if structure_score >= 0.15 else "MID_STRUCT"

        regime = classify_regime(abs_trend)

        if abs_mom < 0.5:
            mom_band = "LOW"
        elif abs_mom < 1.0:
            mom_band = "MID"
        elif abs_mom < 2.0:
            mom_band = "HIGH"
        else:
            mom_band = "EXTREME"

        market_condition = classify_market(regime, mom_band)

        is_prime_setup = (
            mom_band == "EXTREME"
            and regime == "TRANSITION"
            and structure_bucket == "HIGH_STRUCT"
        )

        scenario = "PRIME" if is_prime_setup else "NON_PRIME"

        # =========================
        # FILTER LOGIC
        # =========================
        live_filter_block = ENABLE_MOMENTUM_FILTER and mom_band == "HIGH"

        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"
        elif alignment != "aligned":
            hold_reason = "counter_trend"
        elif abs_trend < MIN_TREND:
            hold_reason = "not_strong_trend"
        elif abs_mom < MIN_MOM:
            hold_reason = "momentum_too_weak"
        elif live_filter_block:
            hold_reason = "filtered_high_momentum"

        if hold_reason is None and tier != "A":
            hold_reason = "low_quality"

        action = "OPEN" if hold_reason is None else "BLOCKED"

        conn = get_db()
        cur = conn.cursor()

        # =========================
        # 🚀 ENTRY (FIXED)
        # =========================
        if action == "OPEN":

            # ✅ ONLY COUNT REAL TRADES
            cur.execute("""
                SELECT COUNT(*) FROM bot_trades 
                WHERE status='OPEN' AND is_shadow = FALSE
            """)
            total_open = cur.fetchone()[0]

            if total_open < MAX_OPEN_TRADES:

                # ✅ ONLY CHECK REAL DUPLICATES
                cur.execute("""
                    SELECT COUNT(*) FROM bot_trades
                    WHERE symbol=%s AND status='OPEN' AND is_shadow = FALSE
                """, (symbol,))
                exists = cur.fetchone()[0]

                if exists == 0:
                    cur.execute("""
                        INSERT INTO bot_trades (
                            symbol, direction, entry_price,
                            status, opened_at, tier, data_version,
                            entry_momentum, entry_trend,
                            entry_tier, entry_subtier,
                            peak_pnl_percent,
                            regime, market_condition,
                            structure_bucket, mom_band,
                            is_prime_setup, scenario,
                            is_shadow, hold_reason
                        )
                        VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,NULL)
                    """, (
                        symbol, decision, price,
                        tier, DATA_VERSION,
                        momentum, trend,
                        tier, subtier,
                        0,
                        regime, market_condition,
                        structure_bucket, mom_band,
                        is_prime_setup, scenario
                    ))

                    print(f"🚀 OPEN: {symbol} | {scenario}", flush=True)

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}", flush=True)

            if ENABLE_SHADOW_TRADES:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, tier, data_version,
                        entry_momentum, entry_trend,
                        entry_tier, entry_subtier,
                        peak_pnl_percent,
                        regime, market_condition,
                        structure_bucket, mom_band,
                        is_prime_setup, scenario,
                        is_shadow, hold_reason
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                """, (
                    symbol, decision, price,
                    tier, DATA_VERSION,
                    momentum, trend,
                    tier, subtier,
                    0,
                    regime, market_condition,
                    structure_bucket, mom_band,
                    is_prime_setup, scenario,
                    hold_reason
                ))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
