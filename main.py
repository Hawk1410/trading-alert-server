# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.21.4
# DEPLOYED: 2026-04-26
# NOTES:
# - 🔧 FIXED tuple crash (entry + exit)
# - 🔒 Schema-safe inserts
# - 🛡 Fully fault-tolerant execution
# =========================

print("🔥🔥🔥 MAIN.PY v3.21.4 RUNNING 🔥🔥🔥", flush=True)

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
ENABLE_CHOP_MODE = True

DATA_VERSION = "v3.21.4"


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
        print(f"\n📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)
        print(f"📦 RAW DATA: {data}", flush=True)

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

        force_shadow_extreme = (mom_band == "EXTREME")

        # =========================
        # HYBRID LOGIC
        # =========================
        regime_block = None

        if regime == "CHOP":
            if not ENABLE_CHOP_MODE:
                regime_block = "chop_disabled"
            elif mom_band != "LOW":
                regime_block = "chop_momentum_block"
            elif alignment != "aligned":
                regime_block = "chop_alignment_block"

        elif regime == "TRANSITION":
            regime_block = "transition_shadow_only"

        print(f"📊 {symbol} | {decision} | {regime} | {mom_band}", flush=True)

        # =========================
        # FILTERS
        # =========================
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"
        elif decision == "SHORT":
            hold_reason = "shorts_disabled"
        elif regime_block:
            hold_reason = regime_block
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
        # ENTRY
        # =========================
        if action == "OPEN" and not force_shadow_extreme:

            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN' AND is_shadow=FALSE")
            if cur.fetchone()[0] < MAX_OPEN_TRADES:

                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, tier, data_version,
                        entry_momentum, entry_trend,
                        entry_tier, entry_subtier,
                        peak_pnl_percent,
                        regime, market_condition,
                        structure_bucket, mom_band,
                        is_shadow, hold_reason
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,%s)
                """, (
                    symbol, decision, price,
                    tier, DATA_VERSION,
                    momentum, trend,
                    tier, subtier,
                    0,
                    regime, market_condition,
                    structure_bucket, mom_band,
                    None
                ))

        else:
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
                        is_shadow, hold_reason
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,%s)
                """, (
                    symbol, decision, price,
                    tier, DATA_VERSION,
                    momentum, trend,
                    tier, subtier,
                    0,
                    regime, market_condition,
                    structure_bucket, mom_band,
                    hold_reason or "shadow"
                ))

        # =========================
        # EXIT ENGINE (SAFE)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN'
        """)

        for row in cur.fetchall():
            try:
                if len(row) < 7:
                    continue

                tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow = row
                peak_pnl = peak_pnl or 0

                if sym != symbol:
                    continue

                pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                      else ((entry_price - price) / entry_price)

                pnl_percent = pnl * 100

                if pnl_percent > peak_pnl:
                    cur.execute("UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s", (pnl_percent, tid))

                mins = (now - opened_at).total_seconds() / 60
                close_reason = None

                if pnl < -0.003:
                    close_reason = "hard_stop"
                elif pnl > 0.003 and mins < 15:
                    close_reason = "quick_profit"
                elif mins > 10 and pnl <= 0:
                    close_reason = "time_fail_fast"

                if close_reason:
                    cur.execute("""
                        UPDATE bot_trades
                        SET status='CLOSED',
                            closed_at=NOW(),
                            close_price=%s,
                            pnl_percent=%s,
                            close_reason=%s
                        WHERE id=%s
                    """, (price, pnl_percent, close_reason, tid))

            except Exception as e:
                print(f"❌ EXIT LOOP ERROR: {e} | row={row}", flush=True)
                continue

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
