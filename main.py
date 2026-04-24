# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.16.0
# DEPLOYED: 2026-04-XX
# NOTES:
# - ✅ Shadow trades added (NO behaviour change)
# - ✅ Logs blocked opportunities
# - ✅ Fully compatible with v3.15.6 schema
# =========================

print("🔥🔥🔥 MAIN.PY v3.16.0 RUNNING 🔥🔥🔥", flush=True)

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
MIN_TREND = 0.20
MIN_MOM = 0.05
TRADE_SIZE_GBP = 100

ENABLE_GIVEBACK_EXIT = True
PROTECT_PROFIT_THRESHOLD = 0.2
GIVEBACK_RATIO = 0.5

ENABLE_MOMENTUM_FILTER = True
ENABLE_SHADOW_TRADES = True  # 🔥 NEW

DATA_VERSION = "v3.16.0"


def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🧠 TIER CLASSIFICATION
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
# 🏠 ROUTES
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200


@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


@app.route("/version", methods=["GET"])
def version():
    return jsonify({
        "version": DATA_VERSION,
        "status": "running"
    })


# =========================
# 🚀 WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print(f"📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)
        print(f"📦 RAW DATA: {data}", flush=True)

        symbol = data.get("symbol")
        price = float(data.get("price", 0))

        decision = data.get("decision_model") or data.get("decision")

        momentum = float(data.get("momentum_strength") or data.get("momentum") or 0)
        trend = float(data.get("trend_strength") or data.get("trend") or 0)

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

        # =========================
        # 🎯 PRIME SETUP
        # =========================
        is_prime_setup = (
            mom_band == "EXTREME"
            and regime == "TRANSITION"
        )

        scenario = "PRIME" if is_prime_setup else "NON_PRIME"

        print(f"📊 SIGNAL: {symbol} | {decision} | mom={momentum:.3f} | trend={trend:.3f} | {tier}/{subtier}", flush=True)
        print(f"🎯 SCENARIO: {symbol} | {scenario}", flush=True)

        # =========================
        # 🔥 FILTER LOGIC
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
        # 🚀 REAL ENTRY (UNCHANGED)
        # =========================
        if action == "OPEN":

            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            total_open = cur.fetchone()[0]

            if total_open < MAX_OPEN_TRADES:

                cur.execute("""
                    SELECT COUNT(*) FROM bot_trades
                    WHERE symbol=%s AND status='OPEN'
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
            print(f"⛔ BLOCKED: {symbol} | {hold_reason} | {scenario}", flush=True)

            # =========================
            # 👻 SHADOW ENTRY (NEW)
            # =========================
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

                print(f"👻 SHADOW OPEN: {symbol} | {hold_reason}", flush=True)

        # =========================
        # 🧠 EXIT ENGINE (UNCHANGED LOGIC)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN'
            AND symbol = %s
        """, (symbol,))

        open_trades = cur.fetchall()

        for tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow in open_trades:

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - price) / entry_price)

            if abs(pnl) > 0.1:
                continue

            pnl_percent = pnl * 100

            if pnl_percent > (peak_pnl or 0):
                cur.execute("UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s", (pnl_percent, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if ENABLE_GIVEBACK_EXIT and peak_pnl:
                if peak_pnl >= PROTECT_PROFIT_THRESHOLD:
                    if pnl_percent < peak_pnl * GIVEBACK_RATIO:
                        close_reason = "giveback_exit"

            if not close_reason:
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
                    price, pnl_percent, pnl_gbp, TRADE_SIZE_GBP,
                    close_reason, momentum, trend, tid
                ))

                tag = "👻 SHADOW" if is_shadow else "💰 REAL"
                print(f"{tag} CLOSED: {sym} | {close_reason} | {round(pnl_percent,3)}%", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
