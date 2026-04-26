# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.24.0
# DEPLOYED: 2026-04-26
# NOTES:
# - ✅ PRICE CACHE ENGINE (true multi-asset support)
# - ✅ CORRECT PnL CALCULATION
# - ✅ GLOBAL EXIT ENGINE (fixed)
# - ✅ SAFETY TIMEOUT
# =========================

print("🔥🔥🔥 MAIN.PY v3.24.0 RUNNING 🔥🔥🔥", flush=True)

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

ENABLE_SAFETY_TIMEOUT = True
MAX_TRADE_DURATION_MIN = 90

DATA_VERSION = "v3.24.0"

# =========================
# 🧠 PRICE CACHE (NEW CORE)
# =========================
PRICE_CACHE = {}

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

        # =========================
        # 🧠 UPDATE PRICE CACHE
        # =========================
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

        # MOM BAND
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
        # 🎯 ENTRY LOGIC
        # =========================
        force_shadow = False
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif decision == "SHORT":
            hold_reason = "shorts_disabled"

        elif mom_band == "EXTREME":
            hold_reason = "forced_extreme_shadow"
            force_shadow = True

        elif regime == "TRANSITION":
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
        # 🚀 ENTRY
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
                        regime, mom_band,
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,FALSE,NULL,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    regime, mom_band
                ))

                print(f"🚀 OPEN | {symbol} | {decision} | {regime} | {mom_band}", flush=True)

        else:
            print(f"👻 SHADOW | {symbol} | reason={hold_reason}", flush=True)

            if ENABLE_SHADOW_TRADES:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        entry_momentum, entry_trend,
                        regime, mom_band,
                        is_shadow, hold_reason,
                        peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,TRUE,%s,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    regime, mom_band,
                    hold_reason
                ))

        # =========================
        # 🌍 GLOBAL EXIT ENGINE (FIXED)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN'
        """)

        open_trades = cur.fetchall()

        for row in open_trades:
            tid = row[0]
            sym = row[1]
            direction = row[2]
            entry_price = row[3]
            opened_at = row[4]
            peak_pnl = row[5] or 0
            is_shadow = row[6]

            # 🧠 USE CORRECT SYMBOL PRICE
            trade_price = PRICE_CACHE.get(sym)

            if not trade_price:
                continue

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - trade_price) / entry_price)

            pnl_percent = pnl * 100

            if pnl_percent > peak_pnl:
                cur.execute(
                    "UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s",
                    (pnl_percent, tid)
                )

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            # SAFETY
            if ENABLE_SAFETY_TIMEOUT and mins > MAX_TRADE_DURATION_MIN:
                close_reason = "safety_timeout"

            elif ENABLE_GIVEBACK_EXIT and peak_pnl >= PROTECT_PROFIT_THRESHOLD:
                if pnl_percent < peak_pnl * GIVEBACK_RATIO:
                    close_reason = "giveback_exit"

            elif pnl < -0.003:
                close_reason = "hard_stop"
            elif pnl > 0.003 and mins < 15:
                close_reason = "quick_profit"
            elif pnl > 0 and abs_mom < 0.1:
                close_reason = "momentum_drop"
            elif pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"
            elif mins > 10 and pnl <= 0:
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
                        close_reason=%s
                    WHERE id=%s
                """, (
                    trade_price, pnl_percent, pnl_gbp, TRADE_SIZE_GBP,
                    close_reason, tid
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
