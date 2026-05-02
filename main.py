# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.34 (HOTFIX: EXIT ENGINE FIXED)
# =========================

print("🔥🔥🔥 MAIN.PY v3.34 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

MAX_OPEN_TRADES = 7
TRADE_SIZE_GBP = 100
ENABLE_SHADOW_TRADES = True

ENABLE_PROFIT_LOCK = True
PROFIT_LOCK_THRESHOLD = 25
PROFIT_LOCK_RATIO = 0.5

ENABLE_EARLY_TRAIL = True
EARLY_TRAIL_THRESHOLD = 15
EARLY_TRAIL_GIVEBACK = 7

ENABLE_STRONG_TRAIL = True
STRONG_TRAIL_THRESHOLD = 40
STRONG_TRAIL_RATIO = 0.65

ENABLE_NO_PROGRESS_EXIT = True
NO_PROGRESS_TIME_MIN = 20
NO_PROGRESS_PEAK_THRESHOLD = 0.05

DATA_VERSION = "v3.34"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

def classify_regime(trend):
    try:
        abs_trend = abs(float(trend))
        if abs_trend >= 0.25:
            return "TRENDING"
        elif abs_trend >= 0.15:
            return "TRANSITION"
        return "CHOP"
    except:
        return "UNKNOWN"

def classify_quality(momentum, trend):
    abs_mom = abs(momentum)
    abs_trend = abs(trend)

    if abs_mom >= 0.6 and abs_trend >= 0.2:
        return "A+"
    if abs_mom >= 0.45 and abs_trend >= 0.18:
        return "A"
    if abs_mom >= 0.3 and abs_trend >= 0.12:
        return "B"
    return "C"

def get_global_regime(cur):
    cur.execute("""
        SELECT AVG(peak_pnl_percent), AVG(pnl_percent)
        FROM (
            SELECT peak_pnl_percent, pnl_percent
            FROM bot_trades
            WHERE status='CLOSED' AND is_shadow = FALSE
            ORDER BY closed_at DESC
            LIMIT 40
        ) t
    """)

    avg_peak, avg_final = cur.fetchone()
    avg_peak = float(avg_peak or 0)
    avg_final = float(avg_final or 0)

    if avg_peak < 0.15:
        return "VERY_BAD", avg_peak, avg_final
    if avg_peak < 0.25:
        return "BAD", avg_peak, avg_final
    if avg_peak > 0.30 and avg_final < 0.05:
        return "LOW_QUALITY", avg_peak, avg_final
    if avg_peak > 0.30:
        return "GOOD", avg_peak, avg_final

    return "NEUTRAL", avg_peak, avg_final

@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print(f"\n📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)

        symbol = data.get("symbol")
        price = float(data.get("price", 0) or 0)
        decision = data.get("decision_model") or data.get("decision")

        momentum = float(data.get("momentum_strength") or 0)
        trend = float(data.get("trend_strength") or 0)

        if symbol:
            PRICE_CACHE[symbol] = price

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        regime = classify_regime(trend)
        quality = classify_quality(momentum, trend)

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        conn = get_db()
        cur = conn.cursor()

        global_regime, avg_peak, avg_final = get_global_regime(cur)

        if not global_regime:
            global_regime = "UNKNOWN"

        # ================= ENTRY CONTROL =================
        allow_real = True
        force_shadow = False
        entry_block_reason = "EXECUTED"

        if global_regime == "VERY_BAD":
            allow_real = False
            force_shadow = True
            entry_block_reason = "very_bad_regime"

        elif global_regime == "BAD":
            if quality in ["A", "A+"]:
                allow_real = True
            else:
                force_shadow = True
                entry_block_reason = "bad_regime_low_quality"

        elif global_regime in ["NEUTRAL", "LOW_QUALITY"]:
            if quality == "C":
                force_shadow = True
                entry_block_reason = "low_quality"

        print(
            f"📊 {symbol} | {decision} | "
            f"mom={round(momentum,3)} ({round(abs_mom,3)}) | "
            f"trend={round(trend,3)} ({round(abs_trend,3)}) | "
            f"Q={quality} | reg={regime} | G={global_regime} | "
            f"action={'REAL' if (allow_real and not force_shadow) else 'SHADOW'} | "
            f"reason={entry_block_reason}",
            flush=True
        )

        def real_exists(symbol, direction):
            cur.execute("""
                SELECT 1 FROM bot_trades
                WHERE status='OPEN'
                AND is_shadow = FALSE
                AND symbol=%s AND direction=%s
                LIMIT 1
            """, (symbol, direction))
            return cur.fetchone() is not None

        # ================= ENTRY =================
        if decision in ["LONG", "SHORT"]:

            real_open = real_exists(symbol, decision)

            if not real_open and allow_real and not force_shadow:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        momentum_strength, trend_strength,
                        entry_momentum_abs, entry_trend_abs,
                        entry_quality, entry_block_reason,
                        regime, global_regime,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,FALSE,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    abs_mom, abs_trend,
                    quality, "EXECUTED",
                    regime, global_regime
                ))

                print(f"🚀 REAL OPEN | {symbol} | Q={quality}", flush=True)

            elif ENABLE_SHADOW_TRADES:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        momentum_strength, trend_strength,
                        entry_momentum_abs, entry_trend_abs,
                        entry_quality, entry_block_reason,
                        regime, global_regime,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,TRUE,0)
                """, (
                    symbol, decision, price,
                    DATA_VERSION,
                    momentum, trend,
                    abs_mom, abs_trend,
                    quality, entry_block_reason,
                    regime, global_regime
                ))

                print(f"👻 SHADOW OPEN | {symbol} | Q={quality}", flush=True)

        # ================= EXIT ENGINE (FIXED) =================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN' AND symbol=%s
        """, (symbol,))

        for tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow in cur.fetchall():

            trade_price = price  # ✅ GUARANTEED fresh

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

            if ENABLE_STRONG_TRAIL and peak_pnl and peak_pnl >= STRONG_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl * STRONG_TRAIL_RATIO:
                    close_reason = "strong_trail_exit"

            elif ENABLE_PROFIT_LOCK and peak_pnl and peak_pnl >= PROFIT_LOCK_THRESHOLD:
                if pnl_percent <= peak_pnl * PROFIT_LOCK_RATIO:
                    close_reason = "profit_lock_exit"

            elif ENABLE_EARLY_TRAIL and peak_pnl and peak_pnl >= EARLY_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl - EARLY_TRAIL_GIVEBACK:
                    close_reason = "early_trail_exit"

            elif ENABLE_NO_PROGRESS_EXIT:
                if mins > NO_PROGRESS_TIME_MIN and (peak_pnl or 0) < NO_PROGRESS_PEAK_THRESHOLD:
                    close_reason = "no_progress_exit"

            elif pnl < -0.004:
                close_reason = "hard_stop"

            if close_reason:
                pnl_gbp = (pnl_percent / 100) * TRADE_SIZE_GBP
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
                    trade_price, pnl_percent, pnl_gbp,
                    leakage, TRADE_SIZE_GBP, close_reason, tid
                ))

                print(f"{'👻' if is_shadow else '💰'} CLOSED | {sym} | {round(pnl_percent,3)}% | {close_reason}", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
