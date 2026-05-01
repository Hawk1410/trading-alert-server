# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.33.2
# NOTES:
# - ✅ REMOVED SHADOW TRADE CAP (multiple allowed per symbol)
# - ✅ FIXED missing shadow trades in DB
# - ✅ ALL LOGGING PRESERVED
# - ✅ GLOBAL EXIT LOOP PRESERVED
# =========================

print("🔥🔥🔥 MAIN.PY v3.33.2 RUNNING 🔥🔥🔥", flush=True)

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

DATA_VERSION = "v3.33.2"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

def classify_regime(trend):
    abs_trend = abs(trend)
    if abs_trend >= 0.25:
        return "TRENDING"
    elif abs_trend >= 0.15:
        return "TRANSITION"
    return "CHOP"

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
        price = float(data.get("price", 0))
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

        conn = get_db()
        cur = conn.cursor()

        global_regime, avg_peak, avg_final = get_global_regime(cur)

        print(
            f"📊 {symbol} | decision={decision} | mom={round(momentum,3)} | "
            f"trend={round(trend,3)} | regime={regime} | GLOBAL={global_regime}",
            flush=True
        )

        force_shadow = (global_regime == "BAD")
        block_entries = (global_regime == "VERY_BAD")

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
        if decision in ["LONG", "SHORT"] and not block_entries:

            real_open = real_exists(symbol, decision)

            print(f"🎯 ENTRY | {symbol} | {decision} | real={real_open}", flush=True)

            if not real_open and not force_shadow:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        momentum_strength, trend_strength,
                        regime, global_regime,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,FALSE,0)
                """, (symbol, decision, price, DATA_VERSION, momentum, trend, regime, global_regime))

                print(f"🚀 REAL OPEN | {symbol}", flush=True)

            elif ENABLE_SHADOW_TRADES:
                # 🔥 FULL FIX: ALWAYS ALLOW SHADOWS
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        momentum_strength, trend_strength,
                        regime, global_regime,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,TRUE,0)
                """, (symbol, decision, price, DATA_VERSION, momentum, trend, regime, global_regime))

                print(f"👻 SHADOW OPEN | {symbol}", flush=True)

        # ================= EXIT ENGINE =================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades WHERE status='OPEN'
        """)

        for tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow in cur.fetchall():

            trade_price = PRICE_CACHE.get(sym)

            if not trade_price:
                cur.execute("""
                    SELECT close_price FROM bot_trades
                    WHERE symbol=%s AND close_price IS NOT NULL
                    ORDER BY closed_at DESC LIMIT 1
                """, (sym,))
                res = cur.fetchone()

                if res and res[0]:
                    trade_price = float(res[0])
                else:
                    print(f"⚠️ NO PRICE DATA | {sym}", flush=True)
                    continue

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - trade_price) / entry_price)

            pnl_percent = pnl * 100

            if pnl_percent > (peak_pnl or 0):
                cur.execute("UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s", (pnl_percent, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            early_factor = 0.8 if global_regime == "LOW_QUALITY" else 1.0

            if ENABLE_STRONG_TRAIL and peak_pnl and peak_pnl >= STRONG_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl * (STRONG_TRAIL_RATIO * early_factor):
                    close_reason = "strong_trail_exit"

            elif ENABLE_PROFIT_LOCK and peak_pnl and peak_pnl >= PROFIT_LOCK_THRESHOLD:
                if pnl_percent <= peak_pnl * (PROFIT_LOCK_RATIO * early_factor):
                    close_reason = "profit_lock_exit"

            elif ENABLE_EARLY_TRAIL and peak_pnl and peak_pnl >= EARLY_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl - (EARLY_TRAIL_GIVEBACK * early_factor):
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
                """, (trade_price, pnl_percent, pnl_gbp, leakage, TRADE_SIZE_GBP, close_reason, tid))

                print(f"{'👻' if is_shadow else '💰'} CLOSED | {sym} | {round(pnl_percent,3)}% | {close_reason}", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
        
