# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.31
# NOTES:
# - ✅ FIXED shadow trade duplication (1 per symbol+direction)
# - ✅ ADDED profit lock system (fixes leakage)
# - ✅ ADDED early trail + strong trail exits
# - ✅ RESTORED LOGGING (entry + exit visibility)
# - ✅ FIXED no_progress_exit threshold (0.05)
# =========================

print("🔥🔥🔥 MAIN.PY v3.31 RUNNING 🔥🔥🔥", flush=True)

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

ENABLE_SHADOW_TRADES = True

# 🚀 NEW EXIT SYSTEM
ENABLE_PROFIT_LOCK = True
PROFIT_LOCK_THRESHOLD = 25     # %
PROFIT_LOCK_RATIO = 0.5

ENABLE_EARLY_TRAIL = True
EARLY_TRAIL_THRESHOLD = 15     # %
EARLY_TRAIL_GIVEBACK = 7       # %

ENABLE_STRONG_TRAIL = True
STRONG_TRAIL_THRESHOLD = 40    # %
STRONG_TRAIL_RATIO = 0.65

ENABLE_NO_PROGRESS_EXIT = True
NO_PROGRESS_TIME_MIN = 20
NO_PROGRESS_PEAK_THRESHOLD = 0.05   # 🔥 FIXED

DATA_VERSION = "v3.31"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

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

        if symbol:
            PRICE_CACHE[symbol] = price

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        now = datetime.utcnow()

        # 🔥 RESTORED CONTEXT LOG
        print(
            f"📊 {symbol} | decision={decision} | "
            f"mom={round(momentum,3)} | trend={round(trend,3)}",
            flush=True
        )

        conn = get_db()
        cur = conn.cursor()

        # =========================
        # 🚨 SHADOW FIX
        # =========================
        def shadow_exists(symbol, direction):
            cur.execute("""
                SELECT 1 FROM bot_trades
                WHERE status='OPEN'
                AND is_shadow = TRUE
                AND symbol=%s
                AND direction=%s
                LIMIT 1
            """, (symbol, direction))
            return cur.fetchone() is not None

        def real_exists(symbol, direction):
            cur.execute("""
                SELECT 1 FROM bot_trades
                WHERE status='OPEN'
                AND is_shadow = FALSE
                AND symbol=%s
                AND direction=%s
                LIMIT 1
            """, (symbol, direction))
            return cur.fetchone() is not None

        # =========================
        # ENTRY (MINIMAL CHANGE)
        # =========================
        if decision in ["LONG", "SHORT"]:

            # 🔥 ENTRY DEBUG LOG
            print(
                f"🎯 ENTRY CHECK | {symbol} | {decision} | "
                f"real={real_exists(symbol, decision)} | "
                f"shadow={shadow_exists(symbol, decision)}",
                flush=True
            )

            if not real_exists(symbol, decision):
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,FALSE,0)
                """, (symbol, decision, price, DATA_VERSION))

                print(f"🚀 REAL OPEN | {symbol}", flush=True)

            elif ENABLE_SHADOW_TRADES and not shadow_exists(symbol, decision):
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol, direction, entry_price,
                        status, opened_at, data_version,
                        is_shadow, peak_pnl_percent
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,TRUE,0)
                """, (symbol, decision, price, DATA_VERSION))

                print(f"👻 SHADOW OPEN | {symbol}", flush=True)

        # =========================
        # EXIT ENGINE (UPGRADED)
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN'
        """)

        open_trades = cur.fetchall()

        for row in open_trades:
            tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow = row

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

            # 🔥 EXIT DEBUG LOG
            print(
                f"🔍 {sym} | pnl={round(pnl_percent,2)}% | "
                f"peak={round((peak_pnl or 0),2)}% | mins={round(mins,1)}",
                flush=True
            )

            # 🚀 1. STRONG TRAIL
            if ENABLE_STRONG_TRAIL and peak_pnl and peak_pnl >= STRONG_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl * STRONG_TRAIL_RATIO:
                    close_reason = "strong_trail_exit"

            # 🚀 2. PROFIT LOCK
            elif ENABLE_PROFIT_LOCK and peak_pnl and peak_pnl >= PROFIT_LOCK_THRESHOLD:
                if pnl_percent <= peak_pnl * PROFIT_LOCK_RATIO:
                    close_reason = "profit_lock_exit"

            # 🚀 3. EARLY TRAIL
            elif ENABLE_EARLY_TRAIL and peak_pnl and peak_pnl >= EARLY_TRAIL_THRESHOLD:
                if pnl_percent <= peak_pnl - EARLY_TRAIL_GIVEBACK:
                    close_reason = "early_trail_exit"

            # 🧠 4. NO PROGRESS (FIXED)
            elif ENABLE_NO_PROGRESS_EXIT:
                if mins > NO_PROGRESS_TIME_MIN and (peak_pnl or 0) < NO_PROGRESS_PEAK_THRESHOLD:
                    close_reason = "no_progress_exit"

            # 🧱 fallback stop
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
                    trade_price, pnl_percent, pnl_gbp, leakage,
                    TRADE_SIZE_GBP, close_reason, tid
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
