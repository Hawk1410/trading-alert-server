# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.35 (EARLY FILTER UPGRADE)
# =========================

print("🔥🔥🔥 MAIN.PY v3.35 RUNNING 🔥🔥🔥", flush=True)

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

# 🔥 UPDATED EARLY FILTER
ENABLE_NO_PROGRESS_EXIT = True
NO_PROGRESS_TIME_MIN = 12
NO_PROGRESS_PEAK_THRESHOLD = 0.06

DATA_VERSION = "v3.35"

PRICE_CACHE = {}

def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# 🧠 REGIME
# =========================
def classify_regime(trend):
    try:
        t = abs(float(trend))
        if t >= 0.25: return "TRENDING"
        if t >= 0.15: return "TRANSITION"
        return "CHOP"
    except:
        return "UNKNOWN"

def classify_quality(momentum, trend):
    m, t = abs(momentum), abs(trend)
    if m >= 0.6 and t >= 0.2: return "A+"
    if m >= 0.45 and t >= 0.18: return "A"
    if m >= 0.3 and t >= 0.12: return "B"
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
    peak, final = cur.fetchone()
    peak, final = float(peak or 0), float(final or 0)

    if peak < 0.15: return "VERY_BAD", peak, final
    if peak < 0.25: return "BAD", peak, final
    if peak > 0.30 and final < 0.05: return "LOW_QUALITY", peak, final
    if peak > 0.30: return "GOOD", peak, final
    return "NEUTRAL", peak, final

# =========================
# 🚀 WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        print(f"\n📩 WEBHOOK HIT | {DATA_VERSION}", flush=True)

        data = request.get_json(force=True)

        symbol = data.get("symbol")
        price = float(data.get("price", 0) or 0)
        decision = (data.get("decision_model") or data.get("decision") or "").upper()

        momentum = float(data.get("momentum_strength") or 0)
        trend = float(data.get("trend_strength") or 0)

        if symbol:
            PRICE_CACHE[symbol] = price

        if decision in ["", "NONE", "NULL"]:
            decision = None

        now = datetime.utcnow()

        regime = classify_regime(trend)
        quality = classify_quality(momentum, trend)

        abs_mom, abs_trend = abs(momentum), abs(trend)

        conn = get_db()
        cur = conn.cursor()

        global_regime, _, _ = get_global_regime(cur)

        # ================= ENTRY CONTROL =================
        allow_real = True
        force_shadow = False
        entry_block_reason = "EXECUTED"

        if global_regime == "VERY_BAD":
            allow_real = False
            force_shadow = True
            entry_block_reason = "very_bad_regime"

        elif global_regime == "BAD":
            if quality not in ["A", "A+"]:
                force_shadow = True
                entry_block_reason = "bad_regime_low_quality"

        elif global_regime in ["NEUTRAL", "LOW_QUALITY"]:
            if quality == "C":
                force_shadow = True
                entry_block_reason = "low_quality"

        print(f"📊 {symbol} | {decision} | Q={quality} | reg={regime} | G={global_regime}", flush=True)

        def real_exists(symbol, direction):
            cur.execute("""
                SELECT 1 FROM bot_trades
                WHERE status='OPEN' AND is_shadow=FALSE
                AND symbol=%s AND direction=%s LIMIT 1
            """, (symbol, direction))
            return cur.fetchone() is not None

        # ================= ENTRY =================
        if decision in ["LONG", "SHORT"]:

            real_open = real_exists(symbol, decision)

            if not real_open and allow_real and not force_shadow:
                is_shadow = False
            else:
                is_shadow = True

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
                VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,0)
            """, (
                symbol, decision, price,
                DATA_VERSION,
                momentum, trend,
                abs_mom, abs_trend,
                quality, entry_block_reason,
                regime, global_regime,
                is_shadow
            ))

            print(f"{'👻' if is_shadow else '🚀'} OPEN | {symbol} | Q={quality}", flush=True)

        # ================= EXIT ENGINE =================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at,
                   peak_pnl_percent, is_shadow
            FROM bot_trades
            WHERE status='OPEN'
        """)

        for (tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow) in cur.fetchall():

            trade_price = PRICE_CACHE.get(sym)
            if not trade_price:
                continue

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" \
                else ((entry_price - trade_price) / entry_price)

            pnl_percent = pnl * 100

            if pnl_percent > (peak_pnl or 0):
                cur.execute("UPDATE bot_trades SET peak_pnl_percent=%s WHERE id=%s",
                            (pnl_percent, tid))

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

                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        close_price=%s,
                        pnl_percent=%s,
                        pnl_gbp=%s,
                        close_reason=%s
                    WHERE id=%s
                """, (trade_price, pnl_percent, pnl_gbp, close_reason, tid))

                print(f"{'👻' if is_shadow else '💰'} CLOSED | {sym} | {round(pnl_percent,3)}% | {close_reason}", flush=True)

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
