# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v4.1 (FIXED PRICE FLOW + TRUE SYMBOL EVALUATION)
# =========================

print("🔥🔥🔥 MAIN.PY v4.1 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

MAX_OPEN_TRADES = 7
TRADE_SIZE_GBP = 100

# =========================
# 🚀 V4 CORE SETTINGS
# =========================

ENABLE_INITIAL_MOVE_FILTER = True
INITIAL_MOVE_WINDOW = 5
MIN_INITIAL_MOVE = 0.15

ENABLE_V4_PROFIT_SYSTEM = True

LOCK_1_TRIGGER = 20
LOCK_1_FLOOR = 10

LOCK_2_TRIGGER = 30
LOCK_2_FLOOR = 20

DATA_VERSION = "v4.1"

def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# 🧠 HELPERS
# =========================
def classify_regime(trend):
    t = abs(float(trend))
    if t >= 0.25: return "TRENDING"
    if t >= 0.15: return "TRANSITION"
    return "CHOP"

def classify_quality(momentum, trend):
    m, t = abs(momentum), abs(trend)
    if m >= 0.6 and t >= 0.2: return "A+"
    if m >= 0.45 and t >= 0.18: return "A"
    if m >= 0.3 and t >= 0.12: return "B"
    return "C"

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
        decision = (data.get("decision_model") or "").upper()

        momentum = float(data.get("momentum_strength") or 0)
        trend = float(data.get("trend_strength") or 0)

        # normalize decision
        if decision in ["", "NONE", "NULL", "UPDATE"]:
            decision = None

        now = datetime.utcnow()

        regime = classify_regime(trend)
        quality = classify_quality(momentum, trend)

        conn = get_db()
        cur = conn.cursor()

        # ================= RAW SIGNAL LOGGING =================
        cur.execute("""
            INSERT INTO signals_raw (
                symbol,
                price,
                momentum,
                trend,
                decision,
                data_version
            )
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            price,
            momentum,
            trend,
            decision,
            DATA_VERSION
        ))

        # ================= ENTRY =================
        if decision in ["LONG", "SHORT"]:

            cur.execute("""
                INSERT INTO bot_trades_v4 (
                    symbol, direction, entry_price,
                    status, opened_at, data_version,
                    momentum_strength, trend_strength,
                    entry_quality, regime,
                    is_shadow, peak_pnl_percent
                )
                VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,FALSE,0)
                RETURNING id
            """, (
                symbol, decision, price,
                DATA_VERSION,
                momentum, trend,
                quality, regime
            ))

            trade_id = cur.fetchone()[0]

            print(f"🚀 OPEN | {symbol} | id={trade_id} | Q={quality}", flush=True)

        # ================= EXIT ENGINE =================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at,
                   peak_pnl_percent
            FROM bot_trades_v4
            WHERE status='OPEN'
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl) in open_trades:

            # 🔥 KEY FIX: only process trades for THIS symbol
            if sym != symbol:
                continue

            trade_price = price

            pnl = ((trade_price - entry_price) / entry_price) if direction == "LONG" \
                else ((entry_price - trade_price) / entry_price)

            pnl_percent = pnl * 100

            current_peak = peak_pnl or 0
            if pnl_percent > current_peak:
                current_peak = pnl_percent
                cur.execute(
                    "UPDATE bot_trades_v4 SET peak_pnl_percent=%s WHERE id=%s",
                    (current_peak, tid)
                )

            # ================= EVENT LOG =================
            cur.execute("""
                INSERT INTO trade_events (
                    trade_id,
                    symbol,
                    price,
                    pnl_percent,
                    peak_pnl_percent,
                    momentum,
                    trend
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s)
            """, (
                tid,
                sym,
                trade_price,
                pnl_percent,
                current_peak,
                momentum,
                trend
            ))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            # ================= INITIAL FILTER =================
            if ENABLE_INITIAL_MOVE_FILTER:
                if mins > INITIAL_MOVE_WINDOW and current_peak < MIN_INITIAL_MOVE:
                    close_reason = "no_expansion"

            # ================= PROFIT SYSTEM =================
            if not close_reason and ENABLE_V4_PROFIT_SYSTEM:

                if current_peak >= LOCK_2_TRIGGER:
                    if pnl_percent <= LOCK_2_FLOOR:
                        close_reason = "lock_2_exit"

                elif current_peak >= LOCK_1_TRIGGER:
                    if pnl_percent <= LOCK_1_FLOOR:
                        close_reason = "lock_1_exit"

            # ================= HARD STOP =================
            if not close_reason and pnl < -0.004:
                close_reason = "hard_stop"

            if close_reason:
                pnl_gbp = (pnl_percent / 100) * TRADE_SIZE_GBP

                cur.execute("""
                    UPDATE bot_trades_v4
                    SET status='CLOSED',
                        closed_at=NOW(),
                        close_price=%s,
                        pnl_percent=%s,
                        pnl_gbp=%s,
                        close_reason=%s
                    WHERE id=%s
                """, (trade_price, pnl_percent, pnl_gbp, close_reason, tid))

                print(
                    f"💰 CLOSED | {sym} | "
                    f"{round(pnl_percent,3)}% | peak={round(current_peak,3)} | {close_reason}",
                    flush=True
                )

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400
