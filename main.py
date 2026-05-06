# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v5.1 (CONTROLLED RELAXED STRUCTURE + REGIME ENGINE)
# =========================

print("🔥🔥🔥 MAIN.PY v5.1 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

MAX_OPEN_TRADES = 7
TRADE_SIZE_GBP = 100

DATA_VERSION = "v5.1"

# =========================
# 🚀 V5 REGIME SETTINGS
# =========================

ENABLE_V5_ENTRY_FILTER = True
ENABLE_MAX_OPEN_TRADES = True

MIN_ENTRY_TREND = 0.25
MIN_ENTRY_MOMENTUM = 0.70

HOT_SNIPER_COUNT = 5
WARM_SNIPER_COUNT = 3
LOW_SNIPER_COUNT = 1

ACTIVE_RISING_MIN_DELTA = 2

# =========================
# 🚪 EXIT SETTINGS
# =========================

ENABLE_EARLY_KILL = True
EARLY_KILL_WINDOW = 5
EARLY_KILL_THRESHOLD = 10

ENABLE_PROFIT_LOCKS = True

LOCK_1_TRIGGER = 20
LOCK_1_RATIO = 0.7

LOCK_2_TRIGGER = 30
LOCK_2_RATIO = 0.8

ENABLE_NO_RED_AFTER_WIN = True

# =========================
# DB
# =========================

def get_db():
    return psycopg2.connect(DATABASE_URL)

def column_exists(cur, table_name, column_name):
    cur.execute("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.columns
            WHERE table_name = %s
              AND column_name = %s
        )
    """, (table_name, column_name))
    return cur.fetchone()[0]

# =========================
# HELPERS
# =========================

def classify_quality(momentum, trend):
    m, t = abs(momentum), abs(trend)

    if t >= 0.35 and m >= 1.00:
        return "V5_SNIPER"
    if t >= 0.30 and m >= 0.80:
        return "V5_STRONG"
    if t >= 0.25 and m >= 0.50:
        return "V5_WATCH"
    return "LOW_QUALITY"

def get_live_regime(cur):
    cur.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE ABS(trend) >= %s
                  AND ABS(momentum) >= %s
            ) AS sniper_now
        FROM signals_raw
        WHERE timestamp >= NOW() - INTERVAL '30 minutes'
    """, (MIN_ENTRY_TREND, MIN_ENTRY_MOMENTUM))

    sniper_now = cur.fetchone()[0] or 0

    cur.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE ABS(trend) >= %s
                  AND ABS(momentum) >= %s
            ) AS sniper_before
        FROM signals_raw
        WHERE timestamp >= NOW() - INTERVAL '60 minutes'
          AND timestamp < NOW() - INTERVAL '30 minutes'
    """, (MIN_ENTRY_TREND, MIN_ENTRY_MOMENTUM))

    sniper_before = cur.fetchone()[0] or 0

    sniper_delta = sniper_now - sniper_before

    if sniper_now >= HOT_SNIPER_COUNT:
        regime_state = "HOT"
    elif sniper_now >= WARM_SNIPER_COUNT:
        regime_state = "WARM"
    elif sniper_now >= LOW_SNIPER_COUNT:
        regime_state = "LOW"
    else:
        regime_state = "COLD"

    active_rising = sniper_delta >= ACTIVE_RISING_MIN_DELTA

    return regime_state, sniper_now, sniper_before, sniper_delta, active_rising

def passes_v5_filter(momentum, trend, regime_state, active_rising):
    if abs(trend) < MIN_ENTRY_TREND:
        return False

    if abs(momentum) < MIN_ENTRY_MOMENTUM:
        return False

    if regime_state in ["HOT", "WARM"]:
        return True

    if active_rising:
        return True

    return False

def log_trade_event(cur, trade_id, symbol, event_type, price, pnl_percent,
                    peak_pnl_percent, minutes_in_trade, momentum, trend, is_entry=False):
    cur.execute("""
        INSERT INTO trade_events (
            trade_id,
            symbol,
            event_time,
            event_type,
            price,
            pnl_percent,
            peak_pnl_percent,
            minutes_in_trade,
            momentum,
            trend,
            is_entry
        )
        VALUES (%s,%s,NOW(),%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        str(trade_id),
        symbol,
        event_type,
        price,
        pnl_percent,
        peak_pnl_percent,
        minutes_in_trade,
        momentum,
        trend,
        is_entry
    ))

# =========================
# WEBHOOK
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

        if decision in ["", "NONE", "NULL", "UPDATE"]:
            decision = None

        now = datetime.utcnow()

        conn = get_db()
        cur = conn.cursor()

        has_peak_time = column_exists(cur, "bot_trades_v4", "peak_time_minutes")

        # ================= RAW SIGNAL — ALWAYS STORE =================
        cur.execute("""
            INSERT INTO signals_raw (
                symbol, price, momentum, trend, decision, data_version
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING id, timestamp
        """, (
            symbol, price, momentum, trend, decision, DATA_VERSION
        ))

        signal_id, signal_time = cur.fetchone()

        # ================= LIVE REGIME =================
        regime_state, sniper_now, sniper_before, sniper_delta, active_rising = get_live_regime(cur)
        quality = classify_quality(momentum, trend)

        print(
            f"🧠 REGIME | {regime_state} | sniper_now={sniper_now} | "
            f"prev={sniper_before} | delta={sniper_delta} | active={active_rising}",
            flush=True
        )

        # ================= ENTRY =================
        if decision in ["LONG", "SHORT"]:

            entry_allowed = True

            if ENABLE_V5_ENTRY_FILTER:
                entry_allowed = passes_v5_filter(
                    momentum,
                    trend,
                    regime_state,
                    active_rising
                )

            if ENABLE_MAX_OPEN_TRADES:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM bot_trades_v4
                    WHERE status = 'OPEN'
                """)
                open_count = cur.fetchone()[0] or 0

                if open_count >= MAX_OPEN_TRADES:
                    entry_allowed = False
                    print(f"⛔ BLOCKED | max open trades reached: {open_count}", flush=True)

            if entry_allowed:

                cur.execute("""
                    INSERT INTO bot_trades_v4 (
                        symbol,
                        direction,
                        entry_price,
                        status,
                        opened_at,
                        data_version,
                        momentum_strength,
                        trend_strength,
                        entry_quality,
                        regime,
                        is_shadow,
                        peak_pnl_percent,
                        signal_id,
                        signal_timestamp
                    )
                    VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,FALSE,0,%s,%s)
                    RETURNING id
                """, (
                    symbol,
                    decision,
                    price,
                    DATA_VERSION,
                    momentum,
                    trend,
                    quality,
                    regime_state,
                    signal_id,
                    signal_time
                ))

                trade_id = cur.fetchone()[0]

                log_trade_event(
                    cur,
                    trade_id,
                    symbol,
                    "entry",
                    price,
                    0,
                    0,
                    0,
                    momentum,
                    trend,
                    True
                )

                print(
                    f"🚀 OPEN | {symbol} | {decision} | id={trade_id} | "
                    f"Q={quality} | regime={regime_state}",
                    flush=True
                )

            else:
                print(
                    f"⛔ BLOCKED | {symbol} | {decision} | "
                    f"mom={round(momentum,3)} trend={round(trend,3)} | "
                    f"regime={regime_state}",
                    flush=True
                )

        # ================= EXIT ENGINE =================
        cur.execute("""
            SELECT
                id,
                symbol,
                direction,
                entry_price,
                opened_at,
                peak_pnl_percent
            FROM bot_trades_v4
            WHERE status = 'OPEN'
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl) in open_trades:

            if sym != symbol:
                continue

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                else ((entry_price - price) / entry_price)

            pnl_percent = pnl * 100
            mins = (now - opened_at).total_seconds() / 60

            current_peak = peak_pnl or 0

            if pnl_percent > current_peak:
                current_peak = pnl_percent

                if has_peak_time:
                    cur.execute("""
                        UPDATE bot_trades_v4
                        SET peak_pnl_percent = %s,
                            peak_time_minutes = %s
                        WHERE id = %s
                    """, (current_peak, mins, tid))
                else:
                    cur.execute("""
                        UPDATE bot_trades_v4
                        SET peak_pnl_percent = %s
                        WHERE id = %s
                    """, (current_peak, tid))

            log_trade_event(
                cur,
                tid,
                sym,
                "update",
                price,
                pnl_percent,
                current_peak,
                mins,
                momentum,
                trend,
                False
            )

            close_reason = None

            # EARLY KILL
            if ENABLE_EARLY_KILL:
                if mins > EARLY_KILL_WINDOW and current_peak < EARLY_KILL_THRESHOLD:
                    close_reason = "dead_trade"

            # PROFIT LOCKS
            if not close_reason and ENABLE_PROFIT_LOCKS:

                if current_peak >= LOCK_2_TRIGGER:
                    lock_floor = current_peak * LOCK_2_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "lock_2"

                elif current_peak >= LOCK_1_TRIGGER:
                    lock_floor = current_peak * LOCK_1_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "lock_1"

            # NO RED AFTER WIN
            if not close_reason and ENABLE_NO_RED_AFTER_WIN:
                if current_peak >= 20 and pnl_percent < 0:
                    close_reason = "gave_back_winner"

            # HARD STOP
            if not close_reason and pnl < -0.004:
                close_reason = "hard_stop"

            if close_reason:
                pnl_gbp = (pnl_percent / 100) * TRADE_SIZE_GBP

                cur.execute("""
                    UPDATE bot_trades_v4
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        close_price = %s,
                        pnl_percent = %s,
                        pnl_gbp = %s,
                        close_reason = %s
                    WHERE id = %s
                """, (
                    price,
                    pnl_percent,
                    pnl_gbp,
                    close_reason,
                    tid
                ))

                log_trade_event(
                    cur,
                    tid,
                    sym,
                    f"exit_{close_reason}",
                    price,
                    pnl_percent,
                    current_peak,
                    mins,
                    momentum,
                    trend,
                    False
                )

                print(
                    f"💰 CLOSED | {sym} | "
                    f"{round(pnl_percent,3)}% | peak={round(current_peak,3)} | "
                    f"{close_reason}",
                    flush=True
                )

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok", "version": DATA_VERSION}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400


@app.route("/", methods=["GET"])
def home():
    return jsonify({"status": "running", "version": DATA_VERSION}), 200
