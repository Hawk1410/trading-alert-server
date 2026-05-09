# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v5.4
# TITLE: LONG CORE + S5 TACTICAL SHORTS + V6/V7 SHADOWS
# =========================

print("🔥🔥🔥 MAIN.PY v5.4 RUNNING 🔥🔥🔥", flush=True)

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

MAX_OPEN_TRADES = 7
MAX_OPEN_SHADOW_TRADES = 30
TRADE_SIZE_GBP = 100

DATA_VERSION = "v5.4"

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
# 🎯 V5.4 LONG CORE
# =========================

ENABLE_V53_CONTROLLED_IGNITION = True
ENABLE_CORE_LONG_ONLY = True

V53_REQUIRED_REGIME = "WARM"
V53_MIN_DENSITY = 3
V53_MAX_DENSITY = 5
V53_MIN_DENSITY_DELTA = 2
V53_MAX_DENSITY_DELTA = 4

# =========================
# 🩸 S5 TACTICAL SHORT ENGINE
# =========================

ENABLE_S5_SHORT_ENGINE = True

S5_MIN_MOMENTUM = -0.70
S5_MIN_DENSITY = 10
S5_MIN_TREND = -0.35
S5_MAX_TREND = -0.15

S5_ELITE_SYMBOLS = {
    "ARUSDT",
    "ATOMUSDT",
    "SEIUSDT",
    "LTCUSDT",
    "INJUSDT",
}

# =========================
# 👻 SHADOW ENGINES
# =========================

ENABLE_SHADOW_V6 = True
ENABLE_SHADOW_V7 = True

# V6 compression continuation
V6_MIN_TREND = 0.13
V6_MAX_TREND = 0.19
V6_MIN_MOMENTUM = 0.70
V6_MIN_DENSITY = 4
V6_MAX_DENSITY = 6
V6_MIN_DELTA = -3
V6_MAX_DELTA = 0

# V7 broad continuation
V7_MIN_TREND = 0.20

# =========================
# 🚪 EXIT SETTINGS
# =========================

ENABLE_CONFIRMATION_EXIT = True
CONFIRMATION_UPDATE_NUM = 2
CONFIRMATION_MIN_PNL = -0.05

ENABLE_PROFIT_LOCKS = True

# LONG lock ladder — percentage values
LONG_LOCK_1_TRIGGER = 0.75
LONG_LOCK_1_RATIO = 0.50

LONG_LOCK_2_TRIGGER = 1.50
LONG_LOCK_2_RATIO = 0.70

LONG_LOCK_3_TRIGGER = 3.00
LONG_LOCK_3_RATIO = 0.75

LONG_LOCK_4_TRIGGER = 5.00
LONG_LOCK_4_RATIO = 0.80

LONG_HARD_STOP = -0.40
LONG_NO_RED_AFTER_WIN_TRIGGER = 0.75

# SHORT tactical exits — percentage values
SHORT_TP_1_TRIGGER = 1.00
SHORT_TP_1_FLOOR = 0.70

SHORT_TP_05_TRIGGER = 0.50
SHORT_TP_05_FLOOR = 0.35

SHORT_HARD_STOP = -0.45

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

def classify_short_tier(symbol, momentum, trend, sniper_density):
    if (
        symbol in S5_ELITE_SYMBOLS
        and S5_MIN_TREND <= trend <= S5_MAX_TREND
        and momentum <= S5_MIN_MOMENTUM
        and sniper_density >= S5_MIN_DENSITY
    ):
        return "S5_SHORT_TIER_2_ELITE"

    if (
        S5_MIN_TREND <= trend <= S5_MAX_TREND
        and momentum <= S5_MIN_MOMENTUM
        and sniper_density >= S5_MIN_DENSITY
    ):
        return "S5_SHORT_TIER_1_BROAD"

    return None

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

def passes_v5_long_filter(momentum, trend, regime_state, active_rising, sniper_density, sniper_density_delta):
    if trend < MIN_ENTRY_TREND:
        return False

    if momentum < MIN_ENTRY_MOMENTUM:
        return False

    if ENABLE_V53_CONTROLLED_IGNITION:
        if regime_state != V53_REQUIRED_REGIME:
            return False

        if not active_rising:
            return False

        if sniper_density < V53_MIN_DENSITY or sniper_density > V53_MAX_DENSITY:
            return False

        if sniper_density_delta < V53_MIN_DENSITY_DELTA or sniper_density_delta > V53_MAX_DENSITY_DELTA:
            return False

        return True

    if regime_state in ["HOT", "WARM"]:
        return True

    if active_rising:
        return True

    return False

def passes_v6_shadow(momentum, trend, sniper_density, sniper_density_delta):
    return (
        V6_MIN_TREND <= trend <= V6_MAX_TREND
        and momentum >= V6_MIN_MOMENTUM
        and V6_MIN_DENSITY <= sniper_density <= V6_MAX_DENSITY
        and V6_MIN_DELTA <= sniper_density_delta <= V6_MAX_DELTA
    )

def passes_v7_shadow(momentum, trend):
    return trend >= V7_MIN_TREND and momentum > 0

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

def get_update_count(cur, trade_id):
    cur.execute("""
        SELECT COUNT(*)
        FROM trade_events
        WHERE trade_id = %s
          AND LOWER(event_type) = 'update'
    """, (str(trade_id),))
    return cur.fetchone()[0] or 0

def open_trade(cur, symbol, direction, price, momentum, trend, quality,
               regime_state, signal_id, signal_time, is_shadow=False):
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
        VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,%s,%s,0,%s,%s)
        RETURNING id
    """, (
        symbol,
        direction,
        price,
        DATA_VERSION,
        momentum,
        trend,
        quality,
        regime_state,
        is_shadow,
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

    return trade_id

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
                symbol,
                price,
                momentum,
                trend,
                decision,
                data_version
            )
            VALUES (%s,%s,%s,%s,%s,%s)
            RETURNING id, timestamp
        """, (
            symbol,
            price,
            momentum,
            trend,
            decision,
            DATA_VERSION
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

        # ================= ENTRY DECISION =================
        entry_allowed = False
        block_reason = None
        live_entry_quality = quality

        if decision in ["LONG", "SHORT"]:

            if decision == "LONG":
                entry_allowed = passes_v5_long_filter(
                    momentum,
                    trend,
                    regime_state,
                    active_rising,
                    sniper_now,
                    sniper_delta
                )

                if not entry_allowed:
                    if trend < MIN_ENTRY_TREND:
                        block_reason = "trend_too_weak"
                    elif momentum < MIN_ENTRY_MOMENTUM:
                        block_reason = "momentum_too_weak"
                    elif ENABLE_V53_CONTROLLED_IGNITION and regime_state != V53_REQUIRED_REGIME:
                        block_reason = "v53_not_warm_regime"
                    elif ENABLE_V53_CONTROLLED_IGNITION and not active_rising:
                        block_reason = "v53_not_active_rising"
                    elif ENABLE_V53_CONTROLLED_IGNITION and (sniper_now < V53_MIN_DENSITY or sniper_now > V53_MAX_DENSITY):
                        block_reason = "v53_density_outside_range"
                    elif ENABLE_V53_CONTROLLED_IGNITION and (sniper_delta < V53_MIN_DENSITY_DELTA or sniper_delta > V53_MAX_DENSITY_DELTA):
                        block_reason = "v53_density_delta_outside_range"
                    else:
                        block_reason = "v5_long_filter_block"

            elif decision == "SHORT":
                short_tier = classify_short_tier(symbol, momentum, trend, sniper_now)

                if ENABLE_S5_SHORT_ENGINE and short_tier:
                    entry_allowed = True
                    block_reason = None
                    live_entry_quality = short_tier
                else:
                    entry_allowed = False
                    block_reason = "short_not_s5_tactical"

            if ENABLE_MAX_OPEN_TRADES and entry_allowed:
                cur.execute("""
                    SELECT COUNT(*)
                    FROM bot_trades_v4
                    WHERE status = 'OPEN'
                      AND COALESCE(is_shadow, FALSE) = FALSE
                """)
                open_count = cur.fetchone()[0] or 0

                if open_count >= MAX_OPEN_TRADES:
                    entry_allowed = False
                    block_reason = "max_open_trades"
                    print(f"⛔ BLOCKED | max open real trades reached: {open_count}", flush=True)

        else:
            block_reason = "no_decision"

        # ================= UPDATE RAW SIGNAL INTELLIGENCE =================
        cur.execute("""
            UPDATE signals_raw
            SET
                regime_state = %s,
                sniper_density = %s,
                sniper_density_delta = %s,
                active_rising = %s,
                entry_allowed = %s,
                block_reason = %s
            WHERE id = %s
        """, (
            regime_state,
            sniper_now,
            sniper_delta,
            active_rising,
            entry_allowed,
            block_reason,
            signal_id
        ))

        # ================= REAL ENTRY EXECUTION =================
        if decision in ["LONG", "SHORT"]:
            if entry_allowed:
                trade_id = open_trade(
                    cur,
                    symbol,
                    decision,
                    price,
                    momentum,
                    trend,
                    live_entry_quality,
                    regime_state,
                    signal_id,
                    signal_time,
                    is_shadow=False
                )

                print(
                    f"🚀 OPEN REAL | {symbol} | {decision} | id={trade_id} | "
                    f"Q={live_entry_quality} | regime={regime_state} | "
                    f"density={sniper_now} | delta={sniper_delta}",
                    flush=True
                )

            else:
                print(
                    f"⛔ BLOCKED | {symbol} | {decision} | "
                    f"mom={round(momentum,3)} trend={round(trend,3)} | "
                    f"regime={regime_state} | density={sniper_now} | "
                    f"delta={sniper_delta} | reason={block_reason}",
                    flush=True
                )

        # ================= SHADOW ENTRIES =================
        if decision == "LONG":
            cur.execute("""
                SELECT COUNT(*)
                FROM bot_trades_v4
                WHERE status = 'OPEN'
                  AND COALESCE(is_shadow, FALSE) = TRUE
            """)
            open_shadow_count = cur.fetchone()[0] or 0

            if open_shadow_count < MAX_OPEN_SHADOW_TRADES:

                if ENABLE_SHADOW_V6 and passes_v6_shadow(momentum, trend, sniper_now, sniper_delta):
                    shadow_id = open_trade(
                        cur,
                        symbol,
                        "LONG",
                        price,
                        momentum,
                        trend,
                        "SHADOW_V6_COMPRESSION",
                        regime_state,
                        signal_id,
                        signal_time,
                        is_shadow=True
                    )
                    print(f"👻 OPEN SHADOW V6 | {symbol} | id={shadow_id}", flush=True)

                if ENABLE_SHADOW_V7 and passes_v7_shadow(momentum, trend):
                    shadow_id = open_trade(
                        cur,
                        symbol,
                        "LONG",
                        price,
                        momentum,
                        trend,
                        "SHADOW_V7_BROAD",
                        regime_state,
                        signal_id,
                        signal_time,
                        is_shadow=True
                    )
                    print(f"👻 OPEN SHADOW V7 | {symbol} | id={shadow_id}", flush=True)

        # ================= EXIT ENGINE =================
        cur.execute("""
            SELECT
                id,
                symbol,
                direction,
                entry_price,
                opened_at,
                peak_pnl_percent,
                COALESCE(is_shadow, FALSE) AS is_shadow,
                entry_quality
            FROM bot_trades_v4
            WHERE status = 'OPEN'
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, entry_quality) in open_trades:

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

            update_count = get_update_count(cur, tid)

            close_reason = None

            # U2 CONFIRMATION EXIT
            if ENABLE_CONFIRMATION_EXIT:
                if update_count == CONFIRMATION_UPDATE_NUM and pnl_percent < CONFIRMATION_MIN_PNL:
                    close_reason = "failed_confirmation"

            # LONG EXIT MODEL
            if not close_reason and direction == "LONG":

                if ENABLE_PROFIT_LOCKS:
                    if current_peak >= LONG_LOCK_4_TRIGGER:
                        lock_floor = current_peak * LONG_LOCK_4_RATIO
                        if pnl_percent <= lock_floor:
                            close_reason = "long_lock_4"

                    elif current_peak >= LONG_LOCK_3_TRIGGER:
                        lock_floor = current_peak * LONG_LOCK_3_RATIO
                        if pnl_percent <= lock_floor:
                            close_reason = "long_lock_3"

                    elif current_peak >= LONG_LOCK_2_TRIGGER:
                        lock_floor = current_peak * LONG_LOCK_2_RATIO
                        if pnl_percent <= lock_floor:
                            close_reason = "long_lock_2"

                    elif current_peak >= LONG_LOCK_1_TRIGGER:
                        lock_floor = current_peak * LONG_LOCK_1_RATIO
                        if pnl_percent <= lock_floor:
                            close_reason = "long_lock_1"

                if not close_reason and current_peak >= LONG_NO_RED_AFTER_WIN_TRIGGER and pnl_percent < 0:
                    close_reason = "long_gave_back_winner"

                if not close_reason and pnl_percent <= LONG_HARD_STOP:
                    close_reason = "long_hard_stop"

            # SHORT EXIT MODEL
            if not close_reason and direction == "SHORT":

                if current_peak >= SHORT_TP_1_TRIGGER and pnl_percent <= SHORT_TP_1_FLOOR:
                    close_reason = "short_tp_1_lock"

                elif current_peak >= SHORT_TP_05_TRIGGER and pnl_percent <= SHORT_TP_05_FLOOR:
                    close_reason = "short_tp_0_5_lock"

                elif pnl_percent <= SHORT_HARD_STOP:
                    close_reason = "short_hard_stop"

            if close_reason:
                pnl_gbp = 0 if is_shadow else (pnl_percent / 100) * TRADE_SIZE_GBP

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

                trade_type = "SHADOW" if is_shadow else "REAL"

                print(
                    f"💰 CLOSED {trade_type} | {sym} | {direction} | "
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
