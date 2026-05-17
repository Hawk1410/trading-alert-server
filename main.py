# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v6.0
# TITLE: LEADERSHIP-PERSISTENCE MAIN ENGINE + DYNAMIC AGGRESSION SIZING + ADAPTIVE LIFECYCLE + SHADOW RESEARCH ENGINES
# =========================

print("🔥🔥🔥 MAIN.PY v6.0 LEADERSHIP PERSISTENCE ENGINE RUNNING 🔥🔥🔥", flush=True)

# =========================
# v6.0 CHANGE SUMMARY
# =========================
#
# CORE CHANGES ONLY — NO UNNECESSARY REWRITES
#
# ✅ Leadership-gated continuation promoted to PRIMARY engine
# ✅ prior_avg_peak now determines whether continuation is tradable
# ✅ Dynamic sizing tiers added
# ✅ MAX_SAME_SYMBOL_OPEN increased from 1 → 2
# ✅ Existing adaptive lifecycle exits retained
# ✅ Existing Telegram/OKX infra retained
# ✅ Shadow engines retained
#
# REMOVED AS PRIMARY CONCEPT:
# ❌ Broad ungated V7 continuation
# ❌ Monster override architecture
#
# NEW CORE THESIS:
# Trade continuation ONLY during proven leadership persistence.
#
# =========================

from flask import Flask, request, jsonify
import os
import json
import hmac
import base64
import hashlib
import requests
import psycopg2
from datetime import datetime, timezone, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# CORE ENGINE SETTINGS
# =========================

MAX_OPEN_TRADES = 5
MAX_OPEN_SHADOW_TRADES = 30

DATA_VERSION = "v6.0"

# Leadership stacking breakthrough:
# 1 was too restrictive
# 3 degraded quality
# 2 was optimal balance.
MAX_SAME_SYMBOL_OPEN = int(
    os.environ.get("MAX_SAME_SYMBOL_OPEN", "2") or 2
)

ENABLE_SAME_SYMBOL_STACKING_LIMIT = (
    os.environ.get("ENABLE_SAME_SYMBOL_STACKING_LIMIT", "true").lower() == "true"
)

# =========================
# 🧠 LEADERSHIP ENGINE
# =========================

ENABLE_LEADERSHIP_ENGINE = True

LEADERSHIP_LOOKBACK_MINUTES = int(
    os.environ.get("LEADERSHIP_LOOKBACK_MINUTES", "120") or 120
)

LEADERSHIP_MIN_TREND = float(
    os.environ.get("LEADERSHIP_MIN_TREND", "0.20") or 0.20
)

LEADERSHIP_MIN_MOMENTUM = float(
    os.environ.get("LEADERSHIP_MIN_MOMENTUM", "0.00") or 0.00
)

# Core validated threshold.
LEADERSHIP_MIN_PRIOR_AVG_PEAK = float(
    os.environ.get("LEADERSHIP_MIN_PRIOR_AVG_PEAK", "1.25") or 1.25
)

# =========================
# 💰 DYNAMIC AGGRESSION SIZING
# =========================

CORE_TRADE_SIZE_GBP = float(
    os.environ.get("CORE_TRADE_SIZE_GBP", "10") or 10
)

AGGRESSIVE_TRADE_SIZE_GBP = float(
    os.environ.get("AGGRESSIVE_TRADE_SIZE_GBP", "18") or 18
)

MONSTER_TRADE_SIZE_GBP = float(
    os.environ.get("MONSTER_TRADE_SIZE_GBP", "30") or 30
)

# Leadership tiers discovered in sweep testing.

CORE_MIN_PRIOR_PEAK = 1.25
AGGRESSIVE_MIN_PRIOR_PEAK = 1.50
MONSTER_MIN_PRIOR_PEAK = 2.00

# =========================
# 🔌 OKX EXECUTION SETTINGS
# =========================

OKX_API_KEY = os.environ.get("OKX_API_KEY")
OKX_API_SECRET = os.environ.get("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.environ.get("OKX_API_PASSPHRASE")

OKX_BASE_URL = os.environ.get(
    "OKX_BASE_URL",
    "https://www.okx.com"
).rstrip("/")

ENABLE_ORDER_LOGGING = (
    os.environ.get("ENABLE_ORDER_LOGGING", "true").lower() == "true"
)

ENABLE_LIVE_ORDERS = (
    os.environ.get("ENABLE_LIVE_ORDERS", "false").lower() == "true"
)

MAX_LIVE_OPEN_TRADES = int(
    os.environ.get("MAX_LIVE_OPEN_TRADES", "8") or 8
)

OKX_TD_MODE = os.environ.get("OKX_TD_MODE", "cash")
OKX_ORDER_TYPE = os.environ.get("OKX_ORDER_TYPE", "market")

OKX_SPOT_TAKER_FEE_RATE = float(
    os.environ.get("OKX_SPOT_TAKER_FEE_RATE", "0.001") or 0.001
)

OKX_EXIT_SIZE_BUFFER = float(
    os.environ.get("OKX_EXIT_SIZE_BUFFER", "0.995") or 0.995
)

# =========================
# 📲 TELEGRAM SETTINGS
# =========================

ENABLE_TELEGRAM_ALERTS = (
    os.environ.get("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"
)

TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_COMMAND_SECRET = os.environ.get("TELEGRAM_COMMAND_SECRET")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET")

ENABLE_TELEGRAM_COMMANDS = (
    os.environ.get("ENABLE_TELEGRAM_COMMANDS", "true").lower() == "true"
)

# =========================
# 👻 SHADOW ENGINES RETAINED
# =========================

ENABLE_SHADOW_V6 = True
ENABLE_SHADOW_V53_SNIPER = True
ENABLE_SHADOW_MONSTER_RECURSION = True
ENABLE_SHADOW_S5_SHORT_ENGINE = True

# =========================
# ♻️ ADAPTIVE LIFECYCLE
# =========================

ENABLE_DEAD_LEADER_RECYCLER = True
DEAD_LEADER_MINUTES = 90
DEAD_LEADER_MAX_PEAK = 0.25
DEAD_LEADER_TREND_THRESHOLD = 0.30

ENABLE_ADAPTIVE_WINNER_PROTECTION = True

ADAPTIVE_SMALL_PEAK_TRIGGER = 0.75
ADAPTIVE_SMALL_DRAWDOWN = 0.25

ADAPTIVE_MEDIUM_PEAK_TRIGGER = 1.50
ADAPTIVE_MEDIUM_DRAWDOWN = 0.40

ADAPTIVE_LARGE_PEAK_TRIGGER = 3.00
ADAPTIVE_LARGE_DRAWDOWN = 0.75

ADAPTIVE_TREND_WEAK_THRESHOLD = 0.15

# =========================
# 🚪 EXIT SETTINGS
# =========================

ENABLE_PROFIT_LOCKS = True

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
# LEADERSHIP CONTEXT
# =========================

def get_leadership_context(cur, symbol):

    cur.execute("""
        SELECT
            COUNT(*) FILTER (
                WHERE COALESCE(peak_pnl_percent, 0) >= 0.75
            ) AS prior_successes,

            COUNT(*) FILTER (
                WHERE COALESCE(peak_pnl_percent, 0) >= 2.00
            ) AS prior_runners,

            AVG(COALESCE(peak_pnl_percent, 0)) AS prior_avg_peak

        FROM bot_trades_v4
        WHERE symbol = %s
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND opened_at >= NOW() - (%s || ' minutes')::INTERVAL
          AND opened_at < NOW()
    """, (symbol, LEADERSHIP_LOOKBACK_MINUTES))

    row = cur.fetchone() or (0, 0, 0)

    return {
        "prior_successes": row[0] or 0,
        "prior_runners": row[1] or 0,
        "prior_avg_peak": float(row[2] or 0)
    }

# =========================
# LEADERSHIP ENGINE
# =========================

def passes_leadership_engine(cur, symbol, momentum, trend):

    if not ENABLE_LEADERSHIP_ENGINE:
        return False, None

    if trend < LEADERSHIP_MIN_TREND:
        return False, None

    if momentum <= LEADERSHIP_MIN_MOMENTUM:
        return False, None

    leadership = get_leadership_context(cur, symbol)

    prior_avg_peak = leadership["prior_avg_peak"]

    if prior_avg_peak < LEADERSHIP_MIN_PRIOR_AVG_PEAK:
        return False, leadership

    return True, leadership

# =========================
# DYNAMIC SIZING
# =========================

def classify_leadership_tier(prior_avg_peak):

    if prior_avg_peak >= MONSTER_MIN_PRIOR_PEAK:
        return "LEADERSHIP_MONSTER"

    if prior_avg_peak >= AGGRESSIVE_MIN_PRIOR_PEAK:
        return "LEADERSHIP_AGGRESSIVE"

    return "LEADERSHIP_CORE"

def get_trade_size_for_quality(entry_quality):

    if entry_quality == "LEADERSHIP_MONSTER":
        return MONSTER_TRADE_SIZE_GBP

    if entry_quality == "LEADERSHIP_AGGRESSIVE":
        return AGGRESSIVE_TRADE_SIZE_GBP

    return CORE_TRADE_SIZE_GBP

# =========================
# HELPERS
# =========================

def fmt_num(value, digits=3):
    try:
        return round(float(value), digits)
    except Exception:
        return value

def fmt_money(value):
    try:
        return f"£{float(value):.2f}"
    except Exception:
        return f"£{value}"

def send_telegram_alert(message):

    if not ENABLE_TELEGRAM_ALERTS:
        return False

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        payload = {
            "chat_id": str(TELEGRAM_CHAT_ID),
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        response = requests.post(url, json=payload, timeout=8)

        return response.status_code == 200

    except Exception:
        return False

# =========================
# OPEN TRADE
# =========================

def open_trade(
    cur,
    symbol,
    direction,
    price,
    momentum,
    trend,
    quality,
    signal_id,
    signal_time,
    leadership_context
):

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
            peak_pnl_percent,
            signal_id,
            signal_timestamp,
            leadership_prior_successes,
            leadership_prior_runners,
            leadership_prior_avg_peak
        )
        VALUES (
            %s,%s,%s,
            'OPEN',
            NOW(),
            %s,%s,%s,%s,
            0,
            %s,%s,
            %s,%s,%s
        )
        RETURNING id
    """, (
        symbol,
        direction,
        price,
        DATA_VERSION,
        momentum,
        trend,
        quality,
        signal_id,
        signal_time,
        leadership_context["prior_successes"],
        leadership_context["prior_runners"],
        leadership_context["prior_avg_peak"]
    ))

    return cur.fetchone()[0]

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

        decision = (
            data.get("decision_model") or ""
        ).upper()

        momentum = float(data.get("momentum_strength") or 0)

        trend = float(data.get("trend_strength") or 0)

        now = datetime.utcnow()

        conn = get_db()
        cur = conn.cursor()

        # =========================
        # RAW SIGNAL STORAGE
        # =========================

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

        # =========================
        # ENTRY ENGINE
        # =========================

        entry_allowed = False
        leadership_context = None
        entry_quality = None
        block_reason = None

        if decision == "LONG":

            entry_allowed, leadership_context = passes_leadership_engine(
                cur,
                symbol,
                momentum,
                trend
            )

            if entry_allowed:

                entry_quality = classify_leadership_tier(
                    leadership_context["prior_avg_peak"]
                )

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

                if entry_allowed and ENABLE_SAME_SYMBOL_STACKING_LIMIT:

                    cur.execute("""
                        SELECT COUNT(*)
                        FROM bot_trades_v4
                        WHERE status = 'OPEN'
                          AND COALESCE(is_shadow, FALSE) = FALSE
                          AND symbol = %s
                    """, (symbol,))

                    same_symbol_count = cur.fetchone()[0] or 0

                    if same_symbol_count >= MAX_SAME_SYMBOL_OPEN:
                        entry_allowed = False
                        block_reason = "max_same_symbol_open"

            else:
                block_reason = "leadership_gate_failed"

        # =========================
        # ENTRY EXECUTION
        # =========================

        if entry_allowed:

            trade_id = open_trade(
                cur,
                symbol,
                "LONG",
                price,
                momentum,
                trend,
                entry_quality,
                signal_id,
                signal_time,
                leadership_context
            )

            trade_size = get_trade_size_for_quality(entry_quality)

            print(
                f"🚀 OPEN REAL | {symbol} | "
                f"{entry_quality} | "
                f"size=£{trade_size} | "
                f"prior_avg_peak={round(leadership_context['prior_avg_peak'],3)}",
                flush=True
            )

            send_telegram_alert(
                f"🚀 <b>LEADERSHIP ENTRY</b>\n"
                f"{symbol}\n"
                f"Tier: {entry_quality}\n"
                f"Size: £{trade_size}\n"
                f"Trend: {fmt_num(trend)}\n"
                f"Momentum: {fmt_num(momentum)}\n"
                f"Prior avg peak: {fmt_num(leadership_context['prior_avg_peak'])}%\n"
                f"Prior runners: {leadership_context['prior_runners']}\n"
                f"Trade ID: {trade_id}"
            )

        else:

            print(
                f"⛔ BLOCKED | {symbol} | "
                f"reason={block_reason}",
                flush=True
            )

        # =========================
        # EXIT ENGINE
        # =========================

        cur.execute("""
            SELECT
                id,
                symbol,
                direction,
                entry_price,
                opened_at,
                peak_pnl_percent,
                entry_quality
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = FALSE
        """)

        open_trades = cur.fetchall()

        for (
            tid,
            sym,
            direction,
            entry_price,
            opened_at,
            peak_pnl,
            entry_quality
        ) in open_trades:

            if sym != symbol:
                continue

            pnl = ((price - entry_price) / entry_price)

            pnl_percent = pnl * 100

            mins = (now - opened_at).total_seconds() / 60

            current_peak = peak_pnl or 0

            if pnl_percent > current_peak:

                current_peak = pnl_percent

                cur.execute("""
                    UPDATE bot_trades_v4
                    SET peak_pnl_percent = %s
                    WHERE id = %s
                """, (
                    current_peak,
                    tid
                ))

            close_reason = None

            drawdown_from_peak = current_peak - pnl_percent

            # =========================
            # DEAD LEADER RECYCLER
            # =========================

            if (
                mins >= DEAD_LEADER_MINUTES
                and current_peak < DEAD_LEADER_MAX_PEAK
                and trend < DEAD_LEADER_TREND_THRESHOLD
            ):
                close_reason = "dead_leader_recycle_exit"

            # =========================
            # ADAPTIVE WINNER PROTECTION
            # =========================

            if not close_reason:

                if (
                    current_peak >= ADAPTIVE_LARGE_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_LARGE_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_large"

                elif (
                    current_peak >= ADAPTIVE_MEDIUM_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_MEDIUM_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_medium"

                elif (
                    current_peak >= ADAPTIVE_SMALL_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_SMALL_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_small"

            # =========================
            # PROFIT LOCKS
            # =========================

            if not close_reason:

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

            # =========================
            # HARD STOP
            # =========================

            if not close_reason and pnl_percent <= LONG_HARD_STOP:
                close_reason = "long_hard_stop"

            # =========================
            # CLOSE TRADE
            # =========================

            if close_reason:

                trade_size = get_trade_size_for_quality(entry_quality)

                pnl_gbp = (
                    pnl_percent / 100
                ) * trade_size

                cur.execute("""
                    UPDATE bot_trades_v4
                    SET
                        status = 'CLOSED',
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

                print(
                    f"💰 CLOSED | {sym} | "
                    f"{round(pnl_percent,3)}% | "
                    f"peak={round(current_peak,3)} | "
                    f"{close_reason}",
                    flush=True
                )

                send_telegram_alert(
                    f"💰 <b>CLOSED</b>\n"
                    f"{sym}\n"
                    f"PnL: {fmt_num(pnl_percent)}%\n"
                    f"Peak: {fmt_num(current_peak)}%\n"
                    f"Reason: {close_reason}"
                )

        conn.commit()

        cur.close()
        conn.close()

        return jsonify({
            "status": "ok",
            "version": DATA_VERSION
        }), 200

    except Exception as e:

        print("❌ ERROR:", e, flush=True)

        return jsonify({
            "error": str(e)
        }), 400

# =========================
# HEALTH
# =========================

@app.route("/", methods=["GET"])
def home():

    return jsonify({
        "status": "running",
        "version": DATA_VERSION,
        "engine": "LEADERSHIP_PERSISTENCE"
    }), 200
