# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v6.1.4
# TITLE: STABLE LEADERSHIP PHASE ENGINE + OKX PRE-ENTRY TRADABILITY + TELEGRAM OPS
# =========================

print("🔥🔥🔥 MAIN.PY v6.1.4 STABLE LEADERSHIP PHASE ENGINE RUNNING 🔥🔥🔥", flush=True)

# =========================
# v6.1 CHANGE SUMMARY
# =========================
#
# ✅ Keeps v6.0 leadership-persistence main engine
# ✅ Adds rolling signal_leadership_scores table support
# ✅ v6.1.1 changes leadership lookback default from 120m → 240m after rolling-window sweep
# ✅ v6.1.2 adds leadership_state_history snapshots on every scorer cron run
# ✅ v6.1.3 hardens trade-size sync and improves Telegram operator visibility
# ✅ v6.1.4 switches entries to stable-leadership phase gating and pre-entry OKX tradability filtering
# ✅ Leadership context now uses scored historical signals, not only prior real trades
# ✅ Restores OKX live/dry-run execution layer for entries and exits
# ✅ Keeps dynamic sizing:
#      1.25–1.49 = CORE £10
#      1.50–1.99 = AGGRESSIVE £18
#      2.00+     = MONSTER £30
# ✅ Keeps max_open_trades = 5
# ✅ Keeps max_same_symbol_open = 2
# ✅ Keeps adaptive lifecycle exits
#
# IMPORTANT:
# Create signal_leadership_scores table before/after deployment using the SQL provided.
# Call /score_signal_leadership every 5 minutes via cron.
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
from datetime import datetime, timezone

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# CORE ENGINE SETTINGS
# =========================

MAX_OPEN_TRADES = int(os.environ.get("MAX_OPEN_TRADES", "5") or 5)
MAX_OPEN_SHADOW_TRADES = int(os.environ.get("MAX_OPEN_SHADOW_TRADES", "30") or 30)

DATA_VERSION = "v6.1.4"

MAX_SAME_SYMBOL_OPEN = int(os.environ.get("MAX_SAME_SYMBOL_OPEN", "1") or 1)
ENABLE_SAME_SYMBOL_STACKING_LIMIT = os.environ.get("ENABLE_SAME_SYMBOL_STACKING_LIMIT", "true").lower() == "true"

# =========================
# 🧠 LEADERSHIP ENGINE
# =========================

ENABLE_LEADERSHIP_ENGINE = os.environ.get("ENABLE_LEADERSHIP_ENGINE", "true").lower() == "true"

LEADERSHIP_LOOKBACK_MINUTES = int(os.environ.get("LEADERSHIP_LOOKBACK_MINUTES", "240") or 240)
LEADERSHIP_SIGNAL_FORWARD_MINUTES = int(os.environ.get("LEADERSHIP_SIGNAL_FORWARD_MINUTES", "120") or 120)
LEADERSHIP_SCORER_LIMIT = int(os.environ.get("LEADERSHIP_SCORER_LIMIT", "500") or 500)

LEADERSHIP_MIN_TREND = float(os.environ.get("LEADERSHIP_MIN_TREND", "0.20") or 0.20)
LEADERSHIP_MIN_MOMENTUM = float(os.environ.get("LEADERSHIP_MIN_MOMENTUM", "0.00") or 0.00)
LEADERSHIP_MIN_PRIOR_AVG_PEAK = float(os.environ.get("LEADERSHIP_MIN_PRIOR_AVG_PEAK", "1.25") or 1.25)

# v6.1.4 phase gating:
# Live data showed explosive score acceleration was climax/exhaustion.
# Preferred entry state is stable dominant leadership.
ENABLE_STABLE_LEADERSHIP_PHASE_FILTER = os.environ.get(
    "ENABLE_STABLE_LEADERSHIP_PHASE_FILTER",
    "true"
).lower() == "true"

STABLE_LEADER_MIN_SCORE = float(os.environ.get("STABLE_LEADER_MIN_SCORE", "2.0") or 2.0)
STABLE_LEADER_DELTA_MIN = float(os.environ.get("STABLE_LEADER_DELTA_MIN", "-0.20") or -0.20)
STABLE_LEADER_DELTA_MAX = float(os.environ.get("STABLE_LEADER_DELTA_MAX", "0.20") or 0.20)

# Explicitly avoid late-stage blowoff/climax leadership.
CLIMAX_LEADER_DELTA_BLOCK = float(os.environ.get("CLIMAX_LEADER_DELTA_BLOCK", "1.0") or 1.0)

# Keep emerging-leader live trading off until larger samples validate it.
ENABLE_EMERGING_LEADER_ENTRIES = os.environ.get(
    "ENABLE_EMERGING_LEADER_ENTRIES",
    "false"
).lower() == "true"
EMERGING_LEADER_MIN_SCORE = float(os.environ.get("EMERGING_LEADER_MIN_SCORE", "1.50") or 1.50)
EMERGING_LEADER_MAX_SCORE = float(os.environ.get("EMERGING_LEADER_MAX_SCORE", "2.00") or 2.00)
EMERGING_LEADER_DELTA_MIN = float(os.environ.get("EMERGING_LEADER_DELTA_MIN", "0.20") or 0.20)
EMERGING_LEADER_DELTA_MAX = float(os.environ.get("EMERGING_LEADER_DELTA_MAX", "0.50") or 0.50)


# Dynamic sizing tiers.
CORE_TRADE_SIZE_GBP = float(os.environ.get("CORE_TRADE_SIZE_GBP", "10") or 10)
AGGRESSIVE_TRADE_SIZE_GBP = float(os.environ.get("AGGRESSIVE_TRADE_SIZE_GBP", "18") or 18)
MONSTER_TRADE_SIZE_GBP = float(os.environ.get("MONSTER_TRADE_SIZE_GBP", "30") or 30)

CORE_MIN_PRIOR_PEAK = 1.25
AGGRESSIVE_MIN_PRIOR_PEAK = 1.50
MONSTER_MIN_PRIOR_PEAK = 2.00

# =========================
# 🔌 OKX EXECUTION SETTINGS
# =========================

OKX_API_KEY = os.environ.get("OKX_API_KEY")
OKX_API_SECRET = os.environ.get("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.environ.get("OKX_API_PASSPHRASE")
OKX_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")

ENABLE_ORDER_LOGGING = os.environ.get("ENABLE_ORDER_LOGGING", "true").lower() == "true"
ENABLE_LIVE_ORDERS = os.environ.get("ENABLE_LIVE_ORDERS", "false").lower() == "true"

MAX_LIVE_OPEN_TRADES = int(os.environ.get("MAX_LIVE_OPEN_TRADES", "5") or 5)

OKX_TD_MODE = os.environ.get("OKX_TD_MODE", "cash")
OKX_ORDER_TYPE = os.environ.get("OKX_ORDER_TYPE", "market")
OKX_EXIT_SIZE_BUFFER = float(os.environ.get("OKX_EXIT_SIZE_BUFFER", "0.995") or 0.995)

ENABLE_OKX_TRADABILITY_FILTER = os.environ.get("ENABLE_OKX_TRADABILITY_FILTER", "true").lower() == "true"
OKX_TRADABILITY_CACHE_SECONDS = int(os.environ.get("OKX_TRADABILITY_CACHE_SECONDS", "900") or 900)

OKX_TRADABLE_SPOT_INST_IDS = set()
OKX_TRADABILITY_CACHE_UPDATED_AT = None
OKX_TRADABILITY_LAST_ERROR = None

# =========================
# 📲 TELEGRAM SETTINGS
# =========================

ENABLE_TELEGRAM_ALERTS = os.environ.get("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_COMMAND_SECRET = os.environ.get("TELEGRAM_COMMAND_SECRET")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET")
ENABLE_TELEGRAM_COMMANDS = os.environ.get("ENABLE_TELEGRAM_COMMANDS", "true").lower() == "true"

# =========================
# ♻️ ADAPTIVE LIFECYCLE
# =========================

ENABLE_DEAD_LEADER_RECYCLER = os.environ.get("ENABLE_DEAD_LEADER_RECYCLER", "true").lower() == "true"
DEAD_LEADER_MINUTES = float(os.environ.get("DEAD_LEADER_MINUTES", "90") or 90)
DEAD_LEADER_MAX_PEAK = float(os.environ.get("DEAD_LEADER_MAX_PEAK", "0.25") or 0.25)
DEAD_LEADER_TREND_THRESHOLD = float(os.environ.get("DEAD_LEADER_TREND_THRESHOLD", "0.30") or 0.30)

ENABLE_ADAPTIVE_WINNER_PROTECTION = os.environ.get("ENABLE_ADAPTIVE_WINNER_PROTECTION", "true").lower() == "true"

ADAPTIVE_SMALL_PEAK_TRIGGER = float(os.environ.get("ADAPTIVE_SMALL_PEAK_TRIGGER", "0.75") or 0.75)
ADAPTIVE_SMALL_DRAWDOWN = float(os.environ.get("ADAPTIVE_SMALL_DRAWDOWN", "0.25") or 0.25)

ADAPTIVE_MEDIUM_PEAK_TRIGGER = float(os.environ.get("ADAPTIVE_MEDIUM_PEAK_TRIGGER", "1.50") or 1.50)
ADAPTIVE_MEDIUM_DRAWDOWN = float(os.environ.get("ADAPTIVE_MEDIUM_DRAWDOWN", "0.40") or 0.40)

ADAPTIVE_LARGE_PEAK_TRIGGER = float(os.environ.get("ADAPTIVE_LARGE_PEAK_TRIGGER", "3.00") or 3.00)
ADAPTIVE_LARGE_DRAWDOWN = float(os.environ.get("ADAPTIVE_LARGE_DRAWDOWN", "0.75") or 0.75)

ADAPTIVE_TREND_WEAK_THRESHOLD = float(os.environ.get("ADAPTIVE_TREND_WEAK_THRESHOLD", "0.15") or 0.15)

# =========================
# 🚪 EXIT SETTINGS
# =========================

ENABLE_PROFIT_LOCKS = os.environ.get("ENABLE_PROFIT_LOCKS", "true").lower() == "true"

LONG_LOCK_1_TRIGGER = float(os.environ.get("LONG_LOCK_1_TRIGGER", "0.75") or 0.75)
LONG_LOCK_1_RATIO = float(os.environ.get("LONG_LOCK_1_RATIO", "0.50") or 0.50)

LONG_LOCK_2_TRIGGER = float(os.environ.get("LONG_LOCK_2_TRIGGER", "1.50") or 1.50)
LONG_LOCK_2_RATIO = float(os.environ.get("LONG_LOCK_2_RATIO", "0.70") or 0.70)

LONG_LOCK_3_TRIGGER = float(os.environ.get("LONG_LOCK_3_TRIGGER", "3.00") or 3.00)
LONG_LOCK_3_RATIO = float(os.environ.get("LONG_LOCK_3_RATIO", "0.75") or 0.75)

LONG_LOCK_4_TRIGGER = float(os.environ.get("LONG_LOCK_4_TRIGGER", "5.00") or 5.00)
LONG_LOCK_4_RATIO = float(os.environ.get("LONG_LOCK_4_RATIO", "0.80") or 0.80)

LONG_HARD_STOP = float(os.environ.get("LONG_HARD_STOP", "-0.40") or -0.40)
LONG_NO_RED_AFTER_WIN_TRIGGER = float(os.environ.get("LONG_NO_RED_AFTER_WIN_TRIGGER", "0.75") or 0.75)

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

def safe_update_trade_telemetry(cur, tid, telemetry):
    allowed = {}
    for col, val in telemetry.items():
        if column_exists(cur, "bot_trades_v4", col):
            allowed[col] = val

    if not allowed:
        return

    set_sql = ", ".join([f"{col} = %s" for col in allowed.keys()])
    values = list(allowed.values()) + [tid]

    cur.execute(f"""
        UPDATE bot_trades_v4
        SET {set_sql}
        WHERE id = %s
    """, values)

def ensure_okx_order_log_table(cur):
    if not ENABLE_ORDER_LOGGING:
        return

    cur.execute("""
        CREATE TABLE IF NOT EXISTS okx_order_log (
            id BIGSERIAL PRIMARY KEY,
            created_at TIMESTAMPTZ DEFAULT NOW(),
            trade_id TEXT,
            symbol TEXT,
            okx_inst_id TEXT,
            action TEXT,
            side TEXT,
            direction TEXT,
            dry_run BOOLEAN,
            live_orders_enabled BOOLEAN,
            request_payload JSONB,
            response_payload JSONB,
            success BOOLEAN,
            error_message TEXT
        )
    """)

def ensure_signal_leadership_scores_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS signal_leadership_scores (
            id BIGSERIAL PRIMARY KEY,
            signal_id BIGINT UNIQUE,
            symbol TEXT,
            signal_timestamp TIMESTAMPTZ,
            price NUMERIC,
            momentum NUMERIC,
            trend NUMERIC,
            future_max_price NUMERIC,
            future_min_price NUMERIC,
            future_peak_percent NUMERIC,
            future_worst_percent NUMERIC,
            scored_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_signal_leadership_symbol_time
        ON signal_leadership_scores(symbol, signal_timestamp)
    """)


def ensure_leadership_state_history_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS leadership_state_history (
            id BIGSERIAL PRIMARY KEY,
            snapshot_time TIMESTAMPTZ DEFAULT NOW(),
            symbol TEXT,
            leadership_score NUMERIC,
            successful_signals INTEGER,
            runners INTEGER,
            monsters INTEGER,
            avg_peak NUMERIC,
            avg_worst NUMERIC,
            tradable BOOLEAN,
            leadership_mode TEXT
        )
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_leadership_state_history_symbol_time
        ON leadership_state_history(symbol, snapshot_time)
    """)

    cur.execute("""
        CREATE INDEX IF NOT EXISTS idx_leadership_state_history_snapshot_time
        ON leadership_state_history(snapshot_time)
    """)

# =========================
# TELEGRAM HELPERS
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

def telegram_send_message(chat_id, message):
    if not ENABLE_TELEGRAM_ALERTS:
        return False

    if not TELEGRAM_BOT_TOKEN or not chat_id:
        print("⚠️ TELEGRAM SEND SKIPPED | missing token/chat_id", flush=True)
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": str(chat_id),
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        response = requests.post(url, json=payload, timeout=8)
        if response.status_code == 200:
            return True
        print(f"⚠️ TELEGRAM SEND FAILED | {response.status_code} | {response.text}", flush=True)
        return False
    except Exception as e:
        print(f"⚠️ TELEGRAM SEND ERROR | {e}", flush=True)
        return False

def send_telegram_alert(message):
    return telegram_send_message(TELEGRAM_CHAT_ID, message)

# =========================
# OKX HELPERS
# =========================

def okx_symbol_to_inst_id(symbol):
    s = (symbol or "").upper().replace("-", "").replace("/", "")
    quote_assets = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP"]
    for quote in quote_assets:
        if s.endswith(quote) and len(s) > len(quote):
            base = s[:-len(quote)]
            return f"{base}-{quote}"
    return symbol

def okx_inst_id_to_base_ccy(okx_inst_id):
    if not okx_inst_id or "-" not in okx_inst_id:
        return None
    return okx_inst_id.split("-")[0].upper()

def get_okx_timestamp():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")

def okx_sign(timestamp, method, request_path, body=""):
    message = f"{timestamp}{method.upper()}{request_path}{body}"
    mac = hmac.new(
        bytes(OKX_API_SECRET, encoding="utf-8"),
        bytes(message, encoding="utf-8"),
        digestmod=hashlib.sha256
    )
    return base64.b64encode(mac.digest()).decode()

def okx_headers(method, request_path, body=""):
    timestamp = get_okx_timestamp()
    sign = okx_sign(timestamp, method, request_path, body)
    return {
        "OK-ACCESS-KEY": OKX_API_KEY,
        "OK-ACCESS-SIGN": sign,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": OKX_API_PASSPHRASE,
        "Content-Type": "application/json",
    }

def okx_api_ready():
    return bool(OKX_API_KEY and OKX_API_SECRET and OKX_API_PASSPHRASE and OKX_BASE_URL)

def okx_authenticated_get(request_path_with_query):
    if not okx_api_ready():
        return {"success": False, "error": "OKX API credentials missing or incomplete", "response": None}

    try:
        headers = okx_headers("GET", request_path_with_query, "")
        response = requests.get(f"{OKX_BASE_URL}{request_path_with_query}", headers=headers, timeout=10)

        try:
            response_payload = response.json()
        except Exception:
            response_payload = {"status_code": response.status_code, "text": response.text}

        okx_code = str(response_payload.get("code")) if isinstance(response_payload, dict) else None
        success = response.status_code == 200 and okx_code == "0"

        return {
            "success": success,
            "status_code": response.status_code,
            "response": response_payload,
            "error": None if success else f"OKX GET failed: {response_payload}"
        }

    except Exception as e:
        return {"success": False, "status_code": None, "response": None, "error": str(e)}

def refresh_okx_tradable_spot_instruments(force=False):
    global OKX_TRADABLE_SPOT_INST_IDS
    global OKX_TRADABILITY_CACHE_UPDATED_AT
    global OKX_TRADABILITY_LAST_ERROR

    now_utc = datetime.now(timezone.utc)

    if (
        not force
        and OKX_TRADABILITY_CACHE_UPDATED_AT is not None
        and (now_utc - OKX_TRADABILITY_CACHE_UPDATED_AT).total_seconds() < OKX_TRADABILITY_CACHE_SECONDS
        and OKX_TRADABLE_SPOT_INST_IDS
    ):
        return {
            "success": True,
            "cached": True,
            "count": len(OKX_TRADABLE_SPOT_INST_IDS),
            "inst_ids": sorted(list(OKX_TRADABLE_SPOT_INST_IDS)),
            "error": None
        }

    result = okx_authenticated_get("/api/v5/account/instruments?instType=SPOT")

    if not result.get("success"):
        OKX_TRADABILITY_LAST_ERROR = result.get("error")
        return {
            "success": False,
            "cached": False,
            "count": len(OKX_TRADABLE_SPOT_INST_IDS),
            "inst_ids": sorted(list(OKX_TRADABLE_SPOT_INST_IDS)),
            "error": result.get("error"),
            "response": result.get("response")
        }

    response_payload = result.get("response") or {}
    data = response_payload.get("data") or []

    inst_ids = set()
    for item in data:
        inst_id = item.get("instId")
        state = (item.get("state") or "").lower()
        if inst_id and (not state or state == "live"):
            inst_ids.add(inst_id.upper())

    OKX_TRADABLE_SPOT_INST_IDS = inst_ids
    OKX_TRADABILITY_CACHE_UPDATED_AT = now_utc
    OKX_TRADABILITY_LAST_ERROR = None

    print(f"🛡️ OKX TRADABILITY CACHE REFRESHED | spot_pairs={len(inst_ids)}", flush=True)

    return {
        "success": True,
        "cached": False,
        "count": len(inst_ids),
        "inst_ids": sorted(list(inst_ids)),
        "error": None
    }

def is_okx_symbol_live_tradable(symbol):
    if not ENABLE_OKX_TRADABILITY_FILTER:
        return True, "tradability_filter_disabled"

    okx_inst_id = okx_symbol_to_inst_id(symbol).upper()
    result = refresh_okx_tradable_spot_instruments(force=False)

    if not result.get("success"):
        return False, f"tradability_check_failed: {result.get('error')}"

    if okx_inst_id in OKX_TRADABLE_SPOT_INST_IDS:
        return True, "tradable"

    return False, "not_in_account_tradable_spot_instruments"

def log_okx_order(cur, trade_id, symbol, okx_inst_id, action, side, direction,
                  dry_run, request_payload, response_payload=None,
                  success=True, error_message=None):
    if not ENABLE_ORDER_LOGGING:
        return

    ensure_okx_order_log_table(cur)

    cur.execute("""
        INSERT INTO okx_order_log (
            trade_id,
            symbol,
            okx_inst_id,
            action,
            side,
            direction,
            dry_run,
            live_orders_enabled,
            request_payload,
            response_payload,
            success,
            error_message
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """, (
        str(trade_id) if trade_id is not None else None,
        symbol,
        okx_inst_id,
        action,
        side,
        direction,
        dry_run,
        ENABLE_LIVE_ORDERS,
        json.dumps(request_payload),
        json.dumps(response_payload) if response_payload is not None else None,
        success,
        error_message
    ))

def get_live_real_open_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
    """)
    return cur.fetchone()[0] or 0

def get_open_same_symbol_real_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND symbol = %s
    """, (symbol,))
    return cur.fetchone()[0] or 0

def okx_get_available_balance(ccy):
    if not ccy:
        return {"success": False, "available": 0.0, "error": "missing_currency", "response": None}

    result = okx_authenticated_get(f"/api/v5/account/balance?ccy={ccy}")

    if not result.get("success"):
        return {
            "success": False,
            "available": 0.0,
            "error": result.get("error"),
            "response": result.get("response")
        }

    response_payload = result.get("response") or {}

    try:
        data = response_payload.get("data") or []
        details = []
        if data and isinstance(data, list):
            details = data[0].get("details") or []

        for item in details:
            if (item.get("ccy") or "").upper() == ccy.upper():
                available_raw = item.get("availBal") or item.get("availableBal") or item.get("cashBal") or "0"
                return {
                    "success": True,
                    "available": float(available_raw or 0),
                    "error": None,
                    "response": response_payload
                }

        return {"success": True, "available": 0.0, "error": None, "response": response_payload}

    except Exception as e:
        return {"success": False, "available": 0.0, "error": str(e), "response": response_payload}

def has_successful_okx_live_entry(cur, trade_id):
    if not ENABLE_ORDER_LOGGING:
        return False

    ensure_okx_order_log_table(cur)

    cur.execute("""
        SELECT COUNT(*)
        FROM okx_order_log
        WHERE trade_id = %s
          AND action = 'entry'
          AND COALESCE(dry_run, FALSE) = FALSE
          AND COALESCE(success, FALSE) = TRUE
          AND error_message IS NULL
          AND COALESCE(response_payload::text, '') NOT ILIKE '%%skipped%%'
          AND COALESCE(response_payload::text, '') NOT ILIKE '%%dry_run%%'
    """, (str(trade_id),))

    return (cur.fetchone()[0] or 0) > 0

def log_okx_exit_skip_no_live_entry(cur, trade_id, symbol, direction, price=None):
    okx_inst_id = okx_symbol_to_inst_id(symbol)
    request_payload = {
        "skipped": True,
        "reason": "no_successful_okx_live_entry_for_trade",
        "symbol": symbol,
        "okx_inst_id": okx_inst_id,
        "direction": direction,
        "action": "exit",
        "price": price
    }
    response_payload = {
        "skipped": True,
        "message": "OKX exit skipped because this bot trade has no successful live OKX entry order.",
        "reason": "no_successful_okx_live_entry_for_trade"
    }

    log_okx_order(
        cur,
        trade_id,
        symbol,
        okx_inst_id,
        "exit",
        "sell",
        direction,
        False,
        request_payload,
        response_payload,
        True,
        "okx_exit_skipped_no_successful_live_entry"
    )

    print(f"🛡️ OKX EXIT SKIPPED | {symbol} | no successful live entry", flush=True)

def calculate_exit_base_size(entry_price, trade_size_quote):
    if entry_price <= 0:
        return 0
    return round(float(trade_size_quote) / float(entry_price), 8)

def okx_place_market_order(cur, trade_id, symbol, direction, action, price=None, entry_price=None, trade_size_quote=None):
    okx_inst_id = okx_symbol_to_inst_id(symbol)

    if direction != "LONG":
        request_payload = {
            "blocked": True,
            "reason": "live_execution_only_supports_long_spot_orders",
            "symbol": symbol,
            "direction": direction,
            "action": action
        }
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, None, direction,
            True, request_payload, None, False,
            "live_execution_only_supports_long_spot_orders"
        )
        return {
            "success": False,
            "dry_run": True,
            "blocked": True,
            "reason": "live_execution_only_supports_long_spot_orders"
        }

    quote_size = float(trade_size_quote if trade_size_quote is not None else CORE_TRADE_SIZE_GBP)

    if action == "entry":
        side = "buy"
        payload = {
            "instId": okx_inst_id,
            "tdMode": OKX_TD_MODE,
            "side": side,
            "ordType": OKX_ORDER_TYPE,
            "sz": str(quote_size),
            "tgtCcy": "quote_ccy"
        }

    elif action == "exit":
        side = "sell"
        base_ccy = okx_inst_id_to_base_ccy(okx_inst_id)

        if ENABLE_LIVE_ORDERS:
            balance_result = okx_get_available_balance(base_ccy)
            if not balance_result.get("success"):
                request_payload = {
                    "blocked": True,
                    "reason": "could_not_fetch_okx_available_balance",
                    "symbol": symbol,
                    "okx_inst_id": okx_inst_id,
                    "base_ccy": base_ccy,
                    "action": action,
                    "balance_error": balance_result.get("error")
                }
                log_okx_order(
                    cur, trade_id, symbol, okx_inst_id, action, side, direction,
                    False, request_payload, balance_result.get("response"), False,
                    f"could_not_fetch_okx_available_balance: {balance_result.get('error')}"
                )
                return {
                    "success": False,
                    "dry_run": False,
                    "blocked": True,
                    "reason": "could_not_fetch_okx_available_balance",
                    "error": balance_result.get("error")
                }

            available_balance = float(balance_result.get("available") or 0)
            reference_price = entry_price or price or 0
            theoretical_trade_size = calculate_exit_base_size(reference_price, quote_size)
            desired_trade_sell_size = theoretical_trade_size * OKX_EXIT_SIZE_BUFFER
            max_safe_available_size = available_balance * OKX_EXIT_SIZE_BUFFER
            sell_size = round(min(desired_trade_sell_size, max_safe_available_size), 8)

            if sell_size <= 0:
                request_payload = {
                    "skipped": True,
                    "reason": "no_available_okx_balance_to_sell",
                    "symbol": symbol,
                    "okx_inst_id": okx_inst_id,
                    "base_ccy": base_ccy,
                    "available_balance": available_balance,
                    "theoretical_trade_size": theoretical_trade_size,
                    "desired_trade_sell_size": desired_trade_sell_size,
                    "action": action
                }
                response_payload = {
                    "skipped": True,
                    "message": "OKX exit skipped because available balance is zero/dust.",
                    "available_balance": available_balance,
                    "base_ccy": base_ccy
                }
                log_okx_order(
                    cur, trade_id, symbol, okx_inst_id, action, side, direction,
                    False, request_payload, response_payload, True,
                    "okx_exit_skipped_no_available_balance"
                )
                return {
                    "success": True,
                    "dry_run": False,
                    "skipped": True,
                    "reason": "no_available_okx_balance_to_sell",
                    "available_balance": available_balance
                }

        else:
            reference_price = entry_price or price or 0
            sell_size = calculate_exit_base_size(reference_price, quote_size)

        payload = {
            "instId": okx_inst_id,
            "tdMode": OKX_TD_MODE,
            "side": side,
            "ordType": OKX_ORDER_TYPE,
            "sz": str(sell_size)
        }

    else:
        payload = {
            "blocked": True,
            "reason": "unknown_okx_action",
            "symbol": symbol,
            "direction": direction,
            "action": action
        }
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, None, direction,
            True, payload, None, False, "unknown_okx_action"
        )
        return {"success": False, "dry_run": True, "blocked": True, "reason": "unknown_okx_action"}

    dry_run = not ENABLE_LIVE_ORDERS

    if dry_run:
        response_payload = {
            "dry_run": True,
            "message": "OKX live orders disabled. No order sent.",
            "payload": payload,
            "requested_quote_size": quote_size
        }
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            True, payload, response_payload, True, None
        )
        print(f"🧪 OKX DRY RUN | {action.upper()} | {symbol}->{okx_inst_id} | size={payload.get('sz')}", flush=True)
        return {"success": True, "dry_run": True, "response": response_payload}

    if not okx_api_ready():
        error_message = "OKX API credentials missing or incomplete"
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, None, False, error_message
        )
        return {"success": False, "dry_run": False, "error": error_message}

    tradable, tradability_reason = is_okx_symbol_live_tradable(symbol)
    if not tradable:
        response_payload = {
            "skipped": True,
            "message": "Live OKX order skipped because symbol is not confirmed tradable for this account.",
            "reason": tradability_reason,
            "symbol": symbol,
            "okx_inst_id": okx_inst_id,
            "payload": payload,
            "requested_quote_size": quote_size
        }
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, response_payload, True, f"okx_live_order_skipped_{tradability_reason}"
        )
        send_telegram_alert(
            f"🛡️ <b>OKX ORDER SKIPPED</b>\n"
            f"{action.upper()} | {symbol} → {okx_inst_id}\n"
            f"Reason: {tradability_reason}"
        )
        return {
            "success": True,
            "dry_run": False,
            "skipped": True,
            "reason": tradability_reason,
            "response": response_payload
        }

    if action == "entry":
        live_open_count = get_live_real_open_count(cur)
        if live_open_count > MAX_LIVE_OPEN_TRADES:
            error_message = f"MAX_LIVE_OPEN_TRADES exceeded: {live_open_count} > {MAX_LIVE_OPEN_TRADES}"
            log_okx_order(
                cur, trade_id, symbol, okx_inst_id, action, side, direction,
                False, payload, None, False, error_message
            )
            return {"success": False, "dry_run": False, "blocked": True, "error": error_message}

    request_path = "/api/v5/trade/order"
    method = "POST"
    body = json.dumps(payload, separators=(",", ":"))

    try:
        headers = okx_headers(method, request_path, body)
        response = requests.post(
            f"{OKX_BASE_URL}{request_path}",
            headers=headers,
            data=body,
            timeout=10
        )

        try:
            response_payload = response.json()
        except Exception:
            response_payload = {"status_code": response.status_code, "text": response.text}

        okx_code = str(response_payload.get("code")) if isinstance(response_payload, dict) else None
        success = response.status_code == 200 and okx_code == "0"

        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, response_payload, success,
            None if success else f"OKX order failed: {response_payload}"
        )

        if success:
            print(f"✅ OKX LIVE ORDER SENT | {action.upper()} | {symbol}->{okx_inst_id} | size={payload.get('sz')}", flush=True)
            send_telegram_alert(
                f"✅ <b>OKX LIVE ORDER SENT</b>\n"
                f"{action.upper()} | {symbol} → {okx_inst_id}\n"
                f"Side: {side}\n"
                f"Size: {payload.get('sz')}"
            )
        else:
            print(f"❌ OKX LIVE ORDER FAILED | {action.upper()} | {symbol}->{okx_inst_id} | {response_payload}", flush=True)
            send_telegram_alert(
                f"❌ <b>OKX LIVE ORDER FAILED</b>\n"
                f"{action.upper()} | {symbol} → {okx_inst_id}\n"
                f"Response: {response_payload}"
            )

        return {
            "success": success,
            "dry_run": False,
            "status_code": response.status_code,
            "response": response_payload
        }

    except Exception as e:
        error_message = str(e)
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, None, False, error_message
        )
        return {"success": False, "dry_run": False, "error": error_message}

# =========================
# LEADERSHIP CONTEXT
# =========================

def get_leadership_context(cur, symbol):
    """
    v6.1.4 source of truth for live entry:
    leadership_state_history latest snapshot + 30m delta.

    Keeps legacy keys prior_successes/prior_runners/prior_avg_peak so the rest
    of the bot, DB insert, sizing, and Telegram remain compatible.
    """
    ensure_leadership_state_history_table(cur)

    cur.execute("""
        WITH latest AS (
            SELECT
                symbol,
                snapshot_time,
                leadership_score,
                successful_signals,
                runners,
                monsters,
                avg_peak,
                avg_worst,
                leadership_mode
            FROM leadership_state_history
            WHERE symbol = %s
            ORDER BY snapshot_time DESC
            LIMIT 1
        )
        SELECT
            l.symbol,
            l.snapshot_time,
            l.leadership_score,
            l.successful_signals,
            l.runners,
            l.monsters,
            l.avg_peak,
            l.avg_worst,
            l.leadership_mode,
            (
                SELECT p.leadership_score
                FROM leadership_state_history p
                WHERE p.symbol = l.symbol
                  AND p.snapshot_time <= l.snapshot_time - INTERVAL '30 minutes'
                ORDER BY p.snapshot_time DESC
                LIMIT 1
            ) AS score_30m_ago
        FROM latest l
    """, (symbol,))

    row = cur.fetchone()

    if not row:
        return {
            "prior_successes": 0,
            "prior_runners": 0,
            "prior_avg_peak": 0.0,
            "leadership_score": 0.0,
            "score_30m_ago": None,
            "delta_30m": None,
            "leadership_phase": "NO_LEADERSHIP_SNAPSHOT",
            "leadership_mode": None,
            "avg_worst": None,
            "snapshot_time": None
        }

    (
        _symbol,
        snapshot_time,
        leadership_score,
        successful_signals,
        runners,
        monsters,
        avg_peak,
        avg_worst,
        leadership_mode,
        score_30m_ago
    ) = row

    leadership_score = float(leadership_score or 0)
    score_30m_ago_float = float(score_30m_ago) if score_30m_ago is not None else None
    delta_30m = (
        leadership_score - score_30m_ago_float
        if score_30m_ago_float is not None
        else None
    )

    if score_30m_ago_float is None:
        phase = "NO_PRIOR"
    elif (
        leadership_score >= STABLE_LEADER_MIN_SCORE
        and STABLE_LEADER_DELTA_MIN <= delta_30m <= STABLE_LEADER_DELTA_MAX
    ):
        phase = "STABLE_LEADER"
    elif delta_30m > CLIMAX_LEADER_DELTA_BLOCK:
        phase = "CLIMAX_LEADER"
    elif (
        ENABLE_EMERGING_LEADER_ENTRIES
        and EMERGING_LEADER_MIN_SCORE <= leadership_score < EMERGING_LEADER_MAX_SCORE
        and EMERGING_LEADER_DELTA_MIN <= delta_30m <= EMERGING_LEADER_DELTA_MAX
    ):
        phase = "EMERGING_LEADER"
    elif leadership_score >= STABLE_LEADER_MIN_SCORE and delta_30m < STABLE_LEADER_DELTA_MIN:
        phase = "DECAYING_LEADER"
    else:
        phase = "OTHER"

    return {
        "prior_successes": successful_signals or 0,
        "prior_runners": runners or 0,
        "prior_avg_peak": leadership_score,
        "leadership_score": leadership_score,
        "score_30m_ago": score_30m_ago_float,
        "delta_30m": delta_30m,
        "leadership_phase": phase,
        "leadership_mode": leadership_mode,
        "avg_peak": float(avg_peak or 0),
        "avg_worst": float(avg_worst or 0),
        "monsters": monsters or 0,
        "snapshot_time": snapshot_time
    }

def passes_leadership_engine(cur, symbol, momentum, trend):
    if not ENABLE_LEADERSHIP_ENGINE:
        return False, None, "leadership_engine_disabled"

    if trend < LEADERSHIP_MIN_TREND:
        return False, None, "leadership_trend_too_low"

    if momentum <= LEADERSHIP_MIN_MOMENTUM:
        return False, None, "leadership_momentum_not_positive"

    leadership = get_leadership_context(cur, symbol)

    # Fallback legacy mode can be enabled in env if needed.
    if not ENABLE_STABLE_LEADERSHIP_PHASE_FILTER:
        prior_avg_peak = leadership["prior_avg_peak"]
        if prior_avg_peak < LEADERSHIP_MIN_PRIOR_AVG_PEAK:
            return False, leadership, "leadership_gate_failed"
        return True, leadership, "leadership_allowed"

    phase = leadership.get("leadership_phase")
    delta = leadership.get("delta_30m")
    score = leadership.get("leadership_score", 0)

    if phase == "NO_LEADERSHIP_SNAPSHOT":
        return False, leadership, "leadership_no_snapshot"

    if phase == "NO_PRIOR":
        return False, leadership, "leadership_no_30m_prior"

    if phase == "CLIMAX_LEADER":
        return False, leadership, "leadership_climax_delta_blocked"

    if phase == "DECAYING_LEADER":
        return False, leadership, "leadership_decaying_blocked"

    if phase == "STABLE_LEADER":
        return True, leadership, "leadership_stable_allowed"

    if phase == "EMERGING_LEADER":
        return True, leadership, "leadership_emerging_allowed"

    return False, leadership, "leadership_phase_not_tradable"

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

def get_trade_size_quote_for_quality(entry_quality):
    return get_trade_size_for_quality(entry_quality)


def get_latest_leadership_state(cur, symbol):
    try:
        ensure_leadership_state_history_table(cur)
        cur.execute("""
            SELECT
                leadership_score,
                successful_signals,
                runners,
                monsters,
                avg_peak,
                avg_worst,
                tradable,
                leadership_mode,
                snapshot_time
            FROM leadership_state_history
            WHERE symbol = %s
            ORDER BY snapshot_time DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "leadership_score": float(row[0] or 0),
            "successful_signals": row[1] or 0,
            "runners": row[2] or 0,
            "monsters": row[3] or 0,
            "avg_peak": float(row[4] or 0),
            "avg_worst": float(row[5] or 0),
            "tradable": bool(row[6]),
            "leadership_mode": row[7],
            "snapshot_time": row[8]
        }
    except Exception as e:
        print(f"⚠️ latest leadership state lookup failed for {symbol}: {e}", flush=True)
        return None

def format_leadership_state_for_telegram(state):
    if not state:
        return "Leadership: n/a"
    return (
        f"Leadership: {fmt_num(state.get('leadership_score'))} | "
        f"{state.get('leadership_mode')} | "
        f"S {state.get('successful_signals')} R {state.get('runners')} M {state.get('monsters')} | "
        f"worst {fmt_num(state.get('avg_worst'))}%"
    )

def get_top_leaders_text(cur, limit=5):
    try:
        ensure_leadership_state_history_table(cur)
        cur.execute("""
            WITH latest AS (
                SELECT MAX(snapshot_time) AS snapshot_time
                FROM leadership_state_history
            )
            SELECT
                l.symbol,
                ROUND(l.leadership_score::numeric, 3),
                l.leadership_mode,
                l.successful_signals,
                l.runners,
                l.tradable
            FROM leadership_state_history l
            JOIN latest x
              ON x.snapshot_time = l.snapshot_time
            ORDER BY l.leadership_score DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        if not rows:
            return "Top leaders: n/a"
        parts = []
        for symbol, score, mode, successes, runners, tradable in rows:
            icon = "✅" if tradable else "👀"
            parts.append(f"{icon} {symbol} {score} {mode} S{successes} R{runners}")
        return "\n".join(parts)
    except Exception as e:
        print(f"⚠️ top leaders lookup failed: {e}", flush=True)
        return "Top leaders: n/a"

def get_latest_signal_state(cur, symbol):
    try:
        cur.execute("""
            SELECT
                timestamp,
                price,
                momentum,
                trend,
                decision,
                block_reason
            FROM signals_raw
            WHERE symbol = %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return None
        return {
            "timestamp": row[0],
            "price": float(row[1] or 0),
            "momentum": float(row[2] or 0),
            "trend": float(row[3] or 0),
            "decision": row[4],
            "block_reason": row[5]
        }
    except Exception as e:
        print(f"⚠️ latest signal lookup failed for {symbol}: {e}", flush=True)
        return None

def build_telegram_open_trades_message(cur):
    cur.execute("""
        WITH latest_price AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                price,
                momentum,
                trend,
                decision,
                block_reason,
                timestamp AS latest_signal_time
            FROM signals_raw
            WHERE timestamp >= NOW() - INTERVAL '12 hours'
            ORDER BY symbol, timestamp DESC
        )
        SELECT
            b.id,
            b.symbol,
            b.entry_quality,
            b.entry_price,
            b.opened_at,
            COALESCE(b.peak_pnl_percent, 0) AS peak_pnl_percent,
            COALESCE(b.leadership_prior_avg_peak, b.leadership_score, 0) AS entry_leadership,
            COALESCE(b.dynamic_trade_size_gbp, b.trade_size_gbp, 0) AS trade_size_gbp,
            lp.price AS current_price,
            lp.momentum AS latest_momentum,
            lp.trend AS latest_trend,
            lp.decision AS latest_decision,
            lp.block_reason AS latest_block_reason,
            lp.latest_signal_time
        FROM bot_trades_v4 b
        LEFT JOIN latest_price lp
          ON lp.symbol = b.symbol
        WHERE b.status = 'OPEN'
          AND COALESCE(b.is_shadow, FALSE) = FALSE
        ORDER BY b.opened_at
        LIMIT 20
    """)
    rows = cur.fetchall()

    if not rows:
        return "📭 <b>Open Trades</b>\nNo open real trades."

    lines = ["📈 <b>Open Trades</b>"]
    for (
        trade_id, symbol, quality, entry_price, opened_at, peak,
        entry_leadership, trade_size_gbp, current_price, latest_momentum,
        latest_trend, latest_decision, latest_block_reason, latest_signal_time
    ) in rows:
        if current_price and entry_price:
            current_pnl = ((float(current_price) - float(entry_price)) / float(entry_price)) * 100
        else:
            current_pnl = None

        age_mins = None
        try:
            now_utc = datetime.now(timezone.utc)
            opened = opened_at if opened_at.tzinfo else opened_at.replace(tzinfo=timezone.utc)
            age_mins = (now_utc - opened).total_seconds() / 60
        except Exception:
            pass

        leadership_state = get_latest_leadership_state(cur, symbol)

        lines.append(
            f"\n<b>{symbol}</b> | {quality} | ID {trade_id}\n"
            f"Size: {fmt_money(trade_size_gbp)} | Age: {fmt_num(age_mins,1)}m\n"
            f"PnL: {fmt_num(current_pnl)}% | Peak: {fmt_num(peak)}%\n"
            f"Entry leadership: {fmt_num(entry_leadership)} | Current {format_leadership_state_for_telegram(leadership_state)}\n"
            f"Latest signal: mom {fmt_num(latest_momentum)} trend {fmt_num(latest_trend)} | "
            f"{latest_decision or 'NONE'} / {latest_block_reason or 'no_reason'}"
        )

    return "\n".join(lines)

# =========================
# TRADE HELPERS
# =========================

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

def open_trade(cur, symbol, direction, price, momentum, trend, quality,
               signal_id, signal_time, leadership_context):
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
            signal_timestamp
        )
        VALUES (%s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,0,%s,%s)
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
        signal_time
    ))

    trade_id = cur.fetchone()[0]

    safe_update_trade_telemetry(cur, trade_id, {
        "entry_architecture": quality,
        "trade_size_gbp": get_trade_size_for_quality(quality),
        "dynamic_trade_size_gbp": get_trade_size_for_quality(quality),
        "leadership_prior_successes": leadership_context.get("prior_successes"),
        "leadership_prior_runners": leadership_context.get("prior_runners"),
        "leadership_prior_avg_peak": leadership_context.get("prior_avg_peak"),
        "leadership_tier": quality,
        "leadership_mode": quality,
        "leadership_score": leadership_context.get("prior_avg_peak")
    })

    try:
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
    except Exception as e:
        print(f"⚠️ trade_events entry log failed: {e}", flush=True)

    return trade_id


def write_leadership_state_snapshots(cur):
    """
    Stores a point-in-time leadership map after each scorer run.
    Uses the same live leadership logic as the bot:
    - signal_leadership_scores
    - rolling LEADERSHIP_LOOKBACK_MINUTES window
    - leadership_score = avg future_peak_percent
    """
    ensure_leadership_state_history_table(cur)

    cur.execute("""
        WITH anchor AS (
            SELECT MAX(signal_timestamp) AS latest_scored_time
            FROM signal_leadership_scores
        ),

        leadership AS (
            SELECT
                s.symbol,

                COUNT(*) FILTER (
                    WHERE s.future_peak_percent >= 0.75
                ) AS successful_signals,

                COUNT(*) FILTER (
                    WHERE s.future_peak_percent >= 2.00
                ) AS runners,

                COUNT(*) FILTER (
                    WHERE s.future_peak_percent >= 5.00
                ) AS monsters,

                AVG(s.future_peak_percent) AS leadership_score,
                AVG(s.future_peak_percent) AS avg_peak,
                AVG(s.future_worst_percent) AS avg_worst

            FROM signal_leadership_scores s
            CROSS JOIN anchor a
            WHERE a.latest_scored_time IS NOT NULL
              AND s.signal_timestamp >= a.latest_scored_time - (%s || ' minutes')::INTERVAL
              AND s.signal_timestamp <= a.latest_scored_time
            GROUP BY s.symbol
        ),

        classified AS (
            SELECT
                symbol,
                leadership_score,
                successful_signals,
                runners,
                monsters,
                avg_peak,
                avg_worst,

                CASE
                    WHEN leadership_score >= %s THEN TRUE
                    ELSE FALSE
                END AS tradable,

                CASE
                    WHEN leadership_score >= 2.00 THEN 'LEADERSHIP_MONSTER'
                    WHEN leadership_score >= 1.50 THEN 'LEADERSHIP_AGGRESSIVE'
                    WHEN leadership_score >= %s THEN 'LEADERSHIP_CORE'
                    WHEN leadership_score >= 0.90 THEN 'NEAR_TRIGGER'
                    WHEN leadership_score >= 0.75 THEN 'WATCH'
                    ELSE 'WEAK'
                END AS leadership_mode

            FROM leadership
        )

        INSERT INTO leadership_state_history (
            symbol,
            leadership_score,
            successful_signals,
            runners,
            monsters,
            avg_peak,
            avg_worst,
            tradable,
            leadership_mode
        )
        SELECT
            symbol,
            leadership_score,
            successful_signals,
            runners,
            monsters,
            avg_peak,
            avg_worst,
            tradable,
            leadership_mode
        FROM classified
    """, (
        LEADERSHIP_LOOKBACK_MINUTES,
        LEADERSHIP_MIN_PRIOR_AVG_PEAK,
        LEADERSHIP_MIN_PRIOR_AVG_PEAK
    ))

    return cur.rowcount or 0


# =========================
# SIGNAL LEADERSHIP SCORER
# =========================

@app.route("/score_signal_leadership", methods=["GET"])
def score_signal_leadership():
    try:
        conn = get_db()
        cur = conn.cursor()
        ensure_signal_leadership_scores_table(cur)
        ensure_leadership_state_history_table(cur)

        cur.execute("""
            SELECT
                s.id,
                s.symbol,
                s.timestamp,
                s.price,
                s.momentum,
                s.trend

            FROM signals_raw s
            LEFT JOIN signal_leadership_scores sls
                ON sls.signal_id = s.id

            WHERE sls.signal_id IS NULL
              AND s.timestamp <= NOW() - (%s || ' minutes')::INTERVAL
              AND s.decision = 'LONG'
              AND s.trend >= %s
              AND s.momentum > %s

            ORDER BY s.timestamp
            LIMIT %s
        """, (
            LEADERSHIP_SIGNAL_FORWARD_MINUTES,
            LEADERSHIP_MIN_TREND,
            LEADERSHIP_MIN_MOMENTUM,
            LEADERSHIP_SCORER_LIMIT
        ))

        signals = cur.fetchall()
        scored_count = 0
        skipped_count = 0

        for signal_id, symbol, signal_timestamp, price, momentum, trend in signals:
            cur.execute("""
                SELECT
                    MAX(price) AS future_max_price,
                    MIN(price) AS future_min_price
                FROM signals_raw
                WHERE symbol = %s
                  AND timestamp > %s
                  AND timestamp <= %s + (%s || ' minutes')::INTERVAL
            """, (
                symbol,
                signal_timestamp,
                signal_timestamp,
                LEADERSHIP_SIGNAL_FORWARD_MINUTES
            ))

            future_max_price, future_min_price = cur.fetchone() or (None, None)

            if future_max_price is None or future_min_price is None or not price:
                skipped_count += 1
                continue

            price_f = float(price)
            future_max_f = float(future_max_price)
            future_min_f = float(future_min_price)

            if price_f <= 0:
                skipped_count += 1
                continue

            future_peak_percent = ((future_max_f - price_f) / price_f) * 100
            future_worst_percent = ((future_min_f - price_f) / price_f) * 100

            cur.execute("""
                INSERT INTO signal_leadership_scores (
                    signal_id,
                    symbol,
                    signal_timestamp,
                    price,
                    momentum,
                    trend,
                    future_max_price,
                    future_min_price,
                    future_peak_percent,
                    future_worst_percent
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (signal_id) DO NOTHING
            """, (
                signal_id,
                symbol,
                signal_timestamp,
                price,
                momentum,
                trend,
                future_max_price,
                future_min_price,
                future_peak_percent,
                future_worst_percent
            ))

            scored_count += 1

        snapshot_count = write_leadership_state_snapshots(cur)

        conn.commit()
        cur.close()
        conn.close()

        print(
            f"🧠 SIGNAL LEADERSHIP SCORED | scored={scored_count} "
            f"skipped={skipped_count} snapshots={snapshot_count}",
            flush=True
        )

        return jsonify({
            "status": "ok",
            "version": DATA_VERSION,
            "scored_signals": scored_count,
            "skipped_signals": skipped_count,
            "leadership_snapshots": snapshot_count
        }), 200

    except Exception as e:
        print("❌ SIGNAL LEADERSHIP ERROR:", e, flush=True)
        return jsonify({"error": str(e), "version": DATA_VERSION}), 400

# =========================
# WEBHOOK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    conn = None
    cur = None

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

        if ENABLE_ORDER_LOGGING:
            ensure_okx_order_log_table(cur)

        ensure_signal_leadership_scores_table(cur)

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

        # ================= ENTRY ENGINE =================
        entry_allowed = False
        leadership_context = None
        entry_quality = None
        block_reason = None

        if decision == "LONG":
            entry_allowed, leadership_context, block_reason = passes_leadership_engine(
                cur,
                symbol,
                momentum,
                trend
            )

            if entry_allowed:
                entry_quality = classify_leadership_tier(leadership_context["prior_avg_peak"])

                # v6.1.4: check OKX tradability BEFORE creating DB trade / consuming slot.
                okx_tradable, okx_tradability_reason = is_okx_symbol_live_tradable(symbol)
                if not okx_tradable:
                    entry_allowed = False
                    block_reason = f"okx_not_tradable_{okx_tradability_reason}"

                if entry_allowed:
                    open_count = get_live_real_open_count(cur)
                    if open_count >= MAX_OPEN_TRADES:
                        entry_allowed = False
                        block_reason = "max_open_trades"

                if entry_allowed and ENABLE_SAME_SYMBOL_STACKING_LIMIT:
                    same_symbol_count = get_open_same_symbol_real_count(cur, symbol)

                    if same_symbol_count >= MAX_SAME_SYMBOL_OPEN:
                        entry_allowed = False
                        block_reason = "max_same_symbol_open"

        elif decision == "SHORT":
            block_reason = "shorts_disabled_v6_1_long_only"
        else:
            block_reason = None

        # ================= UPDATE RAW SIGNAL INTELLIGENCE =================
        try:
            cur.execute("""
                UPDATE signals_raw
                SET
                    entry_allowed = %s,
                    block_reason = %s
                WHERE id = %s
            """, (
                entry_allowed,
                block_reason,
                signal_id
            ))
        except Exception as e:
            print(f"⚠️ signals_raw intelligence update skipped: {e}", flush=True)

        # ================= REAL ENTRY EXECUTION =================
        if decision in ["LONG", "SHORT"]:
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

                entry_trade_size = get_trade_size_for_quality(entry_quality)
                entry_quote_size = get_trade_size_quote_for_quality(entry_quality)

                print(
                    f"🚀 OPEN REAL | {symbol} | LONG | id={trade_id} | "
                    f"tier={entry_quality} | size=£{entry_trade_size} | "
                    f"prior_avg_peak={round(leadership_context.get('prior_avg_peak', 0), 3)} | "
                    f"prior_runners={leadership_context.get('prior_runners')}",
                    flush=True
                )

                live_open_after_entry = get_live_real_open_count(cur)
                same_symbol_after_entry = get_open_same_symbol_real_count(cur, symbol)
                current_leadership_state = get_latest_leadership_state(cur, symbol)
                top_leaders_text = get_top_leaders_text(cur, 4)

                send_telegram_alert(
                    f"🚀 <b>LEADERSHIP ENTRY</b>\n"
                    f"{symbol} | LONG\n"
                    f"Tier: <b>{entry_quality}</b>\n"
                    f"DB/Telegram size: <b>{fmt_money(entry_trade_size)}</b>\n"
                    f"OKX quote requested: <b>{fmt_money(entry_quote_size)}</b>\n"
                    f"Entry: {price}\n"
                    f"Trend/Momentum: {fmt_num(trend)} / {fmt_num(momentum)}\n"
                    f"Entry leadership score: {fmt_num(leadership_context.get('prior_avg_peak'))}\n"
                    f"Phase: {leadership_context.get('leadership_phase')} | Δ30m: {fmt_num(leadership_context.get('delta_30m'))}\n"
                    f"Prior successes/runners: {leadership_context.get('prior_successes')} / {leadership_context.get('prior_runners')}\n"
                    f"Current {format_leadership_state_for_telegram(current_leadership_state)}\n"
                    f"Slots: {live_open_after_entry}/{MAX_OPEN_TRADES} | Same symbol: {same_symbol_after_entry}/{MAX_SAME_SYMBOL_OPEN}\n"
                    f"Trade ID: {trade_id}\n\n"
                    f"<b>Top leaders now</b>\n{top_leaders_text}"
                )

                okx_place_market_order(
                    cur=cur,
                    trade_id=trade_id,
                    symbol=symbol,
                    direction="LONG",
                    action="entry",
                    price=price,
                    entry_price=price,
                    trade_size_quote=entry_quote_size
                )

            else:
                print(
                    f"⛔ BLOCKED | {symbol} | {decision} | "
                    f"mom={round(momentum,3)} trend={round(trend,3)} | "
                    f"reason={block_reason}",
                    flush=True
                )
        else:
            print(
                f"⛔ BLOCKED | {symbol} | reason=None",
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
                peak_pnl_percent,
                COALESCE(is_shadow, FALSE) AS is_shadow,
                entry_quality
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = FALSE
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, entry_quality) in open_trades:
            if sym != symbol:
                continue

            if direction != "LONG":
                continue

            pnl = ((price - entry_price) / entry_price)
            pnl_percent = pnl * 100
            mins = (now - opened_at).total_seconds() / 60
            current_peak = peak_pnl or 0

            if pnl_percent > current_peak:
                current_peak = pnl_percent
                if column_exists(cur, "bot_trades_v4", "peak_time_minutes"):
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

            try:
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
            except Exception as e:
                print(f"⚠️ trade_events update log failed: {e}", flush=True)

            close_reason = None
            exit_architecture = None
            decay_triggered = False
            adaptive_exit_triggered = False
            slot_recycle_candidate = False
            drawdown_from_peak = current_peak - pnl_percent

            if (
                not close_reason
                and ENABLE_DEAD_LEADER_RECYCLER
                and mins >= DEAD_LEADER_MINUTES
                and current_peak < DEAD_LEADER_MAX_PEAK
                and trend < DEAD_LEADER_TREND_THRESHOLD
            ):
                close_reason = "dead_leader_recycle_exit"
                exit_architecture = "leadership_dead_recycler"
                decay_triggered = True
                slot_recycle_candidate = True

            if (
                not close_reason
                and ENABLE_ADAPTIVE_WINNER_PROTECTION
            ):
                if (
                    current_peak >= ADAPTIVE_LARGE_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_LARGE_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_large"
                    exit_architecture = "leadership_adaptive_winner_protection"
                    adaptive_exit_triggered = True

                elif (
                    current_peak >= ADAPTIVE_MEDIUM_PEAK_TRIGGER
                    and current_peak < ADAPTIVE_LARGE_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_MEDIUM_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_medium"
                    exit_architecture = "leadership_adaptive_winner_protection"
                    adaptive_exit_triggered = True

                elif (
                    current_peak >= ADAPTIVE_SMALL_PEAK_TRIGGER
                    and current_peak < ADAPTIVE_MEDIUM_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_SMALL_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_small"
                    exit_architecture = "leadership_adaptive_winner_protection"
                    adaptive_exit_triggered = True

            if not close_reason and ENABLE_PROFIT_LOCKS:
                if current_peak >= LONG_LOCK_4_TRIGGER:
                    lock_floor = current_peak * LONG_LOCK_4_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "long_lock_4"
                        exit_architecture = "long_profit_lock"

                elif current_peak >= LONG_LOCK_3_TRIGGER:
                    lock_floor = current_peak * LONG_LOCK_3_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "long_lock_3"
                        exit_architecture = "long_profit_lock"

                elif current_peak >= LONG_LOCK_2_TRIGGER:
                    lock_floor = current_peak * LONG_LOCK_2_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "long_lock_2"
                        exit_architecture = "long_profit_lock"

                elif current_peak >= LONG_LOCK_1_TRIGGER:
                    lock_floor = current_peak * LONG_LOCK_1_RATIO
                    if pnl_percent <= lock_floor:
                        close_reason = "long_lock_1"
                        exit_architecture = "long_profit_lock"

            if not close_reason and current_peak >= LONG_NO_RED_AFTER_WIN_TRIGGER and pnl_percent < 0:
                close_reason = "long_gave_back_winner"
                exit_architecture = "long_no_red_after_win"

            if not close_reason and pnl_percent <= LONG_HARD_STOP:
                close_reason = "long_hard_stop"
                exit_architecture = "long_hard_stop"

            if close_reason:
                trade_size_for_pnl = get_trade_size_for_quality(entry_quality)
                pnl_gbp = (pnl_percent / 100) * trade_size_for_pnl

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

                safe_update_trade_telemetry(cur, tid, {
                    "decay_triggered": decay_triggered,
                    "decay_checked_at_minutes": mins if decay_triggered else None,
                    "decay_peak_at_check": current_peak if decay_triggered else None,
                    "decay_trend_at_check": trend if decay_triggered else None,
                    "decay_momentum_at_check": momentum if decay_triggered else None,
                    "slot_recycle_candidate": slot_recycle_candidate,
                    "exit_architecture": exit_architecture,
                    "adaptive_exit_triggered": adaptive_exit_triggered,
                    "drawdown_from_peak_at_exit": drawdown_from_peak,
                    "leadership_trend_at_exit": trend,
                    "leadership_momentum_at_exit": momentum
                })

                try:
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
                except Exception as e:
                    print(f"⚠️ trade_events exit log failed: {e}", flush=True)

                if has_successful_okx_live_entry(cur, tid):
                    okx_place_market_order(
                        cur=cur,
                        trade_id=tid,
                        symbol=sym,
                        direction=direction,
                        action="exit",
                        price=price,
                        entry_price=entry_price,
                        trade_size_quote=get_trade_size_quote_for_quality(entry_quality)
                    )
                else:
                    log_okx_exit_skip_no_live_entry(
                        cur=cur,
                        trade_id=tid,
                        symbol=sym,
                        direction=direction,
                        price=price
                    )

                print(
                    f"💰 CLOSED REAL | {sym} | LONG | "
                    f"{round(pnl_percent,3)}% | peak={round(current_peak,3)} | "
                    f"dd_from_peak={round(drawdown_from_peak,3)} | {close_reason}",
                    flush=True
                )

                exit_leadership_state = get_latest_leadership_state(cur, sym)
                latest_signal_state = get_latest_signal_state(cur, sym)

                send_telegram_alert(
                    f"💰 <b>CLOSED REAL</b>\n"
                    f"{sym} | LONG\n"
                    f"PnL: <b>{fmt_num(pnl_percent)}%</b> | £{fmt_num(pnl_gbp, 3)}\n"
                    f"Peak: {fmt_num(current_peak)}%\n"
                    f"Drawdown from peak: {fmt_num(drawdown_from_peak)}%\n"
                    f"Reason: {close_reason}\n"
                    f"Exit architecture: {exit_architecture}\n"
                    f"Exit trend/momentum: {fmt_num(trend)} / {fmt_num(momentum)}\n"
                    f"{format_leadership_state_for_telegram(exit_leadership_state)}\n"
                    f"Latest signal: mom {fmt_num((latest_signal_state or {}).get('momentum'))} "
                    f"trend {fmt_num((latest_signal_state or {}).get('trend'))} | "
                    f"{(latest_signal_state or {}).get('decision') or 'NONE'} / "
                    f"{(latest_signal_state or {}).get('block_reason') or 'no_reason'}"
                )

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok", "version": DATA_VERSION}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)

        try:
            if conn:
                conn.rollback()
            if cur:
                cur.close()
            if conn:
                conn.close()
        except Exception:
            pass

        return jsonify({"error": str(e), "version": DATA_VERSION}), 400

# =========================
# TELEGRAM / STATUS ROUTES
# =========================

def require_summary_secret():
    if not TELEGRAM_COMMAND_SECRET:
        return True
    return request.args.get("secret") == TELEGRAM_COMMAND_SECRET

def build_telegram_health_message(cur):
    cur.execute("""
        SELECT
            MAX(timestamp),
            COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '1 hour')
        FROM signals_raw
    """)
    last_signal, signals_1h = cur.fetchone() or (None, 0)

    cur.execute("""
        SELECT
            MAX(opened_at),
            COUNT(*) FILTER (WHERE opened_at >= NOW() - INTERVAL '24 hours'),
            COUNT(*) FILTER (WHERE status = 'OPEN' AND COALESCE(is_shadow, FALSE) = FALSE)
        FROM bot_trades_v4
        WHERE COALESCE(is_shadow, FALSE) = FALSE
    """)
    last_trade, trades_24h, open_real = cur.fetchone() or (None, 0, 0)

    cur.execute("""
        SELECT COUNT(*)
        FROM signal_leadership_scores
        WHERE scored_at >= NOW() - INTERVAL '24 hours'
    """)
    scored_24h = cur.fetchone()[0] or 0

    try:
        ensure_leadership_state_history_table(cur)
        cur.execute("""
            SELECT COUNT(*)
            FROM leadership_state_history
            WHERE snapshot_time >= NOW() - INTERVAL '24 hours'
        """)
        snapshots_24h = cur.fetchone()[0] or 0
    except Exception:
        snapshots_24h = 0

    return (
        f"🩺 <b>Bot Health</b>\n"
        f"Version: {DATA_VERSION}\n"
        f"Engine: LEADERSHIP_SIGNAL_SCORED\n"
        f"Live orders: {ENABLE_LIVE_ORDERS}\n"
        f"Last signal: {last_signal}\n"
        f"Signals 1h: {signals_1h}\n"
        f"Last real trade: {last_trade}\n"
        f"Real trades 24h: {trades_24h}\n"
        f"Open real: {open_real}\n"
        f"Scored leadership signals 24h: {scored_24h}\n"
        f"Leadership snapshots 24h: {snapshots_24h}\n"
        f"Leadership threshold: {LEADERSHIP_MIN_PRIOR_AVG_PEAK}\n"
        f"Max same symbol: {MAX_SAME_SYMBOL_OPEN}\n"
        f"OKX tradable cache: {len(OKX_TRADABLE_SPOT_INST_IDS)} pairs"
    )

def build_telegram_summary_message(cur, hours=24):
    cur.execute("""
        WITH closed AS (
            SELECT
                entry_quality AS engine,
                pnl_percent,
                pnl_gbp,
                peak_pnl_percent,
                close_reason
            FROM bot_trades_v4
            WHERE closed_at >= NOW() - (%s || ' hours')::INTERVAL
              AND COALESCE(is_shadow, FALSE) = FALSE
              AND status = 'CLOSED'
        )
        SELECT
            engine,
            COUNT(*) AS trades,
            COALESCE(ROUND(SUM(pnl_gbp)::numeric, 3), 0) AS pnl_gbp,
            COALESCE(ROUND(AVG(pnl_percent)::numeric, 3), 0) AS avg_pnl,
            COALESCE(ROUND(AVG(peak_pnl_percent)::numeric, 3), 0) AS avg_peak,
            COUNT(*) FILTER (WHERE pnl_percent > 0) AS winners,
            COUNT(*) FILTER (WHERE peak_pnl_percent >= 2.0) AS runners,
            COUNT(*) FILTER (WHERE peak_pnl_percent >= 5.0) AS monsters
        FROM closed
        GROUP BY engine
        ORDER BY engine
    """, (hours,))
    closed_rows = cur.fetchall()

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'OPEN') AS open_total
        FROM bot_trades_v4
        WHERE COALESCE(is_shadow, FALSE) = FALSE
    """)
    open_total = (cur.fetchone() or (0,))[0] or 0

    total_pnl = sum(float(r[2] or 0) for r in closed_rows)
    total_trades = sum(int(r[1] or 0) for r in closed_rows)

    lines = []
    lines.append(f"📊 <b>Trading Bot {hours}h Summary</b>")
    lines.append(f"Version: <b>{DATA_VERSION}</b>")
    lines.append(f"Closed trades: <b>{total_trades}</b>")
    lines.append(f"Realized PnL: <b>{fmt_money(total_pnl)}</b>")
    lines.append(f"Open real: <b>{open_total}</b>")

    if closed_rows:
        lines.append("\n<b>Engine breakdown</b>")
        for engine, trades, pnl_gbp, avg_pnl, avg_peak, winners, runners, monsters in closed_rows:
            win_rate = (float(winners or 0) / float(trades or 1)) * 100
            lines.append(
                f"{engine}: {trades} trades | {fmt_money(pnl_gbp)} | avg {fmt_num(avg_pnl)}% | "
                f"peak {fmt_num(avg_peak)}% | win {fmt_num(win_rate,1)}% | R {runners} M {monsters}"
            )
    else:
        lines.append("\nNo closed real trades in this window.")

    lines.append("\n<b>Top leaders</b>")
    lines.append(get_top_leaders_text(cur, 5))

    open_message = build_telegram_open_trades_message(cur)
    if "No open real trades" not in open_message:
        lines.append("\n" + open_message)

    return "\n".join(lines)

def handle_telegram_command(text):
    cmd = (text or "").strip().lower().split()[0] if text else "/help"
    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_signal_leadership_scores_table(cur)
        if cmd in ["/status", "/daily", "/summary", "/pnl"]:
            return build_telegram_summary_message(cur, 24)
        if cmd in ["/open", "/trades"]:
            return build_telegram_open_trades_message(cur)
        if cmd in ["/leaders", "/leadership"]:
            return "🧠 <b>Top Leadership States</b>\n" + get_top_leaders_text(cur, 10)
        if cmd == "/health":
            return build_telegram_health_message(cur)
        if cmd == "/help":
            return (
                "🤖 <b>Trading Bot Commands</b>\n"
                "/status - rolling 24h summary + leaders\n"
                "/daily - rolling 24h summary + leaders\n"
                "/open - open trades with latest signal/leadership\n"
                "/leaders - current leadership leaderboard\n"
                "/health - webhook/server health\n"
                "/help - command list"
            )
        return "Unknown command. Send /help."
    finally:
        cur.close()
        conn.close()

def is_authorized_telegram_chat(chat_id):
    if not TELEGRAM_CHAT_ID:
        return False
    return str(chat_id) == str(TELEGRAM_CHAT_ID)

@app.route("/telegram_summary_24h", methods=["GET"])
def telegram_summary_24h():
    try:
        if not require_summary_secret():
            return jsonify({"error": "unauthorized"}), 403

        conn = get_db()
        cur = conn.cursor()
        message = build_telegram_summary_message(cur, 24)
        cur.close()
        conn.close()

        sent = send_telegram_alert(message)
        return jsonify({"status": "ok", "version": DATA_VERSION, "sent": sent, "hours": 24}), 200

    except Exception as e:
        print("❌ TELEGRAM SUMMARY ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400

@app.route("/telegram_webhook", methods=["POST"])
def telegram_webhook():
    try:
        if not ENABLE_TELEGRAM_COMMANDS:
            return jsonify({"status": "commands_disabled"}), 200

        if TELEGRAM_WEBHOOK_SECRET:
            supplied_secret = request.headers.get("X-Telegram-Bot-Api-Secret-Token")
            if supplied_secret != TELEGRAM_WEBHOOK_SECRET:
                return jsonify({"error": "unauthorized"}), 403

        payload = request.get_json(force=True) or {}
        message = payload.get("message") or payload.get("edited_message") or {}
        chat = message.get("chat") or {}
        chat_id = chat.get("id")
        text = message.get("text") or ""

        if not is_authorized_telegram_chat(chat_id):
            telegram_send_message(chat_id, "Unauthorized chat.")
            return jsonify({"status": "unauthorized_chat"}), 200

        response_text = handle_telegram_command(text)
        telegram_send_message(chat_id, response_text)

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        print("❌ TELEGRAM WEBHOOK ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400

@app.route("/telegram_test", methods=["GET"])
def telegram_test():
    try:
        sent = send_telegram_alert(
            f"📲 <b>Telegram test successful</b>\n"
            f"Bot version: {DATA_VERSION}\n"
            f"Live orders: {ENABLE_LIVE_ORDERS}\n"
            f"Leadership threshold: {LEADERSHIP_MIN_PRIOR_AVG_PEAK}"
        )
        return jsonify({
            "status": "ok",
            "telegram_enabled": ENABLE_TELEGRAM_ALERTS,
            "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
            "sent": sent
        }), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/okx_tradability_status", methods=["GET"])
def okx_tradability_status():
    try:
        force = request.args.get("force", "false").lower() == "true"
        result = refresh_okx_tradable_spot_instruments(force=force)

        return jsonify({
            "status": "ok",
            "version": DATA_VERSION,
            "result": result,
            "okx_execution": bool_status()
        }), 200

    except Exception as e:
        print("❌ OKX TRADABILITY STATUS ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400

def bool_status():
    return {
        "DATA_VERSION": DATA_VERSION,
        "MAX_OPEN_TRADES": MAX_OPEN_TRADES,
        "MAX_SAME_SYMBOL_OPEN": MAX_SAME_SYMBOL_OPEN,
        "ENABLE_LIVE_ORDERS": ENABLE_LIVE_ORDERS,
        "ENABLE_ORDER_LOGGING": ENABLE_ORDER_LOGGING,
        "MAX_LIVE_OPEN_TRADES": MAX_LIVE_OPEN_TRADES,
        "CORE_TRADE_SIZE_GBP": CORE_TRADE_SIZE_GBP,
        "AGGRESSIVE_TRADE_SIZE_GBP": AGGRESSIVE_TRADE_SIZE_GBP,
        "MONSTER_TRADE_SIZE_GBP": MONSTER_TRADE_SIZE_GBP,
        "LEADERSHIP_MIN_PRIOR_AVG_PEAK": LEADERSHIP_MIN_PRIOR_AVG_PEAK,
        "ENABLE_STABLE_LEADERSHIP_PHASE_FILTER": ENABLE_STABLE_LEADERSHIP_PHASE_FILTER,
        "STABLE_LEADER_MIN_SCORE": STABLE_LEADER_MIN_SCORE,
        "STABLE_LEADER_DELTA_MIN": STABLE_LEADER_DELTA_MIN,
        "STABLE_LEADER_DELTA_MAX": STABLE_LEADER_DELTA_MAX,
        "CLIMAX_LEADER_DELTA_BLOCK": CLIMAX_LEADER_DELTA_BLOCK,
        "ENABLE_EMERGING_LEADER_ENTRIES": ENABLE_EMERGING_LEADER_ENTRIES,
        "LEADERSHIP_LOOKBACK_MINUTES": LEADERSHIP_LOOKBACK_MINUTES,
        "LEADERSHIP_SIGNAL_FORWARD_MINUTES": LEADERSHIP_SIGNAL_FORWARD_MINUTES,
        "OKX_BASE_URL": OKX_BASE_URL,
        "OKX_TD_MODE": OKX_TD_MODE,
        "ENABLE_OKX_TRADABILITY_FILTER": ENABLE_OKX_TRADABILITY_FILTER,
        "OKX_TRADABLE_SPOT_COUNT": len(OKX_TRADABLE_SPOT_INST_IDS),
        "OKX_TRADABILITY_CACHE_UPDATED_AT": OKX_TRADABILITY_CACHE_UPDATED_AT.isoformat() if OKX_TRADABILITY_CACHE_UPDATED_AT else None,
        "OKX_TRADABILITY_LAST_ERROR": OKX_TRADABILITY_LAST_ERROR,
        "ENABLE_TELEGRAM_ALERTS": ENABLE_TELEGRAM_ALERTS,
        "ENABLE_TELEGRAM_COMMANDS": ENABLE_TELEGRAM_COMMANDS,
        "TELEGRAM_CONFIGURED": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
    }

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "version": DATA_VERSION,
        "engine": "LEADERSHIP_SIGNAL_SCORED",
        "okx_execution": bool_status()
    }), 200
