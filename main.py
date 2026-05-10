# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v5.5
# TITLE: V7 MAIN CONTINUATION ENGINE + TREND DECAY EXIT + V5.3/V6 SHADOWS + S5 SHORTS SHADOW + OKX EXECUTION LAYER + OKX TRADABILITY FILTER + EXIT SAFETY + TELEGRAM ALERTS
# =========================

print("🔥🔥🔥 MAIN.PY v5.5 + OKX EXEC + TRADABILITY FILTER + EXIT SAFETY + TELEGRAM RUNNING 🔥🔥🔥", flush=True)

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

MAX_OPEN_TRADES = 7
MAX_OPEN_SHADOW_TRADES = 30
TRADE_SIZE_GBP = 100

DATA_VERSION = "v5.5"

# =========================
# 🔌 OKX EXECUTION SETTINGS
# =========================

OKX_API_KEY = os.environ.get("OKX_API_KEY")
OKX_API_SECRET = os.environ.get("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.environ.get("OKX_API_PASSPHRASE")
OKX_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")

ENABLE_ORDER_LOGGING = os.environ.get("ENABLE_ORDER_LOGGING", "true").lower() == "true"
ENABLE_LIVE_ORDERS = os.environ.get("ENABLE_LIVE_ORDERS", "false").lower() == "true"

LIVE_TRADE_SIZE_GBP = float(os.environ.get("LIVE_TRADE_SIZE_GBP", "5") or 5)
MAX_LIVE_OPEN_TRADES = int(os.environ.get("MAX_LIVE_OPEN_TRADES", "7") or 7)

OKX_TD_MODE = os.environ.get("OKX_TD_MODE", "cash")
OKX_ORDER_TYPE = os.environ.get("OKX_ORDER_TYPE", "market")

# =========================
# 📲 TELEGRAM ALERT SETTINGS
# =========================

ENABLE_TELEGRAM_ALERTS = os.environ.get("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")

# OKX spot market buys use quote currency when tgtCcy="quote_ccy".
LIVE_TRADE_SIZE_QUOTE = float(os.environ.get("LIVE_TRADE_SIZE_QUOTE", LIVE_TRADE_SIZE_GBP) or LIVE_TRADE_SIZE_GBP)

# =========================
# 🛡️ OKX TRADABILITY FILTER
# =========================

# Keeps strategy universe intact, but only sends real OKX orders for account-tradable SPOT pairs.
ENABLE_OKX_TRADABILITY_FILTER = os.environ.get("ENABLE_OKX_TRADABILITY_FILTER", "true").lower() == "true"
OKX_TRADABILITY_CACHE_SECONDS = int(os.environ.get("OKX_TRADABILITY_CACHE_SECONDS", "900") or 900)

OKX_TRADABLE_SPOT_INST_IDS = set()
OKX_TRADABILITY_CACHE_UPDATED_AT = None
OKX_TRADABILITY_LAST_ERROR = None

# =========================
# 🚀 REGIME SETTINGS
# =========================

ENABLE_MAX_OPEN_TRADES = True

MIN_ENTRY_TREND = 0.25
MIN_ENTRY_MOMENTUM = 0.70

HOT_SNIPER_COUNT = 5
WARM_SNIPER_COUNT = 3
LOW_SNIPER_COUNT = 1

ACTIVE_RISING_MIN_DELTA = 2

# =========================
# 🧠 V7 MAIN LONG ENGINE
# =========================

ENABLE_V7_MAIN_ENGINE = True

V7_MIN_TREND = 0.20
V7_MIN_MOMENTUM = 0.00

# validated V7 decay exit
ENABLE_V7_TREND_DECAY_EXIT = True
V7_DECAY_MINUTES = 120
V7_DECAY_TREND_THRESHOLD = 0.22
V7_DECAY_MAX_PEAK = 0.50

# =========================
# 🎯 V5.3 SHADOW SNIPER ENGINE
# =========================

ENABLE_SHADOW_V53_SNIPER = True

V53_REQUIRED_REGIME = "WARM"
V53_MIN_DENSITY = 3
V53_MAX_DENSITY = 5
V53_MIN_DENSITY_DELTA = 2
V53_MAX_DENSITY_DELTA = 4

# =========================
# 🩸 S5 TACTICAL SHORT ENGINE — SHADOW ONLY
# =========================

ENABLE_SHADOW_S5_SHORT_ENGINE = True

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

# V6 compression continuation
V6_MIN_TREND = 0.13
V6_MAX_TREND = 0.19
V6_MIN_MOMENTUM = 0.70
V6_MIN_DENSITY = 4
V6_MAX_DENSITY = 6
V6_MIN_DELTA = -3
V6_MAX_DELTA = 0

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

# =========================
# TELEGRAM HELPERS
# =========================

def send_telegram_alert(message):
    if not ENABLE_TELEGRAM_ALERTS:
        return False

    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        print("⚠️ TELEGRAM ALERT SKIPPED | missing TELEGRAM_BOT_TOKEN or TELEGRAM_CHAT_ID", flush=True)
        return False

    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }

        response = requests.post(url, json=payload, timeout=8)

        if response.status_code == 200:
            print("📲 TELEGRAM ALERT SENT", flush=True)
            return True

        print(f"⚠️ TELEGRAM ALERT FAILED | status={response.status_code} | body={response.text}", flush=True)
        return False

    except Exception as e:
        print(f"⚠️ TELEGRAM ALERT ERROR | {e}", flush=True)
        return False

def fmt_num(value, digits=3):
    try:
        return round(float(value), digits)
    except Exception:
        return value

# =========================
# OKX HELPERS
# =========================

def bool_status():
    return {
        "ENABLE_ORDER_LOGGING": ENABLE_ORDER_LOGGING,
        "ENABLE_LIVE_ORDERS": ENABLE_LIVE_ORDERS,
        "MAX_LIVE_OPEN_TRADES": MAX_LIVE_OPEN_TRADES,
        "LIVE_TRADE_SIZE_QUOTE": LIVE_TRADE_SIZE_QUOTE,
        "OKX_BASE_URL": OKX_BASE_URL,
        "OKX_TD_MODE": OKX_TD_MODE,
        "ENABLE_OKX_TRADABILITY_FILTER": ENABLE_OKX_TRADABILITY_FILTER,
        "OKX_TRADABILITY_CACHE_SECONDS": OKX_TRADABILITY_CACHE_SECONDS,
        "OKX_TRADABLE_SPOT_COUNT": len(OKX_TRADABLE_SPOT_INST_IDS),
        "OKX_TRADABILITY_CACHE_UPDATED_AT": OKX_TRADABILITY_CACHE_UPDATED_AT.isoformat() if OKX_TRADABILITY_CACHE_UPDATED_AT else None,
        "OKX_TRADABILITY_LAST_ERROR": OKX_TRADABILITY_LAST_ERROR,
        "ENABLE_TELEGRAM_ALERTS": ENABLE_TELEGRAM_ALERTS,
        "TELEGRAM_CONFIGURED": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
    }

def okx_symbol_to_inst_id(symbol):
    """
    Converts TradingView-style symbols like BTCUSDT into OKX spot instIds like BTC-USDT.
    """
    s = (symbol or "").upper().replace("-", "").replace("/", "")

    quote_assets = ["USDT", "USDC", "USD", "BTC", "ETH", "EUR", "GBP"]

    for quote in quote_assets:
        if s.endswith(quote) and len(s) > len(quote):
            base = s[:-len(quote)]
            return f"{base}-{quote}"

    return symbol

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
        return {
            "success": False,
            "error": "OKX API credentials missing or incomplete",
            "response": None
        }

    method = "GET"
    body = ""

    try:
        headers = okx_headers(method, request_path_with_query, body)

        response = requests.get(
            f"{OKX_BASE_URL}{request_path_with_query}",
            headers=headers,
            timeout=10
        )

        try:
            response_payload = response.json()
        except Exception:
            response_payload = {
                "status_code": response.status_code,
                "text": response.text
            }

        okx_code = str(response_payload.get("code")) if isinstance(response_payload, dict) else None
        success = response.status_code == 200 and okx_code == "0"

        return {
            "success": success,
            "status_code": response.status_code,
            "response": response_payload,
            "error": None if success else f"OKX GET failed: {response_payload}"
        }

    except Exception as e:
        return {
            "success": False,
            "status_code": None,
            "response": None,
            "error": str(e)
        }

def refresh_okx_tradable_spot_instruments(force=False):
    """
    Uses OKX account-level instruments so the result reflects this account's actual tradable instruments.
    """
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

        # OKX usually returns live instruments here. Keep only live if state is present.
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
        # Safety first: if we cannot confirm tradability, do not send live orders.
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

def has_successful_okx_live_entry(cur, trade_id):
    """
    Safety gate for exits.

    Only send a live OKX exit if this exact bot trade previously had a successful,
    non-dry-run OKX entry order. This prevents the bot trying to sell coins that
    only exist as database trades, legacy trades, restricted-symbol skips, or failed entries.
    """
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

    print(
        f"🛡️ OKX EXIT SKIPPED | {symbol} -> {okx_inst_id} | "
        f"reason=no_successful_okx_live_entry_for_trade",
        flush=True
    )

def calculate_exit_base_size(entry_price):
    if entry_price <= 0:
        return 0

    base_size = LIVE_TRADE_SIZE_QUOTE / float(entry_price)

    # Conservative rounding to reduce precision errors.
    return round(base_size, 8)

def okx_place_market_order(cur, trade_id, symbol, direction, action, price=None, entry_price=None):
    """
    action:
      - "entry": real LONG entry = buy
      - "exit": real LONG exit = sell

    S5 shorts are shadow only, so this live execution wrapper intentionally
    only supports real LONG spot buy/sell execution.

    The tradability filter does NOT alter strategy/database trades.
    It only skips live OKX order submission when the account cannot trade the pair.
    """

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

        print(f"🛡️ OKX BLOCKED | {symbol} | {direction} | live execution supports LONG spot only", flush=True)

        return {
            "success": False,
            "dry_run": True,
            "blocked": True,
            "reason": "live_execution_only_supports_long_spot_orders"
        }

    if action == "entry":
        side = "buy"
        payload = {
            "instId": okx_inst_id,
            "tdMode": OKX_TD_MODE,
            "side": side,
            "ordType": OKX_ORDER_TYPE,
            "sz": str(LIVE_TRADE_SIZE_QUOTE),
            "tgtCcy": "quote_ccy"
        }

    elif action == "exit":
        side = "sell"
        reference_price = entry_price or price or 0
        sell_size = calculate_exit_base_size(reference_price)

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

        return {
            "success": False,
            "dry_run": True,
            "blocked": True,
            "reason": "unknown_okx_action"
        }

    dry_run = not ENABLE_LIVE_ORDERS

    if dry_run:
        response_payload = {
            "dry_run": True,
            "message": "OKX live orders disabled. No order sent.",
            "payload": payload
        }

        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            True, payload, response_payload, True, None
        )

        print(
            f"🧪 OKX DRY RUN | {action.upper()} | {symbol} -> {okx_inst_id} | "
            f"side={side} | size={payload.get('sz')}",
            flush=True
        )

        return {
            "success": True,
            "dry_run": True,
            "response": response_payload
        }

    if not okx_api_ready():
        error_message = "OKX API credentials missing or incomplete"

        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, None, False, error_message
        )

        print(f"❌ OKX LIVE ORDER BLOCKED | {symbol} | {error_message}", flush=True)

        return {
            "success": False,
            "dry_run": False,
            "error": error_message
        }

    tradable, tradability_reason = is_okx_symbol_live_tradable(symbol)

    if not tradable:
        response_payload = {
            "skipped": True,
            "message": "Live OKX order skipped because symbol is not confirmed tradable for this account.",
            "reason": tradability_reason,
            "symbol": symbol,
            "okx_inst_id": okx_inst_id,
            "payload": payload
        }

        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, response_payload, True, f"okx_live_order_skipped_{tradability_reason}"
        )

        print(
            f"🛡️ OKX LIVE ORDER SKIPPED | {action.upper()} | {symbol} -> {okx_inst_id} | "
            f"reason={tradability_reason}",
            flush=True
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

            print(f"🛡️ OKX LIVE ORDER BLOCKED | {symbol} | {error_message}", flush=True)

            return {
                "success": False,
                "dry_run": False,
                "blocked": True,
                "error": error_message
            }

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
            response_payload = {
                "status_code": response.status_code,
                "text": response.text
            }

        okx_code = str(response_payload.get("code")) if isinstance(response_payload, dict) else None
        success = response.status_code == 200 and okx_code == "0"

        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, response_payload, success,
            None if success else f"OKX order failed: {response_payload}"
        )

        if success:
            print(
                f"✅ OKX LIVE ORDER SENT | {action.upper()} | {symbol} -> {okx_inst_id} | "
                f"side={side} | size={payload.get('sz')}",
                flush=True
            )

            send_telegram_alert(
                f"✅ <b>OKX LIVE ORDER SENT</b>\n"
                f"{action.upper()} | {symbol} → {okx_inst_id}\n"
                f"Side: {side}\n"
                f"Size: {payload.get('sz')}"
            )
        else:
            print(
                f"❌ OKX LIVE ORDER FAILED | {action.upper()} | {symbol} -> {okx_inst_id} | "
                f"response={response_payload}",
                flush=True
            )

            send_telegram_alert(
                f"❌ <b>OKX LIVE ORDER FAILED</b>\n"
                f"{action.upper()} | {symbol} → {okx_inst_id}\n"
                f"Side: {side}\n"
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

        print(f"❌ OKX EXECUTION ERROR | {symbol} | {error_message}", flush=True)

        return {
            "success": False,
            "dry_run": False,
            "error": error_message
        }

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
        return "SHADOW_S5_SHORT_TIER_2_ELITE"

    if (
        S5_MIN_TREND <= trend <= S5_MAX_TREND
        and momentum <= S5_MIN_MOMENTUM
        and sniper_density >= S5_MIN_DENSITY
    ):
        return "SHADOW_S5_SHORT_TIER_1_BROAD"

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

def passes_v7_main(momentum, trend):
    return (
        ENABLE_V7_MAIN_ENGINE
        and trend >= V7_MIN_TREND
        and momentum > V7_MIN_MOMENTUM
    )

def passes_v53_shadow(momentum, trend, regime_state, active_rising, sniper_density, sniper_density_delta):
    if trend < MIN_ENTRY_TREND:
        return False

    if momentum < MIN_ENTRY_MOMENTUM:
        return False

    if regime_state != V53_REQUIRED_REGIME:
        return False

    if not active_rising:
        return False

    if sniper_density < V53_MIN_DENSITY or sniper_density > V53_MAX_DENSITY:
        return False

    if sniper_density_delta < V53_MIN_DENSITY_DELTA or sniper_density_delta > V53_MAX_DENSITY_DELTA:
        return False

    return True

def passes_v6_shadow(momentum, trend, sniper_density, sniper_density_delta):
    return (
        V6_MIN_TREND <= trend <= V6_MAX_TREND
        and momentum >= V6_MIN_MOMENTUM
        and V6_MIN_DENSITY <= sniper_density <= V6_MAX_DENSITY
        and V6_MIN_DELTA <= sniper_density_delta <= V6_MAX_DELTA
    )

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

def safe_update_trade_telemetry(cur, tid, telemetry):
    allowed = {}

    for col, val in telemetry.items():
        if column_exists(cur, "bot_trades_v4", col):
            allowed[col] = val

    if not allowed:
        return

    set_sql = ", ".join([f"{col} = %s" for col in allowed.keys()])
    values = list(allowed.values()) + [tid]

    cur.execute(
        f"""
        UPDATE bot_trades_v4
        SET {set_sql}
        WHERE id = %s
        """,
        values
    )

def is_v7_trade(entry_quality, is_shadow):
    return (
        not is_shadow
        and entry_quality == "V7_MAIN_BROAD"
    )

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

        if ENABLE_ORDER_LOGGING:
            ensure_okx_order_log_table(cur)

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

        print(
            f"🧠 REGIME | {regime_state} | sniper_now={sniper_now} | "
            f"prev={sniper_before} | delta={sniper_delta} | active={active_rising}",
            flush=True
        )

        # ================= ENTRY DECISION =================
        entry_allowed = False
        block_reason = None
        live_entry_quality = None

        if decision in ["LONG", "SHORT"]:

            if decision == "LONG":
                if passes_v7_main(momentum, trend):
                    entry_allowed = True
                    live_entry_quality = "V7_MAIN_BROAD"
                else:
                    entry_allowed = False
                    if trend < V7_MIN_TREND:
                        block_reason = "v7_trend_too_weak"
                    elif momentum <= V7_MIN_MOMENTUM:
                        block_reason = "v7_momentum_not_positive"
                    else:
                        block_reason = "v7_main_filter_block"

            elif decision == "SHORT":
                # S5 shorts are now SHADOW ONLY.
                # No real SHORT trades are opened.
                entry_allowed = False
                short_tier = classify_short_tier(symbol, momentum, trend, sniper_now)

                if ENABLE_SHADOW_S5_SHORT_ENGINE and short_tier:
                    block_reason = "s5_shadow_only"
                else:
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

                send_telegram_alert(
                    f"🚀 <b>OPEN REAL</b>\n"
                    f"{symbol} | {decision}\n"
                    f"Entry: {price}\n"
                    f"Engine: {live_entry_quality}\n"
                    f"Regime: {regime_state}\n"
                    f"Density: {sniper_now} | Delta: {sniper_delta}\n"
                    f"Trade ID: {trade_id}"
                )

                # OKX EXECUTION LAYER — real trades only.
                # Live order is skipped automatically if OKX account cannot trade symbol.
                okx_place_market_order(
                    cur=cur,
                    trade_id=trade_id,
                    symbol=symbol,
                    direction=decision,
                    action="entry",
                    price=price,
                    entry_price=price
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
        cur.execute("""
            SELECT COUNT(*)
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = TRUE
        """)
        open_shadow_count = cur.fetchone()[0] or 0

        if open_shadow_count < MAX_OPEN_SHADOW_TRADES:

            if decision == "LONG":

                if ENABLE_SHADOW_V53_SNIPER and passes_v53_shadow(
                    momentum,
                    trend,
                    regime_state,
                    active_rising,
                    sniper_now,
                    sniper_delta
                ):
                    shadow_id = open_trade(
                        cur,
                        symbol,
                        "LONG",
                        price,
                        momentum,
                        trend,
                        "SHADOW_V53_SNIPER",
                        regime_state,
                        signal_id,
                        signal_time,
                        is_shadow=True
                    )
                    print(f"👻 OPEN SHADOW V5.3 SNIPER | {symbol} | id={shadow_id}", flush=True)

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

            elif decision == "SHORT":

                short_tier = classify_short_tier(symbol, momentum, trend, sniper_now)

                if ENABLE_SHADOW_S5_SHORT_ENGINE and short_tier:
                    shadow_id = open_trade(
                        cur,
                        symbol,
                        "SHORT",
                        price,
                        momentum,
                        trend,
                        short_tier,
                        regime_state,
                        signal_id,
                        signal_time,
                        is_shadow=True
                    )
                    print(f"👻 OPEN SHADOW S5 SHORT | {symbol} | id={shadow_id} | tier={short_tier}", flush=True)

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
            exit_architecture = None
            decay_triggered = False

            # U2 CONFIRMATION EXIT
            if ENABLE_CONFIRMATION_EXIT:
                if update_count == CONFIRMATION_UPDATE_NUM and pnl_percent < CONFIRMATION_MIN_PNL:
                    close_reason = "failed_confirmation"
                    exit_architecture = "confirmation_exit"

            # V7 STRUCTURAL TREND DECAY EXIT
            if (
                not close_reason
                and direction == "LONG"
                and ENABLE_V7_TREND_DECAY_EXIT
                and is_v7_trade(entry_quality, is_shadow)
                and mins >= V7_DECAY_MINUTES
                and current_peak < V7_DECAY_MAX_PEAK
                and trend < V7_DECAY_TREND_THRESHOLD
            ):
                close_reason = "v7_trend_decay_exit"
                exit_architecture = "v7_single_confirm_decay_cut_zero"
                decay_triggered = True

                safe_update_trade_telemetry(cur, tid, {
                    "decay_triggered": True,
                    "decay_checked_at_minutes": mins,
                    "decay_peak_at_check": current_peak,
                    "decay_trend_at_check": trend,
                    "decay_momentum_at_check": momentum,
                    "slot_recycle_candidate": True,
                    "exit_architecture": exit_architecture
                })

            # LONG EXIT MODEL
            if not close_reason and direction == "LONG":

                if ENABLE_PROFIT_LOCKS:
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

            # SHORT EXIT MODEL
            if not close_reason and direction == "SHORT":

                if current_peak >= SHORT_TP_1_TRIGGER and pnl_percent <= SHORT_TP_1_FLOOR:
                    close_reason = "short_tp_1_lock"
                    exit_architecture = "short_tactical_lock"

                elif current_peak >= SHORT_TP_05_TRIGGER and pnl_percent <= SHORT_TP_05_FLOOR:
                    close_reason = "short_tp_0_5_lock"
                    exit_architecture = "short_tactical_lock"

                elif pnl_percent <= SHORT_HARD_STOP:
                    close_reason = "short_hard_stop"
                    exit_architecture = "short_hard_stop"

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

                safe_update_trade_telemetry(cur, tid, {
                    "decay_triggered": decay_triggered,
                    "decay_checked_at_minutes": mins if decay_triggered else None,
                    "decay_peak_at_check": current_peak if decay_triggered else None,
                    "decay_trend_at_check": trend if decay_triggered else None,
                    "decay_momentum_at_check": momentum if decay_triggered else None,
                    "slot_recycle_candidate": decay_triggered,
                    "exit_architecture": exit_architecture
                })

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

                # OKX EXIT EXECUTION — real trades only.
                # Safety rule:
                # Only send an OKX sell if this exact trade had a successful live OKX buy.
                # This prevents sell attempts for legacy DB trades, failed entries, dry-runs,
                # or restricted-symbol skipped entries.
                if not is_shadow:
                    if has_successful_okx_live_entry(cur, tid):
                        okx_place_market_order(
                            cur=cur,
                            trade_id=tid,
                            symbol=sym,
                            direction=direction,
                            action="exit",
                            price=price,
                            entry_price=entry_price
                        )
                    else:
                        log_okx_exit_skip_no_live_entry(
                            cur=cur,
                            trade_id=tid,
                            symbol=sym,
                            direction=direction,
                            price=price
                        )

                trade_type = "SHADOW" if is_shadow else "REAL"

                print(
                    f"💰 CLOSED {trade_type} | {sym} | {direction} | "
                    f"{round(pnl_percent,3)}% | peak={round(current_peak,3)} | "
                    f"{close_reason}",
                    flush=True
                )

                if not is_shadow:
                    send_telegram_alert(
                        f"💰 <b>CLOSED REAL</b>\n"
                        f"{sym} | {direction}\n"
                        f"PnL: {fmt_num(pnl_percent)}%\n"
                        f"Peak: {fmt_num(current_peak)}%\n"
                        f"Reason: {close_reason}\n"
                        f"Exit architecture: {exit_architecture}"
                    )

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok", "version": DATA_VERSION}), 200

    except Exception as e:
        print("❌ ERROR:", e, flush=True)
        return jsonify({"error": str(e)}), 400

# =========================
# OKX TRADABILITY SCANNER ROUTES
# =========================

@app.route("/okx_tradability_scan", methods=["GET"])
def okx_tradability_scan():
    """
    Scans recent bot symbols against OKX account-level SPOT instruments.
    This identifies which strategy symbols can actually be executed live on this OKX account.
    """
    try:
        days = int(request.args.get("days", "14") or 14)
        limit = int(request.args.get("limit", "300") or 300)
        force = request.args.get("force", "true").lower() == "true"

        tradability_result = refresh_okx_tradable_spot_instruments(force=force)

        conn = get_db()
        cur = conn.cursor()

        cur.execute("""
            SELECT
                symbol,
                COUNT(*) AS signal_count,
                MAX(timestamp) AS last_seen
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' days')::INTERVAL
              AND symbol IS NOT NULL
            GROUP BY symbol
            ORDER BY signal_count DESC
            LIMIT %s
        """, (days, limit))

        rows = cur.fetchall()

        cur.close()
        conn.close()

        scan = []
        tradable = []
        restricted_or_unavailable = []

        for symbol, signal_count, last_seen in rows:
            okx_inst_id = okx_symbol_to_inst_id(symbol).upper()
            is_tradable = okx_inst_id in OKX_TRADABLE_SPOT_INST_IDS

            item = {
                "symbol": symbol,
                "okx_inst_id": okx_inst_id,
                "signal_count": signal_count,
                "last_seen": last_seen.isoformat() if last_seen else None,
                "okx_live_tradable": is_tradable
            }

            scan.append(item)

            if is_tradable:
                tradable.append(item)
            else:
                restricted_or_unavailable.append(item)

        return jsonify({
            "status": "ok",
            "version": DATA_VERSION,
            "days": days,
            "limit": limit,
            "okx_account_instruments_success": tradability_result.get("success"),
            "okx_account_spot_pair_count": len(OKX_TRADABLE_SPOT_INST_IDS),
            "cache_updated_at": OKX_TRADABILITY_CACHE_UPDATED_AT.isoformat() if OKX_TRADABILITY_CACHE_UPDATED_AT else None,
            "tradable_count": len(tradable),
            "restricted_or_unavailable_count": len(restricted_or_unavailable),
            "tradable": tradable,
            "restricted_or_unavailable": restricted_or_unavailable,
            "all": scan,
            "okx_error": tradability_result.get("error")
        }), 200

    except Exception as e:
        print("❌ OKX TRADABILITY SCAN ERROR:", e, flush=True)
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

@app.route("/telegram_test", methods=["GET"])
def telegram_test():
    try:
        sent = send_telegram_alert(
            f"📲 <b>Telegram test successful</b>\n"
            f"Bot version: {DATA_VERSION}\n"
            f"Live orders: {ENABLE_LIVE_ORDERS}\n"
            f"OKX tradability filter: {ENABLE_OKX_TRADABILITY_FILTER}"
        )

        return jsonify({
            "status": "ok",
            "telegram_enabled": ENABLE_TELEGRAM_ALERTS,
            "telegram_configured": bool(TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID),
            "sent": sent
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 400

@app.route("/", methods=["GET"])
def home():
    return jsonify({
        "status": "running",
        "version": DATA_VERSION,
        "okx_execution": bool_status()
    }), 200
