# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v5.7
# TITLE: V7 CORE £15 + MONSTER-CATCHER RESERVED SLOTS + ADAPTIVE LIFECYCLE EXITS + TELEGRAM COMMANDS/SUMMARIES + OKX EXECUTION LAYER
# =========================

print("🔥🔥🔥 MAIN.PY v5.7 + CORE £15 + MONSTER-CATCHER RESERVED SLOTS + TELEGRAM OPS RUNNING 🔥🔥🔥", flush=True)

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

MAX_OPEN_TRADES = 5
MAX_OPEN_SHADOW_TRADES = 30
TRADE_SIZE_GBP = 15

DATA_VERSION = "v5.7"

# =========================
# 🔌 OKX EXECUTION SETTINGS
# =========================

OKX_API_KEY = os.environ.get("OKX_API_KEY")
OKX_API_SECRET = os.environ.get("OKX_API_SECRET")
OKX_API_PASSPHRASE = os.environ.get("OKX_API_PASSPHRASE")
OKX_BASE_URL = os.environ.get("OKX_BASE_URL", "https://www.okx.com").rstrip("/")

ENABLE_ORDER_LOGGING = os.environ.get("ENABLE_ORDER_LOGGING", "true").lower() == "true"
ENABLE_LIVE_ORDERS = os.environ.get("ENABLE_LIVE_ORDERS", "false").lower() == "true"

LIVE_TRADE_SIZE_GBP = float(os.environ.get("LIVE_TRADE_SIZE_GBP", "15") or 15)
MAX_LIVE_OPEN_TRADES = int(os.environ.get("MAX_LIVE_OPEN_TRADES", "8") or 8)

OKX_TD_MODE = os.environ.get("OKX_TD_MODE", "cash")
OKX_ORDER_TYPE = os.environ.get("OKX_ORDER_TYPE", "market")

# OKX current spot taker fee is 0.10% for this account.
# We use a larger 0.50% exit-size buffer for live stack-safe exits to avoid
# precision, fee, and dust rejection issues.
OKX_SPOT_TAKER_FEE_RATE = float(os.environ.get("OKX_SPOT_TAKER_FEE_RATE", "0.001") or 0.001)
OKX_EXIT_SIZE_BUFFER = float(os.environ.get("OKX_EXIT_SIZE_BUFFER", "0.995") or 0.995)

# =========================
# 📲 TELEGRAM ALERT SETTINGS
# =========================

ENABLE_TELEGRAM_ALERTS = os.environ.get("ENABLE_TELEGRAM_ALERTS", "false").lower() == "true"
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID")
TELEGRAM_COMMAND_SECRET = os.environ.get("TELEGRAM_COMMAND_SECRET")
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET")

# Send rolling 24h summaries from an external cron by calling /telegram_summary_24h
# at 08:00 and 20:00 UK time.
ENABLE_TELEGRAM_COMMANDS = os.environ.get("ENABLE_TELEGRAM_COMMANDS", "true").lower() == "true"


# OKX spot market buys use quote currency when tgtCcy="quote_ccy".
LIVE_TRADE_SIZE_QUOTE = float(os.environ.get("LIVE_TRADE_SIZE_QUOTE", LIVE_TRADE_SIZE_GBP) or LIVE_TRADE_SIZE_GBP)

# =========================
# 🛡️ SAME-SYMBOL STACKING CONTROL
# =========================

MAX_SAME_SYMBOL_OPEN = int(os.environ.get("MAX_SAME_SYMBOL_OPEN", "1") or 1)
ENABLE_SAME_SYMBOL_STACKING_LIMIT = os.environ.get("ENABLE_SAME_SYMBOL_STACKING_LIMIT", "true").lower() == "true"

# =========================
# 🔥 V5.7 MONSTER-CATCHER RESERVED ENGINE
# =========================

ENABLE_MONSTER_CATCHER = os.environ.get("ENABLE_MONSTER_CATCHER", "true").lower() == "true"
MONSTER_RESERVED_SLOTS = int(os.environ.get("MONSTER_RESERVED_SLOTS", "3") or 3)
MONSTER_TRADE_SIZE_GBP = float(os.environ.get("MONSTER_TRADE_SIZE_GBP", "35") or 35)
MONSTER_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("MONSTER_MAX_SAME_SYMBOL_OPEN", "2") or 2)
MONSTER_LOOKBACK_MINUTES = int(os.environ.get("MONSTER_LOOKBACK_MINUTES", "120") or 120)
MONSTER_MIN_PRIOR_SUCCESSES = int(os.environ.get("MONSTER_MIN_PRIOR_SUCCESSES", "2") or 2)
MONSTER_MIN_PRIOR_AVG_PEAK = float(os.environ.get("MONSTER_MIN_PRIOR_AVG_PEAK", "1.50") or 1.50)
MONSTER_MIN_DENSITY = int(os.environ.get("MONSTER_MIN_DENSITY", "10") or 10)
MONSTER_MIN_DENSITY_DELTA = int(os.environ.get("MONSTER_MIN_DENSITY_DELTA", "3") or 3)


# =========================
# 🛡️ OKX TRADABILITY FILTER
# =========================

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

V7_MIN_TREND = 0.28
V7_MIN_MOMENTUM = 0.00

# Shadow-only V7 lower-trend bucket.
ENABLE_SHADOW_V7_LOW_TREND = True
SHADOW_V7_LOW_MIN_TREND = 0.20
SHADOW_V7_LOW_MAX_TREND = 0.28
SHADOW_V7_LOW_MIN_MOMENTUM = 0.00

# Legacy validated V7 decay exit, retained as fallback.
ENABLE_V7_TREND_DECAY_EXIT = True
V7_DECAY_MINUTES = 120
V7_DECAY_TREND_THRESHOLD = 0.22
V7_DECAY_MAX_PEAK = 0.50

# =========================
# ♻️ V5.6 ADAPTIVE LIFECYCLE EXITS
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

ENABLE_CONFIRMATION_EXIT = False
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
    sent = telegram_send_message(TELEGRAM_CHAT_ID, message)
    if sent:
        print("📲 TELEGRAM ALERT SENT", flush=True)
    return sent

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
        print("⚠️ TELEGRAM SEND SKIPPED | missing token or chat_id", flush=True)
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
        print(f"⚠️ TELEGRAM SEND FAILED | status={response.status_code} | body={response.text}", flush=True)
        return False
    except Exception as e:
        print(f"⚠️ TELEGRAM SEND ERROR | {e}", flush=True)
        return False

def get_trade_size_for_quality(entry_quality):
    if entry_quality == "V7_MONSTER_CATCHER":
        return MONSTER_TRADE_SIZE_GBP
    return TRADE_SIZE_GBP

def get_trade_size_quote_for_quality(entry_quality):
    if entry_quality == "V7_MONSTER_CATCHER":
        return MONSTER_TRADE_SIZE_GBP
    return LIVE_TRADE_SIZE_QUOTE

def is_authorized_telegram_chat(chat_id):
    if not TELEGRAM_CHAT_ID:
        return False
    return str(chat_id) == str(TELEGRAM_CHAT_ID)

# =========================
# OKX HELPERS
# =========================

def bool_status():
    return {
        "DATA_VERSION": DATA_VERSION,
        "MAX_OPEN_TRADES": MAX_OPEN_TRADES,
        "TRADE_SIZE_GBP": TRADE_SIZE_GBP,
        "LIVE_TRADE_SIZE_GBP": LIVE_TRADE_SIZE_GBP,
        "ENABLE_ORDER_LOGGING": ENABLE_ORDER_LOGGING,
        "ENABLE_LIVE_ORDERS": ENABLE_LIVE_ORDERS,
        "MAX_LIVE_OPEN_TRADES": MAX_LIVE_OPEN_TRADES,
        "LIVE_TRADE_SIZE_QUOTE": LIVE_TRADE_SIZE_QUOTE,
        "MAX_SAME_SYMBOL_OPEN": MAX_SAME_SYMBOL_OPEN,
        "ENABLE_SAME_SYMBOL_STACKING_LIMIT": ENABLE_SAME_SYMBOL_STACKING_LIMIT,
        "ENABLE_MONSTER_CATCHER": ENABLE_MONSTER_CATCHER,
        "MONSTER_RESERVED_SLOTS": MONSTER_RESERVED_SLOTS,
        "MONSTER_TRADE_SIZE_GBP": MONSTER_TRADE_SIZE_GBP,
        "MONSTER_MAX_SAME_SYMBOL_OPEN": MONSTER_MAX_SAME_SYMBOL_OPEN,
        "MONSTER_LOOKBACK_MINUTES": MONSTER_LOOKBACK_MINUTES,
        "MONSTER_MIN_PRIOR_SUCCESSES": MONSTER_MIN_PRIOR_SUCCESSES,
        "MONSTER_MIN_PRIOR_AVG_PEAK": MONSTER_MIN_PRIOR_AVG_PEAK,
        "MONSTER_MIN_DENSITY": MONSTER_MIN_DENSITY,
        "MONSTER_MIN_DENSITY_DELTA": MONSTER_MIN_DENSITY_DELTA,
        "ENABLE_DEAD_LEADER_RECYCLER": ENABLE_DEAD_LEADER_RECYCLER,
        "DEAD_LEADER_MINUTES": DEAD_LEADER_MINUTES,
        "DEAD_LEADER_MAX_PEAK": DEAD_LEADER_MAX_PEAK,
        "DEAD_LEADER_TREND_THRESHOLD": DEAD_LEADER_TREND_THRESHOLD,
        "ENABLE_ADAPTIVE_WINNER_PROTECTION": ENABLE_ADAPTIVE_WINNER_PROTECTION,
        "ADAPTIVE_TREND_WEAK_THRESHOLD": ADAPTIVE_TREND_WEAK_THRESHOLD,
        "OKX_BASE_URL": OKX_BASE_URL,
        "OKX_TD_MODE": OKX_TD_MODE,
        "ENABLE_OKX_TRADABILITY_FILTER": ENABLE_OKX_TRADABILITY_FILTER,
        "OKX_TRADABILITY_CACHE_SECONDS": OKX_TRADABILITY_CACHE_SECONDS,
        "OKX_TRADABLE_SPOT_COUNT": len(OKX_TRADABLE_SPOT_INST_IDS),
        "OKX_TRADABILITY_CACHE_UPDATED_AT": OKX_TRADABILITY_CACHE_UPDATED_AT.isoformat() if OKX_TRADABILITY_CACHE_UPDATED_AT else None,
        "OKX_TRADABILITY_LAST_ERROR": OKX_TRADABILITY_LAST_ERROR,
        "ENABLE_TELEGRAM_ALERTS": ENABLE_TELEGRAM_ALERTS,
        "ENABLE_TELEGRAM_COMMANDS": ENABLE_TELEGRAM_COMMANDS,
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

def okx_inst_id_to_base_ccy(okx_inst_id):
    """
    Converts OKX instId like BTC-USDT into BTC.
    """
    if not okx_inst_id or "-" not in okx_inst_id:
        return None

    return okx_inst_id.split("-")[0].upper()

def okx_get_available_balance(ccy):
    """
    Gets actual available OKX balance for a currency.
    Used for exits so we sell what OKX actually holds after fees/rounding,
    instead of a theoretical calculated base size.
    """
    if not ccy:
        return {
            "success": False,
            "available": 0.0,
            "error": "missing_currency",
            "response": None
        }

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
                available_raw = (
                    item.get("availBal")
                    or item.get("availableBal")
                    or item.get("cashBal")
                    or "0"
                )

                return {
                    "success": True,
                    "available": float(available_raw or 0),
                    "error": None,
                    "response": response_payload
                }

        return {
            "success": True,
            "available": 0.0,
            "error": None,
            "response": response_payload
        }

    except Exception as e:
        return {
            "success": False,
            "available": 0.0,
            "error": str(e),
            "response": response_payload
        }

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

def get_live_core_open_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND COALESCE(entry_quality, '') <> 'V7_MONSTER_CATCHER'
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

def get_open_monster_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND entry_quality = 'V7_MONSTER_CATCHER'
    """)
    return cur.fetchone()[0] or 0

def get_open_monster_same_symbol_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND entry_quality = 'V7_MONSTER_CATCHER'
          AND symbol = %s
    """, (symbol,))
    return cur.fetchone()[0] or 0

def get_monster_leadership_context(cur, symbol):
    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE COALESCE(peak_pnl_percent, 0) >= 0.75) AS prior_successes,
            AVG(COALESCE(peak_pnl_percent, 0)) AS prior_avg_peak
        FROM bot_trades_v4
        WHERE symbol = %s
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND opened_at >= NOW() - (%s || ' minutes')::INTERVAL
          AND opened_at < NOW()
    """, (symbol, MONSTER_LOOKBACK_MINUTES))

    row = cur.fetchone() or (0, 0)
    prior_successes = row[0] or 0
    prior_avg_peak = float(row[1] or 0)
    return prior_successes, prior_avg_peak

def passes_monster_catcher(cur, symbol, sniper_density, sniper_density_delta):
    if not ENABLE_MONSTER_CATCHER:
        return False, {"reason": "monster_disabled"}

    prior_successes, prior_avg_peak = get_monster_leadership_context(cur, symbol)
    open_monsters = get_open_monster_count(cur)
    open_monsters_same_symbol = get_open_monster_same_symbol_count(cur, symbol)

    context = {
        "prior_successes": prior_successes,
        "prior_avg_peak": prior_avg_peak,
        "open_monsters": open_monsters,
        "open_monsters_same_symbol": open_monsters_same_symbol,
        "reason": None
    }

    if prior_successes < MONSTER_MIN_PRIOR_SUCCESSES:
        context["reason"] = "monster_prior_successes_too_low"
        return False, context

    if prior_avg_peak < MONSTER_MIN_PRIOR_AVG_PEAK:
        context["reason"] = "monster_prior_avg_peak_too_low"
        return False, context

    if sniper_density < MONSTER_MIN_DENSITY:
        context["reason"] = "monster_density_too_low"
        return False, context

    if sniper_density_delta < MONSTER_MIN_DENSITY_DELTA:
        context["reason"] = "monster_density_delta_too_low"
        return False, context

    if open_monsters >= MONSTER_RESERVED_SLOTS:
        context["reason"] = "monster_reserved_slots_full"
        return False, context

    if open_monsters_same_symbol >= MONSTER_MAX_SAME_SYMBOL_OPEN:
        context["reason"] = "monster_same_symbol_limit"
        return False, context

    context["reason"] = "monster_allowed"
    return True, context

def has_successful_okx_live_entry(cur, trade_id):
    """
    Safety gate for exits.
    Only send a live OKX exit if this exact bot trade previously had a successful,
    non-dry-run OKX entry order.
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

def calculate_exit_base_size(entry_price, trade_size_quote=None):
    if entry_price <= 0:
        return 0

    quote_size = float(trade_size_quote if trade_size_quote is not None else LIVE_TRADE_SIZE_QUOTE)
    base_size = quote_size / float(entry_price)
    return round(base_size, 8)

def okx_place_market_order(cur, trade_id, symbol, direction, action, price=None, entry_price=None, trade_size_quote=None):
    """
    action:
      - "entry": real LONG entry = buy
      - "exit": real LONG exit = sell
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
        quote_size = float(trade_size_quote if trade_size_quote is not None else LIVE_TRADE_SIZE_QUOTE)
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

                print(
                    f"🛡️ OKX EXIT BLOCKED | {symbol} -> {okx_inst_id} | "
                    f"reason=could_not_fetch_available_balance | error={balance_result.get('error')}",
                    flush=True
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
            theoretical_trade_size = calculate_exit_base_size(reference_price, trade_size_quote)

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

                print(
                    f"🛡️ OKX EXIT SKIPPED | {symbol} -> {okx_inst_id} | "
                    f"available_{base_ccy}={available_balance}",
                    flush=True
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
            sell_size = calculate_exit_base_size(reference_price, trade_size_quote)

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
        if live_open_count >= MAX_LIVE_OPEN_TRADES:
            error_message = f"MAX_LIVE_OPEN_TRADES reached: {live_open_count} >= {MAX_LIVE_OPEN_TRADES}"

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

def passes_shadow_v7_low_trend(momentum, trend):
    return (
        ENABLE_SHADOW_V7_LOW_TREND
        and SHADOW_V7_LOW_MIN_TREND <= trend < SHADOW_V7_LOW_MAX_TREND
        and momentum > SHADOW_V7_LOW_MIN_MOMENTUM
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
        monster_entry = False
        monster_context = {}

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
                entry_allowed = False
                short_tier = classify_short_tier(symbol, momentum, trend, sniper_now)

                if ENABLE_SHADOW_S5_SHORT_ENGINE and short_tier:
                    block_reason = "s5_shadow_only"
                else:
                    block_reason = "short_not_s5_tactical"

            if ENABLE_MAX_OPEN_TRADES and entry_allowed:
                open_count = get_live_core_open_count(cur)

                if open_count >= MAX_OPEN_TRADES:
                    entry_allowed = False
                    block_reason = "max_open_trades"
                    print(f"⛔ BLOCKED | max open core trades reached: {open_count}", flush=True)

            if entry_allowed and ENABLE_SAME_SYMBOL_STACKING_LIMIT:
                same_symbol_count = get_open_same_symbol_real_count(cur, symbol)

                if same_symbol_count >= MAX_SAME_SYMBOL_OPEN:
                    entry_allowed = False
                    block_reason = "max_same_symbol_open"
                    print(
                        f"⛔ BLOCKED | same symbol open limit reached: "
                        f"{symbol} count={same_symbol_count} max={MAX_SAME_SYMBOL_OPEN}",
                        flush=True
                    )

            # V5.7 MONSTER-CATCHER RESERVED ENGINE
            # Only allows a blocked V7 long to override normal core caps when
            # persistent rotational leadership is confirmed.
            if (
                decision == "LONG"
                and not entry_allowed
                and block_reason in ["max_open_trades", "max_same_symbol_open"]
                and passes_v7_main(momentum, trend)
            ):
                monster_allowed, monster_context = passes_monster_catcher(
                    cur,
                    symbol,
                    sniper_now,
                    sniper_delta
                )

                if monster_allowed:
                    entry_allowed = True
                    block_reason = None
                    live_entry_quality = "V7_MONSTER_CATCHER"
                    monster_entry = True
                    print(
                        f"🔥 MONSTER CATCHER ALLOWED | {symbol} | "
                        f"prior_successes={monster_context.get('prior_successes')} | "
                        f"prior_avg_peak={round(monster_context.get('prior_avg_peak', 0),3)} | "
                        f"open_monsters={monster_context.get('open_monsters')} | "
                        f"same_symbol_monsters={monster_context.get('open_monsters_same_symbol')}",
                        flush=True
                    )
                else:
                    print(
                        f"🧯 MONSTER CHECK FAILED | {symbol} | "
                        f"reason={monster_context.get('reason')} | "
                        f"prior_successes={monster_context.get('prior_successes')} | "
                        f"prior_avg_peak={round(monster_context.get('prior_avg_peak', 0),3)}",
                        flush=True
                    )

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

                entry_trade_size = get_trade_size_for_quality(live_entry_quality)
                entry_quote_size = get_trade_size_quote_for_quality(live_entry_quality)

                safe_update_trade_telemetry(cur, trade_id, {
                    "entry_architecture": live_entry_quality,
                    "trade_size_gbp": entry_trade_size,
                    "monster_catcher_triggered": monster_entry,
                    "monster_prior_successes": monster_context.get("prior_successes"),
                    "monster_prior_avg_peak": monster_context.get("prior_avg_peak"),
                    "monster_density": sniper_now,
                    "monster_density_delta": sniper_delta
                })

                print(
                    f"🚀 OPEN REAL | {symbol} | {decision} | id={trade_id} | "
                    f"Q={live_entry_quality} | size=£{entry_trade_size} | regime={regime_state} | "
                    f"density={sniper_now} | delta={sniper_delta}",
                    flush=True
                )

                if monster_entry:
                    send_telegram_alert(
                        f"🔥 <b>MONSTER ENTRY</b>\n"
                        f"{symbol} | {decision}\n"
                        f"Size: £{entry_trade_size}\n"
                        f"Entry: {price}\n"
                        f"Prior successes: {monster_context.get('prior_successes')}\n"
                        f"Prior avg peak: {fmt_num(monster_context.get('prior_avg_peak'))}%\n"
                        f"Density: {sniper_now} | Delta: {sniper_delta}\n"
                        f"Trade ID: {trade_id}"
                    )
                else:
                    send_telegram_alert(
                        f"🚀 <b>CORE ENTRY</b>\n"
                        f"{symbol} | {decision}\n"
                        f"Size: £{entry_trade_size}\n"
                        f"Entry: {price}\n"
                        f"Engine: {live_entry_quality}\n"
                        f"Regime: {regime_state}\n"
                        f"Density: {sniper_now} | Delta: {sniper_delta}\n"
                        f"Trade ID: {trade_id}"
                    )

                okx_place_market_order(
                    cur=cur,
                    trade_id=trade_id,
                    symbol=symbol,
                    direction=decision,
                    action="entry",
                    price=price,
                    entry_price=price,
                    trade_size_quote=entry_quote_size
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

                if passes_shadow_v7_low_trend(momentum, trend):
                    shadow_id = open_trade(
                        cur,
                        symbol,
                        "LONG",
                        price,
                        momentum,
                        trend,
                        "SHADOW_V7_LOW_TREND_020_028",
                        regime_state,
                        signal_id,
                        signal_time,
                        is_shadow=True
                    )
                    print(f"👻 OPEN SHADOW V7 LOW TREND | {symbol} | id={shadow_id}", flush=True)

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
            adaptive_exit_triggered = False
            slot_recycle_candidate = False
            drawdown_from_peak = current_peak - pnl_percent

            # U2 CONFIRMATION EXIT
            if ENABLE_CONFIRMATION_EXIT:
                if update_count == CONFIRMATION_UPDATE_NUM and pnl_percent < CONFIRMATION_MIN_PNL:
                    close_reason = "failed_confirmation"
                    exit_architecture = "confirmation_exit"

            # V5.6 DEAD LEADER RECYCLER — checked before profit locks/hard stops.
            if (
                not close_reason
                and direction == "LONG"
                and ENABLE_DEAD_LEADER_RECYCLER
                and is_v7_trade(entry_quality, is_shadow)
                and mins >= DEAD_LEADER_MINUTES
                and current_peak < DEAD_LEADER_MAX_PEAK
                and trend < DEAD_LEADER_TREND_THRESHOLD
            ):
                close_reason = "dead_leader_recycle_exit"
                exit_architecture = "v7_dead_leader_recycler"
                decay_triggered = True
                slot_recycle_candidate = True

            # Legacy V7 structural trend decay fallback.
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
                slot_recycle_candidate = True

            # V5.6 ADAPTIVE WINNER PROTECTION — leadership deterioration + drawdown from peak.
            if (
                not close_reason
                and direction == "LONG"
                and ENABLE_ADAPTIVE_WINNER_PROTECTION
                and is_v7_trade(entry_quality, is_shadow)
            ):
                if (
                    current_peak >= ADAPTIVE_LARGE_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_LARGE_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_large"
                    exit_architecture = "v7_adaptive_winner_protection"
                    adaptive_exit_triggered = True

                elif (
                    current_peak >= ADAPTIVE_MEDIUM_PEAK_TRIGGER
                    and current_peak < ADAPTIVE_LARGE_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_MEDIUM_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_medium"
                    exit_architecture = "v7_adaptive_winner_protection"
                    adaptive_exit_triggered = True

                elif (
                    current_peak >= ADAPTIVE_SMALL_PEAK_TRIGGER
                    and current_peak < ADAPTIVE_MEDIUM_PEAK_TRIGGER
                    and trend < ADAPTIVE_TREND_WEAK_THRESHOLD
                    and drawdown_from_peak >= ADAPTIVE_SMALL_DRAWDOWN
                ):
                    close_reason = "adaptive_winner_protect_small"
                    exit_architecture = "v7_adaptive_winner_protection"
                    adaptive_exit_triggered = True

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
                trade_size_for_pnl = get_trade_size_for_quality(entry_quality)
                pnl_gbp = 0 if is_shadow else (pnl_percent / 100) * trade_size_for_pnl

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
                    "leadership_trend_at_exit": trend
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

                if not is_shadow:
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

                trade_type = "SHADOW" if is_shadow else "REAL"

                print(
                    f"💰 CLOSED {trade_type} | {sym} | {direction} | "
                    f"{round(pnl_percent,3)}% | peak={round(current_peak,3)} | "
                    f"dd_from_peak={round(drawdown_from_peak,3)} | {close_reason}",
                    flush=True
                )

                if not is_shadow:
                    send_telegram_alert(
                        f"💰 <b>CLOSED REAL</b>\n"
                        f"{sym} | {direction}\n"
                        f"PnL: {fmt_num(pnl_percent)}%\n"
                        f"Peak: {fmt_num(current_peak)}%\n"
                        f"Drawdown from peak: {fmt_num(drawdown_from_peak)}%\n"
                        f"Trend at exit: {fmt_num(trend)}\n"
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
# TELEGRAM OPS / SUMMARY HELPERS
# =========================

def require_summary_secret():
    if not TELEGRAM_COMMAND_SECRET:
        return True
    return request.args.get("secret") == TELEGRAM_COMMAND_SECRET

def build_telegram_summary_message(cur, hours=24):
    cur.execute("""
        WITH closed AS (
            SELECT
                CASE WHEN entry_quality = 'V7_MONSTER_CATCHER' THEN 'MONSTER' ELSE 'CORE' END AS engine,
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
        WITH latest_price AS (
            SELECT DISTINCT ON (symbol)
                symbol,
                price
            FROM signals_raw
            WHERE timestamp >= NOW() - INTERVAL '6 hours'
            ORDER BY symbol, timestamp DESC
        ),
        open_trades AS (
            SELECT
                b.id,
                b.symbol,
                b.direction,
                b.entry_price,
                b.opened_at,
                b.entry_quality,
                COALESCE(b.peak_pnl_percent, 0) AS peak_pnl_percent,
                lp.price AS current_price
            FROM bot_trades_v4 b
            LEFT JOIN latest_price lp ON lp.symbol = b.symbol
            WHERE b.status = 'OPEN'
              AND COALESCE(b.is_shadow, FALSE) = FALSE
        )
        SELECT
            symbol,
            entry_quality,
            ROUND(entry_price::numeric, 6),
            ROUND(current_price::numeric, 6),
            ROUND((CASE
                WHEN current_price IS NULL OR entry_price = 0 THEN NULL
                WHEN direction = 'LONG' THEN ((current_price - entry_price) / entry_price) * 100
                ELSE ((entry_price - current_price) / entry_price) * 100
            END)::numeric, 3) AS current_pnl,
            ROUND(peak_pnl_percent::numeric, 3) AS peak,
            ROUND(EXTRACT(EPOCH FROM (NOW() - opened_at)) / 60.0, 1) AS mins_open
        FROM open_trades
        ORDER BY entry_quality DESC, opened_at
        LIMIT 15
    """)
    open_rows = cur.fetchall()

    cur.execute("""
        SELECT
            COALESCE(block_reason, 'allowed') AS block_reason,
            COUNT(*)
        FROM signals_raw
        WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
        GROUP BY COALESCE(block_reason, 'allowed')
        ORDER BY COUNT(*) DESC
        LIMIT 6
    """, (hours,))
    block_rows = cur.fetchall()

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'OPEN' AND entry_quality = 'V7_MONSTER_CATCHER') AS open_monsters,
            COUNT(*) FILTER (WHERE status = 'OPEN' AND entry_quality <> 'V7_MONSTER_CATCHER') AS open_core,
            COUNT(*) FILTER (WHERE status = 'OPEN') AS open_total
        FROM bot_trades_v4
        WHERE COALESCE(is_shadow, FALSE) = FALSE
    """)
    open_counts = cur.fetchone() or (0, 0, 0)

    total_pnl = sum(float(r[2] or 0) for r in closed_rows)
    total_trades = sum(int(r[1] or 0) for r in closed_rows)

    lines = []
    lines.append(f"📊 <b>Trading Bot {hours}h Summary</b>")
    lines.append(f"Version: <b>{DATA_VERSION}</b>")
    lines.append(f"Closed trades: <b>{total_trades}</b>")
    lines.append(f"Realized PnL: <b>{fmt_money(total_pnl)}</b>")
    lines.append(f"Open: <b>{open_counts[2] or 0}</b> total | Core {open_counts[1] or 0} | Monster {open_counts[0] or 0}")

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

    if open_rows:
        lines.append("\n<b>Open trades</b>")
        for symbol, quality, entry_price, current_price, current_pnl, peak, mins_open in open_rows:
            engine = "🔥" if quality == "V7_MONSTER_CATCHER" else "🟢"
            pnl_txt = "n/a" if current_pnl is None else f"{fmt_num(current_pnl)}%"
            lines.append(f"{engine} {symbol} | {pnl_txt} | peak {fmt_num(peak)}% | {fmt_num(mins_open,1)}m")

    if block_rows:
        lines.append("\n<b>Signal blocks</b>")
        for reason, count in block_rows:
            lines.append(f"{reason}: {count}")

    return "\n".join(lines)

def build_telegram_recent_message(cur, limit=8):
    cur.execute("""
        SELECT
            symbol,
            entry_quality,
            closed_at,
            ROUND(pnl_percent::numeric, 3),
            ROUND(pnl_gbp::numeric, 3),
            ROUND(peak_pnl_percent::numeric, 3),
            close_reason
        FROM bot_trades_v4
        WHERE status = 'CLOSED'
          AND COALESCE(is_shadow, FALSE) = FALSE
        ORDER BY closed_at DESC
        LIMIT %s
    """, (limit,))
    rows = cur.fetchall()
    if not rows:
        return "No recent closed real trades."
    lines = ["🧾 <b>Recent Closed Trades</b>"]
    for symbol, quality, closed_at, pnl, pnl_gbp, peak, reason in rows:
        engine = "🔥" if quality == "V7_MONSTER_CATCHER" else "🟢"
        lines.append(f"{engine} {symbol} | {fmt_num(pnl)}% | {fmt_money(pnl_gbp)} | peak {fmt_num(peak)}% | {reason}")
    return "\n".join(lines)

def build_telegram_monster_message(cur):
    cur.execute("""
        SELECT
            symbol,
            opened_at,
            ROUND(entry_price::numeric, 6),
            ROUND(COALESCE(peak_pnl_percent,0)::numeric, 3)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND entry_quality = 'V7_MONSTER_CATCHER'
        ORDER BY opened_at
    """)
    rows = cur.fetchall()
    lines = ["🔥 <b>Monster Engine Status</b>"]
    lines.append(f"Slots: {MONSTER_RESERVED_SLOTS} | Size: £{MONSTER_TRADE_SIZE_GBP} | Same-symbol max: {MONSTER_MAX_SAME_SYMBOL_OPEN}")
    lines.append(f"Rule: prior≥{MONSTER_MIN_PRIOR_SUCCESSES}, avg_peak≥{MONSTER_MIN_PRIOR_AVG_PEAK}, density≥{MONSTER_MIN_DENSITY}, delta≥{MONSTER_MIN_DENSITY_DELTA}")
    if not rows:
        lines.append("No open monster trades.")
    else:
        lines.append("\n<b>Open monster trades</b>")
        for symbol, opened_at, entry_price, peak in rows:
            mins = (datetime.now(timezone.utc) - opened_at.replace(tzinfo=timezone.utc)).total_seconds() / 60 if opened_at.tzinfo is None else (datetime.now(timezone.utc) - opened_at).total_seconds() / 60
            lines.append(f"🔥 {symbol} | entry {entry_price} | peak {fmt_num(peak)}% | {fmt_num(mins,1)}m")
    return "\n".join(lines)

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

    return (
        f"🩺 <b>Bot Health</b>\n"
        f"Version: {DATA_VERSION}\n"
        f"Live orders: {ENABLE_LIVE_ORDERS}\n"
        f"Last signal: {last_signal}\n"
        f"Signals 1h: {signals_1h}\n"
        f"Last real trade: {last_trade}\n"
        f"Real trades 24h: {trades_24h}\n"
        f"Open real: {open_real}\n"
        f"Telegram: {ENABLE_TELEGRAM_ALERTS}\n"
        f"OKX tradable cache: {len(OKX_TRADABLE_SPOT_INST_IDS)} pairs"
    )

def handle_telegram_command(text):
    cmd = (text or "").strip().lower().split()[0] if text else "/help"
    conn = get_db()
    cur = conn.cursor()
    try:
        if cmd in ["/status", "/daily", "/summary", "/pnl"]:
            return build_telegram_summary_message(cur, 24)
        if cmd == "/monster":
            return build_telegram_monster_message(cur)
        if cmd == "/health":
            return build_telegram_health_message(cur)
        if cmd == "/recent":
            return build_telegram_recent_message(cur)
        if cmd == "/help":
            return (
                "🤖 <b>Trading Bot Commands</b>\n"
                "/status - rolling 24h summary\n"
                "/daily - rolling 24h summary\n"
                "/monster - monster engine status\n"
                "/health - webhook/server health\n"
                "/recent - recent closed trades\n"
                "/help - command list"
            )
        return "Unknown command. Send /help."
    finally:
        cur.close()
        conn.close()

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
        return jsonify({
            "status": "ok",
            "version": DATA_VERSION,
            "sent": sent,
            "hours": 24
        }), 200

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

@app.route("/telegram_set_webhook", methods=["GET"])
def telegram_set_webhook():
    try:
        if not require_summary_secret():
            return jsonify({"error": "unauthorized"}), 403

        public_url = os.environ.get("PUBLIC_BASE_URL") or request.args.get("base_url")
        if not public_url:
            return jsonify({
                "error": "missing_PUBLIC_BASE_URL",
                "message": "Set PUBLIC_BASE_URL=https://your-render-app.onrender.com or pass ?base_url="
            }), 400

        if not TELEGRAM_BOT_TOKEN:
            return jsonify({"error": "missing_TELEGRAM_BOT_TOKEN"}), 400

        webhook_url = f"{public_url.rstrip('/')}/telegram_webhook"
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook"
        payload = {"url": webhook_url}
        if TELEGRAM_WEBHOOK_SECRET:
            payload["secret_token"] = TELEGRAM_WEBHOOK_SECRET

        response = requests.post(url, json=payload, timeout=10)
        try:
            response_payload = response.json()
        except Exception:
            response_payload = {"status_code": response.status_code, "text": response.text}

        return jsonify({
            "status": "ok" if response.status_code == 200 else "telegram_error",
            "webhook_url": webhook_url,
            "telegram_response": response_payload
        }), 200 if response.status_code == 200 else 400

    except Exception as e:
        print("❌ TELEGRAM SET WEBHOOK ERROR:", e, flush=True)
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
