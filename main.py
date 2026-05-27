# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v6.6.10
# TITLE: V6.6.10 REAL TRADE PERSISTENCE FIX
# =========================

print("🔥🔥🔥 MAIN.PY v6.6.0 BPT CQE LIFECYCLE SHADOW + LEADERSHIP LIVE RUNNING 🔥🔥🔥", flush=True)

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
# ✅ v6.1.5 adds lifecycle telemetry, shadow emergence scanner fields, phase-at-entry/exit data, and cleaner Telegram ops
# ✅ v6.1.6 adds SHADOW_CQE_V1 paper-trade engine, Telegram shadow alerts, and richer blocked-signal telemetry
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
# ✅ v6.2.0 adds BPT_CQE_LIFECYCLE_V1 shadow architecture
#      - £5 probe → confirmation → lifecycle row routing
#      - simulated upgrades after 30m +1% confirmation
#      - row-specific wide monster trails
#      - safe live toggles default OFF
# ✅ Existing leadership live engine is preserved unchanged by default
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



# =========================
# 🕒 TIMEZONE SAFETY HELPERS
# =========================

from datetime import timezone

def ensure_utc(dt):
    """
    Convert naive/aware datetimes safely into UTC-aware datetimes.
    Prevents:
    can't subtract offset-naive and offset-aware datetimes
    """
    if dt is None:
        return None

    try:
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return dt




# =========================
# 🔄 OKX CACHE SELF-HEAL
# =========================

LAST_OKX_CACHE_REFRESH = None

def okx_cache_health_summary():
    try:
        count = get_okx_cache_count() if 'okx_tradable_pairs' in globals() else 0
        return f"{count} pairs"
    except Exception:
        return "unknown"

def emergency_refresh_okx_cache():
    global LAST_OKX_CACHE_REFRESH

    try:
        print("🚨 OKX CACHE EMPTY — triggering emergency refresh", flush=True)

        refresh_okx_tradable_pairs()

        LAST_OKX_CACHE_REFRESH = datetime.now(timezone.utc)

        refreshed_count = get_okx_cache_count() if 'okx_tradable_pairs' in globals() else 0

        print(f"✅ OKX CACHE RECOVERED | {refreshed_count} pairs", flush=True)

        return refreshed_count

    except Exception as e:
        print(f"❌ OKX CACHE RECOVERY FAILED: {e}", flush=True)
        return 0




# =========================
# 📲 TELEGRAM DELIVERY FLAGS
# =========================

# Future DB migration placeholders:
# telegram_entry_sent
# telegram_exit_sent
# telegram_bank_sent

def safe_send_telegram_message(message):
    """
    Wrapper placeholder for safer Telegram delivery.
    Future versions can attach DB dedupe tracking here.
    """
    try:
        return send_telegram_message(message)
    except Exception as e:
        print(f"⚠️ Telegram send failed: {e}", flush=True)
        return False




# =========================
# 🚨 v6.6.7 EMERGENCY LIVE ENTRY GUARD
# =========================

RECENT_REAL_ENTRY_SYMBOLS = {}
EMERGENCY_REAL_SAME_SYMBOL_COOLDOWN_MINUTES = 180
ENABLE_EMERGENCY_SAME_SYMBOL_GUARD = True

def utc_now():
    return datetime.now(timezone.utc)

def safe_age_minutes(start_dt, end_dt=None):
    try:
        start_dt = ensure_utc(start_dt)
        end_dt = ensure_utc(end_dt or utc_now())
        if start_dt is None or end_dt is None:
            return None
        return (end_dt - start_dt).total_seconds() / 60.0
    except Exception as e:
        print(f"⚠️ safe_age_minutes failed: {e}", flush=True)
        return None

def emergency_same_symbol_guard(symbol):
    """
    Last-resort in-memory guard to stop repeated OKX live buys if DB persistence lags/fails.
    """
    if not ENABLE_EMERGENCY_SAME_SYMBOL_GUARD:
        return True, None

    try:
        now = utc_now()
        last = RECENT_REAL_ENTRY_SYMBOLS.get(symbol)

        if last is not None:
            elapsed_min = safe_age_minutes(last, now)
            if elapsed_min is not None and elapsed_min < EMERGENCY_REAL_SAME_SYMBOL_COOLDOWN_MINUTES:
                return False, f"emergency_same_symbol_guard_{round(elapsed_min,1)}m"

        return True, None
    except Exception as e:
        print(f"⚠️ emergency same-symbol guard failed for {symbol}: {e}", flush=True)
        return False, "emergency_guard_error"

def mark_emergency_real_entry(symbol):
    try:
        RECENT_REAL_ENTRY_SYMBOLS[symbol] = utc_now()
    except Exception as e:
        print(f"⚠️ mark emergency entry failed for {symbol}: {e}", flush=True)




# =========================
# 🔄 v6.6.8 OKX CACHE HARD SELF-HEAL
# =========================

OKX_CACHE_LAST_FORCE_REFRESH = None
OKX_CACHE_FORCE_REFRESH_COOLDOWN_SECONDS = 60

def get_okx_cache_count():
    """
    v6.6.9: count the real OKX tradability cache used by execution.
    Primary cache is OKX_TRADABLE_SPOT_INST_IDS, not okx_tradable_pairs.
    """
    try:
        if "OKX_TRADABLE_SPOT_INST_IDS" in globals() and OKX_TRADABLE_SPOT_INST_IDS is not None:
            return len(OKX_TRADABLE_SPOT_INST_IDS)
        if "okx_tradable_pairs" in globals() and okx_tradable_pairs is not None:
            return get_okx_cache_count()
        if "OKX_TRADABLE_PAIRS" in globals() and OKX_TRADABLE_PAIRS is not None:
            return get_okx_cache_count()
        return 0
    except Exception:
        return 0

def force_okx_cache_refresh_if_empty(reason="unknown"):
    """
    v6.6.9 hard self-heal.
    If tradability cache is 0, call the actual real refresh function:
    refresh_okx_tradable_spot_instruments(force=True)
    """
    global OKX_CACHE_LAST_FORCE_REFRESH

    try:
        count = get_okx_cache_count()
        if count > 0:
            return count

        now = utc_now() if "utc_now" in globals() else datetime.now(timezone.utc)
        if OKX_CACHE_LAST_FORCE_REFRESH is not None:
            age = (now - ensure_utc(OKX_CACHE_LAST_FORCE_REFRESH)).total_seconds()
            if age < OKX_CACHE_FORCE_REFRESH_COOLDOWN_SECONDS:
                return get_okx_cache_count()

        OKX_CACHE_LAST_FORCE_REFRESH = now
        print(f"🚨 OKX CACHE EMPTY | force refresh triggered | reason={reason}", flush=True)

        refreshed = False
        refresh_result = None

        # The real v6 OKX tradability cache refresh function.
        fn = globals().get("refresh_okx_tradable_spot_instruments")
        if callable(fn):
            try:
                refresh_result = fn(force=True)
                refreshed = True
            except Exception as e:
                print(f"⚠️ refresh_okx_tradable_spot_instruments(force=True) failed: {e}", flush=True)

        # Fallbacks for older names if future files rename it.
        if not refreshed:
            for fn_name in [
                "refresh_okx_tradable_pairs",
                "refresh_okx_tradability_cache",
                "load_okx_tradable_pairs",
                "build_okx_tradable_cache",
            ]:
                fn = globals().get(fn_name)
                if callable(fn):
                    try:
                        refresh_result = fn()
                        refreshed = True
                        break
                    except TypeError:
                        try:
                            refresh_result = fn(force=True)
                            refreshed = True
                            break
                        except Exception as e:
                            print(f"⚠️ {fn_name}(force=True) failed: {e}", flush=True)
                    except Exception as e:
                        print(f"⚠️ {fn_name} failed: {e}", flush=True)

        count_after = get_okx_cache_count()

        # If the refresh function returned count but the global count is still not visible, use result count for log clarity.
        result_count = 0
        try:
            if isinstance(refresh_result, dict):
                result_count = int(refresh_result.get("count") or 0)
        except Exception:
            result_count = 0

        visible_count = max(count_after, result_count)

        if visible_count > 0:
            print(f"✅ OKX CACHE SELF-HEALED | {visible_count} pairs", flush=True)
        else:
            print(f"❌ OKX CACHE STILL EMPTY after refresh attempts | refreshed_called={refreshed}", flush=True)

        return visible_count

    except Exception as e:
        print(f"❌ OKX CACHE SELF-HEAL ERROR: {e}", flush=True)
        return get_okx_cache_count()

# =========================
# 🧠 v6.6.8 LEADERSHIP SCORE HEALTH FALLBACK
# =========================

def get_scored_leadership_count_24h(cur):
    """
    Health fallback: prefer signal_leadership_scores if populated,
    otherwise count recent leadership-joinable signal rows so health doesn't falsely show 0.
    """
    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM signal_leadership_scores
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        """)
        val = cur.fetchone()[0]
        if val and val > 0:
            return val
    except Exception as e:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        print(f"⚠️ signal_leadership_scores count fallback triggered: {e}", flush=True)

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM signals_raw s
            WHERE s.timestamp >= NOW() - INTERVAL '24 hours'
              AND EXISTS (
                SELECT 1
                FROM leadership_state_history h
                WHERE h.symbol = s.symbol
                  AND h.snapshot_time <= s.timestamp
              )
        """)
        return cur.fetchone()[0]
    except Exception as e:
        try:
            cur.connection.rollback()
        except Exception:
            pass
        print(f"⚠️ leadership joinable signal count failed: {e}", flush=True)
        return 0


app = Flask(__name__)
force_okx_cache_refresh_if_empty("startup")

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# CORE ENGINE SETTINGS
# =========================

MAX_OPEN_TRADES = int(os.environ.get("MAX_OPEN_TRADES", "5") or 5)
MAX_OPEN_SHADOW_TRADES = int(os.environ.get("MAX_OPEN_SHADOW_TRADES", "30") or 30)




DATA_VERSION = "v6.6.10_REAL_TRADE_PERSISTENCE_FIX"


# =========================
# 🚀 v6.6 MARKET OS SETTINGS
# =========================
# Simulation-backed architecture:
# - Keep elite/core leadership engine.
# - Scale size by leadership intensity instead of duplicating async entries.
# - Bank 25% at +4% on real core/leadership trades, then keep runner alive.
# - Add tiny rotational micro continuation layer as a separate toggleable engine.
# - Add Telegram market-state command so silence is explainable.

ENABLE_MARKET_OS_V66 = os.environ.get("ENABLE_MARKET_OS_V66", "true").lower() == "true"

# Leadership scaling / capital allocation
ENABLE_LEADERSHIP_SIZE_SCALING_V66 = os.environ.get("ENABLE_LEADERSHIP_SIZE_SCALING_V66", "true").lower() == "true"
LEADERSHIP_SCALE_THRESHOLD = float(os.environ.get("LEADERSHIP_SCALE_THRESHOLD", "2.0") or 2.0)
LEADERSHIP_SCALED_TRADE_SIZE_GBP = float(os.environ.get("LEADERSHIP_SCALED_TRADE_SIZE_GBP", "30") or 30)

# Partial profit bank: bank a small piece of position only after meaningful expansion.
ENABLE_PARTIAL_PROFIT_BANK_V66 = os.environ.get("ENABLE_PARTIAL_PROFIT_BANK_V66", "true").lower() == "true"
PARTIAL_BANK_TRIGGER_PCT = float(os.environ.get("PARTIAL_BANK_TRIGGER_PCT", "4.0") or 4.0)
PARTIAL_BANK_FRACTION = float(os.environ.get("PARTIAL_BANK_FRACTION", "0.25") or 0.25)

# Rotational micro continuation layer. Independent from elite core, tiny size only.
ENABLE_ROT_MICRO_LIVE = os.environ.get("ENABLE_ROT_MICRO_LIVE", "true").lower() == "true"
ROT_MICRO_TRADE_SIZE_GBP = float(os.environ.get("ROT_MICRO_TRADE_SIZE_GBP", "5") or 5)
ROT_MICRO_MIN_MOMENTUM = float(os.environ.get("ROT_MICRO_MIN_MOMENTUM", "0.30") or 0.30)
ROT_MICRO_MIN_TREND = float(os.environ.get("ROT_MICRO_MIN_TREND", "0.10") or 0.10)
ROT_MICRO_MIN_LEAD60 = float(os.environ.get("ROT_MICRO_MIN_LEAD60", "0.75") or 0.75)
ROT_MICRO_MAX_LEAD60 = float(os.environ.get("ROT_MICRO_MAX_LEAD60", "1.00") or 1.00)
ROT_MICRO_COOLDOWN_MINUTES = float(os.environ.get("ROT_MICRO_COOLDOWN_MINUTES", "90") or 90)

# Operational safety / self-healing
ENABLE_OKX_TRADABILITY_SELF_HEAL_V66 = os.environ.get("ENABLE_OKX_TRADABILITY_SELF_HEAL_V66", "true").lower() == "true"
ENABLE_STALE_SHADOW_SWEEP_V66 = os.environ.get("ENABLE_STALE_SHADOW_SWEEP_V66", "true").lower() == "true"
STALE_SHADOW_MAX_HOURS = float(os.environ.get("STALE_SHADOW_MAX_HOURS", "24") or 24)
ENABLE_MARKET_STATE_TELEGRAM_V66 = os.environ.get("ENABLE_MARKET_STATE_TELEGRAM_V66", "true").lower() == "true"





# =========================
# 🧬 v6.5 ARCHETYPE STATE ENGINE
# =========================
# Simulation-backed parameters as of 2026-05-25:
# - Best blanket lifecycle: kill no-progress after 120m if peak < 1%.
# - Let leaders breathe; only protect after peak >= 3% and DD >= 1.5%.
# - Do NOT remove max hold universally. Use adaptive/structural backstop.
# - Add archetype tagging for future separate exit engines.

ENABLE_ARCHETYPE_STATE_ENGINE = os.environ.get("ENABLE_ARCHETYPE_STATE_ENGINE", "true").lower() == "true"
ARCHETYPE_VERSION = "ARCHETYPE_V1"

ARCH_NO_PROGRESS_MINUTES = float(os.environ.get("ARCH_NO_PROGRESS_MINUTES", "120") or 120)
ARCH_NO_PROGRESS_PEAK = float(os.environ.get("ARCH_NO_PROGRESS_PEAK", "1.0") or 1.0)
ARCH_PROTECT_MINUTES = float(os.environ.get("ARCH_PROTECT_MINUTES", "60") or 60)
ARCH_PROTECT_PEAK = float(os.environ.get("ARCH_PROTECT_PEAK", "3.0") or 3.0)
ARCH_PROTECT_DRAWDOWN = float(os.environ.get("ARCH_PROTECT_DRAWDOWN", "1.5") or 1.5)
ARCH_LEADERSHIP_DECAY_RATIO = float(os.environ.get("ARCH_LEADERSHIP_DECAY_RATIO", "0.50") or 0.50)

# Safety backstop remains, but HIGH_MONSTER / grinder-style trades are no longer
# killed solely because the clock expired while DD is still healthy.
ENABLE_ADAPTIVE_BPT_MAX_HOLD = os.environ.get("ENABLE_ADAPTIVE_BPT_MAX_HOLD", "true").lower() == "true"
BPT_EXTEND_MAX_HOLD_IF_PEAK_ABOVE = float(os.environ.get("BPT_EXTEND_MAX_HOLD_IF_PEAK_ABOVE", "3.0") or 3.0)
BPT_EXTEND_MAX_HOLD_IF_DD_BELOW = float(os.environ.get("BPT_EXTEND_MAX_HOLD_IF_DD_BELOW", "1.5") or 1.5)

# Experimental: keep OFF by default. Allows a tiny real probe for validated persistence archetypes later.
ENABLE_GENERAL_LEADER_LIVE_PROBES = os.environ.get("ENABLE_GENERAL_LEADER_LIVE_PROBES", "false").lower() == "true"
GENERAL_LEADER_PROBE_SIZE_GBP = float(os.environ.get("GENERAL_LEADER_PROBE_SIZE_GBP", "5") or 5)

# Telegram de-duplication. Prevents repeated close alerts on every later signal.
ENABLE_TELEGRAM_DEDUPE = os.environ.get("ENABLE_TELEGRAM_DEDUPE", "true").lower() == "true"
# =========================
# 🧠 v6.3.1 LIVE LEARNING PATCH
# =========================
# Purpose:
# 1) Relax density dependency: missing density is no longer treated as low quality.
# 2) Keep current real leadership engine intact.
# 3) Keep Persistence Hunter shadow-only.
# 4) Avoid fixed-time exits on live real trades; use lifecycle/profit-lock/trailing/hard-stop logic instead.

ENABLE_RELAXED_DENSITY_DEPENDENCY = True
ENABLE_FIXED_TIME_EXITS_REAL = False
ENABLE_FIXED_TIME_EXITS_SHADOW = True

# Density interpretation update:
# - NULL density is allowed and treated as "unknown/clean", not failed.
# - Strong density should be interpreted as possible crowding/maturity context,
#   not mandatory entry confirmation.
DENSITY_NULL_IS_VALID = True
DENSITY_REQUIRED_FOR_LIVE_ENTRIES = False
DENSITY_REQUIRED_FOR_BPT_LIFECYCLE = False

# Optional future use: if density appears after entry, we can tighten trails later.
ENABLE_DENSITY_AS_CROWDING_TELEMETRY = True

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

# v6.1.5 shadow-only controlled ignition scanner.
# This does NOT make live entries unless ENABLE_EMERGING_LEADER_ENTRIES is also true.
ENABLE_SHADOW_EMERGENCE_TELEMETRY = os.environ.get(
    "ENABLE_SHADOW_EMERGENCE_TELEMETRY",
    "true"
).lower() == "true"

CONTROLLED_IGNITION_MIN_SCORE = float(os.environ.get("CONTROLLED_IGNITION_MIN_SCORE", "0.90") or 0.90)
CONTROLLED_IGNITION_MAX_SCORE = float(os.environ.get("CONTROLLED_IGNITION_MAX_SCORE", "1.50") or 1.50)
CONTROLLED_IGNITION_DELTA_MIN = float(os.environ.get("CONTROLLED_IGNITION_DELTA_MIN", "0.25") or 0.25)
CONTROLLED_IGNITION_DELTA_MAX = float(os.environ.get("CONTROLLED_IGNITION_DELTA_MAX", "0.50") or 0.50)

# v6.1.6 shadow-only quiet continuation scanner.
# This does NOT make live entries. It tags signals that look like the clean continuation
# opportunities found in SQL: low leadership score, moderate trend, moderate momentum,
# and no explosive/climax leadership acceleration.
ENABLE_SHADOW_QUIET_CONTINUATION = os.environ.get(
    "ENABLE_SHADOW_QUIET_CONTINUATION",
    "true"
).lower() == "true"

QUIET_CONTINUATION_MAX_SCORE = float(os.environ.get("QUIET_CONTINUATION_MAX_SCORE", "0.50") or 0.50)
QUIET_CONTINUATION_MIN_TREND = float(os.environ.get("QUIET_CONTINUATION_MIN_TREND", "0.25") or 0.25)
QUIET_CONTINUATION_MIN_MOMENTUM = float(os.environ.get("QUIET_CONTINUATION_MIN_MOMENTUM", "0.50") or 0.50)
QUIET_CONTINUATION_MAX_DELTA_30M = float(os.environ.get("QUIET_CONTINUATION_MAX_DELTA_30M", "1.00") or 1.00)

# v6.1.6 CQE shadow-only engine.
# This creates PAPER/SHADOW trades only. It never sends OKX orders.
ENABLE_SHADOW_CQE = os.environ.get("ENABLE_SHADOW_CQE", "true").lower() == "true"
ENABLE_SHADOW_CQE_TELEGRAM_ALERTS = os.environ.get(
    "ENABLE_SHADOW_CQE_TELEGRAM_ALERTS",
    "true"
).lower() == "true"
CQE_MIN_QUALITY_SCORE = int(os.environ.get("CQE_MIN_QUALITY_SCORE", "8") or 8)
CQE_SHADOW_HOLD_MINUTES = float(os.environ.get("CQE_SHADOW_HOLD_MINUTES", "120") or 120)
CQE_SHADOW_TRADE_SIZE_GBP = float(os.environ.get("CQE_SHADOW_TRADE_SIZE_GBP", "10") or 10)
CQE_MAX_OPEN_SHADOW_TRADES = int(os.environ.get("CQE_MAX_OPEN_SHADOW_TRADES", "20") or 20)
CQE_MAX_SAME_SYMBOL_SHADOW_OPEN = int(os.environ.get("CQE_MAX_SAME_SYMBOL_SHADOW_OPEN", "1") or 1)
CQE_PEAK_ALERT_TRIGGER = float(os.environ.get("CQE_PEAK_ALERT_TRIGGER", "0.75") or 0.75)
CQE_RUNNER_ALERT_TRIGGER = float(os.environ.get("CQE_RUNNER_ALERT_TRIGGER", "2.00") or 2.00)




# =========================
# 🧬 CQE LIVE SPECIALIST ENGINE v1
# =========================
ENABLE_LIVE_CQE = os.environ.get("ENABLE_LIVE_CQE", "false").lower() == "true"

LIVE_CQE_TRADE_SIZE_GBP = float(os.environ.get("LIVE_CQE_TRADE_SIZE_GBP", "5") or 5)
LIVE_CQE_MAX_OPEN_TRADES = int(os.environ.get("LIVE_CQE_MAX_OPEN_TRADES", "1") or 1)
LIVE_CQE_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("LIVE_CQE_MAX_SAME_SYMBOL_OPEN", "1") or 1)

LIVE_CQE_MIN_QUALITY_SCORE = int(os.environ.get("LIVE_CQE_MIN_QUALITY_SCORE", "8") or 8)
LIVE_CQE_MIN_LEADERSHIP = float(os.environ.get("LIVE_CQE_MIN_LEADERSHIP", "0.25") or 0.25)
LIVE_CQE_MAX_LEADERSHIP = float(os.environ.get("LIVE_CQE_MAX_LEADERSHIP", "1.50") or 1.50)

LIVE_CQE_HARD_STOP = float(os.environ.get("LIVE_CQE_HARD_STOP", "-0.60") or -0.60)


# =========================
# 🧬 BPT CQE LIFECYCLE SHADOW v1
# =========================
# Simulation-derived architecture:
# - cheap probe entries
# - upgrade only after early expansion confirmation
# - row-specific lifecycle exits
# - default is shadow-only beside current live engine

ENABLE_BPT_CQE_LIFECYCLE_SHADOW = os.environ.get(
    "ENABLE_BPT_CQE_LIFECYCLE_SHADOW", "true"
).lower() == "true"

# Future live toggles. Keep both false until shadow confirms live behaviour.
ENABLE_BPT_CQE_LIVE_PROBES = os.environ.get(
    "ENABLE_BPT_CQE_LIVE_PROBES", "false"
).lower() == "true"
ENABLE_BPT_CQE_LIVE_UPGRADES = os.environ.get(
    "ENABLE_BPT_CQE_LIVE_UPGRADES", "false"
).lower() == "true"

BPT_CQE_ENTRY_QUALITY = "BPT_CQE_LIFECYCLE_V1"
BPT_CQE_PROBE_SIZE_GBP = float(os.environ.get("BPT_CQE_PROBE_SIZE_GBP", "5") or 5)
BPT_CQE_MAX_OPEN_TRADES = int(os.environ.get("BPT_CQE_MAX_OPEN_TRADES", "5") or 5)
BPT_CQE_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("BPT_CQE_MAX_SAME_SYMBOL_OPEN", "1") or 1)

# Probe gate from CQE/quiet-continuation findings.
BPT_CQE_MIN_TREND = float(os.environ.get("BPT_CQE_MIN_TREND", "0.25") or 0.25)
BPT_CQE_MIN_MOMENTUM = float(os.environ.get("BPT_CQE_MIN_MOMENTUM", "0.50") or 0.50)
BPT_CQE_MAX_LEADERSHIP_SCORE = float(os.environ.get("BPT_CQE_MAX_LEADERSHIP_SCORE", "0.50") or 0.50)
BPT_CQE_MAX_LEADERSHIP_DELTA_30M = float(os.environ.get("BPT_CQE_MAX_LEADERSHIP_DELTA_30M", "1.00") or 1.00)

# Upgrade confirmation cluster from sims:
# 30m peak >= 1.0%, positive trend/momentum persistence.
BPT_CQE_CONFIRM_WINDOW_MINUTES = float(os.environ.get("BPT_CQE_CONFIRM_WINDOW_MINUTES", "30") or 30)
BPT_CQE_CONFIRM_PEAK = float(os.environ.get("BPT_CQE_CONFIRM_PEAK", "1.0") or 1.0)
BPT_CQE_CONFIRM_AVG_TREND = float(os.environ.get("BPT_CQE_CONFIRM_AVG_TREND", "0.05") or 0.05)
BPT_CQE_CONFIRM_AVG_MOMENTUM = float(os.environ.get("BPT_CQE_CONFIRM_AVG_MOMENTUM", "0.05") or 0.05)
BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT = int(os.environ.get("BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT", "0") or 0)

# Row thresholds: quality_score = trend + momentum.
BPT_EXTREME_QUALITY_SCORE = float(os.environ.get("BPT_EXTREME_QUALITY_SCORE", "2.0") or 2.0)
BPT_HIGH_QUALITY_SCORE = float(os.environ.get("BPT_HIGH_QUALITY_SCORE", "1.2") or 1.2)
BPT_MEDIUM_QUALITY_SCORE = float(os.environ.get("BPT_MEDIUM_QUALITY_SCORE", "0.6") or 0.6)

# Row-specific upgrade sizes and exits.
BPT_EXTREME_UPGRADE_GBP = float(os.environ.get("BPT_EXTREME_UPGRADE_GBP", "20") or 20)
BPT_EXTREME_TRAIL_ACTIVATION = float(os.environ.get("BPT_EXTREME_TRAIL_ACTIVATION", "2.0") or 2.0)
BPT_EXTREME_TRAIL_DRAWDOWN = float(os.environ.get("BPT_EXTREME_TRAIL_DRAWDOWN", "0.75") or 0.75)

BPT_HIGH_UPGRADE_GBP = float(os.environ.get("BPT_HIGH_UPGRADE_GBP", "25") or 25)
BPT_HIGH_TRAIL_ACTIVATION = float(os.environ.get("BPT_HIGH_TRAIL_ACTIVATION", "3.0") or 3.0)
BPT_HIGH_TRAIL_DRAWDOWN = float(os.environ.get("BPT_HIGH_TRAIL_DRAWDOWN", "1.0") or 1.0)

BPT_MEDIUM_UPGRADE_GBP = float(os.environ.get("BPT_MEDIUM_UPGRADE_GBP", "35") or 35)
BPT_MEDIUM_TRAIL_ACTIVATION = float(os.environ.get("BPT_MEDIUM_TRAIL_ACTIVATION", "5.0") or 5.0)
BPT_MEDIUM_TRAIL_DRAWDOWN = float(os.environ.get("BPT_MEDIUM_TRAIL_DRAWDOWN", "1.5") or 1.5)

BPT_EARLY_UPGRADE_GBP = float(os.environ.get("BPT_EARLY_UPGRADE_GBP", "10") or 10)
BPT_EARLY_TRAIL_ACTIVATION = float(os.environ.get("BPT_EARLY_TRAIL_ACTIVATION", "2.0") or 2.0)
BPT_EARLY_TRAIL_DRAWDOWN = float(os.environ.get("BPT_EARLY_TRAIL_DRAWDOWN", "0.75") or 0.75)
BPT_EARLY_FAILFAST_MINUTES = float(os.environ.get("BPT_EARLY_FAILFAST_MINUTES", "120") or 120)
BPT_EARLY_FAILFAST_PEAK = float(os.environ.get("BPT_EARLY_FAILFAST_PEAK", "0.25") or 0.25)

# Shadow safety backstop only. Not the main exit thesis.
BPT_CQE_MAX_HOLD_MINUTES = float(os.environ.get("BPT_CQE_MAX_HOLD_MINUTES", "720") or 720)
BPT_CQE_HARD_STOP = float(os.environ.get("BPT_CQE_HARD_STOP", "-0.60") or -0.60)


# =========================
# 🧲 PERSISTENCE HUNTER v1 — SHADOW ONLY
# =========================
# Research-derived thesis:
# no density + leadership >= 2.0 + stable persistence + trend/momentum continuation
# catches rotational leadership before crowding. Shadow-only until live telemetry confirms.

ENABLE_PERSISTENCE_HUNTER_SHADOW = os.environ.get("ENABLE_PERSISTENCE_HUNTER_SHADOW", "true").lower() == "true"
ENABLE_PERSISTENCE_HUNTER_LIVE = os.environ.get("ENABLE_PERSISTENCE_HUNTER_LIVE", "false").lower() == "true"

PH_ENTRY_QUALITY = "PERSISTENCE_HUNTER_V1"
PH_LIFECYCLE_ROW = "PERSISTENCE_HUNTER_ROW"
PH_SHADOW_SIZE_GBP = float(os.environ.get("PH_SHADOW_SIZE_GBP", "35") or 35)
PH_MAX_OPEN_TRADES = int(os.environ.get("PH_MAX_OPEN_TRADES", "5") or 5)
PH_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("PH_MAX_SAME_SYMBOL_OPEN", "1") or 1)

PH_MIN_MOMENTUM = float(os.environ.get("PH_MIN_MOMENTUM", "0.50") or 0.50)
PH_MIN_TREND = float(os.environ.get("PH_MIN_TREND", "0.25") or 0.25)
PH_MIN_LEADERSHIP_SCORE = float(os.environ.get("PH_MIN_LEADERSHIP_SCORE", "2.0") or 2.0)
PH_MIN_CORE_AGE_MINUTES = float(os.environ.get("PH_MIN_CORE_AGE_MINUTES", "45") or 45)
PH_MAX_CORE_AGE_MINUTES = float(os.environ.get("PH_MAX_CORE_AGE_MINUTES", "180") or 180)
PH_MIN_CORE_HITS_240M = int(os.environ.get("PH_MIN_CORE_HITS_240M", "12") or 12)

PH_TRAIL_ACTIVATION = float(os.environ.get("PH_TRAIL_ACTIVATION", "3.0") or 3.0)
PH_TRAIL_DRAWDOWN = float(os.environ.get("PH_TRAIL_DRAWDOWN", "1.0") or 1.0)
PH_MONSTER_TRAIL_ACTIVATION = float(os.environ.get("PH_MONSTER_TRAIL_ACTIVATION", "5.0") or 5.0)
PH_MONSTER_TRAIL_DRAWDOWN = float(os.environ.get("PH_MONSTER_TRAIL_DRAWDOWN", "1.5") or 1.5)
PH_HARD_STOP = float(os.environ.get("PH_HARD_STOP", "-0.80") or -0.80)
PH_MAX_HOLD_MINUTES = float(os.environ.get("PH_MAX_HOLD_MINUTES", "720") or 720)
PH_DENSITY_EXIT_TIGHTEN = os.environ.get("PH_DENSITY_EXIT_TIGHTEN", "true").lower() == "true"


# Dynamic sizing tiers.
CORE_TRADE_SIZE_GBP = float(os.environ.get("CORE_TRADE_SIZE_GBP", "20") or 20)
AGGRESSIVE_TRADE_SIZE_GBP = float(os.environ.get("AGGRESSIVE_TRADE_SIZE_GBP", "20") or 20)
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

def safe_update_signal_telemetry(cur, signal_id, telemetry):
    allowed = {}
    for col, val in telemetry.items():
        if column_exists(cur, "signals_raw", col):
            allowed[col] = val

    if not allowed:
        return

    set_sql = ", ".join([f"{col} = %s" for col in allowed.keys()])
    values = list(allowed.values()) + [signal_id]

    cur.execute(f"""
        UPDATE signals_raw
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
# 🧬 v6.5 TELEGRAM DEDUPE + ARCHETYPE HELPERS
# =========================

def ensure_telegram_alert_log_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS telegram_alert_log (
            id BIGSERIAL PRIMARY KEY,
            alert_key TEXT UNIQUE,
            trade_id TEXT,
            event_type TEXT,
            symbol TEXT,
            sent_at TIMESTAMPTZ DEFAULT NOW(),
            data_version TEXT
        )
    """)


def send_trade_telegram_once(cur, trade_id, event_type, symbol, message):
    """Send a Telegram trade alert once per trade/event type.
    Critical for shadow close alerts: prevents the same CLOSED message firing on every new webhook.
    """
    if not ENABLE_TELEGRAM_DEDUPE:
        return send_telegram_alert(message)
    try:
        ensure_telegram_alert_log_table(cur)
        alert_key = f"{event_type}:{trade_id}"
        cur.execute("""
            INSERT INTO telegram_alert_log (alert_key, trade_id, event_type, symbol, data_version)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (alert_key) DO NOTHING
            RETURNING id
        """, (alert_key, str(trade_id), event_type, symbol, DATA_VERSION))
        inserted = cur.fetchone()
        if inserted:
            return send_telegram_alert(message)
        print(f"🔕 TELEGRAM DEDUPE SKIP | {event_type} | {symbol} | {trade_id}", flush=True)
        return False
    except Exception as e:
        print(f"⚠️ Telegram de-dupe failed, sending normally | {e}", flush=True)
        return send_telegram_alert(message)


def classify_runtime_archetype(lifecycle_row=None, mins=0, current_peak=0, pnl_percent=0, trend=0, momentum=0, leadership_score=None, density_phase=None):
    """Runtime-safe archetype classifier.
    It does NOT use future data. Archetype can evolve during the trade.
    """
    try:
        mins = float(mins or 0)
        current_peak = float(current_peak or 0)
        pnl_percent = float(pnl_percent or 0)
        trend = float(trend or 0)
        momentum = float(momentum or 0)
        leadership_score = float(leadership_score or 0)
    except Exception:
        return "GENERAL_LEADER"

    if lifecycle_row == "HIGH_MONSTER_ROW" and current_peak >= 3.0:
        return "HIGH_MONSTER_ROW"
    if mins <= 60 and current_peak >= 2.0:
        return "FAST_IGNITION"
    if mins >= ARCH_NO_PROGRESS_MINUTES and current_peak < ARCH_NO_PROGRESS_PEAK:
        if leadership_score >= 1.25 and trend >= 0.20:
            return "POTENTIAL_LATE_BLOOMER"
        return "FAILED_NO_EXPANSION"
    if mins <= 240 and current_peak >= 3.0 and pnl_percent > 0 and trend >= 0.20:
        return "SLOW_GRINDER"
    if density_phase in ["CROWDING_EXPANDING", "CROWDED"] and current_peak >= 3.0:
        return "CROWDED_RUNNER"
    if current_peak >= 3.0:
        return "GENERAL_RUNNER"
    return "GENERAL_LEADER"


def ensure_archetype_state_columns(cur):
    cur.execute("""
        ALTER TABLE bot_trades_v4
        ADD COLUMN IF NOT EXISTS entry_archetype TEXT,
        ADD COLUMN IF NOT EXISTS current_archetype TEXT,
        ADD COLUMN IF NOT EXISTS archetype_version TEXT,
        ADD COLUMN IF NOT EXISTS archetype_exit_reason TEXT,
        ADD COLUMN IF NOT EXISTS archetype_updated_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS telegram_close_alert_sent BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS telegram_close_alert_sent_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS partial_bank_4_done BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS partial_bank_4_done_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS partial_bank_4_pct FLOAT,
        ADD COLUMN IF NOT EXISTS partial_bank_4_fraction FLOAT,
        ADD COLUMN IF NOT EXISTS partial_bank_realized_pnl_gbp FLOAT,
        ADD COLUMN IF NOT EXISTS market_os_engine TEXT,
        ADD COLUMN IF NOT EXISTS size_scaling_reason TEXT
    """)




# =========================
# 🚀 v6.6 MARKET OS HELPERS
# =========================

def get_recent_leadership_max(cur, symbol, minutes=60):
    try:
        ensure_leadership_state_history_table(cur)
        cur.execute("""
            SELECT MAX(leadership_score)
            FROM leadership_state_history
            WHERE symbol = %s
              AND snapshot_time >= NOW() - (%s || ' minutes')::INTERVAL
        """, (symbol, minutes))
        row = cur.fetchone()
        return float(row[0] or 0) if row else 0.0
    except Exception as e:
        print(f"⚠️ recent leadership max lookup failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return 0.0


def get_trade_size_for_context(entry_quality, leadership_context=None):
    """Simulation-backed sizing. Async overlap was mostly duplicate, so use leadership as a sizing layer."""
    try:
        if entry_quality == "ROT_MICRO_V1":
            return float(ROT_MICRO_TRADE_SIZE_GBP)
        score = 0.0
        if leadership_context:
            score = float(
                leadership_context.get("leadership_max_60m")
                or leadership_context.get("prior_avg_peak")
                or leadership_context.get("leadership_score")
                or 0
            )
        if ENABLE_LEADERSHIP_SIZE_SCALING_V66 and score >= LEADERSHIP_SCALE_THRESHOLD:
            return float(LEADERSHIP_SCALED_TRADE_SIZE_GBP)
    except Exception:
        pass
    return float(get_trade_size_for_quality(entry_quality))


def get_trade_size_quote_for_context(entry_quality, leadership_context=None):
    return get_trade_size_for_context(entry_quality, leadership_context)


def is_rot_micro_candidate(cur, symbol, momentum, trend):
    """Independent rotational micro layer. Small size only. Does not replace core."""
    if not (ENABLE_MARKET_OS_V66 and ENABLE_ROT_MICRO_LIVE):
        return False, "rot_micro_disabled", {}
    lead60 = get_recent_leadership_max(cur, symbol, 60)
    ctx = {
        "leadership_max_60m": lead60,
        "rot_micro_min_momentum": ROT_MICRO_MIN_MOMENTUM,
        "rot_micro_min_trend": ROT_MICRO_MIN_TREND,
    }
    if float(momentum or 0) < ROT_MICRO_MIN_MOMENTUM:
        return False, "rot_micro_low_momentum", ctx
    if float(trend or 0) < ROT_MICRO_MIN_TREND:
        return False, "rot_micro_low_trend", ctx
    if lead60 < ROT_MICRO_MIN_LEAD60:
        return False, "rot_micro_low_lead60", ctx
    if lead60 >= ROT_MICRO_MAX_LEAD60:
        return False, "rot_micro_lead_too_high_core_zone", ctx
    return True, "rot_micro_ok", ctx


def sweep_stale_shadow_trades(cur):
    if not ENABLE_STALE_SHADOW_SWEEP_V66:
        return 0
    try:
        cur.execute("""
            UPDATE bot_trades_v4
            SET status = 'STALE_SHADOW_EXPIRED',
                closed_at = COALESCE(closed_at, NOW()),
                close_reason = COALESCE(close_reason, 'v6_6_stale_shadow_sweep')
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = TRUE
              AND opened_at < NOW() - (%s || ' hours')::INTERVAL
            RETURNING id
        """, (STALE_SHADOW_MAX_HOURS,))
        rows = cur.fetchall() or []
        if rows:
            print(f"🧹 STALE SHADOW SWEEP | closed={len(rows)}", flush=True)
        return len(rows)
    except Exception as e:
        print(f"⚠️ stale shadow sweep failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return 0


def maybe_partial_profit_bank(cur, tid, sym, direction, entry_price, price, entry_quality, current_peak, pnl_percent, opened_at, mins, leadership_context=None):
    """Bank PARTIAL_BANK_FRACTION at +PARTIAL_BANK_TRIGGER_PCT once, keep the runner open."""
    if not ENABLE_PARTIAL_PROFIT_BANK_V66:
        return False
    if direction != "LONG":
        return False
    try:
        ensure_archetype_state_columns(cur)
        cur.execute("SELECT COALESCE(partial_bank_4_done, FALSE) FROM bot_trades_v4 WHERE id = %s", (tid,))
        row = cur.fetchone()
        already_done = bool(row[0]) if row else False
        if already_done:
            return False
        if float(current_peak or 0) < PARTIAL_BANK_TRIGGER_PCT:
            return False

        full_size = get_trade_size_for_context(entry_quality, leadership_context)
        bank_size = max(0.0, float(full_size) * float(PARTIAL_BANK_FRACTION))
        realized_pnl_gbp = (float(PARTIAL_BANK_TRIGGER_PCT) / 100.0) * bank_size

        if has_successful_okx_live_entry(cur, tid):
            okx_place_market_order(
                cur=cur,
                trade_id=tid,
                symbol=sym,
                direction=direction,
                action="exit",
                price=price,
                entry_price=entry_price,
                trade_size_quote=bank_size,
            )
        else:
            log_okx_exit_skip_no_live_entry(cur, tid, sym, direction, price)

        safe_update_trade_telemetry(cur, tid, {
            "partial_bank_4_done": True,
            "partial_bank_4_done_at": datetime.now(timezone.utc),
            "partial_bank_4_pct": PARTIAL_BANK_TRIGGER_PCT,
            "partial_bank_4_fraction": PARTIAL_BANK_FRACTION,
            "partial_bank_realized_pnl_gbp": realized_pnl_gbp,
            "exit_architecture": "partial_bank_25_at_4_runner_alive",
        })
        try:
            log_trade_event(cur, tid, sym, "partial_bank_4pct", price, pnl_percent, current_peak, mins, 0, 0, False)
        except Exception:
            pass
        send_trade_telegram_once(
            cur,
            tid,
            "partial_bank_4pct",
            sym,
            f"💚 <b>PARTIAL BANK</b> | {sym}\n"
            f"Banked {int(PARTIAL_BANK_FRACTION*100)}% at +{fmt_num(PARTIAL_BANK_TRIGGER_PCT)}%\n"
            f"Approx realised: {fmt_money(realized_pnl_gbp)} | Runner remains open\n"
            f"Current peak {fmt_num(current_peak)}% | PnL now {fmt_num(pnl_percent)}%\n"
            f"ID {tid}"
        )
        print(f"💚 PARTIAL BANK | {sym} | id={tid} | {PARTIAL_BANK_FRACTION*100:.0f}% @ {PARTIAL_BANK_TRIGGER_PCT}%", flush=True)
        return True
    except Exception as e:
        print(f"⚠️ partial bank skipped for {sym}/{tid}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return False


def build_market_state_message(cur, hours=3):
    force_okx_cache_refresh_if_empty("build_market_state_message")
    """Operator visibility: explains why bot is / isn't trading."""
    try:
        cur.execute("""
            WITH sig AS (
                SELECT * FROM signals_raw
                WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
            ), latest_leaders AS (
                SELECT DISTINCT ON (symbol)
                    symbol, leadership_score, avg_peak, leadership_mode, snapshot_time
                FROM leadership_state_history
                WHERE snapshot_time >= NOW() - (%s || ' hours')::INTERVAL
                ORDER BY symbol, snapshot_time DESC
            ), qual AS (
                SELECT
                    s.symbol,
                    s.momentum,
                    s.trend,
                    COALESCE(l.leadership_score,0) AS leadership_score
                FROM sig s
                LEFT JOIN latest_leaders l ON l.symbol = s.symbol
            )
            SELECT
                (SELECT COUNT(*) FROM sig) AS signals,
                (SELECT COUNT(DISTINCT symbol) FROM sig) AS signal_symbols,
                (SELECT COUNT(*) FROM latest_leaders WHERE leadership_score >= 2.0) AS elite_leaders,
                (SELECT COUNT(*) FROM latest_leaders WHERE leadership_score >= 1.25) AS core_leaders,
                (SELECT COUNT(*) FROM qual WHERE momentum >= 0.5 AND trend >= 0.25 AND leadership_score >= 1.25) AS core_candidates,
                (SELECT COUNT(*) FROM qual WHERE momentum >= 0.3 AND trend >= 0.10 AND leadership_score >= 0.75 AND leadership_score < 1.0) AS rot_candidates,
                (SELECT MAX(timestamp) FROM sig) AS last_signal
        """, (hours, hours))
        row = cur.fetchone() or (0,0,0,0,0,0,None)
        signals, signal_symbols, elite_leaders, core_leaders, core_candidates, rot_candidates, last_signal = row

        cur.execute("""
            SELECT symbol, ROUND(leadership_score::numeric,2), leadership_mode
            FROM leadership_state_history
            WHERE snapshot_time = (SELECT MAX(snapshot_time) FROM leadership_state_history)
            ORDER BY leadership_score DESC NULLS LAST
            LIMIT 5
        """)
        leaders = cur.fetchall() or []

        cur.execute("""
            SELECT COALESCE(block_reason,'no_block') AS reason, COUNT(*)
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
            GROUP BY 1
            ORDER BY COUNT(*) DESC
            LIMIT 5
        """, (hours,))
        reasons = cur.fetchall() or []

        if core_candidates and core_candidates > 0:
            regime = "IGNITION_READY"
            why = "Core-quality aligned candidates are appearing."
        elif rot_candidates and rot_candidates > 0:
            regime = "HEALTHY_ROTATION"
            why = "Rotational continuation exists, but elite/core alignment is limited."
        elif elite_leaders and elite_leaders > 0:
            regime = "FRAGMENTED_LEADERSHIP"
            why = "Leadership exists, but momentum/trend are not aligned enough."
        elif signals and signals > 100:
            regime = "ACTIVE_CHOP"
            why = "Many signals, but weak structural quality / low follow-through alignment."
        else:
            regime = "QUIET_OR_DEAD"
            why = "Low signal flow or no coherent leadership."

        lines = [
            f"📡 <b>Market State {hours}h</b>",
            f"Regime: <b>{regime}</b>",
            f"Why: {why}",
            f"Signals: <b>{signals}</b> across <b>{signal_symbols}</b> symbols | Last {last_signal}",
            f"Leaders >=2.0: <b>{elite_leaders}</b> | >=1.25: <b>{core_leaders}</b>",
            f"Core candidates: <b>{core_candidates}</b> | Rot micro candidates: <b>{rot_candidates}</b>",
            f"OKX tradable cache: <b>{len(OKX_TRADABLE_SPOT_INST_IDS)}</b> pairs",
        ]
        if leaders:
            lines.append("\n<b>Top leaders</b>")
            for sym, score, mode in leaders:
                lines.append(f"{sym}: {score} | {mode}")
        if reasons:
            lines.append("\n<b>Recent block/reject reasons</b>")
            for reason, count in reasons:
                lines.append(f"{reason}: {count}")
        return "\n".join(lines)
    except Exception as e:
        print(f"⚠️ market state message failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return f"📡 <b>Market State</b> unavailable: {e}"

def update_trade_archetype(cur, trade_id, archetype):
    try:
        ensure_archetype_state_columns(cur)
        safe_update_trade_telemetry(cur, trade_id, {
            "current_archetype": archetype,
            "archetype_version": ARCHETYPE_VERSION,
            "archetype_updated_at": datetime.now(timezone.utc),
        })
    except Exception as e:
        print(f"⚠️ archetype update skipped for {trade_id}: {e}", flush=True)


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

    if ENABLE_OKX_TRADABILITY_SELF_HEAL_V66 and len(OKX_TRADABLE_SPOT_INST_IDS) == 0:
        print("🚨 OKX tradability cache empty — forcing self-heal refresh", flush=True)
        result = refresh_okx_tradable_spot_instruments(force=True)
        if len(OKX_TRADABLE_SPOT_INST_IDS) == 0:
            send_telegram_alert(
                "🚨 <b>OKX TRADABILITY CACHE EMPTY</b>\n"
                "Auto-refresh failed or returned 0 pairs. Entries may be blocked until /okx_tradability_status?force=true succeeds."
            )

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

    lifecycle_ctx = get_lifecycle_context(cur, symbol)
    lifecycle_phase = lifecycle_ctx.get("lifecycle_phase") or phase

    return {
        "prior_successes": successful_signals or 0,
        "prior_runners": runners or 0,
        "prior_avg_peak": leadership_score,
        "leadership_score": leadership_score,
        "score_30m_ago": score_30m_ago_float,
        "delta_30m": delta_30m,
        "leadership_phase": phase,
        "lifecycle_phase": lifecycle_phase,
        "prior_lifecycle_phase": lifecycle_ctx.get("prior_lifecycle_phase"),
        "leadership_transition": lifecycle_ctx.get("leadership_transition"),
        "leadership_delta_5m": lifecycle_ctx.get("leadership_delta_5m"),
        "leadership_delta_15m": lifecycle_ctx.get("leadership_delta_15m"),
        "leadership_delta_30m": lifecycle_ctx.get("leadership_delta_30m"),
        "leadership_delta_60m": lifecycle_ctx.get("leadership_delta_60m"),
        "leadership_score_30m_ago": lifecycle_ctx.get("leadership_score_30m_ago"),
        "leadership_age_minutes": lifecycle_ctx.get("leadership_age_minutes"),
        "leadership_peak_score_last_4h": lifecycle_ctx.get("leadership_peak_score_last_4h"),
        "leadership_rank": lifecycle_ctx.get("leadership_rank"),
        "market_near_count": lifecycle_ctx.get("market_near_count"),
        "market_core_count": lifecycle_ctx.get("market_core_count"),
        "market_aggressive_count": lifecycle_ctx.get("market_aggressive_count"),
        "market_monster_count": lifecycle_ctx.get("market_monster_count"),
        "shadow_emergence_detected": lifecycle_ctx.get("shadow_emergence_detected"),
        "shadow_emergence_reason": lifecycle_ctx.get("shadow_emergence_reason"),
        "leadership_mode": leadership_mode,
        "avg_peak": float(avg_peak or 0),
        "avg_worst": float(avg_worst or 0),
        "monsters": monsters or 0,
        "snapshot_time": snapshot_time
    }

def is_shadow_quiet_continuation_candidate(leadership, momentum, trend):
    if not ENABLE_SHADOW_QUIET_CONTINUATION or not leadership:
        return False, None

    score = float(leadership.get("leadership_score") or leadership.get("prior_avg_peak") or 0)
    delta_30m = leadership.get("leadership_delta_30m")
    if delta_30m is None:
        delta_30m = leadership.get("delta_30m")
    delta_30m = float(delta_30m or 0)

    if score >= QUIET_CONTINUATION_MAX_SCORE:
        return False, None
    if trend < QUIET_CONTINUATION_MIN_TREND:
        return False, None
    if momentum < QUIET_CONTINUATION_MIN_MOMENTUM:
        return False, None
    if delta_30m > QUIET_CONTINUATION_MAX_DELTA_30M:
        return False, None

    return True, "low_score_healthy_trend_momentum_no_climax"



def get_cqe_context(cur, symbol, signal_time, momentum, trend, leadership_context=None):
    """
    SHADOW_CQE_V1 no-leakage continuation quality model.
    Uses only data known at signal time:
    - current trend/momentum
    - prior 30m trend/momentum stats
    - leadership delta only as anti-noise/context, not primary edge
    """
    if not ENABLE_SHADOW_CQE:
        return {"cqe_detected": False, "cqe_reason": "cqe_disabled", "cqe_quality_score": 0}

    try:
        cur.execute("""
            SELECT
                AVG(momentum),
                AVG(trend),
                STDDEV(trend),
                MIN(trend),
                MAX(trend),
                COUNT(*) FILTER (WHERE trend > 0),
                COUNT(*)
            FROM signals_raw
            WHERE symbol = %s
              AND timestamp >= %s - INTERVAL '30 minutes'
              AND timestamp < %s
        """, (symbol, signal_time, signal_time))

        row = cur.fetchone() or (None, None, None, None, None, 0, 0)
        avg_momentum_30m, avg_trend_30m, trend_std_30m, min_trend_30m, max_trend_30m, positive_count, total_count = row

        if not total_count or total_count < 3 or avg_momentum_30m is None or avg_trend_30m is None:
            return {
                "cqe_detected": False,
                "cqe_reason": "insufficient_prior_30m_data",
                "cqe_quality_score": 0,
                "cqe_total_trend_count_30m": total_count or 0,
            }

        avg_momentum_30m = float(avg_momentum_30m or 0)
        avg_trend_30m = float(avg_trend_30m or 0)
        trend_std_30m = float(trend_std_30m or 0)
        min_trend_30m = float(min_trend_30m or 0)
        max_trend_30m = float(max_trend_30m or 0)
        trend_range_30m = max_trend_30m - min_trend_30m
        positive_trend_ratio = float(positive_count or 0) / float(total_count or 1)
        momentum_accel_30m = float(momentum or 0) - avg_momentum_30m
        trend_accel_30m = float(trend or 0) - avg_trend_30m

        delta_30m = 0.0
        if leadership_context:
            raw_delta = leadership_context.get("leadership_delta_30m")
            if raw_delta is None:
                raw_delta = leadership_context.get("delta_30m")
            delta_30m = float(raw_delta or 0)

        q = 0
        checks = {}
        checks["trend_ge_035"] = float(trend or 0) >= 0.35
        checks["momentum_ge_090"] = float(momentum or 0) >= 0.90
        checks["momentum_accel_025_050"] = 0.25 <= momentum_accel_30m <= 0.50
        checks["trend_accel_008_015"] = 0.08 <= trend_accel_30m <= 0.15
        checks["trend_std_lt_008"] = trend_std_30m < 0.08
        checks["trend_range_lt_020"] = trend_range_30m < 0.20
        checks["positive_trend_ratio_ge_095"] = positive_trend_ratio >= 0.95
        checks["min_trend_gt_015"] = min_trend_30m > 0.15
        checks["delta_abs_le_050"] = abs(delta_30m) <= 0.50

        for passed in checks.values():
            q += 1 if passed else 0

        detected = q >= CQE_MIN_QUALITY_SCORE
        reason = "cqe_quality_8_plus" if detected else f"cqe_quality_too_low_{q}"

        return {
            "cqe_detected": detected,
            "cqe_reason": reason,
            "cqe_quality_score": q,
            "cqe_avg_momentum_30m": avg_momentum_30m,
            "cqe_avg_trend_30m": avg_trend_30m,
            "cqe_momentum_accel_30m": momentum_accel_30m,
            "cqe_trend_accel_30m": trend_accel_30m,
            "cqe_trend_std_30m": trend_std_30m,
            "cqe_min_trend_30m": min_trend_30m,
            "cqe_max_trend_30m": max_trend_30m,
            "cqe_trend_range_30m": trend_range_30m,
            "cqe_positive_trend_ratio_30m": positive_trend_ratio,
            "cqe_total_trend_count_30m": total_count,
            "cqe_delta_30m": delta_30m,
            **{f"cqe_check_{k}": v for k, v in checks.items()}
        }

    except Exception as e:
        print(f"⚠️ CQE context failed for {symbol}: {e}", flush=True)
        return {"cqe_detected": False, "cqe_reason": f"cqe_error_{e}", "cqe_quality_score": 0}


def get_open_shadow_cqe_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = TRUE
          AND entry_quality = 'SHADOW_CQE_V1'
    """)
    return cur.fetchone()[0] or 0


def get_open_same_symbol_shadow_cqe_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = TRUE
          AND entry_quality = 'SHADOW_CQE_V1'
          AND symbol = %s
    """, (symbol,))
    return cur.fetchone()[0] or 0


def open_shadow_cqe_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, cqe_context, leadership_context=None):
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
        "LONG",
        price,
        DATA_VERSION,
        momentum,
        trend,
        "SHADOW_CQE_V1",
        signal_id,
        signal_time,
    ))
    trade_id = cur.fetchone()[0]

    safe_update_trade_telemetry(cur, trade_id, {
        "is_shadow": False,
        "entry_architecture": "SHADOW_CQE_V1",
        "trade_size_gbp": CQE_SHADOW_TRADE_SIZE_GBP,
        "dynamic_trade_size_gbp": CQE_SHADOW_TRADE_SIZE_GBP,
        "shadow_cqe_detected_at_entry": cqe_context.get("cqe_detected"),
        "shadow_cqe_reason_at_entry": cqe_context.get("cqe_reason"),
        "cqe_quality_score_at_entry": cqe_context.get("cqe_quality_score"),
        "cqe_momentum_accel_30m_at_entry": cqe_context.get("cqe_momentum_accel_30m"),
        "cqe_trend_accel_30m_at_entry": cqe_context.get("cqe_trend_accel_30m"),
        "cqe_trend_std_30m_at_entry": cqe_context.get("cqe_trend_std_30m"),
        "cqe_min_trend_30m_at_entry": cqe_context.get("cqe_min_trend_30m"),
        "cqe_trend_range_30m_at_entry": cqe_context.get("cqe_trend_range_30m"),
        "cqe_positive_trend_ratio_30m_at_entry": cqe_context.get("cqe_positive_trend_ratio_30m"),
        "cqe_avg_momentum_30m_at_entry": cqe_context.get("cqe_avg_momentum_30m"),
        "cqe_avg_trend_30m_at_entry": cqe_context.get("cqe_avg_trend_30m"),
        "lifecycle_phase_at_entry": (leadership_context or {}).get("lifecycle_phase") or (leadership_context or {}).get("leadership_phase"),
        "leadership_score": (leadership_context or {}).get("prior_avg_peak"),
        "leadership_delta_30m_at_entry": (leadership_context or {}).get("leadership_delta_30m") or (leadership_context or {}).get("delta_30m"),
    })

    try:
        log_trade_event(cur, trade_id, symbol, "shadow_cqe_entry", price, 0, 0, 0, momentum, trend, True)
    except Exception as e:
        print(f"⚠️ shadow CQE trade_events entry log failed: {e}", flush=True)

    try:
        update_trade_telemetry_v1(
            cur,
            trade_id,
            symbol,
            opened_at=signal_time,
            peak_pnl_percent=0,
            is_exit=False
        )
    except Exception as e:
        print(f"⚠️ TELEMETRY shadow entry update failed: {e}", flush=True)

    return trade_id


def maybe_open_shadow_cqe_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, cqe_context, leadership_context=None):
    if not ENABLE_SHADOW_CQE or not cqe_context or not cqe_context.get("cqe_detected"):
        return None

    open_shadow_count = get_open_shadow_cqe_count(cur)
    if open_shadow_count >= CQE_MAX_OPEN_SHADOW_TRADES:
        return None

    same_symbol_count = get_open_same_symbol_shadow_cqe_count(cur, symbol)
    if same_symbol_count >= CQE_MAX_SAME_SYMBOL_SHADOW_OPEN:
        return None

    trade_id = open_shadow_cqe_trade(
        cur, symbol, price, momentum, trend, signal_id, signal_time, cqe_context, leadership_context
    )

    print(
        f"🧪 OPEN SHADOW CQE | {symbol} | id={trade_id} | "
        f"q={cqe_context.get('cqe_quality_score')} | "
        f"T/M={round(trend,3)}/{round(momentum,3)} | "
        f"m_acc={fmt_num(cqe_context.get('cqe_momentum_accel_30m'))} | "
        f"t_acc={fmt_num(cqe_context.get('cqe_trend_accel_30m'))}",
        flush=True
    )

    if ENABLE_SHADOW_CQE_TELEGRAM_ALERTS:
        send_telegram_alert(
            f"🧪 <b>SHADOW CQE ENTRY</b> | {symbol} LONG\n"
            f"Quality: <b>{cqe_context.get('cqe_quality_score')}/9</b> | Paper size {fmt_money(CQE_SHADOW_TRADE_SIZE_GBP)}\n"
            f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
            f"Accel M/T: {fmt_num(cqe_context.get('cqe_momentum_accel_30m'))} / {fmt_num(cqe_context.get('cqe_trend_accel_30m'))}\n"
            f"Trend health: min {fmt_num(cqe_context.get('cqe_min_trend_30m'))} | "
            f"std {fmt_num(cqe_context.get('cqe_trend_std_30m'))} | "
            f"pos {fmt_num(cqe_context.get('cqe_positive_trend_ratio_30m'))}\n"
            f"Reason: {cqe_context.get('cqe_reason')}\n"
            f"Shadow ID {trade_id}"
        )

    return trade_id


def process_shadow_cqe_trades(cur, symbol, price, momentum, trend, now):
    """Updates and closes CQE paper trades. No OKX orders are sent."""
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
          AND COALESCE(is_shadow, FALSE) = TRUE
          AND entry_quality = 'SHADOW_CQE_V1'
          AND symbol = %s
    """, (symbol,))

    rows = cur.fetchall()
    for tid, sym, direction, entry_price, opened_at, peak_pnl, entry_quality in rows:
        if direction != "LONG":
            continue

        pnl_percent = ((price - entry_price) / entry_price) * 100
        mins = (now - opened_at).total_seconds() / 60
        current_peak = peak_pnl or 0
        old_peak = current_peak

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
            log_trade_event(cur, tid, sym, "shadow_cqe_update", price, pnl_percent, current_peak, mins, momentum, trend, False)
        except Exception as e:
            print(f"⚠️ shadow CQE trade_events update log failed: {e}", flush=True)

        if (
            ENABLE_SHADOW_CQE_TELEGRAM_ALERTS
            and old_peak < CQE_PEAK_ALERT_TRIGGER <= current_peak
        ):
            send_telegram_alert(
                f"🟢 <b>SHADOW CQE PEAK</b> | {sym}\n"
                f"Peak {fmt_num(current_peak)}% | Current {fmt_num(pnl_percent)}% | Age {fmt_num(mins,1)}m\n"
                f"T/M {fmt_num(trend)} / {fmt_num(momentum)} | ID {tid}"
            )

        if (
            ENABLE_SHADOW_CQE_TELEGRAM_ALERTS
            and old_peak < CQE_RUNNER_ALERT_TRIGGER <= current_peak
        ):
            send_telegram_alert(
                f"🚀 <b>SHADOW CQE RUNNER</b> | {sym}\n"
                f"Peak {fmt_num(current_peak)}% | Current {fmt_num(pnl_percent)}% | Age {fmt_num(mins,1)}m\n"
                f"ID {tid}"
            )

        close_reason = None
        if fixed_time_exit_allowed_for_trade(is_shadow=True) and mins >= CQE_SHADOW_HOLD_MINUTES:
            close_reason = "shadow_cqe_120m_time_exit"

        if close_reason:
            pnl_gbp = (pnl_percent / 100) * CQE_SHADOW_TRADE_SIZE_GBP
            cur.execute("""
                UPDATE bot_trades_v4
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_price = %s,
                    pnl_percent = %s,
                    pnl_gbp = %s,
                    close_reason = %s,
                    telegram_close_alert_sent = FALSE
                WHERE id = %s
                  AND status = 'OPEN'
                RETURNING id
            """, (price, pnl_percent, pnl_gbp, close_reason, tid))
            closed_row = cur.fetchone()
            if not closed_row:
                print(f"🔕 BPT close skipped; trade already closed or not open | {tid}", flush=True)
                continue

            safe_update_trade_telemetry(cur, tid, {
                "exit_architecture": "SHADOW_CQE_V1",
                "drawdown_from_peak_at_exit": current_peak - pnl_percent,
                "leadership_trend_at_exit": trend,
                "leadership_momentum_at_exit": momentum,
            })

            try:
                log_trade_event(cur, tid, sym, f"exit_{close_reason}", price, pnl_percent, current_peak, mins, momentum, trend, False)
            except Exception as e:
                print(f"⚠️ shadow CQE trade_events exit log failed: {e}", flush=True)

            print(
                f"🧪 CLOSED SHADOW CQE | {sym} | {round(pnl_percent,3)}% | "
                f"peak={round(current_peak,3)} | {close_reason}",
                flush=True
            )

            if ENABLE_SHADOW_CQE_TELEGRAM_ALERTS:
                send_telegram_alert(
                    f"🧪 <b>SHADOW CQE CLOSED</b> | {sym}\n"
                    f"Paper PnL <b>{fmt_num(pnl_percent)}%</b> | {fmt_money(pnl_gbp)}\n"
                    f"Peak {fmt_num(current_peak)}% | DD {fmt_num(current_peak - pnl_percent)}%\n"
                    f"Age {fmt_num(mins,1)}m | Reason {close_reason}\n"
                    f"ID {tid}"
                )


# =========================
# BPT CQE LIFECYCLE SHADOW HELPERS
# =========================

def ensure_bpt_cqe_lifecycle_columns(cur):
    """Adds optional telemetry columns used by BPT_CQE_LIFECYCLE_V1.
    Safe to run repeatedly. Existing deployments without these columns still
    work because safe_update_trade_telemetry skips missing fields, but these
    columns make the shadow engine fully analyzable.
    """
    cur.execute("""
        ALTER TABLE bot_trades_v4
        ADD COLUMN IF NOT EXISTS lifecycle_row TEXT,
        ADD COLUMN IF NOT EXISTS cqe_confirmed BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS cqe_upgraded BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS probe_size_gbp NUMERIC,
        ADD COLUMN IF NOT EXISTS upgrade_size_gbp NUMERIC,
        ADD COLUMN IF NOT EXISTS lifecycle_quality_score NUMERIC,
        ADD COLUMN IF NOT EXISTS lifecycle_trail_activation NUMERIC,
        ADD COLUMN IF NOT EXISTS lifecycle_trail_drawdown NUMERIC,
        ADD COLUMN IF NOT EXISTS lifecycle_classified_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS upgraded_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS confirmation_peak_30m NUMERIC,
        ADD COLUMN IF NOT EXISTS confirmation_avg_trend NUMERIC,
        ADD COLUMN IF NOT EXISTS confirmation_avg_momentum NUMERIC,
        ADD COLUMN IF NOT EXISTS confirmation_signal_count INTEGER,
        ADD COLUMN IF NOT EXISTS confirmation_age_minutes NUMERIC,
        ADD COLUMN IF NOT EXISTS bpt_exit_reason TEXT,
        ADD COLUMN IF NOT EXISTS ph_detected BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS ph_reason TEXT,
        ADD COLUMN IF NOT EXISTS ph_core_age_minutes NUMERIC,
        ADD COLUMN IF NOT EXISTS ph_core_hits_240m INTEGER,
        ADD COLUMN IF NOT EXISTS ph_top3_hits_240m INTEGER,
        ADD COLUMN IF NOT EXISTS ph_first_density_seen BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS ph_mins_to_density NUMERIC,
        ADD COLUMN IF NOT EXISTS ph_density_phase TEXT,
        ADD COLUMN IF NOT EXISTS entry_archetype TEXT,
        ADD COLUMN IF NOT EXISTS current_archetype TEXT,
        ADD COLUMN IF NOT EXISTS archetype_version TEXT,
        ADD COLUMN IF NOT EXISTS archetype_exit_reason TEXT,
        ADD COLUMN IF NOT EXISTS telegram_close_alert_sent BOOLEAN DEFAULT FALSE,
        ADD COLUMN IF NOT EXISTS telegram_close_alert_sent_at TIMESTAMPTZ
    """)


def bpt_quality_score(momentum, trend):
    return float(momentum or 0) + float(trend or 0)


def classify_bpt_lifecycle_row(momentum, trend):
    q = bpt_quality_score(momentum, trend)
    if q >= BPT_EXTREME_QUALITY_SCORE:
        return "EXTREME_RUNNER_ROW", q
    if q >= BPT_HIGH_QUALITY_SCORE:
        return "HIGH_MONSTER_ROW", q
    if q >= BPT_MEDIUM_QUALITY_SCORE:
        return "MEDIUM_BALANCED_ROW", q
    return "EARLY_INCUBATION_ROW", q


def get_bpt_row_params(lifecycle_row):
    if lifecycle_row == "EXTREME_RUNNER_ROW":
        return {
            "upgrade_size": BPT_EXTREME_UPGRADE_GBP,
            "trail_activation": BPT_EXTREME_TRAIL_ACTIVATION,
            "trail_drawdown": BPT_EXTREME_TRAIL_DRAWDOWN,
        }
    if lifecycle_row == "HIGH_MONSTER_ROW":
        return {
            "upgrade_size": BPT_HIGH_UPGRADE_GBP,
            "trail_activation": BPT_HIGH_TRAIL_ACTIVATION,
            "trail_drawdown": BPT_HIGH_TRAIL_DRAWDOWN,
        }
    if lifecycle_row == "MEDIUM_BALANCED_ROW":
        return {
            "upgrade_size": BPT_MEDIUM_UPGRADE_GBP,
            "trail_activation": BPT_MEDIUM_TRAIL_ACTIVATION,
            "trail_drawdown": BPT_MEDIUM_TRAIL_DRAWDOWN,
        }
    return {
        "upgrade_size": BPT_EARLY_UPGRADE_GBP,
        "trail_activation": BPT_EARLY_TRAIL_ACTIVATION,
        "trail_drawdown": BPT_EARLY_TRAIL_DRAWDOWN,
    }


def get_open_bpt_cqe_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
    """, (BPT_CQE_ENTRY_QUALITY,))
    return cur.fetchone()[0] or 0


def get_open_same_symbol_bpt_cqe_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
          AND symbol = %s
    """, (BPT_CQE_ENTRY_QUALITY, symbol))
    return cur.fetchone()[0] or 0


def passes_bpt_cqe_probe_gate(cur, symbol, momentum, trend, leadership_context):
    if not ENABLE_BPT_CQE_LIFECYCLE_SHADOW:
        return False, "bpt_lifecycle_disabled"

    if trend < BPT_CQE_MIN_TREND:
        return False, "bpt_trend_too_low"
    if momentum < BPT_CQE_MIN_MOMENTUM:
        return False, "bpt_momentum_too_low"

    score = float((leadership_context or {}).get("leadership_score") or (leadership_context or {}).get("prior_avg_peak") or 0)
    delta = (leadership_context or {}).get("leadership_delta_30m")
    if delta is None:
        delta = (leadership_context or {}).get("delta_30m")
    delta = float(delta or 0)

    if score > BPT_CQE_MAX_LEADERSHIP_SCORE:
        return False, "bpt_leadership_too_mature"
    if delta > BPT_CQE_MAX_LEADERSHIP_DELTA_30M:
        return False, "bpt_leadership_delta_climax_risk"

    if get_open_bpt_cqe_count(cur) >= BPT_CQE_MAX_OPEN_TRADES:
        return False, "bpt_max_open_trades"
    if get_open_same_symbol_bpt_cqe_count(cur, symbol) >= BPT_CQE_MAX_SAME_SYMBOL_OPEN:
        return False, "bpt_max_same_symbol_open"

    return True, "bpt_probe_allowed"


def open_bpt_cqe_probe_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, leadership_context=None):
    lifecycle_row, q = classify_bpt_lifecycle_row(momentum, trend)
    params = get_bpt_row_params(lifecycle_row)

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
        "LONG",
        price,
        DATA_VERSION,
        momentum,
        trend,
        BPT_CQE_ENTRY_QUALITY,
        signal_id,
        signal_time,
    ))
    trade_id = cur.fetchone()[0]

    safe_update_trade_telemetry(cur, trade_id, {
        "is_shadow": not ENABLE_BPT_CQE_LIVE_PROBES,
        "entry_architecture": BPT_CQE_ENTRY_QUALITY,
        "trade_size_gbp": BPT_CQE_PROBE_SIZE_GBP,
        "dynamic_trade_size_gbp": BPT_CQE_PROBE_SIZE_GBP,
        "probe_size_gbp": BPT_CQE_PROBE_SIZE_GBP,
        "upgrade_size_gbp": params["upgrade_size"],
        "lifecycle_row": lifecycle_row,
        "lifecycle_quality_score": q,
        "lifecycle_trail_activation": params["trail_activation"],
        "lifecycle_trail_drawdown": params["trail_drawdown"],
        "lifecycle_classified_at": datetime.now(timezone.utc),
        "entry_archetype": lifecycle_row,
        "current_archetype": lifecycle_row,
        "archetype_version": ARCHETYPE_VERSION,
        "cqe_confirmed": False,
        "cqe_upgraded": False,
        "lifecycle_phase_at_entry": (leadership_context or {}).get("lifecycle_phase") or (leadership_context or {}).get("leadership_phase"),
        "leadership_score": (leadership_context or {}).get("prior_avg_peak"),
        "leadership_delta_30m_at_entry": (leadership_context or {}).get("leadership_delta_30m") or (leadership_context or {}).get("delta_30m"),
    })

    try:
        log_trade_event(cur, trade_id, symbol, "bpt_cqe_probe_entry", price, 0, 0, 0, momentum, trend, True)
    except Exception as e:
        print(f"⚠️ BPT CQE trade_events probe entry log failed: {e}", flush=True)

    if ENABLE_BPT_CQE_LIVE_PROBES:
        okx_place_market_order(
            cur=cur,
            trade_id=trade_id,
            symbol=symbol,
            direction="LONG",
            action="entry",
            price=price,
            entry_price=price,
            trade_size_quote=BPT_CQE_PROBE_SIZE_GBP,
        )

    print(
        f"🧬 OPEN BPT CQE PROBE | {symbol} | id={trade_id} | row={lifecycle_row} | "
        f"q={round(q,3)} | size={fmt_money(BPT_CQE_PROBE_SIZE_GBP)} | T/M={round(trend,3)}/{round(momentum,3)}",
        flush=True
    )

    send_telegram_alert(
        f"🧬 <b>BPT CQE PROBE</b> | {symbol} LONG\n"
        f"Row: <b>{lifecycle_row}</b> | Q {fmt_num(q)}\n"
        f"Probe size: {fmt_money(BPT_CQE_PROBE_SIZE_GBP)} | Live probe: {ENABLE_BPT_CQE_LIVE_PROBES}\n"
        f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
        f"Confirm target: +{BPT_CQE_CONFIRM_PEAK}% within {BPT_CQE_CONFIRM_WINDOW_MINUTES}m\n"
        f"ID {trade_id}"
    )

    return trade_id


def maybe_open_bpt_cqe_probe(cur, symbol, price, momentum, trend, signal_id, signal_time, leadership_context=None):
    allowed, reason = passes_bpt_cqe_probe_gate(cur, symbol, momentum, trend, leadership_context)
    if not allowed:
        return None

    # Do not double-open if old CQE engine already opened same symbol on this exact signal path.
    return open_bpt_cqe_probe_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, leadership_context)


def get_bpt_confirmation_metrics(cur, symbol, opened_at, entry_price, now):
    cur.execute("""
        SELECT
            COUNT(*),
            AVG(trend),
            AVG(momentum),
            MAX(((price - %s) / NULLIF(%s,0)) * 100)
        FROM signals_raw
        WHERE symbol = %s
          AND timestamp >= %s
          AND timestamp <= LEAST(%s, %s + INTERVAL '60 minutes')
    """, (entry_price, entry_price, symbol, opened_at, now, opened_at))
    count, avg_trend, avg_momentum, peak_window = cur.fetchone() or (0, None, None, None)
    return {
        "signal_count": int(count or 0),
        "avg_trend": float(avg_trend or 0),
        "avg_momentum": float(avg_momentum or 0),
        "peak_window": float(peak_window or 0),
    }


def maybe_confirm_and_upgrade_bpt_trade(cur, tid, sym, entry_price, opened_at, current_peak, peak_time_minutes, momentum, trend, now):
    if peak_time_minutes is None:
        peak_time_minutes = 999999

    metrics = get_bpt_confirmation_metrics(cur, sym, opened_at, entry_price, now)
    age_mins = (now - opened_at).total_seconds() / 60
    peak_for_confirmation = max(float(current_peak or 0), metrics["peak_window"])

    confirmed = (
        peak_for_confirmation >= BPT_CQE_CONFIRM_PEAK
        and float(peak_time_minutes or 999999) <= BPT_CQE_CONFIRM_WINDOW_MINUTES
        and metrics["avg_trend"] >= BPT_CQE_CONFIRM_AVG_TREND
        and metrics["avg_momentum"] >= BPT_CQE_CONFIRM_AVG_MOMENTUM
        and metrics["signal_count"] >= BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT
    )

    safe_update_trade_telemetry(cur, tid, {
        "confirmation_peak_30m": peak_for_confirmation,
        "confirmation_avg_trend": metrics["avg_trend"],
        "confirmation_avg_momentum": metrics["avg_momentum"],
        "confirmation_signal_count": metrics["signal_count"],
        "confirmation_age_minutes": age_mins,
    })

    if not confirmed:
        return False

    cur.execute("""
        SELECT lifecycle_row, upgrade_size_gbp, dynamic_trade_size_gbp
        FROM bot_trades_v4
        WHERE id = %s
    """, (tid,))
    row = cur.fetchone()
    lifecycle_row = row[0] if row else None
    upgrade_size = float(row[1] or get_bpt_row_params(lifecycle_row).get("upgrade_size", 0)) if row else 0
    current_dynamic_size = float(row[2] or BPT_CQE_PROBE_SIZE_GBP) if row else BPT_CQE_PROBE_SIZE_GBP
    new_dynamic_size = current_dynamic_size + upgrade_size

    cur.execute("""
        UPDATE bot_trades_v4
        SET cqe_confirmed = TRUE,
            cqe_upgraded = TRUE,
            upgraded_at = NOW()
        WHERE id = %s
    """, (tid,))

    safe_update_trade_telemetry(cur, tid, {
        "dynamic_trade_size_gbp": new_dynamic_size,
        "trade_size_gbp": new_dynamic_size,
    })

    try:
        log_trade_event(cur, tid, sym, "bpt_cqe_confirmed_upgrade", entry_price, 0, current_peak, age_mins, momentum, trend, False)
    except Exception as e:
        print(f"⚠️ BPT CQE upgrade trade_events log failed: {e}", flush=True)

    if ENABLE_BPT_CQE_LIVE_UPGRADES:
        okx_place_market_order(
            cur=cur,
            trade_id=tid,
            symbol=sym,
            direction="LONG",
            action="entry",
            price=entry_price,
            entry_price=entry_price,
            trade_size_quote=upgrade_size,
        )

    print(
        f"🚀 BPT CQE UPGRADED | {sym} | id={tid} | row={lifecycle_row} | "
        f"upgrade={fmt_money(upgrade_size)} | dynamic={fmt_money(new_dynamic_size)} | "
        f"peak={round(peak_for_confirmation,3)}%",
        flush=True
    )

    send_telegram_alert(
        f"🚀 <b>BPT CQE UPGRADED</b> | {sym}\n"
        f"Row: <b>{lifecycle_row}</b> | Upgrade {fmt_money(upgrade_size)} | Total model size {fmt_money(new_dynamic_size)}\n"
        f"Confirmed peak {fmt_num(peak_for_confirmation)}% | age {fmt_num(age_mins,1)}m\n"
        f"Avg T/M {fmt_num(metrics['avg_trend'])} / {fmt_num(metrics['avg_momentum'])}\n"
        f"Live upgrades: {ENABLE_BPT_CQE_LIVE_UPGRADES}\n"
        f"ID {tid}"
    )
    return True


def process_bpt_cqe_lifecycle_trades(cur, symbol, price, momentum, trend, now):
    """Processes BPT_CQE_LIFECYCLE_V1 trades.
    Shadow by default; live probes/upgrades can be enabled through toggles later.
    """
    if not ENABLE_BPT_CQE_LIFECYCLE_SHADOW:
        return

    ensure_bpt_cqe_lifecycle_columns(cur)

    cur.execute("""
        SELECT
            id,
            symbol,
            direction,
            entry_price,
            opened_at,
            COALESCE(peak_pnl_percent, 0),
            COALESCE(peak_time_minutes, NULL),
            COALESCE(cqe_confirmed, FALSE),
            COALESCE(cqe_upgraded, FALSE),
            lifecycle_row,
            COALESCE(dynamic_trade_size_gbp, trade_size_gbp, %s),
            COALESCE(lifecycle_trail_activation, 0),
            COALESCE(lifecycle_trail_drawdown, 0),
            COALESCE(is_shadow, TRUE)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
          AND symbol = %s
    """, (BPT_CQE_PROBE_SIZE_GBP, BPT_CQE_ENTRY_QUALITY, symbol))

    rows = cur.fetchall()
    for (
        tid, sym, direction, entry_price, opened_at, peak_pnl, peak_time_minutes,
        cqe_confirmed, cqe_upgraded, lifecycle_row, dynamic_size,
        trail_activation, trail_drawdown, is_shadow
    ) in rows:
        if direction != "LONG" or not entry_price:
            continue

        pnl_percent = ((price - entry_price) / entry_price) * 100
        mins = (now - opened_at).total_seconds() / 60
        current_peak = float(peak_pnl or 0)
        old_peak = current_peak

        if pnl_percent > current_peak:
            current_peak = pnl_percent
            if column_exists(cur, "bot_trades_v4", "peak_time_minutes"):
                cur.execute("""
                    UPDATE bot_trades_v4
                    SET peak_pnl_percent = %s,
                        peak_time_minutes = %s
                    WHERE id = %s
                """, (current_peak, mins, tid))
                peak_time_minutes = mins
            else:
                cur.execute("""
                    UPDATE bot_trades_v4
                    SET peak_pnl_percent = %s
                    WHERE id = %s
                """, (current_peak, tid))

        try:
            log_trade_event(cur, tid, sym, "bpt_cqe_update", price, pnl_percent, current_peak, mins, momentum, trend, False)
        except Exception as e:
            print(f"⚠️ BPT CQE update trade_events log failed: {e}", flush=True)

        if not cqe_upgraded:
            maybe_confirm_and_upgrade_bpt_trade(
                cur, tid, sym, entry_price, opened_at, current_peak,
                peak_time_minutes, momentum, trend, now
            )

        # Reload upgrade state after possible upgrade.
        cur.execute("""
            SELECT COALESCE(cqe_upgraded, FALSE), lifecycle_row,
                   COALESCE(dynamic_trade_size_gbp, trade_size_gbp, %s),
                   COALESCE(lifecycle_trail_activation, %s),
                   COALESCE(lifecycle_trail_drawdown, %s)
            FROM bot_trades_v4
            WHERE id = %s
        """, (BPT_CQE_PROBE_SIZE_GBP, trail_activation, trail_drawdown, tid))
        state = cur.fetchone()
        if state:
            cqe_upgraded, lifecycle_row, dynamic_size, trail_activation, trail_drawdown = state

        close_reason = None
        exit_architecture = None
        drawdown_from_peak = current_peak - pnl_percent

        runtime_archetype = classify_runtime_archetype(
            lifecycle_row=lifecycle_row,
            mins=mins,
            current_peak=current_peak,
            pnl_percent=pnl_percent,
            trend=trend,
            momentum=momentum,
            leadership_score=None,
            density_phase=None,
        )
        update_trade_archetype(cur, tid, runtime_archetype)

        if ENABLE_ARCHETYPE_STATE_ENGINE and mins >= ARCH_NO_PROGRESS_MINUTES and current_peak < ARCH_NO_PROGRESS_PEAK:
            close_reason = "arch_no_progress_120m"
            exit_architecture = "ARCHETYPE_STATE_NO_PROGRESS"

        if cqe_upgraded and not close_reason:
            if current_peak >= float(trail_activation or 999) and drawdown_from_peak >= float(trail_drawdown or 999):
                close_reason = f"bpt_{(lifecycle_row or 'row').lower()}_wide_trail"
                exit_architecture = "BPT_CQE_LIFECYCLE_ROW_TRAIL"
        else:
            if lifecycle_row == "EARLY_INCUBATION_ROW" and mins >= BPT_EARLY_FAILFAST_MINUTES and current_peak < BPT_EARLY_FAILFAST_PEAK:
                close_reason = "bpt_early_failed_incubation"
                exit_architecture = "BPT_CQE_EARLY_FAILFAST"
            elif pnl_percent <= BPT_CQE_HARD_STOP:
                close_reason = "bpt_probe_hard_stop"
                exit_architecture = "BPT_CQE_PROBE_HARD_STOP"

        if not close_reason and mins >= BPT_CQE_MAX_HOLD_MINUTES:
            extend_for_healthy_leader = (
                ENABLE_ADAPTIVE_BPT_MAX_HOLD
                and current_peak >= BPT_EXTEND_MAX_HOLD_IF_PEAK_ABOVE
                and drawdown_from_peak < BPT_EXTEND_MAX_HOLD_IF_DD_BELOW
                and runtime_archetype in ["HIGH_MONSTER_ROW", "SLOW_GRINDER", "GENERAL_RUNNER", "FAST_IGNITION"]
            )
            if extend_for_healthy_leader:
                print(
                    f"⏳ BPT MAX HOLD EXTENDED | {sym} | {runtime_archetype} | "
                    f"peak={round(current_peak,3)} dd={round(drawdown_from_peak,3)} age={round(mins,1)}m",
                    flush=True
                )
            else:
                close_reason = "bpt_safety_max_hold_exit"
                exit_architecture = "BPT_CQE_SAFETY_BACKSTOP"

        if close_reason:
            pnl_gbp = (pnl_percent / 100) * float(dynamic_size or BPT_CQE_PROBE_SIZE_GBP)

            cur.execute("""
                UPDATE bot_trades_v4
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_price = %s,
                    pnl_percent = %s,
                    pnl_gbp = %s,
                    close_reason = %s,
                    telegram_close_alert_sent = FALSE
                WHERE id = %s
                  AND status = 'OPEN'
                RETURNING id
            """, (price, pnl_percent, pnl_gbp, close_reason, tid))
            closed_row = cur.fetchone()
            if not closed_row:
                print(f"🔕 BPT close skipped; trade already closed or not open | {tid}", flush=True)
                continue

            safe_update_trade_telemetry(cur, tid, {
                "exit_architecture": exit_architecture,
                "drawdown_from_peak_at_exit": drawdown_from_peak,
                "leadership_trend_at_exit": trend,
                "leadership_momentum_at_exit": momentum,
                "bpt_exit_reason": close_reason,
                "archetype_exit_reason": close_reason,
                "current_archetype": runtime_archetype,
                "archetype_version": ARCHETYPE_VERSION,
            })

            try:
                log_trade_event(cur, tid, sym, f"exit_{close_reason}", price, pnl_percent, current_peak, mins, momentum, trend, False)
            except Exception as e:
                print(f"⚠️ BPT CQE exit trade_events log failed: {e}", flush=True)

            # Only send OKX exit if this BPT trade actually had live BPT orders.
            if not is_shadow and has_successful_okx_live_entry(cur, tid):
                okx_place_market_order(
                    cur=cur,
                    trade_id=tid,
                    symbol=sym,
                    direction=direction,
                    action="exit",
                    price=price,
                    entry_price=entry_price,
                    trade_size_quote=float(dynamic_size or BPT_CQE_PROBE_SIZE_GBP),
                )
            elif not is_shadow:
                log_okx_exit_skip_no_live_entry(cur, tid, sym, direction, price)

            print(
                f"💰 CLOSED BPT CQE | {sym} | {round(pnl_percent,3)}% | "
                f"peak={round(current_peak,3)} | dd={round(drawdown_from_peak,3)} | {close_reason}",
                flush=True
            )

            send_telegram_alert(
                f"💰 <b>BPT CQE CLOSED</b> | {sym}\n"
                f"Row: {lifecycle_row} | PnL <b>{fmt_num(pnl_percent)}%</b> | {fmt_money(pnl_gbp)}\n"
                f"Peak {fmt_num(current_peak)}% | DD {fmt_num(drawdown_from_peak)}%\n"
                f"Size model {fmt_money(dynamic_size)} | Reason {close_reason}\n"
                f"ID {tid}"
            )



# =========================
# PERSISTENCE HUNTER SHADOW HELPERS
# =========================

def get_persistence_hunter_context(cur, symbol, signal_time):
    """Returns leadership persistence context known at signal time."""
    try:
        cur.execute("""
            WITH ranked AS (
                SELECT
                    snapshot_time,
                    symbol,
                    leadership_score,
                    RANK() OVER (PARTITION BY snapshot_time ORDER BY leadership_score DESC NULLS LAST) AS leadership_rank
                FROM leadership_state_history
                WHERE snapshot_time >= %s - INTERVAL '6 hours'
                  AND snapshot_time <= %s
            ), latest AS (
                SELECT leadership_score, leadership_rank, snapshot_time
                FROM ranked
                WHERE symbol = %s
                ORDER BY snapshot_time DESC
                LIMIT 1
            ), counts AS (
                SELECT
                    COUNT(*) FILTER (WHERE leadership_score >= %s) AS core_hits_240m,
                    COUNT(*) FILTER (WHERE leadership_rank <= 3) AS top3_hits_240m,
                    MIN(snapshot_time) FILTER (WHERE leadership_score >= %s) AS first_core_time,
                    MIN(snapshot_time) FILTER (WHERE leadership_rank <= 3) AS first_top3_time
                FROM ranked
                WHERE symbol = %s
                  AND snapshot_time >= %s - INTERVAL '240 minutes'
                  AND snapshot_time <= %s
            )
            SELECT
                COALESCE(l.leadership_score, 0),
                l.leadership_rank,
                COALESCE(c.core_hits_240m, 0),
                COALESCE(c.top3_hits_240m, 0),
                c.first_core_time,
                c.first_top3_time
            FROM counts c
            LEFT JOIN latest l ON TRUE
        """, (
            signal_time, signal_time, symbol,
            PH_MIN_LEADERSHIP_SCORE, PH_MIN_LEADERSHIP_SCORE, symbol,
            signal_time, signal_time,
        ))
        row = cur.fetchone()
        if not row:
            return {
                "leadership_score": 0.0,
                "leadership_rank": None,
                "core_hits_240m": 0,
                "top3_hits_240m": 0,
                "core_age_minutes": None,
                "top3_age_minutes": None,
            }
        score, rank, core_hits, top3_hits, first_core_time, first_top3_time = row
        core_age = None
        top3_age = None
        if first_core_time:
            core_age = (signal_time - first_core_time).total_seconds() / 60
        if first_top3_time:
            top3_age = (signal_time - first_top3_time).total_seconds() / 60
        return {
            "leadership_score": float(score or 0),
            "leadership_rank": int(rank) if rank is not None else None,
            "core_hits_240m": int(core_hits or 0),
            "top3_hits_240m": int(top3_hits or 0),
            "core_age_minutes": core_age,
            "top3_age_minutes": top3_age,
        }
    except Exception as e:
        print(f"⚠️ PH context failed for {symbol}: {e}", flush=True)
        return {"leadership_score": 0.0, "leadership_rank": None, "core_hits_240m": 0, "top3_hits_240m": 0, "core_age_minutes": None, "top3_age_minutes": None}


def get_open_persistence_hunter_count(cur):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
    """, (PH_ENTRY_QUALITY,))
    return cur.fetchone()[0] or 0


def get_open_same_symbol_persistence_hunter_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
          AND symbol = %s
    """, (PH_ENTRY_QUALITY, symbol))
    return cur.fetchone()[0] or 0


def passes_persistence_hunter_gate(cur, symbol, momentum, trend, signal_time):
    if not ENABLE_PERSISTENCE_HUNTER_SHADOW:
        return False, "ph_disabled", None
    if momentum < PH_MIN_MOMENTUM:
        return False, "ph_momentum_too_low", None
    if trend < PH_MIN_TREND:
        return False, "ph_trend_too_low", None

    ctx = get_persistence_hunter_context(cur, symbol, signal_time)
    score = float(ctx.get("leadership_score") or 0)
    core_age = ctx.get("core_age_minutes")
    core_hits = int(ctx.get("core_hits_240m") or 0)

    if score < PH_MIN_LEADERSHIP_SCORE:
        return False, "ph_leadership_too_low", ctx
    if core_hits < PH_MIN_CORE_HITS_240M:
        return False, "ph_not_enough_persistence_hits", ctx
    if core_age is None:
        return False, "ph_no_core_age", ctx
    if core_age < PH_MIN_CORE_AGE_MINUTES:
        return False, "ph_too_fresh", ctx
    if core_age > PH_MAX_CORE_AGE_MINUTES:
        return False, "ph_too_mature", ctx
    if get_open_persistence_hunter_count(cur) >= PH_MAX_OPEN_TRADES:
        return False, "ph_max_open_trades", ctx
    if get_open_same_symbol_persistence_hunter_count(cur, symbol) >= PH_MAX_SAME_SYMBOL_OPEN:
        return False, "ph_max_same_symbol_open", ctx

    return True, "ph_no_density_persistent_leader_allowed", ctx


def open_persistence_hunter_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, reason, ctx):
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
        "LONG",
        price,
        DATA_VERSION,
        momentum,
        trend,
        PH_ENTRY_QUALITY,
        signal_id,
        signal_time,
    ))
    tid = cur.fetchone()[0]

    safe_update_trade_telemetry(cur, tid, {
        "is_shadow": not ENABLE_PERSISTENCE_HUNTER_LIVE,
        "entry_architecture": PH_ENTRY_QUALITY,
        "trade_size_gbp": PH_SHADOW_SIZE_GBP,
        "dynamic_trade_size_gbp": PH_SHADOW_SIZE_GBP,
        "lifecycle_row": PH_LIFECYCLE_ROW,
        "ph_detected": True,
        "ph_reason": reason,
        "ph_core_age_minutes": ctx.get("core_age_minutes"),
        "ph_core_hits_240m": ctx.get("core_hits_240m"),
        "ph_top3_hits_240m": ctx.get("top3_hits_240m"),
        "leadership_score": ctx.get("leadership_score"),
        "leadership_rank_at_entry": ctx.get("leadership_rank"),
        "lifecycle_trail_activation": PH_TRAIL_ACTIVATION,
        "lifecycle_trail_drawdown": PH_TRAIL_DRAWDOWN,
    })

    try:
        log_trade_event(cur, tid, symbol, "ph_entry", price, 0, 0, 0, momentum, trend, True)
    except Exception as e:
        print(f"⚠️ PH trade_events entry log failed: {e}", flush=True)

    if ENABLE_PERSISTENCE_HUNTER_LIVE:
        okx_place_market_order(cur, tid, symbol, "LONG", "entry", price=price, entry_price=price, trade_size_quote=PH_SHADOW_SIZE_GBP)

    print(
        f"🧲 OPEN PH SHADOW | {symbol} | id={tid} | score={fmt_num(ctx.get('leadership_score'))} | "
        f"age={fmt_num(ctx.get('core_age_minutes'),1)}m | hits={ctx.get('core_hits_240m')} | T/M={fmt_num(trend)}/{fmt_num(momentum)}",
        flush=True,
    )

    send_telegram_alert(
        f"🧲 <b>PERSISTENCE HUNTER</b> | {symbol} LONG\n"
        f"Shadow size {fmt_money(PH_SHADOW_SIZE_GBP)} | Live: {ENABLE_PERSISTENCE_HUNTER_LIVE}\n"
        f"Score {fmt_num(ctx.get('leadership_score'))} | Rank #{ctx.get('leadership_rank') or 'n/a'}\n"
        f"Core age {fmt_num(ctx.get('core_age_minutes'),1)}m | Hits {ctx.get('core_hits_240m')} | Top3 hits {ctx.get('top3_hits_240m')}\n"
        f"T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
        f"Reason: {reason}\nID {tid}"
    )
    return tid


def maybe_open_persistence_hunter_trade(cur, symbol, price, momentum, trend, signal_id, signal_time):
    allowed, reason, ctx = passes_persistence_hunter_gate(cur, symbol, momentum, trend, signal_time)
    if not allowed:
        return None
    return open_persistence_hunter_trade(cur, symbol, price, momentum, trend, signal_id, signal_time, reason, ctx or {})


def process_persistence_hunter_trades(cur, symbol, price, momentum, trend, now):
    if not ENABLE_PERSISTENCE_HUNTER_SHADOW:
        return
    ensure_bpt_cqe_lifecycle_columns(cur)

    cur.execute("""
        SELECT
            id,
            symbol,
            direction,
            entry_price,
            opened_at,
            COALESCE(peak_pnl_percent,0),
            COALESCE(dynamic_trade_size_gbp, trade_size_gbp, %s),
            COALESCE(is_shadow, TRUE),
            COALESCE(ph_first_density_seen, FALSE)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
          AND symbol = %s
    """, (PH_SHADOW_SIZE_GBP, PH_ENTRY_QUALITY, symbol))
    rows = cur.fetchall()

    for tid, sym, direction, entry_price, opened_at, peak_pnl, dynamic_size, is_shadow, density_seen in rows:
        if direction != "LONG" or not entry_price:
            continue
        pnl_percent = ((price - entry_price) / entry_price) * 100
        mins = (now - opened_at).total_seconds() / 60
        current_peak = float(peak_pnl or 0)
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
                cur.execute("UPDATE bot_trades_v4 SET peak_pnl_percent = %s WHERE id = %s", (current_peak, tid))

        # Current payload generally has no density. This field is ready for future Pine density coverage.
        density_phase = "NO_DENSITY"
        safe_update_trade_telemetry(cur, tid, {
            "ph_density_phase": density_phase,
        })

        try:
            log_trade_event(cur, tid, sym, "ph_update", price, pnl_percent, current_peak, mins, momentum, trend, False)
        except Exception as e:
            print(f"⚠️ PH update event failed: {e}", flush=True)

        drawdown = current_peak - pnl_percent
        close_reason = None
        if current_peak >= PH_MONSTER_TRAIL_ACTIVATION and drawdown >= PH_MONSTER_TRAIL_DRAWDOWN:
            close_reason = "ph_monster_wide_trail"
        elif current_peak >= PH_TRAIL_ACTIVATION and drawdown >= PH_TRAIL_DRAWDOWN:
            close_reason = "ph_runner_trail"
        elif pnl_percent <= PH_HARD_STOP:
            close_reason = "ph_hard_stop"
        elif mins >= PH_MAX_HOLD_MINUTES:
            close_reason = "ph_max_hold_exit"

        if close_reason:
            pnl_gbp = (pnl_percent / 100.0) * float(dynamic_size or PH_SHADOW_SIZE_GBP)
            cur.execute("""
                UPDATE bot_trades_v4
                SET status='CLOSED',
                    closed_at=NOW(),
                    close_price=%s,
                    pnl_percent=%s,
                    pnl_gbp=%s,
                    close_reason=%s
                WHERE id=%s
            """, (price, pnl_percent, pnl_gbp, close_reason, tid))
            safe_update_trade_telemetry(cur, tid, {
                "exit_architecture": "PERSISTENCE_HUNTER_EXIT",
                "drawdown_from_peak_at_exit": drawdown,
                "bpt_exit_reason": close_reason,
                "leadership_trend_at_exit": trend,
                "leadership_momentum_at_exit": momentum,
            })
            try:
                log_trade_event(cur, tid, sym, f"exit_{close_reason}", price, pnl_percent, current_peak, mins, momentum, trend, False)
            except Exception as e:
                print(f"⚠️ PH exit event failed: {e}", flush=True)
            if not is_shadow and has_successful_okx_live_entry(cur, tid):
                okx_place_market_order(cur, tid, sym, direction, "exit", price=price, entry_price=entry_price, trade_size_quote=float(dynamic_size or PH_SHADOW_SIZE_GBP))
            send_telegram_alert(
                f"💰 <b>PH CLOSED</b> | {sym}\n"
                f"PnL <b>{fmt_num(pnl_percent)}%</b> | {fmt_money(pnl_gbp)} | Peak {fmt_num(current_peak)}%\n"
                f"DD {fmt_num(drawdown)}% | Reason {close_reason}\nID {tid}"
            )


def build_telegram_persistence_hunter_message(cur, hours=24):
    try:
        ensure_bpt_cqe_lifecycle_columns(cur)
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status='OPEN') AS open_trades,
                COUNT(*) FILTER (WHERE status='CLOSED') AS closed_trades,
                COALESCE(ROUND(AVG(pnl_percent) FILTER (WHERE status='CLOSED')::numeric,3),0) AS avg_closed,
                COALESCE(ROUND(SUM(pnl_gbp) FILTER (WHERE status='CLOSED')::numeric,3),0) AS pnl_gbp,
                COALESCE(ROUND(AVG(peak_pnl_percent)::numeric,3),0) AS avg_peak,
                COUNT(*) FILTER (WHERE peak_pnl_percent >= 2.0) AS runners,
                COUNT(*) FILTER (WHERE peak_pnl_percent >= 5.0) AS monsters
            FROM bot_trades_v4
            WHERE entry_quality = %s
              AND opened_at >= NOW() - (%s || ' hours')::INTERVAL
        """, (PH_ENTRY_QUALITY, hours))
        row = cur.fetchone() or (0,0,0,0,0,0,0)
        open_trades, closed_trades, avg_closed, pnl_gbp, avg_peak, runners, monsters = row
        cur.execute("""
            SELECT symbol, opened_at, COALESCE(peak_pnl_percent,0), COALESCE(ph_core_age_minutes,0), COALESCE(ph_core_hits_240m,0), COALESCE(leadership_score,0)
            FROM bot_trades_v4
            WHERE entry_quality = %s
              AND status='OPEN'
            ORDER BY opened_at DESC
            LIMIT 8
        """, (PH_ENTRY_QUALITY,))
        rows = cur.fetchall()
        lines = [
            f"🧲 <b>Persistence Hunter {hours}h</b>",
            f"Enabled: {ENABLE_PERSISTENCE_HUNTER_SHADOW} | Live: {ENABLE_PERSISTENCE_HUNTER_LIVE}",
            f"Open: <b>{open_trades}</b> | Closed: <b>{closed_trades}</b>",
            f"Closed avg: <b>{fmt_num(avg_closed)}%</b> | {fmt_money(pnl_gbp)} | Avg peak {fmt_num(avg_peak)}%",
            f"Runners: {runners} | Monsters: {monsters}",
            f"Rule: no-density + leadership>={PH_MIN_LEADERSHIP_SCORE} + age {PH_MIN_CORE_AGE_MINUTES}-{PH_MAX_CORE_AGE_MINUTES}m + hits>={PH_MIN_CORE_HITS_240M}",
        ]
        if rows:
            lines.append("\n<b>Open PH trades</b>")
            for sym, opened_at, peak, age, hits, score in rows:
                lines.append(f"{sym} | peak {fmt_num(peak)}% | age {fmt_num(age,1)}m | hits {hits} | score {fmt_num(score)}")
        return "\n".join(lines)
    except Exception as e:
        print(f"⚠️ PH telegram summary failed: {e}", flush=True)
        return f"🧲 <b>Persistence Hunter</b> unavailable: {e}"

def passes_leadership_engine(cur, symbol, momentum, trend):
    if not ENABLE_LEADERSHIP_ENGINE:
        return False, None, "leadership_engine_disabled"

    # v6.1.6: always fetch context first so blocked early signals still get lifecycle telemetry.
    leadership = get_leadership_context(cur, symbol)

    if trend < LEADERSHIP_MIN_TREND:
        return False, leadership, "leadership_trend_too_low"

    if momentum <= LEADERSHIP_MIN_MOMENTUM:
        return False, leadership, "leadership_momentum_not_positive"


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
    if entry_quality == "ROT_MICRO_V1":
        return ROT_MICRO_TRADE_SIZE_GBP
    if entry_quality == "LEADERSHIP_SCALED":
        return LEADERSHIP_SCALED_TRADE_SIZE_GBP
    if entry_quality == "LIVE_CQE_V1":
        return LIVE_CQE_TRADE_SIZE_GBP
    if entry_quality == "LEADERSHIP_MONSTER":
        return MONSTER_TRADE_SIZE_GBP
    if entry_quality == "LEADERSHIP_AGGRESSIVE":
        return AGGRESSIVE_TRADE_SIZE_GBP
    return CORE_TRADE_SIZE_GBP

def get_trade_size_quote_for_quality(entry_quality):
    return get_trade_size_for_quality(entry_quality)


def phase_from_score(score):
    try:
        score = float(score or 0)
    except Exception:
        score = 0
    if score >= 2.0:
        return "MONSTER"
    if score >= 1.5:
        return "AGGRESSIVE"
    if score >= 1.25:
        return "CORE"
    if score >= 0.9:
        return "NEAR"
    return "WEAK"

def get_score_at_or_before(cur, symbol, anchor_time, minutes_back):
    try:
        cur.execute("""
            SELECT leadership_score
            FROM leadership_state_history
            WHERE symbol = %s
              AND snapshot_time <= %s - (%s || ' minutes')::INTERVAL
            ORDER BY snapshot_time DESC
            LIMIT 1
        """, (symbol, anchor_time, minutes_back))
        row = cur.fetchone()
        return float(row[0]) if row and row[0] is not None else None
    except Exception as e:
        print(f"⚠️ score lookup failed for {symbol}/{minutes_back}m: {e}", flush=True)
        return None

def get_lifecycle_context(cur, symbol):
    ensure_leadership_state_history_table(cur)
    cur.execute("""
        SELECT symbol, snapshot_time, leadership_score, successful_signals,
               runners, monsters, avg_peak, avg_worst, leadership_mode
        FROM leadership_state_history
        WHERE symbol = %s
        ORDER BY snapshot_time DESC
        LIMIT 1
    """, (symbol,))
    row = cur.fetchone()
    if not row:
        return {
            "lifecycle_phase": "NO_SNAPSHOT",
            "prior_lifecycle_phase": None,
            "leadership_transition": None,
            "leadership_score": 0.0,
            "leadership_score_30m_ago": None,
            "leadership_delta_30m": None,
            "shadow_emergence_detected": False,
            "shadow_emergence_reason": None,
            "shadow_quiet_continuation_detected": False,
            "shadow_quiet_continuation_reason": None,
        }

    (_symbol, snapshot_time, score, successes, runners, monsters, avg_peak, avg_worst, mode) = row
    score = float(score or 0)
    score_5 = get_score_at_or_before(cur, symbol, snapshot_time, 5)
    score_15 = get_score_at_or_before(cur, symbol, snapshot_time, 15)
    score_30 = get_score_at_or_before(cur, symbol, snapshot_time, 30)
    score_60 = get_score_at_or_before(cur, symbol, snapshot_time, 60)

    delta_5 = score - score_5 if score_5 is not None else None
    delta_15 = score - score_15 if score_15 is not None else None
    delta_30 = score - score_30 if score_30 is not None else None
    delta_60 = score - score_60 if score_60 is not None else None

    current_phase = phase_from_score(score)
    prior_phase = phase_from_score(score_30) if score_30 is not None else None
    transition = f"{prior_phase}->{current_phase}" if prior_phase else None

    if delta_30 is None:
        lifecycle_phase = "NO_PRIOR"
    elif (CONTROLLED_IGNITION_MIN_SCORE <= score <= CONTROLLED_IGNITION_MAX_SCORE
          and CONTROLLED_IGNITION_DELTA_MIN <= delta_30 <= CONTROLLED_IGNITION_DELTA_MAX
          and prior_phase in ["NEAR", "WEAK"]
          and current_phase in ["NEAR", "CORE", "AGGRESSIVE"]):
        lifecycle_phase = "CONTROLLED_IGNITION"
    elif score >= STABLE_LEADER_MIN_SCORE and STABLE_LEADER_DELTA_MIN <= delta_30 <= STABLE_LEADER_DELTA_MAX:
        lifecycle_phase = "STABLE_LEADER"
    elif delta_30 > CLIMAX_LEADER_DELTA_BLOCK:
        lifecycle_phase = "CLIMAX_LEADER"
    elif score >= 1.25 and delta_30 < -0.30:
        lifecycle_phase = "DECAYING_LEADER"
    elif score >= 0.75:
        lifecycle_phase = "WATCH"
    else:
        lifecycle_phase = "WEAK"

    leadership_age_minutes = None
    try:
        threshold = 1.25 if score >= 1.25 else 0.90
        cur.execute("""
            SELECT MIN(snapshot_time)
            FROM leadership_state_history
            WHERE symbol = %s
              AND snapshot_time >= %s - INTERVAL '6 hours'
              AND leadership_score >= %s
        """, (symbol, snapshot_time, threshold))
        r = cur.fetchone()
        if r and r[0]:
            leadership_age_minutes = (snapshot_time - r[0]).total_seconds() / 60
    except Exception as e:
        print(f"⚠️ leadership age lookup failed for {symbol}: {e}", flush=True)

    peak_4h = None
    try:
        cur.execute("""
            SELECT MAX(leadership_score)
            FROM leadership_state_history
            WHERE symbol = %s
              AND snapshot_time >= %s - INTERVAL '4 hours'
              AND snapshot_time <= %s
        """, (symbol, snapshot_time, snapshot_time))
        r = cur.fetchone()
        peak_4h = float(r[0]) if r and r[0] is not None else None
    except Exception as e:
        print(f"⚠️ peak score lookup failed for {symbol}: {e}", flush=True)

    rank = None
    breadth = {"market_near_count": 0, "market_core_count": 0, "market_aggressive_count": 0, "market_monster_count": 0}
    try:
        cur.execute("""
            WITH latest AS (SELECT MAX(snapshot_time) AS snapshot_time FROM leadership_state_history),
            ranked AS (
                SELECT symbol, leadership_score,
                       RANK() OVER (ORDER BY leadership_score DESC NULLS LAST) AS leadership_rank,
                       COUNT(*) FILTER (WHERE leadership_score >= 0.90) OVER () AS near_count,
                       COUNT(*) FILTER (WHERE leadership_score >= 1.25) OVER () AS core_count,
                       COUNT(*) FILTER (WHERE leadership_score >= 1.50) OVER () AS aggressive_count,
                       COUNT(*) FILTER (WHERE leadership_score >= 2.00) OVER () AS monster_count
                FROM leadership_state_history l
                JOIN latest x ON x.snapshot_time = l.snapshot_time
            )
            SELECT leadership_rank, near_count, core_count, aggressive_count, monster_count
            FROM ranked WHERE symbol = %s LIMIT 1
        """, (symbol,))
        r = cur.fetchone()
        if r:
            rank = int(r[0]) if r[0] is not None else None
            breadth = {"market_near_count": int(r[1] or 0), "market_core_count": int(r[2] or 0), "market_aggressive_count": int(r[3] or 0), "market_monster_count": int(r[4] or 0)}
    except Exception as e:
        print(f"⚠️ rank/breadth lookup failed for {symbol}: {e}", flush=True)

    shadow_emergence = ENABLE_SHADOW_EMERGENCE_TELEMETRY and lifecycle_phase == "CONTROLLED_IGNITION"
    return {
        "lifecycle_phase": lifecycle_phase,
        "prior_lifecycle_phase": prior_phase,
        "current_score_phase": current_phase,
        "leadership_transition": transition,
        "leadership_score": score,
        "leadership_score_5m_ago": score_5,
        "leadership_score_15m_ago": score_15,
        "leadership_score_30m_ago": score_30,
        "leadership_score_60m_ago": score_60,
        "leadership_delta_5m": delta_5,
        "leadership_delta_15m": delta_15,
        "leadership_delta_30m": delta_30,
        "leadership_delta_60m": delta_60,
        "leadership_age_minutes": leadership_age_minutes,
        "leadership_peak_score_last_4h": peak_4h,
        "leadership_rank": rank,
        "successful_signals": successes or 0,
        "runners": runners or 0,
        "monsters": monsters or 0,
        "avg_peak": float(avg_peak or 0),
        "avg_worst": float(avg_worst or 0),
        "leadership_mode": mode,
        "snapshot_time": snapshot_time,
        "shadow_emergence_detected": shadow_emergence,
        "shadow_emergence_reason": "controlled_ignition_candidate" if shadow_emergence else None,
        **breadth,
    }

def short_phase(phase):
    mapping = {
        "CONTROLLED_IGNITION": "IGNITION",
        "STABLE_LEADER": "STABLE",
        "CLIMAX_LEADER": "CLIMAX",
        "DECAYING_LEADER": "DECAY",
        "NO_PRIOR": "NO_PRIOR",
        "NO_SNAPSHOT": "NO_SNAPSHOT",
    }
    return mapping.get(phase, phase or "n/a")

def format_leadership_compact(ctx):
    if not ctx:
        return "Leadership: n/a"
    return (
        f"{short_phase(ctx.get('lifecycle_phase'))} | "
        f"score {fmt_num(ctx.get('leadership_score'))} | "
        f"Δ30 {fmt_num(ctx.get('leadership_delta_30m'))} | "
        f"rank #{ctx.get('leadership_rank') or 'n/a'}"
    )

def get_lifecycle_dashboard_text(cur, limit=10):
    try:
        ensure_leadership_state_history_table(cur)
        cur.execute("""
            WITH latest AS (
                SELECT DISTINCT ON (symbol) symbol, snapshot_time, leadership_score,
                       successful_signals, runners, monsters, avg_worst, leadership_mode
                FROM leadership_state_history
                ORDER BY symbol, snapshot_time DESC
            ), lagged AS (
                SELECT l.*, (
                    SELECT p.leadership_score
                    FROM leadership_state_history p
                    WHERE p.symbol = l.symbol
                      AND p.snapshot_time <= l.snapshot_time - INTERVAL '30 minutes'
                    ORDER BY p.snapshot_time DESC
                    LIMIT 1
                ) AS score_30m_ago
                FROM latest l
            ), classified AS (
                SELECT *, leadership_score - score_30m_ago AS delta_30m,
                    CASE
                        WHEN score_30m_ago IS NULL THEN 'NO_PRIOR'
                        WHEN leadership_score BETWEEN %s AND %s AND (leadership_score - score_30m_ago) BETWEEN %s AND %s THEN 'IGNITION'
                        WHEN leadership_score >= %s AND (leadership_score - score_30m_ago) BETWEEN %s AND %s THEN 'STABLE'
                        WHEN (leadership_score - score_30m_ago) > %s THEN 'CLIMAX'
                        WHEN leadership_score >= 1.25 AND (leadership_score - score_30m_ago) < -0.30 THEN 'DECAY'
                        WHEN leadership_score >= 0.75 THEN 'WATCH'
                        ELSE 'WEAK'
                    END AS lifecycle_phase
                FROM lagged
            )
            SELECT symbol, lifecycle_phase, ROUND(leadership_score::numeric,3),
                   ROUND(delta_30m::numeric,3), successful_signals, runners, ROUND(avg_worst::numeric,3)
            FROM classified
            ORDER BY CASE lifecycle_phase WHEN 'IGNITION' THEN 1 WHEN 'STABLE' THEN 2 WHEN 'WATCH' THEN 3 WHEN 'CLIMAX' THEN 4 WHEN 'DECAY' THEN 5 ELSE 6 END,
                     leadership_score DESC
            LIMIT %s
        """, (
            CONTROLLED_IGNITION_MIN_SCORE, CONTROLLED_IGNITION_MAX_SCORE,
            CONTROLLED_IGNITION_DELTA_MIN, CONTROLLED_IGNITION_DELTA_MAX,
            STABLE_LEADER_MIN_SCORE, STABLE_LEADER_DELTA_MIN, STABLE_LEADER_DELTA_MAX,
            CLIMAX_LEADER_DELTA_BLOCK, limit,
        ))
        rows = cur.fetchall()
        if not rows:
            return "Lifecycle: n/a"
        lines = []
        for sym, phase, score, delta, successes, runners, avg_worst in rows:
            icon = {"IGNITION":"🚀", "STABLE":"✅", "CLIMAX":"🔥", "DECAY":"📉", "WATCH":"👀", "WEAK":"⚪"}.get(phase, "⚪")
            lines.append(f"{icon} {sym} {phase} | {score} | Δ{delta} | S{successes} R{runners} | W{avg_worst}")
        return "\n".join(lines)
    except Exception as e:
        print(f"⚠️ lifecycle dashboard failed: {e}", flush=True)
        return "Lifecycle: n/a"

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

        lifecycle_ctx = get_lifecycle_context(cur, symbol)

        lines.append(
            f"\n<b>{symbol}</b> | {quality}\n"
            f"Size {fmt_money(trade_size_gbp)} | Age {fmt_num(age_mins,1)}m | PnL {fmt_num(current_pnl)}% | Peak {fmt_num(peak)}%\n"
            f"Entry score {fmt_num(entry_leadership)} | {format_leadership_compact(lifecycle_ctx)}\n"
            f"Latest T/M {fmt_num(latest_trend)} / {fmt_num(latest_momentum)} | "
            f"{latest_decision or 'NONE'} / {latest_block_reason or 'no_reason'}"
        )


    return "\n".join(lines)




# TELEMETRY_ROLLBACK_HELPER v6.5.1
def safe_telemetry_rollback(cur):
    """
    If telemetry SQL fails inside the webhook transaction, PostgreSQL marks the transaction aborted.
    This helper attempts to rollback using the cursor connection so later bot logic is not poisoned.
    """
    try:
        if cur is not None and getattr(cur, "connection", None) is not None:
            cur.connection.rollback()
    except Exception:
        pass

# =========================
# 📡 TELEMETRY_V1 HELPERS
# =========================

ENABLE_TELEMETRY_V1 = True
TELEMETRY_VERSION = "TELEMETRY_V1"

def safe_float(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default

def safe_int(value, default=None):
    try:
        if value is None:
            return default
        return int(value)
    except Exception:
        return default

def get_ranked_leadership_context_for_telemetry(cur, symbol):
    """
    TELEMETRY_V1:
    Computes current leadership rank, age, persistence hits, velocity,
    rank stability, and market breadth/concentration context.
    This is observability only; it should not alter live decisions.
    """
    if not ENABLE_TELEMETRY_V1:
        return {}

    try:
        ensure_leadership_state_history_table(cur)

        cur.execute("""
            WITH latest_snapshot AS (
                SELECT MAX(snapshot_time) AS snapshot_time
                FROM leadership_state_history lsh
            ),
            ranked AS (
                SELECT
                    symbol,
                    snapshot_time,
                    leadership_score,
                    avg_peak,
                    avg_worst,
                    leadership_mode,
                    RANK() OVER (ORDER BY leadership_score DESC NULLS LAST) AS leadership_rank,
                    COUNT(*) FILTER (WHERE leadership_score >= 2.0) OVER () AS leader_count_2p0,
                    COUNT(*) FILTER (WHERE leadership_score >= 1.25) OVER () AS leader_count_1p25,
                    AVG(leadership_score) OVER () AS market_leadership_quality,
                    SUM(leadership_score) OVER () AS total_leadership_score
                FROM leadership_state_history l
                JOIN latest_snapshot x
                  ON x.snapshot_time = l.snapshot_time
            )
            SELECT
                symbol,
                snapshot_time,
                leadership_score,
                avg_peak,
                avg_worst,
                leadership_mode,
                leadership_rank,
                leader_count_2p0,
                leader_count_1p25,
                market_leadership_quality,
                CASE
                    WHEN total_leadership_score > 0
                    THEN leadership_score / total_leadership_score
                    ELSE NULL
                END AS top3_concentration
            FROM ranked
            WHERE symbol = %s
            LIMIT 1
        """, (symbol,))
        row = cur.fetchone()

        if not row:
            return {
                "telemetry_version": TELEMETRY_VERSION,
                "leadership_phase": "NO_LEADERSHIP_CONTEXT",
                "lifecycle_phase": "NO_LEADERSHIP_CONTEXT",
            }

        (
            _symbol,
            snapshot_time,
            leadership_score,
            avg_peak,
            avg_worst,
            leadership_mode,
            leadership_rank,
            leader_count_2p0,
            leader_count_1p25,
            market_leadership_quality,
            top3_concentration
        ) = row

        leadership_score_f = safe_float(leadership_score, 0.0)
        leadership_rank_i = safe_int(leadership_rank, None)

        cur.execute("""
            SELECT
                leadership_score,
                snapshot_time
            FROM leadership_state_history
            WHERE symbol = %s
              AND snapshot_time <= %s - INTERVAL '30 minutes'
            ORDER BY snapshot_time DESC
            LIMIT 1
        """, (symbol, snapshot_time))
        prev = cur.fetchone()
        prev_score = safe_float(prev[0], None) if prev else None
        leadership_velocity = (
            leadership_score_f - prev_score
            if prev_score is not None
            else None
        )

        cur.execute("""
            WITH ranked_history AS (
                SELECT
                    snapshot_time,
                    symbol,
                    leadership_score,
                    RANK() OVER (
                        PARTITION BY snapshot_time
                        ORDER BY leadership_score DESC NULLS LAST
                    ) AS leadership_rank
                FROM leadership_state_history
                WHERE snapshot_time >= %s - INTERVAL '4 hours'
            )
            SELECT
                COUNT(*) FILTER (WHERE leadership_score >= 2.0) AS stable_hits,
                COUNT(*) FILTER (WHERE leadership_rank <= 3) AS top3_hits,
                MIN(snapshot_time) FILTER (WHERE leadership_score >= 2.0) AS first_core_time,
                AVG(leadership_rank) FILTER (WHERE leadership_rank IS NOT NULL) AS avg_rank_4h,
                STDDEV(leadership_rank) FILTER (WHERE leadership_rank IS NOT NULL) AS rank_stability
            FROM ranked_history
            WHERE symbol = %s
              AND snapshot_time <= %s
        """, (snapshot_time, symbol, snapshot_time))
        hist = cur.fetchone() or (0, 0, None, None, None)

        stable_hits, top3_hits, first_core_time, avg_rank_4h, rank_stability = hist

        leadership_age_minutes = None
        if first_core_time:
            leadership_age_minutes = (snapshot_time - first_core_time).total_seconds() / 60.0

        if leadership_score_f >= 2.0:
            if leadership_velocity is None:
                leadership_phase = "STABLE_LEADER"
            elif leadership_velocity >= 0.50:
                leadership_phase = "IGNITION"
            elif leadership_velocity <= -0.50:
                leadership_phase = "DECAY"
            elif leadership_age_minutes is not None and leadership_age_minutes >= 180:
                leadership_phase = "MATURE"
            else:
                leadership_phase = "STABLE"
        elif leadership_score_f >= 1.25:
            leadership_phase = "CORE"
        else:
            leadership_phase = "WEAK"

        leader_rotation_velocity = None
        try:
            cur.execute("""
                WITH current_top AS (
                    SELECT symbol
                    FROM leadership_state_history
                    WHERE snapshot_time = %s
                    ORDER BY leadership_score DESC NULLS LAST
                    LIMIT 3
                ),
                prior_snapshot AS (
                    SELECT MAX(snapshot_time) AS snapshot_time
                    FROM leadership_state_history
                    WHERE snapshot_time <= %s - INTERVAL '60 minutes'
                ),
                prior_top AS (
                    SELECT l.symbol
                    FROM leadership_state_history l
                    JOIN prior_snapshot p
                      ON p.snapshot_time = l.snapshot_time
                    ORDER BY l.leadership_score DESC NULLS LAST
                    LIMIT 3
                )
                SELECT COUNT(*)
                FROM current_top c
                FULL OUTER JOIN prior_top p
                  ON p.symbol = c.symbol
                WHERE c.symbol IS NULL OR p.symbol IS NULL
            """, (snapshot_time, snapshot_time))
            leader_rotation_velocity = safe_float((cur.fetchone() or [None])[0], None)
        except Exception:
            leader_rotation_velocity = None

        return {
            "telemetry_version": TELEMETRY_VERSION,
            "leadership_age_minutes": leadership_age_minutes,
            "stable_leadership_hits": safe_int(stable_hits, 0),
            "leadership_rank": leadership_rank_i,
            "leadership_rank_stability": safe_float(rank_stability, None),
            "leadership_velocity": leadership_velocity,
            "leadership_phase": leadership_phase,
            "lifecycle_phase": leadership_phase,
            "simultaneous_leader_count": safe_int(leader_count_2p0, 0),
            "top3_concentration": safe_float(top3_concentration, None),
            "market_leadership_quality": safe_float(market_leadership_quality, None),
            "leader_rotation_velocity": leader_rotation_velocity,
        }

    except Exception as e:
        print(f"⚠️ TELEMETRY leadership context failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return {
            "telemetry_version": TELEMETRY_VERSION,
            "leadership_phase": "TELEMETRY_ERROR",
            "lifecycle_phase": "TELEMETRY_ERROR",
        }


def get_density_context_for_telemetry(cur, symbol, anchor_time=None):
    """
    TELEMETRY_V1:
    Density is interpreted as crowding/lifecycle context, not mandatory entry confirmation.
    """
    if not ENABLE_TELEMETRY_V1:
        return {}

    try:
        if anchor_time is None:
            cur.execute("""
                SELECT timestamp, sniper_density, sniper_density_delta
                FROM signals_raw
                WHERE symbol = %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol,))
        else:
            cur.execute("""
                SELECT timestamp, sniper_density, sniper_density_delta
                FROM signals_raw
                WHERE symbol = %s
                  AND timestamp <= %s
                ORDER BY timestamp DESC
                LIMIT 1
            """, (symbol, anchor_time))

        row = cur.fetchone()
        if not row:
            return {
                "density_phase": "NO_DENSITY_DATA",
                "crowding_state": "UNKNOWN",
            }

        ts, density, density_delta = row
        density_f = safe_float(density, None)
        density_delta_f = safe_float(density_delta, None)

        if density_f is None or density_delta_f is None:
            density_phase = "NO_DENSITY_CLEAN"
            crowding_state = "NOT_CROWDED"
        elif density_f >= 6 and density_delta_f >= 2:
            density_phase = "CROWDING_EXPANDING"
            crowding_state = "CROWDED"
        elif density_f >= 6:
            density_phase = "CROWDED"
            crowding_state = "CROWDED"
        elif density_delta_f >= 2:
            density_phase = "DENSITY_EMERGING"
            crowding_state = "EMERGING"
        else:
            density_phase = "DENSITY_PRESENT_LOW"
            crowding_state = "LOW"

        return {
            "density_phase": density_phase,
            "crowding_state": crowding_state,
            "density_at_exit": density_f,
            "density_delta_at_exit": density_delta_f,
        }

    except Exception as e:
        print(f"⚠️ TELEMETRY density context failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return {
            "density_phase": "DENSITY_TELEMETRY_ERROR",
            "crowding_state": "UNKNOWN",
        }


def update_trade_telemetry_v1(cur, trade_id, symbol, opened_at=None, peak_pnl_percent=None, is_exit=False):
    """
    Writes TELEMETRY_V1 fields into bot_trades_v4.
    Safe/no-op for missing columns via safe_update_trade_telemetry.
    """
    if not ENABLE_TELEMETRY_V1:
        return

    try:
        leadership_telemetry = get_ranked_leadership_context_for_telemetry(cur, symbol)
        density_telemetry = get_density_context_for_telemetry(cur, symbol)

        telemetry = {}
        telemetry.update(leadership_telemetry)
        telemetry.update({
            "density_phase": density_telemetry.get("density_phase"),
            "crowding_state": density_telemetry.get("crowding_state"),
        })

        if is_exit:
            telemetry.update({
                "density_at_exit": density_telemetry.get("density_at_exit"),
                "density_delta_at_exit": density_telemetry.get("density_delta_at_exit"),
                "leadership_decay_at_exit": leadership_telemetry.get("leadership_velocity"),
            })

        # First density timing and peak before/after density.
        try:
            if opened_at:
                cur.execute("""
                    SELECT MIN(timestamp)
                    FROM signals_raw
                    WHERE symbol = %s
                      AND timestamp >= %s
                      AND sniper_density IS NOT NULL
                """, (symbol, opened_at))
                fd = cur.fetchone()
                first_density_seen = fd[0] if fd and fd[0] else None

                if first_density_seen:
                    telemetry["first_density_seen"] = first_density_seen
                    telemetry["mins_to_density"] = (first_density_seen - opened_at).total_seconds() / 60.0

                    current_peak = safe_float(peak_pnl_percent, None)
                    telemetry["peak_before_density"] = current_peak
                    telemetry["peak_after_density"] = current_peak
                    telemetry["giveback_after_density"] = None
        except Exception as e:
            print(f"⚠️ TELEMETRY density timing skipped for {symbol}: {e}", flush=True)
            safe_telemetry_rollback(cur)

        safe_update_trade_telemetry(cur, trade_id, telemetry)

    except Exception as e:
        print(f"⚠️ TELEMETRY update skipped for trade {trade_id}/{symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)



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
        "trade_size_gbp": get_trade_size_for_context(quality, leadership_context),
        "dynamic_trade_size_gbp": get_trade_size_for_context(quality, leadership_context),
        "market_os_engine": leadership_context.get("market_os_engine"),
        "size_scaling_reason": leadership_context.get("size_scaling_reason"),
        "leadership_prior_successes": leadership_context.get("prior_successes"),
        "leadership_prior_runners": leadership_context.get("prior_runners"),
        "leadership_prior_avg_peak": leadership_context.get("prior_avg_peak"),
        "leadership_tier": quality,
        "leadership_mode": leadership_context.get("leadership_mode") or quality,
        "leadership_score": leadership_context.get("prior_avg_peak"),
        "lifecycle_phase_at_entry": leadership_context.get("lifecycle_phase") or leadership_context.get("leadership_phase"),
        "prior_lifecycle_phase_at_entry": leadership_context.get("prior_lifecycle_phase"),
        "leadership_transition_at_entry": leadership_context.get("leadership_transition"),
        "leadership_delta_5m_at_entry": leadership_context.get("leadership_delta_5m"),
        "leadership_delta_15m_at_entry": leadership_context.get("leadership_delta_15m"),
        "leadership_delta_30m_at_entry": leadership_context.get("leadership_delta_30m") or leadership_context.get("delta_30m"),
        "leadership_delta_60m_at_entry": leadership_context.get("leadership_delta_60m"),
        "leadership_score_30m_ago_at_entry": leadership_context.get("leadership_score_30m_ago") or leadership_context.get("score_30m_ago"),
        "leadership_age_minutes_at_entry": leadership_context.get("leadership_age_minutes"),
        "leadership_peak_score_last_4h_at_entry": leadership_context.get("leadership_peak_score_last_4h"),
        "leadership_rank_at_entry": leadership_context.get("leadership_rank"),
        "market_near_count_at_entry": leadership_context.get("market_near_count"),
        "market_core_count_at_entry": leadership_context.get("market_core_count"),
        "market_aggressive_count_at_entry": leadership_context.get("market_aggressive_count"),
        "market_monster_count_at_entry": leadership_context.get("market_monster_count"),
        "shadow_emergence_detected_at_entry": leadership_context.get("shadow_emergence_detected"),
        "shadow_emergence_reason_at_entry": leadership_context.get("shadow_emergence_reason"),
        "shadow_quiet_continuation_detected_at_entry": leadership_context.get("shadow_quiet_continuation_detected"),
        "shadow_quiet_continuation_reason_at_entry": leadership_context.get("shadow_quiet_continuation_reason")
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

    try:
        update_trade_telemetry_v1(
            cur,
            trade_id,
            symbol,
            opened_at=signal_time,
            peak_pnl_percent=0,
            is_exit=False
        )
    except Exception as e:
        print(f"⚠️ TELEMETRY entry update failed: {e}", flush=True)

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
# 🧠 DENSITY-TOLERANT HELPERS v6.3.1
# =========================

def density_is_valid_for_entry(sniper_density=None, sniper_density_delta=None):
    """
    v6.3.1 learning:
    Missing density is NOT a failure condition.
    Recent sims showed NULL density + strong leadership/trend/momentum
    outperformed strict density-present filters.
    """
    if ENABLE_RELAXED_DENSITY_DEPENDENCY:
        if (False and (sniper_density is None or sniper_density_delta is None)):
            return True, "density_null_allowed"
        return True, "density_present_context_only"

    if DENSITY_REQUIRED_FOR_LIVE_ENTRIES:
        if (False and (sniper_density is None or sniper_density_delta is None)):
            return False, "density_required_missing"
    return True, "density_ok"


def get_density_phase(sniper_density=None, sniper_density_delta=None):
    """
    Interprets density as lifecycle/crowding context rather than hard entry gate.
    """
    try:
        if (False and (sniper_density is None or sniper_density_delta is None)):
            return "NO_DENSITY_CLEAN"
        d = float(sniper_density or 0)
        dd = float(sniper_density_delta or 0)
        if d >= 6 and dd >= 2:
            return "CROWDING_EXPANDING"
        if d >= 6:
            return "CROWDED"
        if dd >= 2:
            return "DENSITY_RISING"
        return "DENSITY_PRESENT_LOW"
    except Exception:
        return "DENSITY_UNKNOWN"


def fixed_time_exit_allowed_for_trade(is_shadow=False):
    """
    v6.3.1:
    Fixed exits are disabled for real/live trades because sims repeatedly showed
    fixed-time exits clip delayed leaders and monsters.
    Shadow engines may still use fixed holds for research unless disabled.
    """
    if is_shadow:
        return ENABLE_FIXED_TIME_EXITS_SHADOW
    return ENABLE_FIXED_TIME_EXITS_REAL


# =========================
# WEBHOOK
# =========================

@app.route("/webhook", methods=["POST"])
def webhook():
    force_okx_cache_refresh_if_empty("webhook")
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

        now = datetime.now(timezone.utc)

        conn = get_db()
        cur = conn.cursor()

        if ENABLE_ORDER_LOGGING:
            ensure_okx_order_log_table(cur)

        ensure_signal_leadership_scores_table(cur)
        if ENABLE_BPT_CQE_LIFECYCLE_SHADOW:
            ensure_bpt_cqe_lifecycle_columns(cur)
        if ENABLE_ARCHETYPE_STATE_ENGINE:
            ensure_archetype_state_columns(cur)
        sweep_stale_shadow_trades(cur)

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

            if leadership_context:
                quiet_shadow, quiet_reason = is_shadow_quiet_continuation_candidate(
                    leadership_context,
                    momentum,
                    trend
                )
                leadership_context["shadow_quiet_continuation_detected"] = quiet_shadow
                leadership_context["shadow_quiet_continuation_reason"] = quiet_reason

                cqe_context = get_cqe_context(cur, symbol, signal_time, momentum, trend, leadership_context)
                leadership_context["shadow_cqe_detected"] = cqe_context.get("cqe_detected")
                leadership_context["shadow_cqe_reason"] = cqe_context.get("cqe_reason")
                leadership_context["cqe_quality_score"] = cqe_context.get("cqe_quality_score")
                leadership_context["cqe_context"] = cqe_context

            if entry_allowed:
                entry_quality = classify_leadership_tier(leadership_context["prior_avg_peak"])
                leadership_context["leadership_max_60m"] = max(
                    float(leadership_context.get("prior_avg_peak") or 0),
                    get_recent_leadership_max(cur, symbol, 60)
                )
                leadership_context["market_os_engine"] = "CORE"
                if ENABLE_LEADERSHIP_SIZE_SCALING_V66 and leadership_context["leadership_max_60m"] >= LEADERSHIP_SCALE_THRESHOLD:
                    leadership_context["size_scaling_reason"] = "leadership_max_60m_scaled"
                    entry_quality = "LEADERSHIP_SCALED"
                else:
                    leadership_context["size_scaling_reason"] = "base_core_size"

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


            # v6.6 ROT_MICRO: independent tiny continuation harvester.
            # Only considered if core leadership entry did not already allow a trade.
            if not entry_allowed and ENABLE_ROT_MICRO_LIVE:
                rot_ok, rot_reason, rot_ctx = is_rot_micro_candidate(cur, symbol, momentum, trend)
                if rot_ok:
                    okx_tradable, okx_tradability_reason = is_okx_symbol_live_tradable(symbol)
                    if not okx_tradable:
                        block_reason = f"rot_micro_okx_not_tradable_{okx_tradability_reason}"
                    elif get_live_real_open_count(cur) >= MAX_OPEN_TRADES:
                        block_reason = "rot_micro_max_open_trades"
                    elif ENABLE_SAME_SYMBOL_STACKING_LIMIT and get_open_same_symbol_real_count(cur, symbol) >= MAX_SAME_SYMBOL_OPEN:
                        block_reason = "rot_micro_max_same_symbol_open"
                    else:
                        entry_allowed = True
                        entry_quality = "ROT_MICRO_V1"
                        leadership_context = leadership_context or {}
                        leadership_context.update(rot_ctx)
                        leadership_context["prior_avg_peak"] = rot_ctx.get("leadership_max_60m", 0)
                        leadership_context["leadership_mode"] = "ROT_MICRO"
                        leadership_context["market_os_engine"] = "ROT_MICRO"
                        leadership_context["size_scaling_reason"] = "rot_micro_fixed_5gbp"
                        block_reason = None
                elif block_reason is None:
                    block_reason = rot_reason

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

            if leadership_context:
                safe_update_signal_telemetry(cur, signal_id, {
                    "lifecycle_phase": leadership_context.get("lifecycle_phase") or leadership_context.get("leadership_phase"),
                    "leadership_phase": leadership_context.get("leadership_phase"),
                    "prior_lifecycle_phase": leadership_context.get("prior_lifecycle_phase"),
                    "leadership_transition": leadership_context.get("leadership_transition"),
                    "leadership_score_at_signal": leadership_context.get("prior_avg_peak"),
                    "leadership_score_30m_ago_at_signal": leadership_context.get("leadership_score_30m_ago") or leadership_context.get("score_30m_ago"),
                    "leadership_delta_5m_at_signal": leadership_context.get("leadership_delta_5m"),
                    "leadership_delta_15m_at_signal": leadership_context.get("leadership_delta_15m"),
                    "leadership_delta_30m_at_signal": leadership_context.get("leadership_delta_30m") or leadership_context.get("delta_30m"),
                    "leadership_delta_60m_at_signal": leadership_context.get("leadership_delta_60m"),
                    "leadership_age_minutes_at_signal": leadership_context.get("leadership_age_minutes"),
                    "leadership_rank_at_signal": leadership_context.get("leadership_rank"),
                    "market_near_count_at_signal": leadership_context.get("market_near_count"),
                    "market_core_count_at_signal": leadership_context.get("market_core_count"),
                    "market_aggressive_count_at_signal": leadership_context.get("market_aggressive_count"),
                    "market_monster_count_at_signal": leadership_context.get("market_monster_count"),
                    "shadow_emergence_detected": leadership_context.get("shadow_emergence_detected"),
                    "shadow_emergence_reason": leadership_context.get("shadow_emergence_reason"),
                    "shadow_quiet_continuation_detected": leadership_context.get("shadow_quiet_continuation_detected"),
                    "shadow_quiet_continuation_reason": leadership_context.get("shadow_quiet_continuation_reason"),
                    "shadow_cqe_detected": leadership_context.get("shadow_cqe_detected"),
                    "shadow_cqe_reason": leadership_context.get("shadow_cqe_reason"),
                    "cqe_quality_score": leadership_context.get("cqe_quality_score"),
                    "cqe_momentum_accel_30m": (leadership_context.get("cqe_context") or {}).get("cqe_momentum_accel_30m"),
                    "cqe_trend_accel_30m": (leadership_context.get("cqe_context") or {}).get("cqe_trend_accel_30m"),
                    "cqe_trend_std_30m": (leadership_context.get("cqe_context") or {}).get("cqe_trend_std_30m"),
                    "cqe_min_trend_30m": (leadership_context.get("cqe_context") or {}).get("cqe_min_trend_30m"),
                    "cqe_trend_range_30m": (leadership_context.get("cqe_context") or {}).get("cqe_trend_range_30m"),
                    "cqe_positive_trend_ratio_30m": (leadership_context.get("cqe_context") or {}).get("cqe_positive_trend_ratio_30m")
                })
        except Exception as e:
            print(f"⚠️ signals_raw intelligence update skipped: {e}", flush=True)

        # ================= SHADOW CQE ENTRY ENGINE =================
        try:
            if decision == "LONG" and leadership_context:
                maybe_open_shadow_cqe_trade(
                    cur,
                    symbol,
                    price,
                    momentum,
                    trend,
                    signal_id,
                    signal_time,
                    leadership_context.get("cqe_context"),
                    leadership_context,
                )
        except Exception as e:
            print(f"⚠️ shadow CQE entry skipped: {e}", flush=True)

        # ================= BPT CQE LIFECYCLE SHADOW ENTRY ENGINE =================
        # Opens/manages the new v6.2 BPT lifecycle probes. Shadow by default.
        try:
            if decision == "LONG" and leadership_context:
                maybe_open_bpt_cqe_probe(
                    cur,
                    symbol,
                    price,
                    momentum,
                    trend,
                    signal_id,
                    signal_time,
                    leadership_context,
                )
        except Exception as e:
            print(f"⚠️ BPT CQE lifecycle probe skipped: {e}", flush=True)

        # ================= PERSISTENCE HUNTER SHADOW ENTRY ENGINE =================
        # Tests no-density + persistent leadership as a separate shadow architecture.
        try:
            if decision == "LONG":
                maybe_open_persistence_hunter_trade(
                    cur,
                    symbol,
                    price,
                    momentum,
                    trend,
                    signal_id,
                    signal_time,
                )
        except Exception as e:
            print(f"⚠️ Persistence Hunter entry skipped: {e}", flush=True)

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

                entry_trade_size = get_trade_size_for_context(entry_quality, leadership_context)
                entry_quote_size = get_trade_size_quote_for_context(entry_quality, leadership_context)

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
                    f"🚀 <b>ENTRY</b> | {symbol} LONG\n"
                    f"{entry_quality} | {fmt_money(entry_trade_size)} | OKX {fmt_money(entry_quote_size)}\n"
                    f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
                    f"Phase: {short_phase(leadership_context.get('lifecycle_phase') or leadership_context.get('leadership_phase'))} "
                    f"({leadership_context.get('leadership_transition') or 'n/a'})\n"
                    f"Score {fmt_num(leadership_context.get('prior_avg_peak'))} | "
                    f"Δ30 {fmt_num(leadership_context.get('leadership_delta_30m') or leadership_context.get('delta_30m'))} | "
                    f"Age {fmt_num(leadership_context.get('leadership_age_minutes'), 1)}m | "
                    f"Rank #{leadership_context.get('leadership_rank') or 'n/a'}\n"
                    f"S/R/M: {leadership_context.get('prior_successes')} / {leadership_context.get('prior_runners')} / {leadership_context.get('monsters') or 0}\n"
                    f"Breadth N/C/A/M: {leadership_context.get('market_near_count') or 0}/"
                    f"{leadership_context.get('market_core_count') or 0}/"
                    f"{leadership_context.get('market_aggressive_count') or 0}/"
                    f"{leadership_context.get('market_monster_count') or 0}\n"
                    f"Slots {live_open_after_entry}/{MAX_OPEN_TRADES} | Same {same_symbol_after_entry}/{MAX_SAME_SYMBOL_OPEN}\n"
                    f"ID {trade_id}\n\n"
                    f"<b>Leaders</b>\n{top_leaders_text}"
                )

                okx_entry_result = okx_place_market_order(
                    cur=cur,
                    trade_id=trade_id,
                    symbol=symbol,
                    direction="LONG",
                    action="entry",
                    price=price,
                    entry_price=price,
                    trade_size_quote=entry_quote_size
                )

                # v6.6.10 CRITICAL: persist the real trade + OKX order log immediately.
                # Previously, later lifecycle/update exceptions could rollback the trade row
                # after OKX had already bought on exchange.
                if not okx_entry_result.get("success"):
                    cur.execute("""
                        UPDATE bot_trades_v4
                        SET status = 'CLOSED',
                            closed_at = NOW(),
                            close_reason = %s
                        WHERE id = %s
                    """, (f"okx_entry_failed: {okx_entry_result.get('reason') or okx_entry_result.get('error')}", trade_id))
                    conn.commit()
                    print(f"🚨 REAL ENTRY DB SAVED AS FAILED | {symbol} | id={trade_id} | reason={okx_entry_result}", flush=True)
                    send_telegram_alert(
                        f"🚨 <b>ENTRY FAILED AFTER DB CREATE</b> | {symbol}\n"
                        f"Trade row saved/closed for audit. Reason: {okx_entry_result.get('reason') or okx_entry_result.get('error')}\n"
                        f"ID {trade_id}"
                    )
                else:
                    conn.commit()
                    print(f"✅ REAL ENTRY PERSISTED | {symbol} | id={trade_id}", flush=True)

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

        # ================= SHADOW CQE EXIT / UPDATE ENGINE =================
        try:
            process_shadow_cqe_trades(cur, symbol, price, momentum, trend, now)
        except Exception as e:
            print(f"⚠️ shadow CQE processing skipped: {e}", flush=True)

        # ================= BPT CQE LIFECYCLE SHADOW PROCESSING =================
        # Updates BPT probes, confirms/upgrades when conditions are met, and exits by row-specific lifecycle rules.
        try:
            process_bpt_cqe_lifecycle_trades(cur, symbol, price, momentum, trend, now)
        except Exception as e:
            print(f"⚠️ BPT CQE lifecycle processing skipped: {e}", flush=True)

        # ================= PERSISTENCE HUNTER SHADOW PROCESSING =================
        try:
            process_persistence_hunter_trades(cur, symbol, price, momentum, trend, now)
        except Exception as e:
            print(f"⚠️ Persistence Hunter processing skipped: {e}", flush=True)

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
                entry_quality,
                COALESCE(partial_bank_4_done, FALSE) AS partial_bank_done,
                COALESCE(partial_bank_realized_pnl_gbp, 0) AS partial_bank_realized_gbp
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = FALSE
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, entry_quality, partial_bank_done, partial_bank_realized_gbp) in open_trades:
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

            try:
                update_trade_telemetry_v1(
                    cur,
                    tid,
                    sym,
                    opened_at=opened_at,
                    peak_pnl_percent=current_peak,
                    is_exit=False
                )
            except Exception as e:
                print(f"⚠️ TELEMETRY real update failed: {e}", flush=True)

            close_reason = None
            exit_architecture = None
            decay_triggered = False
            adaptive_exit_triggered = False
            slot_recycle_candidate = False
            drawdown_from_peak = current_peak - pnl_percent

            # v6.6 partial profit bank: take 25% at +4%, leave runner open.
            maybe_partial_profit_bank(
                cur, tid, sym, direction, entry_price, price, entry_quality,
                current_peak, pnl_percent, opened_at, mins,
                {"leadership_max_60m": get_recent_leadership_max(cur, sym, 60)}
            )

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
                remaining_fraction = (1.0 - PARTIAL_BANK_FRACTION) if partial_bank_done else 1.0
                pnl_gbp = ((pnl_percent / 100) * trade_size_for_pnl * remaining_fraction) + float(partial_bank_realized_gbp or 0)

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

                exit_lifecycle_context = get_lifecycle_context(cur, sym)

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
                    "leadership_momentum_at_exit": momentum,
                    "lifecycle_phase_at_exit": exit_lifecycle_context.get("lifecycle_phase"),
                    "prior_lifecycle_phase_at_exit": exit_lifecycle_context.get("prior_lifecycle_phase"),
                    "leadership_transition_at_exit": exit_lifecycle_context.get("leadership_transition"),
                    "leadership_score_at_exit": exit_lifecycle_context.get("leadership_score"),
                    "leadership_delta_30m_at_exit": exit_lifecycle_context.get("leadership_delta_30m"),
                    "leadership_rank_at_exit": exit_lifecycle_context.get("leadership_rank")
                })

                try:
                    update_trade_telemetry_v1(
                        cur,
                        tid,
                        sym,
                        opened_at=opened_at,
                        peak_pnl_percent=current_peak,
                        is_exit=True
                    )
                except Exception as e:
                    print(f"⚠️ TELEMETRY exit update failed: {e}", flush=True)

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
                    f"💰 <b>CLOSED</b> | {sym} LONG\n"
                    f"PnL <b>{fmt_num(pnl_percent)}%</b> | {fmt_money(pnl_gbp)} | Peak {fmt_num(current_peak)}%\n"
                    f"DD {fmt_num(drawdown_from_peak)}% | {close_reason}\n"
                    f"Exit T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
                    f"Phase: {format_leadership_compact(exit_lifecycle_context)}\n"
                    f"Latest: mom {fmt_num((latest_signal_state or {}).get('momentum'))} "
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
    force_okx_cache_refresh_if_empty("build_telegram_health_message")
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
        f"Engine: LEADERSHIP_LIVE + BPT_CQE_LIFECYCLE_V1 + SHADOW_CQE_V1 + SHADOW_EMERGENCE\n"
        f"Live orders: {ENABLE_LIVE_ORDERS}\n"
        f"Last signal: {last_signal}\n"
        f"Signals 1h: {signals_1h}\n"
        f"Last real trade: {last_trade}\n"
        f"Real trades 24h: {trades_24h}\n"
        f"Open real: {open_real}\n"
        f"Scored leadership signals 24h: {scored_24h}\n"
        f"Leadership snapshots 24h: {snapshots_24h}\n"
        f"Stable: score>={STABLE_LEADER_MIN_SCORE}, Δ{STABLE_LEADER_DELTA_MIN}..{STABLE_LEADER_DELTA_MAX}\n"
        f"Shadow ignition: {ENABLE_SHADOW_EMERGENCE_TELEMETRY} | Δ{CONTROLLED_IGNITION_DELTA_MIN}..{CONTROLLED_IGNITION_DELTA_MAX}\n"
        f"Shadow CQE: {ENABLE_SHADOW_CQE} | Q>={CQE_MIN_QUALITY_SCORE} | hold {CQE_SHADOW_HOLD_MINUTES}m\n"
        f"BPT lifecycle: {ENABLE_BPT_CQE_LIFECYCLE_SHADOW} | live probes {ENABLE_BPT_CQE_LIVE_PROBES} | live upgrades {ENABLE_BPT_CQE_LIVE_UPGRADES}\n"
        f"Density relaxed: {ENABLE_RELAXED_DENSITY_DEPENDENCY} | fixed real exits: {ENABLE_FIXED_TIME_EXITS_REAL}\n"
        f"Persistence Hunter: {ENABLE_PERSISTENCE_HUNTER_SHADOW} | live {ENABLE_PERSISTENCE_HUNTER_LIVE} | score>={PH_MIN_LEADERSHIP_SCORE} age {PH_MIN_CORE_AGE_MINUTES}-{PH_MAX_CORE_AGE_MINUTES}m\n"
        f"Max same symbol: {MAX_SAME_SYMBOL_OPEN}\n"
        f"OKX tradable cache: {len(OKX_TRADABLE_SPOT_INST_IDS)} pairs\n"
        f"Telemetry V1: {ENABLE_TELEMETRY_V1} | {TELEMETRY_VERSION}\n"
        f"Archetype engine: {ENABLE_ARCHETYPE_STATE_ENGINE} | adaptive BPT max hold {ENABLE_ADAPTIVE_BPT_MAX_HOLD} | tg dedupe {ENABLE_TELEGRAM_DEDUPE}\n"
        f"Market OS v6.6: {ENABLE_MARKET_OS_V66} | partial bank {ENABLE_PARTIAL_PROFIT_BANK_V66} @ {PARTIAL_BANK_TRIGGER_PCT}% x {int(PARTIAL_BANK_FRACTION*100)}% | rot micro {ENABLE_ROT_MICRO_LIVE}\n"
        f"Leadership scaling: {ENABLE_LEADERSHIP_SIZE_SCALING_V66} | >= {LEADERSHIP_SCALE_THRESHOLD} → {fmt_money(LEADERSHIP_SCALED_TRADE_SIZE_GBP)}"
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



def build_telegram_shadow_watch_message(cur, hours=12):
    try:
        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE COALESCE(shadow_quiet_continuation_detected, FALSE) = TRUE) AS quiet_count,
                COUNT(*) FILTER (WHERE COALESCE(shadow_emergence_detected, FALSE) = TRUE) AS ignition_count,
                COUNT(*) FILTER (WHERE COALESCE(shadow_cqe_detected, FALSE) = TRUE) AS cqe_count,
                COUNT(*) FILTER (WHERE decision = 'LONG') AS long_count
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
        """, (hours,))
        quiet_count, ignition_count, cqe_count, long_count = cur.fetchone() or (0, 0, 0, 0)

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE status = 'OPEN') AS open_cqe,
                COUNT(*) FILTER (WHERE status = 'CLOSED') AS closed_cqe,
                COALESCE(ROUND(AVG(pnl_percent) FILTER (WHERE status = 'CLOSED')::numeric, 3), 0) AS avg_closed_pnl,
                COALESCE(ROUND(SUM(pnl_gbp) FILTER (WHERE status = 'CLOSED')::numeric, 3), 0) AS closed_pnl_gbp,
                COALESCE(ROUND(AVG(peak_pnl_percent)::numeric, 3), 0) AS avg_peak
            FROM bot_trades_v4
            WHERE COALESCE(is_shadow, FALSE) = TRUE
              AND entry_quality = 'SHADOW_CQE_V1'
              AND opened_at >= NOW() - (%s || ' hours')::INTERVAL
        """, (hours,))
        open_cqe, closed_cqe, avg_closed_pnl, closed_pnl_gbp, avg_peak = cur.fetchone() or (0, 0, 0, 0, 0)

        cur.execute("""
            SELECT
                id,
                symbol,
                opened_at,
                entry_price,
                peak_pnl_percent,
                COALESCE(cqe_quality_score_at_entry, 0) AS q
            FROM bot_trades_v4
            WHERE COALESCE(is_shadow, FALSE) = TRUE
              AND entry_quality = 'SHADOW_CQE_V1'
              AND status = 'OPEN'
            ORDER BY opened_at DESC
            LIMIT 10
        """)
        open_rows = cur.fetchall()

        cur.execute("""
            SELECT
                timestamp,
                symbol,
                momentum,
                trend,
                COALESCE(cqe_quality_score, 0) AS q,
                COALESCE(cqe_momentum_accel_30m, 0) AS m_acc,
                COALESCE(cqe_trend_accel_30m, 0) AS t_acc,
                block_reason
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
              AND COALESCE(shadow_cqe_detected, FALSE) = TRUE
            ORDER BY timestamp DESC
            LIMIT 8
        """, (hours,))
        cqe_signal_rows = cur.fetchall()

        lines = [
            f"🕵️ <b>Shadow Watch {hours}h</b>",
            f"LONG signals: <b>{long_count}</b>",
            f"CQE signals: <b>{cqe_count}</b> | Open shadow CQE: <b>{open_cqe}</b> | Closed: <b>{closed_cqe}</b>",
            f"CQE closed avg: <b>{fmt_num(avg_closed_pnl)}%</b> | {fmt_money(closed_pnl_gbp)} | Avg peak {fmt_num(avg_peak)}%",
            f"Quiet old shadows: {quiet_count} | Ignition shadows: {ignition_count}",
        ]

        if open_rows:
            lines.append("\n<b>Open CQE shadow trades</b>")
            for tid, sym, opened_at, entry_price, peak, q in open_rows:
                lines.append(f"{sym} | Q{q} | peak {fmt_num(peak)}% | ID {tid}")

        if cqe_signal_rows:
            lines.append("\n<b>Latest CQE signals</b>")
            for ts, sym, mom, tr, q, m_acc, t_acc, block in cqe_signal_rows:
                lines.append(
                    f"{sym} | Q{q} | T/M {fmt_num(tr)}/{fmt_num(mom)} | "
                    f"acc {fmt_num(t_acc)}/{fmt_num(m_acc)} | {block or 'no_block'}"
                )
        else:
            lines.append("\nNo CQE candidates in this window.")

        return "\n".join(lines)

    except Exception as e:
        print(f"⚠️ shadow watch failed: {e}", flush=True)
        return f"🕵️ <b>Shadow Watch</b>\nUnavailable: {e}"



def build_telegram_telemetry_message(cur, hours=24):
    try:
        cur.execute("""
            SELECT
                COUNT(*) AS trades,
                COUNT(*) FILTER (WHERE telemetry_version = 'TELEMETRY_V1') AS telemetry_trades,
                COUNT(*) FILTER (WHERE density_phase = 'NO_DENSITY_CLEAN') AS no_density_clean,
                COUNT(*) FILTER (WHERE crowding_state = 'CROWDED') AS crowded,
                ROUND(AVG(leadership_age_minutes)::numeric, 1) AS avg_leadership_age,
                ROUND(AVG(stable_leadership_hits)::numeric, 1) AS avg_stable_hits,
                ROUND(AVG(leadership_velocity)::numeric, 3) AS avg_leadership_velocity
            FROM bot_trades_v4
            WHERE opened_at >= NOW() - (%s || ' hours')::INTERVAL
        """, (hours,))
        row = cur.fetchone() or (0,0,0,0,0,0,0)
        trades, telemetry_trades, no_density_clean, crowded, avg_age, avg_hits, avg_velocity = row

        cur.execute("""
            SELECT
                COALESCE(leadership_phase, 'UNKNOWN') AS phase,
                COALESCE(density_phase, 'UNKNOWN') AS density,
                COUNT(*) AS trades,
                ROUND(AVG(COALESCE(peak_pnl_percent,0))::numeric, 3) AS avg_peak,
                ROUND(AVG(COALESCE(pnl_percent,0))::numeric, 3) AS avg_final
            FROM bot_trades_v4
            WHERE opened_at >= NOW() - (%s || ' hours')::INTERVAL
            GROUP BY 1,2
            ORDER BY trades DESC
            LIMIT 8
        """, (hours,))
        rows = cur.fetchall()

        lines = [
            f"📡 <b>Telemetry V1 {hours}h</b>",
            f"Trades: <b>{trades}</b> | Telemetry tagged: <b>{telemetry_trades}</b>",
            f"No-density clean: <b>{no_density_clean}</b> | Crowded: <b>{crowded}</b>",
            f"Avg leadership age: <b>{avg_age}</b>m | Stable hits: <b>{avg_hits}</b> | Velocity: <b>{avg_velocity}</b>",
        ]

        if rows:
            lines.append("\n<b>Phase / density breakdown</b>")
            for phase, density, t, avg_peak, avg_final in rows:
                lines.append(f"{phase} / {density}: {t} | peak {avg_peak}% | final {avg_final}%")

        return "\n".join(lines)

    except Exception as e:
        return f"📡 <b>Telemetry V1</b> unavailable: {e}"



# =========================
# 📲 v6.6.2 TELEGRAM MARKET OS COMMAND BUILDERS
# =========================

def build_telegram_rejection_message(cur, hours=6):
    """Show recent block/rejection reasons so silence is explainable from Telegram."""
    try:
        cur.execute("""
            SELECT
                COALESCE(block_reason, decision, 'no_reason_recorded') AS reason,
                COUNT(*) AS count
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
            GROUP BY 1
            ORDER BY count DESC
            LIMIT 12
        """, (hours,))
        rows = cur.fetchall() or []

        cur.execute("""
            SELECT
                COUNT(*) AS total_signals,
                COUNT(*) FILTER (WHERE decision = 'LONG') AS long_decisions,
                COUNT(*) FILTER (WHERE block_reason IS NOT NULL) AS blocked_with_reason,
                MAX(timestamp) AS last_signal
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' hours')::INTERVAL
        """, (hours,))
        total_signals, long_decisions, blocked_with_reason, last_signal = cur.fetchone() or (0, 0, 0, None)

        lines = [
            f"🚫 <b>Rejections / Blocks {hours}h</b>",
            f"Signals: <b>{total_signals}</b> | LONG decisions: <b>{long_decisions}</b>",
            f"Blocked/reason-tagged: <b>{blocked_with_reason}</b>",
            f"Last signal: {last_signal}",
        ]

        if rows:
            lines.append("\n<b>Top reasons</b>")
            for reason, count in rows:
                lines.append(f"{reason}: <b>{count}</b>")
        else:
            lines.append("\nNo signal rows found in this window.")

        lines.append(
            "\n<b>Note</b>\n"
            "If most rows show no_reason_recorded, the next refinement should tag soft blocks "
            "as weak_trend / weak_momentum / leadership_too_low / no_regime_alignment."
        )
        return "\n".join(lines)

    except Exception as e:
        print(f"⚠️ rejection message failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return f"🚫 <b>Rejections</b> unavailable: {e}"


def build_telegram_recent_message(cur, hours=6):
    """Show recent entries, exits, banks, and current activity."""
    try:
        cur.execute("""
            SELECT
                opened_at,
                closed_at,
                symbol,
                status,
                COALESCE(is_shadow, FALSE) AS is_shadow,
                COALESCE(entry_quality, current_archetype, 'UNKNOWN') AS engine,
                COALESCE(pnl_percent, 0) AS pnl_percent,
                COALESCE(peak_pnl_percent, 0) AS peak_pnl_percent,
                COALESCE(close_reason, exit_architecture, '') AS reason,
                COALESCE(partial_bank_4_done, FALSE) AS banked
            FROM bot_trades_v4
            WHERE opened_at >= NOW() - (%s || ' hours')::INTERVAL
               OR closed_at >= NOW() - (%s || ' hours')::INTERVAL
            ORDER BY COALESCE(closed_at, opened_at) DESC
            LIMIT 12
        """, (hours, hours))
        rows = cur.fetchall() or []

        cur.execute("""
            SELECT
                COUNT(*) FILTER (WHERE opened_at >= NOW() - (%s || ' hours')::INTERVAL AND COALESCE(is_shadow,FALSE)=FALSE) AS real_opened,
                COUNT(*) FILTER (WHERE opened_at >= NOW() - (%s || ' hours')::INTERVAL AND COALESCE(is_shadow,FALSE)=TRUE) AS shadow_opened,
                COUNT(*) FILTER (WHERE closed_at >= NOW() - (%s || ' hours')::INTERVAL AND COALESCE(is_shadow,FALSE)=FALSE) AS real_closed,
                COUNT(*) FILTER (WHERE status='OPEN' AND COALESCE(is_shadow,FALSE)=FALSE) AS open_real,
                COUNT(*) FILTER (WHERE status='OPEN' AND COALESCE(is_shadow,FALSE)=TRUE) AS open_shadow
            FROM bot_trades_v4
        """, (hours, hours, hours))
        real_opened, shadow_opened, real_closed, open_real, open_shadow = cur.fetchone() or (0,0,0,0,0)

        lines = [
            f"🕒 <b>Recent Bot Activity {hours}h</b>",
            f"Real opened: <b>{real_opened}</b> | Real closed: <b>{real_closed}</b> | Shadow opened: <b>{shadow_opened}</b>",
            f"Open real: <b>{open_real}</b> | Open shadow: <b>{open_shadow}</b>",
        ]

        if rows:
            lines.append("\n<b>Latest trades/events</b>")
            for opened_at, closed_at, sym, status, is_shadow, engine, pnl, peak, reason, banked in rows:
                mode = "SHADOW" if is_shadow else "REAL"
                bank_txt = " | banked ✅" if banked else ""
                time_txt = closed_at or opened_at
                reason_txt = f" | {reason}" if reason else ""
                lines.append(
                    f"{sym} {mode} {status} | pnl {fmt_num(pnl)}% | peak {fmt_num(peak)}%{bank_txt}{reason_txt} | {time_txt}"
                )
        else:
            lines.append("\nNo trade events in this window.")

        return "\n".join(lines)

    except Exception as e:
        print(f"⚠️ recent message failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return f"🕒 <b>Recent Activity</b> unavailable: {e}"


def build_telegram_regime_message(cur, hours=3):
    force_okx_cache_refresh_if_empty("build_telegram_regime_message")
    """More regime-focused version of /market."""
    try:
        market_msg = build_market_state_message(cur, hours)

        cur.execute("""
            WITH recent AS (
                SELECT
                    s.symbol,
                    s.momentum,
                    s.trend,
                    COALESCE(l.leadership_score,0) AS leadership_score
                FROM signals_raw s
                LEFT JOIN LATERAL (
                    SELECT leadership_score
                    FROM leadership_state_history h
                    WHERE h.symbol = s.symbol
                      AND snapshot_time <= s.timestamp
                    ORDER BY snapshot_time DESC
                    LIMIT 1
                ) l ON true
                WHERE s.timestamp >= NOW() - (%s || ' hours')::INTERVAL
            )
            SELECT
                COUNT(*) AS signals,
                COUNT(DISTINCT symbol) AS symbols,
                COUNT(*) FILTER (WHERE leadership_score >= 2.0) AS elite_leader_hits,
                COUNT(*) FILTER (WHERE momentum >= 0.5 AND trend >= 0.25 AND leadership_score >= 1.25) AS core_hits,
                COUNT(*) FILTER (WHERE momentum >= 0.3 AND trend >= 0.10 AND leadership_score >= 0.75 AND leadership_score < 1.0) AS rot_hits,
                ROUND(AVG(momentum)::numeric,3) AS avg_momentum,
                ROUND(AVG(trend)::numeric,3) AS avg_trend,
                ROUND(MAX(leadership_score)::numeric,3) AS max_leadership
            FROM recent
        """, (hours,))
        sigs, syms, elite_hits, core_hits, rot_hits, avg_mom, avg_trend, max_lead = cur.fetchone() or (0,0,0,0,0,0,0,0)

        lines = [
            f"🧠 <b>Regime Detail {hours}h</b>",
            f"Signals: <b>{sigs}</b> | Symbols: <b>{syms}</b>",
            f"Elite leader hits: <b>{elite_hits}</b> | Core hits: <b>{core_hits}</b> | Rot hits: <b>{rot_hits}</b>",
            f"Avg momentum/trend: <b>{avg_mom}</b> / <b>{avg_trend}</b>",
            f"Max leadership: <b>{max_lead}</b>",
            "",
            market_msg,
        ]
        return "\n".join(lines)

    except Exception as e:
        print(f"⚠️ regime message failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return f"🧠 <b>Regime</b> unavailable: {e}"


def handle_telegram_command(text):
    cmd = (text or "").strip().lower().split()[0] if text else "/help"
    conn = get_db()
    cur = conn.cursor()
    try:
        ensure_signal_leadership_scores_table(cur)

        if cmd in ["/status", "/daily", "/summary", "/pnl"]:
            return build_telegram_summary_message(cur, 24)

        if cmd in ["/open", "/trades", "/positions", "/pos"]:
            return build_telegram_open_trades_message(cur)

        if cmd in ["/leaders", "/leadership"]:
            return "🧠 <b>Top Leadership States</b>\n" + get_top_leaders_text(cur, 10)

        if cmd in ["/lifecycle", "/phases"]:
            return "🧬 <b>Leadership Lifecycle</b>\n" + get_lifecycle_dashboard_text(cur, 12)

        if cmd in ["/telemetry", "/tel"]:
            return build_telegram_telemetry_message(cur, 24)

        if cmd in ["/market", "/why", "/state"]:
            return build_market_state_message(cur, 3)

        if cmd in ["/regime"]:
            return build_telegram_regime_message(cur, 3)

        if cmd in ["/rejections", "/rejects", "/blocks", "/blocked"]:
            return build_telegram_rejection_message(cur, 6)

        if cmd in ["/recent", "/activity", "/events"]:
            return build_telegram_recent_message(cur, 6)

        if cmd in ["/shadows", "/shadow", "/cqe"]:
            return build_telegram_shadow_watch_message(cur, 12)

        if cmd in ["/hunter", "/ph", "/persistence"]:
            return build_telegram_persistence_hunter_message(cur, 24)

        if cmd == "/health":
            return build_telegram_health_message(cur)

        if cmd == "/help":
            return (
                "🤖 <b>Trading Bot Commands</b>\n"
                "/market - explain current market state + why bot is/isn't trading\n"
                "/regime - deeper regime detail\n"
                "/rejections - recent block/rejection reasons\n"
                "/recent - recent entries/exits/banks/activity\n"
                "/positions - open positions\n"
                "/leaders - current leadership leaderboard\n"
                "/status - rolling 24h PnL summary\n"
                "/telemetry - Telemetry V1 summary\n"
                "/shadows - CQE shadow watch\n"
                "/hunter - Persistence Hunter shadow report\n"
                "/lifecycle - leadership lifecycle dashboard\n"
                "/health - server/bot health\n"
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
        "ENABLE_BPT_CQE_LIFECYCLE_SHADOW": ENABLE_BPT_CQE_LIFECYCLE_SHADOW,
        "ENABLE_BPT_CQE_LIVE_PROBES": ENABLE_BPT_CQE_LIVE_PROBES,
        "ENABLE_RELAXED_DENSITY_DEPENDENCY": ENABLE_RELAXED_DENSITY_DEPENDENCY,
        "ENABLE_TELEMETRY_V1": ENABLE_TELEMETRY_V1,
        "TELEMETRY_VERSION": TELEMETRY_VERSION,
        "ENABLE_FIXED_TIME_EXITS_REAL": ENABLE_FIXED_TIME_EXITS_REAL,
        "DENSITY_NULL_IS_VALID": DENSITY_NULL_IS_VALID,
        "ENABLE_BPT_CQE_LIVE_UPGRADES": ENABLE_BPT_CQE_LIVE_UPGRADES,
        "BPT_CQE_PROBE_SIZE_GBP": BPT_CQE_PROBE_SIZE_GBP,
        "BPT_MEDIUM_UPGRADE_GBP": BPT_MEDIUM_UPGRADE_GBP,
        "ENABLE_PERSISTENCE_HUNTER_SHADOW": ENABLE_PERSISTENCE_HUNTER_SHADOW,
        "ENABLE_PERSISTENCE_HUNTER_LIVE": ENABLE_PERSISTENCE_HUNTER_LIVE,
        "PH_MIN_LEADERSHIP_SCORE": PH_MIN_LEADERSHIP_SCORE,
        "PH_MIN_CORE_AGE_MINUTES": PH_MIN_CORE_AGE_MINUTES,
        "PH_MAX_CORE_AGE_MINUTES": PH_MAX_CORE_AGE_MINUTES,
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
        "ENABLE_SHADOW_EMERGENCE_TELEMETRY": ENABLE_SHADOW_EMERGENCE_TELEMETRY,
        "ENABLE_SHADOW_QUIET_CONTINUATION": ENABLE_SHADOW_QUIET_CONTINUATION,
        "ENABLE_SHADOW_CQE": ENABLE_SHADOW_CQE,
        "ENABLE_SHADOW_CQE_TELEGRAM_ALERTS": ENABLE_SHADOW_CQE_TELEGRAM_ALERTS,
        "CQE_MIN_QUALITY_SCORE": CQE_MIN_QUALITY_SCORE,
        "CQE_SHADOW_HOLD_MINUTES": CQE_SHADOW_HOLD_MINUTES,
        "CQE_MAX_OPEN_SHADOW_TRADES": CQE_MAX_OPEN_SHADOW_TRADES,
        "CQE_MAX_SAME_SYMBOL_SHADOW_OPEN": CQE_MAX_SAME_SYMBOL_SHADOW_OPEN,
        "QUIET_CONTINUATION_MAX_SCORE": QUIET_CONTINUATION_MAX_SCORE,
        "QUIET_CONTINUATION_MIN_TREND": QUIET_CONTINUATION_MIN_TREND,
        "QUIET_CONTINUATION_MIN_MOMENTUM": QUIET_CONTINUATION_MIN_MOMENTUM,
        "QUIET_CONTINUATION_MAX_DELTA_30M": QUIET_CONTINUATION_MAX_DELTA_30M,
        "CONTROLLED_IGNITION_MIN_SCORE": CONTROLLED_IGNITION_MIN_SCORE,
        "CONTROLLED_IGNITION_MAX_SCORE": CONTROLLED_IGNITION_MAX_SCORE,
        "CONTROLLED_IGNITION_DELTA_MIN": CONTROLLED_IGNITION_DELTA_MIN,
        "CONTROLLED_IGNITION_DELTA_MAX": CONTROLLED_IGNITION_DELTA_MAX,
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
        "ENABLE_MARKET_OS_V66": ENABLE_MARKET_OS_V66,
        "ENABLE_PARTIAL_PROFIT_BANK_V66": ENABLE_PARTIAL_PROFIT_BANK_V66,
        "PARTIAL_BANK_TRIGGER_PCT": PARTIAL_BANK_TRIGGER_PCT,
        "PARTIAL_BANK_FRACTION": PARTIAL_BANK_FRACTION,
        "ENABLE_ROT_MICRO_LIVE": ENABLE_ROT_MICRO_LIVE,
        "ROT_MICRO_TRADE_SIZE_GBP": ROT_MICRO_TRADE_SIZE_GBP,
        "ENABLE_OKX_TRADABILITY_SELF_HEAL_V66": ENABLE_OKX_TRADABILITY_SELF_HEAL_V66,
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


# =========================
# 📡 TELEGRAM HEALTH DEBUG
# =========================

def build_bpt_debug_summary():
    try:
        return {
            "live_probes": ENABLE_BPT_CQE_LIVE_PROBES,
            "live_upgrades": ENABLE_BPT_CQE_LIVE_UPGRADES,
            "extreme_live": ENABLE_EXTREME_RUNNER_ROW_LIVE,
            "monster_live": ENABLE_HIGH_MONSTER_ROW_LIVE,
            "okx_pairs": OKX_TRADABLE_SPOT_COUNT if 'OKX_TRADABLE_SPOT_COUNT' in globals() else 0
        }
    except Exception as e:
        return {"bpt_debug_error": str(e)}



# =========================
# v6.2.2 NOTES
# =========================
# FIXES:
# - Forces BPT live probes ON
# - Forces BPT live upgrades ON
# - Promotes EXTREME_RUNNER_ROW live
# - Adds Telegram health debug visibility
# - Preserves HIGH_MONSTER_ROW shadow-only


# =========================
# 🚀 EXTREME LIVE ROUTING
# =========================

def should_force_live_trade(lifecycle_row):
    try:
        if lifecycle_row == "EXTREME_RUNNER_ROW":
            return True
        return False
    except:
        return False



# =========================
# v6.2.4 NOTES
# =========================
# FINAL LIVE ROUTING FIX
# EXTREME_RUNNER_ROW now routes to REAL execution
# HIGH_MONSTER_ROW remains SHADOW
# MEDIUM_BALANCED_ROW remains SHADOW
# EARLY_INCUBATION_ROW remains SHADOW


# =========================
# v6.3.0 NOTES
# =========================
# Adds PERSISTENCE_HUNTER_V1 as shadow-only research engine.
# Thesis: no-density + stable leadership >=2.0 + 45-180m core age + persistent hits.
# Keeps current live leadership engine unchanged.
# Keeps BPT CQE lifecycle available but not relied on for this research branch.



# =========================
# v6.3.1 RELEASE NOTES
# =========================
# LIVE CHANGE:
# - Relaxed density dependency: NULL density no longer blocks quality.
# - Density is now context/crowding telemetry, not mandatory entry confirmation.
#
# EXIT CHANGE:
# - Fixed-time exits disabled for real/live trades.
# - Shadow fixed holds remain allowed for research visibility.
#
# SHADOW CHANGE:
# - Persistence Hunter remains shadow-only for validation.
#
# DEPLOYMENT INTENT:
# - Improve live capture of NEAR-style no-density leadership continuation
#   without replacing the current real leadership engine.



# =========================
# v6.4.0 TELEMETRY_V1 NOTES
# =========================
# Adds market-state observability without intentionally changing live entry/exit behavior:
# - leadership_age_minutes
# - stable_leadership_hits
# - leadership_rank
# - leadership_rank_stability
# - leadership_velocity
# - leadership_phase / lifecycle_phase
# - density_phase / crowding_state
# - first_density_seen / mins_to_density
# - density_at_exit / density_delta_at_exit
# - simultaneous_leader_count
# - top3_concentration
# - market_leadership_quality
# - leader_rotation_velocity
#
# Adds Telegram:
# - /telemetry
# - /tel


# =========================
# v6.5.1 NOTES
# =========================
# Adds workflow-safe, simulation-backed changes:
# - Runtime archetype tagging: FAST_IGNITION / SLOW_GRINDER / POTENTIAL_LATE_BLOOMER / HIGH_MONSTER_ROW / FAILED_NO_EXPANSION.
# - Adaptive BPT max-hold: no hard clock close while a strong leader is still healthy with DD < 1.5%.
# - Keeps bounded patience; does NOT remove max hold universally.
# - Adds Telegram trade-close de-dupe table and send_trade_telegram_once().
# - Makes close updates status-guarded so CLOSED alerts are not repeated for already closed rows.
# - Leaves live general-leader probes OFF by default.



# =========================
# v6.5.1 PATCH NOTES
# =========================
# Fixes telemetry SQL alias issue:
# - Qualifies ambiguous snapshot_time references in telemetry leadership queries.
# - Adds defensive telemetry rollback helper to prevent failed telemetry reads from poisoning
#   the rest of the webhook transaction.
#
# Trading logic unchanged from v6.5.0:
# - Archetype state engine remains active.
# - BPT max-hold extension remains active.
# - Telegram de-dupe / shadow close guards remain active.


# =========================
# v6.6.0 MARKET OS RELEASE NOTES
# =========================
# Adds simulation-backed monetization architecture:
# - Core entries preserved.
# - Leadership >=2.0 is treated as a sizing upgrade, not a duplicate async trade.
# - Partial profit bank: bank 25% at +4%, leave runner open.
# - ROT_MICRO live layer: tiny £5 continuation harvester, toggleable.
# - Telegram /market command explains market state, silence, leadership breadth and rejection reasons.
# - OKX tradability self-heal attempts forced refresh when runtime cache returns 0 pairs.
# - Stale shadow sweeper prevents old shadow rows clogging capacity.


# =========================
# 🤖 v6.6.1 TELEGRAM MARKET OS COMMAND SCAFFOLD (superseded by v6.6.2)
# =========================

ENABLE_MARKET_OS_COMMANDS = True  # legacy scaffold flag; real handlers wired in v6.6.2

MARKET_OS_COMMANDS = [
    "/market",
    "/health",
    "/leaders",
    "/positions",
    "/rejections",
    "/recent",
    "/regime"
]

def build_market_state_summary():
    return (
        "📡 MARKET STATE\n"
        "• Regime classification active\n"
        "• Leadership breadth tracking enabled\n"
        "• Rejection telemetry tracking enabled\n"
        "• Partial bank telemetry enabled\n"
        "• Market OS framework online"
    )

def build_rejection_summary():
    return (
        "🚫 REJECTION SUMMARY\n"
        "weak_trend\n"
        "leadership_decay\n"
        "fragmented_market\n"
        "failed_persistence\n"
        "rotational_low_quality"
    )

def build_regime_summary():
    return (
        "🧠 REGIME ENGINE\n"
        "DEAD_CHOP\n"
        "FRAGMENTED_ROTATION\n"
        "HEALTHY_ROTATION\n"
        "IGNITION\n"
        "EUPHORIC_EXPANSION"
    )

print("✅ v6.6.2 Telegram Market OS commands wired", flush=True)




# =========================
# v6.6.2 PATCH NOTES
# =========================
# Telegram command layer is now wired:
# - /market: current market state and why bot is/isn't trading.
# - /regime: deeper regime metrics.
# - /rejections: recent block/rejection reasons.
# - /recent: recent entries/exits/banks/activity.
# - /positions: alias for open trades.
#
# No strategy, sizing, banking, or execution logic changed from v6.6.1.


# =========================
# v6.6.3 SQL ALIAS FIX
# =========================
# Fixed broken SQL alias references:
# lsh.snapshot_time -> h.snapshot_time
# Prevents:
# ERROR: missing FROM-clause entry for table "lsh"
# No strategy logic changed.


# =========================
# v6.6.4 LEADERSHIP QUERY HOTFIX
# =========================
# Fixed lingering leadership SQL alias issues:
# ORDER BY h.snapshot_time DESC
# -> ORDER BY snapshot_time DESC
#
# Prevents:
# ERROR: missing FROM-clause entry for table "h"
#
# No strategy logic changed.


# =========================
# v6.6.5 TIMEZONE HOTFIX
# =========================
# Fixed:
# can't subtract offset-naive and offset-aware datetimes
#
# Standardized runtime timestamps to timezone-aware UTC.
# Added ensure_utc() helper for safe datetime comparisons.
#
# No strategy logic changed.


# =========================
# v6.6.6 STABILIZATION PATCH
# =========================
# Included fixes:
#
# ✅ Timezone-safe datetime handling
# ✅ Leadership SQL alias fixes
# ✅ Telegram command wiring
# ✅ OKX cache self-heal framework
# ✅ Rich rejection telemetry
# ✅ Telegram notification tracking placeholders
#
# No strategy logic changed.


# Defensive tradability cache guard
try:
    if 'okx_tradable_pairs' in globals():
        if get_okx_cache_count() == 0:
            emergency_refresh_okx_cache()
except Exception as e:
    print(f"⚠️ Cache guard failed: {e}", flush=True)




# =========================
# v6.6.7 EMERGENCY STABILIZATION NOTES
# =========================
# Fixes:
# - Prevents repeated same-symbol real buys when OKX execution succeeds but DB persistence is missing/lagging.
# - Adds in-memory same-symbol live cooldown guard.
# - Adds safe_age_minutes() timezone-safe helper.
#
# Operational impact:
# - If NEAR/any symbol was just bought, additional same-symbol live buys are blocked for 180 minutes.
# - Strategy thresholds unchanged.
# - Banking/scaling logic unchanged.



# =========================
# v6.6.8 PATCH NOTES
# =========================
# Fixes:
# - Hard OKX tradable cache self-heal on startup, health, market/regime commands, and webhook.
# - Health fallback for scored leadership count so it doesn't falsely show 0 when snapshots exist.
# - No strategy, scaling, banking, or execution threshold changes.



# =========================
# v6.6.9 OKX CACHE REFRESH WIRING FIX
# =========================
# Fixes:
# - Self-heal now calls the actual real refresh function:
#   refresh_okx_tradable_spot_instruments(force=True)
# - Cache count now reads OKX_TRADABLE_SPOT_INST_IDS, the real execution cache.
#
# This fixes:
# ❌ OKX CACHE STILL EMPTY after refresh attempts | refreshed_called=False
#
# No strategy, banking, scaling, lifecycle, or entry threshold changes.



# =========================
# v6.6.10 REAL TRADE PERSISTENCE FIX
# =========================
# Critical fixes:
# 1. Real open-trade exit/update SELECT now includes:
#    - partial_bank_4_done
#    - partial_bank_realized_pnl_gbp
#    matching the 10-field unpack.
#
# 2. Real OKX entry path now commits immediately after OKX entry handling.
#    This prevents later lifecycle/update exceptions from rolling back the real trade row
#    after OKX has already executed the buy.
#
# No strategy thresholds changed.
