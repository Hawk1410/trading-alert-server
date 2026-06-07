# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v8.4.1
# TITLE: AUTO-DISCOVERY SHADOW COINS + COIN HEALTH + PERSONALITY LEARNING
# =========================

print("🔥🔥🔥 MAIN.PY v8.4.1 AUTO-DISCOVERY SHADOW COINS RUNNING 🔥🔥🔥", flush=True)

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
# v7.0 CHANGE SUMMARY
# =========================
#
# ✅ Adds ADAPTIVE_DEAD_MARKET_LEADERS as a toggleable live BPT probe pathway.
# ✅ Research-backed condition:
#      market_median_peak_context < 2
#      leadership_rank <= 3
#      leadership_score < 0.5
# ✅ Does NOT alter stable/emerging leadership phase thresholds.
# ✅ Does NOT alter BPT confirmation, upgrades, exits, partial banking, or sizing.
# ✅ Tags adaptive probes with market_os_engine='ADAPTIVE_DEAD_MARKET_LEADER'
#    and size_scaling_reason='adaptive_dead_market_leader_probe'.
#
#
# =========================
# v7.1 CHANGE SUMMARY
# =========================
#
# ✅ Accounting-only upgrade.
# ✅ Keeps v7.0 adaptive dead-market leadership behaviour unchanged.
# ✅ Treats OKX quote-size orders as USDT, not GBP.
# ✅ Populates trade_size_usdt, entry_value_usdt, exit_value_usdt, pnl_usdt, usd_gbp_rate.
# ✅ Calculates pnl_gbp from pnl_usdt * USD_GBP_RATE.
# ✅ Leaves legacy trade_size_gbp/dynamic_trade_size_gbp in place for backwards compatibility.
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


def env_bool(name, default=False):
    raw = os.environ.get(name)
    if raw is None:
        return bool(default)
    return str(raw).strip().lower() in ("1", "true", "yes", "y", "on")




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

# v7.1 accounting: OKX quote-size orders are USDT. Convert reporting to GBP.
USD_GBP_RATE = float(os.environ.get("USD_GBP_RATE", "0.74") or 0.74)

# =========================
# CORE ENGINE SETTINGS
# =========================

MAX_OPEN_TRADES = int(os.environ.get("MAX_OPEN_TRADES", "5") or 5)
MAX_OPEN_SHADOW_TRADES = int(os.environ.get("MAX_OPEN_SHADOW_TRADES", "30") or 30)




DATA_VERSION = "v8.4_COIN_HEALTH_PERSONALITY_LEARNING"

# =========================
# 🦄 v6.7 TREND PERSISTENCE + CLEAN NAMING
# =========================
# Research-backed finding from CQE, BPT and Leadership simulations:
# trades that cannot sustain trend structure ~30m after entry have poor expectancy.
# Keep this isolated behind toggles so live behaviour can be reverted instantly.

CQE_CONTINUATION_ENTRY_QUALITY = os.environ.get(
    "CQE_CONTINUATION_ENTRY_QUALITY",
    "CQE_CONTINUATION_V1"
)
LEGACY_CQE_ENTRY_QUALITY = "SHADOW_CQE_V1"

ENABLE_TREND_PERSISTENCE_EXIT = os.environ.get(
    "ENABLE_TREND_PERSISTENCE_EXIT",
    "true"
).lower() == "true"
TREND_PERSISTENCE_CHECK_MINUTES = float(os.environ.get("TREND_PERSISTENCE_CHECK_MINUTES", "30") or 30)
TREND_PERSISTENCE_MIN_TREND = float(os.environ.get("TREND_PERSISTENCE_MIN_TREND", "0.15") or 0.15)

# v6.7: apply to continuation / lifecycle / leadership engines only.
# ROT_MICRO_V1 is deliberately excluded because the sweep did not support the same 0.15 rule.
TREND_PERSISTENCE_ENGINE_QUALITIES = {
    CQE_CONTINUATION_ENTRY_QUALITY,
    LEGACY_CQE_ENTRY_QUALITY,
    "LEADERSHIP_SCALED",
    "BPT_CQE_LIFECYCLE_V1",
}

# Telegram cleanup: keep entries/exits/errors/daily/health, suppress plumbing/debug noise.
ENABLE_OKX_SUCCESS_TELEGRAM = os.environ.get("ENABLE_OKX_SUCCESS_TELEGRAM", "false").lower() == "true"
ENABLE_BLOCKED_TRADE_TELEGRAM = os.environ.get("ENABLE_BLOCKED_TRADE_TELEGRAM", "false").lower() == "true"


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
ENABLE_LEADERSHIP_CORE_LIVE = os.environ.get("ENABLE_LEADERSHIP_CORE_LIVE", "false").lower() == "true"
ENABLE_LEADERSHIP_CORE_SHADOW = os.environ.get("ENABLE_LEADERSHIP_CORE_SHADOW", "true").lower() == "true"
LEADERSHIP_CORE_SHADOW_HOLD_MINUTES = float(os.environ.get("LEADERSHIP_CORE_SHADOW_HOLD_MINUTES", "120") or 120)
LEADERSHIP_CORE_SHADOW_HARD_STOP = float(os.environ.get("LEADERSHIP_CORE_SHADOW_HARD_STOP", "-0.60") or -0.60)

# Partial profit bank: v8.2 lowers the default because recent data showed +2% peaks fading to ~0%.
# This banks half the live position after a meaningful +1.5% expansion, while leaving a runner alive.
ENABLE_PARTIAL_PROFIT_BANK_V66 = os.environ.get("ENABLE_PARTIAL_PROFIT_BANK_V66", "true").lower() == "true"
PARTIAL_BANK_TRIGGER_PCT = float(os.environ.get("PARTIAL_BANK_TRIGGER_PCT", "1.50") or 1.50)
PARTIAL_BANK_FRACTION = float(os.environ.get("PARTIAL_BANK_FRACTION", "0.50") or 0.50)

# Rotational micro continuation layer. Independent from elite core, tiny size only.
ENABLE_ROT_MICRO_LIVE = os.environ.get("ENABLE_ROT_MICRO_LIVE", "false").lower() == "true"
ENABLE_ROT_MICRO_SHADOW = os.environ.get("ENABLE_ROT_MICRO_SHADOW", "true").lower() == "true"
ROT_MICRO_MIN_CONTEXT = float(os.environ.get("ROT_MICRO_MIN_CONTEXT", "4.0") or 4.0)
ROT_MICRO_SHADOW_HOLD_MINUTES = float(os.environ.get("ROT_MICRO_SHADOW_HOLD_MINUTES", "120") or 120)
ROT_MICRO_SHADOW_HARD_STOP = float(os.environ.get("ROT_MICRO_SHADOW_HARD_STOP", "-0.60") or -0.60)
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
# 🩺 v7.6 COIN EXPANSION HEALTH ENGINE
# =========================
# Simulation-backed 2026-06-05:
# 2 dead expansion trades out of the last 3 gave the strongest improvement.
# This does NOT delete a coin permanently. It routes the next qualifying live entry
# to shadow until the coin proves expansion has returned.
ENABLE_COIN_HEALTH_ENGINE = env_bool("ENABLE_COIN_HEALTH_ENGINE", True)
COIN_HEALTH_SHADOW_ENTRY_QUALITY = os.environ.get("COIN_HEALTH_SHADOW_ENTRY_QUALITY", "COIN_HEALTH_SHADOW")
COIN_HEALTH_DEAD_WINDOW = int(os.environ.get("COIN_HEALTH_DEAD_WINDOW", "3") or 3)
COIN_HEALTH_DEAD_THRESHOLD = int(os.environ.get("COIN_HEALTH_DEAD_THRESHOLD", "2") or 2)
COIN_HEALTH_DEAD_PEAK_PCT = float(os.environ.get("COIN_HEALTH_DEAD_PEAK_PCT", "0.25") or 0.25)
COIN_HEALTH_RECOVERY_WINDOW = int(os.environ.get("COIN_HEALTH_RECOVERY_WINDOW", "2") or 2)
COIN_HEALTH_RECOVERY_THRESHOLD = int(os.environ.get("COIN_HEALTH_RECOVERY_THRESHOLD", "2") or 2)
COIN_HEALTH_RECOVERY_PEAK_PCT = float(os.environ.get("COIN_HEALTH_RECOVERY_PEAK_PCT", "1.00") or 1.00)
COIN_HEALTH_LOOKBACK_HOURS = int(os.environ.get("COIN_HEALTH_LOOKBACK_HOURS", "168") or 168)
COIN_HEALTH_SHADOW_SIZE_GBP = float(os.environ.get("COIN_HEALTH_SHADOW_SIZE_GBP", "20") or 20)
COIN_HEALTH_SHADOW_HOLD_MINUTES = float(os.environ.get("COIN_HEALTH_SHADOW_HOLD_MINUTES", "120") or 120)
COIN_HEALTH_SHADOW_HARD_STOP = float(os.environ.get("COIN_HEALTH_SHADOW_HARD_STOP", "-0.60") or -0.60)
ENABLE_COIN_HEALTH_TELEGRAM = env_bool("ENABLE_COIN_HEALTH_TELEGRAM", True)


# =========================
# 🧬 v8.4 COIN HEALTH + PERSONALITY LEARNING ENGINE
# =========================
ENABLE_COIN_PERSONALITY_LEARNING = env_bool("ENABLE_COIN_PERSONALITY_LEARNING", True)
ENABLE_COIN_TIER_GATE = env_bool("ENABLE_COIN_TIER_GATE", True)
ENABLE_COIN_TIER_TELEGRAM = env_bool("ENABLE_COIN_TIER_TELEGRAM", True)
ENABLE_COIN_SCORE_UPSERT_ON_SIGNAL = env_bool("ENABLE_COIN_SCORE_UPSERT_ON_SIGNAL", True)
# v8.4.1: any new TradingView/OKX symbol not listed below is auto-registered
# into the discovery pool as SHADOW mode, so extra alert slots can collect data safely.
ENABLE_UNKNOWN_SYMBOL_AUTO_DISCOVERY = env_bool("ENABLE_UNKNOWN_SYMBOL_AUTO_DISCOVERY", True)
UNKNOWN_SYMBOL_DEFAULT_TIER = os.environ.get("UNKNOWN_SYMBOL_DEFAULT_TIER", "DISCOVERY")
UNKNOWN_SYMBOL_DEFAULT_MODE = os.environ.get("UNKNOWN_SYMBOL_DEFAULT_MODE", "SHADOW")
UNKNOWN_SYMBOL_DEFAULT_PERSONALITY = os.environ.get("UNKNOWN_SYMBOL_DEFAULT_PERSONALITY", "UNKNOWN")
COIN_PROMOTION_MIN_PROBES = int(os.environ.get("COIN_PROMOTION_MIN_PROBES", "20") or 20)
COIN_PROMOTION_MIN_UPGRADES = int(os.environ.get("COIN_PROMOTION_MIN_UPGRADES", "5") or 5)
COIN_PROMOTION_MIN_UPGRADE_AVG = float(os.environ.get("COIN_PROMOTION_MIN_UPGRADE_AVG", "0.50") or 0.50)
COIN_PROMOTION_MIN_IMPROVEMENT = float(os.environ.get("COIN_PROMOTION_MIN_IMPROVEMENT", "0.50") or 0.50)
COIN_DEMOTION_FAILED_UPGRADES = int(os.environ.get("COIN_DEMOTION_FAILED_UPGRADES", "3") or 3)
COIN_DEMOTION_RECENT_UPGRADE_AVG = float(os.environ.get("COIN_DEMOTION_RECENT_UPGRADE_AVG", "-0.20") or -0.20)
COIN_RECENT_WINDOW = int(os.environ.get("COIN_RECENT_WINDOW", "10") or 10)

COIN_TIER_A = {"HBARUSDT", "INJUSDT", "OPUSDT", "ARUSDT", "TIAUSDT", "FETUSDT", "NEARUSDT"}
COIN_TIER_B = {"ETCUSDT", "ARBUSDT", "SUIUSDT"}
COIN_TIER_C = {"ONDOUSDT", "WLDUSDT", "RENDERUSDT", "JUPUSDT", "IMXUSDT", "APTUSDT", "LPTUSDT", "KSMUSDT", "BNBUSDT", "SOLUSDT", "LINKUSDT"}
COIN_TIER_D = {"ATOMUSDT", "ASTRUSDT"}
COIN_RETIRED = {"PHAUSDT", "ADAUSDT"}

INITIAL_COIN_PERSONALITY = {
    "HBARUSDT": "MARKET_SUPPORT",
    "INJUSDT": "TREND_PERSISTENCE",
    "OPUSDT": "MOMENTUM_LEADERSHIP",
    "ARUSDT": "MOMENTUM_TREND",
    "TIAUSDT": "MOMENTUM_LEADERSHIP",
    "FETUSDT": "CONFIRMATION_CLEANER",
    "NEARUSDT": "RUNNER_UPGRADE",
    "ETCUSDT": "SECONDARY_UPGRADE",
    "ARBUSDT": "SECONDARY_UPGRADE",
    "SUIUSDT": "SECONDARY_UPGRADE",
    "ONDOUSDT": "UNKNOWN",
    "WLDUSDT": "UNKNOWN",
    "RENDERUSDT": "UNKNOWN",
    "JUPUSDT": "UNKNOWN",
    "IMXUSDT": "LEARNING_POOL",
    "APTUSDT": "LEARNING_POOL",
    "LPTUSDT": "LEARNING_POOL",
    "KSMUSDT": "LEARNING_POOL",
    "BNBUSDT": "OBSERVE",
    "SOLUSDT": "OBSERVE",
    "LINKUSDT": "OBSERVE",
    "ATOMUSDT": "DISABLED_WEAK_UPGRADE",
    "ASTRUSDT": "DISABLED_UNKNOWN_WEAK",
    "PHAUSDT": "RETIRED_FAILED_ALL_TESTS",
    "ADAUSDT": "RETIRED_LOW_OPPORTUNITY",
}

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

# =========================
# 🧠 ADAPTIVE LEADERSHIP TREND ENGINE (v6.6.20)
# =========================
ENABLE_ADAPTIVE_LEADERSHIP_TREND = os.environ.get(
    "ENABLE_ADAPTIVE_LEADERSHIP_TREND",
    "true"
).lower() == "true"

ADAPTIVE_LEADER_MIN_TREND = float(os.environ.get("ADAPTIVE_LEADER_MIN_TREND", "0.16") or 0.16)
ADAPTIVE_CQE_MIN_TREND = float(os.environ.get("ADAPTIVE_CQE_MIN_TREND", "0.17") or 0.17)
ADAPTIVE_STABLE_LEADER_MIN_TREND = float(os.environ.get("ADAPTIVE_STABLE_LEADER_MIN_TREND", "0.18") or 0.18)

ADAPTIVE_LEADERSHIP_SCORE_THRESHOLD = float(os.environ.get("ADAPTIVE_LEADERSHIP_SCORE_THRESHOLD", "2.5") or 2.5)
ADAPTIVE_CQE_SCORE_THRESHOLD = float(os.environ.get("ADAPTIVE_CQE_SCORE_THRESHOLD", "4") or 4)

# =========================
# 🦄 FREE THE UNICORNS ENTRY OVERRIDE (v6.6.21)
# =========================
ENABLE_UNICORN_ENTRY_OVERRIDE = os.environ.get(
    "ENABLE_UNICORN_ENTRY_OVERRIDE",
    "true"
).lower() == "true"

UNICORN_MIN_LEADERSHIP_SCORE = float(os.environ.get("UNICORN_MIN_LEADERSHIP_SCORE", "2.0") or 2.0)
UNICORN_MAX_LEADERSHIP_SCORE = float(os.environ.get("UNICORN_MAX_LEADERSHIP_SCORE", "3.0") or 3.0)
UNICORN_MIN_CQE_SCORE = float(os.environ.get("UNICORN_MIN_CQE_SCORE", "4.0") or 4.0)
UNICORN_MIN_PRESSURE_SCORE = float(os.environ.get("UNICORN_MIN_PRESSURE_SCORE", "7.0") or 7.0)

# Allows one extra same-symbol continuation slot only for validated Unicorn Candidates.
# Normal MAX_SAME_SYMBOL_OPEN remains unchanged for all ordinary trades.
UNICORN_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("UNICORN_MAX_SAME_SYMBOL_OPEN", "3") or 3)

UNICORN_OVERRIDE_REASONS = {
    "leadership_climax_delta_blocked",
    "max_same_symbol_open",
}


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

LIVE_CQE_TRADE_SIZE_GBP = float(os.environ.get("LIVE_CQE_TRADE_SIZE_GBP", "20") or 20)
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
    "ENABLE_BPT_CQE_LIVE_PROBES", "true"
).lower() == "true"
ENABLE_BPT_CQE_LIVE_UPGRADES = os.environ.get(
    "ENABLE_BPT_CQE_LIVE_UPGRADES", "true"
).lower() == "true"

# v7.7 capital-protection architecture:
# Probes remain OPEN DB lifecycle rows and still drive confirmation/coin-health telemetry,
# but they do NOT place real OKX capital. They are labelled as GHOST_PROBE.
# Real capital is only added if/when the lifecycle confirms and a live upgrade scale-in succeeds.
ENABLE_BPT_CQE_GHOST_PROBES = os.environ.get(
    "ENABLE_BPT_CQE_GHOST_PROBES", "true"
).lower() == "true"
GHOST_PROBE_LABEL = "GHOST_PROBE_NO_CAPITAL"

# v6.6.19/v6.6.20: real capital only added AFTER CQE confirmation.
ENABLE_CQE_REAL_SCALEINS = os.environ.get(
    "ENABLE_CQE_REAL_SCALEINS",
    "true"
).lower() == "true"

CQE_REAL_SCALEIN_ALLOWED_ROWS = [
    "HIGH_MONSTER_ROW",
    "EXTREME_RUNNER_ROW",
    "MEDIUM_BALANCED_ROW",
    "EARLY_INCUBATION_ROW",
]

CQE_SCALEIN_MIN_LIVE_PRESSURE = float(os.environ.get("CQE_SCALEIN_MIN_LIVE_PRESSURE", "0.35") or 0.35)
CQE_SCALEIN_MIN_DELTA_30M = float(os.environ.get("CQE_SCALEIN_MIN_DELTA_30M", "0.00") or 0.00)

# Minimum order protection for tiny OKX scale-ins.
MIN_OKX_ORDER_NOTIONAL_GBP = float(os.environ.get("MIN_OKX_ORDER_NOTIONAL_GBP", "15.0") or 15.0)

# v6.9.2 capital utilisation: use available quote balance instead of skipping near-full-size entries.
ALLOW_PARTIAL_POSITION_FILL = os.environ.get("ALLOW_PARTIAL_POSITION_FILL", "true").lower() == "true"
MIN_POSITION_SIZE_GBP = float(os.environ.get("MIN_POSITION_SIZE_GBP", "10") or 10)


BPT_CQE_ENTRY_QUALITY = "BPT_CQE_LIFECYCLE_V1"
BPT_CQE_PROBE_SIZE_GBP = float(os.environ.get("BPT_CQE_PROBE_SIZE_GBP", "10") or 10)
BPT_CQE_MAX_OPEN_TRADES = int(os.environ.get("BPT_CQE_MAX_OPEN_TRADES", "5") or 5)
BPT_CQE_MAX_SAME_SYMBOL_OPEN = int(os.environ.get("BPT_CQE_MAX_SAME_SYMBOL_OPEN", "1") or 1)

# Probe gate from CQE/quiet-continuation findings.
BPT_CQE_MIN_TREND = float(os.environ.get("BPT_CQE_MIN_TREND", "0.25") or 0.25)
BPT_CQE_MIN_MOMENTUM = float(os.environ.get("BPT_CQE_MIN_MOMENTUM", "0.50") or 0.50)
BPT_CQE_MAX_LEADERSHIP_SCORE = float(os.environ.get("BPT_CQE_MAX_LEADERSHIP_SCORE", "0.50") or 0.50)
BPT_CQE_MAX_LEADERSHIP_DELTA_30M = float(os.environ.get("BPT_CQE_MAX_LEADERSHIP_DELTA_30M", "1.00") or 1.00)

# =========================
# 🦄 v7.0 ADAPTIVE DEAD-MARKET LEADERSHIP
# =========================
# Research-backed live probe pathway from 2026-06-03 analysis:
# In dead/compressed markets, top-ranked low-score leaders outperformed
# higher-score leaders. This is deliberately implemented as a BPT probe
# pathway, not a rewrite of stable/emerging leadership phase logic.
ENABLE_ADAPTIVE_DEAD_MARKET_LEADERS = os.environ.get(
    "ENABLE_ADAPTIVE_DEAD_MARKET_LEADERS",
    "true"
).lower() == "true"

# v8.1 safety: keep the Adaptive Dead Market Leader pathway for telemetry,
# but do not allow it to deploy live upgrade capital unless explicitly re-enabled.
ENABLE_ADAPTIVE_DEAD_MARKET_LIVE_UPGRADES = os.environ.get(
    "ENABLE_ADAPTIVE_DEAD_MARKET_LIVE_UPGRADES",
    "false"
).lower() == "true"


ADAPTIVE_DEAD_MARKET_CONTEXT_MAX = float(os.environ.get("ADAPTIVE_DEAD_MARKET_CONTEXT_MAX", "2.0") or 2.0)
ADAPTIVE_DEAD_MARKET_MAX_RANK = int(os.environ.get("ADAPTIVE_DEAD_MARKET_MAX_RANK", "3") or 3)
ADAPTIVE_DEAD_MARKET_MAX_SCORE = float(os.environ.get("ADAPTIVE_DEAD_MARKET_MAX_SCORE", "0.5") or 0.5)

# Keep these deliberately permissive by default because the validated SQL cohort
# was defined by decision=LONG + context/rank/score, not by the old BPT trend gate.
# These toggles are here so we can tighten quickly from Render env if needed.
ADAPTIVE_DEAD_MARKET_MIN_MOMENTUM = float(os.environ.get("ADAPTIVE_DEAD_MARKET_MIN_MOMENTUM", "0.0") or 0.0)
ADAPTIVE_DEAD_MARKET_MIN_TREND = float(os.environ.get("ADAPTIVE_DEAD_MARKET_MIN_TREND", "0.0") or 0.0)
ADAPTIVE_DEAD_MARKET_ENTRY_QUALITY = os.environ.get(
    "ADAPTIVE_DEAD_MARKET_ENTRY_QUALITY",
    "BPT_CQE_LIFECYCLE_V1"
)

# Upgrade confirmation cluster from sims:
# 30m peak >= 1.0%, positive trend/momentum persistence.
BPT_CQE_CONFIRM_WINDOW_MINUTES = float(os.environ.get("BPT_CQE_CONFIRM_WINDOW_MINUTES", "30") or 30)
BPT_CQE_CONFIRM_PEAK = float(os.environ.get("BPT_CQE_CONFIRM_PEAK", "1.0") or 1.0)
BPT_CQE_CONFIRM_AVG_TREND = float(os.environ.get("BPT_CQE_CONFIRM_AVG_TREND", "0.05") or 0.05)
BPT_CQE_CONFIRM_AVG_MOMENTUM = float(os.environ.get("BPT_CQE_CONFIRM_AVG_MOMENTUM", "0.05") or 0.05)
BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT = int(os.environ.get("BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT", "0") or 0)
# =========================
# 🧬 v7.2 ADAPTIVE PROBE LIFECYCLE ENGINE
# =========================
# Evidence-backed from 2026-06-03 live V7.1 probe analysis.
# Applies ONLY to live BPT adaptive dead-market probes before they have upgraded.
ENABLE_BPT_PROBE_LIFECYCLE_ENGINE = os.environ.get(
    "ENABLE_BPT_PROBE_LIFECYCLE_ENGINE",
    "true"
).lower() == "true"

# 5m monster fast-track:
# Very rare/high-quality cohort in live data.
BPT_MONSTER_FASTTRACK_MIN_AGE_MINUTES = float(os.environ.get("BPT_MONSTER_FASTTRACK_MIN_AGE_MINUTES", "5") or 5)
BPT_MONSTER_FASTTRACK_MIN_LEADERSHIP_DELTA = float(os.environ.get("BPT_MONSTER_FASTTRACK_MIN_LEADERSHIP_DELTA", "0.45") or 0.45)
BPT_MONSTER_FASTTRACK_MIN_TREND = float(os.environ.get("BPT_MONSTER_FASTTRACK_MIN_TREND", "0.25") or 0.25)

# 10m dead-probe protection:
# momentum < 0 and trend < 0.10 showed 0% win rate in initial live sample.
BPT_DEAD_PROBE_MIN_AGE_MINUTES = float(os.environ.get("BPT_DEAD_PROBE_MIN_AGE_MINUTES", "10") or 10)
BPT_DEAD_PROBE_MAX_MOMENTUM = float(os.environ.get("BPT_DEAD_PROBE_MAX_MOMENTUM", "0.0") or 0.0)
BPT_DEAD_PROBE_MAX_TREND = float(os.environ.get("BPT_DEAD_PROBE_MAX_TREND", "0.10") or 0.10)

# 10m leader fast-track:
# broad early-upgrade cohort with high win rate in V7.1 live sample.
BPT_LEADER_FASTTRACK_MIN_AGE_MINUTES = float(os.environ.get("BPT_LEADER_FASTTRACK_MIN_AGE_MINUTES", "10") or 10)
BPT_LEADER_FASTTRACK_MIN_LEADERSHIP_DELTA = float(os.environ.get("BPT_LEADER_FASTTRACK_MIN_LEADERSHIP_DELTA", "0.10") or 0.10)
BPT_LEADER_FASTTRACK_MIN_TREND = float(os.environ.get("BPT_LEADER_FASTTRACK_MIN_TREND", "0.15") or 0.15)

# Row thresholds: quality_score = trend + momentum.
BPT_EXTREME_QUALITY_SCORE = float(os.environ.get("BPT_EXTREME_QUALITY_SCORE", "2.0") or 2.0)
BPT_HIGH_QUALITY_SCORE = float(os.environ.get("BPT_HIGH_QUALITY_SCORE", "1.2") or 1.2)
BPT_MEDIUM_QUALITY_SCORE = float(os.environ.get("BPT_MEDIUM_QUALITY_SCORE", "0.6") or 0.6)

# Row-specific upgrade sizes and exits.
BPT_EXTREME_UPGRADE_GBP = float(os.environ.get("BPT_EXTREME_UPGRADE_GBP", "25") or 25)
BPT_EXTREME_TRAIL_ACTIVATION = float(os.environ.get("BPT_EXTREME_TRAIL_ACTIVATION", "2.0") or 2.0)
BPT_EXTREME_TRAIL_DRAWDOWN = float(os.environ.get("BPT_EXTREME_TRAIL_DRAWDOWN", "0.75") or 0.75)

BPT_HIGH_UPGRADE_GBP = float(os.environ.get("BPT_HIGH_UPGRADE_GBP", "35") or 35)
BPT_HIGH_TRAIL_ACTIVATION = float(os.environ.get("BPT_HIGH_TRAIL_ACTIVATION", "3.0") or 3.0)
BPT_HIGH_TRAIL_DRAWDOWN = float(os.environ.get("BPT_HIGH_TRAIL_DRAWDOWN", "1.0") or 1.0)

BPT_MEDIUM_UPGRADE_GBP = float(os.environ.get("BPT_MEDIUM_UPGRADE_GBP", "45") or 45)
BPT_MEDIUM_TRAIL_ACTIVATION = float(os.environ.get("BPT_MEDIUM_TRAIL_ACTIVATION", "5.0") or 5.0)
BPT_MEDIUM_TRAIL_DRAWDOWN = float(os.environ.get("BPT_MEDIUM_TRAIL_DRAWDOWN", "1.5") or 1.5)

BPT_EARLY_UPGRADE_GBP = float(os.environ.get("BPT_EARLY_UPGRADE_GBP", "15") or 15)
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

ENABLE_ORDER_LOGGING = env_bool("ENABLE_ORDER_LOGGING", True)

# =========================
# 🧭 CONTROL PANEL ARCHITECTURE v6.6.15
# =========================
ENABLE_LIVE_TRADING = env_bool("ENABLE_LIVE_TRADING", env_bool("LIVE_ORDERS_ENABLED", True))
ENABLE_NEW_ENTRIES = env_bool("ENABLE_NEW_ENTRIES", True)
ENABLE_OKX_EXITS = env_bool("ENABLE_OKX_EXITS", True)
ENABLE_SHADOWS = env_bool("ENABLE_SHADOWS", True)
ENABLE_PROBES = env_bool("ENABLE_PROBES", True)
ENABLE_LONGS = env_bool("ENABLE_LONGS", True)
ENABLE_SHORTS = env_bool("ENABLE_SHORTS", False)
ENABLE_PARTIAL_BANKING = env_bool("ENABLE_PARTIAL_BANKING", True)
EMERGENCY_CLOSE_ONLY_MODE = env_bool("EMERGENCY_CLOSE_ONLY_MODE", False)
ENABLE_LIVE_ORDERS = env_bool("ENABLE_LIVE_ORDERS", ENABLE_LIVE_TRADING)

# v6.6.12: exchange truth safety guards. These prevent duplicate live buys
# if DB open-trade reconstruction/persistence is out of sync with OKX.
ENABLE_OKX_POSITION_ENTRY_GUARD = os.environ.get("ENABLE_OKX_POSITION_ENTRY_GUARD", "true").lower() == "true"
OKX_POSITION_DUST_USD = float(os.environ.get("OKX_POSITION_DUST_USD", "1.0") or 1.0)

MAX_LIVE_OPEN_TRADES = int(os.environ.get("MAX_LIVE_OPEN_TRADES", "5") or 5)

OKX_TD_MODE = os.environ.get("OKX_TD_MODE", "cash")
OKX_ORDER_TYPE = os.environ.get("OKX_ORDER_TYPE", "market")
OKX_EXIT_SIZE_BUFFER = float(os.environ.get("OKX_EXIT_SIZE_BUFFER", "0.995") or 0.995)
REGIME_ALERT_COOLDOWN_MINUTES = 30

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
# 🌎 MARKET LIFECYCLE ENGINE v6.9
# =========================
# Data/logging only. No entry, exit, size, or stacking behaviour changes.
ENABLE_MARKET_LIFECYCLE_ENGINE_V69 = os.environ.get("ENABLE_MARKET_LIFECYCLE_ENGINE_V69", "true").lower() == "true"
ENABLE_MARKET_LIFECYCLE_TELEGRAM_V69 = os.environ.get("ENABLE_MARKET_LIFECYCLE_TELEGRAM_V69", "true").lower() == "true"

# V1 core zones discovered 2026-05-31. Treat as observed-state labels, not trade gates.
LIFECYCLE_DEAD_BREADTH_MAX = float(os.environ.get("LIFECYCLE_DEAD_BREADTH_MAX", "3") or 3)
LIFECYCLE_DEAD_MEDIAN_MAX = float(os.environ.get("LIFECYCLE_DEAD_MEDIAN_MAX", "1") or 1)
LIFECYCLE_DEVELOPING_MEDIAN_MIN = float(os.environ.get("LIFECYCLE_DEVELOPING_MEDIAN_MIN", "4") or 4)
LIFECYCLE_PRODUCTIVE_BREADTH_MIN = float(os.environ.get("LIFECYCLE_PRODUCTIVE_BREADTH_MIN", "3") or 3)
LIFECYCLE_PRODUCTIVE_BREADTH_MAX = float(os.environ.get("LIFECYCLE_PRODUCTIVE_BREADTH_MAX", "12") or 12)
LIFECYCLE_CROWDED_BREADTH_MIN = float(os.environ.get("LIFECYCLE_CROWDED_BREADTH_MIN", "10") or 10)


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
LONG_LOCK_1_RATIO = float(os.environ.get("LONG_LOCK_1_RATIO", "0.60") or 0.60)

LONG_LOCK_2_TRIGGER = float(os.environ.get("LONG_LOCK_2_TRIGGER", "1.25") or 1.25)
LONG_LOCK_2_RATIO = float(os.environ.get("LONG_LOCK_2_RATIO", "0.70") or 0.70)

LONG_LOCK_3_TRIGGER = float(os.environ.get("LONG_LOCK_3_TRIGGER", "3.00") or 3.00)
LONG_LOCK_3_RATIO = float(os.environ.get("LONG_LOCK_3_RATIO", "0.75") or 0.75)

LONG_LOCK_4_TRIGGER = float(os.environ.get("LONG_LOCK_4_TRIGGER", "5.00") or 5.00)
LONG_LOCK_4_RATIO = float(os.environ.get("LONG_LOCK_4_RATIO", "0.80") or 0.80)

LONG_HARD_STOP = float(os.environ.get("LONG_HARD_STOP", "-0.40") or -0.40)
LONG_NO_RED_AFTER_WIN_TRIGGER = float(os.environ.get("LONG_NO_RED_AFTER_WIN_TRIGGER", "0.75") or 0.75)

# v8.2: upgraded-trade giveback guard.
# Prevents trades like TIA (+2.08% peak) exiting near flat after trend fails.
BPT_UPGRADED_GIVEBACK_GUARD_ENABLED = os.environ.get(
    "BPT_UPGRADED_GIVEBACK_GUARD_ENABLED", "true"
).lower() == "true"
BPT_UPGRADED_GIVEBACK_GUARD_PEAK = float(os.environ.get("BPT_UPGRADED_GIVEBACK_GUARD_PEAK", "1.25") or 1.25)
BPT_UPGRADED_GIVEBACK_GUARD_MIN_KEEP = float(os.environ.get("BPT_UPGRADED_GIVEBACK_GUARD_MIN_KEEP", "0.35") or 0.35)


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


# =========================
# 🌡️ MARKET HEAT TELEMETRY v8.3
# =========================
# Backfill discovery:
# hourly average market trend is a strong regime detector.
# DEAD < 0.00, NEUTRAL 0.00-0.05, HEALTHY 0.05-0.15, HOT >= 0.15.
# v8.3 stores this as telemetry only; it does NOT block or resize trades.

MARKET_HEAT_LOOKBACK_MINUTES = int(os.environ.get("MARKET_HEAT_LOOKBACK_MINUTES", "60") or 60)
MARKET_HEAT_HOT_TREND = float(os.environ.get("MARKET_HEAT_HOT_TREND", "0.15") or 0.15)
MARKET_HEAT_HEALTHY_TREND = float(os.environ.get("MARKET_HEAT_HEALTHY_TREND", "0.05") or 0.05)
MARKET_HEAT_NEUTRAL_TREND = float(os.environ.get("MARKET_HEAT_NEUTRAL_TREND", "0.00") or 0.00)

def classify_market_heat(avg_trend):
    try:
        t = float(avg_trend or 0)
    except Exception:
        t = 0.0

    if t >= MARKET_HEAT_HOT_TREND:
        return "HOT", 3
    if t >= MARKET_HEAT_HEALTHY_TREND:
        return "HEALTHY", 2
    if t >= MARKET_HEAT_NEUTRAL_TREND:
        return "NEUTRAL", 1
    return "DEAD", 0

def ensure_market_heat_columns(cur):
    try:
        cur.execute("""
            ALTER TABLE bot_trades_v4
            ADD COLUMN IF NOT EXISTS market_heat_regime TEXT,
            ADD COLUMN IF NOT EXISTS market_heat_score INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_avg_trend NUMERIC,
            ADD COLUMN IF NOT EXISTS market_heat_avg_momentum NUMERIC,
            ADD COLUMN IF NOT EXISTS market_heat_signal_count INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_lookback_minutes INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_calculated_at TIMESTAMPTZ
        """)
        cur.execute("""
            ALTER TABLE signals_raw
            ADD COLUMN IF NOT EXISTS market_heat_regime TEXT,
            ADD COLUMN IF NOT EXISTS market_heat_score INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_avg_trend NUMERIC,
            ADD COLUMN IF NOT EXISTS market_heat_avg_momentum NUMERIC,
            ADD COLUMN IF NOT EXISTS market_heat_signal_count INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_lookback_minutes INTEGER,
            ADD COLUMN IF NOT EXISTS market_heat_calculated_at TIMESTAMPTZ
        """)
    except Exception as e:
        print(f"⚠️ market heat column ensure failed: {e}", flush=True)
        safe_telemetry_rollback(cur)

def get_market_heat_context(cur, lookback_minutes=None):
    lookback = int(lookback_minutes or MARKET_HEAT_LOOKBACK_MINUTES)
    try:
        cur.execute("""
            SELECT
                COALESCE(AVG(momentum), 0),
                COALESCE(AVG(trend), 0),
                COUNT(*)
            FROM signals_raw
            WHERE timestamp >= NOW() - (%s || ' minutes')::INTERVAL
        """, (lookback,))
        avg_momentum, avg_trend, signal_count = cur.fetchone() or (0, 0, 0)
        regime, score = classify_market_heat(avg_trend)
        return {
            "market_heat_regime": regime,
            "market_heat_score": score,
            "market_heat_avg_trend": float(avg_trend or 0),
            "market_heat_avg_momentum": float(avg_momentum or 0),
            "market_heat_signal_count": int(signal_count or 0),
            "market_heat_lookback_minutes": lookback,
            "market_heat_calculated_at": datetime.now(timezone.utc),
        }
    except Exception as e:
        print(f"⚠️ market heat context failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return {
            "market_heat_regime": "UNKNOWN",
            "market_heat_score": None,
            "market_heat_avg_trend": None,
            "market_heat_avg_momentum": None,
            "market_heat_signal_count": None,
            "market_heat_lookback_minutes": lookback,
            "market_heat_calculated_at": datetime.now(timezone.utc),
        }

def apply_market_heat_to_trade(cur, trade_id):
    try:
        ensure_market_heat_columns(cur)
        safe_update_trade_telemetry(cur, trade_id, get_market_heat_context(cur))
    except Exception as e:
        print(f"⚠️ market heat trade update failed | id={trade_id} | {e}", flush=True)
        safe_telemetry_rollback(cur)

def apply_market_heat_to_signal(cur, signal_id):
    try:
        ensure_market_heat_columns(cur)
        safe_update_signal_telemetry(cur, signal_id, get_market_heat_context(cur))
    except Exception as e:
        print(f"⚠️ market heat signal update failed | id={signal_id} | {e}", flush=True)
        safe_telemetry_rollback(cur)


# =========================
# 🧬 COIN SCORE / PERSONALITY HELPERS v8.4
# =========================
def normalize_symbol(symbol):
    return (symbol or "").strip().upper()


def get_initial_coin_profile(symbol):
    sym = normalize_symbol(symbol)
    if sym in COIN_RETIRED:
        return {"tier": "RETIRED", "mode": "DISABLED", "personality": INITIAL_COIN_PERSONALITY.get(sym, "RETIRED"), "is_unknown": False}
    if sym in COIN_TIER_D:
        return {"tier": "D", "mode": "DISABLED", "personality": INITIAL_COIN_PERSONALITY.get(sym, "DISABLED"), "is_unknown": False}
    if sym in COIN_TIER_A:
        return {"tier": "A", "mode": "LIVE", "personality": INITIAL_COIN_PERSONALITY.get(sym, "UNKNOWN"), "is_unknown": False}
    if sym in COIN_TIER_B:
        return {"tier": "B", "mode": "LIVE", "personality": INITIAL_COIN_PERSONALITY.get(sym, "UNKNOWN"), "is_unknown": False}
    if sym in COIN_TIER_C:
        return {"tier": "C", "mode": "SHADOW", "personality": INITIAL_COIN_PERSONALITY.get(sym, "UNKNOWN"), "is_unknown": False}

    # v8.4.1: true auto-discovery. Any new alert symbol not configured above
    # starts safely in SHADOW/DISCOVERY and can only earn live permission via promotion.
    if ENABLE_UNKNOWN_SYMBOL_AUTO_DISCOVERY:
        return {
            "tier": UNKNOWN_SYMBOL_DEFAULT_TIER,
            "mode": UNKNOWN_SYMBOL_DEFAULT_MODE.upper(),
            "personality": INITIAL_COIN_PERSONALITY.get(sym, UNKNOWN_SYMBOL_DEFAULT_PERSONALITY),
            "is_unknown": True,
        }

    return {"tier": "C", "mode": "SHADOW", "personality": INITIAL_COIN_PERSONALITY.get(sym, "UNKNOWN"), "is_unknown": True}


def ensure_coin_scores_v84_columns(cur):
    try:
        cur.execute("""
            ALTER TABLE coin_scores
            ADD COLUMN IF NOT EXISTS coin_health_mode TEXT DEFAULT 'SHADOW',
            ADD COLUMN IF NOT EXISTS personality TEXT,
            ADD COLUMN IF NOT EXISTS best_metric TEXT,
            ADD COLUMN IF NOT EXISTS momentum_lift NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS trend_lift NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS leadership_lift NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS heat_lift NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS recent_probe_avg NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS recent_upgrade_avg NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS promotion_score NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS demotion_score NUMERIC DEFAULT 0,
            ADD COLUMN IF NOT EXISTS sample_size INTEGER DEFAULT 0
        """)
    except Exception as e:
        print(f"⚠️ coin_scores v8.4 column ensure failed: {e}", flush=True)
        safe_telemetry_rollback(cur)


def upsert_initial_coin_score(cur, symbol):
    sym = normalize_symbol(symbol)
    profile = get_initial_coin_profile(sym)
    try:
        ensure_coin_scores_v84_columns(cur)
        cur.execute("""
            INSERT INTO coin_scores (
                symbol, total_upgrades, winning_upgrades, losing_upgrades,
                avg_peak, avg_final, total_pnl_gbp, consecutive_failed_upgrades,
                hero_points, tier, last_updated, coin_health_mode, personality,
                best_metric, sample_size
            )
            VALUES (%s,0,0,0,0,0,0,0,0,%s,NOW(),%s,%s,'UNKNOWN',0)
            ON CONFLICT (symbol)
            DO UPDATE SET
                tier = COALESCE(NULLIF(coin_scores.tier,''), EXCLUDED.tier),
                coin_health_mode = COALESCE(NULLIF(coin_scores.coin_health_mode,''), EXCLUDED.coin_health_mode),
                personality = COALESCE(NULLIF(coin_scores.personality,''), EXCLUDED.personality),
                last_updated = NOW()
        """, (sym, profile["tier"], profile["mode"], profile["personality"]))
        if sym in COIN_RETIRED or sym in COIN_TIER_D:
            cur.execute("""
                UPDATE coin_scores
                SET coin_health_mode='DISABLED', tier=%s,
                    personality=COALESCE(NULLIF(personality,''), %s), last_updated=NOW()
                WHERE symbol=%s
            """, (profile["tier"], profile["personality"], sym))
    except Exception as e:
        print(f"⚠️ initial coin score upsert failed for {sym}: {e}", flush=True)
        safe_telemetry_rollback(cur)


def get_coin_score_profile(cur, symbol):
    sym = normalize_symbol(symbol)
    initial = get_initial_coin_profile(sym)
    if not ENABLE_COIN_TIER_GATE:
        return {"symbol": sym, "tier": initial["tier"], "coin_health_mode": "LIVE", "personality": initial["personality"], "source": "tier_gate_disabled"}
    try:
        upsert_initial_coin_score(cur, sym)
        cur.execute("""
            SELECT tier, coin_health_mode, personality, best_metric,
                   promotion_score, demotion_score, total_upgrades, avg_final,
                   consecutive_failed_upgrades
            FROM coin_scores WHERE symbol=%s
        """, (sym,))
        row = cur.fetchone()
        if not row:
            return {"symbol": sym, "tier": initial["tier"], "coin_health_mode": initial["mode"], "personality": initial["personality"], "source": "initial"}
        tier, mode, personality, best_metric, promotion_score, demotion_score, total_upgrades, avg_final, failed = row
        mode = (mode or initial["mode"] or "SHADOW").upper()
        tier = tier or initial["tier"]
        personality = personality or initial["personality"]
        if sym in COIN_RETIRED or sym in COIN_TIER_D:
            mode = "DISABLED"
        return {
            "symbol": sym, "tier": tier, "coin_health_mode": mode,
            "personality": personality, "best_metric": best_metric,
            "promotion_score": float(promotion_score or 0),
            "demotion_score": float(demotion_score or 0),
            "total_upgrades": int(total_upgrades or 0),
            "avg_final": float(avg_final or 0),
            "consecutive_failed_upgrades": int(failed or 0),
            "source": "coin_scores",
        }
    except Exception as e:
        print(f"⚠️ coin score profile lookup failed for {sym}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return {"symbol": sym, "tier": initial["tier"], "coin_health_mode": initial["mode"], "personality": initial["personality"], "source": "fallback_error"}


def classify_personality_from_lifts(momentum_lift, trend_lift, leadership_lift, heat_lift, current_personality="UNKNOWN"):
    lifts = {
        "MOMENTUM": float(momentum_lift or 0),
        "TREND": float(trend_lift or 0),
        "LEADERSHIP": float(leadership_lift or 0),
        "MARKET_HEAT": float(heat_lift or 0),
    }
    best_metric, best_value = max(lifts.items(), key=lambda kv: kv[1])
    if best_value < 0.05:
        return (current_personality or "UNKNOWN"), "NONE"
    if sum(1 for v in lifts.values() if v >= 0.10) >= 2:
        return "HYBRID", best_metric
    return best_metric, best_metric


def maybe_send_coin_mode_alert(symbol, old_mode, new_mode, old_tier, new_tier, reason, stats=None):
    if not ENABLE_COIN_TIER_TELEGRAM:
        return
    old_mode = old_mode or "UNKNOWN"
    new_mode = new_mode or "UNKNOWN"
    if old_mode == new_mode and old_tier == new_tier:
        return
    emoji = "🟢" if new_mode == "LIVE" else "🟡" if new_mode == "SHADOW" else "🔴"
    title = "COIN PROMOTED" if new_mode == "LIVE" else "COIN DEMOTED" if new_mode == "SHADOW" else "COIN DISABLED"
    stats = stats or {}
    try:
        send_telegram_alert(
            f"{emoji} <b>{title}</b>\n"
            f"{symbol}\n"
            f"{old_tier or 'UNKNOWN'} / {old_mode} → {new_tier or old_tier or 'UNKNOWN'} / {new_mode}\n"
            f"Reason: {reason}\n"
            f"Probe avg {fmt_num(stats.get('probe_avg'),3)} | Upgrade avg {fmt_num(stats.get('upgrade_avg'),3)}\n"
            f"Probes {stats.get('probe_trades', 0)} | Upgrades {stats.get('upgrade_trades', 0)}\n"
            f"Best metric: {stats.get('best_metric') or 'UNKNOWN'} | Personality: {stats.get('personality') or 'UNKNOWN'}"
        )
    except Exception as e:
        print(f"⚠️ coin mode telegram failed for {symbol}: {e}", flush=True)


def refresh_coin_learning_from_history(cur, symbol, allow_mode_change=True):
    if not ENABLE_COIN_PERSONALITY_LEARNING:
        return None
    sym = normalize_symbol(symbol)
    try:
        ensure_coin_scores_v84_columns(cur)
        upsert_initial_coin_score(cur, sym)
        cur.execute("""
            WITH closed AS (
                SELECT * FROM bot_trades_v4
                WHERE symbol=%s AND status='CLOSED' AND pnl_percent IS NOT NULL
            ),
            agg AS (
                SELECT
                    COUNT(*) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS probe_trades,
                    AVG(pnl_percent) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS probe_avg,
                    AVG(peak_pnl_percent) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS probe_peak,
                    COUNT(*) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) AS upgrade_trades,
                    COUNT(*) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE AND pnl_percent > 0) AS winning_upgrades,
                    COUNT(*) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE AND pnl_percent <= 0) AS losing_upgrades,
                    AVG(pnl_percent) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) AS upgrade_avg,
                    AVG(peak_pnl_percent) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) AS upgrade_peak,
                    SUM(pnl_gbp) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) AS upgrade_pnl_gbp,
                    AVG(momentum_strength) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) - AVG(momentum_strength) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS momentum_lift,
                    AVG(trend_strength) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) - AVG(trend_strength) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS trend_lift,
                    AVG(leadership_delta_30m_at_entry) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) - AVG(leadership_delta_30m_at_entry) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS leadership_lift,
                    AVG(market_heat_avg_trend) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=TRUE) - AVG(market_heat_avg_trend) FILTER (WHERE COALESCE(cqe_upgraded,FALSE)=FALSE) AS heat_lift
                FROM closed
            ),
            recent_probe AS (
                SELECT AVG(pnl_percent) AS recent_probe_avg FROM (
                    SELECT pnl_percent FROM closed WHERE COALESCE(cqe_upgraded,FALSE)=FALSE ORDER BY closed_at DESC NULLS LAST LIMIT %s
                ) x
            ),
            recent_upgrade AS (
                SELECT AVG(pnl_percent) AS recent_upgrade_avg FROM (
                    SELECT pnl_percent FROM closed WHERE COALESCE(cqe_upgraded,FALSE)=TRUE ORDER BY closed_at DESC NULLS LAST LIMIT %s
                ) x
            ),
            failed_streak AS (
                SELECT COUNT(*) AS consecutive_failed_upgrades FROM (
                    SELECT pnl_percent,
                           SUM(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END) OVER (ORDER BY closed_at DESC NULLS LAST) AS wins_seen
                    FROM closed WHERE COALESCE(cqe_upgraded,FALSE)=TRUE ORDER BY closed_at DESC NULLS LAST
                ) y WHERE wins_seen = 0
            )
            SELECT
                COALESCE(a.probe_trades,0), COALESCE(a.probe_avg,0), COALESCE(a.probe_peak,0),
                COALESCE(a.upgrade_trades,0), COALESCE(a.winning_upgrades,0), COALESCE(a.losing_upgrades,0),
                COALESCE(a.upgrade_avg,0), COALESCE(a.upgrade_peak,0), COALESCE(a.upgrade_pnl_gbp,0),
                COALESCE(a.momentum_lift,0), COALESCE(a.trend_lift,0), COALESCE(a.leadership_lift,0), COALESCE(a.heat_lift,0),
                COALESCE(rp.recent_probe_avg,0), COALESCE(ru.recent_upgrade_avg,0), COALESCE(fs.consecutive_failed_upgrades,0)
            FROM agg a, recent_probe rp, recent_upgrade ru, failed_streak fs
        """, (sym, COIN_RECENT_WINDOW, COIN_RECENT_WINDOW))
        row = cur.fetchone()
        if not row:
            return None
        (probe_trades, probe_avg, probe_peak, upgrade_trades, winning_upgrades, losing_upgrades,
         upgrade_avg, upgrade_peak, upgrade_pnl_gbp, momentum_lift, trend_lift, leadership_lift,
         heat_lift, recent_probe_avg, recent_upgrade_avg, failed_streak) = row
        profile = get_coin_score_profile(cur, sym)
        old_mode = profile.get("coin_health_mode")
        old_tier = profile.get("tier")
        static = get_initial_coin_profile(sym)
        personality, best_metric = classify_personality_from_lifts(momentum_lift, trend_lift, leadership_lift, heat_lift, profile.get("personality") or static.get("personality"))
        improvement = float(upgrade_avg or 0) - float(probe_avg or 0)
        promotion_score = 0.0
        if int(upgrade_trades or 0) >= COIN_PROMOTION_MIN_UPGRADES: promotion_score += 1.0
        if int(probe_trades or 0) >= COIN_PROMOTION_MIN_PROBES: promotion_score += 1.0
        if float(upgrade_avg or 0) >= COIN_PROMOTION_MIN_UPGRADE_AVG: promotion_score += 2.0
        if improvement >= COIN_PROMOTION_MIN_IMPROVEMENT: promotion_score += 1.0
        if float(upgrade_peak or 0) >= 1.0: promotion_score += 1.0
        demotion_score = 0.0
        if int(failed_streak or 0) >= COIN_DEMOTION_FAILED_UPGRADES: demotion_score += 2.0
        if int(upgrade_trades or 0) >= COIN_PROMOTION_MIN_UPGRADES and float(recent_upgrade_avg or 0) <= COIN_DEMOTION_RECENT_UPGRADE_AVG: demotion_score += 2.0
        if int(upgrade_trades or 0) >= COIN_PROMOTION_MIN_UPGRADES and float(upgrade_avg or 0) < 0: demotion_score += 1.0
        new_mode = old_mode or static["mode"]
        new_tier = old_tier or static["tier"]
        reason = "no_mode_change"
        if sym in COIN_RETIRED or sym in COIN_TIER_D:
            new_mode, new_tier, reason = "DISABLED", static["tier"], "static_disabled_or_retired"
        elif allow_mode_change:
            if (old_mode or static["mode"]) == "SHADOW" and promotion_score >= 5:
                new_mode, new_tier, reason = "LIVE", ("B" if static["tier"] == "C" else static["tier"]), f"promotion_score_{round(promotion_score,2)}_upgrade_edge_confirmed"
            elif (old_mode or static["mode"]) == "LIVE" and demotion_score >= 3:
                new_mode, reason = "SHADOW", f"demotion_score_{round(demotion_score,2)}_edge_deterioration"
        if not old_mode:
            new_mode, new_tier, reason = static["mode"], static["tier"], "initial_profile"
        sample_size = int(probe_trades or 0) + int(upgrade_trades or 0)
        hero_points = 3 if float(upgrade_peak or 0) >= 20 else 2 if float(upgrade_peak or 0) >= 10 else 1 if float(upgrade_peak or 0) >= 5 else 0
        cur.execute("""
            UPDATE coin_scores SET
                total_upgrades=%s, winning_upgrades=%s, losing_upgrades=%s,
                avg_peak=%s, avg_final=%s, total_pnl_gbp=%s,
                consecutive_failed_upgrades=%s, hero_points=%s, tier=%s,
                coin_health_mode=%s, personality=%s, best_metric=%s,
                momentum_lift=%s, trend_lift=%s, leadership_lift=%s, heat_lift=%s,
                recent_probe_avg=%s, recent_upgrade_avg=%s,
                promotion_score=%s, demotion_score=%s, sample_size=%s,
                last_updated=NOW()
            WHERE symbol=%s
        """, (
            int(upgrade_trades or 0), int(winning_upgrades or 0), int(losing_upgrades or 0),
            float(upgrade_peak or 0), float(upgrade_avg or 0), float(upgrade_pnl_gbp or 0),
            int(failed_streak or 0), int(hero_points), new_tier, new_mode, personality, best_metric,
            float(momentum_lift or 0), float(trend_lift or 0), float(leadership_lift or 0), float(heat_lift or 0),
            float(recent_probe_avg or 0), float(recent_upgrade_avg or 0), float(promotion_score), float(demotion_score),
            sample_size, sym
        ))
        stats = {"probe_trades": int(probe_trades or 0), "probe_avg": float(probe_avg or 0), "upgrade_trades": int(upgrade_trades or 0), "upgrade_avg": float(upgrade_avg or 0), "best_metric": best_metric, "personality": personality}
        maybe_send_coin_mode_alert(sym, old_mode, new_mode, old_tier, new_tier, reason, stats)
        return stats
    except Exception as e:
        print(f"⚠️ coin learning refresh failed for {sym}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return None


# =========================
# 🌎 MARKET LIFECYCLE ENGINE v6.9 — DATA ONLY
# =========================

def safe_float_value(value, default=None):
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def classify_market_lifecycle_state(market_breadth, market_breadth_accel, market_median_peak_context):
    """
    Market Lifecycle Engine V1 labels discovered 2026-05-31.
    IMPORTANT: This is logging/telemetry only in v6.9. It must not gate entries/exits.
    """
    breadth = safe_float_value(market_breadth, 0.0)
    ba = safe_float_value(market_breadth_accel, 0.0)
    median_ctx = safe_float_value(market_median_peak_context, 0.0)

    if breadth < LIFECYCLE_DEAD_BREADTH_MAX and median_ctx < LIFECYCLE_DEAD_MEDIAN_MAX:
        state = "DEAD_CORE"
    elif breadth < LIFECYCLE_DEAD_BREADTH_MAX and median_ctx >= LIFECYCLE_DEVELOPING_MEDIAN_MIN:
        state = "DEVELOPING_CORE"
    elif (
        LIFECYCLE_PRODUCTIVE_BREADTH_MIN <= breadth <= LIFECYCLE_PRODUCTIVE_BREADTH_MAX
        and ba > 0
    ):
        state = "PRODUCTIVE_CORE"
    elif breadth > LIFECYCLE_CROWDED_BREADTH_MIN and ba < 0:
        state = "CROWDED_CORE"
    else:
        state = "TRANSITION"

    if ba > 0:
        movement = "RISING"
    elif ba < 0:
        movement = "FALLING"
    else:
        movement = "STABLE"

    return state, movement, f"{state}_{movement}"


def get_market_lifecycle_context(cur, leadership_context=None, signal_time=None):
    """
    Builds a no-leakage live lifecycle snapshot.

    Notes:
    - market_breadth uses the same current breadth components already stored on signals.
    - market_breadth_accel compares current breadth against the latest previously stored lifecycle breadth.
    - market_median_peak_context is a live proxy from recent leadership_score_at_signal values where available.
      Historical backfills can use future_trade_paths_v1, but live code cannot use future data.
    """
    ctx = leadership_context or {}

    core = safe_float_value(ctx.get("market_core_count"), 0.0)
    aggressive = safe_float_value(ctx.get("market_aggressive_count"), 0.0)
    monster = safe_float_value(ctx.get("market_monster_count"), 0.0)
    market_breadth = core + aggressive + monster

    previous_breadth = None
    try:
        cur.execute("""
            SELECT market_breadth
            FROM signals_raw
            WHERE market_breadth IS NOT NULL
            ORDER BY timestamp DESC
            LIMIT 1
        """)
        row = cur.fetchone()
        if row and row[0] is not None:
            previous_breadth = float(row[0])
    except Exception as e:
        print(f"⚠️ lifecycle previous breadth lookup failed: {e}", flush=True)

    market_breadth_accel = (market_breadth - previous_breadth) if previous_breadth is not None else 0.0

    market_median_peak_context = None
    try:
        # Live no-leakage proxy. This uses recent already-known signal context, not future paths.
        cur.execute("""
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY leadership_score_at_signal)
            FROM signals_raw
            WHERE leadership_score_at_signal IS NOT NULL
              AND timestamp >= NOW() - INTERVAL '60 minutes'
        """)
        row = cur.fetchone()
        if row and row[0] is not None:
            market_median_peak_context = float(row[0])
    except Exception as e:
        print(f"⚠️ lifecycle median context lookup failed: {e}", flush=True)

    if market_median_peak_context is None:
        # Last-resort no-leakage fallback. Keeps logging populated without blocking the bot.
        market_median_peak_context = safe_float_value(ctx.get("prior_avg_peak") or ctx.get("leadership_score"), 0.0)

    state, movement, window = classify_market_lifecycle_state(
        market_breadth,
        market_breadth_accel,
        market_median_peak_context
    )

    return {
        "market_lifecycle_state": state,
        "market_lifecycle_movement": movement,
        "market_lifecycle_window": window,
        "market_breadth": market_breadth,
        "market_breadth_accel": market_breadth_accel,
        "market_median_peak_context": market_median_peak_context,
    }


def ensure_market_lifecycle_alert_state_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS market_lifecycle_alert_state (
            id INTEGER PRIMARY KEY DEFAULT 1,
            last_window TEXT,
            last_state TEXT,
            last_movement TEXT,
            last_breadth NUMERIC,
            last_breadth_accel NUMERIC,
            last_median_peak_context NUMERIC,
            updated_at TIMESTAMPTZ DEFAULT NOW(),
            CHECK (id = 1)
        )
    """)


def maybe_send_market_lifecycle_change_alert(cur, lifecycle_context):
    if not ENABLE_MARKET_LIFECYCLE_ENGINE_V69 or not ENABLE_MARKET_LIFECYCLE_TELEGRAM_V69:
        return False
    if not lifecycle_context:
        return False

    current_window = lifecycle_context.get("market_lifecycle_window")
    if not current_window:
        return False

    try:
        ensure_market_lifecycle_alert_state_table(cur)
        cur.execute("SELECT last_window, updated_at FROM market_lifecycle_alert_state WHERE id = 1")
        row = cur.fetchone()
        previous_window = row[0] if row else None
        last_alert_time = row[1] if row and len(row) > 1 else None

        if previous_window == current_window:
            return False

        try:
            from datetime import datetime, timedelta
            if last_alert_time and (datetime.utcnow() - last_alert_time.replace(tzinfo=None)).total_seconds() < (REGIME_ALERT_COOLDOWN_MINUTES * 60):
                return False
        except Exception:
            pass

        msg = (
            "🌎 <b>MARKET LIFECYCLE CHANGE</b>\n"
            f"Previous: <b>{previous_window or 'n/a'}</b>\n"
            f"Current: <b>{current_window}</b>\n"
            f"State: {lifecycle_context.get('market_lifecycle_state')} | "
            f"Move: {lifecycle_context.get('market_lifecycle_movement')}\n"
            f"Breadth: {fmt_num(lifecycle_context.get('market_breadth'), 2)} | "
            f"BA: {fmt_num(lifecycle_context.get('market_breadth_accel'), 2)}\n"
            f"Median ctx: {fmt_num(lifecycle_context.get('market_median_peak_context'), 3)}\n"
            "Mode: data/logging only — no trade behaviour changed."
        )

        sent = send_telegram_alert(msg)

        cur.execute("""
            INSERT INTO market_lifecycle_alert_state (
                id, last_window, last_state, last_movement,
                last_breadth, last_breadth_accel, last_median_peak_context, updated_at
            )
            VALUES (1,%s,%s,%s,%s,%s,%s,NOW())
            ON CONFLICT (id) DO UPDATE SET
                last_window = EXCLUDED.last_window,
                last_state = EXCLUDED.last_state,
                last_movement = EXCLUDED.last_movement,
                last_breadth = EXCLUDED.last_breadth,
                last_breadth_accel = EXCLUDED.last_breadth_accel,
                last_median_peak_context = EXCLUDED.last_median_peak_context,
                updated_at = NOW()
        """, (
            lifecycle_context.get("market_lifecycle_window"),
            lifecycle_context.get("market_lifecycle_state"),
            lifecycle_context.get("market_lifecycle_movement"),
            lifecycle_context.get("market_breadth"),
            lifecycle_context.get("market_breadth_accel"),
            lifecycle_context.get("market_median_peak_context"),
        ))

        return sent
    except Exception as e:
        print(f"⚠️ lifecycle Telegram alert skipped: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return False


def lifecycle_trade_telemetry_from_context(ctx):
    ctx = ctx or {}
    return {
        "market_lifecycle_state_at_entry": ctx.get("market_lifecycle_state"),
        "market_lifecycle_movement_at_entry": ctx.get("market_lifecycle_movement"),
        "market_lifecycle_window_at_entry": ctx.get("market_lifecycle_window"),
        "market_breadth_at_entry": ctx.get("market_breadth"),
        "market_breadth_accel_at_entry": ctx.get("market_breadth_accel"),
        "market_median_peak_context_at_entry": ctx.get("market_median_peak_context"),
    }

def live_entry_allowed_by_control_panel():
    return ENABLE_LIVE_ORDERS and ENABLE_LIVE_TRADING and ENABLE_NEW_ENTRIES and not EMERGENCY_CLOSE_ONLY_MODE and ENABLE_LONGS


def live_exit_allowed_by_control_panel():
    return ENABLE_LIVE_ORDERS and ENABLE_LIVE_TRADING and ENABLE_OKX_EXITS


def first_non_empty(*values, default=None):
    for val in values:
        if val is not None and val != "":
            return val
    return default


def build_entry_leadership_snapshot(quality, leadership_context=None):
    ctx = leadership_context or {}
    score = first_non_empty(ctx.get("leadership_score"), ctx.get("prior_avg_peak"), ctx.get("avg_peak"), default=0)
    return {
        "trade_size_gbp": get_trade_size_for_context(quality, ctx),
        "dynamic_trade_size_gbp": get_trade_size_for_context(quality, ctx),
        "leadership_prior_successes": first_non_empty(ctx.get("prior_successes"), ctx.get("successful_signals"), default=0),
        "leadership_prior_runners": first_non_empty(ctx.get("prior_runners"), ctx.get("runners"), default=0),
        "leadership_prior_avg_peak": score,
        "leadership_tier": quality,
        "leadership_mode": first_non_empty(ctx.get("leadership_mode"), quality),
        "leadership_score": score,
        "lifecycle_phase_at_entry": first_non_empty(ctx.get("lifecycle_phase"), ctx.get("leadership_phase")),
        "prior_lifecycle_phase_at_entry": ctx.get("prior_lifecycle_phase"),
        "leadership_transition_at_entry": ctx.get("leadership_transition"),
        "leadership_delta_5m_at_entry": ctx.get("leadership_delta_5m"),
        "leadership_delta_15m_at_entry": ctx.get("leadership_delta_15m"),
        "leadership_delta_30m_at_entry": first_non_empty(ctx.get("leadership_delta_30m"), ctx.get("delta_30m")),
        "leadership_delta_60m_at_entry": ctx.get("leadership_delta_60m"),
        "leadership_score_30m_ago_at_entry": first_non_empty(ctx.get("leadership_score_30m_ago"), ctx.get("score_30m_ago")),
        "leadership_age_minutes_at_entry": ctx.get("leadership_age_minutes"),
        "leadership_peak_score_last_4h_at_entry": ctx.get("leadership_peak_score_last_4h"),
        "leadership_rank_at_entry": ctx.get("leadership_rank"),
        "market_near_count_at_entry": ctx.get("market_near_count"),
        "market_core_count_at_entry": ctx.get("market_core_count"),
        "market_aggressive_count_at_entry": ctx.get("market_aggressive_count"),
        "market_monster_count_at_entry": ctx.get("market_monster_count"),
        "shadow_emergence_detected_at_entry": ctx.get("shadow_emergence_detected"),
        "shadow_emergence_reason_at_entry": ctx.get("shadow_emergence_reason"),
        "market_os_engine": ctx.get("market_os_engine"),
        "size_scaling_reason": ctx.get("size_scaling_reason"),
    }


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


def live_pressure_sql_expr():
    """
    v6.6.18:
    Live leadership pressure is derived from recent raw market signals, not the
    slower/future-quality leadership_state_history score. This gives moving
    deltas during short/none/weak markets and prevents frozen lifecycle displays.
    """
    return """
        CASE
            WHEN UPPER(COALESCE(decision,'')) LIKE '%%LONG%%'
                THEN GREATEST(COALESCE(momentum,0),0) * 0.7 + GREATEST(COALESCE(trend,0),0) * 1.3
            WHEN UPPER(COALESCE(decision,'')) LIKE '%%SHORT%%'
                THEN -1 * (ABS(COALESCE(momentum,0)) * 0.7 + ABS(COALESCE(trend,0)) * 1.3)
            ELSE COALESCE(momentum,0) * 0.4 + COALESCE(trend,0) * 0.6
        END
    """


def get_live_leadership_pressure_context(cur, symbol):
    """
    Returns live pressure context from signals_raw.
    This complements leadership_state_history, which is a slower structural
    quality/history table and can legitimately stay flat for long periods.
    """
    try:
        cur.execute(f"""
            WITH scored AS (
                SELECT
                    symbol,
                    timestamp,
                    momentum,
                    trend,
                    decision,
                    {live_pressure_sql_expr()} AS live_pressure
                FROM signals_raw
                WHERE symbol = %s
                  AND timestamp >= NOW() - INTERVAL '90 minutes'
            ),
            latest AS (
                SELECT *
                FROM scored
                ORDER BY timestamp DESC
                LIMIT 1
            ),
            agg AS (
                SELECT
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '5 minutes') AS pressure_5m,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '15 minutes') AS pressure_15m,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS pressure_30m,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '60 minutes') AS pressure_60m,
                    COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS signals_30m,
                    SUM(CASE WHEN UPPER(COALESCE(decision,'')) LIKE '%%LONG%%' THEN 1 ELSE 0 END)
                        FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS long_signals_30m,
                    SUM(CASE WHEN UPPER(COALESCE(decision,'')) LIKE '%%SHORT%%' THEN 1 ELSE 0 END)
                        FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS short_signals_30m
                FROM scored
            )
            SELECT
                latest.timestamp,
                latest.momentum,
                latest.trend,
                latest.decision,
                latest.live_pressure,
                agg.pressure_5m,
                agg.pressure_15m,
                agg.pressure_30m,
                agg.pressure_60m,
                agg.signals_30m,
                agg.long_signals_30m,
                agg.short_signals_30m
            FROM latest
            CROSS JOIN agg
        """, (symbol,))
        row = cur.fetchone()
        if not row:
            return {
                "live_pressure_available": False,
                "live_pressure_phase": "NO_RECENT_SIGNALS",
            }

        (
            latest_ts,
            momentum,
            trend,
            decision,
            latest_pressure,
            pressure_5m,
            pressure_15m,
            pressure_30m,
            pressure_60m,
            signals_30m,
            long_signals_30m,
            short_signals_30m,
        ) = row

        p5 = safe_float(pressure_5m, None)
        p15 = safe_float(pressure_15m, None)
        p30 = safe_float(pressure_30m, None)
        p60 = safe_float(pressure_60m, None)

        live_delta_30m = (p15 - p60) if p15 is not None and p60 is not None else None
        live_delta_15m = (p5 - p30) if p5 is not None and p30 is not None else None

        if p30 is None:
            phase = "NO_PRESSURE"
        elif p30 >= 0.75 and (live_delta_30m or 0) > 0:
            phase = "LIVE_EXPANDING"
        elif p30 >= 0.75:
            phase = "LIVE_STRONG_FLAT"
        elif p30 >= 0.35 and (live_delta_30m or 0) > 0:
            phase = "LIVE_ROTATION_IMPROVING"
        elif p30 >= 0.35:
            phase = "LIVE_WATCH"
        elif p30 < 0:
            phase = "LIVE_BEARISH_PRESSURE"
        else:
            phase = "LIVE_WEAK"

        return {
            "live_pressure_available": True,
            "live_pressure_phase": phase,
            "live_pressure_latest": safe_float(latest_pressure, None),
            "live_pressure_5m": p5,
            "live_pressure_15m": p15,
            "live_pressure_30m": p30,
            "live_pressure_60m": p60,
            "live_delta_15m": live_delta_15m,
            "live_delta_30m": live_delta_30m,
            "live_signal_timestamp": latest_ts,
            "live_momentum": safe_float(momentum, None),
            "live_trend": safe_float(trend, None),
            "live_decision": decision,
            "live_signals_30m": safe_int(signals_30m, 0),
            "live_long_signals_30m": safe_int(long_signals_30m, 0),
            "live_short_signals_30m": safe_int(short_signals_30m, 0),
        }

    except Exception as e:
        print(f"⚠️ live leadership pressure context failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return {
            "live_pressure_available": False,
            "live_pressure_phase": "LIVE_PRESSURE_ERROR",
        }


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


def get_usd_gbp_rate():
    """v7.1: central USD→GBP conversion rate.

    OKX spot orders use USDT quote sizing. We keep this as an env-controlled
    rate for stable bot execution; update Render env var USD_GBP_RATE when needed.
    """
    try:
        return float(os.environ.get("USD_GBP_RATE", USD_GBP_RATE) or USD_GBP_RATE)
    except Exception:
        return float(USD_GBP_RATE)


def gbp_to_usdt_quote(size_gbp):
    """
    v7.3 hotfix:
    Config/model sizes are GBP, but OKX spot market buys use USDT quote sizing.
    Convert intended GBP exposure into USDT before sending live orders.
    Example at USD_GBP_RATE=0.74: £10 -> ~$13.51 USDT.
    """
    try:
        size_gbp = float(size_gbp or 0)
        rate = float(get_usd_gbp_rate() or USD_GBP_RATE or 0.74)
        if rate <= 0:
            rate = 0.74
        return round(size_gbp / rate, 4)
    except Exception:
        return float(size_gbp or 0)


def usdt_quote_to_gbp(size_usdt):
    """Convert actual OKX USDT quote notional back into GBP reporting value."""
    try:
        return float(size_usdt or 0) * float(get_usd_gbp_rate())
    except Exception:
        return float(size_usdt or 0) * float(USD_GBP_RATE)

def accounting_entry_telemetry(size_usdt):
    """Telemetry payload for opening/expanding a USDT-quoted position."""
    try:
        size_usdt = float(size_usdt or 0)
    except Exception:
        size_usdt = 0.0
    rate = get_usd_gbp_rate()
    return {
        "trade_size_usdt": size_usdt,
        "entry_value_usdt": size_usdt,
        "entry_value_gbp": size_usdt * rate,
        "usd_gbp_rate": rate,
    }

def accounting_exit_values(size_usdt, pnl_percent, partial_bank_realized_gbp=0.0, remaining_fraction=1.0):
    """v7.1 exit accounting.

    size_usdt is the total OKX quote notional represented by the DB trade.
    pnl_usdt is calculated from actual percent movement and converted to GBP.
    Partial-bank GBP, when present, is added to the final GBP result.
    """
    try:
        size_usdt = float(size_usdt or 0)
    except Exception:
        size_usdt = 0.0
    try:
        pnl_percent = float(pnl_percent or 0)
    except Exception:
        pnl_percent = 0.0
    try:
        remaining_fraction = float(remaining_fraction if remaining_fraction is not None else 1.0)
    except Exception:
        remaining_fraction = 1.0
    try:
        partial_bank_realized_gbp = float(partial_bank_realized_gbp or 0)
    except Exception:
        partial_bank_realized_gbp = 0.0

    rate = get_usd_gbp_rate()
    remaining_pnl_usdt = (pnl_percent / 100.0) * size_usdt * remaining_fraction
    partial_bank_usdt_equiv = partial_bank_realized_gbp / rate if rate else 0.0
    pnl_usdt = remaining_pnl_usdt + partial_bank_usdt_equiv
    pnl_gbp = pnl_usdt * rate
    entry_value_usdt = size_usdt
    exit_value_usdt = entry_value_usdt + pnl_usdt

    return {
        "trade_size_usdt": size_usdt,
        "entry_value_usdt": entry_value_usdt,
        "exit_value_usdt": exit_value_usdt,
        "pnl_usdt": pnl_usdt,
        "usd_gbp_rate": rate,
        "entry_value_gbp": entry_value_usdt * rate,
        "exit_value_gbp": exit_value_usdt * rate,
        "pnl_gbp": pnl_gbp,
    }

def fee_adjusted_pnl_gbp(pnl_gbp, fee_gbp=0):
    """Return net GBP PnL after fees when fee_gbp is available.
    We do not estimate fees here; if fee_gbp is NULL/0, net equals recorded PnL.
    """
    try:
        return float(pnl_gbp or 0) - float(fee_gbp or 0)
    except Exception:
        return pnl_gbp

def engine_display_name(entry_quality):
    mapping = {
        LEGACY_CQE_ENTRY_QUALITY: CQE_CONTINUATION_ENTRY_QUALITY,
        CQE_CONTINUATION_ENTRY_QUALITY: CQE_CONTINUATION_ENTRY_QUALITY,
        "LEADERSHIP_SCALED": "LEADERSHIP_SCALED",
        "ROT_MICRO_V1": "ROT_MICRO_V1",
        "BPT_CQE_LIFECYCLE_V1": "BPT_CQE_LIFECYCLE_V1",
        "PERSISTENCE_HUNTER_V1": "PERSISTENCE_HUNTER_V1",
        "COIN_HEALTH_SHADOW": "COIN_HEALTH_SHADOW",
    }
    return mapping.get(entry_quality, entry_quality or "UNKNOWN_ENGINE")

def engine_emoji(entry_quality, is_shadow=False):
    if is_shadow:
        return "🧪"
    if entry_quality in [CQE_CONTINUATION_ENTRY_QUALITY, LEGACY_CQE_ENTRY_QUALITY]:
        return "🧠"
    if entry_quality == "LEADERSHIP_SCALED":
        return "👑"
    if entry_quality == "ROT_MICRO_V1":
        return "⚡"
    if entry_quality == "BPT_CQE_LIFECYCLE_V1":
        return "🧬"
    if entry_quality == "COIN_HEALTH_SHADOW":
        return "🩺"
    return "🤖"

def live_shadow_label(is_shadow=False):
    return "🧪 SHADOW" if bool(is_shadow) else "🟢 LIVE"

def trend_persistence_exit_reason(entry_quality):
    if entry_quality in [CQE_CONTINUATION_ENTRY_QUALITY, LEGACY_CQE_ENTRY_QUALITY]:
        return "cqe_trend_fail_30m"
    if entry_quality == "BPT_CQE_LIFECYCLE_V1":
        return "bpt_trend_fail_30m"
    if entry_quality == "LEADERSHIP_SCALED":
        return "leadership_trend_fail_30m"
    return "trend_fail_30m"

def should_trend_persistence_exit(entry_quality, minutes_in_trade, current_trend):
    if not ENABLE_TREND_PERSISTENCE_EXIT:
        return False
    if entry_quality not in TREND_PERSISTENCE_ENGINE_QUALITIES:
        return False
    try:
        return float(minutes_in_trade or 0) >= TREND_PERSISTENCE_CHECK_MINUTES and float(current_trend or 0) < TREND_PERSISTENCE_MIN_TREND
    except Exception:
        return False

def format_trade_pnl_lines(pnl_percent, gross_pnl_gbp, fee_gbp=0):
    net_gbp = fee_adjusted_pnl_gbp(gross_pnl_gbp, fee_gbp)
    fee = float(fee_gbp or 0)
    if fee:
        return (
            f"Net PnL: <b>{fmt_num(pnl_percent)}%</b> | <b>{fmt_money(net_gbp)}</b>\n"
            f"Gross: {fmt_money(gross_pnl_gbp)} | Fees: {fmt_money(fee)}"
        )
    return f"Net PnL: <b>{fmt_num(pnl_percent)}%</b> | <b>{fmt_money(net_gbp)}</b>"

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
        ADD COLUMN IF NOT EXISTS trade_size_usdt FLOAT,
        ADD COLUMN IF NOT EXISTS entry_value_usdt FLOAT,
        ADD COLUMN IF NOT EXISTS exit_value_usdt FLOAT,
        ADD COLUMN IF NOT EXISTS pnl_usdt FLOAT,
        ADD COLUMN IF NOT EXISTS usd_gbp_rate FLOAT,
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
    """Return USDT quote size for OKX from the configured GBP model size."""
    return gbp_to_usdt_quote(get_trade_size_for_context(entry_quality, leadership_context))


def is_rot_micro_candidate(cur, symbol, momentum, trend, leadership_context=None, shadow_mode=False):
    """Independent rotational micro layer. Small size only. Does not replace core."""
    if not ENABLE_MARKET_OS_V66:
        return False, "rot_micro_market_os_disabled", {}
    if shadow_mode:
        if not ENABLE_ROT_MICRO_SHADOW:
            return False, "rot_micro_shadow_disabled", {}
    elif not ENABLE_ROT_MICRO_LIVE:
        return False, "rot_micro_live_disabled", {}
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

    market_context = safe_float_value((leadership_context or {}).get("market_median_peak_context"), None)
    if market_context is not None:
        ctx["market_median_peak_context"] = market_context
    if shadow_mode and market_context is not None and market_context < ROT_MICRO_MIN_CONTEXT:
        return False, "rot_micro_context_too_low", ctx
    if shadow_mode and market_context is None:
        return False, "rot_micro_context_missing", ctx

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
        realized_pnl_gbp = (float(PARTIAL_BANK_TRIGGER_PCT) / 100.0) * bank_size * get_usd_gbp_rate()

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
            f"{'💚 LIVE PARTIAL BANK' if not is_shadow_trade else '👻 SHADOW PARTIAL BANK'} | {sym}\n"
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

def okx_inst_id_to_quote_ccy(okx_inst_id):
    if not okx_inst_id or "-" not in okx_inst_id:
        return None
    return okx_inst_id.split("-")[-1].upper()

def resolve_live_entry_quote_size(cur, symbol, okx_inst_id, requested_quote_size):
    """
    v6.9.2: If available quote balance is slightly below requested size,
    use what is available instead of skipping a valid signal.
    Below MIN_POSITION_SIZE_GBP, fail closed and do not place the live entry.
    """
    requested = float(requested_quote_size or 0)
    if requested <= 0:
        return 0.0, {"adjusted": False, "reason": "invalid_requested_size"}

    if not ALLOW_PARTIAL_POSITION_FILL:
        return requested, {"adjusted": False, "reason": "partial_fill_disabled"}

    quote_ccy = okx_inst_id_to_quote_ccy(okx_inst_id) or "USDT"

    try:
        balance_result = okx_get_available_balance(quote_ccy)
        if not balance_result.get("success"):
            return requested, {
                "adjusted": False,
                "reason": "quote_balance_lookup_failed_use_requested",
                "quote_ccy": quote_ccy,
                "error": balance_result.get("error"),
            }

        available = float(balance_result.get("available") or 0)
        actual = min(requested, available)

        if actual < float(MIN_POSITION_SIZE_GBP):
            return 0.0, {
                "adjusted": True,
                "reason": "available_below_min_position_size",
                "quote_ccy": quote_ccy,
                "requested_quote_size": requested,
                "available_quote_size": available,
                "min_position_size": float(MIN_POSITION_SIZE_GBP),
            }

        if actual < requested:
            return actual, {
                "adjusted": True,
                "reason": "partial_position_fill",
                "quote_ccy": quote_ccy,
                "requested_quote_size": requested,
                "available_quote_size": available,
                "actual_quote_size": actual,
            }

        return requested, {
            "adjusted": False,
            "reason": "sufficient_quote_balance",
            "quote_ccy": quote_ccy,
            "requested_quote_size": requested,
            "available_quote_size": available,
        }

    except Exception as e:
        return requested, {
            "adjusted": False,
            "reason": "partial_fill_exception_use_requested",
            "quote_ccy": quote_ccy,
            "error": str(e),
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


def okx_get_instrument_last_price(okx_inst_id):
    """
    Fetch current OKX instrument price for exit sizing.

    Important:
    TradingView/Binance signal prices can differ from OKX spot prices for some
    instruments/tokens. Exit orders must convert quote size to base quantity
    using the OKX instrument price, not the signal entry price. This prevents
    upgraded live BPT positions from only being partially sold on exit.
    """
    try:
        response = requests.get(
            f"{OKX_BASE_URL}/api/v5/market/ticker",
            params={"instId": okx_inst_id},
            timeout=10,
        )
        payload = response.json()
        if response.status_code != 200 or str(payload.get("code")) != "0":
            return {
                "success": False,
                "price": None,
                "error": f"ticker_failed: {payload}",
                "response": payload,
            }

        data = payload.get("data") or []
        if not data:
            return {
                "success": False,
                "price": None,
                "error": "ticker_no_data",
                "response": payload,
            }

        price_raw = data[0].get("last") or data[0].get("askPx") or data[0].get("bidPx")
        price = float(price_raw or 0)
        if price <= 0:
            return {
                "success": False,
                "price": None,
                "error": f"ticker_invalid_price: {price_raw}",
                "response": payload,
            }

        return {"success": True, "price": price, "error": None, "response": payload}

    except Exception as e:
        return {"success": False, "price": None, "error": str(e), "response": None}

def okx_has_live_position(symbol, reference_price=None):
    """
    v6.6.12 exchange-truth guard.
    For spot LONGs, a live position means OKX has meaningful available base balance.
    This is intentionally conservative: if OKX says we hold the asset, block new buys
    even when Supabase says open_real = 0.
    """
    if not ENABLE_LIVE_ORDERS:
        return False, {"reason": "live_orders_disabled"}

    try:
        okx_inst_id = okx_symbol_to_inst_id(symbol)
        base_ccy = okx_inst_id_to_base_ccy(okx_inst_id)
        if not base_ccy:
            return False, {"reason": "missing_base_currency", "symbol": symbol, "okx_inst_id": okx_inst_id}

        balance_result = okx_get_available_balance(base_ccy)
        if not balance_result.get("success"):
            # Fail closed for entry safety: if we cannot verify exchange position, do not buy.
            return True, {
                "reason": "could_not_verify_okx_balance_fail_closed",
                "symbol": symbol,
                "base_ccy": base_ccy,
                "error": balance_result.get("error"),
                "response": balance_result.get("response"),
            }

        available = float(balance_result.get("available") or 0)
        ref_price = float(reference_price or 0)
        notional = available * ref_price if ref_price > 0 else 0
        has_position = available > 0 and (notional >= OKX_POSITION_DUST_USD if ref_price > 0 else True)

        return has_position, {
            "reason": "okx_base_balance_check",
            "symbol": symbol,
            "okx_inst_id": okx_inst_id,
            "base_ccy": base_ccy,
            "available": available,
            "reference_price": ref_price,
            "estimated_notional_usd": notional,
            "dust_usd": OKX_POSITION_DUST_USD,
        }

    except Exception as e:
        # Fail closed: the worst outcome is another duplicate buy, so block on guard errors.
        return True, {"reason": "okx_position_guard_error_fail_closed", "symbol": symbol, "error": str(e)}

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

        partial_fill_context = {"adjusted": False, "reason": "not_checked"}
        if live_entry_allowed_by_control_panel():
            adjusted_quote_size, partial_fill_context = resolve_live_entry_quote_size(
                cur, symbol, okx_inst_id, quote_size
            )
            if adjusted_quote_size <= 0:
                request_payload = {
                    "blocked": True,
                    "reason": partial_fill_context.get("reason"),
                    "symbol": symbol,
                    "okx_inst_id": okx_inst_id,
                    "action": action,
                    "requested_quote_size": quote_size,
                    "partial_fill_context": partial_fill_context,
                }
                log_okx_order(
                    cur, trade_id, symbol, okx_inst_id, action, side, direction,
                    False, request_payload, partial_fill_context, False,
                    f"entry_size_below_min_position: {partial_fill_context.get('reason')}"
                )
                print(
                    f"⛔ OKX ENTRY SIZE SKIPPED | {symbol}->{okx_inst_id} | "
                    f"requested={quote_size} | context={partial_fill_context}",
                    flush=True
                )
                return {
                    "success": False,
                    "dry_run": False,
                    "blocked": True,
                    "reason": partial_fill_context.get("reason"),
                    "requested_quote_size": quote_size,
                    "actual_quote_size": 0.0,
                    "partial_fill_context": partial_fill_context,
                }
            quote_size = adjusted_quote_size

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

        if live_exit_allowed_by_control_panel():
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

            # v6.8.3 HOTFIX:
            # Use OKX's own live ticker price for quote->base exit sizing.
            # Some TradingView/Binance signal prices differ from OKX spot prices,
            # which caused upgraded BPT exits to sell only part of the real OKX holding.
            ticker_result = okx_get_instrument_last_price(okx_inst_id)
            okx_reference_price = float(ticker_result.get("price") or 0) if ticker_result.get("success") else 0.0

            reference_price = okx_reference_price or price or entry_price or 0
            theoretical_trade_size = calculate_exit_base_size(reference_price, quote_size)
            desired_trade_sell_size = theoretical_trade_size * OKX_EXIT_SIZE_BUFFER
            max_safe_available_size = available_balance * OKX_EXIT_SIZE_BUFFER

            # v6.9.1 HOTFIX:
            # Current architecture is effectively non-stacking.
            # Use actual OKX available balance as source of truth for exits
            # to prevent residual positions accumulating from sizing drift.
            sell_size = round(max_safe_available_size, 8)

            if not ticker_result.get("success"):
                print(
                    f"⚠️ OKX TICKER PRICE FALLBACK | {symbol}->{okx_inst_id} | "
                    f"error={ticker_result.get('error')} | fallback_price={reference_price}",
                    flush=True
                )

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

    dry_run = not ((action == "entry" and live_entry_allowed_by_control_panel()) or (action == "exit" and live_exit_allowed_by_control_panel()))

    if dry_run:
        response_payload = {
            "dry_run": True,
            "message": "OKX live orders disabled. No order sent.",
            "payload": payload,
            "requested_quote_size": quote_size,
            "actual_quote_size": quote_size
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
        if ENABLE_BLOCKED_TRADE_TELEGRAM:
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
            if ENABLE_OKX_SUCCESS_TELEGRAM:
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
            "response": response_payload,
            "actual_quote_size": quote_size,
            "partial_fill_context": partial_fill_context if action == "entry" else None,
        }

    except Exception as e:
        error_message = str(e)
        log_okx_order(
            cur, trade_id, symbol, okx_inst_id, action, side, direction,
            False, payload, None, False, error_message
        )
        send_telegram_alert(
            f"🚨 <b>OKX API ERROR</b>\n"
            f"{action.upper()} | {symbol} → {okx_inst_id}\n"
            f"Error: {error_message[:300]}"
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

    live_score = first_non_empty(lifecycle_ctx.get("live_pressure_30m"), lifecycle_ctx.get("leadership_score"), leadership_score)
    live_delta = first_non_empty(lifecycle_ctx.get("live_pressure_delta_30m"), lifecycle_ctx.get("leadership_delta_30m"), delta_30m)
    return {
        "prior_successes": successful_signals or 0,
        "prior_runners": runners or 0,
        "prior_avg_peak": live_score,
        "leadership_score": live_score,
        "score_30m_ago": score_30m_ago_float,
        "delta_30m": live_delta,
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
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND entry_quality IN (%s, %s)
    """, (LEGACY_CQE_ENTRY_QUALITY, CQE_CONTINUATION_ENTRY_QUALITY))
    return cur.fetchone()[0] or 0


def get_open_same_symbol_shadow_cqe_count(cur, symbol):
    cur.execute("""
        SELECT COUNT(*)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = FALSE
          AND entry_quality IN (%s, %s)
          AND symbol = %s
    """, (LEGACY_CQE_ENTRY_QUALITY, CQE_CONTINUATION_ENTRY_QUALITY, symbol))
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
        CQE_CONTINUATION_ENTRY_QUALITY,
        signal_id,
        signal_time,
    ))
    trade_id = cur.fetchone()[0]

    apply_market_heat_to_trade(cur, trade_id)

    try:
        safe_update_trade_telemetry(cur, trade_id, accounting_entry_telemetry(CQE_SHADOW_TRADE_SIZE_GBP))
    except Exception as e:
        print(f"⚠️ v7.1 shadow CQE entry accounting telemetry failed | {symbol} | id={trade_id} | {e}", flush=True)
        safe_telemetry_rollback(cur)

    # v6.6.13 CRITICAL: persist the base trade row immediately BEFORE any
    # telemetry/event/OKX work. Previous versions could insert the trade row,
    # then a telemetry helper rollback would erase it while the OKX order log
    # and exchange buy still survived, creating orphan OKX buys invisible to
    # the exit engine.
    try:
        cur.connection.commit()
        print(f"✅ BASE TRADE ROW PERSISTED FIRST | {symbol} | id={trade_id}", flush=True)
    except Exception as e:
        print(f"🚨 BASE TRADE ROW EARLY COMMIT FAILED | {symbol} | id={trade_id} | {e}", flush=True)
        raise

    safe_update_trade_telemetry(cur, trade_id, {
        "is_shadow": False,
        "entry_architecture": CQE_CONTINUATION_ENTRY_QUALITY,
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
        **lifecycle_trade_telemetry_from_context(leadership_context),
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
        f"🟢 OPEN CQE CONTINUATION | {symbol} | id={trade_id} | "
        f"q={cqe_context.get('cqe_quality_score')} | "
        f"T/M={round(trend,3)}/{round(momentum,3)} | "
        f"m_acc={fmt_num(cqe_context.get('cqe_momentum_accel_30m'))} | "
        f"t_acc={fmt_num(cqe_context.get('cqe_trend_accel_30m'))}",
        flush=True
    )

    if ENABLE_SHADOW_CQE_TELEGRAM_ALERTS:
        send_telegram_alert(
            f"🟢 <b>LIVE ENTRY</b>\n🧠 <b>CQE_CONTINUATION_V1</b> | {symbol} LONG\n"
            f"Quality: <b>{cqe_context.get('cqe_quality_score')}/9</b> | Model size {fmt_money(CQE_SHADOW_TRADE_SIZE_GBP)}\n"
            f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
            f"Accel M/T: {fmt_num(cqe_context.get('cqe_momentum_accel_30m'))} / {fmt_num(cqe_context.get('cqe_trend_accel_30m'))}\n"
            f"Trend health: min {fmt_num(cqe_context.get('cqe_min_trend_30m'))} | "
            f"std {fmt_num(cqe_context.get('cqe_trend_std_30m'))} | "
            f"pos {fmt_num(cqe_context.get('cqe_positive_trend_ratio_30m'))}\n"
            f"Reason: {cqe_context.get('cqe_reason')}\n"
            f"ID {trade_id}"
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
          AND entry_quality IN (%s, %s)
          AND symbol = %s
    """, (LEGACY_CQE_ENTRY_QUALITY, CQE_CONTINUATION_ENTRY_QUALITY, symbol))

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
                f"🟢 <b>CQE PEAK</b> | {sym}\n"
                f"Peak {fmt_num(current_peak)}% | Current {fmt_num(pnl_percent)}% | Age {fmt_num(mins,1)}m\n"
                f"T/M {fmt_num(trend)} / {fmt_num(momentum)} | ID {tid}"
            )

        if (
            ENABLE_SHADOW_CQE_TELEGRAM_ALERTS
            and old_peak < CQE_RUNNER_ALERT_TRIGGER <= current_peak
        ):
            send_telegram_alert(
                f"🚀 <b>CQE RUNNER</b> | {sym}\n"
                f"Peak {fmt_num(current_peak)}% | Current {fmt_num(pnl_percent)}% | Age {fmt_num(mins,1)}m\n"
                f"ID {tid}"
            )

        close_reason = None
        if fixed_time_exit_allowed_for_trade(is_shadow=True) and mins >= CQE_SHADOW_HOLD_MINUTES:
            close_reason = "cqe_120m_time_exit"

        if close_reason:
            accounting = accounting_exit_values(CQE_SHADOW_TRADE_SIZE_GBP, pnl_percent)
            pnl_gbp = accounting["pnl_gbp"]
            cur.execute("""
                UPDATE bot_trades_v4
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_price = %s,
                    pnl_percent = %s,
                    pnl_gbp = %s,
                    pnl_usdt = %s,
                    exit_value_usdt = %s,
                    exit_value_gbp = %s,
                    usd_gbp_rate = %s,
                    close_reason = %s,
                    telegram_close_alert_sent = FALSE
                WHERE id = %s
                  AND status = 'OPEN'
                RETURNING id
            """, (
                price,
                pnl_percent,
                pnl_gbp,
                accounting["pnl_usdt"],
                accounting["exit_value_usdt"],
                accounting["exit_value_gbp"],
                accounting["usd_gbp_rate"],
                close_reason,
                tid,
            ))
            closed_row = cur.fetchone()
            if not closed_row:
                print(f"🔕 BPT close skipped; trade already closed or not open | {tid}", flush=True)
                continue

            safe_update_trade_telemetry(cur, tid, {
                "exit_architecture": CQE_CONTINUATION_ENTRY_QUALITY,
                "drawdown_from_peak_at_exit": current_peak - pnl_percent,
                "leadership_trend_at_exit": trend,
                "leadership_momentum_at_exit": momentum,
            })

            try:
                log_trade_event(cur, tid, sym, f"exit_{close_reason}", price, pnl_percent, current_peak, mins, momentum, trend, False)
            except Exception as e:
                print(f"⚠️ shadow CQE trade_events exit log failed: {e}", flush=True)

            print(
                f"🔴 CLOSED CQE CONTINUATION | {sym} | {round(pnl_percent,3)}% | "
                f"peak={round(current_peak,3)} | {close_reason}",
                flush=True
            )

            if ENABLE_SHADOW_CQE_TELEGRAM_ALERTS:
                send_telegram_alert(
                    f"🔴 <b>LIVE EXIT</b>\n🧠 <b>CQE_CONTINUATION_V1</b> | {sym}\n"
                    + format_trade_pnl_lines(pnl_percent, pnl_gbp, 0) + "\n"
                    + f"Peak {fmt_num(current_peak)}% | DD {fmt_num(current_peak - pnl_percent)}%\n"
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
        ADD COLUMN IF NOT EXISTS probe_lifecycle_state TEXT,
        ADD COLUMN IF NOT EXISTS probe_lifecycle_trigger TEXT,
        ADD COLUMN IF NOT EXISTS probe_lifecycle_trigger_age_minutes NUMERIC,
        ADD COLUMN IF NOT EXISTS probe_lifecycle_trigger_momentum NUMERIC,
        ADD COLUMN IF NOT EXISTS probe_lifecycle_trigger_trend NUMERIC,
        ADD COLUMN IF NOT EXISTS probe_lifecycle_trigger_delta NUMERIC,
        ADD COLUMN IF NOT EXISTS live_probe_exited_at TIMESTAMPTZ,
        ADD COLUMN IF NOT EXISTS live_probe_exit_reason TEXT,
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


def is_adaptive_dead_market_leader_candidate(leadership_context, momentum=None, trend=None):
    """
    v7.0 live probe pathway.

    Validated SQL cohort:
      market_median_peak_context < 2
      leadership_rank <= 3
      leadership_score < 0.5

    This intentionally uses rank/context/score only as the primary condition.
    Momentum/trend minimums default to 0.0 and are env-tunable safety rails.
    """
    if not ENABLE_ADAPTIVE_DEAD_MARKET_LEADERS:
        return False, "adaptive_dead_market_disabled"

    ctx = leadership_context or {}

    try:
        market_context = float(
            first_non_empty(
                ctx.get("market_median_peak_context"),
                ctx.get("market_median_peak_context_at_entry"),
                default=999.0,
            ) or 999.0
        )
    except Exception:
        market_context = 999.0

    try:
        rank = int(float(first_non_empty(ctx.get("leadership_rank"), ctx.get("leadership_rank_at_entry"), default=999) or 999))
    except Exception:
        rank = 999

    try:
        score = float(first_non_empty(ctx.get("leadership_score"), ctx.get("prior_avg_peak"), default=0.0) or 0.0)
    except Exception:
        score = 0.0

    try:
        mom = float(momentum if momentum is not None else 0.0)
    except Exception:
        mom = 0.0

    try:
        tr = float(trend if trend is not None else 0.0)
    except Exception:
        tr = 0.0

    if market_context >= ADAPTIVE_DEAD_MARKET_CONTEXT_MAX:
        return False, "adaptive_dead_market_context_not_dead"

    if rank > ADAPTIVE_DEAD_MARKET_MAX_RANK:
        return False, "adaptive_dead_market_rank_too_low"

    if score >= ADAPTIVE_DEAD_MARKET_MAX_SCORE:
        return False, "adaptive_dead_market_score_not_compressed"

    if mom < ADAPTIVE_DEAD_MARKET_MIN_MOMENTUM:
        return False, "adaptive_dead_market_momentum_too_low"

    if tr < ADAPTIVE_DEAD_MARKET_MIN_TREND:
        return False, "adaptive_dead_market_trend_too_low"

    return True, "adaptive_dead_market_leader_probe_allowed"


def tag_adaptive_dead_market_leader_context(leadership_context):
    """Mutates/returns context so entries are easy to track and toggle-analysis is clean."""
    ctx = leadership_context or {}
    ctx["adaptive_dead_market_leader"] = True
    ctx["adaptive_dead_market_context_max"] = ADAPTIVE_DEAD_MARKET_CONTEXT_MAX
    ctx["adaptive_dead_market_max_rank"] = ADAPTIVE_DEAD_MARKET_MAX_RANK
    ctx["adaptive_dead_market_max_score"] = ADAPTIVE_DEAD_MARKET_MAX_SCORE
    ctx["market_os_engine"] = "ADAPTIVE_DEAD_MARKET_LEADER"
    ctx["leadership_mode"] = "ADAPTIVE_DEAD_MARKET_LEADER"
    ctx["size_scaling_reason"] = "adaptive_dead_market_leader_probe"

    # Preserve actual lifecycle phase if present, but make the leadership phase explicit
    # so signals_raw / Telegram / trade rows identify why the BPT probe was allowed.
    ctx["leadership_phase"] = "ADAPTIVE_DEAD_LEADER"
    if not ctx.get("lifecycle_phase"):
        ctx["lifecycle_phase"] = "ADAPTIVE_DEAD_LEADER"

    return ctx


def passes_bpt_cqe_probe_gate(cur, symbol, momentum, trend, leadership_context):
    if not ENABLE_BPT_CQE_LIFECYCLE_SHADOW:
        return False, "bpt_lifecycle_disabled"

    adaptive_ok, adaptive_reason = is_adaptive_dead_market_leader_candidate(
        leadership_context,
        momentum=momentum,
        trend=trend,
    )

    # v7.0: Adaptive dead-market leaders bypass the old BPT trend/momentum
    # gate, because the validated cohort had compressed trend/score conditions.
    # They still obey BPT max-open, same-symbol, probe size, confirmation, upgrade,
    # exits, and OKX live toggles.
    if adaptive_ok:
        tag_adaptive_dead_market_leader_context(leadership_context)

        if get_open_bpt_cqe_count(cur) >= BPT_CQE_MAX_OPEN_TRADES:
            return False, "adaptive_dead_market_bpt_max_open_trades"
        if get_open_same_symbol_bpt_cqe_count(cur, symbol) >= BPT_CQE_MAX_SAME_SYMBOL_OPEN:
            return False, "adaptive_dead_market_bpt_max_same_symbol_open"

        return True, adaptive_reason

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

    apply_market_heat_to_trade(cur, trade_id)

    ghost_probe_active = bool(ENABLE_BPT_CQE_GHOST_PROBES)
    live_probe_enabled = bool(ENABLE_BPT_CQE_LIVE_PROBES and not ghost_probe_active)
    model_probe_size_gbp = 0.0 if ghost_probe_active else BPT_CQE_PROBE_SIZE_GBP
    model_probe_size_usdt = 0.0 if ghost_probe_active else gbp_to_usdt_quote(BPT_CQE_PROBE_SIZE_GBP)

    safe_update_trade_telemetry(cur, trade_id, {
        # Ghost probes are not marked shadow because they are eligible for a future live upgrade.
        # They simply carry zero capital until confirmation.
        "is_shadow": False if ghost_probe_active else not ENABLE_BPT_CQE_LIVE_PROBES,
        "entry_architecture": BPT_CQE_ENTRY_QUALITY,
        "market_os_engine": (leadership_context or {}).get("market_os_engine"),
        "size_scaling_reason": GHOST_PROBE_LABEL if ghost_probe_active else (leadership_context or {}).get("size_scaling_reason"),
        "shadow_reason": GHOST_PROBE_LABEL if ghost_probe_active else None,
        "leadership_mode": (leadership_context or {}).get("leadership_mode"),
        "trade_size_gbp": model_probe_size_gbp,
        "dynamic_trade_size_gbp": model_probe_size_gbp,
        "probe_size_gbp": model_probe_size_gbp,
        **accounting_entry_telemetry(model_probe_size_usdt),
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
        **lifecycle_trade_telemetry_from_context(leadership_context),
    })

    try:
        log_trade_event(cur, trade_id, symbol, "bpt_cqe_probe_entry", price, 0, 0, 0, momentum, trend, True)
    except Exception as e:
        print(f"⚠️ BPT CQE trade_events probe entry log failed: {e}", flush=True)

    if live_probe_enabled:
        okx_probe_result = okx_place_market_order(
            cur=cur,
            trade_id=trade_id,
            symbol=symbol,
            direction="LONG",
            action="entry",
            price=price,
            entry_price=price,
            trade_size_quote=gbp_to_usdt_quote(BPT_CQE_PROBE_SIZE_GBP),
        )
        if okx_probe_result.get("success"):
            requested_probe_quote = gbp_to_usdt_quote(BPT_CQE_PROBE_SIZE_GBP)
            actual_probe_quote = float(okx_probe_result.get("actual_quote_size") or requested_probe_quote)
            actual_probe_gbp = usdt_quote_to_gbp(actual_probe_quote)
            if abs(actual_probe_quote - float(requested_probe_quote)) > 0.0001:
                safe_update_trade_telemetry(cur, trade_id, {
                    "trade_size_gbp": actual_probe_gbp,
                    "dynamic_trade_size_gbp": actual_probe_gbp,
                    "probe_size_gbp": actual_probe_gbp,
                    **accounting_entry_telemetry(actual_probe_quote),
                    "size_scaling_reason": "bpt_probe_partial_position_fill",
                })
        else:
            cur.execute("""
                UPDATE bot_trades_v4
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_reason = %s
                WHERE id = %s
            """, (f"okx_bpt_probe_failed: {okx_probe_result.get('reason') or okx_probe_result.get('error')}", trade_id))

    print(
        f"👻 OPEN BPT CQE GHOST PROBE | {symbol} | id={trade_id} | row={lifecycle_row} | "
        f"q={round(q,3)} | size={fmt_money(model_probe_size_gbp)} | live_probe={live_probe_enabled} | T/M={round(trend,3)}/{round(momentum,3)}",
        flush=True
    )

    send_telegram_alert(
        f"👻 <b>GHOST PROBE OPENED</b>\n🧪 <b>BPT_CQE_LIFECYCLE_V1</b> | {symbol} LONG\n"
        f"Row: <b>{lifecycle_row}</b> | Q {fmt_num(q)}\n"
        f"Probe size: {fmt_money(BPT_CQE_PROBE_SIZE_GBP)} | Live probe: {ENABLE_BPT_CQE_LIVE_PROBES}\n"
        f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
        f"Confirm target: +{BPT_CQE_CONFIRM_PEAK}% within {BPT_CQE_CONFIRM_WINDOW_MINUTES}m\n"
        f"ID {trade_id}"
    )

    return trade_id


def maybe_open_bpt_cqe_probe(cur, symbol, price, momentum, trend, signal_id, signal_time, leadership_context=None):
    try:
        coin_profile = get_coin_score_profile(cur, symbol)
        if coin_profile.get("coin_health_mode") == "DISABLED":
            print(f"🚫 BPT probe skipped | {symbol} | coin disabled/retired", flush=True)
            return None
    except Exception as e:
        print(f"⚠️ BPT coin profile check failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)

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



def get_latest_probe_lifecycle_delta(cur, symbol, opened_at, now):
    """Returns the latest leadership_delta_30m_at_signal known for this probe up to now."""
    try:
        cur.execute("""
            SELECT leadership_delta_30m_at_signal
            FROM signals_raw
            WHERE symbol = %s
              AND timestamp >= %s
              AND timestamp <= %s
            ORDER BY timestamp DESC
            LIMIT 1
        """, (symbol, opened_at, now))
        row = cur.fetchone()
        if row and row[0] is not None:
            return safe_float(row[0], None)
    except Exception as e:
        print(f"⚠️ probe lifecycle delta lookup failed | {symbol} | {e}", flush=True)
    return None


def classify_probe_lifecycle_state(age_mins, momentum, trend, leadership_delta):
    """v7.2 evidence-based probe lifecycle classifier."""
    if not ENABLE_BPT_PROBE_LIFECYCLE_ENGINE:
        return None, "probe_lifecycle_disabled"

    age = safe_float(age_mins, 0)
    mom = safe_float(momentum, 0)
    tr = safe_float(trend, 0)
    delta = safe_float(leadership_delta, None)

    # Defensive branch first: protect capital before considering scale-in.
    if (
        age >= BPT_DEAD_PROBE_MIN_AGE_MINUTES
        and mom < BPT_DEAD_PROBE_MAX_MOMENTUM
        and tr < BPT_DEAD_PROBE_MAX_TREND
    ):
        return "DEAD", "probe_health_fail_10m"

    # Offensive 5m monster fast-track: intentionally strict.
    if (
        delta is not None
        and age >= BPT_MONSTER_FASTTRACK_MIN_AGE_MINUTES
        and delta > BPT_MONSTER_FASTTRACK_MIN_LEADERSHIP_DELTA
        and tr > BPT_MONSTER_FASTTRACK_MIN_TREND
    ):
        return "MONSTER", "probe_monster_fasttrack_5m"

    # Offensive 10m leader fast-track: broader emerging leader detector.
    if (
        delta is not None
        and age >= BPT_LEADER_FASTTRACK_MIN_AGE_MINUTES
        and delta > BPT_LEADER_FASTTRACK_MIN_LEADERSHIP_DELTA
        and tr > BPT_LEADER_FASTTRACK_MIN_TREND
    ):
        return "LEADER", "probe_leader_fasttrack_10m"

    return "NEUTRAL", "probe_lifecycle_neutral"


def mark_probe_lifecycle_state(cur, tid, state, trigger, age_mins, momentum, trend, leadership_delta):
    safe_update_trade_telemetry(cur, tid, {
        "probe_lifecycle_state": state,
        "probe_lifecycle_trigger": trigger,
        "probe_lifecycle_trigger_age_minutes": age_mins,
        "probe_lifecycle_trigger_momentum": momentum,
        "probe_lifecycle_trigger_trend": trend,
        "probe_lifecycle_trigger_delta": leadership_delta,
    })


def exit_live_probe_and_continue_shadow(cur, tid, sym, direction, entry_price, price, pnl_percent, current_peak, age_mins, momentum, trend, dynamic_size, reason, trade_size_usdt=None):
    """Sell the live probe but keep the DB row open as a shadow continuation."""
    try:
        if has_successful_okx_live_entry(cur, tid):
            okx_place_market_order(
                cur=cur,
                trade_id=tid,
                symbol=sym,
                direction=direction,
                action="exit",
                price=price,
                entry_price=entry_price,
                trade_size_quote=float(trade_size_usdt or gbp_to_usdt_quote(float(dynamic_size or BPT_CQE_PROBE_SIZE_GBP))),
            )
        else:
            log_okx_exit_skip_no_live_entry(cur, tid, sym, direction, price)
    except Exception as e:
        print(f"⚠️ live probe exit for shadow continuation failed | {sym} | {tid} | {e}", flush=True)

    safe_update_trade_telemetry(cur, tid, {
        "is_shadow": True,
        "live_probe_exited_at": datetime.now(timezone.utc),
        "live_probe_exit_reason": reason,
        "exit_architecture": "BPT_PROBE_LIFECYCLE_LIVE_EXIT_SHADOW_CONTINUATION",
        "bpt_exit_reason": reason,
        "archetype_exit_reason": reason,
        "current_archetype": "SHADOW_CONTINUATION_AFTER_HEALTH_FAIL",
    })

    try:
        log_trade_event(cur, tid, sym, f"live_exit_shadow_continue_{reason}", price, pnl_percent, current_peak, age_mins, momentum, trend, False)
    except Exception as e:
        print(f"⚠️ probe health fail shadow-continuation event log failed: {e}", flush=True)

    print(
        f"🛡️ PROBE HEALTH FAIL → LIVE EXIT + SHADOW CONTINUE | {sym} | id={tid} | "
        f"{round(pnl_percent,3)}% | peak={round(float(current_peak or 0),3)} | age={round(float(age_mins or 0),1)}m | {reason}",
        flush=True
    )

    send_telegram_alert(
        f"🛡️ <b>PROBE HEALTH FAIL</b> | {sym}\n"
        f"Live probe sold, shadow continuation remains open.\n"
        f"PnL now {fmt_num(pnl_percent)}% | Peak {fmt_num(current_peak)}% | Age {fmt_num(age_mins,1)}m\n"
        f"Reason: {reason}\n"
        f"ID {tid}"
    )

    return True

def maybe_confirm_and_upgrade_bpt_trade(cur, tid, sym, entry_price, opened_at, current_peak, peak_time_minutes, momentum, trend, now, fast_track_reason=None, current_price=None):
    if peak_time_minutes is None:
        peak_time_minutes = 999999

    safe_now = ensure_utc(now)
    safe_opened_at = ensure_utc(opened_at)
    metrics = get_bpt_confirmation_metrics(cur, sym, safe_opened_at, entry_price, safe_now)
    age_mins = (safe_now - safe_opened_at).total_seconds() / 60
    peak_for_confirmation = max(float(current_peak or 0), metrics["peak_window"])

    confirmed = (
        peak_for_confirmation >= BPT_CQE_CONFIRM_PEAK
        and float(peak_time_minutes or 999999) <= BPT_CQE_CONFIRM_WINDOW_MINUTES
        and metrics["avg_trend"] >= BPT_CQE_CONFIRM_AVG_TREND
        and metrics["avg_momentum"] >= BPT_CQE_CONFIRM_AVG_MOMENTUM
        and metrics["signal_count"] >= BPT_CQE_CONFIRM_MIN_SIGNAL_COUNT
    )

    # v6.6.19:
    # CQE confirmation alone is not enough for real scale-in.
    # We also require live market pressure to still be healthy.
    live_ctx = get_live_leadership_pressure_context(cur, sym)

    safe_update_trade_telemetry(cur, tid, {
        "confirmation_peak_30m": peak_for_confirmation,
        "confirmation_avg_trend": metrics["avg_trend"],
        "confirmation_avg_momentum": metrics["avg_momentum"],
        "confirmation_signal_count": metrics["signal_count"],
        "confirmation_age_minutes": age_mins,
    })

    fast_track_active = bool(fast_track_reason)
    if not confirmed and not fast_track_active:
        return False

    cur.execute("""
        SELECT lifecycle_row, upgrade_size_gbp, dynamic_trade_size_gbp, COALESCE(is_shadow, TRUE), trade_size_usdt, probe_size_gbp, size_scaling_reason, market_os_engine
        FROM bot_trades_v4
        WHERE id = %s
    """, (tid,))
    row = cur.fetchone()
    lifecycle_row = row[0] if row else None
    upgrade_size = float(row[1] or get_bpt_row_params(lifecycle_row).get("upgrade_size", 0)) if row else 0
    is_shadow_trade = bool(row[3]) if row else True
    probe_size_gbp = float(row[5] or 0) if row else 0.0
    size_scaling_reason = str(row[6] or "") if row else ""
    market_os_engine = str(row[7] or "") if row and len(row) > 7 else ""
    adaptive_dead_market_shadow_only = (
        market_os_engine == "ADAPTIVE_DEAD_MARKET_LEADER"
        and not ENABLE_ADAPTIVE_DEAD_MARKET_LIVE_UPGRADES
    )
    ghost_probe_active = (probe_size_gbp == 0.0 and size_scaling_reason == GHOST_PROBE_LABEL)
    current_trade_size_usdt = float(row[4] or 0) if row else 0.0

    # v6.6.20:
    # ensure OKX minimum notional compatibility
    upgrade_size = max(
        upgrade_size,
        MIN_OKX_ORDER_NOTIONAL_GBP
    )
    current_dynamic_size = float(row[2]) if row and row[2] is not None else (0.0 if ghost_probe_active else BPT_CQE_PROBE_SIZE_GBP)
    new_dynamic_size = current_dynamic_size + upgrade_size

    live_pressure_30m = safe_float(live_ctx.get("live_pressure_30m"), 0)
    live_delta_30m = safe_float(live_ctx.get("live_delta_30m"), 0)

    row_allows_scalein = lifecycle_row in CQE_REAL_SCALEIN_ALLOWED_ROWS
    confirmation_allows_scalein = (
        fast_track_active
        or (
            live_pressure_30m >= CQE_SCALEIN_MIN_LIVE_PRESSURE
            and live_delta_30m >= CQE_SCALEIN_MIN_DELTA_30M
        )
    )

    coin_profile_for_scalein = get_coin_score_profile(cur, sym)
    coin_allows_live_upgrade = (coin_profile_for_scalein.get("coin_health_mode") == "LIVE")

    scalein_allowed = (
        ENABLE_CQE_REAL_SCALEINS
        and not is_shadow_trade
        and not adaptive_dead_market_shadow_only
        and coin_allows_live_upgrade
        and row_allows_scalein
        and confirmation_allows_scalein
    )

    # v7.4 SCALE-IN VISIBILITY HOTFIX:
    # The old Telegram/log line displayed "Live upgrades: True" even for shadow probes,
    # which made normal shadow confirmations look like failed live scale-ins.
    # This keeps behaviour unchanged but makes the reason explicit.
    if not ENABLE_BPT_CQE_LIVE_UPGRADES:
        scalein_block_reason = "live_upgrade_toggle_off"
    elif adaptive_dead_market_shadow_only:
        scalein_block_reason = "adaptive_dead_market_leader_shadow_only"
    elif not ENABLE_CQE_REAL_SCALEINS:
        scalein_block_reason = "real_scaleins_toggle_off"
    elif not coin_allows_live_upgrade:
        scalein_block_reason = f"coin_profile_{coin_profile_for_scalein.get('tier')}_{coin_profile_for_scalein.get('coin_health_mode')}_no_live_upgrade"
    elif is_shadow_trade:
        scalein_block_reason = "shadow_trade_no_live_scalein"
    elif not row_allows_scalein:
        scalein_block_reason = f"row_not_scalein_allowed:{lifecycle_row}"
    elif not confirmation_allows_scalein:
        scalein_block_reason = (
            f"live_context_below_threshold:"
            f"pressure={round(live_pressure_30m, 4)}<={CQE_SCALEIN_MIN_LIVE_PRESSURE},"
            f"delta={round(live_delta_30m, 4)}<={CQE_SCALEIN_MIN_DELTA_30M}"
        )
    else:
        scalein_block_reason = "scalein_allowed"

    # v7.2.1 ACCOUNTING HOTFIX:
    # Do NOT increase DB trade size/accounting until the OKX live scale-in order
    # has actually succeeded. Previous logic marked the trade as upgraded and
    # increased dynamic_trade_size_gbp before OKX confirmation, which could make
    # pnl_gbp calculate on a phantom upgraded size while OKX only held the probe.
    cur.execute("""
        UPDATE bot_trades_v4
        SET cqe_confirmed = TRUE,
            upgraded_at = NOW()
        WHERE id = %s
    """, (tid,))

    safe_update_trade_telemetry(cur, tid, {
        "probe_lifecycle_state": "FAST_TRACK_CONFIRMED_PENDING_SCALEIN" if fast_track_active else "STANDARD_CONFIRMED_PENDING_SCALEIN",
        "probe_lifecycle_trigger": fast_track_reason or "standard_30m_confirmation",
        "probe_lifecycle_trigger_age_minutes": age_mins,
        "probe_lifecycle_trigger_momentum": momentum,
        "probe_lifecycle_trigger_trend": trend,
    })

    try:
        log_trade_event(cur, tid, sym, "bpt_cqe_confirmed_pending_scalein", entry_price, 0, current_peak, age_mins, momentum, trend, False)
    except Exception as e:
        print(f"⚠️ BPT CQE confirmation trade_events log failed: {e}", flush=True)

    live_scalein_executed = False
    actual_upgrade_size = 0.0
    displayed_dynamic_size = current_dynamic_size

    if ENABLE_BPT_CQE_LIVE_UPGRADES and scalein_allowed:
        okx_upgrade_result = okx_place_market_order(
            cur=cur,
            trade_id=tid,
            symbol=sym,
            direction="LONG",
            action="entry",
            price=current_price or entry_price,
            entry_price=current_price or entry_price,
            trade_size_quote=gbp_to_usdt_quote(upgrade_size),
        )

        live_scalein_executed = bool(
            okx_upgrade_result.get("success")
            and not okx_upgrade_result.get("skipped")
            and not okx_upgrade_result.get("blocked")
            and not okx_upgrade_result.get("dry_run")
        )

        if live_scalein_executed:
            requested_upgrade_quote = gbp_to_usdt_quote(upgrade_size)
            actual_upgrade_quote = float(okx_upgrade_result.get("actual_quote_size") or requested_upgrade_quote or 0)
            actual_upgrade_size = usdt_quote_to_gbp(actual_upgrade_quote)
            new_dynamic_size = current_dynamic_size + actual_upgrade_size
            new_trade_size_usdt = current_trade_size_usdt + actual_upgrade_quote
            displayed_dynamic_size = new_dynamic_size

            cur.execute("""
                UPDATE bot_trades_v4
                SET cqe_upgraded = TRUE
                WHERE id = %s
            """, (tid,))

            ghost_upgrade_reset = {
                "entry_price": current_price or entry_price,
                "peak_pnl_percent": 0,
                "size_scaling_reason": "ghost_probe_upgraded_live_capital_only",
                "shadow_reason": None,
            } if ghost_probe_active else {}

            safe_update_trade_telemetry(cur, tid, {
                **ghost_upgrade_reset,
                "dynamic_trade_size_gbp": new_dynamic_size,
                "trade_size_gbp": new_dynamic_size,
                **accounting_entry_telemetry(new_trade_size_usdt),
                "upgrade_size_gbp": actual_upgrade_size,
                "probe_lifecycle_state": "FAST_TRACK_UPGRADED" if fast_track_active else "STANDARD_UPGRADED",
                "size_scaling_reason": "ghost_probe_upgraded_live_capital_only" if ghost_probe_active else "bpt_live_scalein_executed",
            })
        else:
            safe_update_trade_telemetry(cur, tid, {
                "probe_lifecycle_state": "FAST_TRACK_CONFIRMED_NO_LIVE_SCALEIN" if fast_track_active else "STANDARD_CONFIRMED_NO_LIVE_SCALEIN",
                "size_scaling_reason": f"bpt_live_scalein_not_executed:{okx_upgrade_result.get('reason') or okx_upgrade_result.get('error') or 'unknown'}",
            })
            print(f"⚠️ BPT CQE LIVE UPGRADE ORDER FAILED/SKIPPED | {sym} | id={tid} | {okx_upgrade_result}", flush=True)
    else:
        extra_shadow_tags = {
            "shadow_reason": "ADAPTIVE_DEAD_MARKET_LEADER_SHADOW",
            "size_scaling_reason": "adaptive_dead_market_leader_shadow_only",
        } if adaptive_dead_market_shadow_only else {
            "size_scaling_reason": f"bpt_scalein_disabled_or_not_allowed:{scalein_block_reason}",
        }

        safe_update_trade_telemetry(cur, tid, {
            "probe_lifecycle_state": "FAST_TRACK_CONFIRMED_SCALEIN_DISABLED" if fast_track_active else "STANDARD_CONFIRMED_SCALEIN_DISABLED",
            **extra_shadow_tags,
        })

    trade_mode_label = "🟡 SHADOW COIN" if is_shadow_trade else "🟢 LIVE COIN"
    if live_scalein_executed:
        live_scalein_label = "YES"
    elif is_shadow_trade:
        live_scalein_label = "N/A - SHADOW"
    else:
        live_scalein_label = f"NO - {scalein_block_reason}"

    print(
        f"🚀 BPT CQE CONFIRMED | {sym} | id={tid} | mode={trade_mode_label} | row={lifecycle_row} | "
        f"scalein_allowed={scalein_allowed} | live_scalein={live_scalein_executed} | reason={scalein_block_reason} | "
        f"upgrade={fmt_money(actual_upgrade_size or upgrade_size)} | dynamic={fmt_money(displayed_dynamic_size)} | "
        f"peak={round(peak_for_confirmation,3)}%",
        flush=True
    )

    upgrade_alert_title = "🚀 LIVE UPGRADE EXECUTED" if live_scalein_executed else "👻 SHADOW UPGRADE (NO REAL MONEY)"
    send_telegram_alert(
        f"{upgrade_alert_title} | {sym}\n"
        f"Mode: <b>{trade_mode_label}</b> | Row: <b>{lifecycle_row}</b>\n"
        f"Live scale-in: <b>{live_scalein_label}</b>\n"
        f"Model upgrade target {fmt_money(upgrade_size)} | Actual live added {fmt_money(actual_upgrade_size)} | Total live/model size {fmt_money(displayed_dynamic_size)}\n"
        f"Confirmed peak {fmt_num(peak_for_confirmation)}% | age {fmt_num(age_mins,1)}m\n"
        f"Trigger: {fast_track_reason or 'standard_30m_confirmation'}\n"
        f"Avg T/M {fmt_num(metrics['avg_trend'])} / {fmt_num(metrics['avg_momentum'])}\n"
        f"Live upgrades toggle: {ENABLE_BPT_CQE_LIVE_UPGRADES} | Real scale-in allowed: {scalein_allowed}\n"
        f"Reason: {scalein_block_reason}\n"
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
            COALESCE(is_shadow, TRUE),
            trade_size_usdt
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND entry_quality = %s
          AND symbol = %s
    """, (BPT_CQE_PROBE_SIZE_GBP, BPT_CQE_ENTRY_QUALITY, symbol))

    rows = cur.fetchall()
    for (
        tid, sym, direction, entry_price, opened_at, peak_pnl, peak_time_minutes,
        cqe_confirmed, cqe_upgraded, lifecycle_row, dynamic_size,
        trail_activation, trail_drawdown, is_shadow, trade_size_usdt
    ) in rows:
        if direction != "LONG" or not entry_price:
            continue

        safe_now = ensure_utc(now)
        safe_opened_at = ensure_utc(opened_at)

        pnl_percent = ((price - entry_price) / entry_price) * 100
        mins = (safe_now - safe_opened_at).total_seconds() / 60
        opened_at = safe_opened_at
        now = safe_now
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
            lifecycle_delta = get_latest_probe_lifecycle_delta(cur, sym, opened_at, now)
            lifecycle_state, lifecycle_trigger = classify_probe_lifecycle_state(
                mins,
                momentum,
                trend,
                lifecycle_delta,
            )
            mark_probe_lifecycle_state(
                cur,
                tid,
                lifecycle_state,
                lifecycle_trigger,
                mins,
                momentum,
                trend,
                lifecycle_delta,
            )

            if lifecycle_state == "DEAD" and not is_shadow:
                exit_live_probe_and_continue_shadow(
                    cur=cur,
                    tid=tid,
                    sym=sym,
                    direction=direction,
                    entry_price=entry_price,
                    price=price,
                    pnl_percent=pnl_percent,
                    current_peak=current_peak,
                    age_mins=mins,
                    momentum=momentum,
                    trend=trend,
                    dynamic_size=dynamic_size,
                    reason=lifecycle_trigger,
                    trade_size_usdt=trade_size_usdt,
                )
                is_shadow = True
                continue

            fast_track_reason = None
            if lifecycle_state == "MONSTER":
                fast_track_reason = lifecycle_trigger
            elif lifecycle_state == "LEADER":
                fast_track_reason = lifecycle_trigger

            maybe_confirm_and_upgrade_bpt_trade(
                cur, tid, sym, entry_price, opened_at, current_peak,
                peak_time_minutes, momentum, trend, now,
                fast_track_reason=fast_track_reason,
                current_price=price
            )

        # Reload upgrade state after possible upgrade.
        cur.execute("""
            SELECT COALESCE(cqe_upgraded, FALSE), lifecycle_row,
                   COALESCE(dynamic_trade_size_gbp, trade_size_gbp, %s),
                   COALESCE(lifecycle_trail_activation, %s),
                   COALESCE(lifecycle_trail_drawdown, %s),
                   trade_size_usdt
            FROM bot_trades_v4
            WHERE id = %s
        """, (BPT_CQE_PROBE_SIZE_GBP, trail_activation, trail_drawdown, tid))
        state = cur.fetchone()
        if state:
            cqe_upgraded, lifecycle_row, dynamic_size, trail_activation, trail_drawdown, trade_size_usdt = state

        close_reason = None
        exit_architecture = None
        drawdown_from_peak = current_peak - pnl_percent

        if should_trend_persistence_exit(BPT_CQE_ENTRY_QUALITY, mins, trend):
            close_reason = trend_persistence_exit_reason(BPT_CQE_ENTRY_QUALITY)
            exit_architecture = "TREND_PERSISTENCE_EXIT"

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

        if not close_reason and ENABLE_ARCHETYPE_STATE_ENGINE and mins >= ARCH_NO_PROGRESS_MINUTES and current_peak < ARCH_NO_PROGRESS_PEAK:
            close_reason = "arch_no_progress_120m"
            exit_architecture = "ARCHETYPE_STATE_NO_PROGRESS"

        if not close_reason:
            if cqe_upgraded:
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
            bpt_size_usdt_for_pnl = float(trade_size_usdt or gbp_to_usdt_quote(float(dynamic_size or BPT_CQE_PROBE_SIZE_GBP)))
            accounting = accounting_exit_values(bpt_size_usdt_for_pnl, pnl_percent)
            pnl_gbp = accounting["pnl_gbp"]

            cur.execute("""
                UPDATE bot_trades_v4
                SET status = 'CLOSED',
                    closed_at = NOW(),
                    close_price = %s,
                    pnl_percent = %s,
                    pnl_gbp = %s,
                    pnl_usdt = %s,
                    exit_value_usdt = %s,
                    exit_value_gbp = %s,
                    usd_gbp_rate = %s,
                    close_reason = %s,
                    telegram_close_alert_sent = FALSE
                WHERE id = %s
                  AND status = 'OPEN'
                RETURNING id
            """, (
                price,
                pnl_percent,
                pnl_gbp,
                accounting["pnl_usdt"],
                accounting["exit_value_usdt"],
                accounting["exit_value_gbp"],
                accounting["usd_gbp_rate"],
                close_reason,
                tid,
            ))
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

            try:
                refresh_coin_learning_from_history(cur, sym, allow_mode_change=True)
            except Exception as e:
                print(f"⚠️ coin learning refresh after BPT close failed: {e}", flush=True)

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
                    trade_size_quote=bpt_size_usdt_for_pnl,
                )
            elif not is_shadow:
                log_okx_exit_skip_no_live_entry(cur, tid, sym, direction, price)

            print(
                f"💰 CLOSED BPT CQE | {sym} | {round(pnl_percent,3)}% | "
                f"peak={round(current_peak,3)} | dd={round(drawdown_from_peak,3)} | {close_reason}",
                flush=True
            )

            send_telegram_alert(
                f"{'👻 GHOST EXIT' if is_shadow else '💰 LIVE EXIT'}\n🧪 <b>BPT_CQE_LIFECYCLE_V1</b> | {sym}\n"
                f"Row: {lifecycle_row} | " + format_trade_pnl_lines(pnl_percent, pnl_gbp, 0) + "\n"
                + f"Peak {fmt_num(current_peak)}% | DD {fmt_num(drawdown_from_peak)}%\n"
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

    apply_market_heat_to_trade(cur, tid)

    try:
        safe_update_trade_telemetry(cur, tid, accounting_entry_telemetry(PH_SHADOW_SIZE_GBP))
    except Exception as e:
        print(f"⚠️ v7.1 PH entry accounting telemetry failed | {symbol} | id={tid} | {e}", flush=True)
        safe_telemetry_rollback(cur)

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
            accounting = accounting_exit_values(float(dynamic_size or PH_SHADOW_SIZE_GBP), pnl_percent)
            pnl_gbp = accounting["pnl_gbp"]
            cur.execute("""
                UPDATE bot_trades_v4
                SET status='CLOSED',
                    closed_at=NOW(),
                    close_price=%s,
                    pnl_percent=%s,
                    pnl_gbp=%s,
                    pnl_usdt=%s,
                    exit_value_usdt=%s,
                    exit_value_gbp=%s,
                    usd_gbp_rate=%s,
                    close_reason=%s
                WHERE id=%s
            """, (
                price, pnl_percent, pnl_gbp,
                accounting["pnl_usdt"], accounting["exit_value_usdt"], accounting["exit_value_gbp"],
                accounting["usd_gbp_rate"], close_reason, tid
            ))
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


def calculate_unicorn_pressure_score(leadership_context, momentum, trend):
    """
    v6.6.21 FREE THE UNICORNS:
    Positive-only pressure formula matching the research sweeps that found:
    Leadership 2-3 + Pressure >= 7 + CQE >= 4 as the strongest expansion predictor.
    """
    ctx = leadership_context or {}

    leadership_score = safe_float(
        first_non_empty(ctx.get("leadership_score"), ctx.get("prior_avg_peak"), default=0),
        0
    )
    delta_30m = safe_float(
        first_non_empty(ctx.get("leadership_delta_30m"), ctx.get("delta_30m"), default=0),
        0
    )
    cqe_score = safe_float(ctx.get("cqe_quality_score"), 0)
    momentum_f = safe_float(momentum, 0)
    trend_f = safe_float(trend, 0)

    pressure = 0

    if leadership_score >= 3:
        pressure += 3
    elif leadership_score >= 2:
        pressure += 2
    elif leadership_score >= 1:
        pressure += 1

    if delta_30m > 0.25:
        pressure += 2
    elif delta_30m > 0:
        pressure += 1

    if cqe_score >= 5:
        pressure += 2
    elif cqe_score >= 4:
        pressure += 1

    if momentum_f > 0.7:
        pressure += 2
    elif momentum_f > 0.3:
        pressure += 1

    if trend_f >= 0.20:
        pressure += 2
    elif trend_f >= 0.16:
        pressure += 1

    return pressure


def is_unicorn_entry_candidate(leadership_context, momentum, trend):
    """
    Returns (is_candidate, pressure_score, reason).
    This is intentionally strict and only defines the candidate;
    it does not decide which legacy block reasons are allowed to be overridden.
    """
    if not ENABLE_UNICORN_ENTRY_OVERRIDE:
        return False, 0, "unicorn_override_disabled"

    ctx = leadership_context or {}

    leadership_score = safe_float(
        first_non_empty(ctx.get("leadership_score"), ctx.get("prior_avg_peak"), default=0),
        0
    )
    cqe_score = safe_float(ctx.get("cqe_quality_score"), 0)
    pressure_score = calculate_unicorn_pressure_score(ctx, momentum, trend)

    if leadership_score < UNICORN_MIN_LEADERSHIP_SCORE:
        return False, pressure_score, "unicorn_leadership_too_low"

    if leadership_score >= UNICORN_MAX_LEADERSHIP_SCORE:
        return False, pressure_score, "unicorn_leadership_too_mature"

    if cqe_score < UNICORN_MIN_CQE_SCORE:
        return False, pressure_score, "unicorn_cqe_too_low"

    if pressure_score < UNICORN_MIN_PRESSURE_SCORE:
        return False, pressure_score, "unicorn_pressure_too_low"

    return True, pressure_score, "unicorn_candidate"


def apply_unicorn_entry_override(leadership_context, momentum, trend, original_block_reason):
    """
    Medium-risk live version:
    Only parole the two tested Unicorn subclasses:
    - leadership_climax_delta_blocked
    - max_same_symbol_open

    Does not override trend_too_low, phase_not_tradable, max_open_trades,
    OKX position guard, control panel, or exchange tradability.
    """
    ctx = leadership_context or {}
    candidate, pressure_score, candidate_reason = is_unicorn_entry_candidate(ctx, momentum, trend)

    ctx["unicorn_candidate"] = candidate
    ctx["unicorn_pressure_score"] = pressure_score
    ctx["unicorn_candidate_reason"] = candidate_reason
    ctx["unicorn_original_block_reason"] = original_block_reason

    if not candidate:
        return False, ctx, candidate_reason

    if original_block_reason not in UNICORN_OVERRIDE_REASONS:
        return False, ctx, "unicorn_block_reason_not_paroled"

    ctx["unicorn_override_triggered"] = True
    ctx["unicorn_override_reason"] = original_block_reason
    ctx["market_os_engine"] = "UNICORN_OVERRIDE"
    ctx["size_scaling_reason"] = f"unicorn_override_{original_block_reason}"

    return True, ctx, f"unicorn_override_{original_block_reason}"


def passes_leadership_engine(cur, symbol, momentum, trend):
    if not ENABLE_LEADERSHIP_ENGINE:
        return False, None, "leadership_engine_disabled"

    # v6.1.6: always fetch context first so blocked early signals still get lifecycle telemetry.
    leadership = get_leadership_context(cur, symbol)

    required_trend = LEADERSHIP_MIN_TREND

    # =========================
    # v6.6.20 ADAPTIVE LEADERSHIP THRESHOLDS
    # =========================
    if ENABLE_ADAPTIVE_LEADERSHIP_TREND:

        leadership_score_signal = safe_float(
            leadership.get("leadership_score", 0),
            0
        )

        cqe_score_signal = safe_float(
            leadership.get("cqe_quality_score", 0),
            0
        )

        lifecycle_phase_signal = str(
            leadership.get("lifecycle_phase", "")
        ).upper()

        if leadership_score_signal >= ADAPTIVE_LEADERSHIP_SCORE_THRESHOLD:
            required_trend = min(
                required_trend,
                ADAPTIVE_LEADER_MIN_TREND
            )

        if cqe_score_signal >= ADAPTIVE_CQE_SCORE_THRESHOLD:
            required_trend = min(
                required_trend,
                ADAPTIVE_CQE_MIN_TREND
            )

        if lifecycle_phase_signal in [
            "STABLE_LEADER",
            "IGNITION",
            "EARLY_EXPANSION"
        ]:
            required_trend = min(
                required_trend,
                ADAPTIVE_STABLE_LEADER_MIN_TREND
            )

    if trend < required_trend:
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
    """Return USDT quote size for OKX from the configured GBP model size."""
    return gbp_to_usdt_quote(get_trade_size_for_quality(entry_quality))


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

    # v6.6.18: augment static structural score with live signal pressure.
    live_ctx = get_live_leadership_pressure_context(cur, symbol)
    live_pressure_30m = live_ctx.get("live_pressure_30m")
    live_delta_30m = live_ctx.get("live_delta_30m")
    if live_pressure_30m is not None:
        score = live_pressure_30m
        score_30 = live_ctx.get("live_pressure_60m")
        delta_30 = live_delta_30m
        delta_15 = live_ctx.get("live_delta_15m")
        delta_5 = live_ctx.get("live_pressure_latest")
        # Keep score_60 as pressure_60 for telemetry meaning.
        score_60 = live_ctx.get("live_pressure_60m")

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
        "live_pressure_phase": live_ctx.get("live_pressure_phase") if 'live_ctx' in locals() else None,
        "live_pressure_30m": live_ctx.get("live_pressure_30m") if 'live_ctx' in locals() else None,
        "live_pressure_delta_30m": live_ctx.get("live_delta_30m") if 'live_ctx' in locals() else None,
        "live_signal_timestamp": live_ctx.get("live_signal_timestamp") if 'live_ctx' in locals() else None,
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
        cur.execute(f"""
            WITH scored AS (
                SELECT
                    symbol,
                    timestamp,
                    momentum,
                    trend,
                    decision,
                    {live_pressure_sql_expr()} AS live_pressure
                FROM signals_raw
                WHERE timestamp >= NOW() - INTERVAL '90 minutes'
            ),
            latest AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    timestamp,
                    momentum,
                    trend,
                    decision,
                    live_pressure
                FROM scored
                ORDER BY symbol, timestamp DESC
            ),
            agg AS (
                SELECT
                    symbol,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '5 minutes') AS p5,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '15 minutes') AS p15,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS p30,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '60 minutes') AS p60,
                    COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS signals_30m
                FROM scored
                GROUP BY symbol
            ),
            combined AS (
                SELECT
                    l.symbol,
                    l.timestamp,
                    l.live_pressure,
                    l.decision,
                    a.p15,
                    a.p30,
                    a.p60,
                    (a.p15 - a.p60) AS live_delta,
                    a.signals_30m
                FROM latest l
                JOIN agg a USING(symbol)
            )
            SELECT
                symbol,
                CASE
                    WHEN p30 >= 0.75 AND live_delta > 0 THEN 'EXPANDING'
                    WHEN p30 >= 0.75 THEN 'STRONG_FLAT'
                    WHEN p30 >= 0.35 AND live_delta > 0 THEN 'ROTATION_UP'
                    WHEN p30 >= 0.35 THEN 'WATCH'
                    WHEN p30 < 0 THEN 'BEARISH'
                    ELSE 'WEAK'
                END AS lifecycle_phase,
                ROUND(p30::numeric, 3) AS pressure_30m,
                ROUND(live_delta::numeric, 3) AS delta_30m,
                ROUND(live_pressure::numeric, 3) AS latest_pressure,
                decision,
                signals_30m
            FROM combined
            ORDER BY
                CASE
                    WHEN p30 >= 0.75 AND live_delta > 0 THEN 1
                    WHEN p30 >= 0.75 THEN 2
                    WHEN p30 >= 0.35 AND live_delta > 0 THEN 3
                    WHEN p30 >= 0.35 THEN 4
                    WHEN p30 < 0 THEN 6
                    ELSE 5
                END,
                p30 DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        if not rows:
            return "Lifecycle: n/a"
        lines = []
        for sym, phase, p30, delta, latest_pressure, decision, sigs in rows:
            icon = {
                "EXPANDING": "🚀",
                "STRONG_FLAT": "✅",
                "ROTATION_UP": "👀",
                "WATCH": "⚪",
                "BEARISH": "📉",
                "WEAK": "⚪",
            }.get(phase, "⚪")
            lines.append(f"{icon} {sym} {phase} | P30 {p30} | Δ{delta} | latest {latest_pressure} | {decision or 'NONE'} | sigs{sigs}")
        return "\n".join(lines)
    except Exception as e:
        print(f"⚠️ live pressure lifecycle dashboard failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
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
        cur.execute(f"""
            WITH scored AS (
                SELECT
                    symbol,
                    timestamp,
                    momentum,
                    trend,
                    decision,
                    {live_pressure_sql_expr()} AS live_pressure
                FROM signals_raw
                WHERE timestamp >= NOW() - INTERVAL '90 minutes'
            ),
            latest AS (
                SELECT DISTINCT ON (symbol)
                    symbol,
                    timestamp,
                    momentum,
                    trend,
                    decision,
                    live_pressure
                FROM scored
                ORDER BY symbol, timestamp DESC
            ),
            agg AS (
                SELECT
                    symbol,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS pressure_30m,
                    AVG(live_pressure) FILTER (WHERE timestamp >= NOW() - INTERVAL '60 minutes') AS pressure_60m,
                    COUNT(*) FILTER (WHERE timestamp >= NOW() - INTERVAL '30 minutes') AS signals_30m
                FROM scored
                GROUP BY symbol
            )
            SELECT
                l.symbol,
                ROUND(l.live_pressure::numeric, 3) AS latest_pressure,
                ROUND(a.pressure_30m::numeric, 3) AS pressure_30m,
                ROUND((a.pressure_30m - a.pressure_60m)::numeric, 3) AS live_delta,
                l.decision,
                a.signals_30m
            FROM latest l
            JOIN agg a USING(symbol)
            ORDER BY a.pressure_30m DESC NULLS LAST
            LIMIT %s
        """, (limit,))
        rows = cur.fetchall()
        if not rows:
            return "Top live pressure leaders: n/a"
        parts = []
        for symbol, latest_pressure, pressure_30m, live_delta, decision, sigs in rows:
            icon = "✅" if (pressure_30m or 0) >= 0.75 and (live_delta or 0) > 0 else ("👀" if (pressure_30m or 0) >= 0.35 else "⚪")
            parts.append(f"{icon} {symbol} P30 {pressure_30m} | Δ{live_delta} | latest {latest_pressure} | {decision or 'NONE'} | sigs{sigs}")
        return "\n".join(parts)
    except Exception as e:
        print(f"⚠️ top live pressure leaders lookup failed: {e}", flush=True)
        safe_telemetry_rollback(cur)
        return "Top live pressure leaders: n/a"

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
            COALESCE(b.leadership_score, b.leadership_prior_avg_peak, 0) AS entry_leadership,
            b.leadership_rank_at_entry,
            b.leadership_age_minutes_at_entry,
            b.leadership_delta_30m_at_entry,
            b.lifecycle_phase_at_entry,
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
        entry_leadership, entry_rank, entry_leadership_age, entry_delta_30m,
        entry_lifecycle_phase, trade_size_gbp, current_price, latest_momentum,
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
            f"Entry score {fmt_num(entry_leadership)} | Entry age {fmt_num(entry_leadership_age,1)}m | "
            f"Entry Δ30 {fmt_num(entry_delta_30m)} | Entry rank #{entry_rank or 'n/a'}\n"
            f"Entry phase {short_phase(entry_lifecycle_phase)} | Now {format_leadership_compact(lifecycle_ctx)}\n"
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
                    l.symbol,
                    l.snapshot_time,
                    l.leadership_score,
                    l.avg_peak,
                    l.avg_worst,
                    l.leadership_mode,
                    RANK() OVER (ORDER BY l.leadership_score DESC NULLS LAST) AS leadership_rank,
                    COUNT(*) FILTER (WHERE l.leadership_score >= 2.0) OVER () AS leader_count_2p0,
                    COUNT(*) FILTER (WHERE l.leadership_score >= 1.25) OVER () AS leader_count_1p25,
                    AVG(l.leadership_score) OVER () AS market_leadership_quality,
                    SUM(l.leadership_score) OVER () AS total_leadership_score
                FROM leadership_state_history l
                JOIN latest_snapshot x
                  ON x.snapshot_time = l.snapshot_time
            )
            SELECT
                ranked.symbol,
                ranked.snapshot_time,
                ranked.leadership_score,
                ranked.avg_peak,
                ranked.avg_worst,
                ranked.leadership_mode,
                ranked.leadership_rank,
                ranked.leader_count_2p0,
                ranked.leader_count_1p25,
                ranked.market_leadership_quality,
                CASE
                    WHEN ranked.total_leadership_score > 0
                    THEN ranked.leadership_score / ranked.total_leadership_score
                    ELSE NULL
                END AS top3_concentration
            FROM ranked
            WHERE ranked.symbol = %s
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

        # v6.6.18: use live signal pressure for velocity/phase when available.
        live_ctx = get_live_leadership_pressure_context(cur, symbol)
        if live_ctx.get("live_pressure_30m") is not None:
            leadership_score_f = safe_float(live_ctx.get("live_pressure_30m"), leadership_score_f)

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
        if 'live_ctx' in locals() and live_ctx.get("live_delta_30m") is not None:
            leadership_velocity = safe_float(live_ctx.get("live_delta_30m"), leadership_velocity)

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
            "live_pressure_phase": live_ctx.get("live_pressure_phase") if 'live_ctx' in locals() else None,
            "live_pressure_30m": live_ctx.get("live_pressure_30m") if 'live_ctx' in locals() else None,
            "live_pressure_delta_30m": live_ctx.get("live_delta_30m") if 'live_ctx' in locals() else None,
            "live_signal_timestamp": live_ctx.get("live_signal_timestamp") if 'live_ctx' in locals() else None,
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

def get_coin_health_snapshot(cur, symbol):
    """v8.4 coin gate. Persistent coin_scores tier/mode is checked before legacy expansion health."""
    default = {
        "coin_health_mode": "LIVE",
        "coin_health_reason": "insufficient_history",
        "coin_dead_streak": 0,
        "coin_recovery_streak": 0,
        "coin_health_sample_size": 0,
        "coin_health_avg_peak": None,
        "coin_health_dead_rate": None,
    }

    if not ENABLE_COIN_HEALTH_ENGINE:
        default["coin_health_reason"] = "disabled"
        return default

    try:
        profile = get_coin_score_profile(cur, symbol)
        mode = (profile.get("coin_health_mode") or "LIVE").upper()
        if mode in ("DISABLED", "RETIRED"):
            return {**default, "coin_health_mode": "DISABLED", "coin_health_reason": f"coin_profile_{profile.get('tier')}_disabled", "coin_profile": profile}
        if mode in ("SHADOW", "SHADOW_ONLY"):
            return {**default, "coin_health_mode": "SHADOW", "coin_health_reason": f"coin_profile_{profile.get('tier')}_shadow_only", "coin_profile": profile}
        default["coin_profile"] = profile
    except Exception as e:
        print(f"⚠️ coin profile pre-gate failed for {symbol}: {e}", flush=True)
        safe_telemetry_rollback(cur)

    try:
        limit_n = max(COIN_HEALTH_DEAD_WINDOW, COIN_HEALTH_RECOVERY_WINDOW)
        cur.execute("""
            SELECT
                id,
                COALESCE(peak_pnl_percent, 0) AS peak_pnl_percent,
                COALESCE(is_shadow, FALSE) AS is_shadow,
                COALESCE(closed_at, opened_at) AS resolved_at,
                close_reason
            FROM bot_trades_v4
            WHERE symbol = %s
              AND status = 'CLOSED'
              AND COALESCE(closed_at, opened_at) >= NOW() - (%s || ' hours')::INTERVAL
              AND peak_pnl_percent IS NOT NULL
            ORDER BY COALESCE(closed_at, opened_at) DESC
            LIMIT %s
        """, (symbol, COIN_HEALTH_LOOKBACK_HOURS, limit_n))
        rows = cur.fetchall() or []

        peaks = [float(r[1] or 0) for r in rows]
        sample_size = len(peaks)
        if sample_size == 0:
            return default

        avg_peak = sum(peaks) / sample_size if sample_size else None
        dead_count = sum(1 for x in peaks[:COIN_HEALTH_DEAD_WINDOW] if x < COIN_HEALTH_DEAD_PEAK_PCT)
        dead_rate = (dead_count / min(sample_size, COIN_HEALTH_DEAD_WINDOW) * 100.0) if sample_size else None
        recovery_count = sum(1 for x in peaks[:COIN_HEALTH_RECOVERY_WINDOW] if x >= COIN_HEALTH_RECOVERY_PEAK_PCT)

        mode = "LIVE"
        reason = "healthy"

        if sample_size >= COIN_HEALTH_RECOVERY_WINDOW and recovery_count >= COIN_HEALTH_RECOVERY_THRESHOLD:
            mode = "LIVE"
            reason = f"recovered_{recovery_count}_of_{COIN_HEALTH_RECOVERY_WINDOW}_expanding"
        elif sample_size >= COIN_HEALTH_DEAD_WINDOW and dead_count >= COIN_HEALTH_DEAD_THRESHOLD:
            mode = "SHADOW"
            reason = f"dead_{dead_count}_of_{COIN_HEALTH_DEAD_WINDOW}_peak_under_{COIN_HEALTH_DEAD_PEAK_PCT}"
        elif sample_size < COIN_HEALTH_DEAD_WINDOW:
            reason = "insufficient_history"

        return {
            "coin_health_mode": mode,
            "coin_health_reason": reason,
            "coin_dead_streak": int(dead_count),
            "coin_recovery_streak": int(recovery_count),
            "coin_health_sample_size": int(sample_size),
            "coin_health_avg_peak": avg_peak,
            "coin_health_dead_rate": dead_rate,
        }
    except Exception as e:
        print(f"⚠️ coin health lookup failed for {symbol}: {e}", flush=True)
        default["coin_health_reason"] = "lookup_error"
        return default


def coin_health_trade_telemetry(snapshot):
    snapshot = snapshot or {}
    return {
        "coin_health_mode": snapshot.get("coin_health_mode"),
        "coin_health_reason": snapshot.get("coin_health_reason"),
        "coin_dead_streak": snapshot.get("coin_dead_streak"),
        "coin_recovery_streak": snapshot.get("coin_recovery_streak"),
        "coin_health_sample_size": snapshot.get("coin_health_sample_size"),
        "coin_health_avg_peak": snapshot.get("coin_health_avg_peak"),
        "coin_health_dead_rate": snapshot.get("coin_health_dead_rate"),
    }


def market_context_trade_telemetry(ctx):
    """Best-effort v7.6 logging repair. Uses safe_update_trade_telemetry, so missing columns are harmless."""
    ctx = ctx or {}
    median_ctx = ctx.get("market_median_peak_context")
    lifecycle_state = ctx.get("market_lifecycle_state")
    lifecycle_window = ctx.get("market_lifecycle_window")
    return {
        "global_avg_peak": median_ctx,
        "global_regime": lifecycle_window or lifecycle_state,
        "market_median_peak_context": median_ctx,
        "market_median_peak_context_at_entry": median_ctx,
        "market_breadth": ctx.get("market_breadth"),
        "market_breadth_at_entry": ctx.get("market_breadth"),
        "market_breadth_accel": ctx.get("market_breadth_accel"),
        "market_breadth_accel_at_entry": ctx.get("market_breadth_accel"),
        "market_lifecycle_state": lifecycle_state,
        "market_lifecycle_state_at_entry": lifecycle_state,
        "market_lifecycle_movement": ctx.get("market_lifecycle_movement"),
        "market_lifecycle_movement_at_entry": ctx.get("market_lifecycle_movement"),
        "market_lifecycle_window": lifecycle_window,
        "market_lifecycle_window_at_entry": lifecycle_window,
        "market_near_count_at_entry": ctx.get("market_near_count"),
        "market_core_count_at_entry": ctx.get("market_core_count"),
        "market_aggressive_count_at_entry": ctx.get("market_aggressive_count"),
        "market_monster_count_at_entry": ctx.get("market_monster_count"),
    }


def open_coin_health_shadow_trade(cur, symbol, direction, price, momentum, trend, signal_id, signal_time, leadership_context, health_snapshot):
    if not ENABLE_SHADOWS:
        print(f"🩺 COIN HEALTH SHADOW SKIPPED | {symbol} | shadows disabled", flush=True)
        return None

    try:
        cur.execute("""
            SELECT COUNT(*)
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND symbol = %s
              AND COALESCE(is_shadow, FALSE) = TRUE
              AND entry_quality = %s
        """, (symbol, COIN_HEALTH_SHADOW_ENTRY_QUALITY))
        existing = cur.fetchone()[0] or 0
        if existing >= 1:
            print(f"🩺 COIN HEALTH SHADOW ALREADY OPEN | {symbol} | skipping duplicate", flush=True)
            return None
    except Exception as e:
        print(f"⚠️ coin health duplicate-shadow check failed for {symbol}: {e}", flush=True)

    shadow_reason = (health_snapshot or {}).get("coin_health_reason") or "coin_health_shadow"
    ctx = dict(leadership_context or {})
    ctx["market_os_engine"] = COIN_HEALTH_SHADOW_ENTRY_QUALITY

    trade_id = open_shadow_market_os_trade(
        cur,
        symbol,
        direction,
        price,
        momentum,
        trend,
        COIN_HEALTH_SHADOW_ENTRY_QUALITY,
        signal_id,
        signal_time,
        ctx,
        shadow_reason,
        model_size_gbp=COIN_HEALTH_SHADOW_SIZE_GBP,
    )

    if trade_id:
        safe_update_trade_telemetry(cur, trade_id, {
            **coin_health_trade_telemetry(health_snapshot),
            **market_context_trade_telemetry(ctx),
            "shadow_reason": f"COIN_HEALTH:{shadow_reason}",
            "entry_block_reason": f"coin_health_{shadow_reason}",
        })
        try:
            cur.connection.commit()
        except Exception:
            pass

        if ENABLE_COIN_HEALTH_TELEGRAM:
            try:
                send_telegram_alert(
                    f"🩺 <b>COIN HEALTH SHADOW</b> | {symbol}\n"
                    f"Reason: {shadow_reason}\n"
                    f"Dead {health_snapshot.get('coin_dead_streak')}/{COIN_HEALTH_DEAD_WINDOW} | "
                    f"Recovery {health_snapshot.get('coin_recovery_streak')}/{COIN_HEALTH_RECOVERY_WINDOW}\n"
                    f"Avg peak {fmt_num(health_snapshot.get('coin_health_avg_peak'), 3)} | "
                    f"Dead rate {fmt_num(health_snapshot.get('coin_health_dead_rate'), 1)}%\n"
                    "Real capital blocked; shadow observation opened."
                )
            except Exception as e:
                print(f"⚠️ coin health telegram failed: {e}", flush=True)

    return trade_id


def open_trade(cur, symbol, direction, price, momentum, trend, quality,
               signal_id, signal_time, leadership_context):
    entry_snapshot = build_entry_leadership_snapshot(quality, leadership_context)

    cur.execute("""
        INSERT INTO bot_trades_v4 (
            symbol, direction, entry_price, status, opened_at, data_version,
            momentum_strength, trend_strength, entry_quality, peak_pnl_percent,
            signal_id, signal_timestamp,
            trade_size_gbp, dynamic_trade_size_gbp,
            leadership_prior_successes, leadership_prior_runners, leadership_prior_avg_peak,
            leadership_tier, leadership_mode, leadership_score,
            lifecycle_phase_at_entry, prior_lifecycle_phase_at_entry, leadership_transition_at_entry,
            leadership_delta_5m_at_entry, leadership_delta_15m_at_entry, leadership_delta_30m_at_entry,
            leadership_delta_60m_at_entry, leadership_score_30m_ago_at_entry,
            leadership_age_minutes_at_entry, leadership_peak_score_last_4h_at_entry, leadership_rank_at_entry,
            market_near_count_at_entry, market_core_count_at_entry, market_aggressive_count_at_entry,
            market_monster_count_at_entry, shadow_emergence_detected_at_entry, shadow_emergence_reason_at_entry,
            is_shadow
        )
        VALUES (
            %s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,0,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            FALSE
        )
        RETURNING id
    """, (
        symbol, direction, price, DATA_VERSION, momentum, trend, quality, signal_id, signal_time,
        entry_snapshot.get("trade_size_gbp"), entry_snapshot.get("dynamic_trade_size_gbp"),
        entry_snapshot.get("leadership_prior_successes"), entry_snapshot.get("leadership_prior_runners"),
        entry_snapshot.get("leadership_prior_avg_peak"), entry_snapshot.get("leadership_tier"),
        entry_snapshot.get("leadership_mode"), entry_snapshot.get("leadership_score"),
        entry_snapshot.get("lifecycle_phase_at_entry"), entry_snapshot.get("prior_lifecycle_phase_at_entry"),
        entry_snapshot.get("leadership_transition_at_entry"), entry_snapshot.get("leadership_delta_5m_at_entry"),
        entry_snapshot.get("leadership_delta_15m_at_entry"), entry_snapshot.get("leadership_delta_30m_at_entry"),
        entry_snapshot.get("leadership_delta_60m_at_entry"), entry_snapshot.get("leadership_score_30m_ago_at_entry"),
        entry_snapshot.get("leadership_age_minutes_at_entry"), entry_snapshot.get("leadership_peak_score_last_4h_at_entry"),
        entry_snapshot.get("leadership_rank_at_entry"), entry_snapshot.get("market_near_count_at_entry"),
        entry_snapshot.get("market_core_count_at_entry"), entry_snapshot.get("market_aggressive_count_at_entry"),
        entry_snapshot.get("market_monster_count_at_entry"), entry_snapshot.get("shadow_emergence_detected_at_entry"),
        entry_snapshot.get("shadow_emergence_reason_at_entry"),
    ))

    trade_id = cur.fetchone()[0]

    apply_market_heat_to_trade(cur, trade_id)

    try:
        cur.connection.commit()
        print(
            f"✅ REAL ENTRY SNAPSHOT PERSISTED FIRST | {symbol} | id={trade_id} | "
            f"score={entry_snapshot.get('leadership_score')} | rank={entry_snapshot.get('leadership_rank_at_entry')} | "
            f"age={entry_snapshot.get('leadership_age_minutes_at_entry')} | d30={entry_snapshot.get('leadership_delta_30m_at_entry')}",
            flush=True
        )
    except Exception as e:
        print(f"🚨 REAL ENTRY SNAPSHOT EARLY COMMIT FAILED | {symbol} | id={trade_id} | {e}", flush=True)
        raise

    try:
        entry_optional_telemetry = {
            "market_os_engine": entry_snapshot.get("market_os_engine"),
            "size_scaling_reason": entry_snapshot.get("size_scaling_reason"),
            **accounting_entry_telemetry(gbp_to_usdt_quote(entry_snapshot.get("dynamic_trade_size_gbp") or entry_snapshot.get("trade_size_gbp"))),
        }
        entry_optional_telemetry.update(lifecycle_trade_telemetry_from_context(leadership_context))
        entry_optional_telemetry.update(market_context_trade_telemetry(leadership_context))
        entry_optional_telemetry.update(coin_health_trade_telemetry((leadership_context or {}).get("coin_health_snapshot") or {"coin_health_mode": "LIVE", "coin_health_reason": "live_entry"}))
        safe_update_trade_telemetry(cur, trade_id, entry_optional_telemetry)
        cur.connection.commit()
    except Exception as e:
        print(f"⚠️ optional entry telemetry update failed after snapshot persisted: {e}", flush=True)
        safe_telemetry_rollback(cur)

    try:
        log_trade_event(cur, trade_id, symbol, "entry", price, 0, 0, 0, momentum, trend, True)
        cur.connection.commit()
    except Exception as e:
        print(f"⚠️ trade_events entry log failed after snapshot persisted: {e}", flush=True)
        safe_telemetry_rollback(cur)

    try:
        update_trade_telemetry_v1(cur, trade_id, symbol, opened_at=signal_time, peak_pnl_percent=0, is_exit=False)
        cur.connection.commit()
    except Exception as e:
        print(f"⚠️ TELEMETRY entry update failed after snapshot persisted: {e}", flush=True)
        safe_telemetry_rollback(cur)

    try:
        cur.execute("""
            SELECT COALESCE(leadership_score,0), leadership_rank_at_entry,
                   leadership_age_minutes_at_entry, leadership_delta_30m_at_entry
            FROM bot_trades_v4
            WHERE id = %s
        """, (str(trade_id),))
        verify_row = cur.fetchone()
        if not verify_row:
            raise Exception("base_trade_row_not_visible_after_snapshot_commit")
        cur.connection.commit()
        print(f"✅ REAL ENTRY SNAPSHOT VERIFIED | {symbol} | id={trade_id} | score={verify_row[0]} | rank={verify_row[1]} | age={verify_row[2]} | d30={verify_row[3]}", flush=True)
    except Exception as e:
        print(f"🚨 REAL ENTRY SNAPSHOT VERIFY FAILED | {symbol} | id={trade_id} | {e}", flush=True)
        safe_telemetry_rollback(cur)
        raise

    return trade_id


def open_shadow_market_os_trade(cur, symbol, direction, price, momentum, trend, quality,
                                signal_id, signal_time, leadership_context, shadow_reason,
                                model_size_gbp=10.0):
    """v6.9.2: paper/shadow version of live leadership/ROT entries."""
    entry_snapshot = build_entry_leadership_snapshot(quality, leadership_context or {})

    cur.execute("""
        INSERT INTO bot_trades_v4 (
            symbol, direction, entry_price, status, opened_at, data_version,
            momentum_strength, trend_strength, entry_quality, peak_pnl_percent,
            signal_id, signal_timestamp,
            trade_size_gbp, dynamic_trade_size_gbp,
            leadership_prior_successes, leadership_prior_runners, leadership_prior_avg_peak,
            leadership_tier, leadership_mode, leadership_score,
            lifecycle_phase_at_entry, prior_lifecycle_phase_at_entry, leadership_transition_at_entry,
            leadership_delta_5m_at_entry, leadership_delta_15m_at_entry, leadership_delta_30m_at_entry,
            leadership_delta_60m_at_entry, leadership_score_30m_ago_at_entry,
            leadership_age_minutes_at_entry, leadership_peak_score_last_4h_at_entry, leadership_rank_at_entry,
            market_near_count_at_entry, market_core_count_at_entry, market_aggressive_count_at_entry,
            market_monster_count_at_entry, shadow_emergence_detected_at_entry, shadow_emergence_reason_at_entry,
            is_shadow
        )
        VALUES (
            %s,%s,%s,'OPEN',NOW(),%s,%s,%s,%s,0,%s,%s,
            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,
            TRUE
        )
        RETURNING id
    """, (
        symbol, direction, price, DATA_VERSION, momentum, trend, quality, signal_id, signal_time,
        model_size_gbp, model_size_gbp,
        entry_snapshot.get("leadership_prior_successes"), entry_snapshot.get("leadership_prior_runners"),
        entry_snapshot.get("leadership_prior_avg_peak"), entry_snapshot.get("leadership_tier"),
        entry_snapshot.get("leadership_mode"), entry_snapshot.get("leadership_score"),
        entry_snapshot.get("lifecycle_phase_at_entry"), entry_snapshot.get("prior_lifecycle_phase_at_entry"),
        entry_snapshot.get("leadership_transition_at_entry"), entry_snapshot.get("leadership_delta_5m_at_entry"),
        entry_snapshot.get("leadership_delta_15m_at_entry"), entry_snapshot.get("leadership_delta_30m_at_entry"),
        entry_snapshot.get("leadership_delta_60m_at_entry"), entry_snapshot.get("leadership_score_30m_ago_at_entry"),
        entry_snapshot.get("leadership_age_minutes_at_entry"), entry_snapshot.get("leadership_peak_score_last_4h_at_entry"),
        entry_snapshot.get("leadership_rank_at_entry"), entry_snapshot.get("market_near_count_at_entry"),
        entry_snapshot.get("market_core_count_at_entry"), entry_snapshot.get("market_aggressive_count_at_entry"),
        entry_snapshot.get("market_monster_count_at_entry"), entry_snapshot.get("shadow_emergence_detected_at_entry"),
        entry_snapshot.get("shadow_emergence_reason_at_entry"),
    ))
    trade_id = cur.fetchone()[0]

    apply_market_heat_to_trade(cur, trade_id)

    safe_update_trade_telemetry(cur, trade_id, {
        "is_shadow": True,
        "entry_architecture": quality,
        "trade_size_gbp": model_size_gbp,
        "dynamic_trade_size_gbp": model_size_gbp,
        **accounting_entry_telemetry(model_size_gbp),
        "market_os_engine": (leadership_context or {}).get("market_os_engine"),
        "size_scaling_reason": shadow_reason,
        "shadow_reason": shadow_reason,
        **lifecycle_trade_telemetry_from_context(leadership_context),
        **market_context_trade_telemetry(leadership_context),
        **coin_health_trade_telemetry((leadership_context or {}).get("coin_health_snapshot") or {}),
    })

    try:
        log_trade_event(cur, trade_id, symbol, f"{quality.lower()}_shadow_entry", price, 0, 0, 0, momentum, trend, True)
    except Exception as e:
        print(f"⚠️ {quality} shadow trade_events entry log failed: {e}", flush=True)

    print(f"🧪 OPEN SHADOW | {quality} | {symbol} | id={trade_id} | size={fmt_money(model_size_gbp)} | reason={shadow_reason}", flush=True)
    send_telegram_alert(
        f"🧪 <b>SHADOW ENTRY</b>\n"
        f"{engine_emoji(quality)} <b>{engine_display_name(quality)}</b> | {symbol} LONG\n"
        f"Model size {fmt_money(model_size_gbp)} | Reason: {shadow_reason}\n"
        f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
        f"Context {fmt_num((leadership_context or {}).get('market_median_peak_context'), 3)} | "
        f"Score {fmt_num((leadership_context or {}).get('prior_avg_peak'))}\n"
        f"ID {trade_id}"
    )
    return trade_id



def calculate_pnl_percent(direction, entry_price, current_price):
    """Shared helper for shadow engines."""
    if not entry_price:
        return 0.0

    if direction == "SHORT":
        return ((entry_price - current_price) / entry_price) * 100

    return ((current_price - entry_price) / entry_price) * 100


def process_market_os_shadow_trades(cur, symbol, price, momentum, trend, now):
    """Simple shadow lifecycle for ROT/Core research rows."""
    cur.execute("""
        SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent,
               entry_quality, COALESCE(dynamic_trade_size_gbp, trade_size_gbp, 10)
        FROM bot_trades_v4
        WHERE status = 'OPEN'
          AND COALESCE(is_shadow, FALSE) = TRUE
          AND entry_quality IN ('ROT_MICRO_V1', 'LEADERSHIP_CORE', 'COIN_HEALTH_SHADOW')
          AND symbol = %s
    """, (symbol,))
    rows = cur.fetchall() or []

    for tid, sym, direction, entry_price, opened_at, peak_pnl, entry_quality, dyn_size in rows:
        try:
            pnl_percent = calculate_pnl_percent(direction, entry_price, price)
            current_peak = max(float(peak_pnl or 0), float(pnl_percent or 0))
            mins = safe_age_minutes(opened_at, now) or 0
            model_size = float(dyn_size or 10)

            if entry_quality == "ROT_MICRO_V1":
                hold_minutes = ROT_MICRO_SHADOW_HOLD_MINUTES
                hard_stop = ROT_MICRO_SHADOW_HARD_STOP
            elif entry_quality == "COIN_HEALTH_SHADOW":
                hold_minutes = COIN_HEALTH_SHADOW_HOLD_MINUTES
                hard_stop = COIN_HEALTH_SHADOW_HARD_STOP
            else:
                hold_minutes = LEADERSHIP_CORE_SHADOW_HOLD_MINUTES
                hard_stop = LEADERSHIP_CORE_SHADOW_HARD_STOP

            close_reason = None
            if pnl_percent <= hard_stop:
                close_reason = "shadow_hard_stop"
            elif mins >= hold_minutes:
                close_reason = "shadow_time_exit"

            if close_reason:
                accounting = accounting_exit_values(model_size, pnl_percent)
                pnl_gbp = accounting["pnl_gbp"]
                cur.execute("""
                    UPDATE bot_trades_v4
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        close_price = %s,
                        close_reason = %s,
                        pnl_percent = %s,
                        pnl_gbp = %s,
                        pnl_usdt = %s,
                        exit_value_usdt = %s,
                        exit_value_gbp = %s,
                        usd_gbp_rate = %s,
                        peak_pnl_percent = %s,
                        exit_momentum = %s,
                        exit_trend = %s
                    WHERE id = %s
                """, (
                    price, close_reason, pnl_percent, pnl_gbp,
                    accounting["pnl_usdt"], accounting["exit_value_usdt"], accounting["exit_value_gbp"],
                    accounting["usd_gbp_rate"], current_peak, momentum, trend, tid
                ))
                log_trade_event(cur, tid, sym, close_reason, price, pnl_percent, current_peak, mins, momentum, trend, False)
                try:
                    refresh_coin_learning_from_history(cur, sym, allow_mode_change=True)
                except Exception as e:
                    print(f"⚠️ coin learning refresh after shadow close failed: {e}", flush=True)
                print(f"🧪 CLOSE SHADOW | {entry_quality} | {sym} | {round(pnl_percent,3)}% | peak={round(current_peak,3)}% | {close_reason}", flush=True)
            else:
                cur.execute("""
                    UPDATE bot_trades_v4
                    SET peak_pnl_percent = GREATEST(COALESCE(peak_pnl_percent,0), %s),
                        last_price = %s
                    WHERE id = %s
                """, (current_peak, price, tid))
        except Exception as e:
            print(f"⚠️ market OS shadow processing failed for {symbol}/{tid}: {e}", flush=True)


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
        ensure_market_heat_columns(cur)
        ensure_coin_scores_v84_columns(cur)
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

        apply_market_heat_to_signal(cur, signal_id)
        if ENABLE_COIN_SCORE_UPSERT_ON_SIGNAL:
            upsert_initial_coin_score(cur, symbol)
            refresh_coin_learning_from_history(cur, symbol, allow_mode_change=True)

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

                # v6.6.21 FREE THE UNICORNS:
                # If the only reason the leadership gate blocked this was a tested Unicorn subclass,
                # allow it through to the normal OKX / max-open / sizing checks.
                if not entry_allowed and block_reason == "leadership_climax_delta_blocked":
                    unicorn_ok, leadership_context, unicorn_reason = apply_unicorn_entry_override(
                        leadership_context,
                        momentum,
                        trend,
                        block_reason
                    )
                    if unicorn_ok:
                        entry_allowed = True
                        block_reason = unicorn_reason
                        print(
                            f"🦄 UNICORN OVERRIDE | {symbol} | {unicorn_reason} | "
                            f"pressure={leadership_context.get('unicorn_pressure_score')} | "
                            f"score={leadership_context.get('prior_avg_peak')} | "
                            f"cqe={leadership_context.get('cqe_quality_score')}",
                            flush=True
                        )

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

                # v6.9.2: Core leadership is research/shadow only unless explicitly re-enabled.
                if entry_quality == "LEADERSHIP_CORE" and not ENABLE_LEADERSHIP_CORE_LIVE:
                    if ENABLE_LEADERSHIP_CORE_SHADOW:
                        shadow_ctx = dict(leadership_context or {})
                        shadow_ctx["market_os_engine"] = "LEADERSHIP_CORE_SHADOW"
                        open_shadow_market_os_trade(
                            cur, symbol, "LONG", price, momentum, trend,
                            "LEADERSHIP_CORE", signal_id, signal_time, shadow_ctx,
                            "leadership_core_shadow_only",
                            model_size_gbp=float(get_trade_size_for_quality("LEADERSHIP_CORE"))
                        )
                    entry_allowed = False
                    block_reason = "leadership_core_shadow_only"

                # v7.5 SHADOW FILTER:
                # DEAD_CORE markets that are not RISING are shadow-only.
                if entry_allowed:
                    market_state = (leadership_context or {}).get("market_lifecycle_state")
                    market_move = (leadership_context or {}).get("market_lifecycle_movement")

                    if (
                        market_state == "DEAD_CORE"
                        and market_move != "RISING"
                    ):
                        shadow_ctx = dict(leadership_context or {})
                        shadow_ctx["market_os_engine"] = "DEAD_CORE_FILTER_SHADOW"

                        try:
                            open_shadow_market_os_trade(
                                cur,
                                symbol,
                                "LONG",
                                price,
                                momentum,
                                trend,
                                entry_quality or "DEAD_CORE_FILTER",
                                signal_id,
                                signal_time,
                                shadow_ctx,
                                "dead_core_not_rising_shadow",
                                model_size_gbp=float(get_trade_size_for_quality(entry_quality or "STANDARD"))
                            )
                        except Exception as e:
                            print(f"⚠️ DEAD_CORE shadow creation failed: {e}", flush=True)

                        entry_allowed = False
                        block_reason = "dead_core_not_rising_shadow"

                # v6.1.4: check OKX tradability BEFORE creating DB trade / consuming slot.
                if entry_allowed:
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
                        # v6.6.21 FREE THE UNICORNS:
                        # Allow one extra same-symbol continuation only if the signal matches
                        # the validated Unicorn Candidate profile.
                        unicorn_ok, leadership_context, unicorn_reason = apply_unicorn_entry_override(
                            leadership_context,
                            momentum,
                            trend,
                            "max_same_symbol_open"
                        )

                        if unicorn_ok and same_symbol_count < UNICORN_MAX_SAME_SYMBOL_OPEN:
                            entry_allowed = True
                            block_reason = unicorn_reason
                            print(
                                f"🦄 UNICORN SAME-SYMBOL CONTINUATION | {symbol} | "
                                f"same={same_symbol_count}/{MAX_SAME_SYMBOL_OPEN} -> allowed up to {UNICORN_MAX_SAME_SYMBOL_OPEN} | "
                                f"pressure={leadership_context.get('unicorn_pressure_score')}",
                                flush=True
                            )
                        else:
                            entry_allowed = False
                            block_reason = "max_same_symbol_open"


            # v6.6 ROT_MICRO: independent tiny continuation harvester.
            # Only considered if core leadership entry did not already allow a trade.
            if not entry_allowed and ENABLE_ROT_MICRO_LIVE:
                rot_ok, rot_reason, rot_ctx = is_rot_micro_candidate(cur, symbol, momentum, trend, leadership_context, shadow_mode=False)
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

            # v6.9.2 ROT_MICRO_V2_SHADOW: live ROT is disabled, but filtered shadow entries are still logged.
            try:
                if not entry_allowed and ENABLE_ROT_MICRO_SHADOW:
                    rot_shadow_ok, rot_shadow_reason, rot_shadow_ctx = is_rot_micro_candidate(
                        cur, symbol, momentum, trend, leadership_context, shadow_mode=True
                    )
                    if rot_shadow_ok:
                        shadow_ctx = dict(leadership_context or {})
                        shadow_ctx.update(rot_shadow_ctx or {})
                        shadow_ctx["prior_avg_peak"] = rot_shadow_ctx.get("leadership_max_60m", shadow_ctx.get("prior_avg_peak", 0))
                        shadow_ctx["leadership_mode"] = "ROT_MICRO"
                        shadow_ctx["market_os_engine"] = "ROT_MICRO_SHADOW"
                        shadow_ctx["size_scaling_reason"] = "rot_micro_shadow_context_filtered"
                        open_shadow_market_os_trade(
                            cur, symbol, "LONG", price, momentum, trend,
                            "ROT_MICRO_V1", signal_id, signal_time, shadow_ctx,
                            f"rot_micro_shadow_ctx_ge_{ROT_MICRO_MIN_CONTEXT}",
                            model_size_gbp=ROT_MICRO_TRADE_SIZE_GBP
                        )
            except Exception as e:
                print(f"⚠️ ROT micro shadow entry skipped: {e}", flush=True)

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
                    "adaptive_dead_market_leader_at_signal": leadership_context.get("adaptive_dead_market_leader"),
                    "market_os_engine_at_signal": leadership_context.get("market_os_engine"),
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
                    "cqe_positive_trend_ratio_30m": (leadership_context.get("cqe_context") or {}).get("cqe_positive_trend_ratio_30m"),
                    "unicorn_candidate": leadership_context.get("unicorn_candidate"),
                    "unicorn_pressure_score": leadership_context.get("unicorn_pressure_score"),
                    "unicorn_candidate_reason": leadership_context.get("unicorn_candidate_reason"),
                    "unicorn_override_triggered": leadership_context.get("unicorn_override_triggered"),
                    "unicorn_override_reason": leadership_context.get("unicorn_override_reason"),
                    "unicorn_original_block_reason": leadership_context.get("unicorn_original_block_reason")
                })
        except Exception as e:
            print(f"⚠️ signals_raw intelligence update skipped: {e}", flush=True)

        # ================= MARKET LIFECYCLE ENGINE v6.9 — DATA ONLY =================
        # Calculates/stores lifecycle labels for research. Does not alter entries/exits/sizing.
        try:
            if ENABLE_MARKET_LIFECYCLE_ENGINE_V69:
                lifecycle_context_v69 = get_market_lifecycle_context(cur, leadership_context, signal_time)
                safe_update_signal_telemetry(cur, signal_id, lifecycle_context_v69)

                leadership_context = leadership_context or {}
                leadership_context.update(lifecycle_context_v69)

                maybe_send_market_lifecycle_change_alert(cur, lifecycle_context_v69)
        except Exception as e:
            print(f"⚠️ market lifecycle v6.9 logging skipped: {e}", flush=True)
            safe_telemetry_rollback(cur)

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
            if entry_allowed and not live_entry_allowed_by_control_panel():
                entry_allowed = False
                block_reason = "control_panel_entries_disabled"

            if entry_allowed:
                # ================= COIN HEALTH SELF-HEALING GATE v7.6 =================
                # If a symbol has stopped producing expansion, do not risk capital.
                # Open a same-setup shadow observation instead so the coin can prove recovery.
                coin_health_snapshot = get_coin_health_snapshot(cur, symbol)
                leadership_context = leadership_context or {}
                leadership_context["coin_health_snapshot"] = coin_health_snapshot

                if ENABLE_COIN_HEALTH_ENGINE and coin_health_snapshot.get("coin_health_mode") == "DISABLED":
                    entry_allowed = False
                    block_reason = f"coin_disabled_{coin_health_snapshot.get('coin_health_reason')}"
                    print(f"🚫 COIN DISABLED | {symbol} | reason={coin_health_snapshot.get('coin_health_reason')}", flush=True)

                elif ENABLE_COIN_HEALTH_ENGINE and coin_health_snapshot.get("coin_health_mode") == "SHADOW":
                    entry_allowed = False
                    block_reason = f"coin_health_{coin_health_snapshot.get('coin_health_reason')}"
                    print(
                        f"🩺 COIN HEALTH ROUTE TO SHADOW | {symbol} | "
                        f"dead={coin_health_snapshot.get('coin_dead_streak')}/{COIN_HEALTH_DEAD_WINDOW} | "
                        f"avg_peak={coin_health_snapshot.get('coin_health_avg_peak')} | "
                        f"reason={coin_health_snapshot.get('coin_health_reason')}",
                        flush=True
                    )
                    open_coin_health_shadow_trade(
                        cur, symbol, "LONG", price, momentum, trend,
                        signal_id, signal_time, leadership_context, coin_health_snapshot
                    )

                # v6.6.12 EMERGENCY EXCHANGE RECONCILIATION GUARD:
                # OKX exchange truth overrides Supabase state. If OKX already holds
                # this base asset, block the live entry to prevent repeated duplicate buys.
                if entry_allowed and ENABLE_OKX_POSITION_ENTRY_GUARD:
                    okx_position_exists, okx_position_context = okx_has_live_position(symbol, price)
                    if okx_position_exists:
                        entry_allowed = False
                        block_reason = f"okx_live_position_guard_{okx_position_context.get('reason')}"
                        print(
                            f"🚫 BLOCKED LIVE ENTRY | {symbol} | existing/unknown OKX position | {okx_position_context}",
                            flush=True
                        )
                        if ENABLE_BLOCKED_TRADE_TELEGRAM:
                            send_telegram_alert(
                                f"🚫 <b>LIVE ENTRY BLOCKED</b> | {symbol}\n"
                                f"OKX position guard triggered before buy.\n"
                                f"Reason: {okx_position_context.get('reason')}\n"
                                f"Available: {okx_position_context.get('available')} {okx_position_context.get('base_ccy')}\n"
                                f"Estimated notional: {fmt_money(okx_position_context.get('estimated_notional_usd'))}"
                            )

                if not entry_allowed:
                    print(
                        f"⛔ BLOCKED | {symbol} | {decision} | "
                        f"mom={round(momentum,3)} trend={round(trend,3)} | "
                        f"reason={block_reason}",
                        flush=True
                    )
                else:
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
                        f"🟢 <b>LIVE ENTRY</b>\n"
                        f"{engine_emoji(entry_quality)} <b>{engine_display_name(entry_quality)}</b> | {symbol} LONG\n"
                        f"Model size {fmt_money(entry_trade_size)} | OKX {fmt_money(entry_quote_size)}\n"
                        f"Entry {price} | T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
                        f"{'🦄 Unicorn override: ' + str(leadership_context.get('unicorn_override_reason')) + ' | P ' + str(leadership_context.get('unicorn_pressure_score')) + chr(10) if leadership_context.get('unicorn_override_triggered') else ''}"
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
                        f"Lifecycle: {leadership_context.get('market_lifecycle_window') or 'n/a'} | "
                        f"B {fmt_num(leadership_context.get('market_breadth'), 2)} | "
                        f"BA {fmt_num(leadership_context.get('market_breadth_accel'), 2)} | "
                        f"Med {fmt_num(leadership_context.get('market_median_peak_context'), 3)}\n"
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
                        actual_entry_quote_size = float(okx_entry_result.get("actual_quote_size") or entry_quote_size)
                        if abs(actual_entry_quote_size - float(entry_quote_size or 0)) > 0.0001:
                            actual_entry_gbp_size = usdt_quote_to_gbp(actual_entry_quote_size)
                            safe_update_trade_telemetry(cur, trade_id, {
                                "trade_size_gbp": actual_entry_gbp_size,
                                "dynamic_trade_size_gbp": actual_entry_gbp_size,
                                **accounting_entry_telemetry(actual_entry_quote_size),
                                "size_scaling_reason": (leadership_context.get("size_scaling_reason") or "") + "_partial_position_fill",
                            })
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

        # ================= MARKET OS SHADOW PROCESSING =================
        try:
            process_market_os_shadow_trades(cur, symbol, price, momentum, trend, now)
        except Exception as e:
            print(f"⚠️ Market OS shadow processing skipped: {e}", flush=True)

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
                COALESCE(cqe_upgraded, FALSE) AS cqe_upgraded,
                COALESCE(partial_bank_4_done, FALSE) AS partial_bank_done,
                COALESCE(partial_bank_realized_pnl_gbp, 0) AS partial_bank_realized_gbp,
                COALESCE(trade_size_usdt, dynamic_trade_size_gbp, trade_size_gbp, 0) AS trade_size_usdt_for_pnl
            FROM bot_trades_v4
            WHERE status = 'OPEN'
              AND COALESCE(is_shadow, FALSE) = FALSE
        """)

        open_trades = cur.fetchall()

        for (tid, sym, direction, entry_price, opened_at, peak_pnl, is_shadow, entry_quality, cqe_upgraded, partial_bank_done, partial_bank_realized_gbp, trade_size_usdt_for_pnl) in open_trades:
            try:
                if sym != symbol:
                    continue

                if direction != "LONG":
                    continue

                pnl = ((price - entry_price) / entry_price)
                pnl_percent = pnl * 100
                safe_now = ensure_utc(now)
                safe_opened_at = ensure_utc(opened_at)
                mins = (safe_now - safe_opened_at).total_seconds() / 60
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
                if not close_reason:
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

                if (
                    not close_reason
                    and BPT_UPGRADED_GIVEBACK_GUARD_ENABLED
                    and entry_quality == "BPT_CQE_LIFECYCLE_V1"
                    and bool(cqe_upgraded)
                    and current_peak >= BPT_UPGRADED_GIVEBACK_GUARD_PEAK
                    and pnl_percent <= BPT_UPGRADED_GIVEBACK_GUARD_MIN_KEEP
                ):
                    close_reason = "bpt_upgrade_giveback_guard"
                    exit_architecture = "bpt_upgrade_profit_protection"

                if not close_reason and should_trend_persistence_exit(entry_quality, mins, trend):
                    close_reason = trend_persistence_exit_reason(entry_quality)
                    exit_architecture = "TREND_PERSISTENCE_EXIT"
                    decay_triggered = True
                    slot_recycle_candidate = True

                if not close_reason and pnl_percent <= LONG_HARD_STOP:
                    close_reason = "long_hard_stop"
                    exit_architecture = "long_hard_stop"

                if close_reason:
                    legacy_size = get_trade_size_quote_for_quality(entry_quality)
                    size_usdt_for_pnl = float(trade_size_usdt_for_pnl or legacy_size or 0)
                    remaining_fraction = (1.0 - PARTIAL_BANK_FRACTION) if partial_bank_done else 1.0
                    accounting = accounting_exit_values(
                        size_usdt_for_pnl,
                        pnl_percent,
                        partial_bank_realized_gbp=partial_bank_realized_gbp,
                        remaining_fraction=remaining_fraction
                    )
                    pnl_gbp = accounting["pnl_gbp"]

                    cur.execute("""
                        UPDATE bot_trades_v4
                        SET status = 'CLOSED',
                            closed_at = NOW(),
                            close_price = %s,
                            pnl_percent = %s,
                            pnl_gbp = %s,
                            pnl_usdt = %s,
                            exit_value_usdt = %s,
                            exit_value_gbp = %s,
                            usd_gbp_rate = %s,
                            close_reason = %s
                        WHERE id = %s
                    """, (
                        price,
                        pnl_percent,
                        pnl_gbp,
                        accounting["pnl_usdt"],
                        accounting["exit_value_usdt"],
                        accounting["exit_value_gbp"],
                        accounting["usd_gbp_rate"],
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
                            trade_size_quote=size_usdt_for_pnl
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

                    try:
                        refresh_coin_learning_from_history(cur, sym, allow_mode_change=True)
                    except Exception as e:
                        print(f"⚠️ coin learning refresh after real close failed: {e}", flush=True)

                    exit_leadership_state = get_latest_leadership_state(cur, sym)
                    latest_signal_state = get_latest_signal_state(cur, sym)

                    send_telegram_alert(
                        f"🔴 <b>LIVE EXIT</b>\n"
                        f"{engine_emoji(entry_quality)} <b>{engine_display_name(entry_quality)}</b> | {sym} LONG\n"
                        f"{format_trade_pnl_lines(pnl_percent, pnl_gbp, 0)} | Peak {fmt_num(current_peak)}%\n"
                        f"DD {fmt_num(drawdown_from_peak)}% | Reason: {close_reason}\n"
                        f"Exit T/M {fmt_num(trend)} / {fmt_num(momentum)}\n"
                        f"Phase: {format_leadership_compact(exit_lifecycle_context)}\n"
                        f"Latest: mom {fmt_num((latest_signal_state or {}).get('momentum'))} "
                        f"trend {fmt_num((latest_signal_state or {}).get('trend'))} | "
                        f"{(latest_signal_state or {}).get('decision') or 'NONE'} / "
                        f"{(latest_signal_state or {}).get('block_reason') or 'no_reason'}"
                    )
            except Exception as e:
                print(
                    f"🚨 REAL EXIT ENGINE TRADE FAILURE | {sym} | id={tid} | error={e}",
                    flush=True
                )
                send_telegram_alert(
                    f"🚨 <b>REAL EXIT ENGINE FAILURE</b>\n"
                    f"{sym}\n"
                    f"Trade ID: {tid}\n"
                    f"Error: {str(e)[:300]}"
                )
                continue


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
        f"Engine: LEADERSHIP_LIVE + CQE_CONTINUATION_V1 + BPT_CQE_LIFECYCLE_V1 + ROT_MICRO_V1\n"
        f"Live orders: {ENABLE_LIVE_ORDERS} | New entries: {ENABLE_NEW_ENTRIES} | OKX exits: {ENABLE_OKX_EXITS} | Emergency close-only: {EMERGENCY_CLOSE_ONLY_MODE}\n"
        f"Last signal: {last_signal}\n"
        f"Signals 1h: {signals_1h}\n"
        f"Last real trade: {last_trade}\n"
        f"Real trades 24h: {trades_24h}\n"
        f"Open real: {open_real}\n"
        f"Scored leadership signals 24h: {scored_24h}\n"
        f"Leadership snapshots 24h: {snapshots_24h}\n"
        f"Stable: score>={STABLE_LEADER_MIN_SCORE}, Δ{STABLE_LEADER_DELTA_MIN}..{STABLE_LEADER_DELTA_MAX}\n"
        f"Shadow ignition: {ENABLE_SHADOW_EMERGENCE_TELEMETRY} | Δ{CONTROLLED_IGNITION_DELTA_MIN}..{CONTROLLED_IGNITION_DELTA_MAX}\n"
        f"CQE continuation: {ENABLE_SHADOW_CQE} | future name {CQE_CONTINUATION_ENTRY_QUALITY} | Q>={CQE_MIN_QUALITY_SCORE}\n"
        f"BPT lifecycle: {ENABLE_BPT_CQE_LIFECYCLE_SHADOW} | ghost probes {ENABLE_BPT_CQE_GHOST_PROBES} | live probes {ENABLE_BPT_CQE_LIVE_PROBES} | live upgrades {ENABLE_BPT_CQE_LIVE_UPGRADES}\n"
        f"Density relaxed: {ENABLE_RELAXED_DENSITY_DEPENDENCY} | fixed real exits: {ENABLE_FIXED_TIME_EXITS_REAL}\n"
        f"Persistence Hunter: {ENABLE_PERSISTENCE_HUNTER_SHADOW} | live {ENABLE_PERSISTENCE_HUNTER_LIVE} | score>={PH_MIN_LEADERSHIP_SCORE} age {PH_MIN_CORE_AGE_MINUTES}-{PH_MAX_CORE_AGE_MINUTES}m\n"
        f"Max same symbol: {MAX_SAME_SYMBOL_OPEN}\n"
        f"OKX tradable cache: {len(OKX_TRADABLE_SPOT_INST_IDS)} pairs\n"
        f"Telemetry V1: {ENABLE_TELEMETRY_V1} | {TELEMETRY_VERSION}\n"
        f"Archetype engine: {ENABLE_ARCHETYPE_STATE_ENGINE} | adaptive BPT max hold {ENABLE_ADAPTIVE_BPT_MAX_HOLD} | tg dedupe {ENABLE_TELEGRAM_DEDUPE}\n"
        f"Market OS v6.6: {ENABLE_MARKET_OS_V66} | partial bank {ENABLE_PARTIAL_PROFIT_BANK_V66} @ {PARTIAL_BANK_TRIGGER_PCT}% x {int(PARTIAL_BANK_FRACTION*100)}% | rot live {ENABLE_ROT_MICRO_LIVE} shadow {ENABLE_ROT_MICRO_SHADOW} ctx>={ROT_MICRO_MIN_CONTEXT}\n"
        f"Leadership scaling: {ENABLE_LEADERSHIP_SIZE_SCALING_V66} | >= {LEADERSHIP_SCALE_THRESHOLD} → {fmt_money(LEADERSHIP_SCALED_TRADE_SIZE_GBP)} | core live {ENABLE_LEADERSHIP_CORE_LIVE} shadow {ENABLE_LEADERSHIP_CORE_SHADOW}\n"
        f"Trend persistence exit: {ENABLE_TREND_PERSISTENCE_EXIT} | {TREND_PERSISTENCE_CHECK_MINUTES}m trend < {TREND_PERSISTENCE_MIN_TREND}"
    )

def build_telegram_summary_message(cur, hours=24):
    """Clean Telegram /daily summary.
    v6.7 separates LIVE and SHADOW engines and prioritises fee-adjusted Net PnL.
    Net uses recorded fee_gbp only; if fees are not populated, net equals pnl_gbp.
    """
    cur.execute("""
        WITH closed AS (
            SELECT
                entry_quality AS engine,
                COALESCE(is_shadow, FALSE) AS is_shadow,
                pnl_percent,
                COALESCE(pnl_gbp, 0) AS pnl_gbp,
                COALESCE(fee_gbp, 0) AS fee_gbp,
                peak_pnl_percent,
                close_reason
            FROM bot_trades_v4
            WHERE closed_at >= NOW() - (%s || ' hours')::INTERVAL
              AND status = 'CLOSED'
        )
        SELECT
            engine,
            is_shadow,
            COUNT(*) AS trades,
            COALESCE(ROUND(SUM(pnl_gbp - fee_gbp)::numeric, 3), 0) AS net_pnl_gbp,
            COALESCE(ROUND(SUM(pnl_gbp)::numeric, 3), 0) AS gross_pnl_gbp,
            COALESCE(ROUND(SUM(fee_gbp)::numeric, 3), 0) AS fee_gbp,
            COALESCE(ROUND(AVG(pnl_percent)::numeric, 3), 0) AS avg_pnl,
            COALESCE(ROUND(AVG(peak_pnl_percent)::numeric, 3), 0) AS avg_peak,
            COUNT(*) FILTER (WHERE pnl_percent > 0) AS winners,
            COUNT(*) FILTER (WHERE peak_pnl_percent >= 2.0) AS runners,
            COUNT(*) FILTER (WHERE peak_pnl_percent >= 5.0) AS monsters
        FROM closed
        GROUP BY engine, is_shadow
        ORDER BY is_shadow, engine
    """, (hours,))
    closed_rows = cur.fetchall()

    cur.execute("""
        SELECT
            COUNT(*) FILTER (WHERE status = 'OPEN' AND COALESCE(is_shadow, FALSE) = FALSE) AS open_real,
            COUNT(*) FILTER (WHERE status = 'OPEN' AND COALESCE(is_shadow, FALSE) = TRUE) AS open_shadow
        FROM bot_trades_v4
    """)
    open_real, open_shadow = cur.fetchone() or (0, 0)

    total_net = sum(float(r[3] or 0) for r in closed_rows)
    total_gross = sum(float(r[4] or 0) for r in closed_rows)
    total_fees = sum(float(r[5] or 0) for r in closed_rows)
    total_trades = sum(int(r[2] or 0) for r in closed_rows)

    lines = []
    lines.append(f"📊 <b>Trading Bot {hours}h Summary</b>")
    lines.append(f"Version: <b>{DATA_VERSION}</b>")
    lines.append(f"Closed trades: <b>{total_trades}</b>")
    if total_fees:
        lines.append(f"Net PnL: <b>{fmt_money(total_net)}</b> | Gross {fmt_money(total_gross)} | Fees {fmt_money(total_fees)}")
    else:
        lines.append(f"Net PnL: <b>{fmt_money(total_net)}</b>")
    lines.append(f"Open: 🟢 live <b>{open_real}</b> | 🧪 shadow <b>{open_shadow}</b>")

    live_rows = [r for r in closed_rows if not bool(r[1])]
    shadow_rows = [r for r in closed_rows if bool(r[1])]

    def add_engine_rows(title, rows):
        lines.append(f"\n<b>{title}</b>")
        if not rows:
            lines.append("No closed trades in this window.")
            return
        for engine, is_shadow, trades, net_pnl_gbp, gross_pnl_gbp, fee_gbp, avg_pnl, avg_peak, winners, runners, monsters in rows:
            win_rate = (float(winners or 0) / float(trades or 1)) * 100
            label = engine_display_name(engine)
            emoji = engine_emoji(engine, is_shadow)
            fee_text = f" | fees {fmt_money(fee_gbp)}" if float(fee_gbp or 0) else ""
            lines.append(
                f"{emoji} <b>{label}</b>: {trades} trades | net {fmt_money(net_pnl_gbp)}{fee_text} | "
                f"avg {fmt_num(avg_pnl)}% | peak {fmt_num(avg_peak)}% | win {fmt_num(win_rate,1)}% | R {runners} M {monsters}"
            )

    add_engine_rows("🟢 LIVE ENGINES", live_rows)
    add_engine_rows("🧪 SHADOW ENGINES", shadow_rows)

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
            WHERE COALESCE(is_shadow, FALSE) = FALSE
              AND entry_quality IN (%s, %s)
              AND opened_at >= NOW() - (%s || ' hours')::INTERVAL
        """, (LEGACY_CQE_ENTRY_QUALITY, CQE_CONTINUATION_ENTRY_QUALITY, hours))
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
            WHERE COALESCE(is_shadow, FALSE) = FALSE
              AND entry_quality IN (%s, %s)
              AND status = 'OPEN'
            ORDER BY opened_at DESC
            LIMIT 10
        """, (LEGACY_CQE_ENTRY_QUALITY, CQE_CONTINUATION_ENTRY_QUALITY))
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
            f"🕵️ <b>CQE / Shadow Watch {hours}h</b>",
            f"LONG signals: <b>{long_count}</b>",
            f"CQE signals: <b>{cqe_count}</b> | Open CQE: <b>{open_cqe}</b> | Closed: <b>{closed_cqe}</b>",
            f"CQE closed avg: <b>{fmt_num(avg_closed_pnl)}%</b> | {fmt_money(closed_pnl_gbp)} | Avg peak {fmt_num(avg_peak)}%",
            f"Quiet old shadows: {quiet_count} | Ignition shadows: {ignition_count}",
        ]

        if open_rows:
            lines.append("\n<b>Open CQE continuation trades</b>")
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




def build_telegram_coins_message(cur):
    try:
        cur.execute("""
            SELECT symbol, tier, coin_health_mode, promotion_score, demotion_score
            FROM coin_scores
            ORDER BY
                CASE tier
                    WHEN 'A' THEN 1
                    WHEN 'B' THEN 2
                    WHEN 'C' THEN 3
                    WHEN 'D' THEN 4
                    ELSE 5
                END,
                symbol
        """)
        rows = cur.fetchall()

        groups = {"A":[],"B":[],"C":[],"D":[],"DISCOVERY":[]}

        for symbol,tier,mode,promo,demo in rows:
            move = ""
            if (promo or 0) >= 5:
                move = " ⬆️"
            elif (demo or 0) >= 3:
                move = " ⬇️"

            status = "🟢" if mode == "LIVE" else ("🔴" if mode == "DISABLED" else "🟡")
            groups.setdefault(tier, []).append(f"{status} {symbol}{move}")

        msg = "🪙 <b>Coin Universe</b>\n\n"
        msg += "🏆 <b>A Tier LIVE</b>\n" + ("\n".join(groups["A"]) or "None") + "\n\n"
        msg += "🥈 <b>B Tier LIVE</b>\n" + ("\n".join(groups["B"]) or "None") + "\n\n"
        msg += "🥉 <b>C Tier SHADOW</b>\n" + ("\n".join(groups["C"]) or "None") + "\n\n"
        msg += "🚫 <b>D Tier DISABLED</b>\n" + ("\n".join(groups["D"]) or "None") + "\n\n"
        msg += "🔬 <b>Discovery</b>\n" + ("\n".join(groups["DISCOVERY"]) or "None")
        return msg
    except Exception as e:
        return f"Coin report error: {e}"

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

        if cmd in ["/coins", "/tiers"]:
            return build_telegram_coins_message(cur)

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
            f"Live orders: {ENABLE_LIVE_ORDERS} | Entries: {ENABLE_NEW_ENTRIES} | Exits: {ENABLE_OKX_EXITS}\n"
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
        "ENABLE_BPT_CQE_GHOST_PROBES": ENABLE_BPT_CQE_GHOST_PROBES,
        "ENABLE_RELAXED_DENSITY_DEPENDENCY": ENABLE_RELAXED_DENSITY_DEPENDENCY,
        "ENABLE_TELEMETRY_V1": ENABLE_TELEMETRY_V1,
        "TELEMETRY_VERSION": TELEMETRY_VERSION,
        "ENABLE_FIXED_TIME_EXITS_REAL": ENABLE_FIXED_TIME_EXITS_REAL,
        "DENSITY_NULL_IS_VALID": DENSITY_NULL_IS_VALID,
        "ENABLE_BPT_CQE_LIVE_UPGRADES": ENABLE_BPT_CQE_LIVE_UPGRADES,
        "BPT_CQE_PROBE_SIZE_GBP": BPT_CQE_PROBE_SIZE_GBP,
        "ENABLE_BPT_PROBE_LIFECYCLE_ENGINE": ENABLE_BPT_PROBE_LIFECYCLE_ENGINE,
        "BPT_MONSTER_FASTTRACK_MIN_AGE_MINUTES": BPT_MONSTER_FASTTRACK_MIN_AGE_MINUTES,
        "BPT_MONSTER_FASTTRACK_MIN_LEADERSHIP_DELTA": BPT_MONSTER_FASTTRACK_MIN_LEADERSHIP_DELTA,
        "BPT_MONSTER_FASTTRACK_MIN_TREND": BPT_MONSTER_FASTTRACK_MIN_TREND,
        "BPT_DEAD_PROBE_MIN_AGE_MINUTES": BPT_DEAD_PROBE_MIN_AGE_MINUTES,
        "BPT_DEAD_PROBE_MAX_MOMENTUM": BPT_DEAD_PROBE_MAX_MOMENTUM,
        "BPT_DEAD_PROBE_MAX_TREND": BPT_DEAD_PROBE_MAX_TREND,
        "BPT_LEADER_FASTTRACK_MIN_AGE_MINUTES": BPT_LEADER_FASTTRACK_MIN_AGE_MINUTES,
        "BPT_LEADER_FASTTRACK_MIN_LEADERSHIP_DELTA": BPT_LEADER_FASTTRACK_MIN_LEADERSHIP_DELTA,
        "BPT_LEADER_FASTTRACK_MIN_TREND": BPT_LEADER_FASTTRACK_MIN_TREND,
        "ENABLE_ADAPTIVE_DEAD_MARKET_LEADERS": ENABLE_ADAPTIVE_DEAD_MARKET_LEADERS,
        "ADAPTIVE_DEAD_MARKET_CONTEXT_MAX": ADAPTIVE_DEAD_MARKET_CONTEXT_MAX,
        "ADAPTIVE_DEAD_MARKET_MAX_RANK": ADAPTIVE_DEAD_MARKET_MAX_RANK,
        "ADAPTIVE_DEAD_MARKET_MAX_SCORE": ADAPTIVE_DEAD_MARKET_MAX_SCORE,
        "ADAPTIVE_DEAD_MARKET_MIN_MOMENTUM": ADAPTIVE_DEAD_MARKET_MIN_MOMENTUM,
        "ADAPTIVE_DEAD_MARKET_MIN_TREND": ADAPTIVE_DEAD_MARKET_MIN_TREND,
        "BPT_EARLY_UPGRADE_GBP": BPT_EARLY_UPGRADE_GBP,
        "BPT_EXTREME_UPGRADE_GBP": BPT_EXTREME_UPGRADE_GBP,
        "BPT_HIGH_UPGRADE_GBP": BPT_HIGH_UPGRADE_GBP,
        "BPT_MEDIUM_UPGRADE_GBP": BPT_MEDIUM_UPGRADE_GBP,
        "CQE_REAL_SCALEIN_ALLOWED_ROWS": CQE_REAL_SCALEIN_ALLOWED_ROWS,
        "ENABLE_PERSISTENCE_HUNTER_SHADOW": ENABLE_PERSISTENCE_HUNTER_SHADOW,
        "ENABLE_PERSISTENCE_HUNTER_LIVE": ENABLE_PERSISTENCE_HUNTER_LIVE,
        "PH_MIN_LEADERSHIP_SCORE": PH_MIN_LEADERSHIP_SCORE,
        "PH_MIN_CORE_AGE_MINUTES": PH_MIN_CORE_AGE_MINUTES,
        "PH_MAX_CORE_AGE_MINUTES": PH_MAX_CORE_AGE_MINUTES,
        "MAX_OPEN_TRADES": MAX_OPEN_TRADES,
        "MAX_SAME_SYMBOL_OPEN": MAX_SAME_SYMBOL_OPEN,
        "ENABLE_COIN_HEALTH_ENGINE": ENABLE_COIN_HEALTH_ENGINE,
        "COIN_HEALTH_DEAD_WINDOW": COIN_HEALTH_DEAD_WINDOW,
        "COIN_HEALTH_DEAD_THRESHOLD": COIN_HEALTH_DEAD_THRESHOLD,
        "COIN_HEALTH_DEAD_PEAK_PCT": COIN_HEALTH_DEAD_PEAK_PCT,
        "COIN_HEALTH_RECOVERY_WINDOW": COIN_HEALTH_RECOVERY_WINDOW,
        "COIN_HEALTH_RECOVERY_THRESHOLD": COIN_HEALTH_RECOVERY_THRESHOLD,
        "COIN_HEALTH_RECOVERY_PEAK_PCT": COIN_HEALTH_RECOVERY_PEAK_PCT,
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
        "ALLOW_PARTIAL_POSITION_FILL": ALLOW_PARTIAL_POSITION_FILL,
        "MIN_POSITION_SIZE_GBP": MIN_POSITION_SIZE_GBP,
        "ENABLE_LEADERSHIP_CORE_LIVE": ENABLE_LEADERSHIP_CORE_LIVE,
        "ENABLE_LEADERSHIP_CORE_SHADOW": ENABLE_LEADERSHIP_CORE_SHADOW,
        "ENABLE_ROT_MICRO_LIVE": ENABLE_ROT_MICRO_LIVE,
        "ENABLE_ROT_MICRO_SHADOW": ENABLE_ROT_MICRO_SHADOW,
        "ROT_MICRO_MIN_CONTEXT": ROT_MICRO_MIN_CONTEXT,
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
            "ghost_probes": ENABLE_BPT_CQE_GHOST_PROBES,
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



# =========================
# v6.6.11 TIMEZONE LIFECYCLE FIX
# =========================
# Fixed offset-naive vs offset-aware datetime subtraction errors.
#
# Root cause:
# - lifecycle engine mixed:
#     datetime.utcnow()  (naive)
# with:
#     postgres timestamptz values (aware UTC)
#
# Fix:
# - standardized runtime timestamps to:
#     datetime.now(timezone.utc)
#
# Replaced utcnow() occurrences: 0
#
# No strategy logic changed.


# =========================
# PATCH NOTE v6.6.17
# =========================
# - Fixed TELEMETRY leadership context SQL ambiguity by qualifying ranked.snapshot_time.
# - Fixed BPT CQE lifecycle timezone subtraction by normalising now/opened_at with ensure_utc().
# - Keeps strategy logic unchanged.


# =========================
# PATCH NOTE v6.6.18
# =========================
# - Adds live leadership pressure from signals_raw momentum/trend/decision.
# - /leaders and /lifecycle now show moving live pressure and deltas.
# - Entry leadership context now prefers live pressure for score/delta snapshots.
# - leadership_state_history remains structural history; signals_raw supplies live market pressure.
# - Strategy thresholds unchanged except entry telemetry context source is made live.


# =========================
# PATCH NOTE v6.6.19
# =========================
# CQE CONFIRMED SCALE-IN ENGINE
#
# Architecture:
# Probe -> Confirm -> Scale
#
# - Tiny exploratory probe enters first.
# - Real capital added ONLY after CQE confirmation.
# - Restricted to:
#     HIGH_MONSTER_ROW
#     EXTREME_RUNNER_ROW
# - Requires live pressure still positive.
# - Requires live delta still positive/non-decaying.
# - Prevents scaling into dead volatility spikes.


# =========================
# PATCH NOTE v6.6.20
# =========================
# ADAPTIVE LEADERSHIP TREND ENGINE
#
# DISCOVERY:
# Strong ignition leaders were being blocked
# by legacy mature-trend thresholds.
#
# SOLUTION:
# Adaptive trend thresholds based on:
# - leadership score
# - CQE quality
# - lifecycle phase
#
# ADDITIONAL FIX:
# Minimum order protection added to prevent
# tiny OKX scale-ins / exits failing due to
# exchange minimum notional limits.

# =========================
# PATCH NOTE v6.6.21
# =========================
# FREE THE UNICORNS ENTRY OVERRIDE
#
# Data-backed medium version:
# - Defines Unicorn Candidate as:
#     Leadership 2 <= score < 3
#     CQE >= 4
#     Pressure >= 7
# - Live override ONLY for:
#     leadership_climax_delta_blocked
#     max_same_symbol_open
# - Same-symbol override allows one extra continuation slot:
#     normal MAX_SAME_SYMBOL_OPEN remains unchanged
#     unicorn continuation cap = UNICORN_MAX_SAME_SYMBOL_OPEN
# - Does NOT override:
#     leadership_trend_too_low
#     leadership_phase_not_tradable
#     max_open_trades
#     OKX position guard
#     control panel
#     exits
#     sizing

# =========================
# 🏆 COIN INTELLIGENCE v8.4 COMPATIBILITY WRAPPER
# =========================
def update_coin_score(conn, symbol, peak_pnl_percent=None, pnl_percent=None, pnl_gbp=None):
    """Backward-compatible wrapper. v8.4 recomputes coin_scores from closed history."""
    try:
        cur = conn.cursor()
        refresh_coin_learning_from_history(cur, symbol, allow_mode_change=True)
        conn.commit()
    except Exception as e:
        print(f"⚠️ coin score update failed: {e}", flush=True)

