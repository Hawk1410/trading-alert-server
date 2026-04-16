# =========================
# 🤖 BOT VERSION
# =========================
# VERSION: v3.9
# DEPLOYED: 2026-04-16
# NOTES:
# - Added Adaptive Coin Filter (3-day whitelist)
# - Added trade analytics tracking (peak pnl, exit momentum/trend)
# - Preserved market filter + all existing logic
# =========================

from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# ⚙️ LIVE CONFIG
# =========================
MAX_OPEN_TRADES = 7
CAPITAL_PER_TRADE = 60

ENABLE_COOLDOWN = True
COOLDOWN_MINUTES = 20
LOSS_STREAK_LIMIT = 2

# 🔥 MARKET FILTER (UNCHANGED)
ENABLE_MARKET_FILTER = True
MARKET_WINDOW_MINUTES = 15
MARKET_MIN_TRADES = 3
MARKET_MAX_WINRATE = 0.34
MARKET_MAX_AVG_PNL = -0.1
MARKET_COOLDOWN_MINUTES = 20

ENABLE_REGIME_FILTER = False
ENABLE_STACKING = False

ENABLE_EARLY_TREND = False
ENABLE_MOMENTUM_CAP = True
ENABLE_SWEET_SPOT = False
ENABLE_SMART_STACKING = False

# 🆕 ADAPTIVE COIN FILTER
ENABLE_ADAPTIVE_COIN_FILTER = True
COIN_LOOKBACK_DAYS = 3
MIN_COIN_TRADES = 15
MIN_COIN_PNL = 0

MIN_TREND = 0.20
MIN_MOM = 0.05

MOMENTUM_CAP = 0.8

SWEET_MIN_MOM = 0.2
SWEET_MAX_MOM = 0.6
SWEET_MAX_TREND = 0.2

STACK_STRONG_MOM = 0.35
STACK_STRONG_TREND = 0.15


def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🧠 ADAPTIVE COIN FILTER
# =========================
def get_allowed_symbols(cur):
    if not ENABLE_ADAPTIVE_COIN_FILTER:
        return None

    cur.execute(f"""
        SELECT symbol
        FROM (
            SELECT symbol,
                   COUNT(*) AS trades,
                   SUM(pnl_percent) AS total_pnl
            FROM bot_trades
            WHERE status = 'CLOSED'
            AND opened_at >= NOW() - INTERVAL '{COIN_LOOKBACK_DAYS} days'
            GROUP BY symbol
        ) t
        WHERE trades >= %s
        AND total_pnl > %s
    """, (MIN_COIN_TRADES, MIN_COIN_PNL))

    return set(r[0] for r in cur.fetchall())


@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json(force=True)

        symbol = data.get("symbol")
        decision = data.get("decision_model")
        price = float(data.get("price", 0))
        momentum = float(data.get("momentum_strength", 0))
        trend = float(data.get("trend_strength", 0))
        alignment = data.get("trend_alignment")
        data_version = data.get("data_version")

        if decision:
            decision = decision.upper().strip()

        if decision in ["NONE", "", "NULL"]:
            decision = None

        conn = get_db()
        cur = conn.cursor()
        now = datetime.utcnow()

        abs_mom = abs(momentum)
        abs_trend = abs(trend)

        hold_reason = None

        # =========================
        # 🆕 ADAPTIVE COIN FILTER
        # =========================
        allowed_symbols = get_allowed_symbols(cur)
        if ENABLE_ADAPTIVE_COIN_FILTER and allowed_symbols is not None:
            if symbol not in allowed_symbols:
                hold_reason = "coin_not_allowed"

        # =========================
        # 🧠 MARKET DANGER FILTER (UNCHANGED)
        # =========================
        if hold_reason is None and ENABLE_MARKET_FILTER:
            window_start = now - timedelta(minutes=MARKET_WINDOW_MINUTES)

            cur.execute("""
                SELECT 
                    COUNT(*),
                    AVG(pnl_percent),
                    AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END)
                FROM bot_trades
                WHERE status = 'CLOSED'
                AND closed_at >= %s
            """, (window_start,))

            count, avg_pnl, winrate = cur.fetchone()

            if count and count >= MARKET_MIN_TRADES:
                if (winrate is not None and winrate <= MARKET_MAX_WINRATE) or \
                   (avg_pnl is not None and avg_pnl <= MARKET_MAX_AVG_PNL):

                    cur.execute("""
                        SELECT MAX(closed_at)
                        FROM bot_trades
                        WHERE status = 'CLOSED'
                        AND closed_at >= %s
                    """, (window_start,))
                    last_event = cur.fetchone()[0]

                    if last_event:
                        mins = (now - last_event).total_seconds() / 60
                        if mins < MARKET_COOLDOWN_MINUTES:
                            hold_reason = "market_danger"

        # =========================
        # 🧠 COOLDOWN
        # =========================
        if hold_reason is None and ENABLE_COOLDOWN:
            cur.execute("""
                SELECT pnl_percent, closed_at
                FROM bot_trades
                WHERE status = 'CLOSED'
                ORDER BY closed_at DESC
                LIMIT %s
            """, (LOSS_STREAK_LIMIT,))
            recent = cur.fetchall()

            if len(recent) == LOSS_STREAK_LIMIT:
                if all(r[0] < 0 for r in recent if r[0] is not None):
                    last_time = recent[0][1]
                    mins = (now - last_time).total_seconds() / 60
                    if mins < COOLDOWN_MINUTES:
                        hold_reason = "cooldown_active"

        # =========================
        # 🔥 ENTRY FILTER (UNCHANGED)
        # =========================
        if hold_reason is None:

            if decision not in ["LONG", "SHORT"]:
                hold_reason = "no_decision"

            elif alignment != "aligned":
                hold_reason = "counter_trend"

            elif abs_trend < MIN_TREND:
                hold_reason = "not_strong_trend"

            elif abs_mom < MIN_MOM:
                hold_reason = "momentum_too_weak"

            elif abs_mom > 2.5:
                hold_reason = "extreme_momentum"

            elif ENABLE_MOMENTUM_CAP and abs_mom > MOMENTUM_CAP:
                hold_reason = "overextended"

            elif ENABLE_SWEET_SPOT:
                if not (SWEET_MIN_MOM <= abs_mom <= SWEET_MAX_MOM and abs_trend <= SWEET_MAX_TREND):
                    hold_reason = "outside_sweet_spot"

        # =========================
        # 🔒 GLOBAL LIMIT
        # =========================
        if hold_reason is None:
            cur.execute("SELECT COUNT(*) FROM bot_trades WHERE status='OPEN'")
            if cur.fetchone()[0] >= MAX_OPEN_TRADES:
                hold_reason = "max_open_trades"

        # =========================
        # 🔁 STACKING CONTROL
        # =========================
        if hold_reason is None:
            cur.execute("""
                SELECT entry_price, opened_at
                FROM bot_trades
                WHERE symbol=%s AND status='OPEN'
                ORDER BY opened_at ASC
            """, (symbol,))
            existing = cur.fetchall()

            if ENABLE_STACKING:

                if len(existing) >= 2:
                    hold_reason = "too_many_positions"

                elif len(existing) == 1:
                    first_price, first_time = existing[0]

                    if now - first_time < timedelta(minutes=20):
                        hold_reason = "too_soon"

                    else:
                        worse_price = (
                            (decision == "LONG" and price >= first_price) or
                            (decision == "SHORT" and price <= first_price)
                        )

                        if worse_price:

                            if ENABLE_SMART_STACKING:
                                if abs_mom >= STACK_STRONG_MOM and abs_trend >= STACK_STRONG_TREND:
                                    pass
                                else:
                                    hold_reason = "no_better_price"
                            else:
                                hold_reason = "no_better_price"

            else:
                if len(existing) >= 1:
                    hold_reason = "stacking_disabled"

        # =========================
        # 💾 SAVE SIGNAL
        # =========================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason
        ))

        # =========================
        # 🚀 OPEN TRADE
        # =========================
        if hold_reason is None:
            stop_price = price * (0.996 if decision == "LONG" else 1.004)
            target_price = price * (1.008 if decision == "LONG" else 0.992)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    stop_price, target_price,
                    status, data_version, opened_at,
                    peak_pnl_percent
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW(),0)
            """, (
                symbol, decision, price,
                stop_price, target_price, data_version
            ))

            print(f"🚀 OPEN: {symbol}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        # =========================
        # 🧠 EXIT ENGINE + TRACKING
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price, opened_at, peak_pnl_percent
            FROM bot_trades
            WHERE status='OPEN'
        """)
        open_trades = cur.fetchall()

        for tid, sym, direction, entry_price, opened_at, peak_pnl in open_trades:

            if sym != symbol:
                continue

            pnl = ((price - entry_price) / entry_price) if direction == "LONG" \
                  else ((entry_price - price) / entry_price)

            # 🆕 TRACK PEAK
            if pnl * 100 > (peak_pnl or 0):
                cur.execute("""
                    UPDATE bot_trades
                    SET peak_pnl_percent = %s
                    WHERE id = %s
                """, (pnl * 100, tid))

            mins = (now - opened_at).total_seconds() / 60
            close_reason = None

            if pnl < -0.004:
                close_reason = "hard_stop"

            elif pnl > 0.004 and mins < 10:
                close_reason = "quick_profit"

            elif pnl > 0 and abs_mom < 0.1:
                close_reason = "momentum_drop"

            elif pnl > 0 and alignment != "aligned":
                close_reason = "trend_flip"

            elif mins > 20 and abs(pnl) < 0.001:
                close_reason = "no_follow_through"

            elif ENABLE_REGIME_FILTER and abs_trend < 0.15:
                close_reason = "regime_exit"

            elif mins > 60:
                close_reason = "time_cut"

            if close_reason:
                cur.execute("""
                    UPDATE bot_trades
                    SET status='CLOSED',
                        closed_at=NOW(),
                        pnl_percent=%s,
                        close_reason=%s,
                        exit_momentum=%s,
                        exit_trend=%s
                    WHERE id=%s
                """, (pnl * 100, close_reason, momentum, trend, tid))

                print(f"💰 CLOSED: {sym} | {close_reason} | {round(pnl*100,3)}%")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "processed"}), 200

    except Exception as e:
        print("❌ WEBHOOK ERROR:", e)
        return jsonify({"error": str(e)}), 400
