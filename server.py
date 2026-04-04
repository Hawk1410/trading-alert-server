from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime
import requests

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

STOP_LOSS = 0.4   # %
TAKE_PROFIT = 0.8 # %

DATA_VERSION = "v2_final"


def log(msg):
    print(msg, flush=True)


def get_connection():
    return psycopg2.connect(DATABASE_URL)


def get_live_price(symbol):
    try:
        url = f"https://api.binance.com/api/v3/ticker/price?symbol={symbol}"
        return float(requests.get(url, timeout=2).json()["price"])
    except:
        return None


@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    log(f"📩 DATA_V2 PAYLOAD: {data}")

    try:
        # ================================
        # INPUTS
        # ================================
        symbol = data.get("symbol")
        price = float(data.get("price", 0))
        decision = data.get("decision_model", "NONE")

        trend_alignment = data.get("trend_alignment", "unknown")
        momentum_strength = float(data.get("momentum_strength", 0))
        trend_strength = float(data.get("trend_strength", 0))
        vwap_bucket = data.get("vwap_distance_bucket", "unknown")

        timeframe = data.get("timeframe", "unknown")
        model_version = data.get("model_version", "v2_final_atr")

        conn = get_connection()
        cur = conn.cursor()

        # ================================
        # LOAD CONFIG
        # ================================
        cur.execute("""
            SELECT min_momentum, min_trend_strength, trade_timeframe
            FROM bot_config
            WHERE id = 1
        """)
        config = cur.fetchone()

        MIN_MOMENTUM, MIN_TREND_STRENGTH, TRADE_TIMEFRAME = config

        # ================================
        # HOLD REASON ENGINE (V2.2)
        # ================================
        hold_reason = None

        if decision == "NONE":
            hold_reason = "no_decision"

        elif abs(momentum_strength) < MIN_MOMENTUM:
            hold_reason = "weak_momentum"

        # ✅ FIXED: only block TRUE extremes (both directions)
        elif abs(momentum_strength) > 1.0:
            log(f"🚨 EXTREME MOMENTUM BLOCKED: {momentum_strength}")
            hold_reason = "too_strong_momentum"

        elif abs(trend_strength) < MIN_TREND_STRENGTH:
            hold_reason = "weak_trend"

        elif trend_alignment != "aligned":
            hold_reason = "counter_trend"

        # 🚨 TIMEFRAME FILTER
        if timeframe != TRADE_TIMEFRAME:
            hold_reason = "not_" + str(TRADE_TIMEFRAME)

        # ================================
        # CLOSE EXISTING TRADES
        # ================================
        cur.execute("""
            SELECT id, symbol, direction, entry_price
            FROM bot_trades
            WHERE status = 'OPEN'
            AND data_version = %s
        """, (DATA_VERSION,))

        for trade_id, sym, direction, entry_price in cur.fetchall():

            live_price = get_live_price(sym)
            if not live_price:
                continue

            pnl = ((live_price - entry_price) / entry_price) * 100
            if direction == "SHORT":
                pnl = -pnl

            if pnl <= -STOP_LOSS or pnl >= TAKE_PROFIT:

                if pnl >= TAKE_PROFIT:
                    close_reason = "take_profit"
                else:
                    close_reason = "stop_loss"

                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = %s,
                        close_price = %s,
                        close_reason = %s,
                        pnl_percent = %s
                    WHERE id = %s
                """, (
                    datetime.utcnow(),
                    live_price,
                    close_reason,
                    pnl,
                    trade_id
                ))

                log(f"💥 CLOSED {sym} {close_reason} {pnl:.2f}%")

        conn.commit()

        trade_taken = False

        # ================================
        # FILTER (NO TRADE)
        # ================================
        if hold_reason:
            cur.execute("""
                INSERT INTO signal_history_v2 (
                    symbol, price, decision_model, trade_taken,
                    trend_alignment, momentum_strength, trend_strength,
                    vwap_distance_bucket, hold_reason,
                    timeframe, model_version, data_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, price, decision, False,
                trend_alignment, momentum_strength, trend_strength,
                vwap_bucket, hold_reason,
                timeframe, model_version, DATA_VERSION
            ))

            conn.commit()
            cur.close()
            conn.close()

            log(f"🛑 FILTERED: {hold_reason}")

            return jsonify({"status": "filtered", "reason": hold_reason})

        # ================================
        # CHECK EXISTING TRADE
        # ================================
        cur.execute("""
            SELECT id FROM bot_trades
            WHERE symbol = %s AND status = 'OPEN'
            AND data_version = %s
        """, (symbol, DATA_VERSION))

        if cur.fetchone():
            cur.execute("""
                INSERT INTO signal_history_v2 (
                    symbol, price, decision_model, trade_taken,
                    trend_alignment, momentum_strength, trend_strength,
                    vwap_distance_bucket, hold_reason,
                    timeframe, model_version, data_version
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                symbol, price, decision, False,
                trend_alignment, momentum_strength, trend_strength,
                vwap_bucket, "trade_exists",
                timeframe, model_version, DATA_VERSION
            ))

            conn.commit()
            cur.close()
            conn.close()

            return jsonify({"status": "exists"})

        # ================================
        # CALCULATE SL / TP
        # ================================
        if decision == "LONG":
            stop_price = price * (1 - STOP_LOSS / 100)
            target_price = price * (1 + TAKE_PROFIT / 100)

        elif decision == "SHORT":
            stop_price = price * (1 + STOP_LOSS / 100)
            target_price = price * (1 - TAKE_PROFIT / 100)

        # ================================
        # OPEN TRADE
        # ================================
        cur.execute("""
            INSERT INTO bot_trades (
                symbol, direction, entry_price, stop_price, target_price,
                status, opened_at,
                strategy_type, model_version, trend_alignment,
                momentum_strength, trend_strength,
                vwap_bucket, data_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol,
            decision,
            price,
            stop_price,
            target_price,
            'OPEN',
            datetime.utcnow(),
            "trend" if trend_alignment == "aligned" else "counter",
            model_version,
            trend_alignment,
            momentum_strength,
            trend_strength,
            vwap_bucket,
            DATA_VERSION
        ))

        trade_taken = True

        # ================================
        # LOG SIGNAL
        # ================================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, price, decision_model, trade_taken,
                trend_alignment, momentum_strength, trend_strength,
                vwap_distance_bucket, hold_reason,
                timeframe, model_version, data_version
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            symbol, price, decision, True,
            trend_alignment, momentum_strength, trend_strength,
            vwap_bucket, "trade_opened",
            timeframe, model_version, DATA_VERSION
        ))

        conn.commit()
        cur.close()
        conn.close()

        log(f"🚀 TRADE OPENED: {symbol} {decision}")

        return jsonify({"status": "opened"})

    except Exception as e:
        log(f"❌ ERROR: {e}")
        return jsonify({"error": str(e)}), 200


@app.route("/")
def home():
    return "V2.2 SMART MOMENTUM FILTER LIVE 🚀"
