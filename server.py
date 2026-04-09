from flask import Flask, request, jsonify
import os
import psycopg2
from datetime import datetime, timedelta

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    return psycopg2.connect(DATABASE_URL)


# =========================
# 🏠 HEALTH ROUTES (FIX)
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀", 200

@app.route("/ping", methods=["GET"])
def ping():
    return "pong", 200


# =========================
# 📊 SAFE SNAPSHOT (LIGHT VERSION)
# =========================
@app.route("/system_snapshot", methods=["GET"])
def system_snapshot():
    try:
        conn = get_db()
        cur = conn.cursor()

        # Keep this LIGHT to avoid Render timeouts
        cur.execute("""
            SELECT COUNT(*) FROM bot_trades
            WHERE status = 'OPEN'
        """)
        open_trades = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM signal_history_v2
            WHERE created_at > NOW() - INTERVAL '1 hour'
        """)
        signals_1h = cur.fetchone()[0]

        cur.execute("""
            SELECT COUNT(*) FROM bot_trades
            WHERE opened_at > NOW() - INTERVAL '1 hour'
        """)
        trades_1h = cur.fetchone()[0]

        cur.close()
        conn.close()

        return jsonify({
            "status": "ok",
            "open_trades": open_trades,
            "signals_last_hour": signals_1h,
            "trades_last_hour": trades_1h
        }), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500


# =========================
# 🚀 WEBHOOK (ENTRY + EXIT ENGINE v2.6.2)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📩 PAYLOAD:", data)

    symbol = data.get("symbol")
    decision = data.get("decision_model")
    price = data.get("price")
    momentum = data.get("momentum_strength")
    trend = data.get("trend_strength")
    alignment = data.get("trend_alignment")
    data_version = data.get("data_version")
    vwap_bucket = data.get("vwap_distance_bucket")

    conn = get_db()
    cur = conn.cursor()

    now = datetime.utcnow()

    try:
        # =========================
        # 🧠 TAGGING ENGINE
        # =========================
        abs_mom = abs(momentum) if momentum is not None else 0
        abs_trend = abs(trend) if trend is not None else 0

        if abs_trend > 0.25:
            trend_quality = "strong"
        elif abs_trend > 0.12:
            trend_quality = "medium"
        else:
            trend_quality = "weak"

        if abs_mom > 0.45:
            momentum_state = "impulse"
        elif abs_mom > 0.2:
            momentum_state = "build"
        else:
            momentum_state = "weak"

        score = 0
        if abs_mom > 0.45:
            score += 2
        elif abs_mom > 0.2:
            score += 1

        if abs_trend > 0.25:
            score += 2
        elif abs_trend > 0.12:
            score += 1

        if alignment == "aligned":
            score += 1

        if score >= 5:
            signal_quality = "A+"
        elif score >= 3:
            signal_quality = "B"
        else:
            signal_quality = "C"

        # =========================
        # 🧠 EXIT ENGINE
        # =========================
        cur.execute("""
            SELECT id, symbol, direction, entry_price,
                   stop_price, target_price, opened_at
            FROM bot_trades
            WHERE status = 'OPEN'
        """)

        open_trades = cur.fetchall()

        for trade in open_trades:
            trade_id, t_symbol, direction, entry, stop, target, opened = trade

            if t_symbol != symbol:
                continue

            duration = (now - opened).total_seconds()

            pnl = ((price - entry) / entry * 100) if direction == "LONG" else ((entry - price) / entry * 100)

            close = False
            reason = None

            if direction == "LONG":
                if price >= target:
                    close = True
                    reason = "tp_hit"
                elif price <= stop:
                    close = True
                    reason = "sl_hit"
            else:
                if price <= target:
                    close = True
                    reason = "tp_hit"
                elif price >= stop:
                    close = True
                    reason = "sl_hit"

            if not close and duration > 21600 and pnl < 0.2:
                close = True
                reason = "timeout_weak"

            if not close and duration > 43200:
                close = True
                reason = "max_duration"

            if close:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = NOW(),
                        pnl_percent = %s
                    WHERE id = %s
                """, (pnl, trade_id))

                print(f"🔒 CLOSED: {t_symbol} | {reason} | PnL: {pnl:.3f}")

        # =========================
        # 🚫 ENTRY FILTER LOGIC
        # =========================
        hold_reason = None

        if decision not in ["LONG", "SHORT"]:
            hold_reason = "no_decision"

        elif alignment != "aligned":
            hold_reason = "counter_trend"

        if hold_reason is None:
            cur.execute("""
                SELECT entry_price, opened_at
                FROM bot_trades
                WHERE symbol = %s AND status = 'OPEN'
                ORDER BY opened_at ASC
            """, (symbol,))
            existing_trades = cur.fetchall()

            open_count = len(existing_trades)

            if open_count >= 2:
                hold_reason = "trade_exists"

            elif open_count == 1:
                first_entry, first_time = existing_trades[0]

                if now - first_time < timedelta(minutes=20):
                    hold_reason = "too_soon"

                elif decision == "LONG" and price >= first_entry:
                    hold_reason = "no_better_price"

                elif decision == "SHORT" and price <= first_entry:
                    hold_reason = "no_better_price"

        # =========================
        # 💾 SAVE SIGNAL
        # =========================
        cur.execute("""
            INSERT INTO signal_history_v2 (
                symbol, decision_model, momentum_strength, trend_strength,
                trend_alignment, price, data_version, hold_reason,
                signal_quality, trend_quality, momentum_state, signal_score, created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
        """, (
            symbol, decision, momentum, trend,
            alignment, price, data_version, hold_reason,
            signal_quality, trend_quality, momentum_state, score
        ))

        # =========================
        # ✅ EXECUTE TRADE
        # =========================
        if hold_reason is None:

            if decision == "LONG":
                stop_price = price * (1 - 0.004)
                target_price = price * (1 + 0.008)
            else:
                stop_price = price * (1 + 0.004)
                target_price = price * (1 - 0.008)

            cur.execute("""
                INSERT INTO bot_trades (
                    symbol, direction, entry_price,
                    stop_price, target_price,
                    status, data_version, opened_at
                )
                VALUES (%s,%s,%s,%s,%s,'OPEN',%s,NOW())
            """, (
                symbol, decision, price,
                stop_price, target_price, data_version
            ))

            print(f"🚀 TRADE OPENED: {symbol} {decision} @ {price} | {signal_quality}")

        else:
            print(f"⛔ BLOCKED: {symbol} | {hold_reason}")

        conn.commit()

    except Exception as e:
        conn.rollback()
        print("❌ ERROR:", e)

    finally:
        cur.close()
        conn.close()

    return jsonify({"status": "processed"}), 200
