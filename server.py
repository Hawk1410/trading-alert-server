from flask import Flask, request, jsonify
import os
import psycopg2
import traceback
from datetime import datetime

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# === STRATEGY CONFIG ===
STOP_LOSS = 0.4     # %
TAKE_PROFIT = 0.8   # %

# === DB CONNECTION ===
def get_db_connection():
    return psycopg2.connect(DATABASE_URL)


@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.json
        print("📩 Incoming:", data)

        # =========================
        # 🧹 SAFE PARSING
        # =========================

        symbol = data.get("symbol")

        try:
            price = float(data.get("price", 0))
        except:
            price = 0

        if price <= 0:
            print("❌ Invalid price, skipping")
            return jsonify({"status": "invalid price"})

        decision_model = str(data.get("decision_model", "")).strip().upper()
        model_version = data.get("model_version")

        trend_alignment = data.get("trend_alignment")

        try:
            momentum = float(data.get("momentum", 0))
        except:
            momentum = 0

        trend_strength_raw = data.get("trend_strength")
        try:
            trend_strength = float(trend_strength_raw)
        except:
            trend_strength = None

        conn = get_db_connection()
        cur = conn.cursor()

        # =========================
        # 🔥 CLOSE EXISTING TRADES
        # =========================

        cur.execute("""
            SELECT id, entry_price, direction
            FROM bot_trades
            WHERE status = 'OPEN'
            AND symbol = %s
        """, (symbol,))

        open_trades = cur.fetchall()

        for trade_id, entry_price, direction in open_trades:

            if direction == "LONG":
                pnl = ((price - entry_price) / entry_price) * 100
            else:
                pnl = ((entry_price - price) / entry_price) * 100

            if pnl >= TAKE_PROFIT or pnl <= -STOP_LOSS:
                cur.execute("""
                    UPDATE bot_trades
                    SET status = 'CLOSED',
                        closed_at = %s,
                        pnl_percent = %s
                    WHERE id = %s
                """, (datetime.utcnow(), pnl, trade_id))

                print(f"✅ CLOSED TRADE {trade_id} | PnL: {pnl:.2f}%")

        # =========================
        # 📊 SAVE SIGNAL
        # =========================

        cur.execute("""
            INSERT INTO signal_history (
                symbol,
                price,
                decision_model,
                model_version,
                trend_alignment,
                momentum,
                trend_strength,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            symbol,
            price,
            decision_model,
            model_version,
            trend_alignment,
            momentum,
            trend_strength,
            datetime.utcnow()
        ))

        # =========================
        # 🎯 ENTRY LOGIC
        # =========================

        take_trade = False

        if decision_model in ["LONG", "SHORT"]:
            if trend_alignment == "aligned":
                if momentum > 0:
                    if trend_strength is None or trend_strength > 0:
                        take_trade = True

        print(f"DEBUG → align={trend_alignment} momentum={momentum} strength={trend_strength} take={take_trade}")

        # =========================
        # 🚫 PREVENT DUPLICATES
        # =========================

        if take_trade:
            cur.execute("""
                SELECT COUNT(*) FROM bot_trades
                WHERE symbol = %s AND status = 'OPEN'
            """, (symbol,))

            existing = cur.fetchone()[0]

            if existing == 0:
                cur.execute("""
                    INSERT INTO bot_trades (
                        symbol,
                        direction,
                        entry_price,
                        status,
                        opened_at
                    )
                    VALUES (%s,%s,%s,'OPEN',%s)
                """, (
                    symbol,
                    decision_model,
                    price,
                    datetime.utcnow()
                ))

                print("🚀 NEW TRADE OPENED")

            else:
                print("⚠️ Trade already open — skipping")

        else:
            print("⛔ FILTERED")

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "ok"})

    except Exception as e:
        print("❌ ERROR:", str(e))
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route("/")
def home():
    return "🚀 Trading bot is live!"
