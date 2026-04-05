from flask import Flask, request, jsonify
import os
import psycopg2

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# WEBHOOK
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📩 PAYLOAD:", data)
    return jsonify({"status": "received"}), 200


# =========================
# 🔥 SYSTEM SNAPSHOT (FINAL FIXED)
# =========================
@app.route("/system_snapshot", methods=["GET"])
def system_snapshot():
    conn = get_db()
    cur = conn.cursor()

    # =========================
    # MASTER (bot_trades)
    # =========================
    cur.execute("""
        SELECT
            symbol,
            COUNT(*) AS trades,
            ROUND(AVG(pnl_percent), 3) AS avg_pnl,
            ROUND(SUM(pnl_percent), 3) AS total_pnl,
            ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate
        FROM bot_trades
        WHERE status = 'CLOSED'
        GROUP BY symbol
        ORDER BY total_pnl DESC;
    """)
    master = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # OPEN TRADES
    # =========================
    cur.execute("""
        SELECT symbol, direction, entry_price, opened_at
        FROM bot_trades
        WHERE status = 'OPEN'
        ORDER BY opened_at DESC;
    """)
    open_trades = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # RECENT TRADES
    # =========================
    cur.execute("""
        SELECT symbol, pnl_percent, opened_at, closed_at
        FROM bot_trades
        WHERE status = 'CLOSED'
        ORDER BY closed_at DESC
        LIMIT 50;
    """)
    recent = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # TRIAL COINS
    # =========================
    cur.execute("""
        SELECT
            symbol,
            COUNT(*) AS trades,
            ROUND(AVG(pnl_percent),3) AS avg_pnl
        FROM bot_trades
        WHERE data_version = 'expansion_v1'
        AND status = 'CLOSED'
        GROUP BY symbol
        ORDER BY avg_pnl DESC;
    """)
    trial = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # 🧠 FILTER SUMMARY (FIXED)
    # =========================
    cur.execute("""
        SELECT
            hold_reason,
            COUNT(*) AS count
        FROM signal_history_v2
        WHERE decision_model = 'NONE'
        GROUP BY hold_reason
        ORDER BY count DESC;
    """)
    filter_summary = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # 🧠 CONDITION PERFORMANCE (FIXED)
    # =========================
    cur.execute("""
        SELECT
            vwap_distance_bucket,
            COUNT(*) FILTER (WHERE decision_model != 'NONE') AS taken,
            COUNT(*) FILTER (WHERE decision_model = 'NONE') AS missed
        FROM signal_history_v2
        GROUP BY vwap_distance_bucket
        ORDER BY taken DESC;
    """)
    condition_perf = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # 🧠 DECISION DISTRIBUTION
    # =========================
    cur.execute("""
        SELECT
            decision_model,
            COUNT(*) AS count
        FROM signal_history_v2
        GROUP BY decision_model
        ORDER BY count DESC;
    """)
    decisions = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # =========================
    # 🧠 EXPOSURE
    # =========================
    cur.execute("""
        SELECT
            symbol,
            COUNT(*) AS open_trades
        FROM bot_trades
        WHERE status = 'OPEN'
        GROUP BY symbol;
    """)
    exposure = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    cur.close()
    conn.close()

    return jsonify({
        "master": master,
        "open_trades": open_trades,
        "recent_trades": recent,
        "trial_coins": trial,
        "filter_summary": filter_summary,
        "condition_performance": condition_perf,
        "decision_distribution": decisions,
        "exposure": exposure
    })


# =========================
# HEALTH CHECK
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀"
