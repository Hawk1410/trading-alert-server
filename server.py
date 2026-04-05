from flask import Flask, request, jsonify
import os
import psycopg2

app = Flask(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL")

# =========================
# DB CONNECTION
# =========================
def get_db():
    return psycopg2.connect(DATABASE_URL)

# =========================
# WEBHOOK (UNCHANGED)
# =========================
@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    print("📩 PAYLOAD:", data)

    # your existing trade logic continues here...

    return jsonify({"status": "received"}), 200


# =========================
# 🔥 MASTER DASH
# =========================
@app.route("/master_dashboard", methods=["GET"])
def master_dashboard():
    conn = get_db()
    cur = conn.cursor()

    cur.execute("""
        WITH closed AS (
            SELECT *
            FROM bot_trades
            WHERE status = 'CLOSED'
        ),
        open AS (
            SELECT symbol, COUNT(*) AS open_trades
            FROM bot_trades
            WHERE status = 'OPEN'
            GROUP BY symbol
        ),
        stats AS (
            SELECT
                symbol,
                COUNT(*) AS trades,
                ROUND(AVG(pnl_percent), 3) AS avg_pnl,
                ROUND(SUM(pnl_percent), 3) AS total_pnl,
                ROUND(AVG(CASE WHEN pnl_percent > 0 THEN 1 ELSE 0 END)::numeric, 3) AS winrate
            FROM closed
            GROUP BY symbol
        )
        SELECT
            s.symbol,
            s.trades,
            s.winrate,
            s.avg_pnl,
            s.total_pnl,
            COALESCE(o.open_trades, 0) AS open_trades,
            CASE
                WHEN s.trades >= 40 AND s.avg_pnl < 0 AND s.winrate < 0.45 THEN 'DROP'
                WHEN s.trades >= 40 AND s.avg_pnl > 0 AND s.winrate >= 0.5 THEN 'KEEP'
                ELSE 'TEST'
            END AS verdict
        FROM stats s
        LEFT JOIN open o ON s.symbol = o.symbol
        ORDER BY s.total_pnl DESC;
    """)

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    result = [dict(zip(cols, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify(result)


# =========================
# 🔥 TRIAL DASH
# =========================
@app.route("/trial_dashboard", methods=["GET"])
def trial_dashboard():
    conn = get_db()
    cur = conn.cursor()

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

    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]

    result = [dict(zip(cols, row)) for row in rows]

    cur.close()
    conn.close()

    return jsonify(result)


# =========================
# 🔥 FULL SYSTEM SNAPSHOT (THE BIG ONE)
# =========================
@app.route("/system_snapshot", methods=["GET"])
def system_snapshot():
    conn = get_db()
    cur = conn.cursor()

    # ===== MASTER =====
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

    # ===== OPEN TRADES =====
    cur.execute("""
        SELECT symbol, direction, entry_price, opened_at
        FROM bot_trades
        WHERE status = 'OPEN'
        ORDER BY opened_at DESC;
    """)
    open_trades = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # ===== RECENT TRADES =====
    cur.execute("""
        SELECT symbol, pnl_percent, opened_at, closed_at
        FROM bot_trades
        WHERE status = 'CLOSED'
        ORDER BY closed_at DESC
        LIMIT 50;
    """)
    recent = [dict(zip([d[0] for d in cur.description], row)) for row in cur.fetchall()]

    # ===== TRIAL =====
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

    cur.close()
    conn.close()

    return jsonify({
        "master": master,
        "open_trades": open_trades,
        "recent_trades": recent,
        "trial_coins": trial
    })


# =========================
# HEALTH CHECK
# =========================
@app.route("/", methods=["GET"])
def home():
    return "Bot is running 🚀"
    
