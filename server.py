from flask import Flask, request, jsonify
import os
import psycopg2
import traceback

app = Flask(__name__)

# === DATABASE CONNECTION ===
DATABASE_URL = os.environ.get("DATABASE_URL")

def get_db_connection():
    return psycopg2.connect(DATABASE_URL)

# === SAFE FLOAT HELPER ===
def safe_float(value):
    try:
        return float(value)
    except:
        return None

# === WEBHOOK ENDPOINT ===
@app.route("/webhook", methods=["POST"])
def webhook():
    try:
        data = request.get_json()

        print("Incoming data:", data)

        conn = get_db_connection()
        cur = conn.cursor()

        # === EXTRACT VALUES ===
        model_version = data.get("model_version")
        decision_model = data.get("decision_model")
        symbol = data.get("symbol")

        price = safe_float(data.get("price"))
        entry_price = price  # same as price for now

        vwap_distance_bucket = data.get("vwap_distance_bucket")
        trend_alignment = data.get("trend_alignment")

        momentum = data.get("momentum")
        momentum_strength = safe_float(data.get("momentum_strength"))

        atr = safe_float(data.get("atr"))
        trend_strength = safe_float(data.get("trend_strength"))

        # === INSERT INTO DATABASE ===
        cur.execute("""
            INSERT INTO signal_history (
                model_version,
                decision_model,
                symbol,
                price,
                entry_price,
                vwap_distance_bucket,
                trend_alignment,
                momentum,
                momentum_strength,
                atr,
                trend_strength
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            model_version,
            decision_model,
            symbol,
            price,
            entry_price,
            vwap_distance_bucket,
            trend_alignment,
            momentum,
            momentum_strength,
            atr,
            trend_strength
        ))

        conn.commit()
        cur.close()
        conn.close()

        return jsonify({"status": "success"}), 200

    except Exception as e:
        print("ERROR:", str(e))
        print(traceback.format_exc())
        return jsonify({"status": "error", "message": str(e)}), 500


# === HEALTH CHECK ===
@app.route("/")
def home():
    return "Trading bot is running 🚀", 200


# === RUN APP ===
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
