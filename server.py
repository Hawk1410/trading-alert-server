from flask import Flask, request
import csv
import os
from datetime import datetime

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Trading Alert Server Running"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json or {}

    print("Alert received:", data)

    file_exists = os.path.isfile("trade_signals.csv")

    with open("trade_signals.csv", "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["received_at", "symbol", "price", "time", "exchange"])
        writer.writerow([
            datetime.utcnow().isoformat(),
            data.get("symbol", ""),
            data.get("price", ""),
            data.get("time", ""),
            data.get("exchange", "")
        ])

    return {"status": "received"}, 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
