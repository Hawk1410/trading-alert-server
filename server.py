from flask import Flask, request
import json
import os
from datetime import datetime

app = Flask(__name__)

ALERTS_FILE = "alerts.json"


@app.route("/", methods=["GET"])
def home():
    return "Trading Alert Server Running"


@app.route("/webhook", methods=["POST"])
def webhook():

    # Accept both JSON and plain text alerts
    data = request.get_json(silent=True)

    if data is None:
        data = {"message": request.data.decode("utf-8")}

    alert_entry = {
        "received_at": datetime.utcnow().isoformat() + "Z",
        "data": data
    }

    # Load existing alerts
    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE, "r") as f:
            try:
                alerts = json.load(f)
            except json.JSONDecodeError:
                alerts = []
    else:
        alerts = []

    # Add new alert
    alerts.append(alert_entry)

    # Save alerts
    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)

    print("Alert received:", data)

    return {"status": "received", "saved_alerts": len(alerts)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
