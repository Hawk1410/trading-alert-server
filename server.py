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

    try:
        data = request.get_json(force=True)
    except:
        data = {"message": request.data.decode("utf-8")}

    alert_entry = {
        "received_at": datetime.utcnow().isoformat() + "Z",
        "data": data
    }

    if os.path.exists(ALERTS_FILE):
        with open(ALERTS_FILE, "r") as f:
            try:
                alerts = json.load(f)
            except:
                alerts = []
    else:
        alerts = []

    alerts.append(alert_entry)

    with open(ALERTS_FILE, "w") as f:
        json.dump(alerts, f, indent=2)

    print("Alert received:", alert_entry)

    return {"status": "ok", "alerts_saved": len(alerts)}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
