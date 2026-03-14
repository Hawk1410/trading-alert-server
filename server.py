from flask import Flask, request
import json
from datetime import datetime

app = Flask(__name__)

@app.route("/", methods=["GET"])
def home():
    return "Trading Alert Server Running"

@app.route("/webhook", methods=["POST"])
def webhook():
    data = request.json
    
    print("Alert received:", data)

    with open("alerts_log.txt", "a") as f:
        f.write(f"{datetime.now()} - {json.dumps(data)}\n")

    return {"status": "received"}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
