from flask import Flask, request

app = Flask(__name__)

@app.route('/webhook', methods=['POST'])
def webhook():

    data = request.json

    symbol = data["symbol"]
    price = float(data["price"])
    vwap = float(data["vwap"])

    distance = ((price - vwap) / vwap) * 100

    decision = "HOLD"

    if distance < -0.3:
        decision = "LONG"

    if distance > 0.3:
        decision = "SHORT"

    print(f"""
Signal received
Symbol: {symbol}
Price: {price}
VWAP: {vwap}
Distance from VWAP: {distance:.3f}%
Decision: {decision}
""")

    return {"status": "ok"}
