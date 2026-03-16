from flask import Flask, request
import json
from datetime import datetime

app = Flask(__name__)

# --- Trading State ---
trade_open = False
trade_direction = None
entry_price = None
stop_price = None
target_price = None

# --- Strategy Settings ---
LONG_THRESHOLD = -0.3
SHORT_THRESHOLD = 0.3
STOP_LOSS = 0.4
TAKE_PROFIT = 0.8

import os
import psycopg2
from flask import Flask, request, jsonify

app = Flask(__name__)

def get_db_connection():
    return psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD")
    )


@app.route("/")
def home():
    return "Trading bot running"


@app.route("/webhook", methods=["POST"])
def webhook():

    global trade_open, trade_direction, entry_price, stop_price, target_price

    data = request.json

    price = float(data["price"])
    vwap = float(data["vwap"])
    symbol = data["symbol"]

    distance = ((price - vwap) / vwap) * 100

    print("\n==============================")
    print("Signal received")
    print("Symbol:", symbol)
    print("Price:", price)
    print("VWAP:", vwap)
    print("Distance from VWAP:", round(distance, 3), "%")

    # --- ENTRY LOGIC ---
    if not trade_open:

        if distance < LONG_THRESHOLD:

            trade_open = True
            trade_direction = "LONG"
            entry_price = price
            stop_price = entry_price * (1 - STOP_LOSS/100)
            target_price = entry_price * (1 + TAKE_PROFIT/100)

            print("\nTRADE OPENED")
            print("Direction: LONG")
            print("Entry:", entry_price)
            print("Stop:", stop_price)
            print("Target:", target_price)

        elif distance > SHORT_THRESHOLD:

            trade_open = True
            trade_direction = "SHORT"
            entry_price = price
            stop_price = entry_price * (1 + STOP_LOSS/100)
            target_price = entry_price * (1 - TAKE_PROFIT/100)

            print("\nTRADE OPENED")
            print("Direction: SHORT")
            print("Entry:", entry_price)
            print("Stop:", stop_price)
            print("Target:", target_price)

        else:
            print("Decision: HOLD")

    # --- TRADE MANAGEMENT ---
    else:

        print("\nTrade currently open:", trade_direction)

        if trade_direction == "LONG":

            if price <= stop_price:
                print("\nTRADE CLOSED - STOP LOSS HIT")
                trade_open = False

            elif price >= target_price:
                print("\nTRADE CLOSED - TARGET HIT")
                trade_open = False

        elif trade_direction == "SHORT":

            if price >= stop_price:
                print("\nTRADE CLOSED - STOP LOSS HIT")
                trade_open = False

            elif price <= target_price:
                print("\nTRADE CLOSED - TARGET HIT")
                trade_open = False

    return "ok"

import os

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port)
