import os
import time
import csv
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler

import ccxt
import yfinance as yf
import requests

# ================== Logging ==================
LOG_FILE = "bot.log"
logger = logging.getLogger("VolumeBot")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=7)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# ================== Load settings ==================
REFRESH_MINUTES = int(os.getenv("REFRESH_MINUTES", "15"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")

TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO = os.getenv("TWILIO_TO")

# ================== Alerts ==================
def send_alert(message: str):
    """Send alerts to Telegram, Discord, WhatsApp (Twilio)"""
    logger.info(message)

    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    if DISCORD_WEBHOOK:
        try:
            requests.post(DISCORD_WEBHOOK, json={"content": message})
        except Exception as e:
            logger.error(f"Discord error: {e}")

    if TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM and TWILIO_TO:
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(from_=TWILIO_FROM, to=TWILIO_TO, body=message)
        except Exception as e:
            logger.error(f"Twilio error: {e}")

# ================== Volume check ==================
def check_crypto(symbol="BTC/USDT", limit=200):
    """Fetch trades from Binance and split buy vs sell volume"""
    exchange = ccxt.binance()
    try:
        trades = exchange.fetch_trades(symbol, limit=limit)
        buy_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] == "buy")
        sell_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] == "sell")
        return buy_vol, sell_vol
    except Exception as e:
        logger.error(f"Crypto fetch error {symbol}: {e}")
        return 0, 0

def check_stock(symbol="AAPL"):
    """Approximate buy/sell split using tick rule on daily data"""
    try:
        data = yf.download(symbol, period="2d", interval="1d", progress=False)
        if len(data) < 2:
            return 0, 0
        vol = float(data["Volume"].iloc[-1])
        change = data["Close"].iloc[-1] - data["Close"].iloc[-2]
        if change >= 0:
            return vol, 0
        else:
            return 0, vol
    except Exception as e:
        logger.error(f"Stock fetch error {symbol}: {e}")
        return 0, 0

# ================== Save CSV ==================
def save_csv(rows, filename=None):
    if not filename:
        filename = f"alerts_{datetime.now().date()}.csv"
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["timestamp", "market", "buy_volume", "sell_volume", "net_flow", "message"])
        for row in rows:
            writer.writerow(row)

# ================== Main Loop ==================
def run_once():
    results = []

    # --- Crypto Example
    for symbol in ["BTC/USDT", "ETH/USDT", "XRP/USDT"]:
        buy, sell = check_crypto(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.0f}, Sell {sell:,.0f}, Net {net:,.0f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    # --- Stocks / Commodities Example
    for symbol in ["AAPL", "MSFT", "GOOG", "GC=F"]:  # GC=F = Gold futures
        buy, sell = check_stock(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.0f}, Sell {sell:,.0f}, Net {net:,.0f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    save_csv(results)

if __name__ == "__main__":
    logger.info("Starting Volume Bot")
    while True:
        run_once()
        if REFRESH_MINUTES <= 0:
            break
        logger.info(f"Sleeping {REFRESH_MINUTES} minutes...")
        time.sleep(REFRESH_MINUTES * 60)@echo off
cd C:\hv_bot
call venv\Scripts\activate
python high_volume_bot.py
pause

