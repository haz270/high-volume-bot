import os
import csv
import logging
from datetime import datetime
from logging.handlers import RotatingFileHandler
import ccxt
import yfinance as yf
import requests
import pandas as pd

# ================== Logging ==================
LOG_FILE = "bot.log"
logger = logging.getLogger("VolumeBot")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5*1024*1024, backupCount=7)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# ================== Settings ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

BINANCE_API_KEY = os.getenv("BINANCE_API_KEY")
BINANCE_API_SECRET = os.getenv("BINANCE_API_SECRET")

# ================== Alerts ==================
def send_alert(message: str):
    logger.info(message)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# ================== Crypto Volume ==================
def check_crypto(symbol="BTC/USDT", limit=200):
    """Fetch real buy/sell volume using Binance trades"""
    try:
        exchange = ccxt.binance({
            "apiKey": BINANCE_API_KEY,
            "secret": BINANCE_API_SECRET,
            "enableRateLimit": True
        })
        trades = exchange.fetch_trades(symbol, limit=limit)
        buy_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] == "buy")
        sell_vol = sum(t["amount"] * t["price"] for t in trades if t["side"] == "sell")
        return float(buy_vol), float(sell_vol)
    except Exception as e:
        logger.error(f"Crypto fetch error {symbol}: {e}")
        return 0, 0

# ================== Stock / Forex / Commodity Volume ==================
def check_stock(symbol="AAPL"):
    """Use yfinance; approximate buy/sell with tick rule"""
    try:
        data = yf.download(symbol, period="3d", interval="1d", progress=False)
        if data.empty or len(data) < 2:
            return 0, 0
        vol = float(data["Volume"].iloc[-1]) if pd.notna(data["Volume"].iloc[-1]) else 0
        change = data["Close"].iloc[-1] - data["Close"].iloc[-2]
        buy = vol if change >= 0 else 0
        sell = vol if change < 0 else 0
        logger.info(f"{symbol}: Buy {buy}, Sell {sell}, Total Volume {vol}")
        return buy, sell
    except Exception as e:
        logger.error(f"Stock fetch error {symbol}: {e}")
        return 0, 0

# ================== Save CSV ==================
def save_csv(rows):
    filename = f"alerts_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp","market","buy_volume","sell_volume","net_flow","message"])
        writer.writerows(rows)
    logger.info(f"CSV saved as {filename}")
    return filename

# ================== Main ==================
def run_once():
    results = []

    crypto_pairs = ["BTC/USDT","ETH/USDT","XRP/USDT","SOL/USDT","ADA/USDT"]
    forex_pairs = ["EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X"]
    commodity_pairs = ["GC=F","SI=F","CL=F"]
    stock_symbols = ["AAPL","MSFT","AMZN"]

    # --- Crypto ---
    for symbol in crypto_pairs:
        buy, sell = check_crypto(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.2f}, Sell {sell:,.2f}, Net {net:,.2f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    # --- Stocks / Forex / Commodities ---
    for symbol in forex_pairs + commodity_pairs + stock_symbols:
        buy, sell = check_stock(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.2f}, Sell {sell:,.2f}, Net {net:,.2f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    save_csv(results)

if __name__ == "__main__":
    logger.info("Starting Volume Bot")
    run_once()
