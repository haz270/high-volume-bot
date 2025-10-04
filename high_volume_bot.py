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

# ================== Settings ==================
REFRESH_MINUTES = int(os.getenv("REFRESH_MINUTES", "0"))  # one-time for GitHub
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ================== Alerts ==================
def send_alert(message: str):
    """Send alerts to Telegram"""
    logger.info(message)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# ================== Volume checks ==================
def check_crypto(symbol="BTC/USDT", limit=200):
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
    try:
        data = yf.download(symbol, period="2d", interval="1d", progress=False)
        if len(data) < 2:
            return 0, 0
        vol = float(data["Volume"].iloc[-1])
        change = data["Close"].iloc[-1] - data["Close"].iloc[-2]
        return (vol, 0) if change >= 0 else (0, vol)
    except Exception as e:
        logger.error(f"Stock fetch error {symbol}: {e}")
        return 0, 0

# ================== Save CSV ==================
def save_csv(rows):
    filename = f"alerts_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    with open(filename, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp", "market", "buy_volume", "sell_volume", "net_flow", "message"])
        writer.writerows(rows)
    return filename

# ================== Main ==================
def run_once():
    results = []

    crypto_pairs = ["BTC/USDT", "ETH/USDT", "XRP/USDT", "SOL/USDT", "ADA/USDT", "BNB/USDT", "DOGE/USDT", "LTC/USDT", "DOT/USDT"]
    forex_pairs = ["EURUSD=X", "GBPUSD=X", "USDJPY=X", "AUDUSD=X", "USDCAD=X", "USDCHF=X", "NZDUSD=X"]
    commodity_pairs = ["GC=F", "SI=F", "CL=F", "NG=F"]
    stock_symbols = ["AAPL", "MSFT", "AMZN", "TSLA", "GOOG", "NVDA"]

    for symbol in crypto_pairs:
        buy, sell = check_crypto(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.0f}, Sell {sell:,.0f}, Net {net:,.0f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    for symbol in forex_pairs + commodity_pairs + stock_symbols:
        buy, sell = check_stock(symbol)
        net = buy - sell
        msg = f"{symbol}: Buy {buy:,.0f}, Sell {sell:,.0f}, Net {net:,.0f}"
        send_alert(msg)
        results.append([datetime.utcnow().isoformat(), symbol, buy, sell, net, msg])

    filename = save_csv(results)
    logger.info(f"CSV saved as {filename}")

if __name__ == "__main__":
    logger.info("Starting Volume Bot")
    run_once()
