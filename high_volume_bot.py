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
handler = RotatingFileHandler(LOG_FILE, maxBytes=5 * 1024 * 1024, backupCount=7)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# ================== Settings ==================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ================== Alerts ==================
def send_alert(message: str):
    logger.info(message)
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message})
        except Exception as e:
            logger.error(f"Telegram error: {e}")

# ================== Volume checks ==================
def check_crypto(symbol="BTC/USDT", timeframe="1h", limit=24):
    """Approximate buy/sell volume using OHLCV candles"""
    exchange = ccxt.binance()
    try:
        ohlcv = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        df = pd.DataFrame(ohlcv, columns=["timestamp","open","high","low","close","volume"])
        buy_vol = df[df["close"] >= df["open"]]["volume"].sum()
        sell_vol = df[df["close"] < df["open"]]["volume"].sum()
        return float(buy_vol), float(sell_vol)
    except Exception as e:
        logger.error(f"Crypto fetch error {symbol}: {e}")
        return 0, 0

def check_stock(symbol="AAPL"):
    """Approximate buy/sell using tick rule on daily data"""
    try:
        data = yf.download(symbol, period="3d", interval="1d", progress=False)
        if data.empty or len(data) < 2:
            return 0, 0
        vol = float(data["Volume"].iloc[-1])
        change = data["Close"].iloc[-1] - data["Close"].iloc[-2]
        vol = vol if pd.notna(vol) else 0
        buy = vol if change >= 0 else 0
        sell = vol if change < 0 else 0
        logger.info(f"{symbol}: Volume fetched {vol}, Buy {buy}, Sell {sell}")
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

    crypto_pairs = ["BTC/USDT","ETH/USDT","XRP/USDT","SOL/USDT","ADA/USDT","BNB/USDT","DOGE/USDT","LTC/USDT","DOT/USDT"]
    forex_pairs = ["EURUSD=X","GBPUSD=X","USDJPY=X","AUDUSD=X","USDCAD=X","USDCHF=X","NZDUSD=X"]
    commodity_pairs = ["GC=F","SI=F","CL=F","NG=F"]
    stock_symbols = ["AAPL","MSFT","AMZN","TSLA","GOOG","NVDA"]

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
