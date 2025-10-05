import os
import time
import csv
import logging
from datetime import datetime
import ccxt
import yfinance as yf
import requests
import pandas as pd

# ================== Settings ==================
REFRESH_SECONDS = int(os.getenv("REFRESH_SECONDS", "3"))
DEBUG = True  # Set True to print debug logs to console without sending alerts

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK")
TWILIO_SID = os.getenv("TWILIO_SID")
TWILIO_TOKEN = os.getenv("TWILIO_TOKEN")
TWILIO_FROM = os.getenv("TWILIO_FROM")
TWILIO_TO = os.getenv("TWILIO_TO")

# ================== Logging ==================
logger = logging.getLogger("VolumeBot")
logger.setLevel(logging.INFO)

# Log to file
file_handler = logging.FileHandler("bot.log")
file_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

# Log to console
console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)

# ================== Alerts ==================
def send_alert(message: str):
    logger.info(message)
    if DEBUG:
        print(message)
        return  # Skip sending alerts in debug mode

    # Telegram
    if TELEGRAM_TOKEN and TELEGRAM_CHAT_ID:
        try:
            url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
            requests.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message}, timeout=5)
        except Exception as e:
            logger.error(f"Telegram error: {e}")

    # Discord
    if DISCORD_WEBHOOK:
        try:
            requests.post(DISCORD_WEBHOOK, json={"content": message}, timeout=5)
        except Exception as e:
            logger.error(f"Discord error: {e}")

    # WhatsApp via Twilio
    if TWILIO_SID and TWILIO_TOKEN and TWILIO_FROM and TWILIO_TO:
        try:
            from twilio.rest import Client
            client = Client(TWILIO_SID, TWILIO_TOKEN)
            client.messages.create(from_=TWILIO_FROM, to=TWILIO_TO, body=message)
        except Exception as e:
            logger.error(f"Twilio error: {e}")

# ================== Crypto Check ==================
def check_crypto(symbol="BTC/USDT", timeframe="1h", limit=12, retries=3, retry_delay=5):
    exchange = ccxt.kucoin()
    for attempt in range(retries):
        try:
            logging.info(f"Fetching crypto data: {symbol}")
            candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
            buy_vol, sell_vol = 0, 0
            for c in candles:
                open_, high, low, close, volume = c[1], c[2], c[3], c[4], c[5]
                if close > open_:
                    buy_vol += volume * 0.6
                    sell_vol += volume * 0.4
                elif close < open_:
                    buy_vol += volume * 0.4
                    sell_vol += volume * 0.6
                else:
                    buy_vol += volume * 0.5
                    sell_vol += volume * 0.5
            return buy_vol, sell_vol
        except ccxt.NetworkError as e:
            logging.warning(f"Network error for {symbol}, attempt {attempt+1}/{retries}: {e}")
            time.sleep(retry_delay)
        except ccxt.ExchangeError as e:
            logging.warning(f"Exchange error for {symbol}, attempt {attempt+1}/{retries}: {e}")
            time.sleep(retry_delay)
        except Exception as e:
            logging.error(f"Unexpected error for {symbol}: {type(e).__name__} - {e}")
            return 0, 0
    logging.warning(f"Failed to fetch crypto data for {symbol} after {retries} attempts")
    return 0, 0

# ================== Stock / Commodities Check ==================
def check_stock(symbol="AAPL"):
    try:
        logging.info(f"Fetching stock data: {symbol}")
        data = yf.download(symbol, period="2d", interval="1d", progress=False, timeout=10)
        if data is None or data.empty or len(data) < 2:
            logging.warning(f"No data returned for {symbol}")
            return 0, 0
        vol = float(data["Volume"].iloc[-1])
        change = float(data["Close"].iloc[-1] - data["Close"].iloc[-2])
        if change >= 0:
            return vol, 0
        else:
            return 0, vol
    except Exception as e:
        logging.error(f"Stock fetch error for {symbol}: {type(e).__name__} - {e}")
        return 0, 0

# ================== Save CSV ==================
def save_csv(rows, filename=None):
    if not filename:
        filename = f"alerts_{datetime.now().date()}.csv"
    file_exists = os.path.isfile(filename)
    with open(filename, "a", newline="") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow([
                "timestamp", "market", "buy_volume", "sell_volume",
                "net_flow", "buy_%", "sell_%", "net_%", "trend", "message"
            ])
        for row in rows:
            writer.writerow(row)

def save_to_csv(data_list, filename="volume_data.csv"):
    """
    Saves fetched data to a CSV file.
    Appends new rows if file exists.
    """
    df = pd.DataFrame(data_list)

    # Add timestamp
    df["Timestamp"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Check if file exists
    if os.path.exists(filename):
        df.to_csv(filename, mode='a', header=False, index=False)
    else:
        df.to_csv(filename, index=False)

# ================== Main Loop ==================
def run_once():
    results = []

    crypto_symbols = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
        "DOGE/USDT", "ADA/USDT", "TRX/USDT", "AVAX/USDT", "DOT/USDT",
        "LINK/USDT", "SUI/USDT", "SHIB/USDT", "LTC/USDT", "ATOM/USDT",
        "XLM/USDT", "UNI/USDT", "ICP/USDT", "APT/USDT", "NEAR/USDT"
    ]
    stock_symbols = ["AAPL", "MSFT", "GOOG", "GC=F", "CL=F", "TSLA", "NVDA"]

    all_symbols = crypto_symbols + stock_symbols

    fetched_data = []

    for symbol in all_symbols:
        try:
            logger.info(f"Fetching data for {symbol}...")
            if "/USDT" in symbol:
                buy, sell = check_crypto(symbol)
            else:
                buy, sell = check_stock(symbol)
            logger.info(f"{symbol} fetched: buy={buy}, sell={sell}")
        except Exception as e:
            logger.error(f"Error fetching {symbol}: {e}")
            buy = sell = 0

        total = buy + sell
        net = buy - sell
        if total > 0:
            buy_pct = (buy / total) * 100
            sell_pct = (sell / total) * 100
            net_pct = (net / total) * 100
        else:
            buy_pct = sell_pct = net_pct = 0

        if buy_pct >= 60:
            trend_emoji = "🟢"
        elif sell_pct >= 60:
            trend_emoji = "🔴"
        else:
            trend_emoji = "⚪"

        results.append({
            "timestamp": datetime.utcnow().isoformat(),
            "market": symbol,
            "buy": buy,
            "sell": sell,
            "net": net,
            "buy_pct": buy_pct,
            "sell_pct": sell_pct,
            "net_pct": net_pct,
            "trend": trend_emoji
        })

        # Prepare data for CSV
        fetched_data.append({
            "Symbol": symbol,
            "Buy": buy,
            "Sell": sell,
            "Net": net
        })

    # Top 5 bullish/bearish
    strong_bullish = sorted([r for r in results if r["trend"] == "🟢"], key=lambda x: x["buy_pct"], reverse=True)[:5]
    strong_bearish = sorted([r for r in results if r["trend"] == "🔴"], key=lambda x: x["sell_pct"], reverse=True)[:5]

    # Send alerts
    for group, title in [(strong_bullish, "🚀 Top Strong Bullish Markets:"), (strong_bearish, "🔻 Top Strong Bearish Markets:")]:
        if group:
            send_alert(title)
            for r in group:
                msg = f"{r['trend']} {r['market']} — Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}%\nNet Flow: {r['net']:,.0f}"
                send_alert(msg)

    # Save CSVs
    save_csv([
        [r["timestamp"], r["market"], r["buy"], r["sell"], r["net"],
         r["buy_pct"], r["sell_pct"], r["net_pct"], r["trend"],
         f"{r['trend']} Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}% | Value: {r['net']:,.0f}"]
        for r in results
    ])
    save_to_csv(fetched_data)

# ================== Entry Point ==================
if __name__ == "__main__":
    logger.info("Starting High Volume Bot")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Run failed: {e}")

        # Continuous loop — no long sleep
        logger.info("Restarting immediately for next scan...")
        time.sleep(3)  # adjust this delay (in seconds) as you like
