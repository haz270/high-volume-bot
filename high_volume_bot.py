import os
import time
import csv
import logging
from datetime import datetime
import ccxt
import yfinance as yf
import requests

# ================== Logging ==================
LOG_FILE = "bot.log"
logger = logging.getLogger("VolumeBot")
logger.setLevel(logging.INFO)
handler = logging.FileHandler(LOG_FILE)
formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

# ================== Settings ==================
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

# ================== Crypto Check ==================
def check_crypto(symbol="BTC/USDT", timeframe="1h", limit=24):
    exchange = ccxt.binance()  # Public endpoint
    try:
        logger.info(f"Fetching crypto data: {symbol}")
        candles = exchange.fetch_ohlcv(symbol, timeframe=timeframe, limit=limit)
        if not candles:
            return 0, 0
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
    except Exception as e:
        logger.error(f"Crypto fetch error {symbol}: {e}")
        return 0, 0

# ================== Stock / Commodities Check ==================
def check_stock(symbol="AAPL"):
    try:
        logger.info(f"Fetching stock data: {symbol}")
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
            writer.writerow([
                "timestamp", "market", "buy_volume", "sell_volume",
                "net_flow", "buy_%", "sell_%", "net_%", "trend", "message"
            ])
        for row in rows:
            writer.writerow(row)

# ================== Main Run ==================
def run_once():
    results = []

    crypto_symbols = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
        "DOGE/USDT", "ADA/USDT", "TRX/USDT", "AVAX/USDT", "DOT/USDT",
        "LINK/USDT", "MATIC/USDT", "SHIB/USDT", "LTC/USDT", "ATOM/USDT",
        "XLM/USDT", "UNI/USDT", "ICP/USDT", "APT/USDT", "NEAR/USDT"
    ]
    stock_symbols = ["AAPL", "MSFT", "GOOG", "GC=F", "CL=F", "TSLA", "NVDA"]

    all_symbols = crypto_symbols + stock_symbols

    for symbol in all_symbols:
        try:
            if "/USDT" in symbol:
                buy, sell = check_crypto(symbol)
            else:
                buy, sell = check_stock(symbol)
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

    # Filter strong signals
    strong_bullish = [r for r in results if r["trend"] == "🟢"]
    strong_bearish = [r for r in results if r["trend"] == "🔴"]

    top_bullish = sorted(strong_bullish, key=lambda x: x["buy_pct"], reverse=True)[:5]
    top_bearish = sorted(strong_bearish, key=lambda x: x["sell_pct"], reverse=True)[:5]

    if top_bullish:
        send_alert("🚀 Top Strong Bullish Markets:")
        for r in top_bullish:
            msg = f"{r['trend']} {r['market']} — Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}%\nNet Flow: {r['net']:,.0f}"
            send_alert(msg)

    if top_bearish:
        send_alert("🔻 Top Strong Bearish Markets:")
        for r in top_bearish:
            msg = f"{r['trend']} {r['market']} — Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}%\nNet Flow: {r['net']:,.0f}"
            send_alert(msg)

    # Save all markets to CSV
    save_csv([
        [r["timestamp"], r["market"], r["buy"], r["sell"], r["net"],
         r["buy_pct"], r["sell_pct"], r["net_pct"], r["trend"],
         f"{r['trend']} Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}% | Value: {r['net']:,.0f}"]
        for r in results
    ])

# ================== Main Loop ==================
if __name__ == "__main__":
    logger.info("Starting High Volume Bot")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Run failed: {e}")
        if REFRESH_MINUTES <= 0:
            break
        logger.info(f"Sleeping {REFRESH_MINUTES} minutes...")
        time.sleep(REFRESH_MINUTES * 60)
