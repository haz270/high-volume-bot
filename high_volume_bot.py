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
import ccxt

def check_crypto(symbol="BTC/USDT", timeframe="1h", limit=24):
    """Estimate buy/sell volume using OHLCV candles without API keys"""
    exchange = ccxt.binance()  # Public endpoints only
    try:
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

# ================== Main ==================
def run_once():
    results = []

    # --- Top 20 Crypto Pairs
    crypto_symbols = [
        "BTC/USDT", "ETH/USDT", "BNB/USDT", "XRP/USDT", "SOL/USDT",
        "DOGE/USDT", "ADA/USDT", "TRX/USDT", "AVAX/USDT", "DOT/USDT",
        "LINK/USDT", "MATIC/USDT", "SHIB/USDT", "LTC/USDT", "ATOM/USDT",
        "XLM/USDT", "UNI/USDT", "ICP/USDT", "APT/USDT", "NEAR/USDT"
    ]

    # --- Stocks & Commodities
    stock_symbols = ["AAPL", "MSFT", "GOOG", "GC=F", "CL=F", "TSLA", "NVDA"]

    all_symbols = crypto_symbols + stock_symbols

    for symbol in all_symbols:
        if "/USDT" in symbol:
            buy, sell = check_crypto(symbol)
        else:
            buy, sell = check_stock(symbol)

        total = buy + sell
        net = buy - sell
        if total > 0:
            buy_pct = (buy / total) * 100
            sell_pct = (sell / total) * 100
            net_pct = (net / total) * 100
        else:
            buy_pct = sell_pct = net_pct = 0

        # Determine trend emoji
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

    # Top 5 only
    top_bullish = sorted(strong_bullish, key=lambda x: x["buy_pct"], reverse=True)[:5]
    top_bearish = sorted(strong_bearish, key=lambda x: x["sell_pct"], reverse=True)[:5]

    # Send alerts
    if top_bullish:
        send_alert("🚀 Top Strong Bullish Markets:")
        for r in top_bullish:
            msg = (
                f"{r['trend']} {r['market']} — Buy: {r['buy_pct']:.1f}% | "
                f"Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}%\n"
                f"Net Flow: {r['net']:,.0f}"
            )
            send_alert(msg)

    if top_bearish:
        send_alert("🔻 Top Strong Bearish Markets:")
        for r in top_bearish:
            msg = (
                f"{r['trend']} {r['market']} — Buy: {r['buy_pct']:.1f}% | "
                f"Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}%\n"
                f"Net Flow: {r['net']:,.0f}"
            )
            send_alert(msg)

    # Save all markets to CSV
    save_csv([
        [r["timestamp"], r["market"], r["buy"], r["sell"], r["net"],
         r["buy_pct"], r["sell_pct"], r["net_pct"], r["trend"],
         f"{r['trend']} Buy: {r['buy_pct']:.1f}% | Sell: {r['sell_pct']:.1f}% | Net: {r['net_pct']:.1f}% | Value: {r['net']:,.0f}"]
        for r in results
    ])
    save_csv(results)

if __name__ == "__main__":
    logger.info("Starting High Volume Bot")
    while True:
        run_once()
        if REFRESH_MINUTES <= 0:
            break
        logger.info(f"Sleeping {REFRESH_MINUTES} minutes...")
        time.sleep(REFRESH_MINUTES * 60)
