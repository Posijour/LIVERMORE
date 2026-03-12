MAX_TELEGRAM_TEXT_LEN = 4000

ALERT_COOLDOWN = 30 * 60  # 30 минут
ANOMALY_ALERT_COOLDOWN = 30 * 60  # 30 минут
ALERT_CHAT_ID = -1003829481191

TELEGRAM_TOKEN = "8473159744:AAFokOIhOXg9O9qzPYwtTYkdGcROddbToaQ"

SUPPORTED_TICKERS = {
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "TRXUSDT",
    "DOGEUSDT",
    "BCHUSDT",
    "ADAUSDT",
    "HYPEUSDT",
    "XMRUSDT",
    "LINKUSDT",
    "XLMUSDT",
    "LTCUSDT",
    "HBARUSDT",
    "ZECUSDT",
}


def normalize_ticker(raw: str) -> str:
    ticker = raw.strip().upper()
    if not ticker.endswith("USDT"):
        ticker = f"{ticker}USDT"
    return ticker
