import os

MAX_TELEGRAM_TEXT_LEN = 4000

ALERT_COOLDOWN = int(os.getenv("ALERT_COOLDOWN_SECONDS", "1800"))
ANOMALY_ALERT_COOLDOWN = int(os.getenv("ANOMALY_ALERT_COOLDOWN_SECONDS", "1800"))

TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "").strip()
MAIN_CHAT_ID = int(os.getenv("TELEGRAM_MAIN_CHAT_ID", "0"))
ALERT_CHAT_ID = int(os.getenv("TELEGRAM_ALERT_CHAT_ID", "0"))

_DEFAULT_TICKERS = [
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
]


_tickers_raw = os.getenv("SUPPORTED_TICKERS")
if _tickers_raw:
    SUPPORTED_TICKERS = {
        t.strip().upper() for t in _tickers_raw.split(",") if t.strip()
    }
else:
    SUPPORTED_TICKERS = set(_DEFAULT_TICKERS)


def normalize_ticker(raw: str) -> str:
    ticker = raw.strip().upper()
    if not ticker.endswith("USDT"):
        ticker = f"{ticker}USDT"
    return ticker
