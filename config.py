import os

SUPABASE_URL = os.getenv("SUPABASE_URL", "").strip()
SUPABASE_KEY = os.getenv("SUPABASE_KEY", "").strip()

DEFAULT_WINDOW_HOURS = int(os.getenv("DEFAULT_WINDOW_HOURS", "12"))

DATA_SCOPE = {
    "futures": "multi-symbol",
    "options": "BTC / ETH",
    "vol": "BTC / ETH",
}
