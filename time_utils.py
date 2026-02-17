import time
from datetime import datetime, timezone

def now_ms() -> int:
    return int(time.time() * 1000)

def parse_window(arg: str) -> tuple[int, int]:
    """
    '6h', '90m', '1d'
    """
    n = int(arg[:-1])
    unit = arg[-1]

    mult = {
        "m": 60 * 1000,
        "h": 60 * 60 * 1000,
        "d": 24 * 60 * 60 * 1000,
    }

    if unit not in mult:
        raise ValueError("Invalid window format")

    ts_to = now_ms()
    ts_from = ts_to - n * mult[unit]

    return ts_from, ts_to


def parse_datetime(dt: str) -> int:
    """
    '2026-02-15 12:00'
    """
    d = datetime.strptime(dt, "%Y-%m-%d %H:%M")
    return int(d.replace(tzinfo=timezone.utc).timestamp() * 1000)
