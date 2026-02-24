from datetime import datetime, timezone
from json import dumps, loads
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import SUPABASE_KEY, SUPABASE_URL


def _build_request(url: str, method: str = "GET", payload: dict | None = None) -> Request:
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
    }

    data = None
    if payload is not None:
        headers["Content-Type"] = "application/json"
        headers["Prefer"] = "return=minimal"
        data = dumps(payload).encode("utf-8")

    return Request(url, headers=headers, method=method, data=data)


def _fetch_last_state(layer: str, state_key: str, symbol: str | None) -> str | None:
    query_params: list[tuple[str, str]] = [
        ("select", "state_value"),
        ("layer", f"eq.{layer}"),
        ("state_key", f"eq.{state_key}"),
        ("order", "ts.desc"),
        ("limit", "1"),
    ]

    if symbol is None:
        query_params.append(("symbol", "is.null"))
    else:
        query_params.append(("symbol", f"eq.{symbol}"))

    query = urlencode(query_params)
    url = f"{SUPABASE_URL}/rest/v1/state_history?{query}"

    req = _build_request(url)
    with urlopen(req, timeout=10) as response:
        rows = loads(response.read().decode("utf-8"))

    if not rows:
        return None

    return rows[0].get("state_value")


def record_state(
    layer: str,
    state_key: str,
    state_value: str,
    symbol: str | None = None,
) -> None:
    """
    Writes a new row into state_history only when state_value changed.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    state_value_str = str(state_value)
    last_value = _fetch_last_state(layer, state_key, symbol)

    if last_value == state_value_str:
        return

    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "layer": layer,
        "state_key": state_key,
        "state_value": state_value_str,
        "symbol": symbol,
    }

    url = f"{SUPABASE_URL}/rest/v1/state_history"
    req = _build_request(url, method="POST", payload=payload)

    with urlopen(req, timeout=10):
        return
