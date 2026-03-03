from datetime import datetime, timezone
from json import dumps, loads
import socket
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import SUPABASE_KEY, SUPABASE_URL


PAGE_SIZE = 200
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5


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


def _build_state_query(
    layer: str,
    state_key: str,
    symbol: str | None,
    select: str,
    limit: int,
    offset: int = 0,
) -> str:
    query_params: list[tuple[str, str]] = [
        ("select", select),
        ("layer", f"eq.{layer}"),
        ("state_key", f"eq.{state_key}"),
        ("order", "ts.desc"),
        ("limit", str(limit)),
        ("offset", str(offset)),
    ]

    if symbol is None:
        query_params.append(("symbol", "is.null"))
    else:
        query_params.append(("symbol", f"eq.{symbol}"))

    return urlencode(query_params)


def _fetch_state_rows(
    layer: str,
    state_key: str,
    symbol: str | None,
    limit: int,
    offset: int = 0,
) -> list[dict]:
    query = _build_state_query(
        layer=layer,
        state_key=state_key,
        symbol=symbol,
        select="ts,state_value",
        limit=limit,
        offset=offset,
    )
    url = f"{SUPABASE_URL}/rest/v1/state_history?{query}"

    req = _build_request(url)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"state_history HTTP error {exc.code}: {detail}") from exc
        except (TimeoutError, socket.timeout, URLError) as exc:
            if attempt == MAX_RETRIES:
                reason = getattr(exc, "reason", exc)
                raise RuntimeError(f"state_history connection error: {reason}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("state_history connection error: exhausted retries")


def _fetch_last_state(layer: str, state_key: str, symbol: str | None) -> str | None:
    rows = _fetch_state_rows(layer=layer, state_key=state_key, symbol=symbol, limit=1)

    if not rows:
        return None

    return rows[0].get("state_value")


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def get_state_persistence_hours(
    layer: str,
    state_key: str,
    symbol: str | None = None,
) -> tuple[str, int] | None:
    """
    Returns current state value and how many full hours it has persisted.
    """
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    rows = _fetch_state_rows(layer=layer, state_key=state_key, symbol=symbol, limit=PAGE_SIZE)
    if not rows:
        return None

    current_value = str(rows[0].get("state_value"))
    state_start_ts = rows[0].get("ts")

    offset = 0
    while True:
        for row in rows:
            row_value = str(row.get("state_value"))
            if row_value != current_value:
                break
            state_start_ts = row.get("ts")
        else:
            if len(rows) < PAGE_SIZE:
                break
            offset += PAGE_SIZE
            rows = _fetch_state_rows(
                layer=layer,
                state_key=state_key,
                symbol=symbol,
                limit=PAGE_SIZE,
                offset=offset,
            )
            if not rows:
                break
            continue
        break

    if not state_start_ts:
        return current_value, 0

    started_at = _parse_ts(state_start_ts)
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=timezone.utc)

    hours = int((datetime.now(timezone.utc) - started_at).total_seconds() // 3600)
    return current_value, max(0, hours)


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

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS):
                return
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"state_history HTTP error {exc.code}: {detail}") from exc
        except (TimeoutError, socket.timeout, URLError) as exc:
            if attempt == MAX_RETRIES:
                reason = getattr(exc, "reason", exc)
                raise RuntimeError(f"state_history connection error: {reason}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("state_history connection error: exhausted retries")
