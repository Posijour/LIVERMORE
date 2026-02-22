from datetime import datetime, timezone
from json import loads
import socket
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import SUPABASE_URL, SUPABASE_KEY


class SupabaseClient:
    PAGE_SIZE = 1000
    REQUEST_TIMEOUT_SECONDS = 10
    MAX_RETRIES = 3
    RETRY_BACKOFF_SECONDS = 1.5

    def _row_ts_ms(self, row: dict) -> int | None:
        ts = row.get("ts")

        if ts is None and isinstance(row.get("data"), dict):
            data = row["data"]
            ts = (
                data.get("ts")
                or data.get("timestamp")
                or data.get("time")
                or data.get("created_at")
            )

        if ts is None:
            ts = row.get("created_at") or row.get("timestamp") or row.get("time")

        if isinstance(ts, (int, float)):
            value = int(ts)
            # Heuristic: convert seconds to milliseconds.
            return value * 1000 if value < 10_000_000_000 else value

        if isinstance(ts, str):
            try:
                return int(ts)
            except ValueError:
                try:
                    dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
                    if dt.tzinfo is None:
                        dt = dt.replace(tzinfo=timezone.utc)
                    return int(dt.timestamp() * 1000)
                except ValueError:
                    return None

        return None

    def _filter_by_window(self, rows: list[dict], ts_from: int, ts_to: int) -> list[dict]:
        filtered: list[dict] = []

        for row in rows:
            row_ts = self._row_ts_ms(row)
            if row_ts is None:
                # Strict mode: rows without a parsable timestamp are excluded,
                # otherwise different windows can collapse to the same dataset.
                continue
            if ts_from <= row_ts <= ts_to:
                filtered.append(row)

        return filtered

    def _normalize_symbol(self, symbol: str) -> str:
        separators = ("-", "_", "/", ":")
        value = symbol.upper()

        for sep in separators:
            value = value.split(sep, 1)[0]

        for quote in ("USDT", "USD", "PERP"):
            if value.endswith(quote):
                return value[: -len(quote)]

        return value

    def _row_symbol(self, row: dict) -> str | None:
        if isinstance(row.get("symbol"), str):
            return row["symbol"]

        data = row.get("data")
        if isinstance(data, dict) and isinstance(data.get("symbol"), str):
            return data["symbol"]

        return None

    def _filter_by_symbol(self, rows: list[dict], symbol: str | None) -> list[dict]:
        if not symbol:
            return rows

        normalized_symbol = self._normalize_symbol(symbol)
        filtered: list[dict] = []

        for row in rows:
            row_symbol = self._row_symbol(row)
            if row_symbol is None:
                # Keep rows without symbol (global market metrics) to avoid
                # empty snapshots when only part of the pipeline is symbolized.
                filtered.append(row)
                continue

            if self._normalize_symbol(row_symbol) == normalized_symbol:
                filtered.append(row)

        return filtered

    def _request_page(self, event: str, ts_from: int, ts_to: int, offset: int) -> tuple[list[dict], str]:
        query = urlencode(
            [
                ("event", f"eq.{event}"),
                ("and", f"(ts.gte.{ts_from},ts.lte.{ts_to})"),
                ("order", "ts.asc"),
                ("limit", str(self.PAGE_SIZE)),
                ("offset", str(offset)),
            ]
        )
        url = f"{SUPABASE_URL}/rest/v1/logs?{query}"

        req = Request(
            url,
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
                "Prefer": "count=exact",
            },
            method="GET",
        )

        for attempt in range(1, self.MAX_RETRIES + 1):
            try:
                with urlopen(req, timeout=self.REQUEST_TIMEOUT_SECONDS) as response:
                    raw = response.read().decode("utf-8")
                    page = loads(raw)
                    content_range = response.headers.get("Content-Range", "")
                    return page, content_range
            except HTTPError as exc:
                detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
                raise RuntimeError(f"Supabase HTTP error {exc.code}: {detail}") from exc
            except (TimeoutError, socket.timeout, URLError) as exc:
                if attempt == self.MAX_RETRIES:
                    reason = getattr(exc, "reason", exc)
                    raise RuntimeError(f"Supabase connection error: {reason}") from exc
                time.sleep(self.RETRY_BACKOFF_SECONDS * attempt)

        raise RuntimeError("Supabase connection error: exhausted retries")

    def fetch(self, event: str, ts_from: int, ts_to: int, symbol: str | None = None) -> list[dict]:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("Supabase credentials not set")
        rows: list[dict] = []
        offset = 0

        while True:
            page, content_range = self._request_page(event, ts_from, ts_to, offset)
            rows.extend(page)

            total = None
            if "/" in content_range:
                _, total_part = content_range.split("/", 1)
                if total_part.isdigit():
                    total = int(total_part)

            if total is not None:
                if offset + len(page) >= total:
                    break
            elif len(page) < self.PAGE_SIZE:
                break

            offset += self.PAGE_SIZE

        rows = self._filter_by_window(rows, ts_from, ts_to)
        return self._filter_by_symbol(rows, symbol)


from datetime import datetime, timezone


def record_state(
    client: SupabaseClient,
    layer: str,
    state_key: str,
    state_value: str,
    symbol: str | None = None,
):
    """
    Записывает состояние в state_history,
    ТОЛЬКО если оно изменилось.
    """

    # 1. Берём последнюю запись
    query = (
        client
        .table("state_history")
        .select("state_value")
        .eq("layer", layer)
        .eq("state_key", state_key)
        .order("ts", desc=True)
        .limit(1)
    )

    if symbol is None:
        query = query.is_("symbol", None)
    else:
        query = query.eq("symbol", symbol)

    res = query.execute()
    rows = res.data or []

    # 2. Если значение не изменилось — ничего не делаем
    if rows and rows[0]["state_value"] == str(state_value):
        return

    # 3. Пишем новую запись
    client.table("state_history").insert({
        "ts": datetime.now(timezone.utc).isoformat(),
        "layer": layer,
        "state_key": state_key,
        "state_value": str(state_value),
        "symbol": symbol,
    }).execute()