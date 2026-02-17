from datetime import datetime, timezone

import requests
from config import SUPABASE_URL, SUPABASE_KEY


class SupabaseClient:
    PAGE_SIZE = 1000

    def _row_ts_ms(self, row: dict) -> int | None:
        ts = row.get("ts")

        if ts is None and isinstance(row.get("data"), dict):
            ts = row["data"].get("ts")

        if ts is None:
            ts = row.get("created_at")

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
                filtered.append(row)
                continue
            if ts_from <= row_ts <= ts_to:
                filtered.append(row)

        return filtered

    def fetch(self, event: str, ts_from: int, ts_to: int) -> list[dict]:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("Supabase credentials not set")

        rows: list[dict] = []
        offset = 0

        while True:
            r = requests.get(
                f"{SUPABASE_URL}/rest/v1/logs",
                headers={
                    "apikey": SUPABASE_KEY,
                    "Authorization": f"Bearer {SUPABASE_KEY}",
                    "Prefer": "count=exact",
                },
                params=[
                    ("event", f"eq.{event}"),
                    ("and", f"(ts.gte.{ts_from},ts.lte.{ts_to})"),
                    ("order", "ts.asc"),
                    ("limit", str(self.PAGE_SIZE)),
                    ("offset", str(offset)),
                ],
                timeout=10,
            )
            r.raise_for_status()

            page = r.json()
            rows.extend(page)

            content_range = r.headers.get("Content-Range", "")
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

        return self._filter_by_window(rows, ts_from, ts_to)
