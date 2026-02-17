import requests
from config import SUPABASE_URL, SUPABASE_KEY


class SupabaseClient:
    PAGE_SIZE = 1000
    
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
                    ("ts", f"gte.{ts_from}"),
                    ("ts", f"lte.{ts_to}"),
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

        return rows
