import requests
from config import SUPABASE_URL, SUPABASE_KEY

class SupabaseClient:
    def fetch(self, event: str, ts_from: int, ts_to: int) -> list[dict]:
        if not SUPABASE_URL or not SUPABASE_KEY:
            raise RuntimeError("Supabase credentials not set")

        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/logs",
            headers={
                "apikey": SUPABASE_KEY,
                "Authorization": f"Bearer {SUPABASE_KEY}",
            },
            params={
                "event": f"eq.{event}",
                "ts": f"gte.{ts_from}",
                "ts": f"lte.{ts_to}",
            },
            timeout=10,
        )
        r.raise_for_status()
        return r.json()
