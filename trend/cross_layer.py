from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from json import dumps, loads
import logging
import socket
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from config import SUPABASE_KEY, SUPABASE_URL
from data.queries import load_bybit_market_state, load_deribit, load_risk

logger = logging.getLogger(__name__)

OPTIONS_FRESHNESS_MS = 45 * 60 * 1000
DERIBIT_FRESHNESS_MS = 15 * 60 * 1000
CLASSIFIER_VERSION = "cross_v1"
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5


@dataclass
class CrossContext:
    bybit: dict | None
    deribit_btc: dict | None
    deribit_eth: dict | None
    missing_parts: list[str]

    @property
    def is_complete(self) -> bool:
        return not self.missing_parts


def _to_int_ms(value) -> int | None:
    if value is None:
        return None

    if isinstance(value, (int, float)):
        numeric = int(value)
        return numeric * 1000 if numeric < 10_000_000_000 else numeric

    if isinstance(value, str):
        try:
            return _to_int_ms(int(float(value)))
        except ValueError:
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)

    return None


def get_event_ts_ms(row: dict) -> int | None:
    data = row.get("data", {})
    return _to_int_ms(data.get("ts_unix_ms")) or _to_int_ms(row.get("ts"))


def _risk_value(row: dict) -> float | None:
    risk = row.get("data", {}).get("risk")
    if isinstance(risk, (int, float)):
        return float(risk)

    if isinstance(risk, str):
        try:
            return float(risk)
        except ValueError:
            return None

    return None


def _row_ts_ms(row: dict) -> int | None:
    data = row.get("data", {})
    return _to_int_ms(data.get("ts_unix_ms")) or _to_int_ms(row.get("ts"))


def _latest_fresh_row(rows: list[dict], event_ts_ms: int, freshness_ms: int) -> dict | None:
    latest: dict | None = None
    latest_ts = -1

    for row in rows:
        row_ts = _row_ts_ms(row)
        if row_ts is None or row_ts > event_ts_ms:
            continue

        if event_ts_ms - row_ts > freshness_ms:
            continue

        if row_ts > latest_ts:
            latest = row
            latest_ts = row_ts

    return latest


def get_latest_bybit_context(event_ts_ms: int) -> dict | None:
    ts_from = max(0, event_ts_ms - OPTIONS_FRESHNESS_MS)
    rows = load_bybit_market_state(ts_from, event_ts_ms)
    return _latest_fresh_row(rows, event_ts_ms, OPTIONS_FRESHNESS_MS)


def get_latest_deribit_context(event_ts_ms: int) -> dict[str, dict | None]:
    ts_from = max(0, event_ts_ms - DERIBIT_FRESHNESS_MS)

    btc_rows = load_deribit(ts_from, event_ts_ms, symbol="BTC")
    eth_rows = load_deribit(ts_from, event_ts_ms, symbol="ETH")

    return {
        "BTC": _latest_fresh_row(btc_rows, event_ts_ms, DERIBIT_FRESHNESS_MS),
        "ETH": _latest_fresh_row(eth_rows, event_ts_ms, DERIBIT_FRESHNESS_MS),
    }


def get_latest_cross_context_for_event(event_ts_ms: int) -> CrossContext:
    bybit_row = get_latest_bybit_context(event_ts_ms)
    deribit_rows = get_latest_deribit_context(event_ts_ms)
    missing_parts: list[str] = []

    if bybit_row is None:
        missing_parts.append("bybit")
    if deribit_rows.get("BTC") is None:
        missing_parts.append("deribit_btc")
    if deribit_rows.get("ETH") is None:
        missing_parts.append("deribit_eth")

    return CrossContext(
        bybit=bybit_row,
        deribit_btc=deribit_rows.get("BTC"),
        deribit_eth=deribit_rows.get("ETH"),
        missing_parts=missing_parts,
    )


def compute_global_deribit_state(btc_state: str | None, eth_state: str | None) -> str:
    states = {str(btc_state or "").upper(), str(eth_state or "").upper()}

    if "HOT" in states:
        return "HOT"
    if "WARM" in states:
        return "WARM"
    if states == {"COLD"}:
        return "COLD"
    return "WARM"


def classify_market_mode(bybit_row: dict, deribit_btc_row: dict, deribit_eth_row: dict) -> tuple[str, str]:
    bybit_data = bybit_row.get("data", {})
    btc_data = deribit_btc_row.get("data", {})
    eth_data = deribit_eth_row.get("data", {})

    regime = str(bybit_data.get("regime", "")).upper()
    mci = float(bybit_data.get("mci", 0.0))

    global_deribit_state = compute_global_deribit_state(
        str(btc_data.get("vbi_state", "")).upper(),
        str(eth_data.get("vbi_state", "")).upper(),
    )

    if regime == "CALM" and mci < 0.35 and global_deribit_state == "COLD":
        return "CALM", global_deribit_state

    if mci >= 0.60 or global_deribit_state == "HOT":
        return "HOT", global_deribit_state

    if regime == "UNCERTAIN" or (0.35 <= mci < 0.60) or global_deribit_state == "WARM":
        return "TENSE", global_deribit_state

    return "TRANSITION", global_deribit_state


def _build_notes(context: CrossContext) -> str:
    if context.is_complete:
        return "complete context"

    missing_to_text = {
        "bybit": "missing fresh bybit context",
        "deribit_btc": "missing fresh deribit BTC snapshot",
        "deribit_eth": "missing fresh deribit ETH snapshot",
    }
    return "; ".join(missing_to_text[item] for item in context.missing_parts)


def classify_cross_event(risk_row: dict, context: CrossContext) -> dict:
    risk_data = risk_row.get("data", {})
    event_ts_ms = get_event_ts_ms(risk_row)
    if event_ts_ms is None:
        raise ValueError("risk event has no timestamp")

    symbol = risk_data.get("symbol")
    risk_value = _risk_value(risk_row)

    result = {
        "ts_unix_ms": event_ts_ms,
        "event_key": f"{symbol}:{event_ts_ms}:{risk_value}:cross_v1",
        "symbol": symbol,
        "source_event_ts_ms": event_ts_ms,
        "risk": risk_value,
        "risk_bucket": compute_risk_bucket(risk_value),
        "price": risk_data.get("price"),
        "direction": risk_data.get("direction"),
        "risk_driver": risk_data.get("risk_driver"),
        "classifier_version": CLASSIFIER_VERSION,
        "context_status": "INCOMPLETE",
        "cross_type": None,
        "market_mode": None,
        "notes": _build_notes(context),
        "bybit_regime": None,
        "bybit_mci": None,
        "bybit_confidence": None,
        "deribit_btc_state": None,
        "deribit_btc_score": None,
        "deribit_eth_state": None,
        "deribit_eth_score": None,
        "global_deribit_state": None,
    }

    if not context.is_complete:
        return result

    market_mode, global_deribit_state = classify_market_mode(
        context.bybit,
        context.deribit_btc,
        context.deribit_eth,
    )

    if market_mode == "CALM":
        cross_type = "crowd_no_confirm"
    elif market_mode == "TRANSITION":
        cross_type = "stress_inside_transition"
    else:
        cross_type = "stress_aligned_with_regime"

    bybit_data = context.bybit.get("data", {})
    btc_data = context.deribit_btc.get("data", {})
    eth_data = context.deribit_eth.get("data", {})

    result.update(
        {
            "context_status": "COMPLETE",
            "cross_type": cross_type,
            "market_mode": market_mode,
            "notes": "complete context",
            "bybit_regime": bybit_data.get("regime"),
            "bybit_mci": bybit_data.get("mci"),
            "bybit_confidence": bybit_data.get("confidence"),
            "deribit_btc_state": btc_data.get("vbi_state"),
            "deribit_btc_score": btc_data.get("vbi_score"),
            "deribit_eth_state": eth_data.get("vbi_state"),
            "deribit_eth_score": eth_data.get("vbi_score"),
            "global_deribit_state": global_deribit_state,
        }
    )

    return result


def _build_cross_layer_request(payload: dict) -> Request:
    query = urlencode([("on_conflict", "event_key")])
    url = f"{SUPABASE_URL}/rest/v1/cross_layer_events?{query}"

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=ignore-duplicates,return=minimal",
    }

    body = dumps(payload).encode("utf-8")
    return Request(url, headers=headers, method="POST", data=body)


def compute_risk_bucket(risk: float | int | None) -> str | None:
    if risk is None:
        return None

    value = float(risk)
    if value >= 5:
        return "5+"
    if value >= 4:
        return "4"
    return "3"


def _field_value(row: dict, key: str):
    if key in row:
        return row.get(key)

    data = row.get("data")
    if isinstance(data, dict):
        return data.get(key)

    return None


def build_state_signature(row: dict) -> tuple:
    risk_bucket = _field_value(row, "risk_bucket")
    if risk_bucket is None:
        risk_bucket = compute_risk_bucket(_field_value(row, "risk"))

    return (
        _field_value(row, "context_status"),
        _field_value(row, "cross_type"),
        _field_value(row, "market_mode"),
        _field_value(row, "bybit_regime"),
        risk_bucket,
    )


def load_last_cross_layer_event(symbol: str) -> dict | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    query = urlencode(
        [
            ("symbol", f"eq.{symbol}"),
            ("order", "ts_unix_ms.desc"),
            ("limit", "1"),
        ]
    )
    url = f"{SUPABASE_URL}/rest/v1/cross_layer_events?{query}"
    req = Request(
        url,
        headers={
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Prefer": "count=exact",
        },
        method="GET",
    )

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                payload = loads(response.read().decode("utf-8"))
                if not payload:
                    return None
                return payload[0]
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"cross_layer_events read HTTP error {exc.code}: {detail}") from exc
        except (TimeoutError, socket.timeout, URLError) as exc:
            if attempt == MAX_RETRIES:
                reason = getattr(exc, "reason", exc)
                raise RuntimeError(f"cross_layer_events read connection error: {reason}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("cross_layer_events read connection error: exhausted retries")


def should_persist_cross_layer_event(candidate: dict, last_row: dict | None) -> bool:
    if last_row is None:
        return True

    return build_state_signature(candidate) != build_state_signature(last_row)


def persist_cross_layer_event(result: dict) -> None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    req = _build_cross_layer_request(result)

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS):
                return
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"cross_layer_events HTTP error {exc.code}: {detail}") from exc
        except (TimeoutError, socket.timeout, URLError) as exc:
            if attempt == MAX_RETRIES:
                reason = getattr(exc, "reason", exc)
                raise RuntimeError(f"cross_layer_events connection error: {reason}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("cross_layer_events connection error: exhausted retries")


def process_cross_layer_events(lookback_minutes: int = 180) -> dict[str, int]:
    now_ms = int(time.time() * 1000)
    ts_from = now_ms - (lookback_minutes * 60 * 1000)
    risk_rows = load_risk(ts_from, now_ms)

    counters = {
        "total_risk_rows": 0,
        "high_risk_rows": 0,
        "processed": 0,
        "errors": 0,
    }

    for risk_row in risk_rows:
        counters["total_risk_rows"] += 1

        risk_value = _risk_value(risk_row)
        if risk_value is None or risk_value < 3:
            continue

        counters["high_risk_rows"] += 1

        try:
            event_ts_ms = get_event_ts_ms(risk_row)
            if event_ts_ms is None:
                raise ValueError("risk event has no timestamp")

            context = get_latest_cross_context_for_event(event_ts_ms)
            result = classify_cross_event(risk_row, context)

            symbol = result.get("symbol")
            if symbol:
                try:
                    last_row = load_last_cross_layer_event(str(symbol))
                except Exception:
                    logger.exception(
                        "failed to load last cross-layer event, skip persist: symbol=%s",
                        symbol,
                    )
                    continue
            else:
                last_row = None

            if not should_persist_cross_layer_event(result, last_row):
                logger.info(
                    "cross-layer state unchanged, skip persist: symbol=%s state=%s",
                    symbol,
                    build_state_signature(result),
                )
                continue

            persist_cross_layer_event(result)
            counters["processed"] += 1
        except Exception:
            counters["errors"] += 1
            logger.exception(
                "cross-layer classification failed for row: %s",
                risk_row,
            )

    return counters


