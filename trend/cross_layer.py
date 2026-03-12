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
from data.queries import load_bybit_market_state, load_deribit, load_event, load_risk

logger = logging.getLogger(__name__)

OPTIONS_FRESHNESS_MS = 45 * 60 * 1000
DERIBIT_FRESHNESS_MS = 15 * 60 * 1000
CLASSIFIER_VERSION = "cross_v1"
REQUEST_TIMEOUT_SECONDS = 10
MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.5
WINDOW_MS = 30 * 60 * 1000
WINDOW_JOB_LAG_MINUTES = 2

WINDOW_RISK_AVG_THRESHOLD = 2.5
WINDOW_RISK_MAX_THRESHOLD = 4.0
WINDOW_RISK_COUNT_THRESHOLD = 3
ALERT_EVENT_COOLDOWN_MINUTES = 30

SOURCE_MODE_ALERT_EVENT = "ALERT_EVENT"
SOURCE_MODE_WINDOW_30M = "WINDOW_30M"


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


def _nearest_fresh_row_after(rows: list[dict], event_ts_ms: int, freshness_ms: int) -> dict | None:
    nearest: dict | None = None
    nearest_ts: int | None = None

    for row in rows:
        row_ts = _row_ts_ms(row)
        if row_ts is None or row_ts < event_ts_ms:
            continue

        if row_ts - event_ts_ms > freshness_ms:
            continue

        if nearest_ts is None or row_ts < nearest_ts:
            nearest = row
            nearest_ts = row_ts

    return nearest


def get_latest_bybit_context(event_ts_ms: int) -> dict | None:
    ts_from = max(0, event_ts_ms - OPTIONS_FRESHNESS_MS)
    rows = load_bybit_market_state(ts_from, event_ts_ms)
    return _latest_fresh_row(rows, event_ts_ms, OPTIONS_FRESHNESS_MS)


def get_bybit_context_for_window(window_end_ts_ms: int) -> dict | None:
    ts_from = max(0, window_end_ts_ms - OPTIONS_FRESHNESS_MS)
    rows_before_end = load_bybit_market_state(ts_from, window_end_ts_ms)
    latest_before_end = _latest_fresh_row(rows_before_end, window_end_ts_ms, OPTIONS_FRESHNESS_MS)

    if latest_before_end is not None:
        return latest_before_end

    rows_after_end = load_bybit_market_state(window_end_ts_ms, window_end_ts_ms + OPTIONS_FRESHNESS_MS)
    return _nearest_fresh_row_after(rows_after_end, window_end_ts_ms, OPTIONS_FRESHNESS_MS)


def get_latest_deribit_context(event_ts_ms: int) -> dict[str, dict | None]:
    ts_from = max(0, event_ts_ms - DERIBIT_FRESHNESS_MS)

    btc_rows = load_deribit(ts_from, event_ts_ms, symbol="BTC")
    eth_rows = load_deribit(ts_from, event_ts_ms, symbol="ETH")

    return {
        "BTC": _latest_fresh_row(btc_rows, event_ts_ms, DERIBIT_FRESHNESS_MS),
        "ETH": _latest_fresh_row(eth_rows, event_ts_ms, DERIBIT_FRESHNESS_MS),
    }


def get_deribit_context_for_window(window_end_ts_ms: int) -> dict[str, dict | None]:
    return get_latest_deribit_context(window_end_ts_ms)


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


def get_cross_context_for_window(window_end_ts_ms: int) -> CrossContext:
    bybit_row = get_bybit_context_for_window(window_end_ts_ms)
    deribit_rows = get_deribit_context_for_window(window_end_ts_ms)
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


def _build_base_cross_result(
    *,
    ts_unix_ms: int,
    symbol: str,
    event_key: str,
    source_mode: str,
    context: CrossContext,
) -> dict:
    result = {
        "ts_unix_ms": ts_unix_ms,
        "event_key": event_key,
        "symbol": symbol,
        "source_mode": source_mode,
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


def _to_float_or_none(value) -> float | None:
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


def compute_risk_bucket(risk: float | int | None) -> str | None:
    if risk is None:
        return None

    value = float(risk)
    if value >= 5:
        return "5+"
    if value >= 4:
        return "4"
    return "3"


def classify_alert_event_cross(alert_row: dict, context: CrossContext) -> dict:
    data = alert_row.get("data", {})
    event_ts_ms = get_event_ts_ms(alert_row)
    if event_ts_ms is None:
        raise ValueError("alert_sent event has no timestamp")

    symbol = data.get("symbol")
    if not symbol:
        raise ValueError("alert_sent event has no symbol")

    risk_value = _to_float_or_none(data.get("risk"))
    result = _build_base_cross_result(
        ts_unix_ms=event_ts_ms,
        symbol=str(symbol),
        event_key=f"{symbol}:{event_ts_ms}:{SOURCE_MODE_ALERT_EVENT}",
        source_mode=SOURCE_MODE_ALERT_EVENT,
        context=context,
    )

    result.update(
        {
            "source_event_ts_ms": event_ts_ms,
            "risk": risk_value,
            "risk_bucket": compute_risk_bucket(risk_value),
            "price": data.get("price"),
            "direction": data.get("direction"),
            "risk_driver": data.get("risk_driver"),
        }
    )
    return result


def _aggregate_window_risk_by_symbol(rows: list[dict]) -> dict[str, dict]:
    acc: dict[str, dict] = {}

    for row in rows:
        data = row.get("data", {})
        symbol = data.get("symbol")
        risk_value = _to_float_or_none(data.get("risk"))
        if not symbol or risk_value is None:
            continue

        row_ts_ms = get_event_ts_ms(row)
        bucket = acc.setdefault(
            str(symbol),
            {
                "count": 0,
                "risk_sum": 0.0,
                "risk_max": None,
                "count_risk_ge_3": 0,
                "anchor_ts_ms": None,
                "anchor_price": None,
                "anchor_direction": None,
            },
        )

        bucket["count"] += 1
        bucket["risk_sum"] += risk_value
        if bucket["risk_max"] is None or risk_value > bucket["risk_max"]:
            bucket["risk_max"] = risk_value
            bucket["anchor_ts_ms"] = row_ts_ms
            bucket["anchor_price"] = data.get("price")
            bucket["anchor_direction"] = data.get("direction")
        elif bucket["risk_max"] == risk_value and row_ts_ms is not None:
            anchor_ts_ms = _to_int_ms(bucket["anchor_ts_ms"])
            if anchor_ts_ms is None or row_ts_ms > anchor_ts_ms:
                bucket["anchor_ts_ms"] = row_ts_ms
                bucket["anchor_price"] = data.get("price")
                bucket["anchor_direction"] = data.get("direction")

        if risk_value >= 3:
            bucket["count_risk_ge_3"] += 1

    aggregated: dict[str, dict] = {}
    for symbol, bucket in acc.items():
        count = bucket["count"]
        risk_avg = bucket["risk_sum"] / count if count else None
        risk_max = bucket["risk_max"]
        count_risk_ge_3 = bucket["count_risk_ge_3"]

        aggregated[symbol] = {
            "risk_avg": risk_avg,
            "risk_max": risk_max,
            "count_risk_ge_3": count_risk_ge_3,
            "qualifies": (
                (risk_avg is not None and risk_avg >= WINDOW_RISK_AVG_THRESHOLD)
                or (risk_max is not None and risk_max >= WINDOW_RISK_MAX_THRESHOLD)
                or (count_risk_ge_3 >= WINDOW_RISK_COUNT_THRESHOLD)
            ),
            "source_event_ts_ms": _to_int_ms(bucket["anchor_ts_ms"]),
            "price": bucket["anchor_price"],
            "direction": bucket["anchor_direction"],
        }

    return aggregated


def classify_window_event_cross(
    *,
    symbol: str,
    window_start_ts_ms: int,
    window_end_ts_ms: int,
    risk_avg: float | None,
    risk_max: float | None,
    count_risk_ge_3: int,
    source_event_ts_ms: int | None,
    price,
    direction,
    context: CrossContext,
) -> dict:
    result = _build_base_cross_result(
        ts_unix_ms=window_end_ts_ms,
        symbol=symbol,
        event_key=f"{symbol}:{window_end_ts_ms}:{SOURCE_MODE_WINDOW_30M}",
        source_mode=SOURCE_MODE_WINDOW_30M,
        context=context,
    )

    result.update(
        {
            "window_start_ts_ms": window_start_ts_ms,
            "window_end_ts_ms": window_end_ts_ms,
            "source_event_ts_ms": source_event_ts_ms,
            "price": price,
            "direction": direction,
            "risk": risk_max,
            "risk_bucket": compute_risk_bucket(risk_max),
            "risk_avg": risk_avg,
            "risk_max": risk_max,
            "count_risk_ge_3": count_risk_ge_3,
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


def _request_json(req: Request) -> list[dict]:
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            with urlopen(req, timeout=REQUEST_TIMEOUT_SECONDS) as response:
                return loads(response.read().decode("utf-8"))
        except HTTPError as exc:
            detail = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
            raise RuntimeError(f"cross_layer_events HTTP error {exc.code}: {detail}") from exc
        except (TimeoutError, socket.timeout, URLError) as exc:
            if attempt == MAX_RETRIES:
                reason = getattr(exc, "reason", exc)
                raise RuntimeError(f"cross_layer_events connection error: {reason}") from exc
            time.sleep(RETRY_BACKOFF_SECONDS * attempt)

    raise RuntimeError("cross_layer_events connection error: exhausted retries")


def has_window_30m_been_processed(window_end_ts_ms: int) -> bool:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    query = urlencode(
        [
            ("source_mode", f"eq.{SOURCE_MODE_WINDOW_30M}"),
            ("window_end_ts_ms", f"eq.{window_end_ts_ms}"),
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
    rows = _request_json(req)
    return bool(rows)


def get_latest_alert_event_for_symbol(symbol: str) -> dict | None:
    if not SUPABASE_URL or not SUPABASE_KEY:
        raise RuntimeError("Supabase credentials not set")

    query = urlencode(
        [
            ("source_mode", f"eq.{SOURCE_MODE_ALERT_EVENT}"),
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
        },
        method="GET",
    )
    rows = _request_json(req)
    return rows[0] if rows else None


def should_persist_alert_event(result: dict) -> bool:
    symbol = result.get("symbol")
    event_ts_ms = _to_int_ms(result.get("ts_unix_ms"))
    if not symbol or event_ts_ms is None:
        return True

    latest = get_latest_alert_event_for_symbol(str(symbol))
    if latest is None:
        return True

    latest_ts_ms = _to_int_ms(latest.get("ts_unix_ms"))
    if latest_ts_ms is None:
        return True

    cooldown_ms = ALERT_EVENT_COOLDOWN_MINUTES * 60 * 1000
    if event_ts_ms - latest_ts_ms >= cooldown_ms:
        return True

    state_fields = ("context_status", "cross_type", "market_mode", "bybit_regime")
    state_changed = any(result.get(field) != latest.get(field) for field in state_fields)
    return state_changed


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


def process_alert_event_cross_layer(lookback_minutes: int = 180) -> dict[str, int]:
    now_ms = int(time.time() * 1000)
    ts_from = now_ms - (lookback_minutes * 5 * 1000)
    alert_rows = load_event("alert_sent", ts_from, now_ms)

    counters = {
        "total_alert_rows": 0,
        "processed": 0,
        "skipped_cooldown": 0,
        "errors": 0,
    }

    for alert_row in alert_rows:
        counters["total_alert_rows"] += 1

        try:
            event_ts_ms = get_event_ts_ms(alert_row)
            if event_ts_ms is None:
                raise ValueError("alert event has no timestamp")

            context = get_latest_cross_context_for_event(event_ts_ms)
            result = classify_alert_event_cross(alert_row, context)
            if not should_persist_alert_event(result):
                counters["skipped_cooldown"] += 1
                continue
            persist_cross_layer_event(result)
            counters["processed"] += 1
        except Exception:
            counters["errors"] += 1
            logger.exception("cross-layer ALERT_EVENT classification failed for row: %s", alert_row)

    return counters


def get_last_completed_window_30m(now_ts_ms: int | None = None, lag_minutes: int = WINDOW_JOB_LAG_MINUTES) -> tuple[int, int]:
    if now_ts_ms is None:
        now_ts_ms = int(time.time() * 1000)

    effective_now = now_ts_ms - (lag_minutes * 60 * 1000)
    window_end_ts_ms = (effective_now // WINDOW_MS) * WINDOW_MS
    window_start_ts_ms = window_end_ts_ms - WINDOW_MS
    return window_start_ts_ms, window_end_ts_ms


def process_window_30m_cross_layer(window_start_ts_ms: int, window_end_ts_ms: int) -> dict[str, int]:
    risk_rows = load_risk(window_start_ts_ms, window_end_ts_ms)
    aggregated = _aggregate_window_risk_by_symbol(risk_rows)

    counters = {
        "window_start_ts_ms": window_start_ts_ms,
        "window_end_ts_ms": window_end_ts_ms,
        "total_symbols": len(aggregated),
        "qualified_symbols": 0,
        "processed": 0,
        "errors": 0,
    }

    if has_window_30m_been_processed(window_end_ts_ms):
        counters["already_processed"] = 1
        return counters

    context = get_cross_context_for_window(window_end_ts_ms)

    for symbol, stats in aggregated.items():
        if not stats["qualifies"]:
            continue

        counters["qualified_symbols"] += 1

        try:
            result = classify_window_event_cross(
                symbol=symbol,
                window_start_ts_ms=window_start_ts_ms,
                window_end_ts_ms=window_end_ts_ms,
                risk_avg=stats["risk_avg"],
                risk_max=stats["risk_max"],
                count_risk_ge_3=stats["count_risk_ge_3"],
                source_event_ts_ms=stats["source_event_ts_ms"],
                price=stats["price"],
                direction=stats["direction"],
                context=context,
            )
            persist_cross_layer_event(result)
            counters["processed"] += 1
        except Exception:
            counters["errors"] += 1
            logger.exception(
                "cross-layer WINDOW_30M classification failed: symbol=%s window_end=%s",
                symbol,
                window_end_ts_ms,
            )

    return counters


def process_latest_window_30m_cross_layer(lag_minutes: int = WINDOW_JOB_LAG_MINUTES) -> dict[str, int]:
    window_start_ts_ms, window_end_ts_ms = get_last_completed_window_30m(lag_minutes=lag_minutes)
    return process_window_30m_cross_layer(window_start_ts_ms, window_end_ts_ms)




