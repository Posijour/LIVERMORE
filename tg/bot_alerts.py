from datetime import datetime, timezone

from tg.bot_config import ALERT_COOLDOWN, ANOMALY_ALERT_COOLDOWN

_LAST_ALERTS = {}  # (symbol, div_type) -> event_ts
_LAST_ANOMALIES = {}  # anomaly_key -> event_ts


def normalize_event_ts_ms(value):
    if isinstance(value, str):
        if value.isdigit():
            value = int(value)
        else:
            try:
                dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError:
                return None

            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            value = int(dt.timestamp() * 1000)

    if isinstance(value, (int, float)):
        value = int(value)
        if value < 10_000_000_000:
            value *= 1000
        return value

    return None


def can_send_alert(symbol, div_type, event_ts):
    key = (symbol, div_type)

    event_ts = normalize_event_ts_ms(event_ts)
    if event_ts is None:
        return False

    last_event_ts = _LAST_ALERTS.get(key)
    if last_event_ts and event_ts <= last_event_ts:
        return False

    if last_event_ts and (event_ts - last_event_ts) < ALERT_COOLDOWN * 1000:
        return False

    _LAST_ALERTS[key] = event_ts
    return True


def can_send_anomaly(anomaly_key, event_ts):
    event_ts = normalize_event_ts_ms(event_ts)
    if event_ts is None:
        return False

    last_event_ts = _LAST_ANOMALIES.get(anomaly_key)
    if last_event_ts and event_ts <= last_event_ts:
        return False

    if last_event_ts and (event_ts - last_event_ts) < ANOMALY_ALERT_COOLDOWN * 1000:
        return False

    _LAST_ANOMALIES[anomaly_key] = event_ts
    return True


def detect_buildup_anomalies(alert_rows):
    buildup_rows = []

    for r in alert_rows:
        data = r.get("data", {}) or {}
        if data.get("type") != "BUILDUP":
            continue

        ts = r.get("ts") or r.get("created_at")
        symbol = data.get("symbol")
        direction = data.get("direction")

        if not ts or not symbol:
            continue

        buildup_rows.append({
            "ts": ts,
            "symbol": symbol,
            "direction": direction,
        })

    if not buildup_rows:
        return []

    for row in buildup_rows:
        row["ts_ms"] = normalize_event_ts_ms(row["ts"])

    buildup_rows = [r for r in buildup_rows if r["ts_ms"] is not None]
    buildup_rows.sort(key=lambda x: x["ts_ms"])

    anomalies = []

    per_symbol = {}
    for row in buildup_rows:
        per_symbol[row["symbol"]] = per_symbol.get(row["symbol"], 0) + 1

    top_symbol = None
    top_count = 0
    for symbol, count in per_symbol.items():
        if count > top_count:
            top_symbol = symbol
            top_count = count

    if top_symbol and top_count >= 3:
        last_ts = max(r["ts_ms"] for r in buildup_rows if r["symbol"] == top_symbol)
        anomalies.append({
            "key": f"REPEATED_BUILDUP:{top_symbol}",
            "event_ts": last_ts,
            "text": (
                "⚠️ Futures anomaly detected\n"
                f"{top_symbol} — repeated buildups\n"
                f"Count: {top_count}"
            ),
        })

    n = len(buildup_rows)
    left = 0
    for right in range(n):
        while buildup_rows[right]["ts_ms"] - buildup_rows[left]["ts_ms"] > 180000:
            left += 1

        window = buildup_rows[left:right + 1]
        if len(window) >= 5:
            distinct_symbols = {r["symbol"] for r in window}
            if len(distinct_symbols) >= 3:
                last_ts = window[-1]["ts_ms"]
                anomalies.append({
                    "key": "MULTI_COIN_BUILDUP_BURST",
                    "event_ts": last_ts,
                    "text": (
                        "⚠️ Futures anomaly detected\n"
                        "Multi-coin buildup burst\n"
                        f"Events: {len(window)} | Symbols: {len(distinct_symbols)}"
                    ),
                })
                break

    return anomalies
