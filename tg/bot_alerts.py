from datetime import datetime, timezone
import re

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


def _pick_first(mapping, *keys):
    for key in keys:
        value = mapping.get(key)
        if value not in (None, ""):
            return value
    return None


def _normalize_text(value):
    if value in (None, ""):
        return ""

    normalized = re.sub(r"[^a-z0-9]+", " ", str(value).lower())
    return " ".join(normalized.split())


def _strip_duplicate_message_lines(message, title, summary_line):
    if not message:
        return None

    lines = [line.strip() for line in str(message).splitlines() if line.strip()]
    if not lines:
        return None

    title_norm = _normalize_text(title)
    summary_norm = _normalize_text(summary_line)
    filtered = []

    for line in lines:
        line_norm = _normalize_text(line)
        if not line_norm:
            continue
        if line_norm == title_norm:
            continue
        if summary_norm and (
            line_norm == summary_norm
            or summary_norm in line_norm
            or line_norm in summary_norm
        ):
            continue
        filtered.append(line)

    return "\n".join(filtered) if filtered else None


def build_anomaly_alert(row):
    data = row.get("data", {}) or {}
    if not isinstance(data, dict):
        data = {}

    event_ts = (
        row.get("ts")
        or row.get("created_at")
        or _pick_first(data, "ts", "timestamp", "time", "created_at", "ts_unix_ms")
    )
    event_ts = normalize_event_ts_ms(event_ts)
    if event_ts is None:
        return None

    symbol = _pick_first(data, "symbol", "ticker", "asset", "coin", "instrument")
    anomaly_type = _pick_first(
        data,
        "anomaly_type",
        "type",
        "kind",
        "name",
        "signal_type",
        "category",
    )
    anomaly_id = _pick_first(data, "id", "event_id", "anomaly_id", "signal_id")
    message = _pick_first(data, "message", "text", "description", "summary")
    severity = _pick_first(data, "severity", "level", "priority")

    key_parts = [str(v).strip() for v in (symbol, anomaly_type, anomaly_id) if v not in (None, "")]
    anomaly_key = ":".join(key_parts) if key_parts else f"anomaly:{event_ts}"

    title = "⚠️ Futures anomaly detected"
    summary_line = None
    body_parts = []

    if symbol and anomaly_type:
        summary_line = f"{symbol} — {anomaly_type}"
    elif symbol:
        summary_line = str(symbol)
    elif anomaly_type:
        summary_line = str(anomaly_type)

    cleaned_message = _strip_duplicate_message_lines(message, title, summary_line)

    if summary_line:
        body_parts.append(summary_line)

    if severity:
        body_parts.append(f"Severity: {severity}")

    if cleaned_message:
        body_parts.append(cleaned_message)

    text = title if not body_parts else title + "\n" + "\n".join(body_parts)

    return {
        "key": anomaly_key,
        "event_ts": event_ts,
        "text": text,
    }
