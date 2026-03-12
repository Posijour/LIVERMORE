import time
from datetime import datetime, timezone

from persistence.state_history import get_state_persistence_hours
from time_utils import parse_window
from tg.bot_config import MAX_TELEGRAM_TEXT_LEN

_PERSISTENCE_CACHE_TTL_SECONDS = 45
_PERSISTENCE_CACHE = {
    "value": None,
    "at": 0.0,
}


def _extract_iv_slope(deribit: dict) -> float:
    for key in ("iv_slope", "iv_slope_avg"):
        value = deribit.get(key)
        if isinstance(value, (int, float)):
            return float(value)
    return 0.0


def _format_last_snapshot_utc(ts_value) -> str:
    if ts_value is None:
        return "N/A"

    if isinstance(ts_value, str):
        try:
            dt = datetime.fromisoformat(ts_value.replace("Z", "+00:00"))
        except ValueError:
            return "N/A"
    elif isinstance(ts_value, (int, float)):
        ts_value = int(ts_value)
        if ts_value < 10_000_000_000:
            ts_value *= 1000
        dt = datetime.fromtimestamp(ts_value / 1000, tz=timezone.utc)
    else:
        return "N/A"

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    else:
        dt = dt.astimezone(timezone.utc)

    return dt.strftime("%H:%M UTC")


def snapshot_to_text(snapshot):
    r = snapshot.risk or {}
    o = snapshot.options or {}
    v = snapshot.deribit or {}
    d = getattr(snapshot, "divergence", {}) or {}

    return (
        f"Risk: {r.get('avg_risk', 0):.2f} | "
        f"RiskAct: {r.get('risk_2plus_pct', 0):.1f}%\n"
        f"Struct: {o.get('dominant_phase_pct', 0):.1f}% "
        f"{o.get('dominant_phase')}\n"
        f"Vol: {_extract_iv_slope(v):+.2f} "
        f"{v.get('vbi_state')}\n"
        f"Div: {d.get('count', 0)} | "
        f"{d.get('share', 0)}% | "
        f"{d.get('dominant_type')}"
    )


def _format_hours(hours: int) -> str:
    return f"{hours}h"


def _humanize_risk_band(state_key: str, value: str) -> str:
    avg_risk_map = {
        "LE_0_7": "≤ 0.7",
        "GT_0_7": "> 0.7",
        "GT_1_0": "> 1.0",
        "NO_DATA": "no data",
    }
    riskact_map = {
        "LE_20": "≤ 20%",
        "GT_20": "> 20%",
        "GT_30": "> 30%",
        "NO_DATA": "no data",
    }

    if state_key == "avg_risk":
        return avg_risk_map.get(value, value)
    if state_key == "risk_2plus_pct":
        return riskact_map.get(value, value)
    return value


def _format_risk_band_persistence(label: str, state_key: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    human_value = _humanize_risk_band(state_key, value)
    return f"{label} {human_value} for {_format_hours(hours)}"


def _format_named_state_persistence(label: str, state: tuple[str, int] | None) -> str:
    if state is None:
        return f"{label}: no data"

    value, hours = state
    return f"{label}: {value} for {_format_hours(hours)}"


def build_market_persistence_block() -> str:
    avg_risk_state = get_state_persistence_hours("risk", "avg_risk", symbol=None)
    riskact_state = get_state_persistence_hours("risk", "risk_2plus_pct", symbol=None)
    struct_state = get_state_persistence_hours("structure", "dominant_phase", symbol=None)
    vol_state = get_state_persistence_hours("volatility", "vbi_state", symbol=None)

    lines = ["Market Persistence:"]
    lines.append(_format_risk_band_persistence("Risk", "avg_risk", avg_risk_state))
    lines.append(_format_risk_band_persistence("RiskAct", "risk_2plus_pct", riskact_state))
    lines.append(_format_named_state_persistence("Struct", struct_state))
    lines.append(_format_named_state_persistence("Vol", vol_state))
    return "\n".join(lines)


def build_market_persistence_block_cached() -> str:
    now = time.monotonic()
    cached_at = _PERSISTENCE_CACHE["at"]
    cached_value = _PERSISTENCE_CACHE["value"]

    if cached_value and (now - cached_at) < _PERSISTENCE_CACHE_TTL_SECONDS:
        return cached_value

    fresh_value = build_market_persistence_block()
    _PERSISTENCE_CACHE["value"] = fresh_value
    _PERSISTENCE_CACHE["at"] = now
    return fresh_value


def parse_window_safe(window: str) -> tuple[int, int] | None:
    try:
        return parse_window(window)
    except ValueError:
        return None


def split_text_chunks(text: str, chunk_size: int = MAX_TELEGRAM_TEXT_LEN):
    lines = text.splitlines(keepends=True)
    chunks = []
    current = ""

    for line in lines:
        if len(line) > chunk_size:
            if current:
                chunks.append(current)
                current = ""
            for i in range(0, len(line), chunk_size):
                chunks.append(line[i:i + chunk_size])
            continue

        if len(current) + len(line) > chunk_size:
            chunks.append(current)
            current = line
        else:
            current += line

    if current:
        chunks.append(current)

    return chunks or [""]


def _avg(rows: list[dict], field: str) -> float | None:
    values = [r.get("data", {}).get(field) for r in rows]
    numeric = [float(v) for v in values if isinstance(v, (int, float))]
    if not numeric:
        return None
    return sum(numeric) / len(numeric)


def _min(rows: list[dict], field: str) -> float | None:
    values = [r.get("data", {}).get(field) for r in rows]
    numeric = [float(v) for v in values if isinstance(v, (int, float))]
    if not numeric:
        return None
    return min(numeric)


def _max(rows: list[dict], field: str) -> float | None:
    values = [r.get("data", {}).get(field) for r in rows]
    numeric = [float(v) for v in values if isinstance(v, (int, float))]
    if not numeric:
        return None
    return max(numeric)


def _mode(rows: list[dict], field: str):
    values = [r.get("data", {}).get(field) for r in rows]
    values = [v for v in values if v not in (None, "")]
    if not values:
        return None
    return max(set(values), key=values.count)


def _latest(rows: list[dict], field: str):
    if not rows:
        return None
    latest_row = max(rows, key=lambda r: r.get("ts", 0))
    return latest_row.get("data", {}).get(field)


def _extract_status_price(rows: list[dict]) -> float | None:
    price_fields = (
        "price",
        "last_price",
        "mark_price",
        "index_price",
        "close",
        "mid_price",
    )

    if not rows:
        return None

    latest_row = max(rows, key=lambda r: r.get("ts", 0))
    data = latest_row.get("data", {})

    for field in price_fields:
        value = data.get(field)
        if isinstance(value, (int, float)):
            return float(value)

    return None


def _fmt_price(value: float | None) -> str:
    if not isinstance(value, (int, float)):
        return "N/A"

    if abs(value) >= 1000:
        return f"{value:,.0f}"

    return f"{value:,.2f}"


def _fmt_number(value, digits: int = 3, signed: bool = False) -> str:
    if value is None or not isinstance(value, (int, float)):
        return "N/A"
    return f"{value:+.{digits}f}" if signed else f"{value:.{digits}f}"


def _fmt_text(value) -> str:
    return "N/A" if value in (None, "") else str(value)


def _derive_term_structure(iv_slope, curvature) -> str:
    if not isinstance(iv_slope, (int, float)) or not isinstance(curvature, (int, float)):
        return "N/A"
    return f"iv_slope={iv_slope:+.3f}, curvature={curvature:+.3f}"


def aggregate_options_snapshot(bybit_rows, okx_rows, deribit_rows) -> dict:
    return {
        "bybit": {
            "regime": _mode(bybit_rows, "regime"),
            "mci": _avg(bybit_rows, "mci"),
            "mci_slope": _avg(bybit_rows, "mci_slope"),
            "mci_phase": _mode(bybit_rows, "mci_phase"),
            "confidence": _latest(bybit_rows, "confidence"),
        },
        "okx": {
            "okx_olsi_latest": _latest(okx_rows, "okx_olsi_avg"),
            "okx_olsi_avg": _avg(okx_rows, "okx_olsi_avg"),
            "okx_olsi_min": _min(okx_rows, "okx_olsi_avg"),
            "okx_olsi_max": _max(okx_rows, "okx_olsi_avg"),
            "okx_olsi_slope": _latest(okx_rows, "okx_olsi_slope"),
            "okx_liquidity_regime": _mode(okx_rows, "okx_liquidity_regime"),
            "divergence": _mode(okx_rows, "divergence_type"),
            "divergence_diff": _avg(okx_rows, "divergence_diff"),
            "divergence_strength": _avg(okx_rows, "divergence_strength"),
            "divergence_strength_label": _mode(okx_rows, "divergence_strength_label"),
        },
        "deribit": {
            "vbi_state": _mode(deribit_rows, "vbi_state"),
            "iv_slope": _latest(deribit_rows, "iv_slope"),
            "curvature": _avg(deribit_rows, "curvature"),
            "skew": _latest(deribit_rows, "skew"),
        },
    }


def render_options_snapshot(window: str, payload: dict) -> str:
    bybit = payload.get("bybit", {})
    okx = payload.get("okx", {})
    deribit = payload.get("deribit", {})

    def arrow(value):
        if not isinstance(value, (int, float)):
            return ""
        return "↑" if value > 0 else "↓" if value < 0 else "→"

    def divergence_strength_label(value):
        if not isinstance(value, (int, float)):
            return "N/A"
        if value < 0.2:
            return "VERY_WEAK"
        if value < 0.4:
            return "WEAK"
        if value < 0.6:
            return "MODERATE"
        if value < 0.8:
            return "STRONG"
        return "VERY_STRONG"

    iv_slope = deribit.get("iv_slope")

    term_structure = (
        "flat"
        if isinstance(iv_slope, (int, float)) and abs(iv_slope) < 0.3
        else "upward"
        if isinstance(iv_slope, (int, float)) and iv_slope > 0
        else "downward"
        if isinstance(iv_slope, (int, float))
        else "N/A"
    )

    confidence_label = (
        "LOW"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.30
        else "WEAK"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.50
        else "MODERATE"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.70
        else "HIGH"
        if isinstance(bybit.get("confidence"), (int, float))
        and bybit.get("confidence") < 0.85
        else "VERY_HIGH"
        if isinstance(bybit.get("confidence"), (int, float))
        else "N/A"
    )

    divergence_strength_value = okx.get("divergence_strength")
    divergence_strength_text = _fmt_text(okx.get("divergence_strength_label"))
    if divergence_strength_text == "N/A":
        divergence_strength_text = divergence_strength_label(divergence_strength_value)

    return (
        f"=== OPTIONS SNAPSHOT ({window}) ===\n\n"
        "Behavior (Bybit):\n"
        f"• MCI: {_fmt_number(bybit.get('mci'), 2)} "
        f"({arrow(bybit.get('mci_slope'))})\n"
        f"• Regime: {_fmt_text(bybit.get('regime'))}\n"
        f"• Confidence: {_fmt_number(bybit.get('confidence'), 2)} ({confidence_label})\n\n"
        "Liquidity (OKX):\n"
        f"• OLSI: {_fmt_number(okx.get('okx_olsi_latest'), 3)} "
        f"({arrow(okx.get('okx_olsi_slope'))})\n"
        f"• Regime: {_fmt_text(okx.get('okx_liquidity_regime'))}\n\n"
        "Dislocation (Bybit ↔ OKX):\n"
        f"• {_fmt_text(okx.get('divergence'))}\n"
        f"• Strength: {_fmt_number(divergence_strength_value, 2)} ({divergence_strength_text})\n\n"
        "Volatility (Deribit):\n"
        f"• VBI: {_fmt_text(deribit.get('vbi_state'))}\n"
        f"• Term structure: {term_structure}"
    )
