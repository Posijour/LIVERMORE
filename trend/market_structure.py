# trend/market_structure.py
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from time_utils import parse_window
from data.queries import load_risk, load_bybit_market_state, load_deribit


# ----------------- small math helpers -----------------

def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))

def _stdev(xs: List[float]) -> Optional[float]:
    xs = [float(x) for x in xs if _is_num(x)]
    if len(xs) < 2:
        return 0.0 if xs else None
    m = sum(xs) / len(xs)
    var = sum((x - m) ** 2 for x in xs) / (len(xs) - 1)
    return math.sqrt(var)

def _clamp01(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    return max(0.0, min(1.0, float(x)))

def _norm01(x: Optional[float], lo: Optional[float], hi: Optional[float]) -> Optional[float]:
    if x is None or lo is None or hi is None or hi <= lo:
        return None
    return _clamp01((x - lo) / (hi - lo))

def _safe_abs(x: Optional[float]) -> Optional[float]:
    return abs(float(x)) if _is_num(x) else None

def _fmt_float(x: Optional[float], digits: int = 2) -> str:
    if x is None or not _is_num(x):
        return "N/A"
    return f"{float(x):.{digits}f}"


# ----------------- data extraction -----------------

def _bucket_hour(ts: int) -> int:
    # ts in seconds, bucket by hour
    return ts - (ts % 3600)

def _extract_symbol(row: dict) -> Optional[str]:
    return row.get("data", {}).get("symbol")

def _extract_risk_value(row: dict) -> Optional[float]:
    # prefer avg_risk (used across your codebase)
    v = row.get("data", {}).get("avg_risk")
    return float(v) if _is_num(v) else None

def _extract_mci(row: dict) -> Optional[float]:
    v = row.get("data", {}).get("mci")
    return float(v) if _is_num(v) else None

def _extract_mci_slope(row: dict) -> Optional[float]:
    v = row.get("data", {}).get("mci_slope")
    return float(v) if _is_num(v) else None

def _extract_iv_slope(row: dict) -> Optional[float]:
    # your bot already reads "iv_slope" and sometimes "iv_slope_avg"
    data = row.get("data", {})
    for k in ("iv_slope", "iv_slope_avg"):
        v = data.get(k)
        if _is_num(v):
            return float(v)
    return None


def _latest_per_symbol(rows: List[dict], value_fn) -> Dict[str, float]:
    """
    Returns latest value per symbol from rows (ts assumed seconds).
    """
    best: Dict[str, Tuple[int, float]] = {}
    for r in rows:
        sym = _extract_symbol(r)
        if not sym:
            continue
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        val = value_fn(r)
        if val is None:
            continue
        ts = int(ts)
        prev = best.get(sym)
        if prev is None or ts > prev[0]:
            best[sym] = (ts, float(val))
    return {k: v for k, (_, v) in best.items()}


def _hourly_dispersion_from_risk_rows(
    risk_rows_12h: List[dict],
    supported_tickers: List[str],
    max_points: int = 12,
) -> List[float]:
    """
    Build up to max_points hourly dispersion_xs values using the latest value per symbol inside each hour bucket.
    Avoids N queries.
    """
    # bucket -> symbol -> (ts, value)
    buckets: Dict[int, Dict[str, Tuple[int, float]]] = {}

    for r in risk_rows_12h:
        sym = _extract_symbol(r)
        if sym not in supported_tickers:
            continue
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        ts = int(ts)
        v = _extract_risk_value(r)
        if v is None:
            continue
        b = _bucket_hour(ts)
        sym_map = buckets.setdefault(b, {})
        prev = sym_map.get(sym)
        if prev is None or ts > prev[0]:
            sym_map[sym] = (ts, float(v))

    # take last max_points buckets (most recent)
    bucket_keys = sorted(buckets.keys())[-max_points:]
    dispersions: List[float] = []
    for b in bucket_keys:
        sym_map = buckets[b]
        vals = []
        for sym in supported_tickers:
            if sym in sym_map:
                vals.append(sym_map[sym][1])
        d = _stdev(vals)
        if d is not None:
            dispersions.append(float(d))
    return dispersions


# ----------------- main compute -----------------

def compute_market_structure(
    supported_tickers: List[str],
    short_lookback_hours: int = 12,
) -> dict:
    """
    Read-only computation from logs:
    - Futures coherence (cross-sectional)
    - Volatility compression score
    - Cross-layer driver (live)
    - Diagnostic regime tag
    """
    # windows
    ts_from_30m, ts_to_now = parse_window("30m")
    ts_from_1h, _ = parse_window("1h")
    ts_from_12h, _ = parse_window(f"{short_lookback_hours}h")

    # --- load rows (bulk) ---
    # IMPORTANT: we assume load_risk supports symbol=None to return multi-symbol rows.
    # If your load_risk requires a symbol, we can adjust later (but try bulk first).
    risk_rows_30m = load_risk(ts_from_30m, ts_to_now, None)
    risk_rows_1h = load_risk(ts_from_1h, ts_to_now, None)
    risk_rows_12h = load_risk(ts_from_12h, ts_to_now, None)

    bybit_rows_1h = load_bybit_market_state(ts_from_1h, ts_to_now)
    bybit_rows_12h = load_bybit_market_state(ts_from_12h, ts_to_now)

    deribit_rows_1h = load_deribit(ts_from_1h, ts_to_now)
    deribit_rows_12h = load_deribit(ts_from_12h, ts_to_now)

    # --- Futures coherence (now) ---
    latest_risk = _latest_per_symbol(risk_rows_30m or [], _extract_risk_value)
    # keep only supported tickers
    risk_vals_now = [latest_risk.get(sym) for sym in supported_tickers]
    risk_vals_now = [v for v in risk_vals_now if _is_num(v)]

    dispersion_xs_now = _stdev(risk_vals_now)

    # adaptive normalization for dispersion_xs using 12h hourly samples (max 12 points)
    disp_samples = _hourly_dispersion_from_risk_rows(risk_rows_12h or [], supported_tickers, max_points=12)
    disp_lo = min(disp_samples) if disp_samples else None
    disp_hi = max(disp_samples) if disp_samples else None

    disp_norm = _norm01(dispersion_xs_now, disp_lo, disp_hi)
    coherence = None if disp_norm is None else (1.0 - disp_norm)

    if coherence is None:
        coherence_label = "N/A"
    elif coherence >= 0.67:
        coherence_label = "HIGH"
    elif coherence >= 0.40:
        coherence_label = "MEDIUM"
    else:
        coherence_label = "LOW"

    # --- Compression components ---
    # VBI component: prefer low absolute iv_slope (flat term-structure) => more compression
    iv_slope_1h_vals = [_extract_iv_slope(r) for r in (deribit_rows_1h or [])]
    iv_slope_1h_vals = [v for v in iv_slope_1h_vals if _is_num(v)]
    iv_slope_abs_now = _safe_abs(iv_slope_1h_vals[-1]) if iv_slope_1h_vals else None

    iv_abs_12h = [_safe_abs(_extract_iv_slope(r)) for r in (deribit_rows_12h or [])]
    iv_abs_12h = [v for v in iv_abs_12h if _is_num(v)]
    iv_lo = min(iv_abs_12h) if iv_abs_12h else None
    iv_hi = max(iv_abs_12h) if iv_abs_12h else None
    iv_norm = _norm01(iv_slope_abs_now, iv_lo, iv_hi)
    vbi_comp = None if iv_norm is None else (1.0 - iv_norm)

    # MCI component: prefer low MCI (compression) OR low abs(mci_slope)
    mci_1h = [_extract_mci(r) for r in (bybit_rows_1h or [])]
    mci_1h = [v for v in mci_1h if _is_num(v)]
    mci_now = sum(mci_1h) / len(mci_1h) if mci_1h else None

    mci_12h = [_extract_mci(r) for r in (bybit_rows_12h or [])]
    mci_12h = [v for v in mci_12h if _is_num(v)]
    mci_lo = min(mci_12h) if mci_12h else None
    mci_hi = max(mci_12h) if mci_12h else None

    mci_norm = _norm01(mci_now, mci_lo, mci_hi)
    mci_comp = None if mci_norm is None else (1.0 - mci_norm)

    # dispersion component uses same dispersion_xs normalization (low dispersion => more compression)
    disp_comp = None if disp_norm is None else (1.0 - disp_norm)

    if vbi_comp is None or mci_comp is None or disp_comp is None:
        compression_score = None
        compression_label = "N/A"
    else:
        compression_score = 100.0 * (0.4 * vbi_comp + 0.4 * mci_comp + 0.2 * disp_comp)
        if compression_score >= 75:
            compression_label = "EXTREME"
        elif compression_score >= 60:
            compression_label = "HIGH"
        elif compression_score >= 40:
            compression_label = "MEDIUM"
        else:
            compression_label = "LOW"

    # --- Cross-layer driver (LIVE via 1h impulses) ---
    # Futures impulse: change in market avg risk over 1h (earliest vs latest)
    latest_risk_1h = _latest_per_symbol(risk_rows_1h or [], _extract_risk_value)

    # approximate earliest by taking first-seen per symbol (not perfect but stable)
    earliest: Dict[str, Tuple[int, float]] = {}
    for r in (risk_rows_1h or []):
        sym = _extract_symbol(r)
        if sym not in supported_tickers:
            continue
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        ts = int(ts)
        v = _extract_risk_value(r)
        if v is None:
            continue
        prev = earliest.get(sym)
        if prev is None or ts < prev[0]:
            earliest[sym] = (ts, float(v))

    start_vals = [earliest.get(s, (None, None))[1] for s in supported_tickers]
    end_vals = [latest_risk_1h.get(s) for s in supported_tickers]
    start_vals = [v for v in start_vals if _is_num(v)]
    end_vals = [v for v in end_vals if _is_num(v)]
    fut_impulse = None
    if start_vals and end_vals:
        fut_impulse = (sum(end_vals) / len(end_vals)) - (sum(start_vals) / len(start_vals))

    # Options impulse: latest avg mci_slope (abs)
    mci_slope_1h = [_extract_mci_slope(r) for r in (bybit_rows_1h or [])]
    mci_slope_1h = [v for v in mci_slope_1h if _is_num(v)]
    opt_impulse = (sum(mci_slope_1h) / len(mci_slope_1h)) if mci_slope_1h else None

    # Vol impulse: delta of iv_slope over 1h (latest - earliest)
    iv_series = [_extract_iv_slope(r) for r in (deribit_rows_1h or [])]
    iv_series = [v for v in iv_series if _is_num(v)]
    vol_impulse = None
    if len(iv_series) >= 2:
        vol_impulse = iv_series[-1] - iv_series[0]

    impulses = {
        "FUTURES": _safe_abs(fut_impulse),
        "OPTIONS": _safe_abs(opt_impulse),
        "VOL": _safe_abs(vol_impulse),
    }

    # pick leader
    sorted_imp = sorted(
        [(k, v) for k, v in impulses.items() if v is not None],
        key=lambda kv: kv[1],
        reverse=True,
    )

    if not sorted_imp:
        driver = "NONE"
        driver_conf = 0.0
    else:
        top_k, top_v = sorted_imp[0]
        # threshold to avoid noise
        noise_floor = 0.02  # small, because values are already deltas/slopes
        if top_v < noise_floor:
            driver = "NONE"
            driver_conf = 0.0
        elif len(sorted_imp) >= 2:
            second_v = sorted_imp[1][1]
            # if within 15% => ALIGNED
            if second_v > 0 and (top_v - second_v) / top_v <= 0.15:
                driver = "ALIGNED"
                driver_conf = 0.6
            else:
                driver = f"{top_k}_LEAD"
                driver_conf = 0.75
        else:
            driver = f"{top_k}_LEAD"
            driver_conf = 0.6

    # --- Diagnostic regime tag ---
    if compression_label == "EXTREME" and driver in ("OPTIONS_LEAD", "VOL_LEAD"):
        regime = "PRE_BREAKOUT"
    elif compression_label in ("HIGH", "EXTREME") and driver == "FUTURES_LEAD":
        regime = "RANGE_ABSORB"
    elif compression_label == "LOW" and driver == "ALIGNED":
        regime = "TREND_CONFIRM"
    elif compression_label == "N/A":
        regime = "N/A"
    else:
        regime = "MIXED"

    return {
        "coherence": coherence,
        "coherence_label": coherence_label,
        "dispersion_xs": dispersion_xs_now,
        "dispersion_xs_lo": disp_lo,
        "dispersion_xs_hi": disp_hi,

        "compression_score": compression_score,
        "compression_label": compression_label,
        "compression_vbi_comp": vbi_comp,
        "compression_mci_comp": mci_comp,
        "compression_disp_comp": disp_comp,

        "driver": driver,
        "driver_confidence": driver_conf,

        "regime": regime,
    }