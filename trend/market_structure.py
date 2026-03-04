# trend/market_structure.py
import math
from datetime import datetime, timezone
from typing import Dict, List, Optional, Tuple

from time_utils import parse_window
from data.queries import load_risk, load_bybit_market_state, load_deribit


# ----------------- small math helpers -----------------

def _is_num(x) -> bool:
    return isinstance(x, (int, float)) and not (isinstance(x, float) and math.isnan(x))

def _to_float(x) -> Optional[float]:
    if _is_num(x):
        return float(x)
    if isinstance(x, str):
        try:
            v = float(x.strip())
        except ValueError:
            return None
        return None if math.isnan(v) else v
    return None

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
    if x is None or lo is None or hi is None:
        return None
    # Degenerate ranges are common on low-vol datasets (e.g., 1-2 hourly points
    # or fully flat baselines). Return a neutral score instead of N/A so higher
    # level labels/regimes still remain informative.
    if hi == lo:
        return 0.5
    if hi < lo:
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
    # ts should be in milliseconds
    hour_ms = 3600 * 1000
    return ts - (ts % hour_ms)


def _ts_to_ms(ts) -> Optional[int]:
    v = _to_float(ts)
    if v is not None:
        value = int(v)
        return value * 1000 if value < 10_000_000_000 else value

    if isinstance(ts, str):
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return int(dt.timestamp() * 1000)
        except ValueError:
            return None

    return None


def _normalize_symbol(symbol: Optional[str]) -> Optional[str]:
    if not isinstance(symbol, str) or not symbol:
        return None

    value = symbol.upper()

    for sep in ("-", "_", "/", ":"):
        value = value.split(sep, 1)[0]

    for quote in ("USDT", "USD", "PERP"):
        if value.endswith(quote):
            value = value[: -len(quote)]
            break

    return value

def _extract_symbol(row: dict) -> Optional[str]:
    data = row.get("data", {}) if isinstance(row.get("data"), dict) else {}
    return row.get("symbol") or data.get("symbol") or row.get("ticker") or data.get("ticker")

def _extract_risk_value(row: dict) -> Optional[float]:
    # risk_eval may contain either aggregated avg_risk or per-row risk values.
    data = row.get("data", {}) if isinstance(row.get("data"), dict) else {}
    for key in ("avg_risk", "risk", "risk_score"):
        value = _to_float(data.get(key))
        if value is not None:
            return value
    return None

def _extract_mci(row: dict) -> Optional[float]:
    v = row.get("data", {}).get("mci")
    return _to_float(v)

def _extract_mci_slope(row: dict) -> Optional[float]:
    v = row.get("data", {}).get("mci_slope")
    return _to_float(v)

def _extract_iv_slope(row: dict) -> Optional[float]:
    # your bot already reads "iv_slope" and sometimes "iv_slope_avg"
    data = row.get("data", {})
    for k in ("iv_slope", "iv_slope_avg"):
        v = _to_float(data.get(k))
        if v is not None:
            return v
    return None


def _latest_per_symbol(rows: List[dict], value_fn) -> Dict[str, float]:
    """
    Returns latest value per symbol from rows (ts assumed seconds).
    """
    best: Dict[str, Tuple[int, float]] = {}
    for r in rows:
        sym = _normalize_symbol(_extract_symbol(r))
        if not sym:
            continue
        ts = _ts_to_ms(r.get("ts"))
        if ts is None:
            continue
        val = value_fn(r)
        if val is None:
            continue
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
    supported_norm = {_normalize_symbol(s) for s in supported_tickers}
    supported_norm.discard(None)

    for r in risk_rows_12h:
        sym = _normalize_symbol(_extract_symbol(r))
        if sym not in supported_norm:
            continue
        ts = _ts_to_ms(r.get("ts"))
        if ts is None:
            continue
        v = _extract_risk_value(r)
        if v is None:
            continue
        b = _bucket_hour(ts)
        sym_map = buckets.setdefault(b, {})
        prev = sym_map.get(sym)
        if prev is None or ts > prev[0]:
            sym_map[sym] = (ts, float(v))

    # take last max_points buckets (most recent)
        # take last max_points buckets (most recent)
    bucket_keys = sorted(buckets.keys())[-max_points:]
    dispersions: List[float] = []

    MIN_COVERAGE = max(3, int(len(supported_norm) * 0.65))  # ~65% coverage, e.g. 10/16

    for b in bucket_keys:
        sym_map = buckets[b]
        vals = []
        for sym in supported_norm:
            if sym in sym_map:
                vals.append(sym_map[sym][1])

        # ✅ coverage guard: hourly dispersion only makes sense with stable N
        if len(vals) < MIN_COVERAGE:
            continue

        d = _stdev(vals)
        if d is not None:
            dispersions.append(float(d))

    return dispersions

def _hourly_impulses_12h(
    risk_rows_12h: List[dict],
    bybit_rows_12h: List[dict],
    deribit_rows_12h: List[dict],
    supported_norm: List[str],
    max_points: int = 12,
) -> dict:
    """
    Returns hourly samples (up to max_points) for:
      - futures market avg risk
      - options mci_slope abs avg (BTC/ETH market rows)
      - deribit iv_slope abs avg
    Used ONLY for normalization (lo/hi), not for the main signal.
    """
    # --- futures: bucket -> symbol -> latest (ts,value)
    fut_buckets: Dict[int, Dict[str, Tuple[int, float]]] = {}
    for r in (risk_rows_12h or []):
        sym = _extract_symbol(r)
        if not sym:
            continue
        sym = _normalize_symbol(sym)
        if sym not in supported_norm:
            continue
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        ts = int(ts)
        v = _extract_risk_value(r)
        if v is None:
            continue
        b = _bucket_hour(ts)
        m = fut_buckets.setdefault(b, {})
        prev = m.get(sym)
        if prev is None or ts > prev[0]:
            m[sym] = (ts, float(v))

    # --- options: bucket -> list of abs(mci_slope)
    opt_buckets: Dict[int, List[float]] = {}
    for r in (bybit_rows_12h or []):
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        ts = int(ts)
        v = _extract_mci_slope(r)
        if v is None:
            continue
        b = _bucket_hour(ts)
        opt_buckets.setdefault(b, []).append(abs(float(v)))

    # --- vol: bucket -> list of abs(iv_slope)
    vol_buckets: Dict[int, List[float]] = {}
    for r in (deribit_rows_12h or []):
        ts = r.get("ts")
        if not isinstance(ts, (int, float)):
            continue
        ts = int(ts)
        v = _extract_iv_slope(r)
        if v is None:
            continue
        b = _bucket_hour(ts)
        vol_buckets.setdefault(b, []).append(abs(float(v)))

    # pick last buckets present in any of the series
    all_buckets = sorted(set(fut_buckets.keys()) | set(opt_buckets.keys()) | set(vol_buckets.keys()))[-max_points:]

    fut_avg_series: List[float] = []
    opt_abs_series: List[float] = []
    vol_abs_series: List[float] = []

    MIN_COVERAGE = max(3, int(len(supported_norm) * 0.65))

    for b in all_buckets:
        # futures avg risk for this hour (only if enough symbols)
        sym_map = fut_buckets.get(b, {})
        if sym_map:
            vals = [sym_map[s][1] for s in supported_norm if s in sym_map]
            if len(vals) >= MIN_COVERAGE:
                fut_avg_series.append(sum(vals) / len(vals))

        # options abs avg slope
        xs = opt_buckets.get(b)
        if xs:
            opt_abs_series.append(sum(xs) / len(xs))

        # vol abs avg slope
        ys = vol_buckets.get(b)
        if ys:
            vol_abs_series.append(sum(ys) / len(ys))

    # convert to hourly impulses where possible
    def to_impulses(series: List[float]) -> List[float]:
        if len(series) < 2:
            return []
        return [abs(series[i] - series[i - 1]) for i in range(1, len(series))]

    return {
        "fut_impulses": to_impulses(fut_avg_series),
        "opt_impulses": opt_abs_series,  # already "activity", no delta needed
        "vol_impulses": vol_abs_series,  # already "activity", no delta needed
    }

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
    supported_norm = [_normalize_symbol(s) for s in supported_tickers]
    supported_norm = [s for s in supported_norm if s]

    latest_risk = _latest_per_symbol(risk_rows_30m or [], _extract_risk_value)
    # keep only supported tickers
    risk_vals_now = [latest_risk.get(sym) for sym in supported_norm]
    risk_vals_now = [v for v in risk_vals_now if _is_num(v)]

    dispersion_xs_now = _stdev(risk_vals_now)

    # adaptive normalization for dispersion_xs using 12h hourly samples (max 12 points)
    disp_samples = _hourly_dispersion_from_risk_rows(risk_rows_12h or [], supported_tickers, max_points=12)
    disp_lo = min(disp_samples) if disp_samples else None
    disp_hi = max(disp_samples) if disp_samples else None

    disp_norm = _norm01(dispersion_xs_now, disp_lo, disp_hi)
    if disp_norm is None and _is_num(dispersion_xs_now):
        # If we have a live value but no valid range, keep a neutral stance.
        disp_norm = 0.5
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
    if iv_norm is None and _is_num(iv_slope_abs_now):
        iv_norm = 0.5
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
    if mci_norm is None and _is_num(mci_now):
        mci_norm = 0.5
    mci_comp = None if mci_norm is None else (1.0 - mci_norm)

    # dispersion component uses same dispersion_xs normalization (low dispersion => more compression)
    disp_comp = None if disp_norm is None else (1.0 - disp_norm)

    compression_components = [
        (vbi_comp, 0.4),
        (mci_comp, 0.4),
        (disp_comp, 0.2),
    ]
    available_components = [(value, weight) for value, weight in compression_components if value is not None]

    if not available_components:
        compression_score = None
        compression_label = "N/A"
    else:
        weight_sum = sum(weight for _, weight in available_components)
        weighted_score = sum(value * weight for value, weight in available_components) / weight_sum
        compression_score = 100.0 * weighted_score
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
        sym = _normalize_symbol(_extract_symbol(r))
        if sym not in supported_norm:
            continue
        ts = _ts_to_ms(r.get("ts"))
        if ts is None:
            continue
        v = _extract_risk_value(r)
        if v is None:
            continue
        prev = earliest.get(sym)
        if prev is None or ts < prev[0]:
            earliest[sym] = (ts, float(v))

    start_vals = [earliest.get(s, (None, None))[1] for s in supported_norm]
    end_vals = [latest_risk_1h.get(s) for s in supported_norm]
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

        # --- Cross-layer driver (LIVE) with 12h adaptive normalization ---
    # Current impulses (1h)
    # futures impulse: change in market avg risk over 1h
    fut_impulse_abs = _safe_abs(fut_impulse)

    # options impulse: abs(avg mci_slope over 1h)
    opt_impulse_abs = _safe_abs(opt_impulse)

    # vol impulse: abs(delta iv_slope over 1h)
    vol_impulse_abs = _safe_abs(vol_impulse)

    # Build 12h reference ranges (<= 12 points), no heavy scans:
    supported_norm = [_normalize_symbol(s) for s in supported_tickers]
    ref = _hourly_impulses_12h(
        risk_rows_12h or [],
        bybit_rows_12h or [],
        deribit_rows_12h or [],
        supported_norm=supported_norm,
        max_points=12,
    )

    def lohi(xs: List[float]) -> Tuple[Optional[float], Optional[float]]:
        xs = [float(x) for x in xs if _is_num(x)]
        if not xs:
            return None, None
        return min(xs), max(xs)

    fut_lo, fut_hi = lohi(ref.get("fut_impulses", []))
    opt_lo, opt_hi = lohi(ref.get("opt_impulses", []))
    vol_lo, vol_hi = lohi(ref.get("vol_impulses", []))

    fut_norm = _norm01(fut_impulse_abs, fut_lo, fut_hi)
    opt_norm = _norm01(opt_impulse_abs, opt_lo, opt_hi)
    vol_norm = _norm01(vol_impulse_abs, vol_lo, vol_hi)

    norm_impulses = {
        "FUTURES": fut_norm,
        "OPTIONS": opt_norm,
        "VOL": vol_norm,
    }

    sorted_imp = sorted(
        [(k, v) for k, v in norm_impulses.items() if v is not None],
        key=lambda kv: kv[1],
        reverse=True,
    )

    if not sorted_imp:
        driver = "NONE"
        driver_conf = 0.0
    else:
        top_k, top_v = sorted_imp[0]

        # ✅ normalized space noise floor: treat < 0.20 as noise (tunable)
        noise_floor = 0.20
        if top_v < noise_floor:
            driver = "NONE"
            driver_conf = 0.0
        elif len(sorted_imp) >= 2:
            second_v = sorted_imp[1][1]
            # ✅ ALIGNED if within 15% of top in normalized space
            if second_v > 0 and (top_v - second_v) / top_v <= 0.15:
                driver = "ALIGNED"
                driver_conf = 0.65
            else:
                driver = f"{top_k}_LEAD"
                # confidence grows with separation
                sep = (top_v - second_v) if second_v is not None else top_v
                driver_conf = 0.60 + 0.30 * max(0.0, min(1.0, sep))
        else:
            driver = f"{top_k}_LEAD"
            driver_conf = 0.60 + 0.30 * max(0.0, min(1.0, top_v))


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



