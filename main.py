
import argparse

from typing import Optional
from aggregation.deribit import aggregate_deribit
from aggregation.meta import aggregate_meta
from aggregation.options import aggregate_options
from aggregation.risk import aggregate_risk
from config import DEFAULT_WINDOW_HOURS
from data.queries import load_deribit, load_meta, load_bybit_market_state, load_okx_market_state, load_risk
from interpretation.engine import interpret
from interpretation.states import detect_states
from models.snapshot import MarketSnapshot
from time_utils import parse_datetime, parse_window
from trend.analyzer import analyze_direction
from data.queries import load_divergence
from persistence.state_history import record_state


def aggregate_divergence(rows, risk_rows):
    if not rows:
        return {}

    return {
        "count": len(rows),
        "share": round(len(rows) / len(risk_rows) * 100, 1) if risk_rows else 0.0,
        "dominant_type": rows[0]["data"].get("divergence_type"),
        "confidence_avg": round(
            sum(r["data"].get("confidence", 0) for r in rows) / len(rows), 2
        ),
    }

def aggregate_alert_divergence(rows):
    alerts = {}

    for r in rows:
        data = r.get("data", {})
        symbol = data.get("symbol")
        div_type = data.get("divergence_type")

        if not symbol or not div_type:
            continue

        key = (symbol, div_type)
        alerts[key] = alerts.get(key, 0) + 1

    return alerts

def run_snapshot(ts_from, ts_to, symbol: Optional[str] = None):
    risk_rows = load_risk(ts_from, ts_to, symbol=symbol)
    div_rows = load_divergence(ts_from, ts_to, symbol=symbol)

    snapshot = MarketSnapshot(
        ts_from=ts_from,
        ts_to=ts_to,
        risk=aggregate_risk(risk_rows),
        options=aggregate_options(load_okx_market_state(ts_from, ts_to)),
        deribit=aggregate_deribit(load_deribit(ts_from, ts_to, symbol=symbol)),
        meta=aggregate_meta(load_meta(ts_from, ts_to, symbol=symbol)),
    )

    snapshot.interpretation = interpret(snapshot)
    snapshot.active_states = detect_states(snapshot)
    snapshot.divergence = aggregate_divergence(div_rows, risk_rows)

    return snapshot


def _risk_band(value: float | int | None, levels: tuple[float, float], labels: tuple[str, str, str]) -> str:
    if value is None:
        return "NO_DATA"

    numeric = float(value)
    low, high = levels
    low_label, mid_label, high_label = labels

    if numeric > high:
        return high_label
    if numeric > low:
        return mid_label
    return low_label

def persist_snapshot_state(snapshot, symbol):
    """
    Persist snapshot states into state_history.

    snapshot — объект Snapshot
    symbol   — None (market) или тикер (BTCUSDT и т.д.)
    """

    from persistence.state_history import record_state

    # -------- RISK --------
    if snapshot.risk:
        avg_risk_band = _risk_band(
            snapshot.risk.get("avg_risk"),
            levels=(0.7, 1.0),
            labels=("LE_0_7", "GT_0_7", "GT_1_0"),
        )
        riskact_band = _risk_band(
            snapshot.risk.get("risk_2plus_pct"),
            levels=(20.0, 30.0),
            labels=("LE_20", "GT_20", "GT_30"),
        )

        record_state(
            layer="risk",
            state_key="avg_risk",
            state_value=avg_risk_band,
            symbol=symbol,
        )
        record_state(
            layer="risk",
            state_key="risk_2plus_pct",
            state_value=riskact_band,
            symbol=symbol,
        )

    # -------- STRUCTURE (OPTIONS / OKX) --------
    if snapshot.options:
        phase = snapshot.options.get("dominant_phase")
        if phase:
            record_state(
                layer="structure",
                state_key="dominant_phase",
                state_value=str(phase),
                symbol=None,  # structure is market-wide
            )

    # -------- VOLATILITY (DERIBIT) --------
    if snapshot.deribit:
        vbi = snapshot.deribit.get("vbi_state")
        if vbi:
            record_state(
                layer="volatility",
                state_key="vbi_state",
                state_value=str(vbi),
                ts=ts,
                symbol=None,  # vol is market-wide
            )

def parse_args():
    parser = argparse.ArgumentParser(description="Build market snapshot from Supabase logs")
    parser.add_argument(
        "--window",
        default=f"{DEFAULT_WINDOW_HOURS}h",
        help="Relative window (e.g. 30m, 1h, 6h, 1d). Ignored if --from and --to are set.",
    )
    parser.add_argument(
        "--from",
        dest="from_dt",
        help="UTC start datetime in format 'YYYY-MM-DD HH:MM'",
    )
    parser.add_argument(
        "--to",
        dest="to_dt",
        help="UTC end datetime in format 'YYYY-MM-DD HH:MM'",
    )
    return parser.parse_args()


if __name__ == "__main__":
    from time_utils import parse_window
    from trend.state_evolution import analyze_state_evolution
    from output.console import print_state_evolution

    snap_12h = run_snapshot(*parse_window("12h"))
    snap_6h = run_snapshot(*parse_window("6h"))
    snap_1h = run_snapshot(*parse_window("1h"))

    analysis = analyze_state_evolution([
        ("12h", snap_12h),
        ("6h", snap_6h),
        ("1h", snap_1h),
    ])

    print_state_evolution(
        analysis["windows"],
        analysis["metrics"],
        analysis["changes"],
        analysis["conclusion"],
    )
