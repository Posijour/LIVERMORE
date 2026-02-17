import argparse

from aggregation.deribit import aggregate_deribit
from aggregation.meta import aggregate_meta
from aggregation.options import aggregate_options
from aggregation.risk import aggregate_risk
from config import DEFAULT_WINDOW_HOURS
from data.queries import load_deribit, load_meta, load_options, load_risk
from interpretation.engine import interpret
from interpretation.states import detect_states
from models.snapshot import MarketSnapshot
from time_utils import parse_datetime, parse_window
from trend.analyzer import analyze_direction


def run_snapshot(ts_from, ts_to):
    snapshot = MarketSnapshot(
        ts_from=ts_from,
        ts_to=ts_to,
        risk=aggregate_risk(load_risk(ts_from, ts_to)),
        options=aggregate_options(load_options(ts_from, ts_to)),
        deribit=aggregate_deribit(load_deribit(ts_from, ts_to)),
        meta=aggregate_meta(load_meta(ts_from, ts_to)),
    )

    snapshot.interpretation = interpret(snapshot)
    snapshot.active_states = detect_states(snapshot)

    return snapshot

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

    snap_12h = run_snapshot(*parse_window("12h"))
    snap_6h = run_snapshot(*parse_window("6h"))
    snap_1h = run_snapshot(*parse_window("1h"))

    print("\n=== SNAPSHOTS ===")
    print("12h:", snap_12h.interpretation)
    print("6h :", snap_6h.interpretation)
    print("1h :", snap_1h.interpretation)

    trend = analyze_direction([snap_12h, snap_6h, snap_1h])

    print("\n=== MARKET DIRECTION ===")
    print(trend["summary"])
    print("Details:", trend["direction"])

