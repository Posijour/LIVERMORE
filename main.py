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
    args = parse_args()

    if args.from_dt and args.to_dt:
        ts_from = parse_datetime(args.from_dt)
        ts_to = parse_datetime(args.to_dt)
    else:
        ts_from, ts_to = parse_window(args.window)

    snap = run_snapshot(ts_from, ts_to)

    print("=== MARKET SNAPSHOT ===")
    print(snap)
