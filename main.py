from data.queries import *
from aggregation.risk import aggregate_risk
from aggregation.options import aggregate_options
from aggregation.deribit import aggregate_deribit
from aggregation.meta import aggregate_meta
from interpretation.engine import interpret
from interpretation.states import detect_states
from models.snapshot import MarketSnapshot
from time_utils import parse_window


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


if __name__ == "__main__":
    ts_from, ts_to = parse_window("6h")
    snap = run_snapshot(ts_from, ts_to)

    print("=== MARKET SNAPSHOT ===")
    print(snap)
