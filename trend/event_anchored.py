from time_utils import parse_window
from main import run_snapshot


def event_anchored_analysis(event_ts_ms):
    """
    event_ts_ms — timestamp события (дивергенции) в миллисекундах
    """

    # --- windows relative to event ---
    before_from = event_ts_ms - 2 * 60 * 60 * 1000
    before_to = event_ts_ms

    during_from = event_ts_ms - 15 * 60 * 1000
    during_to = event_ts_ms + 15 * 60 * 1000

    after_from = event_ts_ms
    after_to = event_ts_ms + 1 * 60 * 60 * 1000

    snap_before = run_snapshot(before_from, before_to)
    snap_during = run_snapshot(during_from, during_to)
    snap_after = run_snapshot(after_from, after_to)

    return {
        "before": snap_before,
        "during": snap_during,
        "after": snap_after,
    }
