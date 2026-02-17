from collections import Counter

def aggregate_meta(rows: list[dict]) -> dict:
    if not rows:
        return {}

    regs = [r["data"].get("regime") for r in rows if r["data"].get("regime")]
    act = [r["data"].get("activity") for r in rows if r["data"].get("activity")]

    return {
        "market_regime": Counter(regs).most_common(1)[0][0] if regs else None,
        "activity_regime": Counter(act).most_common(1)[0][0] if act else None,
    }
