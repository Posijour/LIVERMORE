from collections import Counter

def avg(values, digits: int):
    numeric = [v for v in values if isinstance(v, (int, float))]
    if not numeric:
        return 0
    return round(sum(numeric) / len(numeric), digits)

def dominant(values):
    if not values:
        return None, 0.0
    c = Counter(values)
    k, v = c.most_common(1)[0]
    return k, round(v / len(values) * 100, 1)


def aggregate_options(rows: list[dict]) -> dict:
    if not rows:
        return {}

    data = [r["data"] for r in rows]

    regimes = [
        d.get("okx_liquidity_regime")
        for d in data
        if d.get("okx_liquidity_regime")
    ]

    regime, pct = dominant(regimes)

    return {
        "dominant_phase": regime,          # OKX liquidity regime
        "dominant_phase_pct": pct,
    }
