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

    phases = [d.get("mci_phase") for d in data if d.get("mci_phase")]

    phase, pct = dominant(phases)

    return {
        "dominant_phase": phase,
        "dominant_phase_pct": pct,
        "mci_avg": avg((d.get("mci") for d in data), 2),
        "mci_slope": avg((d.get("mci_slope") for d in data), 3),
        "phase_divergence": any(d.get("phase_divergence") for d in data),
    }
