from collections import Counter

def dominant(values):
    if not values:
        return None
    return Counter(values).most_common(1)[0][0]


def aggregate_deribit(rows: list[dict]) -> dict:
    if not rows:
        return {}

    data = [r["data"] for r in rows]

    return {
        "vbi_state": dominant([d.get("vbi_state") for d in data]),
        "vbi_pattern": dominant([d.get("vbi_pattern") for d in data]),
        "iv_slope_avg": round(
            sum(d.get("iv_slope", 0) for d in data) / len(data), 3
        ),
    }
