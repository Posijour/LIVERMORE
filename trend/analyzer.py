def sign(x, eps):
    if x > eps:
        return "UP"
    if x < -eps:
        return "DOWN"
    return "FLAT"


def compare_metrics(old: dict, new: dict) -> dict:
    """
    old → new
    """
    return {
        "risk": sign(new["avg_risk"] - old["avg_risk"], eps=0.05),
        "risk_activity": sign(new["risk_activity"] - old["risk_activity"], eps=1.0),
        "structure": sign(new["structure_pct"] - old["structure_pct"], eps=2.0),
        "vol": sign(new["iv_slope"] - old["iv_slope"], eps=0.5),
    }


def analyze_direction(snapshots: list) -> dict:
    """
    snapshots: [oldest, ..., newest]
    """
    if len(snapshots) < 2:
        return {}

    from trend.metrics import extract_metrics

    metrics = [extract_metrics(s) for s in snapshots]

    deltas = []
    for i in range(len(metrics) - 1):
        deltas.append(compare_metrics(metrics[i], metrics[i + 1]))

    # агрегируем направление
    agg = {
        "risk": [],
        "risk_activity": [],
        "structure": [],
        "vol": [],
    }

    for d in deltas:
        for k in agg:
            agg[k].append(d[k])

    def dominant_dir(values):
        if values.count("UP") > values.count("DOWN"):
            return "UP"
        if values.count("DOWN") > values.count("UP"):
            return "DOWN"
        return "FLAT"

    direction = {k: dominant_dir(v) for k, v in agg.items()}

    # смысловая интерпретация
    narrative = []

    if (
        direction["vol"] == "DOWN"
        and direction["structure"] == "DOWN"
        and direction["risk_activity"] == "DOWN"
    ):

        narrative.append(
            "Previous market tension is dissipating across risk, structure, and volatility."
        )

    if (
        direction["risk"] == "UP"
        and direction["structure"] == "UP"
        and direction["vol"] in ("UP", "FLAT")
    ):
        narrative.append(
            "Pressure is building: risk participation increasing under persistent structural compression."
        )

    if (
        direction["structure"] == "FLAT"
        and direction["risk"] == "DOWN"
        and direction["vol"] == "DOWN"
    ):
        narrative.append(
            "Compression persists but participation is fading — dead compression."
        )

    if not narrative:
        narrative.append(
            "Directional signals are mixed; no clear acceleration or decay detected."
        )

    return {
        "direction": direction,
        "summary": narrative[0],
    }
