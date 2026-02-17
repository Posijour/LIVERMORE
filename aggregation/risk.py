def aggregate_risk(rows: list[dict]) -> dict:
    if not rows:
        return {}

    risks = [r["data"].get("risk", 0) for r in rows]
    dirs = [r["data"].get("direction") for r in rows]

    total = len(risks)

    return {
        "avg_risk": round(sum(risks) / total, 2),
        "risk_2plus_pct": round(sum(r >= 2 for r in risks) / total * 100, 1),
        "buildups": sum(r >= 2 for r in risks),
        "alerts": sum(r >= 3 for r in risks),
        "long_bias": dirs.count("LONG"),
        "short_bias": dirs.count("SHORT"),
    }
