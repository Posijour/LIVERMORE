def classify_change(delta, eps=0.0):
    if delta > eps:
        return "increasing"
    if delta < -eps:
        return "decreasing"
    return "stable"


def analyze_state_evolution(labeled_snapshots: list[tuple[str, object]]) -> dict:
    """
    labeled_snapshots:
    [
      ("12h", snapshot_12h),
      ("6h", snapshot_6h),
      ("1h", snapshot_1h),
    ]
    Order MUST be old → new
    """

    from trend.metrics import extract_metrics

    windows = []
    metrics = []

    for label, snap in labeled_snapshots:
        m = extract_metrics(snap)
        windows.append(label)
        metrics.append(m)

    old = metrics[0]
    new = metrics[-1]

    changes = {
        "risk level": classify_change(new["avg_risk"] - old["avg_risk"], eps=0.05),
        "risk activity": classify_change(
            new["risk_activity"] - old["risk_activity"], eps=2.0
        ),
        "structural compression": classify_change(
            new["structure_pct"] - old["structure_pct"], eps=2.0
        ),
        "volatility expectations": classify_change(
            new["iv_slope"] - old["iv_slope"], eps=0.5
        ),
    }

    # ---------- CONTEXT CONCLUSION ----------

    conclusion = []

    if (
        changes["risk activity"] == "decreasing"
        and changes["volatility expectations"] == "decreasing"
        and changes["structural compression"] == "decreasing"
    ):
        conclusion.append(
            "Previously observed market tension is dissipating. "
            "Crowd participation faded first, followed by cooling volatility "
            "expectations and gradual easing of structural compression."
        )

    if (
        changes["risk activity"] == "increasing"
        and changes["structural compression"] == "increasing"
        and changes["volatility expectations"] in ("stable", "increasing")
    ):
        conclusion.append(
            "Pressure is building beneath the surface. "
            "Risk participation is increasing under persistent structural compression."
        )

    if (
        changes["risk activity"] == "stable"
        and changes["structural compression"] == "stable"
        and changes["volatility expectations"] == "stable"
    ):
        conclusion.append(
            "Market state remains largely unchanged across observed windows."
        )

    if not conclusion:
        conclusion.append(
            "Market conditions evolved unevenly across layers, "
            "with no single dominant acceleration or decay pattern."
        )

    return {
        "windows": windows,
        "metrics": metrics,
        "changes": changes,
        "conclusion": conclusion[0],
    }
