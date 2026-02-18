def print_state_evolution(labels, metrics, changes, conclusion):
    print("\n=== MARKET METRICS ===\n")
    print("Window     Risk   RiskAct   Structure   Volatility        DivCnt  DivShare  DivType")

    for label, m in zip(labels, metrics):
        div_type = m.get("dominant_divergence") or "-"
        print(
            f"{label:<10} "
            f"{m['avg_risk']:<6.2f} "
            f"{m['risk_activity']:<8.1f}% "
            f"{m['structure_pct']:<10.1f}% "
            f"{m['vol_state']} ({m['iv_slope']:+.2f})".ljust(18) + " "
            f"{m.get('divergence_count', 0):<7} "
            f"{m.get('divergence_share', 0):<8.1f}% "
            f"{div_type}"
        )

    print("\n=== STATE CHANGES (old → new) ===\n")
    for k, v in changes.items():
        print(f"{k:<25}: {v}")

    print("\n=== CONCLUSION ===\n")
    print(conclusion)
