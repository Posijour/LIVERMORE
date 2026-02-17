def print_state_evolution(labels, metrics, changes, conclusion):
    print("\n=== MARKET METRICS ===\n")
    print("Window     Risk   RiskAct   Structure   Volatility")

    for label, m in zip(labels, metrics):
        print(
            f"{label:<10} "
            f"{m['avg_risk']:<6.2f} "
            f"{m['risk_activity']:<8.1f}% "
            f"{m['structure_pct']:<10.1f}% "
            f"{m['vol_state']} ({m['iv_slope']:+.2f})"
        )

    print("\n=== STATE CHANGES (old → new) ===\n")
    for k, v in changes.items():
        print(f"{k:<25}: {v}")

    print("\n=== CONCLUSION ===\n")
    print(conclusion)
