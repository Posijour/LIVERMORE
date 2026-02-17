def print_state_evolution(windows, metrics, changes, conclusion):
    print("\n=== MARKET METRICS ===\n")
    print("Window     Risk   RiskAct   Structure   Vol")

    for w, m in windows:
        print(
            f"{w:<10} "
            f"{m['avg_risk']:<5} "
            f"{m['risk_activity']:<8}% "
            f"{m['structure_pct']:<10}% "
            f"{m['vol_state']} ({m['iv_slope']:+.2f})"
        )

    print("\n=== STATE CHANGES ===\n")
    for k, v in changes.items():
        print(f"{k.replace('_',' ').title():<25}: {v.lower()}")

    print("\nConclusion:\n")
    print(conclusion)
