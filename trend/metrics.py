def extract_metrics(snapshot) -> dict:
    r = snapshot.risk or {}
    o = snapshot.options or {}
    v = snapshot.deribit or {}
    d = snapshot.divergence or {}   # ← НОВОЕ

    return {
        # --- core ---
        "avg_risk": r.get("avg_risk", 0),
        "risk_activity": r.get("risk_2plus_pct", 0),
        "structure_pct": o.get("dominant_phase_pct", 0),
        "structure_phase": o.get("dominant_phase"),
        "vol_state": v.get("vbi_state"),
        "iv_slope": v.get("iv_slope_avg", 0),

        # --- divergence context ---
        "divergence_count": d.get("count", 0),
        "divergence_share": d.get("share", 0),
        "dominant_divergence": d.get("dominant_type"),
        "divergence_conf_avg": d.get("confidence_avg"),
    }
