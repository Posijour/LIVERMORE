def extract_metrics(snapshot) -> dict:
    """
    Приводим snapshot к компактному числовому виду,
    пригодному для сравнения во времени.
    """
    r = snapshot.risk or {}
    o = snapshot.options or {}
    v = snapshot.deribit or {}

    return {
        "avg_risk": r.get("avg_risk", 0),
        "risk_activity": r.get("risk_2plus_pct", 0),

        "structure_pct": o.get("dominant_phase_pct", 0),
        "structure_phase": o.get("dominant_phase"),

        "vol_state": v.get("vbi_state"),
        "iv_slope": v.get("iv_slope_avg", 0),
    }
