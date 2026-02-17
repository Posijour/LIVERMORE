def interpret(snapshot) -> str:
    r = snapshot.risk
    o = snapshot.options
    v = snapshot.deribit

    if not r or not o or not v:
        return "Insufficient data."

    if (
    r.get("avg_risk", 0) < 1
    and o.get("dominant_phase") == "OVERCOMPRESSED"
    and v.get("vbi_state") == "HOT"
):
    return (
        "Latent tension: structural compression with elevated "
        "volatility expectations, but no active crowd stress yet."
    )

    if (
        r.get("avg_risk", 0) >= 2
        and o.get("dominant_phase") == "OVERCOMPRESSED"
        and v.get("vbi_state") == "HOT"
    ):
        return "Pre-break tension: pressure building under compression."

    if (
        r.get("avg_risk", 0) < 1
        and o.get("dominant_phase") == "RELEASING"
        and v.get("vbi_pattern") == "POST_EVENT"
    ):
        return "Post-move decay: stress released."

    return "Mixed market conditions."
