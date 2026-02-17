def interpret(snapshot) -> str:
    r = snapshot.risk or {}
    o = snapshot.options or {}
    v = snapshot.deribit or {}
    m = snapshot.meta or {}

    avg_risk = r.get("avg_risk", 0)
    phase = o.get("dominant_phase")
    vbi = v.get("vbi_state")
    pattern = v.get("vbi_pattern")
    regime = m.get("market_regime")

    # 1. TRUE CALM
    if (
        avg_risk < 0.5
        and phase not in ("OVERCOMPRESSED",)
        and vbi in ("COLD", "NEUTRAL", None)
    ):
        return (
            "True calm: low systemic risk, no structural compression, "
            "volatility expectations muted."
        )

    # 2. LATENT TENSION
    if (
        avg_risk < 1
        and phase == "OVERCOMPRESSED"
        and vbi == "HOT"
    ):
        return (
            "Latent tension: structural compression with elevated "
            "volatility expectations, but no active crowd stress yet."
        )

    # 3. EARLY ACTIVATION
    if (
        1 <= avg_risk < 2
        and phase == "OVERCOMPRESSED"
        and vbi in ("HOT",)
    ):
        return (
            "Early activation: crowd risk beginning to surface under "
            "compressed market structure."
        )

    # 4. PRE-BREAK / ACTIVE TENSION
    if (
        avg_risk >= 2
        and phase == "OVERCOMPRESSED"
        and vbi == "HOT"
    ):
        return (
            "Pre-break tension: crowd stress confirmed under structural "
            "compression. Breakout risk elevated."
        )

    # 5. POST-MOVE / DECAY
    if (
        avg_risk < 1
        and phase == "RELEASING"
        and pattern == "POST_EVENT"
    ):
        return (
            "Post-move decay: stress released, volatility compressing, "
            "market normalizing."
        )

    # 6. FALLBACK
    return (
        "Mixed conditions: signals are not aligned across risk, "
        "structure, and volatility layers."
    )
