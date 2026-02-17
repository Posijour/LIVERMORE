STATE_RULES = [
    {
        "name": "STRUCTURAL_COMPRESSION",
        "check": lambda s: s.options.get("dominant_phase") == "OVERCOMPRESSED",
    },
    {
        "name": "VOL_EXPANSION_EXPECTED",
        "check": lambda s: s.deribit.get("vbi_state") == "HOT",
    },
]


def detect_states(snapshot):
    return [
        rule["name"]
        for rule in STATE_RULES
        if rule["check"](snapshot)
    ]
