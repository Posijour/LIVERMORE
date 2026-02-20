from aggregation.risk import aggregate_risk
from aggregation.options import aggregate_options
from aggregation.deribit import aggregate_deribit


RISK_THRESHOLDS = {
    "LOW": 0.05,
    "MEDIUM": 0.15,
}


def risk_dispersion(values):
    diff = max(values) - min(values)
    if diff < RISK_THRESHOLDS["LOW"]:
        return "LOW"
    if diff < RISK_THRESHOLDS["MEDIUM"]:
        return "MEDIUM"
    return "HIGH"


def categorical_dispersion(values):
    unique = set(values)
    if len(unique) == 1:
        return "LOW"
    if len(unique) == 2:
        return "MEDIUM"
    return "HIGH"


def compute_dispersion():
    windows = ["12h", "6h", "1h"]

    risk_vals = {}
    struct_vals = {}
    vol_vals = {}

    for w in windows:
        risk = aggregate_risk(w)
        options = aggregate_options(w)
        deribit = aggregate_deribit(w)

        if risk:
            risk_vals[w] = risk.get("avg_risk")

        if options:
            struct_vals[w] = options.get("dominant_phase")

        if deribit:
            vol_vals[w] = deribit.get("vbi_state")

    def pair_dispersion(vals, a, b, mode="numeric"):
        if a not in vals or b not in vals:
            return "N/A"
        if mode == "numeric":
            return risk_dispersion([vals[a], vals[b]])
        return categorical_dispersion([vals[a], vals[b]])

    return {
        "risk": {
            "12h_1h": pair_dispersion(risk_vals, "12h", "1h", "numeric"),
            "6h_1h": pair_dispersion(risk_vals, "6h", "1h", "numeric"),
        },
        "structure": {
            "12h_1h": pair_dispersion(struct_vals, "12h", "1h", "categorical"),
            "6h_1h": pair_dispersion(struct_vals, "6h", "1h", "categorical"),
        },
        "volatility": {
            "12h_1h": pair_dispersion(vol_vals, "12h", "1h", "categorical"),
            "6h_1h": pair_dispersion(vol_vals, "6h", "1h", "categorical"),
        },
    }