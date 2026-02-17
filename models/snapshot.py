from dataclasses import dataclass

@dataclass
class MarketSnapshot:
    ts_from: int
    ts_to: int

    risk: dict
    options: dict
    deribit: dict
    meta: dict

    interpretation: str = ""
    active_states: list[str] = None
