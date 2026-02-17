from data.client import SupabaseClient

client = SupabaseClient()

def load_risk(ts_from, ts_to):
    return client.fetch("risk_eval", ts_from, ts_to)

def load_options(ts_from, ts_to):
    return client.fetch("options_ticker_cycle", ts_from, ts_to)

def load_deribit(ts_from, ts_to):
    return client.fetch("deribit_vbi_snapshot", ts_from, ts_to)

def load_meta(ts_from, ts_to):
    return client.fetch("market_regime", ts_from, ts_to)
