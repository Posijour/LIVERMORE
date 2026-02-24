from data.client import SupabaseClient

client = SupabaseClient()

def load_risk(ts_from, ts_to, symbol=None):
    return client.fetch("risk_eval", ts_from, ts_to, symbol=symbol)

def load_options(ts_from, ts_to, symbol=None):
    return client.fetch_table(
        "daily_options_analysis",
        ts_from,
        ts_to,
        symbol=symbol,
        ts_column="ts_unix_ms",
    )

def load_deribit(ts_from, ts_to, symbol=None):
    return client.fetch("deribit_vbi_snapshot", ts_from, ts_to, symbol=symbol)

def load_meta(ts_from, ts_to, symbol=None):
    return client.fetch("market_regime", ts_from, ts_to, symbol=symbol)

def load_divergence(ts_from, ts_to, symbol=None):
    return client.fetch("risk_divergence", ts_from, ts_to, symbol=symbol)
