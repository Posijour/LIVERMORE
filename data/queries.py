from data.client import SupabaseClient

client = SupabaseClient()

def load_risk(ts_from, ts_to, symbol=None):
    return client.fetch("risk_eval", ts_from, ts_to, symbol=symbol)

def load_okx_market_state(ts_from, ts_to):
    return client.fetch("okx_market_state", ts_from, ts_to, symbol="MARKET")

def load_bybit_market_state(ts_from, ts_to):
    return client.fetch("bybit_market_state", ts_from, ts_to, symbol=None)

def load_deribit(ts_from, ts_to, symbol=None):
    return client.fetch("deribit_vbi_snapshot", ts_from, ts_to, symbol=symbol)

def load_meta(ts_from, ts_to, symbol=None):
    return client.fetch("market_regime", ts_from, ts_to, symbol=symbol)

def load_divergence(ts_from, ts_to, symbol=None):
    return client.fetch("risk_divergence", ts_from, ts_to, symbol=symbol)

def load_event(event, ts_from, ts_to, symbol=None):
    return client.fetch(event, ts_from, ts_to, symbol=symbol)


def load_latest_log_ts():
    res = (
        client.supabase
        .table("logs")
        .select("ts")
        .order("ts", desc=True)
        .limit(1)
        .execute()
    )

    data = res.data or []
    if not data:
        return None

    return data[0]["ts"]
