import pandas as pd
from sqlalchemy import text
from config import TZ_NAME

# CoinMarkets 表接口

def fetch_latest_snapshot(engine):
    return pd.read_sql("SELECT * FROM coinmarkets ORDER BY ts DESC LIMIT 1", engine)


def fetch_snapshot_at(engine, at_dt):
    return pd.read_sql(
        text("SELECT * FROM coinmarkets WHERE ts BETWEEN :a AND :b ORDER BY ts"),
        engine,
        params={"a": at_dt, "b": at_dt},
    )


def fetch_coinmarkets_history(engine, symbols, start_dt, end_dt):
    if not symbols:
        return pd.DataFrame()
    ph = ",".join(f":s{i}" for i in range(len(symbols)))
    params = {f"s{i}": s for i, s in enumerate(symbols)}
    params.update({"a": start_dt, "b": end_dt})
    sql = text(f"""
        SELECT
          ts AT TIME ZONE 'UTC' AT TIME ZONE '{TZ_NAME}' AS ts,
          symbol, current_price
        FROM coinmarkets
        WHERE symbol IN ({ph}) AND ts BETWEEN :a AND :b
        ORDER BY ts
    """)
    return pd.read_sql(sql, engine, params=params)


def fetch_instruments(engine):
    return pd.read_sql(
        "SELECT instrument_id AS symbol FROM instruments ORDER BY instrument_id",
        engine
    )

# OHLCV 表接口

def fetch_ohlcv(engine, symbol, start_ts, end_ts):
    return pd.read_sql(
        text(
            "SELECT * FROM ohlcv_1h WHERE symbol=:symbol AND time BETWEEN :start AND :end ORDER BY time"
        ),
        engine,
        params={"symbol": symbol, "start": start_ts, "end": end_ts},
    )


def fetch_distinct_ohlcv_symbols(engine) -> list[str]:
    df = pd.read_sql("SELECT DISTINCT symbol FROM ohlcv_1h", engine)
    return df["symbol"].tolist()
