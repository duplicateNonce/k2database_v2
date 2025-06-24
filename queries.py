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

def fetch_ohlcv(engine, symbol, start_ts, end_ts, table="ohlcv_1h"):
    """Fetch OHLCV records from the specified table.

    Parameters
    ----------
    engine : sqlalchemy.engine.Engine
        Database engine to execute the query against.
    symbol : str
        Trading pair symbol.
    start_ts : int
        Start timestamp in milliseconds.
    end_ts : int
        End timestamp in milliseconds.
    table : str, optional
        Table name to query, defaults to ``"ohlcv_1h"``.
    """

    if table not in {"ohlcv_1h", "ohlcv_4h"}:
        raise ValueError("Invalid OHLCV table name")

    sql = text(
        f"SELECT * FROM {table} WHERE symbol=:symbol AND time BETWEEN :start AND :end ORDER BY time"
    )
    return pd.read_sql(
        sql,
        engine,
        params={"symbol": symbol, "start": start_ts, "end": end_ts},
    )


def fetch_distinct_ohlcv_symbols(engine, table="ohlcv_1h") -> list[str]:
    """Return all distinct symbols from the specified OHLCV table."""

    if table not in {"ohlcv_1h", "ohlcv_4h"}:
        raise ValueError("Invalid OHLCV table name")

    df = pd.read_sql(f"SELECT DISTINCT symbol FROM {table}", engine)
    return df["symbol"].tolist()
