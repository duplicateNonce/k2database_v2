from db import engine_ohlcv
from queries import fetch_ohlcv
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy import Table, MetaData


def get_ohlcv(symbol: str, start_ts: int, end_ts: int):
    return fetch_ohlcv(engine_ohlcv, symbol, start_ts, end_ts)


def upsert_ohlcv(df):
    metadata = MetaData(bind=engine_ohlcv)
    ohlcv_table = Table('ohlcv', metadata, autoload_with=engine_ohlcv)
    conn = engine_ohlcv.connect()
    for row in df.to_dict(orient='records'):
        stmt = insert(ohlcv_table).values(**row).on_conflict_do_nothing()
        conn.execute(stmt)
    conn.commit()
    conn.close()
