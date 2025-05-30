import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text

from db import engine_ohlcv
from config import TZ_NAME


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS monitor_levels (
        symbol TEXT PRIMARY KEY,
        start_ts BIGINT NOT NULL,
        end_ts BIGINT NOT NULL,
        p1 NUMERIC NOT NULL,
        alerted BOOLEAN NOT NULL DEFAULT FALSE
    );
    """
    with engine_ohlcv.begin() as conn:
        conn.execute(text(sql))


def compute_p1(start_ts: int, end_ts: int) -> pd.DataFrame:
    with engine_ohlcv.begin() as conn:
        symbols = [
            row[0] for row in conn.execute(text("SELECT instrument_id FROM instruments"))
        ]
        records = []
        for sym in symbols:
            row = conn.execute(
                text(
                    "SELECT close, time FROM ohlcv "
                    "WHERE symbol=:s AND time BETWEEN :a AND :b "
                    "ORDER BY close DESC LIMIT 1"
                ),
                {"s": sym, "a": start_ts, "b": end_ts},
            ).fetchone()
            if not row:
                continue
            p1 = float(row.close)
            conn.execute(
                text(
                    """
                    INSERT INTO monitor_levels(symbol,start_ts,end_ts,p1,alerted)
                    VALUES(:sym,:start,:end,:p1,false)
                    ON CONFLICT(symbol) DO UPDATE
                      SET start_ts=excluded.start_ts,
                          end_ts=excluded.end_ts,
                          p1=excluded.p1,
                          alerted=false
                    """
                ),
                {"sym": sym, "start": start_ts, "end": end_ts, "p1": p1},
            )
            records.append({"symbol": sym, "p1": p1, "time": row.time})
    df = pd.DataFrame(records)
    if not df.empty:
        df["time"] = (
            pd.to_datetime(df["time"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
    return df


def render_monitor():
    ensure_table()
    st.header("Monitor")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("开始日期", date.today() - timedelta(days=7))
        start_time = st.time_input("开始时间", time(0, 0))
    with col2:
        end_date = st.date_input("结束日期", date.today())
        end_time = st.time_input("结束时间", time(23, 59))

    if st.button("计算并保存 P1"):
        tz = dt_timezone(timedelta(hours=8))
        start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
        end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        df = compute_p1(start_ts, end_ts)
        if df.empty:
            st.warning("区间内无数据")
        else:
            st.dataframe(df)

    st.subheader("已保存的 P1")
    df_exist = pd.read_sql(
        "SELECT symbol, p1, start_ts, end_ts, alerted FROM monitor_levels", engine_ohlcv
    )
    if not df_exist.empty:
        df_exist["start_ts"] = (
            pd.to_datetime(df_exist["start_ts"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        df_exist["end_ts"] = (
            pd.to_datetime(df_exist["end_ts"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
    st.dataframe(df_exist)

