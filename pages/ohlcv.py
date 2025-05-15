import streamlit as st
from datetime import datetime, timedelta, time
from db import engine_ohlcv
from queries import fetch_ohlcv, fetch_instruments

def render_ohlcv_page():
    st.header("OHLCV 数据展示")
    instruments = fetch_instruments(engine_ohlcv)["symbol"].tolist()
    symbol = st.selectbox("选择交易对", instruments, key="ohlcv_sym")

    start_date = st.date_input("开始日期", datetime.now() - timedelta(days=7), key="ohlcv_start_date")
    start_time = st.time_input("开始时间", time(0, 0), key="ohlcv_start_time")
    end_date   = st.date_input("结束日期", datetime.now(), key="ohlcv_end_date")
    end_time   = st.time_input("结束时间", time(23, 59), key="ohlcv_end_time")

    if st.button("加载", key="ohlcv_btn"):
        # 注意 ohlcv.time 存 bigint ms，所以要 *1000
        start_dt = datetime.combine(start_date, start_time)
        end_dt   = datetime.combine(end_date, end_time)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts   = int(end_dt.timestamp() * 1000)

        df = fetch_ohlcv(engine_ohlcv, symbol, start_ts, end_ts)
        if not df.empty:
            st.dataframe(df)
            st.line_chart(df.set_index("time")[["open", "high", "low", "close"]])
        else:
            st.warning("无数据")
