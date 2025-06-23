import streamlit as st
from datetime import datetime, timedelta, time
from db import engine_coin
from queries import fetch_coinmarkets_history, fetch_instruments
from utils import quick_range_buttons

def render_history():
    st.header("历史数据查询")
    instruments = fetch_instruments(engine_coin)["symbol"].tolist()
    syms = st.multiselect("选择交易对", instruments, default=instruments[:3], key="hist_syms")

    start_date = st.date_input("开始日期", datetime.now() - timedelta(days=7), key="hist_start_date")
    start_time = st.time_input("开始时间", time(0, 0), key="hist_start_time")
    end_date   = st.date_input("结束日期", datetime.now(), key="hist_end_date")
    end_time   = st.time_input("结束时间", time(23, 59), key="hist_end_time")

    quick_range_buttons(
        "hist_start_date",
        "hist_start_time",
        "hist_end_date",
        "hist_end_time",
    )

    if st.button("查询", key="hist_btn"):
        # combine date + time → datetime
        start_dt = datetime.combine(start_date, start_time)
        end_dt   = datetime.combine(end_date, end_time)

        df = fetch_coinmarkets_history(
            engine_coin,
            syms,
            start_dt,
            end_dt,
        )
        if not df.empty:
            chart = df.pivot(index="ts", columns="symbol", values="current_price")
            st.line_chart(chart)
        else:
            st.warning("无数据")
