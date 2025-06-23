import streamlit as st
from datetime import datetime, timedelta, time
from db import engine_ohlcv
from queries import fetch_ohlcv, fetch_instruments
import pandas as pd
from utils import quick_range_buttons

def render_ohlcv_page():
    st.header("OHLCV 数据展示")
    instruments = fetch_instruments(engine_ohlcv)["symbol"].tolist()
    symbol = st.selectbox("选择交易对", instruments, key="ohlcv_sym")

    start_date = st.date_input(
        "开始日期", datetime.now() - timedelta(days=7), key="ohlcv_start_date"
    )
    start_time = st.time_input("开始时间", time(0, 0), key="ohlcv_start_time")
    end_date = st.date_input("结束日期", datetime.now(), key="ohlcv_end_date")
    end_time = st.time_input("结束时间", time(23, 59), key="ohlcv_end_time")

    quick_range_buttons(
        "ohlcv_start_date",
        "ohlcv_start_time",
        "ohlcv_end_date",
        "ohlcv_end_time",
    )

    if st.button("加载", key="ohlcv_btn"):
        # 用户输入的日期时间转换为毫秒级时间戳
        start_dt = datetime.combine(start_date, start_time)
        end_dt = datetime.combine(end_date, end_time)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        df = fetch_ohlcv(engine_ohlcv, symbol, start_ts, end_ts)
        if not df.empty:
            # 1. 将毫秒时间戳转换为 UTC+0 datetime 再转到 UTC+8
            df['datetime'] = (
                pd.to_datetime(df['time'], unit='ms', utc=True)
                .dt.tz_convert('Asia/Shanghai')
            )
            # 2. 用于表格展示——格式化为字符串
            df_display = df.copy()
            df_display['time'] = df_display['datetime'].dt.strftime('%Y-%m-%d %H:%M')
            st.dataframe(
                df_display[['time', 'open', 'high', 'low', 'close', 'volume_usd']]
            )

            # 3. 用于绘图——以 datetime 作为索引
            df_chart = df.set_index('datetime')
            st.line_chart(df_chart[['open', 'high', 'low', 'close']])
        else:
            st.warning("无数据")
