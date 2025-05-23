import streamlit as st
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
import pandas as pd

def render_strong_assets_page():
    st.header("区间收益 & 回调 列表（所有资产）")

    # 时间选择（UTC+8）
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期", date.today() - timedelta(days=7), key="sa_start_date"
        )
        start_time = st.time_input(
            "开始时间", time(0, 0), key="sa_start_time"
        )
    with col2:
        end_date = st.date_input(
            "结束日期", date.today(), key="sa_end_date"
        )
        end_time = st.time_input(
            "结束时间", time(23, 59), key="sa_end_time"
        )

    if st.button("计算区间指标", key="sa_btn"):
        # 合并日期时间并转毫秒
        tz = dt_timezone(timedelta(hours=8))
        start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
        end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        # 拉取所有 symbol
        with engine_ohlcv.connect() as conn:
            symbols = [row[0] for row in conn.execute(text(
                "SELECT DISTINCT symbol FROM ohlcv"
            ))]

        records = []
        for symbol in symbols:
            try:
                m = compute_period_metrics(symbol, start_ts, end_ts)
            except ValueError:
                continue
            m['symbol'] = symbol
            records.append(m)

        if not records:
            st.warning("该区间内无数据")
            return

        # 组织 DataFrame
        df = pd.DataFrame(records)
        df['period_return (%)'] = (df['period_return'] * 100).round(2)
        df['drawdown (%)'] = (df['drawdown'] * 100).round(2)
        df = df[[
            'symbol',
            'first_close',
            'last_close',
            'period_return (%)',
            'max_close',
            'max_close_dt',
            'min_close',
            'min_close_dt',
            'drawdown (%)'
        ]]
        # 按期间收益率降序排序
        df = df.sort_values('period_return (%)', ascending=False).reset_index(drop=True)

        # 展示结果表格（可点击列标题排序）
        st.dataframe(df, use_container_width=True)
