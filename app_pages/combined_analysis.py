import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from strategies.bottom_lift import analyze_bottom_lift
from app_pages.price_change_by_label import get_mappings, compute_period_metrics as label_compute
from utils import update_shared_range

def render_combined_page():
    st.title("综合分析")

    # 日期时间输入，只需填写一次
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期", st.session_state.get("range_start_date", date.today() - timedelta(days=7)), key="combo_start_date"
        )
        start_time = st.time_input(
            "开始时间", st.session_state.get("range_start_time", time(0, 0)), key="combo_start_time"
        )
    with col2:
        end_date = st.date_input(
            "结束日期", st.session_state.get("range_end_date", date.today()), key="combo_end_date"
        )
        end_time = st.time_input(
            "结束时间", st.session_state.get("range_end_time", time(23, 59)), key="combo_end_time"
        )

    tz = dt_timezone(timedelta(hours=8))
    start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
    end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    update_shared_range(start_date, start_time, end_date, end_time)

    st.subheader("强势标的筛选")
    if st.button("计算强势标的", key="combo_sa_btn"):
        with engine_ohlcv.connect() as conn:
            symbols = [row[0] for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]
            result = conn.execute(text("SELECT instrument_id, labels FROM instruments"))
            labels_map = {instr_id: labels for instr_id, labels in result}

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
        else:
            df = pd.DataFrame(records)
            df['max_close_dt'] = pd.to_datetime(df['max_close_dt'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%m-%d %H:%M')
            df['min_close_dt'] = pd.to_datetime(df['min_close_dt'], unit='ms', utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%m-%d %H:%M')
            df['period_return (%)'] = (df['period_return'] * 100).round(2)
            df['drawdown (%)'] = (df['drawdown'] * 100).round(2)
            df['标签'] = df['symbol'].map(lambda s: '，'.join(labels_map.get(s, [])) if labels_map.get(s) else '')
            df = df[[
                '标签','symbol','first_close','last_close','period_return (%)',
                'max_close','max_close_dt','min_close','min_close_dt','drawdown (%)'
            ]].sort_values('period_return (%)', ascending=False).reset_index(drop=True)
            df = df.rename(columns={
                'symbol': '代币名字',
                'first_close': '时间1',
                'last_close': '时间2',
                'period_return (%)': '期间收益',
                'max_close': '期间最高价',
                'max_close_dt': '最高价时间',
                'min_close': '期间最低价',
                'min_close_dt': '最低价时间',
                'drawdown (%)': '最大回撤'
            })
            st.dataframe(df, use_container_width=True)

    st.subheader("底部抬升筛选")
    bars = st.number_input("± N 根 K 线 (bars)", min_value=1, value=4, step=1, key="combo_bars")
    factor = st.number_input("放大因子 (factor)", min_value=1.0, value=100.0, step=1.0, key="combo_factor")
    if st.button("运行底部抬升分析", key="combo_bl_btn"):
        df = analyze_bottom_lift(start_dt, end_dt, bars=bars, factor=factor)
        if df.empty:
            st.warning("无符合条件的数据")
        else:
            with engine_ohlcv.connect() as conn:
                result = conn.execute(text("SELECT instrument_id, labels FROM instruments"))
                labels_map = {instr_id: labels for instr_id, labels in result}
            df['标签'] = df.index.map(lambda s: '，'.join(labels_map.get(s, [])) if labels_map.get(s) else '')
            df['L1_time'] = pd.to_datetime(df['L1_time'], utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%m-%d %H:%M')
            df['L2_time'] = pd.to_datetime(df['L2_time'], utc=True).dt.tz_convert('Asia/Shanghai').dt.strftime('%m-%d %H:%M')
            df = df.reset_index()
            df = df[['标签','symbol','L1_time','L1_low','L2_time','L2_low','slope']]
            df = df.rename(columns={'symbol': '代币名字'})
            df = df.sort_values('slope', ascending=False).reset_index(drop=True)
            st.dataframe(df, use_container_width=True)

    st.subheader("标签化涨跌幅")
    if st.button("计算标签涨跌幅", key="combo_label_btn"):
        df_map = get_mappings()
        symbols = df_map['symbol'].unique()
        recs = []
        for sym in symbols:
            ret, _ = label_compute(sym, start_ts, end_ts)
            if ret is None:
                continue
            lbls = df_map.loc[df_map['symbol']==sym, 'label']
            for lbl in lbls:
                recs.append({'symbol': sym, 'label': lbl, 'return': ret})
        df = pd.DataFrame(recs)
        if df.empty:
            st.warning("无有效数据")
        else:
            rmin, rmax = df['return'].min(), df['return'].max()
            low = (rmin*100//5)*5/100
            high = (rmax*100 + 4)//5*5/100
            bins = np.arange(low, high+0.0001, 0.05)
            bucket_labels = [f"{int(l*100)}%~{int(u*100)}%" for l,u in zip(bins[:-1],bins[1:])]
            df['bucket'] = pd.cut(df['return'], bins=bins, labels=bucket_labels, include_lowest=True)
            grp = df.groupby(['label','bucket'])['symbol'].nunique().reset_index(name='count')
            pivot = grp.pivot(index='label', columns='bucket', values='count').fillna(0).astype(int)
            pivot = pivot.loc[:, pivot.sum(axis=0) > 0]
            styled = pivot.style.format(lambda v: "" if v==0 else v)
            st.dataframe(styled, use_container_width=True)
            for bucket in pivot.columns[::-1]:
                df_b = df[df['bucket']==bucket]
                if df_b.empty:
                    continue
                with st.expander(f"{bucket} （共 {len(df_b)} 条）"):
                    df_show = df_b[['symbol','label','return']].copy()
                    df_show['return'] = df_show['return'].map("{:.2%}".format)
                    st.dataframe(df_show.sort_values(['return','symbol'], ascending=[False,True]), use_container_width=True)

