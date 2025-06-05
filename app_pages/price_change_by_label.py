# 文件：pages/price_change_by_label.py

import os
import math
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta, timezone
from sqlalchemy import text
from db import engine_ohlcv
import psycopg2
from dotenv import load_dotenv
from config import secret_get
from utils import safe_rerun, short_time_range
from query_history import add_entry, get_history
from result_cache import load_cached, save_cached

# —— 1. 读取环境 & 配置 ——
load_dotenv()
INSTR_DB = {
    'host':   secret_get('DB_HOST','127.0.0.1'),
    'port':   secret_get('DB_PORT','5432'),
    'dbname': secret_get('DB_NAME','postgres'),
    'user':   secret_get('DB_USER','postgres'),
    'password': secret_get('DB_PASSWORD',''),
}

def get_mappings():
    conn = psycopg2.connect(**INSTR_DB)
    df = pd.read_sql("SELECT instrument_id AS symbol, labels FROM instruments", conn)
    conn.close()
    def normalize(x):
        if isinstance(x, list): return x
        if pd.isna(x): return []
        s = x.strip('{} ')
        return s.split(',') if s else []
    df['labels'] = df['labels'].apply(normalize)
    return df.explode('labels').dropna(subset=['labels']).rename(columns={'labels':'label'})[['symbol','label']]

def compute_period_metrics(symbol, start_ts, end_ts):
    """Return percentage change and drawdown for the given period."""
    try:
        with engine_ohlcv.connect() as conn:
            df = pd.read_sql(
                text(
                    "SELECT time, high, low, close FROM ohlcv "
                    "WHERE symbol=:symbol AND time BETWEEN :start AND :end ORDER BY time"
                ),
                conn,
                params={"symbol": symbol, "start": start_ts, "end": end_ts},
            )

        if df.empty:
            return None, None

        first_close = df["close"].iat[0]
        last_close = df["close"].iat[-1]
        ret = last_close / first_close - 1 if first_close else None

        peak = df["high"].max()
        trough = df["low"].min()
        dd = (peak - trough) / peak if peak else None

        return ret, dd
    except Exception:
        return None, None

def render_price_change_by_label():
    st.title("📊 按涨跌幅区间 分析 Label")

    user = st.session_state.get("username", "default")
    history = get_history("price_change_by_label", user)

    if "pcl_load_params" in st.session_state:
        params = st.session_state.pop("pcl_load_params")
        sd = date.fromisoformat(params["start_date"])
        stime = time.fromisoformat(params["start_time"])
        ed = date.fromisoformat(params["end_date"])
        etime = time.fromisoformat(params["end_time"])
        preset = params.get("preset", True)
    else:
        preset = True
        sd = None
        stime = None
        ed = None
        etime = None

    with st.sidebar.expander("历史记录", expanded=False):
        if history:
            labels = [
                short_time_range(
                    h["params"]["start_date"],
                    h["params"]["start_time"],
                    h["params"]["end_date"],
                    h["params"]["end_time"],
                )
                for h in history
            ]
            idx = st.selectbox(
                "选择记录",
                range(len(history)),
                format_func=lambda i: labels[i],
                key="pcl_hist_select",
            )
            if st.button("载入历史", key="pcl_hist_load"):
                st.session_state["pcl_load_params"] = history[idx]["params"]
                safe_rerun()
        else:
            st.write("暂无历史记录")

    # —— 2. 时间区间控件 —— 
    now = datetime.now(timezone.utc)
    st.markdown("#### 请选择时间区间")
    c1, c2 = st.columns(2)
    with c1:
        sd = st.date_input(
            "开始日期",
            sd or (now.date() - timedelta(hours=4)),
        )
        stime = st.time_input(
            "开始时间",
            stime or time(now.hour, now.minute),
        )
    with c2:
        ed = st.date_input("结束日期", ed or now.date())
        etime = st.time_input(
            "结束时间",
            etime or time(now.hour, now.minute),
        )
    start_dt = datetime(sd.year, sd.month, sd.day, stime.hour, stime.minute, tzinfo=timezone.utc)
    end_dt   = datetime(ed.year, ed.month, ed.day, etime.hour, etime.minute, tzinfo=timezone.utc)
    start_custom_ts = int(start_dt.timestamp()*1000)
    end_custom_ts   = int(end_dt.timestamp()*1000)

    preset = st.checkbox("使用 4 小时 窗口", value=preset)
    if preset:
        start_ts = int((now - timedelta(hours=4)).timestamp()*1000)
        end_ts   = int(now.timestamp()*1000)
    else:
        start_ts = start_custom_ts
        end_ts   = end_custom_ts

    params = {
        "start_date": sd.isoformat(),
        "start_time": stime.isoformat(),
        "end_date": ed.isoformat(),
        "end_time": etime.isoformat(),
        "preset": preset,
    }
    cache_id, df = load_cached("price_change_by_label", params)
    if df is None:
        # —— 3. 读取映射 & 计算 ——
        df_map = get_mappings()
        symbols = df_map['symbol'].unique()
        recs = []
        for sym in symbols:
            ret, _ = compute_period_metrics(sym, start_ts, end_ts)
            if ret is None:
                continue
            lbls = df_map.loc[df_map['symbol'] == sym, 'label']
            for lbl in lbls:
                recs.append({'symbol': sym, 'label': lbl, 'return': ret})
        df = pd.DataFrame(recs)
        if df.empty:
            st.warning("无有效数据")
            return
        # —— 4. 构建桶区间 & 分配 ——
        rmin, rmax = df['return'].min(), df['return'].max()
        low = math.floor(rmin * 100 / 5) * 5 / 100
        high = math.ceil(rmax * 100 / 5) * 5 / 100
        bins = np.arange(low, high + 0.0001, 0.05)
        bucket_labels = [f"{int(l*100)}%~{int(u*100)}%" for l, u in zip(bins[:-1], bins[1:])]
        df['bucket'] = pd.cut(df['return'], bins=bins, labels=bucket_labels, include_lowest=True)
        save_cached("price_change_by_label", params, df)
    add_entry("price_change_by_label", user, params, {"id": cache_id})

    # —— 5. 统计透视表 ——
    grp = df.groupby(['label','bucket'])['symbol'].nunique().reset_index(name='count')
    pivot = grp.pivot(index='label', columns='bucket', values='count').fillna(0).astype(int)

    # 去掉全 0 的列（区间）
    pivot = pivot.loc[:, pivot.sum(axis=0) > 0]
    # 格式化 0 显示为空
    styled = pivot.style.format(lambda v: "" if v==0 else v)

    st.subheader("📈 标签分布概览")
    st.dataframe(styled, use_container_width=True)

    stats = (
        df.groupby("label")["return"]
        .agg(["mean", "median"])
        .reset_index()
        .rename(columns={"label": "标签", "mean": "平均涨幅", "median": "中位数涨幅"})
    )
    stats["平均涨幅"] = stats["平均涨幅"].map("{:.2%}".format)
    stats["中位数涨幅"] = stats["中位数涨幅"].map("{:.2%}".format)

    st.subheader("📈 标签平均涨幅")
    st.dataframe(stats, use_container_width=True)

    # —— 6. Expander 展开详情 —— 
    for bucket in pivot.columns[::-1]:  # 从涨幅大到小
        df_b = df[df['bucket'] == bucket]
        if df_b.empty:
            continue
        with st.expander(f"{bucket} （共 {df_b['symbol'].nunique()} 个标的）"):
            df_show = (
                df_b.groupby('symbol')
                .agg({
                    'label': lambda x: '，'.join(sorted(set(x))),
                    'return': 'first',
                })
                .reset_index()
            )
            df_show['return'] = df_show['return'].map("{:.2%}".format)
            st.dataframe(
                df_show.sort_values(['return', 'symbol'], ascending=[False, True]),
                use_container_width=True,
            )
