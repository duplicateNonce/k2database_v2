# 文件：pages/price_change_by_label.py

import os
import math
import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta, timezone
from sqlalchemy import text
from db import engine_ohlcv
from config import TZ_NAME
import psycopg2
from dotenv import load_dotenv

# —— 1. 读取环境 & 配置 ——
load_dotenv()
INSTR_DB = {
    'host':   os.getenv('DB_HOST','127.0.0.1'),
    'port':   os.getenv('DB_PORT','5432'),
    'dbname': os.getenv('DB_NAME','postgres'),
    'user':   os.getenv('DB_USER','postgres'),
    'password': os.getenv('DB_PASSWORD',''),
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
    try:
        with engine_ohlcv.connect() as conn:
            df = pd.read_sql(
                text(
                    "SELECT time, open, high, low, close, volume_usd FROM ohlcv "
                    "WHERE symbol=:symbol AND time BETWEEN :start AND :end ORDER BY time"
                ),
                conn,
                params={"symbol": symbol, "start": start_ts, "end": end_ts},
            )

        if df.empty:
            return None, None

        df['dt'] = pd.to_datetime(df['time'], unit='ms', utc=True)
        df = df.set_index('dt').sort_index()
        counts = df['open'].resample('4H').count()
        complete = counts[counts == 16].index
        if complete.empty:
            return None, None
        o = df['open'].resample('4H').first().loc[complete]
        c = df['close'].resample('4H').last().loc[complete]
        h = df['high'].resample('4H').max().loc[complete]
        l = df['low'].resample('4H').min().loc[complete]
        v = df['volume_usd'].resample('4H').sum().loc[complete]
        agg = pd.DataFrame({'open': o, 'high': h, 'low': l, 'close': c, 'volume_usd': v})
        first_o = agg['open'].iat[0]
        last_c = agg['close'].iat[-1]
        ret = last_c / first_o - 1 if first_o else None
        peak = agg['high'].max()
        trough = agg['low'].min()
        dd = (peak - trough) / peak if peak else None
        return ret, dd
    except Exception:
        return None, None

def render_price_change_by_label():
    st.title("📊 按涨跌幅区间 分析 Label")

    # —— 2. 时间区间控件 —— 
    now = datetime.now(timezone.utc)
    st.markdown("#### 请选择时间区间")
    c1, c2 = st.columns(2)
    with c1:
        sd = st.date_input("开始日期", now.date() - timedelta(hours=4))
        stime = st.time_input("开始时间", time(now.hour, now.minute))
    with c2:
        ed = st.date_input("结束日期", now.date())
        etime = st.time_input("结束时间", time(now.hour, now.minute))
    start_dt = datetime(sd.year, sd.month, sd.day, stime.hour, stime.minute, tzinfo=timezone.utc)
    end_dt   = datetime(ed.year, ed.month, ed.day, etime.hour, etime.minute, tzinfo=timezone.utc)
    start_custom_ts = int(start_dt.timestamp()*1000)
    end_custom_ts   = int(end_dt.timestamp()*1000)

    preset = st.checkbox("使用 4 小时 窗口", value=True)
    if preset:
        start_ts = int((now - timedelta(hours=4)).timestamp()*1000)
        end_ts   = int(now.timestamp()*1000)
    else:
        start_ts = start_custom_ts
        end_ts   = end_custom_ts

    # —— 3. 读取映射 & 计算 —— 
    df_map = get_mappings()
    symbols = df_map['symbol'].unique()
    recs = []
    for sym in symbols:
        ret, _ = compute_period_metrics(sym, start_ts, end_ts)
        if ret is None: continue
        lbls = df_map.loc[df_map['symbol']==sym, 'label']
        for lbl in lbls:
            recs.append({'symbol':sym, 'label':lbl, 'return':ret})
    df = pd.DataFrame(recs)

    # —— 4. 构建桶区间 & 分配 —— 
    rmin, rmax = df['return'].min(), df['return'].max()
    low = math.floor(rmin*100/5)*5/100
    high= math.ceil(rmax*100/5)*5/100
    bins = np.arange(low, high+0.0001, 0.05)
    bucket_labels = [f"{int(l*100)}%~{int(u*100)}%" for l,u in zip(bins[:-1],bins[1:])]
    df['bucket'] = pd.cut(df['return'], bins=bins, labels=bucket_labels, include_lowest=True)

    # —— 5. 统计透视表 —— 
    grp = df.groupby(['label','bucket'])['symbol'].nunique().reset_index(name='count')
    pivot = grp.pivot(index='label', columns='bucket', values='count').fillna(0).astype(int)

    # 去掉全 0 的列（区间）
    pivot = pivot.loc[:, pivot.sum(axis=0) > 0]
    # 格式化 0 显示为空
    styled = pivot.style.format(lambda v: "" if v==0 else v)

    st.subheader("📈 标签分布概览")
    st.dataframe(styled, use_container_width=True)

    # —— 6. Expander 展开详情 —— 
    for bucket in pivot.columns[::-1]:  # 从涨幅大到小
        df_b = df[df['bucket']==bucket]
        if df_b.empty: continue
        with st.expander(f"{bucket} （共 {len(df_b)} 条）"):
            df_show = df_b[['symbol','label','return']].copy()
            df_show['return'] = df_show['return'].map("{:.2%}".format)
            st.dataframe(df_show.sort_values(['return','symbol'], ascending=[False,True]),
                         use_container_width=True)
