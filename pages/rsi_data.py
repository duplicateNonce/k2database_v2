# pages/rsi_data.py

import os
import requests
import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime


def fetch_and_store_rsi():
    # Coinglass 接口请求
    api_key = os.getenv("CG_API_KEY", "14ea99a0b48244d8a3761a7277c51401")
    url = "https://open-api-v4.coinglass.com/api/futures/rsi/list"
    headers = {"CG-API-KEY": api_key, "accept": "application/json"}

    resp = requests.get(url, headers=headers, timeout=10)
    data = resp.json()
    if data.get("code") != "0":
        print(f"Coinglass API 错误：{data.get('msg')}")
        return

    items = data.get("data", [])
    if not items:
        print("返回数据为空。")
        return

    # 建立数据库连接
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=os.getenv("DB_PORT", "5432"),
        database="rsidata",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )
    cur = conn.cursor()
    ts = datetime.utcnow()

    # 插入记录，包含4h RSI和4h涨跌
    insert_sql = """
    INSERT INTO rsi_data (
      symbol,
      rsi_instrument_id,
      rsi_4h,
      price_change_percent_4h,
      ts
    ) VALUES (
      %(symbol)s,
      %(rsi_instrument_id)s,
      %(rsi_4h)s,
      %(price_change_percent_4h)s,
      %(ts)s
    );
    """

    for it in items:
        symbol = it.get("symbol", "")
        cur.execute(insert_sql, {
            "symbol": symbol,
            "rsi_instrument_id": f"{symbol}USDT",
            "rsi_4h": it.get("rsi_4h"),
            "price_change_percent_4h": it.get("price_change_percent_4h"),
            "ts": ts
        })

    conn.commit()
    cur.close()
    conn.close()


def render_rsi_data_page():
    st.title("4H RSI 超卖列表 (RSI < 30)")
    st.write(
        "展示最新一次 4H RSI < 30 的币种列表，按 RSI 升序排，并基于过去6次拉取（包含当前）累计超卖次数以五角星形式标注，同时显示4H涨跌百分比。"
    )

    # 读取数据库数据
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "127.0.0.1"),
        port=os.getenv("DB_PORT", "5432"),
        database="rsidata",
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )
    df = pd.read_sql(
        "SELECT symbol, rsi_4h, price_change_percent_4h, ts FROM rsi_data ORDER BY ts DESC", conn,
        parse_dates=["ts"]
    )
    conn.close()

    if df.empty:
        st.warning("rsidata.rsi_data 表中无数据。")
        return

    # 最近6个时间点（当前+前5次）
    ts_list = df['ts'].drop_duplicates().nlargest(6).tolist()
    latest_ts = ts_list[0]

    # 最新一次超卖数据并排序
    df_latest = df[df['ts'] == latest_ts]
    df_oversold = df_latest[df_latest['rsi_4h'] < 30].sort_values('rsi_4h').copy()

    if df_oversold.empty:
        st.info("当前无超卖 (RSI<30) 币种。")
        return

    # 统计过去6次中超卖出现次数
    df_window = df[df['ts'].isin(ts_list) & (df['rsi_4h'] < 30)]
    counts = df_window.groupby('symbol').size().to_dict()

    # 拼接告警星级和格式化涨跌
    df_oversold['超卖次数'] = df_oversold['symbol'].map(lambda s: counts.get(s, 0))
    df_oversold['告警星级'] = df_oversold['超卖次数'].map(lambda c: '★' * c)
    df_oversold['4H涨跌'] = df_oversold['price_change_percent_4h'].map(
        lambda x: f"{x}%" if pd.notnull(x) else ""
    )

    # 展示
    df_display = df_oversold[['symbol', 'rsi_4h', '4H涨跌', '告警星级']]
    df_display.columns = ['币种', '4H RSI', '4H涨跌', '告警星级']
    st.table(df_display)


# CLI 测试入口
def main():
    fetch_and_store_rsi()

if __name__ == '__main__':
    main()
