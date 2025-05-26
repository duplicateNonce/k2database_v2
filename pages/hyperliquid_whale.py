# 文件路径：pages/hyperliquid_whale.py

import os
import requests
import psycopg2
import pandas as pd
import streamlit as st
from datetime import datetime, timedelta
from streamlit_autorefresh import st_autorefresh
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    """
    创建并缓存数据库连接，使用 st.cache_resource
    """
    DB_CONFIG = {
        'host': os.getenv('DB_HOST', '127.0.0.1'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('INSTR_DB', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }

    @st.cache_resource
    def _connect():
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        return conn

    return _connect()


def fetch_hyperliquid_data(api_key: str) -> list:
    """
    从 Coinglass Hyperliquid Whale API 拉取实时鲸鱼数据
    """
    url = "https://open-api-v4.coinglass.com/api/hyperliquid/whale-alert"
    headers = {
        'CG-API-KEY': api_key,
        'accept': 'application/json'
    }
    resp = requests.get(url, headers=headers)
    if resp.status_code != 200:
        st.error(f"API 请求失败：{resp.status_code}")
        return []
    data = resp.json().get("data", [])
    # 兼容不同字段名称
    for r in data:
        r['liq_price'] = r.get('liq_price') if 'liq_price' in r else r.get('liquidation_price')
    return data


def update_database(conn, records: list):
    """
    将 API 返回数据写入或更新到 hl_realtime 表
    """
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS hl_realtime (
            user_address TEXT PRIMARY KEY,
            symbol TEXT,
            position_size DOUBLE PRECISION,
            entry_price DOUBLE PRECISION,
            liq_price DOUBLE PRECISION,
            position_value_usd DOUBLE PRECISION,
            position_action INTEGER,
            api_create_time TIMESTAMP,
            remark TEXT,
            last_update TIMESTAMP
        )
    """)
    now = datetime.now()
    for r in records:
        # 安全获取字段
        user = r.get('user') or r.get('user_address')
        symbol = r.get('symbol')
        position_size = r.get('position_size')
        entry_price = r.get('entry_price')
        liq_price = r.get('liq_price')
        if liq_price is None:
            continue
        position_value = r.get('position_value_usd')
        action = r.get('position_action')
        api_ts = datetime.fromtimestamp(r.get('create_time', 0) / 1000)

        cur.execute("""
            INSERT INTO hl_realtime
            (user_address, symbol, position_size, entry_price, liq_price,
             position_value_usd, position_action, api_create_time, last_update)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (user_address) DO UPDATE SET
              symbol = EXCLUDED.symbol,
              position_size = EXCLUDED.position_size,
              entry_price = EXCLUDED.entry_price,
              liq_price = EXCLUDED.liq_price,
              position_value_usd = EXCLUDED.position_value_usd,
              position_action = EXCLUDED.position_action,
              api_create_time = EXCLUDED.api_create_time,
              last_update = CASE
                WHEN hl_realtime.api_create_time < EXCLUDED.api_create_time
                THEN EXCLUDED.last_update ELSE hl_realtime.last_update END
        """, (
            user, symbol, position_size, entry_price,
            liq_price, position_value, action, api_ts, now
        ))
    conn.commit()


def load_recent_data(conn, hours: int = 4) -> pd.DataFrame:
    """
    从数据库加载最近 hours 小时内的记录
    """
    cutoff = datetime.now() - timedelta(hours=hours)
    df = pd.read_sql(
        "SELECT * FROM hl_realtime WHERE api_create_time >= %s ORDER BY api_create_time DESC", 
        conn, params=(cutoff,)
    )
    return df


def format_action(row) -> str:
    """
    格式化动作列：杠杆+做多/做空+开仓/平仓
    """
    side = "做多" if row['position_size'] > 0 else "做空"
    act = "开仓" if row['position_action'] == 1 else "平仓"
    # 防止除零
    liq = row['liq_price'] if row['liq_price'] else 1
    ratio = row['entry_price'] / liq
    return f"{ratio:.1f}x {side}{act}"


def render_table(df: pd.DataFrame):
    """
    按要求渲染表格：
    - 地址/备注
    - 时间：x分钟前
    - 币种
    - 动作
    - 价格
    - 仓位价值(USD) 千分位格式
    """
    now = datetime.now()
    # 限制最多 4 小时
    df = df[df['api_create_time'] >= now - timedelta(hours=4)]

    # 计算显示字段
    df['地址/备注'] = df['remark'].fillna(df['user_address'])
    df['时间'] = df['api_create_time'].apply(
        lambda t: f"{int((now - t).total_seconds()/60)} 分钟前"
    )
    df['动作'] = df.apply(format_action, axis=1)
    df['价格'] = df['entry_price']
    df['仓位价值(USD)'] = df['position_value_usd'].map(lambda v: f"{v:,.2f}")

    display_cols = ['地址/备注', '时间', 'symbol', '动作', '价格', '仓位价值(USD)']
    df = df[display_cols]
    df = df.rename(columns={'symbol': '币种'})

    st.table(df)


def render_hyperliquid_whale_page():
    st.title("Hyperliquid 鲸鱼监控")

    conn = get_conn()
    cur = conn.cursor()

    # 备注输入
    with st.form("remark_form"):
        addr = st.text_input("钱包地址（User Address）")
        note = st.text_input("备注")
        if st.form_submit_button("提交备注"):
            cur.execute(
                "UPDATE hl_realtime SET remark=%s, last_update=NOW() WHERE user_address=%s", (note, addr)
            )
            conn.commit()
            st.success("备注已保存")

    # 自动刷新
    st_autorefresh(interval=5_000, key="hyperliquid_refresh")

    # 拉取并存储数据
    api_key = os.getenv('CG_API_KEY')
    if not api_key:
        st.error('未配置 CG_API_KEY')
        return
    records = fetch_hyperliquid_data(api_key)
    update_database(conn, records)

    # 渲染表格
    df_recent = load_recent_data(conn, hours=4)
    render_table(df_recent)
