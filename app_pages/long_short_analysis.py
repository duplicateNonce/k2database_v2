import os
import streamlit as st
import pandas as pd
import psycopg2
import altair as alt
from datetime import timedelta
from dotenv import load_dotenv


def render_long_short_analysis_page():
    """
    子页面：USD 单位多空持仓、OI/Mcap 分析，支持“只看币安”并先表格后绘图
    """
    load_dotenv()
    # 主数据：coinmarket_aggregated 数据库
    DB_CFG = {
        'host': os.getenv('DB_HOST', '127.0.0.1'),
        'port': os.getenv('DB_PORT', '5432'),
        'dbname': os.getenv('DB_NAME', 'coinmarket_aggregated'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASSWORD', ''),
    }
    # instruments 表所在数据库（默认postgres）
    DB_CFG_INST = DB_CFG.copy()
    DB_CFG_INST['dbname'] = os.getenv('INSTR_DB', 'postgres')

    @st.cache_data(ttl=60)
    def load_data():
        conn = psycopg2.connect(**DB_CFG)
        # 选取所有可用字段
        sql = '''
        SELECT *
        FROM coinmarket_aggregated
        ORDER BY ts
        '''
        df = pd.read_sql(sql, conn, parse_dates=['ts'])
        conn.close()
        return df

    @st.cache_data(ttl=300)
    def load_binance_symbols():
        """
        从 instruments 表读取 Binance 交易对列表，提取基础币种
        """
        conn = psycopg2.connect(**DB_CFG_INST)
        df = pd.read_sql("SELECT instrument_id FROM instruments", conn)
        conn.close()
        ids = df['instrument_id'].tolist()
        # 提取基础符号：去掉 USDT 后缀等
        base_syms = []
        for inst in ids:
            # 去掉常见后缀
            if inst.endswith('USDT'):
                base_syms.append(inst[:-4])
            elif inst.endswith('-PERP'):
                base_syms.append(inst.replace('-PERP', ''))
            else:
                base_syms.append(inst)
        return list(set(base_syms))

    st.title("📊 多空持仓 USD 分析")
    df_all = load_data()
    if df_all.empty:
        st.warning("未读取到持仓数据，请检查数据库配置。")
        return

    # 时间窗口选择
    window = st.selectbox("时间窗口", ['1h','4h','12h','1d','7d'], index=0)
    delta_map = {'1h': timedelta(hours=1), '4h': timedelta(hours=4),
                 '12h': timedelta(hours=12), '1d': timedelta(days=1), '7d': timedelta(days=7)}
    latest_ts = df_all['ts'].max()
    start_ts = latest_ts - delta_map[window]
    df_window = df_all[df_all['ts'] >= start_ts].copy()

    # 只看币安过滤
    show_binance = st.checkbox("只看币安")
    if show_binance:
        binance_syms = load_binance_symbols()
        # 仅保留基础符号在列表中的记录
        df_window = df_window[df_window['symbol'].isin(binance_syms)]

    # 计算指标
    df_window['L/S'] = df_window['long_short_ratio_24h']
    df_window['Long_USD'] = df_window['L/S'] / (1 + df_window['L/S']) * df_window['open_interest_usd']
    df_window['Short_USD'] = df_window['open_interest_usd'] - df_window['Long_USD']
    df_window['OI/Mcap'] = df_window['open_interest_usd'] / df_window['market_cap_usd']
    df_window['price'] = df_window['current_price']
    df_window['Long_Liq24h'] = df_window['long_liquidation_usd_24h']
    df_window['Short_Liq24h'] = df_window['short_liquidation_usd_24h']

    # 表格展示：最新快照
    st.subheader(f"📋 最新快照：{latest_ts:%Y-%m-%d %H:%M:%S}")
    df_latest = df_window[df_window['ts'] == latest_ts].drop_duplicates(subset=['symbol'])
    display_cols = ['symbol','L/S','Long_USD','Short_USD','OI/Mcap','price','Long_Liq24h','Short_Liq24h']
    # 仅保留实际存在的列
    display_cols = [c for c in display_cols if c in df_latest.columns]
    df_disp = df_latest[display_cols].reset_index(drop=True)
    fmt = {'L/S':'{:.4f}','Long_USD':'{:,.0f}','Short_USD':'{:,.0f}',
           'OI/Mcap':'{:.4f}','price':'{:,.2f}','Long_Liq24h':'{:,.0f}','Short_Liq24h':'{:,.0f}'}
    sort_col = st.selectbox("表格排序字段", options=[c for c in display_cols if c!='symbol'], index=0)
    asc = st.radio("排序方式", ['降序','升序'], index=0)=='升序'
    df_disp = df_disp.sort_values(by=sort_col, ascending=asc)
    st.dataframe(df_disp.style.format(fmt), use_container_width=True, height=300)

    # 绘图可选
    st.subheader("📈 绘图")
    if st.checkbox("显示图表"):
        symbols = df_window['symbol'].unique().tolist()
        sel_syms = st.multiselect("选择标的(symbol)", symbols, default=symbols[:3])
        metrics = ['L/S','Long_USD','Short_USD','OI/Mcap','price','Long_Liq24h','Short_Liq24h']
        sel_metrics = st.multiselect("选择指标", metrics, default=['L/S','price'])
        if sel_syms and sel_metrics:
            df_plot = df_window[df_window['symbol'].isin(sel_syms)]
            df_melt = df_plot.melt(
                id_vars=['ts','symbol'], value_vars=sel_metrics,
                var_name='metric', value_name='value'
            )
            df_melt['series'] = df_melt['symbol'] + ' ' + df_melt['metric']
            series_list = df_melt['series'].unique().tolist()
            colors = {s: st.color_picker(s, '#%06x' % (hash(s)&0xFFFFFF)) for s in series_list}
            chart = alt.Chart(df_melt).mark_line().encode(
                x=alt.X('ts:T', title='时间'),
                y=alt.Y('value:Q', title='数值'),
                color=alt.Color('series:N', scale=alt.Scale(domain=series_list, range=[colors[s] for s in series_list]), legend=alt.Legend(title="系列"))
            ).properties(width=800, height=400)
            st.altair_chart(chart, use_container_width=True)
        else:
            st.info("请先选择至少一个标的和一个指标。")
