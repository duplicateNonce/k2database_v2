import streamlit as st
from db import engine_coin
from queries import fetch_latest_snapshot


def render_overview():
    st.header("最新行情 & 24h 涨幅榜")
    n = st.slider("前 N", 5, 100, 20, key="overview_n")
    df = fetch_latest_snapshot(engine_coin)
    df_rank = df.sort_values("price_change_percent_24h", ascending=False).head(n)
    st.dataframe(df_rank[["symbol", "price_change_percent_24h", "current_price"]], use_container_width=True)
