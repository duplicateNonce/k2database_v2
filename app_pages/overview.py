import streamlit as st
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh
import pandas as pd
from sqlalchemy import text

from config import TZ_NAME
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from tgbot.time_strong_asset import last_4h_range
from tgbot.time_vol_alert import last_hour_label, volume_deviation


def _latest_strong_assets() -> tuple[str, pd.DataFrame]:
    """Return label and dataframe for the last 4h strong assets."""
    start_ts, end_ts, label = last_4h_range()
    with engine_ohlcv.begin() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]
        label_map = {r[0]: r[1] for r in conn.execute(text("SELECT instrument_id, labels FROM instruments"))}

    records = []
    for sym in symbols:
        try:
            m = compute_period_metrics(sym, start_ts, end_ts)
        except ValueError:
            continue
        m["symbol"] = sym
        records.append(m)

    if not records:
        return label, pd.DataFrame()

    df = pd.DataFrame(records)
    df["标签"] = df["symbol"].map(lambda s: "，".join(label_map.get(s, [])) if label_map.get(s) else "")
    df = df.sort_values("period_return", ascending=False).head(10).reset_index(drop=True)
    df["期间收益"] = (df["period_return"] * 100).map(lambda x: f"{x:.2f}%")
    df["symbol"] = df["symbol"].str.replace("USDT", "")
    df = df[["标签", "symbol", "期间收益"]].rename(columns={"symbol": "代币名字"})
    return label, df


def _latest_volume_alert() -> tuple[str, pd.DataFrame]:
    """Return label and dataframe for the last hour volume alert."""
    start_ts, label = last_hour_label()
    with engine_ohlcv.begin() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]

    records = []
    for sym in symbols:
        pct = volume_deviation(sym, start_ts, 24)
        if pct is None:
            continue
        records.append({"symbol": sym, "pct": pct})

    if not records:
        return label, pd.DataFrame()

    df = pd.DataFrame(records)
    df = df[df["pct"] >= 200]
    if df.empty:
        return label, pd.DataFrame()

    df["差异"] = df["pct"].map(lambda x: f"{x:.2f}%")
    df = df.sort_values("pct", ascending=False).head(20).reset_index(drop=True)
    df["symbol"] = df["symbol"].str.replace("USDT", "")
    df = df[["symbol", "差异"]].rename(columns={"symbol": "代币名字"})
    return label, df

def render_overview():
    st.title("Dashboard")

    # Auto refresh every hour at the 6 minute mark
    tz = pytz.timezone(TZ_NAME)
    now = datetime.now(tz)
    refresh_time = now.replace(minute=6, second=0, microsecond=0)
    if now >= refresh_time:
        refresh_time += timedelta(hours=1)
    interval_ms = int((refresh_time - now).total_seconds() * 1000)
    st_autorefresh(interval=interval_ms, key="overview_refresh")

    st.subheader("定时推送任务")
    st.markdown(
        "- **time_strong_asset.py**：每小时05分发送最近4小时强势标的信息\n"
        "- **time_vol_alert.py**：每小时06分推送成交量异动提醒"
    )

    if (
        "sa_df" not in st.session_state
        or "vol_df" not in st.session_state
        or st.button("手动刷新")
    ):
        sa_label, st.session_state["sa_df"] = _latest_strong_assets()
        st.session_state["sa_label"] = sa_label
        vol_label, st.session_state["vol_df"] = _latest_volume_alert()
        st.session_state["vol_label"] = vol_label

    sa_label = st.session_state.get("sa_label", "")
    vol_label = st.session_state.get("vol_label", "")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**最近4h（{sa_label}）强势标的**")
        st.dataframe(st.session_state.get("sa_df"), use_container_width=True)
    with col2:
        st.markdown(f"**{vol_label} 成交量异动 (24h均量)**")
        st.dataframe(st.session_state.get("vol_df"), use_container_width=True)

