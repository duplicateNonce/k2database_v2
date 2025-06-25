import streamlit as st
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh

from config import TZ_NAME

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
