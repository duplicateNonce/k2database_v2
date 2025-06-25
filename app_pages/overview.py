import streamlit as st
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh
import pandas as pd
from utils import safe_rerun
from config import TZ_NAME
from tgbot.time_strong_asset import last_4h_range
from tgbot.time_vol_alert import last_hour_label
from result_cache import load_cached


def _latest_strong_assets() -> tuple[str, pd.DataFrame]:
    """Return label and cached dataframe for the last 4h strong assets."""
    _, _, label = last_4h_range()
    _cid, df = load_cached("overview_sa", {})
    if df is None:
        return label, pd.DataFrame()
    return label, df


def _latest_volume_alert() -> tuple[str, pd.DataFrame]:
    """Return label and cached dataframe for the last hour volume alert."""
    _, label = last_hour_label()
    _cid, df = load_cached("overview_vol", {})
    if df is None:
        return label, pd.DataFrame()
    return label, df

def render_overview():
    st.title("Dashboard")

    tz = pytz.timezone(TZ_NAME)
    now = datetime.now(tz)

    def _next_refresh_time(dt: datetime) -> datetime:
        rt = dt.replace(minute=6, second=0, microsecond=0)
        if dt >= rt:
            rt += timedelta(hours=1)
        return rt

    if "next_refresh_time" not in st.session_state:
        st.session_state["next_refresh_time"] = _next_refresh_time(now)

    refresh_time = st.session_state["next_refresh_time"]

    time_left = refresh_time - now
    refresh_due = time_left.total_seconds() <= 0

    interval = 1000 if time_left.total_seconds() <= 30 else 10_000
    st_autorefresh(interval=interval, key="overview_timer")

    def manual_refresh() -> None:
        """Reload cached results and refresh the page."""
        sa_label, st.session_state["sa_df"] = _latest_strong_assets()
        st.session_state["sa_label"] = sa_label
        vol_label, st.session_state["vol_df"] = _latest_volume_alert()
        st.session_state["vol_label"] = vol_label
        st.session_state["next_refresh_time"] = _next_refresh_time(datetime.now(tz))
        safe_rerun()

    col_spacer, col_countdown, col_button = st.columns([8, 2, 1])
    with col_countdown:
        if refresh_due:
            st.markdown("下次刷新倒计时：刷新中")
        else:
            st.markdown(f"下次刷新倒计时：{str(time_left).split('.')[0]}")
    with col_button:
        st.button("↻", help="手动刷新", on_click=manual_refresh)

    if (
        "sa_df" not in st.session_state
        or "vol_df" not in st.session_state
        or refresh_due
    ):
        sa_label, st.session_state["sa_df"] = _latest_strong_assets()
        st.session_state["sa_label"] = sa_label
        vol_label, st.session_state["vol_df"] = _latest_volume_alert()
        st.session_state["vol_label"] = vol_label
        st.session_state["next_refresh_time"] = _next_refresh_time(datetime.now(tz))

    sa_label = st.session_state.get("sa_label", "")
    vol_label = st.session_state.get("vol_label", "")

    col1, col2 = st.columns(2)
    with col1:
        st.markdown(f"**最近4h（{sa_label}）强势标的**")
        st.dataframe(st.session_state.get("sa_df"), use_container_width=True)
    with col2:
        st.markdown(f"**{vol_label} 成交量异动 (24h均量)**")
        st.dataframe(st.session_state.get("vol_df"), use_container_width=True)

