import streamlit as st
from datetime import datetime, timedelta
import pytz
from streamlit_autorefresh import st_autorefresh
import pandas as pd
from pathlib import Path
from utils import safe_rerun
from config import TZ_NAME
from tgbot.time_vol_alert import last_hour_label
from sqlalchemy import text
from result_cache import load_cached, save_cached
from strategies.strong_assets import compute_period_metrics
from db import engine_ohlcv

CACHE_BASE = Path("/home/ubuntu/k2database/tgbot/data/cache")


def _read_latest(subdir: str) -> pd.DataFrame:
    """Return the contents of the most recently modified CSV in ``subdir``."""
    folder = CACHE_BASE / subdir
    if not folder.exists():
        return pd.DataFrame()
    csv_files = list(folder.glob("*.csv"))
    if not csv_files:
        return pd.DataFrame()
    latest = max(csv_files, key=lambda p: p.stat().st_mtime)
    try:
        return pd.read_csv(latest, index_col=0)
    except Exception:
        return pd.DataFrame()



def _slot_range(now: datetime, start_h: int, end_h: int) -> tuple[int, int, str]:
    """Return start/end timestamps and label for a time slot.

    ``start_h`` and ``end_h`` are hour values in 24h format.  If the slot for
    the current day has not finished yet, only completed hours are included.
    When no data is available for today the same slot of the previous day is
    used instead.
    """
    tz = pytz.timezone(TZ_NAME)
    now = now.astimezone(tz)
    start_today = now.replace(hour=start_h, minute=0, second=0, microsecond=0)
    end_today = now.replace(hour=end_h, minute=0, second=0, microsecond=0)
    last_hour = now.replace(minute=0, second=0, microsecond=0) - timedelta(hours=1)

    slot_hours = end_h - start_h

    if last_hour < start_today:
        start_dt = start_today - timedelta(days=1)
        end_dt = start_dt + timedelta(hours=slot_hours) - timedelta(hours=1)
    else:
        start_dt = start_today
        end_dt = min(last_hour, end_today - timedelta(hours=1))
        if end_dt < start_dt:
            start_dt -= timedelta(days=1)
            end_dt = start_dt + timedelta(hours=slot_hours) - timedelta(hours=1)

    label = (
        f"{start_dt.strftime('%Y.%m.%d %H:%M')}-"
        f"{(end_dt + timedelta(hours=1)).strftime('%H:%M')}"
    )
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000), label


def _load_slot_df(start_ts: int, end_ts: int) -> pd.DataFrame:
    """Load or compute strong assets for the given time range."""
    params = {"start": start_ts, "end": end_ts}
    cache_id, df = load_cached("overview_sa_slots", params)
    if df is not None:
        return df

    with engine_ohlcv.begin() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]
        label_map = {
            r[0]: r[1] for r in conn.execute(text("SELECT instrument_id, labels FROM instruments"))
        }

    records = []
    for sym in symbols:
        try:
            m = compute_period_metrics(sym, start_ts, end_ts)
        except ValueError:
            continue
        m["symbol"] = sym
        records.append(m)

    if not records:
        df = pd.DataFrame()
    else:
        df = pd.DataFrame(records)
        df["标签"] = df["symbol"].map(
            lambda s: "，".join(label_map.get(s, [])) if label_map.get(s) else ""
        )
        df["期间收益"] = (df["period_return"] * 100).map(lambda x: f"{x:.2f}%")
        df = df.sort_values("period_return", ascending=False).reset_index(drop=True)
        df = df.head(10)
        df["symbol"] = df["symbol"].str.replace("USDT", "")
        df = df[["标签", "symbol", "期间收益"]].rename(columns={"symbol": "代币名字"})

    save_cached("overview_sa_slots", params, df)
    return df


def _latest_volume_alert() -> tuple[str, pd.DataFrame]:
    """Return label and cached dataframe for the last hour volume alert."""
    _, label = last_hour_label()
    df = _read_latest("overview_vol")
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

    SLOT_INFO = [
        (4, 8, "美股收盘强势标的"),
        (8, 12, "亚盘时间强势标的"),
        (16, 22, "美股盘前强势标的"),
    ]

    def _load_all_slots() -> list[tuple[str, pd.DataFrame]]:
        results = []
        for start_h, end_h, title in SLOT_INFO:
            s_ts, e_ts, label = _slot_range(now, start_h, end_h)
            df = _load_slot_df(s_ts, e_ts)
            results.append((f"{label} {title}", df))
        return results

    def manual_refresh() -> None:
        """Reload cached results and refresh the page."""
        st.session_state["sa_slots"] = _load_all_slots()
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
        "sa_slots" not in st.session_state
        or "vol_df" not in st.session_state
        or refresh_due
    ):
        st.session_state["sa_slots"] = _load_all_slots()
        vol_label, st.session_state["vol_df"] = _latest_volume_alert()
        st.session_state["vol_label"] = vol_label
        st.session_state["next_refresh_time"] = _next_refresh_time(datetime.now(tz))

    if "sa_index" not in st.session_state:
        st.session_state["sa_index"] = 0
    elif refresh_due:
        st.session_state["sa_index"] = (st.session_state["sa_index"] + 1) % len(SLOT_INFO)

    vol_label = st.session_state.get("vol_label", "")

    col1, col2 = st.columns(2)
    with col1:
        sa_label, sa_df = st.session_state["sa_slots"][st.session_state["sa_index"]]
        st.markdown(f"**{sa_label}**")
        st.dataframe(sa_df, use_container_width=True)
    with col2:
        st.markdown(f"**{vol_label} 成交量异动 (24h均量)**")
        st.dataframe(st.session_state.get("vol_df"), use_container_width=True)

