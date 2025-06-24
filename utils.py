import re
from datetime import datetime, timezone, timedelta
import pytz
import pandas as pd
from config import TZ_NAME
import streamlit as st

# 时区对象
TZ = pytz.timezone(TZ_NAME)


def utc8_now() -> datetime:
    """返回当前 UTC 时间转换为本地时区"""
    return datetime.now(timezone.utc).astimezone(TZ)


def extract_cols(expr: str, cols: list[str]) -> list[str]:
    """从一个表达式中提取所有列名"""
    pattern = r"\b([a-zA-Z_]\w*)\b"
    tokens = re.findall(pattern, expr)
    return [t for t in tokens if t in cols]


def safe_rerun() -> None:
    """Rerun the Streamlit app if the API is available.

    This helper checks both the modern ``st.rerun`` and the older
    ``st.experimental_rerun`` functions.  When running on very old
    Streamlit versions where neither helper exists, it falls back to
    raising the internal ``RerunException`` to trigger a script rerun."""

    if hasattr(st, "rerun"):
        st.rerun()
        return
    if hasattr(st, "experimental_rerun"):
        st.experimental_rerun()
        return
    # Fallback for Streamlit <0.65 without the rerun helpers
    try:
        from streamlit.script_runner import RerunException
        from streamlit.script_request_queue import RerunData

        raise RerunException(RerunData(None))
    except Exception:
        # In case the internals changed or are unavailable, quietly no-op
        pass


def short_time_range(d1: str, t1: str, d2: str, t2: str) -> str:
    """Return a short label like '5.3 17:45 - 5.5 17:30'."""
    try:
        dt1 = datetime.fromisoformat(f"{d1}T{t1}")
        dt2 = datetime.fromisoformat(f"{d2}T{t2}")
        return f"{dt1.month}.{dt1.day} {dt1.strftime('%H:%M')} - {dt2.month}.{dt2.day} {dt2.strftime('%H:%M')}"
    except Exception:
        return ""


def update_shared_range(start_date, start_time, end_date, end_time) -> None:
    """Store a time range in session state for cross-page reuse."""
    st.session_state["range_start_date"] = start_date
    st.session_state["range_start_time"] = start_time
    st.session_state["range_end_date"] = end_date
    st.session_state["range_end_time"] = end_time


def format_time_col(col: pd.Series) -> pd.Series:
    """Return formatted time strings for a column of ms timestamps or ISO datetimes."""
    if pd.api.types.is_datetime64_any_dtype(col):
        dt = col.dt.tz_convert("Asia/Shanghai")
    else:
        dt = pd.to_datetime(col, errors="coerce", unit="ms", utc=True)
        dt_alt = pd.to_datetime(col, errors="coerce", utc=True)
        dt = dt.fillna(dt_alt).dt.tz_convert("Asia/Shanghai")
    return dt.dt.strftime("%m-%d %H:%M")


def quick_range_buttons(
    start_date_key: str,
    start_time_key: str,
    end_date_key: str,
    end_time_key: str,
    *,
    tz_name: str = TZ_NAME,
) -> None:
    """Render quick-select buttons for common time ranges.

    When a button is clicked the corresponding start/end date and time values
    in ``st.session_state`` are updated and the app reruns so that the
    associated inputs reflect the new values.
    """

    tz = pytz.timezone(tz_name)
    now = datetime.now(tz)
    hours = [1, 4, 12, 24, 72, 168]
    labels = ["最近1h", "最近4h", "最近12h", "最近24h", "最近72h", "最近一周"]
    cols = st.columns(len(hours))

    def set_range(hours: int) -> None:
        """Callback to update the time range and trigger a rerun."""
        # Data is stored in hourly granularity.  Always align the end
        # time to the most recent completed hour regardless of the
        # selected range length.
        now_ts = int(datetime.now(tz).timestamp())
        # use the last completed hour as end time since each candle's
        # timestamp marks the start of that hour
        end_ts = ((now_ts // 3600) - 1) * 3600
        end = datetime.fromtimestamp(end_ts, tz)
        # include ``hours`` candles ending at ``end`` (inclusive)
        start = end - timedelta(hours=hours - 1)

        st.session_state[start_date_key] = start.date()
        st.session_state[start_time_key] = start.time().replace(second=0, microsecond=0)
        st.session_state[end_date_key] = end.date()
        st.session_state[end_time_key] = end.time().replace(second=0, microsecond=0)
        safe_rerun()

    for col, h, label in zip(cols, hours, labels):
        col.button(label, key=f"{start_date_key}_q{h}", on_click=set_range, args=(h,))
