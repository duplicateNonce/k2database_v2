import re
from datetime import datetime, timezone
import pytz
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
    """Call Streamlit rerun function if available."""
    if hasattr(st, "rerun"):
        st.rerun()
    elif hasattr(st, "experimental_rerun"):
        st.experimental_rerun()


def short_time_range(d1: str, t1: str, d2: str, t2: str) -> str:
    """Return a short label like '5.3 17:45 - 5.5 17:30'."""
    try:
        dt1 = datetime.fromisoformat(f"{d1}T{t1}")
        dt2 = datetime.fromisoformat(f"{d2}T{t2}")
        return f"{dt1.month}.{dt1.day} {dt1.strftime('%H:%M')} - {dt2.month}.{dt2.day} {dt2.strftime('%H:%M')}"
    except Exception:
        return ""
