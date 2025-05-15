import re
from datetime import datetime, timezone
import pytz
from config import TZ_NAME

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
