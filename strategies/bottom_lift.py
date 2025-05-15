import pandas as pd
import pytz
from datetime import timedelta
from utils import TZ
from db import engine_ohlcv
from sqlalchemy import text

def analyze_bottom_lift(t1, t2, factor=100):
    """
    t1, t2: naive datetime in Asia/Shanghai
    factor: 放大因子，默认 100

    在 t1、t2 各 ±1 小时窗口内，分别取 min low 值 L1 和 L2，
    计算斜率 = (L2 - L1) * factor，返回按斜率降序排序。
    同时输出 L1_time, L1_low, L2_time, L2_low。
    """
    # 本地化到上海时区
    t1_local = TZ.localize(t1)
    t2_local = TZ.localize(t2)

    # 窗口 ±1h
    start1 = t1_local - timedelta(hours=1)
    end1   = t1_local + timedelta(hours=1)
    start2 = t2_local - timedelta(hours=1)
    end2   = t2_local + timedelta(hours=1)

    # 转 UTC ms 时间戳
    start1_ms = int(start1.astimezone(pytz.utc).timestamp() * 1000)
    end1_ms   = int(end1.astimezone(pytz.utc).timestamp() * 1000)
    start2_ms = int(start2.astimezone(pytz.utc).timestamp() * 1000)
    end2_ms   = int(end2.astimezone(pytz.utc).timestamp() * 1000)

    # 获取所有 symbol
    with engine_ohlcv.connect() as conn:
        symbols = [row[0] for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]

    records = []
    for s in symbols:
        with engine_ohlcv.connect() as conn:
            df1 = pd.read_sql(
                text("SELECT time, low FROM ohlcv WHERE symbol=:s AND time BETWEEN :a AND :b"),
                conn,
                params={"s": s, "a": start1_ms, "b": end1_ms},
            )
            df2 = pd.read_sql(
                text("SELECT time, low FROM ohlcv WHERE symbol=:s AND time BETWEEN :a AND :b"),
                conn,
                params={"s": s, "a": start2_ms, "b": end2_ms},
            )
        if df1.empty or df2.empty:
            continue

        idx1 = df1["low"].idxmin()
        idx2 = df2["low"].idxmin()
        L1_time_ms = df1.at[idx1, "time"]
        L1_low     = df1.at[idx1, "low"]
        L2_time_ms = df2.at[idx2, "time"]
        L2_low     = df2.at[idx2, "low"]

        slope = (L2_low - L1_low) * factor

        # 转换回本地时区 datetime
        L1_time = pd.to_datetime(L1_time_ms, unit="ms").tz_localize('UTC').tz_convert(TZ)
        L2_time = pd.to_datetime(L2_time_ms, unit="ms").tz_localize('UTC').tz_convert(TZ)

        records.append({
            "symbol":   s,
            "L1_time":  L1_time,
            "L1_low":   L1_low,
            "L2_time":  L2_time,
            "L2_low":   L2_low,
            "slope":    slope,
        })

    df = pd.DataFrame(records).set_index("symbol")
    return df.sort_values("slope", ascending=False)
