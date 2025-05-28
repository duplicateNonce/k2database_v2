# File: strategies/bottom_lift.py
import pandas as pd
import numpy as np
import pytz
from db import engine_ohlcv


def analyze_bottom_lift(t1, t2, bars: int = 4, factor: float = 100.0) -> pd.DataFrame:
    """
    在两个时间点 t1, t2 周围各 ± bars 根 15 分钟 K 线窗口内计算最低价，
    并根据放大因子计算对数收益斜率。

    假设 ohlcv.time 列为 Unix 时间戳（毫秒），存储于 bigint 类型。

    参数：
    - t1, t2: datetime 对象（本地化时区 Asia/Shanghai）
    - bars: 窗口大小，以 K 线根数计（每根 15 分钟）
    - factor: 放大因子，用于将对数收益乘以此数，以便更直观地展示

    返回：
    - DataFrame，索引为 symbol，列为 L1_time, L1_low, L2_time, L2_low, slope
      其中 L1_time、L2_time 已转换为 Asia/Shanghai 时区的 datetime，
      slope 表示对数收益 ln(L2_low / L1_low) * factor
    """
    # 本地化时区
    tz = pytz.timezone("Asia/Shanghai")
    if t1.tzinfo is None:
        t1 = tz.localize(t1)
    if t2.tzinfo is None:
        t2 = tz.localize(t2)

    # 时间窗口偏移
    window = pd.Timedelta(minutes=15 * bars)
    t1_start, t1_end = t1 - window, t1 + window
    t2_start, t2_end = t2 - window, t2 + window

    # 转为 Unix 毫秒时间戳
    t1_start_ts = int(t1_start.timestamp() * 1000)
    t1_end_ts = int(t1_end.timestamp() * 1000)
    t2_start_ts = int(t2_start.timestamp() * 1000)
    t2_end_ts = int(t2_end.timestamp() * 1000)

    # 获取所有交易符号
    instruments_sql = "SELECT instrument_id AS symbol FROM instruments"
    instruments = pd.read_sql(instruments_sql, engine_ohlcv)
    symbols = instruments['symbol'].tolist()

    records = []
    # 统一 SQL 模板，使用时间戳比较
    sql = (
        "SELECT time, low FROM ohlcv "
        "WHERE symbol = %(sym)s AND time BETWEEN %(start_ts)s AND %(end_ts)s"
    )

    for sym in symbols:
        # 查询 t1 窗口
        df1 = pd.read_sql(
            sql,
            engine_ohlcv,
            params={"sym": sym, "start_ts": t1_start_ts, "end_ts": t1_end_ts},
        )
        if df1.empty:
            L1_time = None
            L1_low = None
        else:
            l1 = df1.loc[df1["low"].idxmin()]
            L1_time = (
                pd.to_datetime(l1["time"], unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
            )
            L1_low = l1["low"]

        # 查询 t2 窗口
        df2 = pd.read_sql(
            sql,
            engine_ohlcv,
            params={"sym": sym, "start_ts": t2_start_ts, "end_ts": t2_end_ts},
        )
        if df2.empty:
            L2_time = None
            L2_low = None
        else:
            l2 = df2.loc[df2["low"].idxmin()]
            L2_time = (
                pd.to_datetime(l2["time"], unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
            )
            L2_low = l2["low"]

        # 计算对数收益斜率：ln(L2_low / L1_low) * factor
        if L1_low is not None and L2_low is not None and L1_low > 0:
            slope = np.log(L2_low / L1_low) * factor
        else:
            slope = None

        records.append(
            {
                "symbol": sym,
                "L1_time": L1_time,
                "L1_low": L1_low,
                "L2_time": L2_time,
                "L2_low": L2_low,
                "slope": slope,
            }
        )

    result = pd.DataFrame(records)
    if not result.empty:
        result.set_index('symbol', inplace=True)
    return result

