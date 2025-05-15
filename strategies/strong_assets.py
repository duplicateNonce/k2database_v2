import pandas as pd
from sqlalchemy import text
from db import engine_ohlcv
from config import TZ_NAME
from datetime import timezone as dt_timezone, timedelta

def analyze_strong_assets(benchmark: str, start_ts: int, end_ts: int, agg: int) -> pd.DataFrame:
    # 1. 获取全量 symbol
    with engine_ohlcv.connect() as conn:
        symbols = [row[0] for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]

    if not symbols:
        return pd.DataFrame()

    # 2. 批量拉取 OHLCV
    with engine_ohlcv.connect() as conn:
        sql = text("""
            SELECT symbol, time, open, high, low, close, volume_usd
            FROM ohlcv
            WHERE symbol = ANY(:symbols) AND time BETWEEN :start AND :end
            ORDER BY symbol, time
        """)
        df = pd.read_sql(sql, conn, params={
            "symbols": symbols,
            "start": start_ts,
            "end": end_ts
        })

    # 3. 时间戳转换 & 本地化
    df["time"] = (
        pd.to_datetime(df["time"], unit="ms")
          .dt.tz_localize("UTC")
          .dt.tz_convert(TZ_NAME)
    )
    df = df.set_index("time")

    # 4. 按 symbol+agg 分组聚合
    ohlc_dict = {
        "open": "first", "high": "max", "low": "min",
        "close": "last", "volume_usd": "sum"
    }
    df_agg = (
        df.groupby("symbol")
          .resample(f"{agg*15}min")
          .apply(ohlc_dict)
          .dropna()
    )

    # 5. 计算超额收益与信息比率
    returns = df_agg["close"].groupby("symbol").pct_change().dropna()
    # 基准资产的收益序列
    bench_ret_series = returns.loc[benchmark]
    # 超额收益
    excess_ret = returns.sub(bench_ret_series, axis=0)
    # 累计收益 & 超额累计收益 & IR
    cum_ret         = returns.groupby("symbol").sum()
    excess_cum_ret  = excess_ret.groupby("symbol").sum()
    ir              = excess_ret.groupby("symbol").mean() / excess_ret.groupby("symbol").std()

    # 6. 组织结果表
    results = pd.DataFrame({
        "cum_ret (%)":         (cum_ret * 100).round(2),
        "excess_cum_ret (%)":  (excess_cum_ret * 100).round(2),
        "IR":                  ir.round(4),
    })

    # 7. 按累计收益降序返回
    return results.sort_values("cum_ret (%)", ascending=False)
