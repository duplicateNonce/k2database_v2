import pandas as pd
from sqlalchemy import text
from db import engine_ohlcv      # 使用项目中的 db 连接引擎
from config import TZ_NAME

def compute_period_metrics(symbol: str,
                           start_ts: int,
                           end_ts: int) -> dict:
    """
    计算单个交易对在 [start_ts, end_ts] 区间内基于原始 15 分钟 OHLCV 的指标：
      1) 期间收益率 = (最后一条 close / 第一条 close) - 1
      2) 区间内最高 close 及其北京时间；最低 close 及其北京时间
      3) 回调比例 = (最高 close 后的最低 close - 最高 close) / 最高 close
    """
    # 1. 拉取 close 数据
    with engine_ohlcv.connect() as conn:
        df = pd.read_sql(text("""
            SELECT time, close
            FROM ohlcv_1h
            WHERE symbol = :symbol
              AND time BETWEEN :start AND :end
            ORDER BY time
        """), conn, params={
            "symbol": symbol,
            "start":  start_ts,
            "end":    end_ts,
        })

    if df.empty:
        raise ValueError(f"{symbol} 在指定区间无数据")

    # 2. 时区转换
    df['dt'] = (
        pd.to_datetime(df['time'], unit='ms', utc=True)
          .dt.tz_convert(TZ_NAME)
    )
    df = df.reset_index(drop=True)

    # 3. 首尾收盘价 & 期间收益率
    first_close = df.at[0, 'close']
    last_close = df.at[len(df)-1, 'close']
    period_return = last_close / first_close - 1

    # 4. 区间峰值及峰值后最低点
    # 找到峰值位置
    peak_idx = df['close'].idxmax()
    peak_close = df.at[peak_idx, 'close']
    peak_dt = df.at[peak_idx, 'dt']
    # 在峰值之后的数据里找最低
    if peak_idx < len(df) - 1:
        trough_df = df.iloc[peak_idx+1:]
        trough_idx = trough_df['close'].idxmin()
    else:
        # 如果峰值是最后一条，回调为0，最低点即峰值
        trough_idx = peak_idx
    trough_close = df.at[trough_idx, 'close']
    trough_dt = df.at[trough_idx, 'dt']

    # 5. 回调比例 = (peak - trough) / peak
    drawdown = (peak_close - trough_close) / peak_close if peak_close else 0

    return {
        "first_close": first_close,
        "last_close": last_close,
        "period_return": period_return,
        "max_close": peak_close,
        "max_close_dt": peak_dt,
        "min_close": trough_close,
        "min_close_dt": trough_dt,
        "drawdown": drawdown,
    }
