import streamlit as st
import pandas as pd
from datetime import timezone, timedelta
from sqlalchemy import text
from db import engine_ohlcv
from config import TZ_NAME
from strategies.strong_assets import compute_period_metrics

# 缓存加载原始BTC 15m数据
@st.cache_resource
def load_btc_15m():
    df = pd.read_sql(text(
        "SELECT time, open, high, low, close, volume_usd "
        "FROM ohlcv WHERE symbol='BTCUSDT'"
    ), engine_ohlcv)
    df['dt'] = pd.to_datetime(df['time'], unit='ms', utc=True).dt.tz_convert(TZ_NAME)
    return df

# 使用 pandas.resample 聚合严格完整的1小时K线
def aggregate_hourly(df_raw: pd.DataFrame) -> pd.DataFrame:
    df = df_raw.set_index('dt').sort_index()
    counts = df['open'].resample('1H').count()
    complete = counts[counts == 4].index
    o = df['open'].resample('1H').first()
    h = df['high'].resample('1H').max()
    l = df['low'].resample('1H').min()
    c = df['close'].resample('1H').last()
    v = df['volume_usd'].resample('1H').sum()
    res = pd.DataFrame({'open': o, 'high': h, 'low': l, 'close': c, 'volume_usd': v}).loc[complete]
    res = res.reset_index()
    res['hour_start'] = (res['dt'].astype('int64') // 10**6).astype(int)
    return res[['hour_start', 'dt', 'open', 'high', 'low', 'close', 'volume_usd']]


def render_monitor():
    st.title("Monitor")

    # 1. 加载并聚合1h数据
    btc15m = load_btc_15m()
    hourly = aggregate_hourly(btc15m)
    if hourly.empty:
        st.info("暂无完整1小时K线可用")
        return

    # 2. 找到最新上涨>=1%的1h bar
    mask_up = (hourly['close'] > hourly['open']) & ((hourly['high'] / hourly['low'] - 1) >= 0.01)
    up_bars = hourly[mask_up]
    if up_bars.empty:
        st.info("暂无上涨>=1%的1小时K线，监测工具暂不运行")
        return
    last_bar = up_bars.iloc[-1]
    end_ms = int(last_bar['hour_start'] + 45 * 60 * 1000)

    # 3. 找到最近跌幅>=1%的1h bar（发生在 last_bar 之前）
    mask_down = (hourly['close'] - hourly['open']) / hourly['open'] <= -0.01
    down_bars = hourly[mask_down & (hourly['hour_start'] < last_bar['hour_start'])]
    if down_bars.empty:
        st.error("尚未检测到跌幅>=1%的1小时K线，无法进行区间分析")
        return
    start_bar = down_bars.iloc[-1]
    start_ms = int(start_bar['hour_start'])

    # 4. 显示区间起止
    start_dt = start_bar['dt']
    end_dt = last_bar['dt'] + timedelta(minutes=45)
    st.markdown(
        f"**区间起点：** {start_dt.strftime('%Y-%m-%d %H:%M')}  "
        f"**区间终点：** {end_dt.strftime('%Y-%m-%d %H:%M')}"
    )

    # 5. 核对1h聚合结果
    period = hourly[(hourly['hour_start'] >= start_ms) & (hourly['hour_start'] <= last_bar['hour_start'])]
    st.subheader("1小时聚合K线(供核对)")
    st.dataframe(period[['dt', 'open', 'high', 'low', 'close', 'volume_usd']])

    # 6. 计算全标的及标签强势排序
    inst = pd.read_sql(text("SELECT symbol, labels FROM instruments"), engine_ohlcv)
    df_all = pd.read_sql(text(
        "SELECT symbol, low, close FROM ohlcv "
        "WHERE time BETWEEN :start AND :end"
    ), engine_ohlcv, params={"start": int(start_ms), "end": int(end_ms)})
    records = []
    for sym, grp in df_all.groupby('symbol'):
        low_price = grp['low'].min()
        last_price = grp.iloc[-1]['close']
        reb = (last_price / low_price - 1) * 100
        labels = inst.loc[inst['symbol'] == sym, 'labels'].iloc[0] if sym in inst['symbol'].values else ''
        records.append({'symbol': sym, 'labels': labels, '反弹幅度(%)': reb})
    df_reb = pd.DataFrame(records).sort_values('反弹幅度(%)', ascending=False)
    st.subheader("全标的及标签强势排序")
    st.dataframe(df_reb)

# 在 streamlit_app.py 注册 render_monitor()
