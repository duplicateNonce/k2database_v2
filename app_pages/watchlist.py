# 新页面：自选标的跟踪
import json
from pathlib import Path
from datetime import datetime, date, time, timedelta, timezone

import pandas as pd
import streamlit as st
from utils import safe_rerun
from sqlalchemy import text

from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from strategies.bottom_lift import analyze_bottom_lift

WATCHLIST_FILE = Path("data/watchlist.json")


def load_watchlist() -> list[str]:
    if WATCHLIST_FILE.exists():
        try:
            data = json.loads(WATCHLIST_FILE.read_text())
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def save_watchlist(lst: list[str]) -> None:
    WATCHLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    WATCHLIST_FILE.write_text(json.dumps(lst, ensure_ascii=False, indent=2))


def aggregate_4h(df: pd.DataFrame) -> pd.DataFrame:

    """Aggregate 15m candles into 4h bars starting from local midnight."""
    df = df.set_index("dt").sort_index()
    df.index = df.index.floor("15min")
    origin = pd.Timestamp("1970-01-01", tz=df.index.tz)

    rs = df.resample(
        "4H",
        label="left",
        closed="left",
        origin=origin,

        offset="0H",

    )

    counts = rs["open"].count()
    complete = counts[counts == 16].index
    if complete.empty:
        return pd.DataFrame()

    o = rs["open"].first().loc[complete]
    h = rs["high"].max().loc[complete]
    l = rs["low"].min().loc[complete]
    c = rs["close"].last().loc[complete]
    v = rs["volume_usd"].sum().loc[complete]
    res = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v})
    res = res.reset_index().rename(columns={"dt": "start"})
    return res


def aggregate_daily(df4h: pd.DataFrame) -> pd.DataFrame:
    df = df4h.set_index("start").sort_index()
    counts = df["open"].resample("24H").count()
    complete = counts[counts == 6].index
    if complete.empty:
        return pd.DataFrame()
    o = df["open"].resample("24H").first().loc[complete]
    h = df["high"].resample("24H").max().loc[complete]
    l = df["low"].resample("24H").min().loc[complete]
    c = df["close"].resample("24H").last().loc[complete]
    v = df["volume"].resample("24H").sum().loc[complete]
    res = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v})
    res = res.reset_index().rename(columns={"start": "date"})
    return res


def rsi(series: pd.Series, period: int = 14) -> pd.Series:
    delta = series.diff()
    up = delta.clip(lower=0)
    down = -delta.clip(upper=0)
    ma_up = up.rolling(period).mean()
    ma_down = down.rolling(period).mean()
    rs = ma_up / ma_down
    return 100 - (100 / (1 + rs))


def macd(series: pd.Series, fast: int = 12, slow: int = 26, signal: int = 9):
    ema_fast = series.ewm(span=fast, adjust=False).mean()
    ema_slow = series.ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    return macd_line, signal_line


def find_cross_times(dates: pd.Series, macd_line: pd.Series, signal_line: pd.Series):
    cond_g = (macd_line.shift(1) <= signal_line.shift(1)) & (macd_line > signal_line)
    cond_d = (macd_line.shift(1) >= signal_line.shift(1)) & (macd_line < signal_line)
    golden = dates[cond_g.fillna(False)].dt.strftime("%m-%d %H:%M").tolist()
    death = dates[cond_d.fillna(False)].dt.strftime("%m-%d %H:%M").tolist()
    return golden, death


def render_watchlist_page():
    st.title("自选标的")

    watchlist = load_watchlist()
    # ---- 管理自选列表 ----
    with st.expander("管理自选标的", expanded=False):
        with engine_ohlcv.connect() as conn:
            syms = [row[0] for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]
        add_sym = st.selectbox("添加标的", [s for s in syms if s not in watchlist])
        if st.button("添加"):
            if add_sym and add_sym not in watchlist:
                watchlist.append(add_sym)
                save_watchlist(watchlist)
                st.success(f"已添加 {add_sym}")
                safe_rerun()
        if watchlist:
            for sym in watchlist:
                if st.button(f"删除 {sym}", key=f"del_{sym}"):
                    watchlist.remove(sym)
                    save_watchlist(watchlist)
                    safe_rerun()
        else:
            st.write("暂无自选标的")

    if not watchlist:
        st.info("请先在上方添加自选标的")
        return

    # ---- 时间区间选择 ----
    col1, col2 = st.columns(2)
    with col1:
        sd = st.date_input("开始日期", date.today() - timedelta(days=7))
        stime = st.time_input("开始时间", time(0, 0))
    with col2:
        ed = st.date_input("结束日期", date.today())
        etime = st.time_input("结束时间", time(23, 59))

    if st.button("计算", key="watch_calc"):
        tz = timezone(timedelta(hours=8))
        start_dt = datetime.combine(sd, stime).replace(tzinfo=tz)
        end_dt = datetime.combine(ed, etime).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        records = []
        charts = {}
        for sym in watchlist:
            df = pd.read_sql(
                text(
                    "SELECT time, open, high, low, close, volume_usd FROM ohlcv "
                    "WHERE symbol=:sym AND time BETWEEN :a AND :b ORDER BY time"
                ),
                engine_ohlcv,
                params={"sym": sym, "a": start_ts, "b": end_ts},
            )
            if df.empty:
                continue
            df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert("Asia/Shanghai")
            # strong_asset 指标
            try:
                m = compute_period_metrics(sym, start_ts, end_ts)
            except ValueError:
                continue
            max_vol = df["high"].max() - df["low"].min()
            # 聚合4H
            agg4h = aggregate_4h(df)
            if agg4h.empty:
                continue
            rsi4 = rsi(agg4h["close"]).iloc[-1] if len(agg4h) >= 14 else None
            daily = aggregate_daily(agg4h)
            rsi1d = rsi(daily["close"]).iloc[-1] if len(daily) >= 14 else None
            macd_line, signal_line = macd(agg4h["close"])
            golden, death = find_cross_times(agg4h["start"], macd_line, signal_line)
            last_golden = golden[-1] if golden else ""
            last_death = death[-1] if death else ""
            records.append({
                "symbol": sym,
                "period_return": m["period_return"] * 100,
                "drawdown": m["drawdown"] * 100,
                "max_volatility": max_vol,
                "RSI_4h": rsi4,
                "RSI_1d": rsi1d,
                "golden": last_golden,
                "death": last_death,
            })
            chart = pd.DataFrame({"dt": agg4h["start"], "close": agg4h["close"]})
            charts[sym] = chart

        if not records:
            st.warning("无有效数据")
            return
        df_res = pd.DataFrame(records)
        df_res = df_res.round({"period_return": 2, "drawdown": 2, "RSI_4h": 2, "RSI_1d": 2})
        df_res = df_res.rename(columns={
            "symbol": "标的",
            "period_return": "期间涨跌%",
            "drawdown": "最大回撤%",
            "max_volatility": "最大波动",
            "RSI_4h": "RSI4h",
            "RSI_1d": "RSI1d",
            "golden": "最后金叉",
            "death": "最后死叉",
        })
        st.dataframe(df_res, use_container_width=True)
        for sym, ch in charts.items():
            st.subheader(sym)
            st.line_chart(ch.set_index("dt"))
