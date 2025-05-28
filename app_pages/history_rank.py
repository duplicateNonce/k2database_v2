import pandas as pd
import streamlit as st
import altair as alt
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from sqlalchemy import text
from db import engine_ohlcv
from config import TZ_NAME
CSV_FILE = Path("data/rank_history.csv")
SKIP_SYMBOLS = {"USDCUSDT", "BTCDOMUSDT"}


def aggregate_4h(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("dt").sort_index()
    counts = df["open"].resample("4H").count()
    complete = counts[counts == 16].index
    if complete.empty:
        return pd.DataFrame()
    o = df["open"].resample("4H").first().loc[complete]
    h = df["high"].resample("4H").max().loc[complete]
    l = df["low"].resample("4H").min().loc[complete]
    c = df["close"].resample("4H").last().loc[complete]
    v = df["volume_usd"].resample("4H").sum().loc[complete]
    res = pd.DataFrame({"start": complete, "open": o, "high": h, "low": l, "close": c, "volume": v})
    return res.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_history() -> pd.DataFrame:
    if CSV_FILE.exists():
        try:
            return pd.read_csv(CSV_FILE, parse_dates=["time"])
        except Exception:
            return pd.DataFrame()
    return pd.DataFrame()


def update_history() -> pd.DataFrame:
    df_hist = load_history()
    last_time = None
    if not df_hist.empty:
        last_time = df_hist["time"].max()
        if not pd.api.types.is_datetime64_any_dtype(df_hist["time"]):
            last_time = pd.to_datetime(last_time)
    records = []

    def fetch_history(sym: str):
        if last_time is None:
            sql = text(
                "SELECT time, open, high, low, close, volume_usd FROM ohlcv WHERE symbol=:sym ORDER BY time"
            )
            params = {"sym": sym}
        else:
            start_ms = int(last_time.tz_convert("UTC").timestamp() * 1000) - 16 * 15 * 60 * 1000
            sql = text(
                "SELECT time, open, high, low, close, volume_usd FROM ohlcv WHERE symbol=:sym AND time >= :s ORDER BY time"
            )
            params = {"sym": sym, "s": start_ms}
        with engine_ohlcv.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        if df.empty:
            return None
        df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert(TZ_NAME)
        df4h = aggregate_4h(df)
        if last_time is not None:
            df4h = df4h[df4h["start"] > last_time]
        if df4h.empty:
            return None
        df4h["symbol"] = sym
        df4h["change"] = df4h["close"] / df4h["open"] - 1
        return df4h[["start", "symbol", "change"]]

    with engine_ohlcv.connect() as conn:
        syms = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]
    syms = [s for s in syms if s not in SKIP_SYMBOLS]

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [pool.submit(fetch_history, sym) for sym in syms]
        for fut in futures:
            res = fut.result()
            if res is not None:
                records.append(res)
    if not records:
        return df_hist
    df_new = pd.concat(records, ignore_index=True)
    df_new["rank"] = df_new.groupby("start")["change"].rank(ascending=False, method="min").astype(int)
    df_new = df_new.rename(columns={"start": "time"})
    if df_hist.empty:
        df_res = df_new
    else:
        df_res = pd.concat([df_hist, df_new], ignore_index=True)
    df_res.sort_values(["time", "rank"], inplace=True)
    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_res.to_csv(CSV_FILE, index=False)
    return df_res


def render_history_rank():
    st.title("历史排名")
    df = load_history()
    if st.button("刷新数据"):
        with st.spinner("更新中..."):
            df = update_history()
            st.success("已更新")
    if df.empty:
        st.info("暂无数据")
        return
    last_time = df["time"].max().tz_convert(TZ_NAME)
    st.write(f"最后更新：{last_time.strftime('%Y-%m-%d %H:%M')}")
    med = df.groupby("symbol")["rank"].median()
    mean = df.groupby("symbol")["rank"].mean()
    threshold = st.number_input("显示中位数>=", min_value=1, value=10)
    symbols = [s for s in med.index if med[s] >= threshold]
    st.write("统计表")
    st.dataframe(pd.DataFrame({"mean": mean, "median": med}).loc[symbols].sort_values("median"))
    if not symbols:
        st.info("无满足条件的标的")
        return
    group_input = st.text_input("自定义分组(逗号分隔)")
    chart_df = df[df["symbol"].isin(symbols)].copy()
    if group_input.strip():
        syms = [s.strip() for s in group_input.split(',') if s.strip()]
        sub = chart_df[chart_df["symbol"].isin(syms)]
        if not sub.empty:
            grp = sub.groupby("time")["rank"].mean().reset_index()
            grp["symbol"] = "自定义组"
            chart_df = pd.concat([chart_df, grp], ignore_index=True)
    pivot = chart_df.pivot(index="time", columns="symbol", values="rank")
    chart_data = pivot.reset_index().melt('time', var_name='symbol', value_name='rank')
    base = (
        alt.Chart(chart_data)
        .mark_line(strokeWidth=1)
        .encode(
            x=alt.X('time:T', title='时间'),
            y=alt.Y('rank:Q', title='排名'),
            color='symbol:N'
        )
    )
    enlarged = st.checkbox('放大图表')
    chart = base.properties(height=600 if enlarged else 400).interactive()
    st.altair_chart(chart, use_container_width=True)
