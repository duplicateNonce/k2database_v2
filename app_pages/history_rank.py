import pandas as pd
import streamlit as st
import altair as alt
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from sqlalchemy import text
from db import engine_ohlcv
from config import TZ_NAME
from datetime import timedelta
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


@st.cache_data(show_spinner=False)
def compute_stats(df: pd.DataFrame):
    med = df.groupby("symbol")["rank"].median()
    mean = df.groupby("symbol")["rank"].mean()
    return mean, med


@st.cache_data(show_spinner=False)
def prepare_chart_data(df: pd.DataFrame, symbols: list[str], group_input: str) -> pd.DataFrame:
    """Prepare pivoted data used for the chart."""
    chart_df = df[df["symbol"].isin(symbols)].copy()
    if group_input.strip():
        syms = [s.strip() for s in group_input.split(',') if s.strip()]
        sub = chart_df[chart_df["symbol"].isin(syms)]
        if not sub.empty:
            grp = sub.groupby("time")["rank"].mean().reset_index()
            grp["symbol"] = "自定义组"
            chart_df = pd.concat([chart_df, grp], ignore_index=True)
    pivot = chart_df.pivot(index="time", columns="symbol", values="rank")
    return pivot.reset_index().melt('time', var_name='symbol', value_name='rank')


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
        # 使用相邻 4H 收盘价计算涨幅
        df4h["change"] = df4h["close"].pct_change()
        if last_time is not None:
            df4h = df4h[df4h["start"] > last_time]
        if df4h.empty:
            return None
        df4h["symbol"] = sym
        return df4h[["start", "symbol", "change"]]

    with engine_ohlcv.connect() as conn:
        syms = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))]
    syms = [s for s in syms if s not in SKIP_SYMBOLS]

    # 使用更多线程提高增量更新速度
    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(fetch_history, sym) for sym in syms]
        for fut in futures:
            res = fut.result()
            if res is not None:
                records.append(res)
    if not records:
        return df_hist
    df_new = pd.concat(records, ignore_index=True)
    # drop rows where change is NaN before ranking
    df_new = df_new.dropna(subset=["change"])
    df_new["rank"] = (
        df_new.groupby("start")["change"].rank(ascending=False, method="min")
        .astype(int)
    )
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

    # 缓存到 session_state，避免每次交互都重新读取文件
    if "rank_history_df" not in st.session_state:
        st.session_state["rank_history_df"] = load_history()

    df = st.session_state["rank_history_df"]

    # 将数据刷新入口放在折叠面板中，刷新后更新 session_state
    with st.expander("数据刷新", expanded=df.empty):
        if st.button("刷新数据"):
            with st.spinner("更新中..."):
                df = update_history()
                st.session_state["rank_history_df"] = df
                st.success("已更新")
    if df.empty:
        st.info("暂无数据")
        return
    last_time = df["time"].max().tz_convert(TZ_NAME)
    st.write(f"最后更新：{last_time.strftime('%Y-%m-%d %H:%M')}")

    # 选择时间区间，默认全选
    min_t = df["time"].min()
    max_t = df["time"].max()
    start, end = st.slider(
        "选择时间区间",
        min_value=min_t.to_pydatetime(),
        max_value=max_t.to_pydatetime(),
        value=(min_t.to_pydatetime(), max_t.to_pydatetime()),
        step=timedelta(hours=4),
        format="YYYY-MM-DD HH:mm",
    )
    df_range = df[(df["time"] >= start) & (df["time"] <= end)]
    if df_range.empty:
        st.info("该区间无数据")
        return

    mean, med = compute_stats(df_range)
    threshold = st.number_input(
        "显示中位数>",
        min_value=0,
        value=st.session_state.get("rank_threshold", 10),
        key="rank_threshold",
    )
    symbols = [s for s in med.index if med[s] > threshold]
    st.write("统计表")
    st.dataframe(pd.DataFrame({"mean": mean, "median": med}).loc[symbols].sort_values("median"))
    if not symbols:
        st.info("无满足条件的标的")
        return
    group_input = st.text_input(
        "自定义分组(逗号分隔)",
        value=st.session_state.get("rank_group", ""),
        key="rank_group",
    )
    # 准备图表数据，同样使用缓存减少计算量
    chart_data = prepare_chart_data(df_range, symbols, group_input)
    base = (
        alt.Chart(chart_data)
        .mark_line(strokeWidth=1)
        .encode(
            x=alt.X('time:T', title='时间'),
            y=alt.Y('rank:Q', title='排名', scale=alt.Scale(domain=[1, 408], reverse=True)),
            color='symbol:N'
        )
    )
    enlarged = st.checkbox(
        "放大图表",
        value=st.session_state.get("rank_enlarged", False),
        key="rank_enlarged",
    )
    chart = base.properties(height=600 if enlarged else 400).interactive()
    st.altair_chart(chart, use_container_width=True)
