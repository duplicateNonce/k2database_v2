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


def aggregate_1d(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("dt").sort_index()
    counts = df["open"].resample("24H").count()
    complete = counts[counts == 96].index
    if complete.empty:
        return pd.DataFrame()
    o = df["open"].resample("24H").first().loc[complete]
    h = df["high"].resample("24H").max().loc[complete]
    l = df["low"].resample("24H").min().loc[complete]
    c = df["close"].resample("24H").last().loc[complete]
    v = df["volume_usd"].resample("24H").sum().loc[complete]
    res = pd.DataFrame({"start": complete, "open": o, "high": h, "low": l, "close": c, "volume": v})
    return res.reset_index(drop=True)


@st.cache_data(show_spinner=False)
def load_history(mtime: float | None = None) -> pd.DataFrame:
    """Load cached history from CSV.

    The modification time of the CSV file is used as a cache key so that the
    cache is invalidated whenever the file changes."""
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
def prepare_chart_data(df: pd.DataFrame, symbols: list[str]) -> pd.DataFrame:
    """Prepare pivoted data used for the chart."""
    chart_df = df[df["symbol"].isin(symbols)].copy()
    pivot = chart_df.pivot(index="time", columns="symbol", values="rank")
    return pivot.reset_index().melt('time', var_name='symbol', value_name='rank')


def _scale_rank(r: float, max_rank: int) -> float:
    """Scale rank so 1-30 occupies 75%% of the visual range.

    The returned value is normalised to ``[0, 1]`` so that it can be used as
    the y coordinate directly in Altair charts.  When ``max_rank`` is less than
    or equal to 30 no compression is applied and the range is mapped linearly.
    """

    if pd.isna(r):
        return float('nan')
    if max_rank <= 30:
        if max_rank > 1:
            return (r - 1) / (max_rank - 1)
        return 0.0

    if r <= 30:
        return (r - 1) / 29 * 0.75
    return 0.75 + (r - 30) / (max_rank - 30) * 0.25


def update_history() -> pd.DataFrame:
    df_hist = load_history(CSV_FILE.stat().st_mtime if CSV_FILE.exists() else None)
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
            start_ms = int(last_time.tz_convert("UTC").timestamp() * 1000) - 96 * 15 * 60 * 1000
            sql = text(
                "SELECT time, open, high, low, close, volume_usd FROM ohlcv WHERE symbol=:sym AND time >= :s ORDER BY time"
            )
            params = {"sym": sym, "s": start_ms}
        with engine_ohlcv.connect() as conn:
            df = pd.read_sql(sql, conn, params=params)
        if df.empty:
            return None
        df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True).dt.tz_convert(TZ_NAME)
        df1d = aggregate_1d(df)
        # 使用相邻 1D 收盘价计算涨幅
        df1d["change"] = df1d["close"].pct_change()
        if last_time is not None:
            df1d = df1d[df1d["start"] > last_time]
        if df1d.empty:
            return None
        df1d["symbol"] = sym
        return df1d[["start", "symbol", "change"]]

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
        mtime = CSV_FILE.stat().st_mtime if CSV_FILE.exists() else None
        st.session_state["rank_history_df"] = load_history(mtime)

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
        step=timedelta(days=1),
        format="YYYY-MM-DD",
    )
    df_range = df[(df["time"] >= start) & (df["time"] <= end)]
    if df_range.empty:
        st.info("该区间无数据")
        return

    mean, med = compute_stats(df_range)
    symbols_all = sorted(med.index)
    hide_low = st.checkbox(
        "隐去期间内从未到达过前30的标的",
        value=st.session_state.get("rank_hide_low", False),
        key="rank_hide_low",
    )
    if hide_low:
        min_rank = df_range.groupby("symbol")["rank"].min()
        symbols_all = [s for s in symbols_all if min_rank.get(s, float("inf")) <= 30]

    selected_symbols = st.multiselect(
        "选择展示的标的",
        options=symbols_all,
        default=st.session_state.get("rank_selected_symbols", symbols_all[:5]),
        key="rank_selected_symbols",
    )
    symbols = selected_symbols or symbols_all
    if not symbols:
        st.info("无满足条件的标的")
        return
    # 准备图表数据，同样使用缓存减少计算量
    chart_data = prepare_chart_data(df_range, symbols)
    max_rank = int(df_range["rank"].max())
    chart_data["rank_scaled"] = chart_data["rank"].apply(lambda r: _scale_rank(r, max_rank))

    if max_rank <= 30:
        tick_values = list(range(1, max_rank + 1))
        axis = alt.Axis(
            title='排名',
            values=[_scale_rank(v, max_rank) for v in tick_values],
            labelExpr=f"Math.round(datum.value * ({max_rank - 1}) + 1)"
        )
    else:
        tick_values = [v for v in [1,5,10,15,20,25,30] if v <= max_rank]
        if max_rank not in tick_values:
            tick_values.append(max_rank)
        axis = alt.Axis(
            title='排名',
            values=[_scale_rank(v, max_rank) for v in tick_values],
            labelExpr=f"datum.value <= 0.75 ? Math.round(datum.value / 0.75 * 29 + 1) : Math.round(30 + (datum.value - 0.75) / 0.25 * {max_rank - 30})"
        )

    base = (
        alt.Chart(chart_data)
        .mark_line(strokeWidth=1)
        .encode(
            x=alt.X('time:T', title='时间', axis=alt.Axis(format='%m/%d')),
            y=alt.Y(
                'rank_scaled:Q',
                scale=alt.Scale(domain=[1, 0], nice=False),
                axis=axis,
            ),
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
    st.write("统计表")
    st.dataframe(pd.DataFrame({"mean": mean, "median": med}).loc[symbols].sort_values("median"))
