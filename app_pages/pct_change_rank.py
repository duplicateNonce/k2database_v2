import streamlit as st
import pandas as pd
import psycopg2
from config import secret_get


DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "ohlcv_1h"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}


def get_labels_map() -> dict[str, list]:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT instrument_id, labels FROM instruments")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = {}
    for sym, lbl in rows:
        if isinstance(lbl, list):
            labels[sym] = lbl
        elif lbl is None:
            labels[sym] = []
        else:
            labels[sym] = [str(lbl)]
    return labels


def get_latest_ts() -> int:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT MAX(time) FROM ohlcv_1h")
    res = cur.fetchone()
    cur.close()
    conn.close()
    if not res or res[0] is None:
        raise RuntimeError("ohlcv_1h 表无数据")
    return int(res[0])


def fetch_range(start_ts: int, end_ts: int) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CFG)
    sql = (
        "SELECT symbol, time, open, close FROM ohlcv_1h "
        "WHERE time BETWEEN %s AND %s"
    )
    df = pd.read_sql(sql, conn, params=(start_ts, end_ts))
    conn.close()
    return df


def hourly_rank(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "pct", "rank"])

    df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["rank"] = (
        df.groupby("time")["pct"].rank(ascending=False, method="min").astype(int)
    )
    df = df.sort_values(["time", "rank"])
    return df[["time", "symbol", "pct", "rank"]]


def aggregate_stats(ranks: pd.DataFrame, top_n: int = 40) -> pd.DataFrame:
    if ranks.empty:
        return pd.DataFrame(
            columns=["symbol", "times", "avg_percentile", "median_rank"]
        )

    pivot_rank = ranks.pivot(index="symbol", columns="time", values="rank")
    pivot_pct = ranks.pivot(index="time", columns="symbol", values="pct")

    times = (pivot_rank <= top_n).sum(axis=1)

    max_rank_by_time = ranks.groupby("time")["rank"].max()
    pivot_rank = pivot_rank.apply(
        lambda col: col.fillna(max_rank_by_time[col.name] + 1)
    )

    median_rank = pivot_rank.median(axis=1)

    df_percentile = pd.DataFrame(
        index=pivot_pct.index, columns=pivot_pct.columns, dtype=float
    )
    N = pivot_pct.shape[1]
    for h in pivot_pct.index:
        ranks_series = pivot_pct.loc[h].rank(
            ascending=False, method="first", na_option="bottom"
        )
        df_percentile.loc[h] = (N - ranks_series) / (N - 1)

    avg_percentile = df_percentile.mean(axis=0)

    stats = pd.DataFrame(
        {
            "symbol": avg_percentile.index,
            "times": times.reindex(avg_percentile.index).fillna(0).astype(int),
            "avg_percentile": avg_percentile,
            "median_rank": median_rank.reindex(avg_percentile.index),
        }
    )

    stats = stats.sort_values(
        ["times", "avg_percentile"], ascending=[False, False]
    )
    return stats.reset_index(drop=True)


def render_pct_change_rank_page():
    st.title("百分化涨幅排名")

    hours = st.selectbox(
        "统计时长 (小时)", [4, 12, 24, 72, 168], index=2,
        format_func=lambda x: f"最近 {x}h"
    )

    top_n = st.number_input(
        "计入次数的前 N 名", min_value=1, value=40, step=1
    )

    if st.button("计算排名"):
        end_ts = get_latest_ts()
        start_ts = end_ts - (hours - 1) * 3600 * 1000

        with st.spinner("计算中..."):
            df = fetch_range(start_ts, end_ts)
            ranks = hourly_rank(df)
            stats = aggregate_stats(ranks, int(top_n))

        if stats.empty:
            st.warning("指定区间没有数据")
            return

        labels_map = get_labels_map()
        stats["标签"] = stats["symbol"].map(
            lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
        )
        stats = stats[["标签", "symbol", "times", "avg_percentile", "median_rank"]]
        stats["avg_percentile"] = (stats["avg_percentile"] * 100).round(4)
        stats = stats.rename(columns={"symbol": "代币名字", "avg_percentile": "平均百分位(%)"})
        st.dataframe(stats.reset_index(drop=True), use_container_width=True)

