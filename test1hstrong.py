#!/usr/bin/env python3
"""Compute hourly strong asset rankings from the ``ohlcv_1h`` table.

For each of the latest 24 hours this script calculates the price
change percentage between the hour's open and close.  Each hour's
symbols are ranked by this percentage and the top 40 records are kept.
Finally the script counts how many times each symbol appears in the
hourly top 40 list and computes its average and median rank.
"""

from __future__ import annotations

import statistics
from typing import Dict, List

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


def get_latest_ts() -> int:
    """Return latest ``time`` value from ``ohlcv_1h``."""
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
    """Return OHLCV rows for the given time range."""
    conn = psycopg2.connect(**DB_CFG)
    sql = (
        "SELECT symbol, time, open, close FROM ohlcv_1h "
        "WHERE time BETWEEN %s AND %s"
    )
    df = pd.read_sql(sql, conn, params=(start_ts, end_ts))
    conn.close()
    return df


def hourly_rank(df: pd.DataFrame) -> pd.DataFrame:
    """Return records of hourly ranking (top 40)."""
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "pct", "rank"])
    df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
    records: List[dict] = []
    for ts, grp in df.groupby("time"):
        grp_sorted = grp.sort_values("pct", ascending=False).head(40)
        grp_sorted = grp_sorted.reset_index(drop=True)
        for idx, row in grp_sorted.iterrows():
            records.append({
                "time": ts,
                "symbol": row["symbol"],
                "pct": float(row["pct"]),
                "rank": idx + 1,
            })
    return pd.DataFrame(records)


def aggregate_stats(ranks: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ranking statistics for each symbol."""
    if ranks.empty:
        return pd.DataFrame(columns=["symbol", "times", "avg_rank", "median_rank"])
    grp = ranks.groupby("symbol")["rank"].agg(list)
    stats_records = []
    for sym, rank_list in grp.items():
        count = len(rank_list)
        avg = sum(rank_list) / count
        med = statistics.median(rank_list)
        stats_records.append({
            "symbol": sym,
            "times": count,
            "avg_rank": avg,
            "median_rank": med,
        })
    stats = pd.DataFrame(stats_records)
    stats = stats.sort_values(["times", "avg_rank"], ascending=[False, True])
    return stats.reset_index(drop=True)


def main() -> None:
    end_ts = get_latest_ts()
    start_ts = end_ts - 23 * 3600 * 1000

    df = fetch_range(start_ts, end_ts)
    ranks = hourly_rank(df)
    stats = aggregate_stats(ranks)

    if stats.empty:
        print("No data in the selected time range")
        return

    print("symbol\tcount\tavg_rank\tmedian_rank")
    for row in stats.itertuples(index=False):
        print(f"{row.symbol}\t{row.times}\t{row.avg_rank:.2f}\t{row.median_rank:.2f}")


if __name__ == "__main__":
    main()
