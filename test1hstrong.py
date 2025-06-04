#!/usr/bin/env python3
"""Compute hourly strong asset rankings from the ``ohlcv_1h`` table.

For each of the latest 24 hours this script calculates the price
change percentage between the hour's open and close.  Each hour's
symbols are ranked by this percentage.  All ranks are retained when
calculating statistics so that tokens outside the hourly top 40 still
contribute their actual positions.  ``times`` still counts how many
hours a symbol made it into the top 40.
"""

from __future__ import annotations

from typing import Dict, List

import pandas as pd
import psycopg2
import unicodedata

from config import secret_get


DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "ohlcv_1h"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}


def get_labels_map() -> Dict[str, list]:
    """Return a mapping of symbol -> labels list."""
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
            # ensure string -> [string]
            labels[sym] = [str(lbl)]
    return labels


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
    """Return records of hourly ranking for **all** symbols."""
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "pct", "rank"])

    df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["rank"] = (
        df.groupby("time")["pct"].rank(ascending=False, method="min").astype(int)
    )

    df = df.sort_values(["time", "rank"])  # nicer ordering
    return df[["time", "symbol", "pct", "rank"]]


def aggregate_stats(ranks: pd.DataFrame) -> pd.DataFrame:
    """Aggregate ranking statistics for each symbol.

    ``avg_percentile`` is the mean of each hour's percentile score where a
    higher value means better performance. ``median_rank`` retains the original
    median rank logic. Missing hours are counted as one rank worse than the
    worst actual rank for that hour. ``times`` counts how often a symbol ranked
    within the top 40.
    """

    if ranks.empty:
        return pd.DataFrame(
            columns=["symbol", "times", "avg_percentile", "median_rank"]
        )

    # Pivot tables for percentile calculation and median rank
    pivot_rank = ranks.pivot(index="symbol", columns="time", values="rank")
    pivot_pct = ranks.pivot(index="time", columns="symbol", values="pct")

    times = (pivot_rank <= 40).sum(axis=1)

    max_rank_by_time = ranks.groupby("time")["rank"].max()
    pivot_rank = pivot_rank.apply(
        lambda col: col.fillna(max_rank_by_time[col.name] + 1)
    )

    median_rank = pivot_rank.median(axis=1)

    # Calculate percentile scores for each hour
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


def _display_width(text: str) -> int:
    """Return the display width accounting for wide characters."""
    width = 0
    for ch in text:
        if unicodedata.east_asian_width(ch) in ("F", "W"):
            width += 2
        else:
            width += 1
    return width


def _ljust_display(text: str, width: int) -> str:
    """Left justify ``text`` considering display width."""
    pad = width - _display_width(text)
    if pad > 0:
        return text + " " * pad
    return text


def format_ascii_table(df: pd.DataFrame) -> str:
    """Return ``df`` formatted as a simple ASCII table."""
    if df.empty:
        return ""

    headers = list(df.columns)
    rows = [[str(v) for v in row] for row in df.itertuples(index=False)]

    widths = [_display_width(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            w = _display_width(cell)
            if w > widths[i]:
                widths[i] = w

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    header_line = "| " + " | ".join(_ljust_display(headers[i], widths[i]) for i in range(len(headers))) + " |"
    sep_line = "+" + "+".join("=" * (w + 2) for w in widths) + "+"
    lines = [border, header_line, sep_line]
    for row in rows:
        line = "| " + " | ".join(_ljust_display(row[i], widths[i]) for i in range(len(headers))) + " |"
        lines.append(line)
    lines.append(border)
    return "\n".join(lines)


def main() -> None:
    end_ts = get_latest_ts()
    start_ts = end_ts - 23 * 3600 * 1000

    df = fetch_range(start_ts, end_ts)
    ranks = hourly_rank(df)
    stats = aggregate_stats(ranks)

    if stats.empty:
        print("No data in the selected time range")
        return

    labels_map = get_labels_map()

    display_df = stats.copy()
    display_df["label"] = display_df["symbol"].map(
        lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
    )
    cols = ["label"] + [c for c in display_df.columns if c != "label"]
    display_df = display_df[cols]
    display_df["avg_percentile"] = display_df["avg_percentile"].map(
        lambda x: f"{x:.2f}"
    )
    display_df["median_rank"] = display_df["median_rank"].map(lambda x: f"{x:.2f}")
    print(format_ascii_table(display_df))


if __name__ == "__main__":
    main()
