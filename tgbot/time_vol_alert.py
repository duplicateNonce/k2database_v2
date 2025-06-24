#!/usr/bin/env python3
"""Check hourly trading volume and notify Telegram on anomalies."""

from __future__ import annotations

from datetime import datetime
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from sqlalchemy import text
import pytz

from db import engine_ohlcv
from monitor_bot import send_message
from config import TZ_NAME


def parse_args() -> argparse.Namespace:
    """Return CLI arguments."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--symbol",
        help=(
            "Trading pair to check. If omitted, all symbols in the database are"
            " checked"
        ),
    )
    parser.add_argument(
        "--window",
        type=int,
        default=720,
        help="Look-back window in hours",
    )
    parser.add_argument(
        "--quantile",
        type=float,
        default=0.95,
        help="Quantile threshold",
    )
    return parser.parse_args()


def latest_volume(symbol: str) -> tuple[int, float] | tuple[None, None]:
    """Return ``(time, volume)`` of the newest record for ``symbol``."""
    sql = text(
        "SELECT time, volume_usd AS volume FROM ohlcv_1h "
        "WHERE symbol=:sym ORDER BY time DESC LIMIT 1"
    )
    df = pd.read_sql(sql, engine_ohlcv, params={"sym": symbol})
    if df.empty:
        return None, None
    return int(df.loc[0, "time"]), float(df.loc[0, "volume"])


def history_volumes(symbol: str, start_ts: int, end_ts: int) -> pd.Series:
    """Return historical volumes for ``symbol`` between ``start_ts`` and ``end_ts``."""
    sql = text(
        "SELECT volume_usd AS volume FROM ohlcv_1h "
        "WHERE symbol=:sym AND time BETWEEN :start AND :end"
    )
    df = pd.read_sql(
        sql,
        engine_ohlcv,
        params={"sym": symbol, "start": start_ts, "end": end_ts},
    )
    return df["volume"]


def last_volumes(symbol: str, count: int = 4) -> pd.DataFrame:
    """Return the last ``count`` hourly volumes for ``symbol`` sorted by time."""
    sql = text(
        f"SELECT time, volume_usd AS volume FROM ohlcv_1h "
        f"WHERE symbol=:sym ORDER BY time DESC LIMIT {int(count)}"
    )
    df = pd.read_sql(sql, engine_ohlcv, params={"sym": symbol})
    return df.sort_values("time")


def check_symbol(symbol: str, window: int, quantile: float) -> list[str]:
    """Check ``symbol`` for volume anomalies and return alert messages."""
    df_latest = last_volumes(symbol, 4)
    if df_latest.empty:
        print(f"No data for {symbol}")
        return []

    earliest = int(df_latest["time"].min())
    hist_end = earliest - 1
    start_ts = hist_end - window * 3600 * 1000
    vols = history_volumes(symbol, start_ts, hist_end)
    if vols.empty:
        print(f"Not enough history for {symbol}")
        return []

    threshold = vols.quantile(quantile)

    def percentile_rank(v: float) -> float:
        """Return the percentile rank of ``v`` within ``vols``."""
        return pd.concat([vols, pd.Series([v])]).rank(pct=True).iloc[-1] * 100

    tz = pytz.timezone(TZ_NAME)
    alerts = []
    report_rows = []
    for row in df_latest.itertuples(index=False):
        ts_str = datetime.fromtimestamp(row.time / 1000, tz).strftime("%Y-%m-%d %H:%M")
        pct = percentile_rank(row.volume)
        report_rows.append((ts_str, pct))
        if row.volume > threshold:
            alerts.append(
                f"[{ts_str}] {symbol} 成交量异动：当前 {row.volume:.0f} > "
                f"{quantile * 100:.0f}% 分位 {threshold:.0f}"
            )

    print(f"Symbol: {symbol}")
    print("Last 4 periods and percentile ranks:")
    for ts_str, pct in report_rows:
        print(f"{ts_str} -> {pct:.2f}%")

    return alerts


def main() -> None:
    args = parse_args()

    with engine_ohlcv.begin() as conn:
        if args.symbol:
            symbols = [args.symbol]
        else:
            symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]

    all_alerts: list[str] = []
    for sym in symbols:
        alerts = check_symbol(sym, args.window, args.quantile)
        all_alerts.extend(alerts)

    if all_alerts:
        for msg in all_alerts:
            send_message(msg)
    else:
        print("当前4h内Binance场内无交易量异动")


if __name__ == "__main__":
    main()
