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
        default="BTCUSDT",
        help="Trading pair to check",
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


def main() -> None:
    args = parse_args()

    df_latest = last_volumes(args.symbol, 4)
    if df_latest.empty:
        print("No data for symbol")
        return

    earliest = int(df_latest["time"].min())
    hist_end = earliest - 1
    start_ts = hist_end - args.window * 3600 * 1000
    vols = history_volumes(args.symbol, start_ts, hist_end)
    if vols.empty:
        print("Not enough history")
        return

    threshold = vols.quantile(args.quantile)

    tz = pytz.timezone(TZ_NAME)
    alerts = []
    for row in df_latest.itertuples(index=False):
        if row.volume > threshold:
            ts_str = datetime.fromtimestamp(row.time / 1000, tz).strftime(
                "%Y-%m-%d %H:%M"
            )
            alerts.append(
                f"[{ts_str}] {args.symbol} 成交量异动：当前 {row.volume:.0f} > "
                f"{args.quantile * 100:.0f}% 分位 {threshold:.0f}"
            )

    if alerts:
        for msg in alerts:
            print(msg)
            send_message(msg)
    else:
        print("当前4h内Binance场内无交易量异动")


if __name__ == "__main__":
    main()
