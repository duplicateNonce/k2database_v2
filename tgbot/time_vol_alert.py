#!/usr/bin/env python3
"""Check hourly trading volume and notify Telegram on anomalies."""

from __future__ import annotations

from datetime import datetime, timedelta
import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import pandas as pd
from sqlalchemy import text
import pytz

from db import engine_ohlcv
from monitor_bot import ascii_table, send_message
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
        help="Look-back window in hours for rolling average",
    )
    parser.add_argument(
        "--top",
        type=int,
        default=20,
        help="Number of results to show",
    )
    return parser.parse_args()


def hour_volume(symbol: str, ts: int) -> float | None:
    """Return the hourly volume for ``symbol`` at ``ts``."""
    sql = text(
        "SELECT volume_usd AS volume FROM ohlcv_1h "
        "WHERE symbol=:sym AND time=:t"
    )
    df = pd.read_sql(sql, engine_ohlcv, params={"sym": symbol, "t": ts})
    if df.empty:
        return None
    return float(df.loc[0, "volume"])


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


def volume_deviation(symbol: str, ts: int, window: int) -> float | None:
    """Return percentage difference to ``window`` hour mean for ``symbol``."""
    vol = hour_volume(symbol, ts)
    if vol is None:
        return None
    start_ts = ts - window * 3600 * 1000
    vols = history_volumes(symbol, start_ts, ts - 1)
    if vols.empty:
        return None
    mean_vol = vols.mean()
    if not mean_vol:
        return None
    return (vol - mean_vol) / mean_vol * 100


def last_hour_label() -> tuple[int, str]:
    """Return timestamp (ms) of the last full hour and a label."""
    tz = pytz.timezone(TZ_NAME)
    now_ts = int(datetime.now(tz).timestamp())
    end_dt = datetime.fromtimestamp((now_ts // 3600) * 3600, tz)
    start_dt = end_dt - timedelta(hours=1)
    label = f"{start_dt.strftime('%Y.%m.%d %H:%M')}-{end_dt.strftime('%H:%M')}"
    return int(start_dt.timestamp() * 1000), label


def main() -> None:
    args = parse_args()

    start_ts, label = last_hour_label()
    with engine_ohlcv.begin() as conn:
        if args.symbol:
            symbols = [args.symbol]
        else:
            symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]
        label_map = {r[0]: r[1] for r in conn.execute(text("SELECT instrument_id, labels FROM instruments"))}

    records = []
    for sym in symbols:
        pct = volume_deviation(sym, start_ts, args.window)
        if pct is None:
            continue
        records.append({"symbol": sym, "pct": pct})

    if not records:
        print("No data for the specified period")
        return

    df = pd.DataFrame(records)
    df["标签"] = df["symbol"].map(lambda s: "，".join(label_map.get(s, [])) if label_map.get(s) else "")
    df["差异"] = df["pct"].map(lambda x: f"{x:.2f}%")
    df = df.sort_values("pct", ascending=False).reset_index(drop=True)
    df = df.head(args.top)
    df["symbol"] = df["symbol"].str.replace("USDT", "")
    df = df[["标签", "symbol", "差异"]].rename(columns={"symbol": "代币名字"})

    table = ascii_table(df)
    header = f"{label} 成交量异动"
    print(header)
    print(table)
    send_message(f"{header}\n```\n{table}\n```", parse_mode="Markdown")


if __name__ == "__main__":
    main()
