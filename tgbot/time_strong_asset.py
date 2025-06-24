#!/usr/bin/env python3
"""Send the strongest assets over the last 4h to Telegram."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
import pandas as pd
from sqlalchemy import text
import pytz

from strategies.strong_assets import compute_period_metrics
from db import engine_ohlcv
from monitor_bot import ascii_table, send_message
from config import TZ_NAME


def last_4h_range() -> tuple[int, int, str]:
    """Return start/end timestamps in ms and a HH:MM-HH:MM label."""
    tz = pytz.timezone(TZ_NAME)
    now_ts = int(datetime.now(tz).timestamp())
    end_dt = datetime.fromtimestamp(((now_ts // 3600) - 1) * 3600, tz)
    start_dt = end_dt - timedelta(hours=3)
    label = f"{start_dt.strftime('%H:%M')}-{end_dt.strftime('%H:%M')}"
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000), label


def main() -> None:
    start_ts, end_ts, label = last_4h_range()
    with engine_ohlcv.begin() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]
        label_map = {r[0]: r[1] for r in conn.execute(text("SELECT instrument_id, labels FROM instruments"))}

    records = []
    for sym in symbols:
        try:
            m = compute_period_metrics(sym, start_ts, end_ts)
        except ValueError:
            continue
        m["symbol"] = sym
        records.append(m)

    if not records:
        print("No data for the specified period")
        return

    df = pd.DataFrame(records)
    df["标签"] = df["symbol"].map(lambda s: "，".join(label_map.get(s, [])) if label_map.get(s) else "")
    df["期间收益"] = (df["period_return"] * 100).map(lambda x: f"{x:.2f}%")
    df = df.sort_values("period_return", ascending=False).reset_index(drop=True)
    # Only keep the top 10 assets to avoid overly long Telegram messages
    df = df.head(10)
    df["symbol"] = df["symbol"].str.replace("USDT", "")
    df = df[["标签", "symbol", "期间收益"]]
    df = df.rename(columns={"symbol": "代币名字"})

    table = ascii_table(df)
    header = f"最近4h（{label}）强势标的"

    # Print to console so manual runs have visible output
    print(header)
    print(table)

    # Send to Telegram if credentials are configured
    send_message(f"{header}\n```\n{table}\n```", parse_mode="Markdown")


if __name__ == "__main__":
    main()
