#!/usr/bin/env python3
"""Monitor Hyperliquid whale alerts and send Telegram notifications."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
import pytz

from monitor_bot import send_message
from config import CG_API_KEY, TZ_NAME

CSV_FILE = Path("data/hyper_whale.csv")
API_URL = "https://open-api-v4.coinglass.com/api/hyperliquid/whale-alert"


def fetch_records(api_key: str) -> list[dict]:
    """Return whale alert records from Coinglass."""
    headers = {"accept": "application/json", "CG-API-KEY": api_key}
    try:
        resp = requests.get(API_URL, headers=headers, timeout=15)
        data = resp.json().get("data", []) if resp.status_code == 200 else []
    except Exception:
        return []

    for r in data:
        if "liq_price" not in r:
            r["liq_price"] = r.get("liquidation_price")
    return data


def update_csv(records: list[dict]) -> None:
    """Append ``records`` to ``CSV_FILE`` avoiding duplicates."""
    if not records:
        return
    df_new = pd.DataFrame(records)
    if CSV_FILE.exists():
        try:
            df_old = pd.read_csv(CSV_FILE)
        except Exception:
            df_old = pd.DataFrame()
        df_all = pd.concat([df_old, df_new], ignore_index=True)
        df_all = df_all.drop_duplicates(
            subset=["user", "create_time", "position_action"], keep="first"
        )
    else:
        df_all = df_new
    df_all = df_all.sort_values("create_time")
    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(CSV_FILE, index=False)


def _action_text(action: int, size: float) -> str:
    if action == 1:
        return "开多" if size > 0 else "开空"
    if action == 2:
        return "平多" if size > 0 else "平空"
    return str(action)


def _direction_text(size: float) -> str:
    return "做多📈" if size > 0 else "做空🈳"


def format_message(record: dict) -> str:
    tz = pytz.timezone(TZ_NAME)
    dt = datetime.fromtimestamp(record["create_time"] / 1000, tz)
    time_str = dt.strftime("%Y-%m-%d %H:%M:%S")
    lev = "N/A"
    ep = record.get("entry_price")
    lp = record.get("liq_price")
    if ep is not None and lp is not None and ep != lp:
        lev = f"{ep / (ep - lp):.1f}x"
    msg_lines = [
        "\uD83D\uDEA8\uD83D\uDEA8\uD83D\uDEA8 Hyperliquid大额开仓 \uD83D\uDEA8\uD83D\uDEA8\uD83D\uDEA8",
        f"user = 开仓地址：https://hyperdash.info/zh-CN/trader/{record['user']}",
        f"时间：{time_str}",
        f"symbol = 标的：{record['symbol']}",
        f"position action：{_action_text(record['position_action'], record['position_size'])}",
        f"方向：{_direction_text(record['position_size'])}",
        f"position size：{record['position_size']} 枚 {record['symbol']}",
        f"entry_price：{record['entry_price']:.6f}",
        f"liq_price：{record['liq_price']:.6f}",
        f"position_value_usd：{record['position_value_usd']} USD",
        f"估算名义杠杆率 {lev}",
    ]
    return "\n".join(msg_lines)


def main() -> None:
    api_key = CG_API_KEY
    if not api_key:
        print("CG_API_KEY is not configured")
        return

    records = fetch_records(api_key)
    if not records:
        print("no records fetched")
        return

    update_csv(records)

    last = records[-1]
    if last.get("position_value_usd", 0) >= 10_000_000:
        msg = format_message(last)
        send_message(msg)


if __name__ == "__main__":
    main()
