#!/usr/bin/env python3
"""Monitor Hyperliquid whale alerts and send Telegram notifications."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import datetime
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

import requests
import pandas as pd
import pytz

from monitor_bot import send_message
from config import CG_API_KEY, TZ_NAME

CSV_FILE = Path("data/hyper_whale.csv")
API_URL = "https://open-api-v4.coinglass.com/api/hyperliquid/whale-alert"
LOG_FILE = "hyper_whale_alert.log"

TZ = pytz.timezone(TZ_NAME)


def log_msg(msg: str) -> None:
    """Print and append ``msg`` to ``LOG_FILE`` with timestamp."""
    ts = datetime.now(TZ).strftime("%Y-%m-%d %H:%M:%S")
    line = f"{ts} {msg}"
    print(line)
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception as exc:
        print("Failed to write log:", exc)


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


def update_csv(records: list[dict]) -> list[dict]:
    """Append ``records`` to ``CSV_FILE`` if they are newer than existing ones.

    Returns the list of records that were newly inserted."""
    if not records:
        return []

    df_new = pd.DataFrame(records).sort_values("create_time")

    if CSV_FILE.exists():
        try:
            df_old = pd.read_csv(CSV_FILE)
            last_ts = df_old["create_time"].max() if not df_old.empty else 0
        except Exception:
            df_old = pd.DataFrame()
            last_ts = 0
    else:
        df_old = pd.DataFrame()
        last_ts = 0

    df_insert = df_new[df_new["create_time"] > last_ts]

    df_all = pd.concat([df_old, df_insert], ignore_index=True)
    df_all = df_all.sort_values("create_time")
    CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    df_all.to_csv(CSV_FILE, index=False)

    return df_insert.to_dict("records")


def last_record() -> dict | None:
    """Return the last record from ``CSV_FILE`` if available."""
    if not CSV_FILE.exists():
        return None
    try:
        df = pd.read_csv(CSV_FILE)
    except Exception:
        return None
    if df.empty:
        return None
    return df.iloc[-1].to_dict()


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
        "🚨🚨🚨 Hyperliquid大额开仓 🚨🚨🚨",
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


def process_once(api_key: str) -> None:
    """Fetch records, update CSV and handle notifications."""
    records = fetch_records(api_key)
    new_records = update_csv(records)

    log_msg(f"写入{len(new_records)}条数据")

    if new_records:
        msgs = []
        for r in new_records:
            msg = format_message(r)
            msgs.append(msg)
            log_msg(msg)
        send_message("\n\n".join(msgs))
    else:
        log_msg("无大户开仓数据")
        rec = last_record()
        if rec:
            log_msg(format_message(rec))


def main() -> None:
    api_key = CG_API_KEY
    if not api_key:
        print("CG_API_KEY is not configured")
        return

    while True:
        try:
            process_once(api_key)
        except Exception as exc:
            log_msg(f"error: {exc}")
        time.sleep(60)


if __name__ == "__main__":
    main()
