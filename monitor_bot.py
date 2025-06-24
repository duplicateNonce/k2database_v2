#!/usr/bin/env python3
"""Telegram bot for reporting strong assets over various time ranges."""

import time
import pandas as pd
import requests
from sqlalchemy import text
from datetime import datetime, timedelta
import pytz
import unicodedata

from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    TZ_NAME,
    get_proxy_dict,
)


# ---------------------------------------------------------------------------
# Messaging helpers
# ---------------------------------------------------------------------------

def send_message(text_msg: str, chat_id: int | str | None = None, parse_mode: str | None = None) -> None:
    """Send ``text_msg`` to Telegram."""
    token = TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": cid, "text": text_msg}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        requests.post(url, json=payload, timeout=10, proxies=get_proxy_dict())
    except Exception as exc:
        print("Failed to send telegram message:", exc)


def _display_width(text: str) -> int:
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def ascii_table(df: pd.DataFrame) -> str:
    """Return ``df`` formatted as ``label : token : gain`` lines."""

    if df.empty:
        return ""

    rows = df.astype(str).values.tolist()

    lines = [f"{r[0]} : {r[1]} : {r[2]}" for r in rows]
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Strong asset computation
# ---------------------------------------------------------------------------

def _latest_range(hours: int) -> tuple[int, int, str]:
    """Return start/end timestamps in ms and an ``HH:MM-HH:MM`` label."""
    tz = pytz.timezone(TZ_NAME)
    now_ts = int(datetime.now(tz).timestamp())
    end_dt = datetime.fromtimestamp(((now_ts // 3600) - 1) * 3600, tz)
    start_dt = end_dt - timedelta(hours=hours - 1)
    label = f"{start_dt.strftime('%H:%M')}-{(end_dt + timedelta(hours=1)).strftime('%H:%M')}"
    return int(start_dt.timestamp() * 1000), int(end_dt.timestamp() * 1000), label


def strong_assets_command(chat_id: int, hours: int) -> None:
    start_ts, end_ts, label = _latest_range(hours)
    with engine_ohlcv.begin() as conn:
        symbols = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]
        label_map = {r[0]: r[1] for r in conn.execute(text("SELECT instrument_id, labels FROM instruments"))}

    records = []
    for sym in symbols:
        try:
            metrics = compute_period_metrics(sym, start_ts, end_ts)
        except ValueError:
            continue
        metrics["symbol"] = sym
        records.append(metrics)

    if not records:
        send_message("无有效数据", chat_id)
        return

    df = pd.DataFrame(records)
    df["标签"] = df["symbol"].map(lambda s: "，".join(label_map.get(s, [])) if label_map.get(s) else "")
    df["期间收益"] = (df["period_return"] * 100).map(lambda x: f"{x:.2f}%")
    df = df.sort_values("period_return", ascending=False).reset_index(drop=True)
    df = df.head(20)
    df["symbol"] = df["symbol"].str.replace("USDT", "")
    df = df[["标签", "symbol", "期间收益"]].rename(columns={"symbol": "代币名字"})

    period_label = f"{hours}h" if hours < 24 else f"{hours // 24}d"
    header = f"最近{period_label}（{label}）强势标的"
    table = ascii_table(df)
    send_message(f"{header}\n```\n{table}\n```", chat_id, parse_mode="Markdown")


# ---------------------------------------------------------------------------
# Telegram update handling
# ---------------------------------------------------------------------------

def fetch_updates(offset: int) -> list[dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(
            url,
            params={"timeout": 10, "offset": offset},
            timeout=15,
            proxies=get_proxy_dict(),
        )
        data = resp.json()
        if not data.get("ok"):
            return []
        return data.get("result", [])
    except Exception:
        return []


def handle_update(upd: dict) -> int:
    if "message" not in upd:
        return upd.get("update_id", 0)
    msg = upd["message"]
    text_msg = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")

    if text_msg.startswith("/4h"):
        strong_assets_command(chat_id, 4)
    elif text_msg.startswith("/12h"):
        strong_assets_command(chat_id, 12)
    elif text_msg.startswith("/1d"):
        strong_assets_command(chat_id, 24)
    elif text_msg.startswith("/7d"):
        strong_assets_command(chat_id, 168)
    return upd.get("update_id", 0)


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main() -> None:
    send_message("monitor bot started")
    last_update = 0
    while True:
        updates = fetch_updates(last_update + 1)
        for upd in updates:
            last_update = handle_update(upd)
        time.sleep(5)


if __name__ == "__main__":
    main()
