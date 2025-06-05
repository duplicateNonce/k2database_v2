#!/usr/bin/env python3
"""Send 4h AI summaries for top assets to Telegram."""

from __future__ import annotations

import os
import time
from pathlib import Path

import pandas as pd
import requests

from test1hstrong import (
    get_latest_ts,
    fetch_range,
    hourly_rank,
    get_labels_map,
    format_ascii_table,
)
from prompt_manager import get_prompt
from grok_api import ask_xai
from config import load_proxy_env, get_proxy_dict, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Ensure proxy variables are loaded
load_proxy_env()
PROXIES = get_proxy_dict()


def aggregate_stats(ranks: pd.DataFrame, top_rank: int = 20) -> pd.DataFrame:
    """Aggregate ranking statistics with ``top_rank`` threshold."""
    if ranks.empty:
        return pd.DataFrame(
            columns=["symbol", "times", "avg_percentile", "median_rank"]
        )

    pivot_rank = ranks.pivot(index="symbol", columns="time", values="rank")
    pivot_pct = ranks.pivot(index="time", columns="symbol", values="pct")

    times = (pivot_rank <= top_rank).sum(axis=1)

    max_rank_by_time = ranks.groupby("time")["rank"].max()
    pivot_rank = pivot_rank.apply(lambda col: col.fillna(max_rank_by_time[col.name] + 1))

    median_rank = pivot_rank.median(axis=1)

    df_percentile = pd.DataFrame(index=pivot_pct.index, columns=pivot_pct.columns, dtype=float)
    N = pivot_pct.shape[1]
    for h in pivot_pct.index:
        ranks_series = pivot_pct.loc[h].rank(ascending=False, method="first", na_option="bottom")
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

    stats = stats.sort_values(["times", "avg_percentile"], ascending=[False, False])
    return stats.reset_index(drop=True)


def send_telegram(text: str, parse_mode: str | None = None) -> None:
    """Send ``text`` to Telegram or print a notice when not configured."""
    token = TELEGRAM_BOT_TOKEN
    chat_id = TELEGRAM_CHAT_ID
    if not token or not chat_id:
        print("Telegram not configured, message below:\n" + text)
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        resp = requests.post(url, json=payload, timeout=10, proxies=PROXIES or None)
        if resp.status_code != 200:
            print("Failed to send telegram message:", resp.text)
    except Exception as exc:
        print("Failed to send telegram message:", exc)


START_FILE = Path(".tgaisum_started")


def top_assets(limit: int = 5) -> pd.DataFrame:
    """Return top ``limit`` assets ranked by avg_percentile over last 4h."""
    end_ts = get_latest_ts()
    start_ts = end_ts - 3 * 3600 * 1000
    df = fetch_range(start_ts, end_ts)
    ranks = hourly_rank(df)
    stats = aggregate_stats(ranks, top_rank=20)
    if stats.empty:
        return stats
    labels_map = get_labels_map()
    stats["label"] = stats["symbol"].map(
        lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
    )
    cols = ["symbol", "label"] + [c for c in stats.columns if c not in ("symbol", "label")]
    stats = stats[cols]
    return stats.sort_values("avg_percentile", ascending=False).head(limit)


def print_telegram_config() -> None:
    """Print Telegram bot token and chat ID for debugging."""
    print("TELEGRAM_BOT_TOKEN:", TELEGRAM_BOT_TOKEN)
    print("TELEGRAM_CHAT_ID:", TELEGRAM_CHAT_ID)


def main() -> None:
    print_telegram_config()
    if not START_FILE.exists():
        send_telegram("4小时级别ai定时推送已启动")
        START_FILE.touch()

    df = top_assets()
    if df.empty:
        return

    table_df = df[["symbol", "label", "avg_percentile"]].copy()
    table_df["symbol"] = table_df["symbol"].str.replace("USDT", "")
    table_df["avg_percentile"] = table_df["avg_percentile"].map(lambda x: f"{x:.2f}")
    table = format_ascii_table(table_df)
    send_telegram(f"```\n{table}\n```")

    parts = []
    for _, row in df.iterrows():
        symbol = row["symbol"]
        label = row["label"] or "无"
        search_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        _, template = get_prompt("tgaisum")
        prompt = template.format(search_symbol=search_symbol, label=label)
        try:
            answer = ask_xai(prompt)
        except Exception as exc:
            answer = f"查询失败: {exc}"
        parts.append(f"<b>{symbol}</b>\n{answer}")

    for part in parts:
        send_telegram(part, parse_mode="HTML")
        time.sleep(1)


if __name__ == "__main__":
    main()
