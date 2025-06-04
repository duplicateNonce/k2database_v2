#!/usr/bin/env python3
"""Summarize top hourly strong assets using Grok.

This script combines the ranking logic from ``test1hstrong.py`` with
Grok search queries.  After computing the latest hourly strong assets it
asks Grok for a summary of each top asset's sector status, social media
discussion, price influencing events and retail sentiment.
"""

from __future__ import annotations

import os
import time
import requests
import pandas as pd

from test1hstrong import (
    get_latest_ts,
    fetch_range,
    hourly_rank,
    aggregate_stats,
    get_labels_map,
)
from config import load_proxy_env, get_proxy_dict

# Ensure proxy variables are applied for requests
load_proxy_env()
PROXIES = get_proxy_dict()

API_URL = "https://api.x.ai/v1/chat/completions"
MODEL = "grok-3-latest"


def ask_xai(prompt: str, retries: int = 1, timeout: int = 30) -> str:
    """Query Grok with ``prompt`` and return the reply text.

    ``retries`` controls how many additional attempts are made if the
    request times out or fails.
    """
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('XAI_API_KEY')}",
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "model": MODEL,
        "search_parameters": {"mode": "auto", "return_citations": True},
    }
    for _ in range(retries + 1):
        try:
            resp = requests.post(
                API_URL,
                headers=headers,
                json=payload,
                proxies=PROXIES or None,
                timeout=timeout,
            )
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"].strip()
        except Exception:
            if not retries:
                raise
            retries -= 1
            time.sleep(2)


def top_assets(limit: int = 5) -> pd.DataFrame:
    """Return top ``limit`` assets ranked by ``avg_percentile``."""
    end_ts = get_latest_ts()
    start_ts = end_ts - 23 * 3600 * 1000
    df = fetch_range(start_ts, end_ts)
    ranks = hourly_rank(df)
    stats = aggregate_stats(ranks)
    if stats.empty:
        return stats
    labels_map = get_labels_map()
    stats["label"] = stats["symbol"].map(
        lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
    )
    cols = ["symbol", "label"] + [c for c in stats.columns if c not in ("symbol", "label")]
    stats = stats[cols]
    return stats.sort_values("avg_percentile", ascending=False).head(limit)


def main() -> None:
    df = top_assets()
    if df.empty:
        print("No data available")
        return
    print("Top assets:\n", df[["symbol", "label"]].to_string(index=False))
    for _, row in df.iterrows():
        symbol = row["symbol"]
        label = row["label"] or "无"
        search_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
        search_symbol = f"${search_symbol}"
        prompt = (
            f"请用中文概述以下内容：\n"
            f"1. 当前 {label} 板块的整体情况；\n"
            f"2. {search_symbol} 在社交媒体上的讨论情况；\n"
            f"3. 近期影响 {search_symbol} 价格的事件；\n"
            f"4. 散户投资者对 {search_symbol} 的情绪。"
        )
        print(f"\n==== {symbol} ====")
        try:
            answer = ask_xai(prompt)
            print(answer)
        except Exception as exc:
            print(f"查询失败: {exc}")


if __name__ == "__main__":
    main()
