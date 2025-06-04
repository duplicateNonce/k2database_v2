#!/usr/bin/env python3
"""Summarize top hourly strong assets using Grok.

This script combines the ranking logic from ``test1hstrong.py`` with
Grok search queries.  After computing the latest hourly strong assets it
asks Grok for a summary of each top asset's sector status, social media
discussion, price influencing events and retail sentiment.
"""

from __future__ import annotations

import os
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


def ask_xai(prompt: str) -> str:
    """Query Grok with ``prompt`` and return the reply text."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('XAI_API_KEY')}",
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "model": MODEL,
        "search_parameters": {"mode": "auto", "return_citations": True},
    }
    resp = requests.post(
        API_URL, headers=headers, json=payload, proxies=PROXIES or None, timeout=15
    )
    resp.raise_for_status()
    data = resp.json()
    try:
        return data["choices"][0]["message"]["content"].strip()
    except Exception:
        return str(data)


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
        prompt = (
            f"请用中文概述以下内容：\n"
            f"1. 当前 {label} 板块的整体情况；\n"
            f"2. {symbol} 在社交媒体上的讨论情况；\n"
            f"3. 近期影响 {symbol} 价格的事件；\n"
            f"4. 散户投资者对 {symbol} 的情绪。"
        )
        print(f"\n==== {symbol} ====")
        try:
            answer = ask_xai(prompt)
            print(answer)
        except Exception as exc:
            print(f"查询失败: {exc}")


if __name__ == "__main__":
    main()
