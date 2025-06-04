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
from config import load_proxy_env, get_proxy_dict, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

# Ensure proxy variables are loaded
load_proxy_env()
PROXIES = get_proxy_dict()

API_URL = "https://api.x.ai/v1/chat/completions"
MODEL = "grok-3-latest"


def ask_xai(prompt: str, retries: int = 1, timeout: int = 30) -> str:
    """Query Grok and return the reply text."""
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
        prompt = (
            f"你是一个高级crypto交易者和分析师，行为经济学博士。针对资产 {search_symbol}（包括其常见名称、别名或全称、项目名称、search_symbol后接USDT），搜索各类网站以及X平台上的全语言内容，请用中文回答，省略风险提示：\n"
            f"1. 过去一个月哪些新闻、基本面变化或重要人物(如以太坊/sol等基金会的核心人物/founder）的观点影响了 {label} 板块的价格和估值？请100字内概括并给出评分；\n"
            f"2. 基于 X 平台内容，总结本周（截止今天）影响 {search_symbol} 价格的事件，100字内；\n"
            f"3. 根据 X 上评论、点赞和转发统计本周（截止今天）散户对 {search_symbol} 的情绪，着重突出变化，尤其是今天的情绪和之前情绪的区别。若有大V观点（比如著名KOL）请单独说明，100字内并给出 0-100 的评分；\n"
            f"4. 汇总近一周（截止今天）技术分析博主对 {search_symbol} 的观点，列出阻力位、支撑位等关键指标，200字内概括。"
        )
        try:
            answer = ask_xai(prompt)
        except Exception as exc:
            answer = f"查询失败: {exc}"
        parts.append(f"<b>{symbol}</b>\n{answer}")
    analysis_msg = "\n\n".join(parts)
    send_telegram(analysis_msg, parse_mode="HTML")


if __name__ == "__main__":
    main()
