import os
import json
import time
from pathlib import Path
import re

import pandas as pd
import psycopg2
import requests
import streamlit as st

from config import secret_get, load_proxy_env, get_proxy_dict

DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "ohlcv_1h"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}

load_proxy_env()
PROXIES = get_proxy_dict()

API_URL = "https://api.x.ai/v1/chat/completions"
MODEL = "grok-3-latest"

AI_CACHE_FILE = Path("data/airank_cache.json")


def _load_ai_cache() -> dict:
    if AI_CACHE_FILE.exists():
        try:
            return json.loads(AI_CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_ai_cache(cache: dict) -> None:
    AI_CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    AI_CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def ask_xai(prompt: str, retries: int = 1, timeout: int = 30) -> str:
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


# ---- DB helpers copied from pct_change_rank.py ----


def get_labels_map() -> dict[str, list]:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT instrument_id, labels FROM instruments")
    rows = cur.fetchall()
    cur.close()
    conn.close()

    labels = {}
    for sym, lbl in rows:
        if isinstance(lbl, list):
            labels[sym] = lbl
        elif lbl is None:
            labels[sym] = []
        else:
            labels[sym] = [str(lbl)]
    return labels


def get_latest_ts() -> int:
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT MAX(time) FROM ohlcv_1h")
    res = cur.fetchone()
    cur.close()
    conn.close()
    if not res or res[0] is None:
        raise RuntimeError("ohlcv_1h \u8868\u65e0\u6570\u636e")
    return int(res[0])


def fetch_range(start_ts: int, end_ts: int) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CFG)
    sql = "SELECT symbol, time, open, close FROM ohlcv_1h WHERE time BETWEEN %s AND %s"
    df = pd.read_sql(sql, conn, params=(start_ts, end_ts))
    conn.close()
    return df


def hourly_rank(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "pct", "rank"])

    df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["rank"] = (
        df.groupby("time")["pct"].rank(ascending=False, method="min").astype(int)
    )
    df = df.sort_values(["time", "rank"])
    return df[["time", "symbol", "pct", "rank"]]


def aggregate_stats(ranks: pd.DataFrame, top_n: int = 40) -> pd.DataFrame:
    if ranks.empty:
        return pd.DataFrame(
            columns=["symbol", "times", "avg_percentile", "median_rank"]
        )

    pivot_rank = ranks.pivot(index="symbol", columns="time", values="rank")
    pivot_pct = ranks.pivot(index="time", columns="symbol", values="pct")

    times = (pivot_rank <= top_n).sum(axis=1)

    max_rank_by_time = ranks.groupby("time")["rank"].max()
    pivot_rank = pivot_rank.apply(
        lambda col: col.fillna(max_rank_by_time[col.name] + 1)
    )
    median_rank = pivot_rank.median(axis=1)

    df_percentile = pd.DataFrame(
        index=pivot_pct.index, columns=pivot_pct.columns, dtype=float
    )
    N = pivot_pct.shape[1]
    for h in pivot_pct.index:
        ranks_series = pivot_pct.loc[h].rank(
            ascending=False, method="first", na_option="bottom"
        )
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


# ---- AI summary helpers ----


def get_ai_summary(symbol: str, label: str) -> str:
    cache = _load_ai_cache()
    hour_key = int(time.time() // 3600)
    entry = cache.get(symbol)
    if entry and entry.get("hour") == hour_key:
        return entry.get("answer", "")

    search_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
    prompt = (
        f"\u4f60\u662f\u4e00\u4e2a\u9ad8\u7ea7crypto\u4ea4\u6613\u8005\u548c\u5206\u6790\u5e08\uff0c\u884c\u4e3a\u7ecf\u6d4e\u5b66\u535a\u58eb\u3002\u9488\u5bf9\u8d44\u4ea7 {search_symbol}\uff08\u5305\u62ec\u5176\u5e38\u89c1\u540d\u79f0\u3001\u522b\u540d\u6216\u5168\u79f0\u3001\u9879\u76ee\u540d\u79f0\u3001search_symbol\u540e\u63d0USDT\uff09\uff0c\u641c\u7d22\u5404\u7c7b\u7f51\u7ad9\u4ee5\u53caX\u5e73\u53f0\u4e0a\u7684\u5168\u8bed\u8a00\u5185\u5bb9\uff0c\u8bf7\u7528\u4e2d\u6587\u56de\u7b54\uff0c\u7701\u7565\u98ce\u9669\u63d0\u793a:\n"
        f"1. \u8fc7\u53bb\u4e00\u4e2a\u6708\u54ea\u4e9b\u65b0\u95fb\u3001\u57fa\u672c\u9762\u53d8\u5316\u6216\u91cd\u8981\u4eba\u7269(\u5982\u4ee5\u592a\u574a/sol\u7b49\u57fa\u91d1\u4f1a\u7684\u6838\u5fc3\u4eba\u7269/founder)\u7684\u89c2\u70b9\u5f71\u54cd\u4e86 {label or '无'} \u677f\u5757\u7684\u4ef7\u683c\u548c\u4f30\u503c\uff1f\u8bf7100\u5b57\u5185\u6982\u62ec\u5e76\u7ed9\u51fa\u8bc4\u5206;\n"
        f"2. \u57fa\u4e8e X \u5e73\u53f0\u5185\u5bb9\uff0c\u603b\u7ed3\u672c\u5468(\u622a\u6b62\u4eca\u5929)\u5f71\u54cd {search_symbol} \u4ef7\u683c\u7684\u4e8b\u4ef6\uff0c100\u5b57\u5185;\n"
        f"3. \u6839\u636e X \u4e0a\u8bc4\u8bba\u3001\u70b9\u8d5e\u548c\u8f6c\u53d1\u7edf\u8ba1\u672c\u5468(\u622a\u6b62\u4eca\u5929)\u6563\u6237\u5bf9 {search_symbol} \u7684\u60c5\u7eea\uff0c\u7740\u91cd\u7d2f\u51fa\u53d8\u5316\uff0c\u5c24\u5176\u662f\u4eca\u5929\u7684\u60c5\u7eea\u548c\u4e4b\u524d\u60c5\u7eea\u7684\u533a\u522b\u3002\u82e5\u6709\u5927V\u89c2\u70b9(\u6bd4\u5982\u8457\u540dKOL)\u8bf7\u5355\u72ec\u8bf4\u660e\uff0c100\u5b57\u5185\u5e76\u7ed9\u51fa 0-100 \u7684\u8bc4\u5206;\n"
        f"4. \u6c47\u603b\u8fd1\u4e00\u5468(\u622a\u6b62\u4eca\u5929)\u6280\u672f\u5206\u6790\u535a\u4e3b\u5bf9 {search_symbol} \u7684\u89c2\u70b9\uff0c\u5217\u51fa\u963b\u529b\u4f4d\u3001\u652f\u6491\u4f4d\u7b49\u5173\u952e\u6307\u6807\uff0c200\u5b57\u5185\u6982\u62ec."
    )
    answer = ask_xai(prompt)
    cache[symbol] = {"hour": hour_key, "answer": answer}
    _save_ai_cache(cache)
    return answer


def _split_answer(answer: str) -> list[str]:
    """Split Grok answer into sections by leading numbers."""
    if not answer:
        return []
    parts = re.split(r"(?:^|\n)\s*\d+[\.\uff0e]?", answer)
    parts = [p.strip() for p in parts if p.strip()]
    return parts


def display_ai_result(symbol: str, answer: str) -> None:
    """Show AI summary with formatted headings."""
    st.header(symbol)
    sections = _split_answer(answer)
    titles = [
        "\u8fc7\u53bb\u4e00\u4e2a\u6708\u65b0\u95fb\u7b49\u5f71\u54cd",
        "\u672c\u5468\u5f71\u54cd\u4ef7\u683c\u4e8b\u4ef6",
        "\u6563\u6237\u60c5\u7eea\u4e0e\u5927V\u89c2\u70b9",
        "\u6280\u672f\u5206\u6790\u89c2\u70b9",
    ]
    for i, text in enumerate(sections):
        title = titles[i] if i < len(titles) else f"\u7b2c {i + 1} \u90e8\u5206"
        st.markdown(f"**{title}**")
        st.write(text)


# ---- Streamlit page ----


def render_ai_rank_page():
    st.title("\u6da8\u5e45\u5f52\u56e0")

    hours = st.selectbox(
        "\u7edf\u8ba1\u65f6\u957f (\u5c0f\u65f6)",
        [4, 12, 24, 72, 168],
        index=2,
        format_func=lambda x: f"\u6700\u8fd1 {x}h",
    )
    top_n = st.number_input(
        "\u8ba1\u5165\u6b21\u6570\u7684\u524d N \u540d", min_value=1, value=40, step=1
    )

    if st.button("\u8ba1\u7b97\u6392\u540d"):
        end_ts = get_latest_ts()
        start_ts = end_ts - (hours - 1) * 3600 * 1000

        with st.spinner("\u8ba1\u7b97\u4e2d..."):
            df = fetch_range(start_ts, end_ts)
            ranks = hourly_rank(df)
            stats = aggregate_stats(ranks, int(top_n))

        if stats.empty:
            st.warning("\u6307\u5b9a\u533a\u95f4\u6ca1\u6709\u6570\u636e")
            return

        labels_map = get_labels_map()
        stats["\u6807\u7b7e"] = stats["symbol"].map(
            lambda s: "\uff0c".join(labels_map.get(s, [])) if labels_map.get(s) else ""
        )
        stats = stats[
            ["\u6807\u7b7e", "symbol", "times", "avg_percentile", "median_rank"]
        ]
        stats["avg_percentile"] = (stats["avg_percentile"] * 100).round(4)
        stats = stats.rename(
            columns={
                "symbol": "\u4ee3\u5e01\u540d\u5b57",
                "avg_percentile": "\u5e73\u5747\u767e\u5206\u4f4d(%)",
            }
        )

        st.dataframe(stats.reset_index(drop=True), use_container_width=True)

        top_df = stats.sort_values(
            "\u5e73\u5747\u767e\u5206\u4f4d(%)", ascending=False
        ).head(5)
        st.subheader("\u524d\u4e94\u540d AI \u5206\u6790")
        for _, row in top_df.iterrows():
            sym = row["\u4ee3\u5e01\u540d\u5b57"]
            label = row["\u6807\u7b7e"]
            with st.spinner(f"{sym} \u5206\u6790\u4e2d..."):
                try:
                    answer = get_ai_summary(sym, label)
                    display_ai_result(sym, answer)
                except Exception as exc:
                    st.error(f"{sym} \u67e5\u8be2\u5931\u8d25: {exc}")

        st.subheader("\u81ea\u5b9a\u4e49\u6807\u7684 AI \u5206\u6790")
        symbol = st.selectbox(
            "\u9009\u62e9\u4ee3\u5e01",
            stats["\u4ee3\u5e01\u540d\u5b57"],
            key="ai_rank_symbol",
        )
        if st.button("AI\u5206\u6790", key="ai_rank_analyze"):
            with st.spinner("\u5206\u6790\u4e2d..."):
                label = "\uff0c".join(labels_map.get(symbol, []))
                try:
                    answer = get_ai_summary(symbol, label)
                    display_ai_result(symbol, answer)
                except Exception as exc:
                    st.error(f"\u67e5\u8be2\u5931\u8d25: {exc}")
