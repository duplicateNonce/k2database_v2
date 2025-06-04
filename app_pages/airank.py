import os
import json
import time
from pathlib import Path

import pandas as pd
import psycopg2
import requests
import streamlit as st

from config import secret_get, load_proxy_env, get_proxy_dict

# Database config same as pct_change_rank.py
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
    AI_CACHE_FILE.write_text(json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8")


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
        raise RuntimeError("ohlcv_1h 表无数据")
    return int(res[0])


def fetch_range(start_ts: int, end_ts: int) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CFG)
    sql = (
        "SELECT symbol, time, open, close FROM ohlcv_1h "
        "WHERE time BETWEEN %s AND %s"
    )
    df = pd.read_sql(sql, conn, params=(start_ts, end_ts))
    conn.close()
    return df


def hourly_rank(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return pd.DataFrame(columns=["time", "symbol", "pct", "rank"])

    df["pct"] = (df["close"] - df["open"]) / df["open"] * 100
    df["rank"] = df.groupby("time")["pct"].rank(ascending=False, method="min").astype(int)
    df = df.sort_values(["time", "rank"])
    return df[["time", "symbol", "pct", "rank"]]


def aggregate_stats(ranks: pd.DataFrame, top_n: int = 40) -> pd.DataFrame:
    if ranks.empty:
        return pd.DataFrame(columns=["symbol", "times", "avg_percentile", "median_rank"])

    pivot_rank = ranks.pivot(index="symbol", columns="time", values="rank")
    pivot_pct = ranks.pivot(index="time", columns="symbol", values="pct")

    times = (pivot_rank <= top_n).sum(axis=1)

    max_rank_by_time = ranks.groupby("time")["rank"].max()
    pivot_rank = pivot_rank.apply(lambda col: col.fillna(max_rank_by_time[col.name] + 1))
    median_rank = pivot_rank.median(axis=1)

    df_percentile = pd.DataFrame(index=pivot_pct.index, columns=pivot_pct.columns, dtype=float)
    N = pivot_pct.shape[1]
    for h in pivot_pct.index:
        ranks_series = pivot_pct.loc[h].rank(ascending=False, method="first", na_option="bottom")
        df_percentile.loc[h] = (N - ranks_series) / (N - 1)

    avg_percentile = df_percentile.mean(axis=0)

    stats = pd.DataFrame({
        "symbol": avg_percentile.index,
        "times": times.reindex(avg_percentile.index).fillna(0).astype(int),
        "avg_percentile": avg_percentile,
        "median_rank": median_rank.reindex(avg_percentile.index),
    })

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
        f"你是一个高级crypto交易者和分析师，行为经济学博士。针对资产 {search_symbol}（包括其常见名称、别名或全称、项目名称、search_symbol后接USDT），搜索各类网站以及X平台上的全语言内容，请用中文回答，省略风险提示：\n"
        f"1. 过去一个月哪些新闻、基本面变化或重要人物(如以太坊/sol等基金会的核心人物/founder）的观点影响了 {label or '无'} 板块的价格和估值？请100字内概括并给出评分；\n"
        f"2. 基于 X 平台内容，总结本周（截止今天）影响 {search_symbol} 价格的事件，100字内；\n"
        f"3. 根据 X 上评论、点赞和转发统计本周（截止今天）散户对 {search_symbol} 的情绪，着重突出变化，尤其是今天的情绪和之前情绪的区别。若有大V观点（比如著名KOL）请单独说明，100字内并给出 0-100 的评分；\n"
        f"4. 汇总近一周（截止今天）技术分析博主对 {search_symbol} 的观点，列出阻力位、支撑位等关键指标，200字内概括。"
    )
    answer = ask_xai(prompt)
    cache[symbol] = {"hour": hour_key, "answer": answer}
    _save_ai_cache(cache)
    return answer


# ---- Streamlit page ----

def render_ai_rank_page():
    st.title("涨幅归因")

    with st.spinner("计算排名中..."):
        end_ts = get_latest_ts()
        start_ts = end_ts - 23 * 3600 * 1000
        df = fetch_range(start_ts, end_ts)
        ranks = hourly_rank(df)
        stats = aggregate_stats(ranks)

    if stats.empty:
        st.warning("指定区间没有数据")
        return

    labels_map = get_labels_map()
    stats["标签"] = stats["symbol"].map(lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else "")
    stats["avg_percentile"] = (stats["avg_percentile"] * 100).round(2)
    stats = stats.rename(columns={"symbol": "代币名字", "avg_percentile": "平均百分位(%)"})

    top_df = stats.sort_values("平均百分位(%)", ascending=False).head(5)
    st.subheader("前五名 (按平均百分位)")
    st.dataframe(top_df[["标签", "代币名字", "平均百分位(%)", "times", "median_rank"]].reset_index(drop=True), use_container_width=True)

    st.subheader("AI 分析")
    symbol = st.selectbox("选择代币", stats["代币名字"])
    if st.button("AI分析"):
        with st.spinner("分析中..."):
            label = "，".join(labels_map.get(symbol, []))
            try:
                answer = get_ai_summary(symbol, label)
                st.write(answer)
            except Exception as exc:
                st.error(f"查询失败: {exc}")
