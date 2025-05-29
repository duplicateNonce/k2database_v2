import os
import requests
from googletrans import Translator
from datetime import date
from urllib.parse import quote_plus

GROK_API_KEY = os.getenv("GROK_API_KEY")
API_URL = "https://api.grok.x.ai/v1/search"  # Placeholder

def live_search(query: str, limit: int = 5) -> dict:
    """Perform a live search using Grok's API.

    Parameters
    ----------
    query: str
        Search query.
    limit: int, default 5
        Maximum number of results returned.
    Returns
    -------
    dict
        Parsed JSON response from the API.
    """
    if not GROK_API_KEY:
        raise RuntimeError("GROK_API_KEY not configured")
    headers = {"Authorization": f"Bearer {GROK_API_KEY}"}
    payload = {"query": query, "limit": limit}
    resp = requests.post(API_URL, json=payload, headers=headers, timeout=10)
    resp.raise_for_status()
    return resp.json()


def live_search_summary(query: str, limit: int = 10) -> str:
    """Search Grok and return a Chinese summary of the results."""
    data = live_search(query, limit=limit)
    items = []
    for item in data.get("results", []):
        title = item.get("title", "")
        snippet = item.get("snippet") or item.get("text", "")
        text = f"{title}: {snippet}" if snippet else title
        if text:
            items.append(text)
    summary_en = "\n".join(items)
    if not summary_en:
        return "未找到相关信息"
    try:
        zh = Translator().translate(summary_en, dest="zh-cn").text
        return zh
    except Exception:
        # 如果翻译失败，直接返回英文摘要
        return summary_en


def build_x_search_url(instrument_id: str, start: date, end: date) -> str:
    """Return X advanced search URL for a coin instrument."""
    if instrument_id.endswith("USDT"):
        coin = instrument_id[:-4]
    else:
        coin = instrument_id
    q = (
        f"{coin} min_replies:5 min_faves:5 min_retweets:2 "
        f"until:{end.isoformat()} since:{start.isoformat()}"
    )
    return "https://x.com/search?f=live&q=" + quote_plus(q) + "&src=typed_query"


def x_search_summary(instrument_id: str, start: date, end: date, limit: int = 10) -> tuple[str, str]:
    """Summarize X search results for the given instrument and period."""
    url = build_x_search_url(instrument_id, start, end)
    summary = live_search_summary(url, limit=limit)
    return url, summary


def bubble_market_summary(limit: int = 10) -> str:
    """Fetch cryptobubbles data and return a summary via Grok."""
    url = "https://cryptobubbles.net/backend/data/bubbles1000.usd.json"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    data = resp.text
    # Pass raw JSON to Grok for summarization
    return live_search_summary(data, limit=limit)

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: python grok_search.py [query]")
        sys.exit(1)
    query = " ".join(sys.argv[1:])
    data = live_search(query)
    print(json.dumps(data, ensure_ascii=False, indent=2))
