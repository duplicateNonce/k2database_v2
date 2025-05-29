import os
import requests
from googletrans import Translator

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

if __name__ == "__main__":
    import sys, json
    if len(sys.argv) < 2:
        print("Usage: python grok_search.py [query]")
        sys.exit(1)
    query = " ".join(sys.argv[1:])
    data = live_search(query)
    print(json.dumps(data, ensure_ascii=False, indent=2))
