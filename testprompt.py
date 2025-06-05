#!/usr/bin/env python3
"""Test Grok 3 API prompt for VIRTUALUSDT with news, sentiment, and technical analysis."""

import asyncio
import logging
import os
import re
import requests
from datetime import datetime
from config import load_proxy_env, get_proxy_dict

# Configure logging
logging.basicConfig(
    filename="testprompt.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# Load proxy configuration
load_proxy_env()
PROXIES = get_proxy_dict()

# API configuration
API_URL = "https://api.x.ai/v1/chat/completions"
MODEL = "grok-3-latest"

def ask_xai(prompt: str, retries: int = 3, timeout: int = 20) -> str:
    """Query Grok 3 API with retries and timeout."""
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {os.getenv('XAI_API_KEY')}",
    }
    payload = {
        "messages": [{"role": "user", "content": prompt}],
        "model": MODEL,
        "search_parameters": {
            "mode": "DeepSearch",
            "platform": "X",
            "max_search_results": 50,
            "date_range": {"start": "2025-05-30", "end": "2025-06-05"}
        }
    }
    for attempt in range(retries + 1):
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
            response = data["choices"][0]["message"]["content"].strip()
            logging.info(f"API response: {response}")
            return response
        except Exception as exc:
            if attempt == retries:
                logging.error(f"API request failed after {retries} retries: {exc}")
                return f"查询失败: {exc}"
            logging.warning(f"API request failed, retrying... ({exc})")
            asyncio.sleep(2)
    return "查询失败: 未知错误"

def validate_response(response: str, start_date: str = "2025-05-30", end_date: str = "2025-06-05") -> bool:
    """Validate that response dates are within the specified range."""
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", response)
    return all(start_date <= date <= end_date for date in dates) or not dates

async def main():
    """Test Grok 3 API prompt for VIRTUALUSDT."""
    symbol = "VIRTUALUSDT"
    search_symbol = symbol[:-4] if symbol.endswith("USDT") else symbol
    prompt = f"""
    你是一个高级加密货币交易者和分析师，行为经济学博士，精通实时市场分析。针对资产 {search_symbol}（包括其常见名称、别名或全称、项目名称、{search_symbol}USDT），执行以下任务，数据必须严格限于2025-05-30至2025-06-05（JST）（新闻除外，允许2025-05-07至2025-06-05），优先使用X平台最新帖子，结合CoinGecko或CoinMarketCap获取技术指标。回答需简洁、准确，中文，省略风险提示：
    1. 过去30天（2025-05-07至2025-06-05）影响{search_symbol}价格的新闻、基本面变化或重要人物（如基金会成员、KOL）观点，100字内，评分0-100。
    2. 本周（2025-05-30至2025-06-05）X平台上影响{search_symbol}价格的关键事件，100字内。
    3. 本周（2025-05-30至2025-06-05）X平台散户情绪分析，突出今日（2025-06-05 14:58 JST）与昨日情绪变化，基于评论、点赞、转发统计，注明KOL观点，100字内，评分0-100。
    4. 汇总X平台上2025-05-30至2025-06-05技术分析博主对{search_symbol}的观点，列出：
       - 阻力位和支撑位（精确到4位小数）。
       - RSI（14周期，当前值及趋势）。
       - MACD（12,26,9，当前状态）。
       - 其他指标（如布林带、斐波那契回撤）。
       引用X帖子（用户名、日期），若无足够数据，基于SOL生态推断，200字内，优先最新、最详细分析。
    示例（第4部分）：
    - 阻力位：0.XXXX、0.XXXX；支撑位：0.XXXX、0.XXXX
    - RSI：XX.X（超买/中性/超卖）
    - MACD：看涨/看跌交叉
    - 来源：@用户名，YYYY-MM-DD
    """

    # Query API
    answer = ask_xai(prompt)

    # Validate response for time-sensitive sections (2, 3, 4)
    if not validate_response(answer):
        logging.warning(f"Response for {symbol} contains outdated data: {answer}")
        answer = ask_xai(prompt)  # Retry once

    # Print result to console
    print(f"<b>{symbol}</b>\n{answer}")

if __name__ == "__main__":
    asyncio.run(main())
