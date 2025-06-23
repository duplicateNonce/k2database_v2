#!/usr/bin/env python3
import os
import sys
import time
import threading
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from config import secret_get
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal
from tqdm import tqdm

# 请求时间记录，用于速率限制
REQ_TIMESTAMPS = []
MAX_REQS_PER_MIN = 79
REQ_LOCK = threading.Lock()

# API request limit
LIMIT = 1500

# 加载环境变量并校验
load_dotenv()
API_KEY = secret_get("CG_API_KEY")
if not API_KEY:
    print("请在 .env 中配置 CG_API_KEY", flush=True)
    sys.exit(1)

# 数据库配置，默认连接到 ohlcv_4h 库
DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "ohlcv_4h"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}


def wait_rate_limit() -> None:
    """Ensure request rate within limit."""
    with REQ_LOCK:
        now = time.time()
        REQ_TIMESTAMPS[:] = [t for t in REQ_TIMESTAMPS if now - t < 60]
        if len(REQ_TIMESTAMPS) >= MAX_REQS_PER_MIN:
            print(f"达到每分钟{MAX_REQS_PER_MIN}次请求上限，暂停10秒", flush=True)
            time.sleep(10)
            now = time.time()
            REQ_TIMESTAMPS[:] = [t for t in REQ_TIMESTAMPS if now - t < 60]
        REQ_TIMESTAMPS.append(time.time())


def ensure_table():
    """确保 ohlcv_4h 表存在"""
    sql = '''
CREATE TABLE IF NOT EXISTS ohlcv_4h (
    symbol TEXT NOT NULL,
    time BIGINT NOT NULL,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume_usd NUMERIC,
    PRIMARY KEY(symbol, time)
);
'''
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print("表结构已确保", flush=True)


def get_symbols_from_db():
    """从 instruments 表读取所有 instrument_id"""
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT instrument_id FROM instruments;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols


def get_latest_db_ts() -> int | None:
    """Return the newest ``time`` across all records or ``None`` when empty."""
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT MAX(time) FROM ohlcv_4h")
    row = cur.fetchone()
    cur.close()
    conn.close()
    return int(row[0]) if row and row[0] is not None else None


def get_symbol_latest_ts(symbol: str) -> int | None:
    """Return latest timestamp for a symbol."""
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT MAX(time) FROM ohlcv_4h WHERE symbol=%s", (symbol,))
    row = cur.fetchone()
    cur.close()
    conn.close()
    return int(row[0]) if row and row[0] is not None else None


def build_url(symbol: str, start_ts: int, end_ts: int) -> str:
    """构造 API 请求 URL"""
    return (
        f"https://open-api-v4.coinglass.com/api/futures/price/history"
        f"?exchange=Binance"
        f"&symbol={symbol}"
        f"&interval=4h"
        f"&limit={LIMIT}"
        f"&start_time={start_ts}"
        f"&end_time={end_ts}"
    )




def fetch_ohlcv(symbol: str, start_ts: int, end_ts: int):
    """拉取指定交易对的 OHLCV，包含简单重试和速率限制"""

    url = build_url(symbol, start_ts, end_ts)
    for attempt in range(3):
        wait_rate_limit()
        try:
            resp = requests.get(
                url,
                headers={"CG-API-KEY": API_KEY, "accept": "application/json"},
                timeout=30,
            )
            data = resp.json()
            if data.get("code") != "0":
                raise RuntimeError(data.get("msg"))
            return [(
                symbol,
                int(item["time"]),
                Decimal(item["open"]),
                Decimal(item["high"]),
                Decimal(item["low"]),
                Decimal(item["close"]),
                Decimal(item["volume_usd"]),
            ) for item in data.get("data", [])]
        except Exception as exc:
            if attempt == 2:
                raise
            print(f"{symbol} 请求失败尝试重试: {exc}", flush=True)
            time.sleep(1)


def insert_data(records) -> int:
    """插入记录，返回成功写入数量"""
    sql = '''
INSERT INTO ohlcv_4h(symbol, time, open, high, low, close, volume_usd)
VALUES(%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT(symbol, time) DO NOTHING;
'''
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    count = 0
    for rec in records:
        try:
            cur.execute(sql, rec)
            if cur.rowcount:
                count += 1
        except Exception as e:
            print(f"{rec[0]} 插入失败: {e}", flush=True)
    conn.commit()
    cur.close()
    conn.close()
    return count


def process_symbol(symbol: str, end_ts: int, tz8, interval: int) -> tuple[int, int]:
    """处理单个交易对：下载并写入数据"""
    latest = get_symbol_latest_ts(symbol)
    if latest is not None and latest >= end_ts:
        print(f"{symbol} 数据已是最新，跳过", flush=True)
        return 0, 0

    if latest is None:
        start_ts = end_ts - interval * LIMIT
    else:
        start_ts = latest + interval
        min_start = end_ts - interval * LIMIT
        if start_ts < min_start:
            start_ts = min_start

    if start_ts > end_ts:
        print(f"{symbol} 数据已是最新，跳过", flush=True)
        return 0, 0

    try:
        records = fetch_ohlcv(symbol, start_ts, end_ts)
    except Exception as e:
        print(f"{symbol} 请求失败: {e}", flush=True)
        return 0, 1
    n = len(records)
    if n == 0:
        print(f"{symbol} 请求0条数据 写入0条数据", flush=True)
        return 0, 0
    written = insert_data(records)
    last_ts = max(r[1] for r in records)
    last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).astimezone(tz8)
    last_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{symbol} 请求{n}条数据 成功写入{written}条数据 最新时间为{last_str}", flush=True)
    return written, 0


def main() -> None:
    ensure_table()
    tz8 = timezone(timedelta(hours=8))
    # 计算时间段，按4小时对齐
    interval = 4 * 3600 * 1000
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    # last completed 4h period
    end_ts = (now_ms // interval) * interval - interval

    symbols = get_symbols_from_db()
    if not symbols:
        print("没有 instrument，请检查 instruments 表。", flush=True)
        sys.exit(1)

    total = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {executor.submit(process_symbol, sym, end_ts, tz8, interval): sym for sym in symbols}
        with tqdm(total=len(symbols)) as bar:
            for fut in as_completed(futures):
                written, fail = fut.result()
                total += written
                failed += fail
                bar.update(1)
                bar.set_postfix(written=total, failed=failed)

    print(f"\n总共写入 {total} 条数据，失败 {failed} 个symbol", flush=True)


if __name__ == "__main__":
    main()
