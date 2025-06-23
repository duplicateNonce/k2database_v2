#!/usr/bin/env python3
import os
import sys
import time
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from config import secret_get
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

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


def build_url(symbol: str, start_ts: int, end_ts: int) -> str:
    """构造 API 请求 URL"""
    return (
        f"https://open-api-v4.coinglass.com/api/futures/price/history"
        f"?exchange=Binance"
        f"&symbol={symbol}"
        f"&interval=4h"
        f"&limit=1500"
        f"&start_time={start_ts}"
        f"&end_time={end_ts}"
    )


def fetch_ohlcv(symbol: str, start_ts: int, end_ts: int):
    """拉取指定交易对的 OHLCV"""
    url = build_url(symbol, start_ts, end_ts)
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


def process_symbol(symbol: str, start_ts: int, end_ts: int, tz8) -> int:
    """处理单个交易对：下载并写入数据"""
    try:
        records = fetch_ohlcv(symbol, start_ts, end_ts)
    except Exception as e:
        print(f"{symbol} 请求失败: {e}", flush=True)
        return 0
    n = len(records)
    if n == 0:
        print(f"{symbol} 请求0条数据 写入0条数据", flush=True)
        return 0
    written = insert_data(records)
    last_ts = max(r[1] for r in records)
    last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).astimezone(tz8)
    last_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{symbol} 请求{n}条数据 成功写入{written}条数据 最新时间为{last_str}", flush=True)
    return written


def main() -> None:
    ensure_table()
    tz8 = timezone(timedelta(hours=8))
    # 计算时间段，按4小时对齐
    interval = 4 * 3600 * 1000
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ts = (now // interval) * interval
    start_ts = end_ts - interval * 4500

    symbols = get_symbols_from_db()
    if not symbols:
        print("没有 instrument，请检查 instruments 表。", flush=True)
        sys.exit(1)

    total = 0
    with ThreadPoolExecutor(max_workers=1) as executor:
        futures = [executor.submit(process_symbol, sym, start_ts, end_ts, tz8) for sym in symbols]
        for future in as_completed(futures):
            total += future.result()

    print(f"\n总共写入 {total} 条数据", flush=True)


if __name__ == "__main__":
    main()
