#!/usr/bin/env python3
import os
import sys
import time
import requests
import psycopg2
from datetime import datetime, timezone
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

# 加载环境变量
load_dotenv()
API_KEY = os.getenv("CG_API_KEY")
if not API_KEY:
    sys.exit("请在 .env 中配置 CG_API_KEY")

# 数据库配置
DB_CFG = {
    "host":     os.getenv("DB_HOST", "127.0.0.1"),
    "port":     os.getenv("DB_PORT", "5432"),
    "dbname":   os.getenv("DB_NAME", "postgres"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# 建表
def ensure_table():
    create_sql = '''
CREATE TABLE IF NOT EXISTS ohlcv (
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
    cur.execute(create_sql)
    cur.close()
    conn.close()

# 从 instruments 表获取 symbols
def get_symbols_from_db():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    # 首次运行 ohlcv 表为空，故改为从 instruments 表读取可用 symbol 列表
    cur.execute("SELECT instrument_id FROM instruments;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols

# 构造API URL，使用 start_time/end_time 和 limit 参数
def build_url(symbol, start_ts, end_ts):
    base = "https://open-api-v4.coinglass.com/api/futures/price/history"
    return (
        f"{base}"
        f"?exchange=Binance"
        f"&symbol={symbol}"
        f"&interval=15m"
        f"&limit=4500"
        f"&start_time={start_ts}"
        f"&end_time={end_ts}"
    )

# 拉取单个symbol数据，使用 Decimal 保持数值精度
def fetch_ohlcv(symbol, start_ts, end_ts):
    url = build_url(symbol, start_ts, end_ts)
    headers = {"CG-API-KEY": API_KEY, "accept": "application/json"}
    try:
        resp = requests.get(url, headers=headers, timeout=30)
        data = resp.json()
        if data.get("code") != "0":
            print(f"API Error ({symbol}): {data.get('msg')}")
            return []
        return [
            (
                symbol,
                int(d["time"]),
                Decimal(d["open"]),
                Decimal(d["high"]),
                Decimal(d["low"]),
                Decimal(d["close"]),
                Decimal(d["volume_usd"])
            ) for d in data.get("data", [])
        ]
    except Exception as e:
        print(f"Request error ({symbol}): {e}")
        return []

# 插入数据并逐条日志
def insert_data(data):
    insert_sql = '''
INSERT INTO ohlcv (symbol, time, open, high, low, close, volume_usd)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (symbol, time) DO NOTHING;
'''
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    inserted = 0
    for record in data:
        try:
            cur.execute(insert_sql, record)
            if cur.rowcount > 0:
                inserted += 1
                print(f"写入: symbol={record[0]}, time={record[1]}")
        except Exception as e:
            print(f"插入出错: {e} -- record: {record}")
    conn.commit()
    cur.close()
    conn.close()
    return inserted

# 主函数：并发拉取、写入并输出日志
def main(symbols, start_ts, end_ts):
    ensure_table()
    total_inserted = 0
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = {}
        for symbol in symbols:
            futures[executor.submit(fetch_ohlcv, symbol, start_ts, end_ts)] = symbol
            time.sleep(0.3)

        for future in as_completed(futures):
            symbol = futures[future]
            data = future.result()
            if data:
                inserted_rows = insert_data(data)
                total_inserted += inserted_rows
                print(f"{symbol}: 请求 {len(data)} 条，写入 {inserted_rows} 条")
            else:
                print(f"{symbol}: 无数据写入")
    print(f"\n总共插入 {total_inserted} 条数据")

if __name__ == "__main__":
    # 对齐到最近的 15 分钟整点
    interval_ms = 15 * 60 * 1000
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ts = (now_ms // interval_ms) * interval_ms
    # 拉取过去 24 小时的数据；若只要上一个完整 K 线，可改为 start_ts = end_ts - interval_ms
    start_ts = end_ts - 24 * 60 * 60 * 1000

    symbols = get_symbols_from_db()
    if not symbols:
        sys.exit("没有可用的 symbol，请先确认 instruments 表中有数据。")
    main(symbols, start_ts, end_ts)
