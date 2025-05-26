#!/usr/bin/env python3
import os
import sys
import time
import requests
import psycopg2
from datetime import datetime, timezone, timedelta
from dotenv import load_dotenv
from concurrent.futures import ThreadPoolExecutor
from decimal import Decimal

# 加载环境变量并校验
load_dotenv()
API_KEY = os.getenv("CG_API_KEY")
if not API_KEY:
    print("请在 .env 中配置 CG_API_KEY", flush=True)
    sys.exit(1)

# 数据库配置
DB_CFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "postgres"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# 确保 ohlcv 表存在
def ensure_table():
    sql = '''
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
    cur.execute(sql)
    cur.close()
    conn.close()
    print("表结构已确保", flush=True)

# 从 instruments 表读取所有 instrument_id
def get_symbols_from_db():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT instrument_id FROM instruments;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols

# 构造请求 URL

def build_url(symbol, start_ts, end_ts):
    return (
        f"https://open-api-v4.coinglass.com/api/futures/price/history"
        f"?exchange=Binance"
        f"&symbol={symbol}"
        f"&interval=15m"
        f"&limit=4500"
        f"&start_time={start_ts}"
        f"&end_time={end_ts}"
    )

# 拉取 OHLCV

def fetch_ohlcv(symbol, start_ts, end_ts):
    url = build_url(symbol, start_ts, end_ts)
    resp = requests.get(
        url,
        headers={"CG-API-KEY": API_KEY, "accept": "application/json"},
        timeout=30
    )
    data = resp.json()
    if data.get("code") != "0":
        raise RuntimeError(data.get('msg'))
    return [(
        symbol,
        int(item["time"]),
        Decimal(item["open"]),
        Decimal(item["high"]),
        Decimal(item["low"]),
        Decimal(item["close"]),
        Decimal(item["volume_usd"])
    ) for item in data.get("data", [])]

# 插入记录，返回写入数量

def insert_data(records):
    sql = '''
INSERT INTO ohlcv(symbol, time, open, high, low, close, volume_usd)
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

# 单个交易对处理：包含拉取、写入及日志输出

def process_symbol(symbol, start_ts, end_ts, tz8):
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
    last_dt = datetime.fromtimestamp(last_ts/1000, tz=timezone.utc).astimezone(tz8)
    last_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"{symbol} 请求{n}条数据 成功写入{written}条数据 最新时间为{last_str}", flush=True)
    return written

# 主流程：并发调度 + 结果聚合

def main():
    ensure_table()
    tz8 = timezone(timedelta(hours=8))
    # 计算时间段
    interval = 15 * 60 * 1000
    now = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ts = (now // interval) * interval
    start_ts = end_ts - interval * 4500

    symbols = get_symbols_from_db()
    if not symbols:
        print("没有 instrument，请检查 instruments 表。", flush=True)
        sys.exit(1)

    total = 0
    with ThreadPoolExecutor(max_workers=2) as executor:
        futures = []
        for sym in symbols:
            futures.append(
                executor.submit(process_symbol, sym, start_ts, end_ts, tz8)
            )
            time.sleep(0.3)
        for f in futures:
            total += f.result()

    print(f"\n总共写入 {total} 条数据", flush=True)

if __name__ == "__main__":
    main()
