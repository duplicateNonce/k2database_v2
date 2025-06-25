#!/usr/bin/env python3
"""Download open interest history for Binance and Bybit and store in Postgres."""
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

REQ_TIMESTAMPS = []
MAX_REQS_PER_MIN = 79
REQ_LOCK = threading.Lock()

LIMIT = 2000

load_dotenv()
API_KEY = secret_get("CG_API_KEY")
if not API_KEY:
    print("\u8bf7\u5728 .env \u4e2d\u914d\u7f6e CG_API_KEY", flush=True)
    sys.exit(1)

DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "postgres"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}


def wait_rate_limit() -> None:
    """Ensure request rate within limit."""
    with REQ_LOCK:
        now = time.time()
        REQ_TIMESTAMPS[:] = [t for t in REQ_TIMESTAMPS if now - t < 60]
        if len(REQ_TIMESTAMPS) >= MAX_REQS_PER_MIN:
            print(f"\u8fbe\u5230\u6bcf\u5206\u949f{MAX_REQS_PER_MIN}\u6b21\u8bf7\u6c42\u4e0a\u9650\uff0c\u6682\u505c10\u79d2", flush=True)
            time.sleep(10)
            now = time.time()
            REQ_TIMESTAMPS[:] = [t for t in REQ_TIMESTAMPS if now - t < 60]
        REQ_TIMESTAMPS.append(time.time())


def ensure_table():
    """Ensure ``oi_binance_bybit_1h`` table exists."""
    sql = '''
CREATE TABLE IF NOT EXISTS oi_binance_bybit_1h (
    symbol TEXT NOT NULL,
    time BIGINT NOT NULL,
    binance NUMERIC,
    bybit NUMERIC,
    PRIMARY KEY(symbol, time)
);
'''
    conn = psycopg2.connect(**DB_CFG)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute(sql)
    cur.close()
    conn.close()
    print("\u8868\u7ed3\u6784\u5df2\u786e\u4fdd", flush=True)


def get_symbols_from_db() -> list[str]:
    """Read all trading pairs from ``instruments`` table."""
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT instrument_id FROM instruments;")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols


def get_symbol_latest_ts(symbol: str) -> int | None:
    """Return latest timestamp for ``symbol`` from table."""
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute(
        "SELECT MAX(time) FROM oi_binance_bybit_1h WHERE symbol=%s", (symbol,)
    )
    row = cur.fetchone()
    cur.close()
    conn.close()
    return int(row[0]) if row and row[0] is not None else None


def build_url(exchange: str, symbol: str, end_ts: int) -> str:
    """Build API request URL."""
    return (
        "https://open-api-v4.coinglass.com/api/futures/open-interest/history"
        f"?exchange={exchange}"
        f"&symbol={symbol}"
        "&interval=1h"
        f"&limit={LIMIT}"
        f"&end_time={end_ts}"
        "&unit=usd"
    )


def fetch_oi(exchange: str, symbol: str, end_ts: int) -> dict[int, Decimal]:
    """Fetch open interest data for one exchange."""
    url = build_url(exchange, symbol, end_ts)
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
            result = {}
            for item in data.get("data", []):
                ts = int(item.get("time") or item.get("t") or item.get("timestamp"))
                val = (
                    item.get("sumOpenInterest")
                    or item.get("openInterest")
                    or item.get("value")
                )
                if val is not None:
                    result[ts] = Decimal(str(val))
            return result
        except Exception as exc:
            if attempt == 2:
                raise
            print(f"{symbol} {exchange} \u8bf7\u6c42\u5931\u8d25\u5c1d\u8bd5\u91cd\u8bd5: {exc}", flush=True)
            time.sleep(1)


def insert_data(records) -> int:
    """Insert open interest records."""
    sql = '''
INSERT INTO oi_binance_bybit_1h(symbol, time, binance, bybit)
VALUES(%s, %s, %s, %s)
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
            print(f"{rec[0]} \u63d2\u5165\u5931\u8d25: {e}", flush=True)
    conn.commit()
    cur.close()
    conn.close()
    return count


def process_symbol(symbol: str, end_ts: int, tz8, interval: int) -> tuple[int, int]:
    """Download and save open interest for one symbol."""
    latest = get_symbol_latest_ts(symbol)
    expected_last = end_ts
    if latest is not None and latest >= expected_last:
        print(f"{symbol} \u6570\u636e\u5df2\u662f\u6700\u65b0\uff0c\u8df3\u8fc7", flush=True)
        return 0, 0

    try:
        data_binance = fetch_oi("Binance", symbol, end_ts)
    except Exception as e:
        print(f"{symbol} Binance \u8bf7\u6c42\u5931\u8d25: {e}", flush=True)
        return 0, 1

    try:
        data_bybit = fetch_oi("Bybit", symbol, end_ts)
    except Exception as e:
        print(f"{symbol} Bybit \u8bf7\u6c42\u5931\u8d25\uff0c\u4f7f\u7528 0: {e}", flush=True)
        data_bybit = {}

    all_ts = sorted(data_binance)
    records = [
        (
            symbol,
            ts,
            data_binance.get(ts),
            data_bybit.get(ts, Decimal("0")),
        )
        for ts in all_ts
    ]

    n = len(records)
    if n == 0:
        print(f"{symbol} \u8bf7\u6c420\u6761\u6570\u636e \u5199\u51650\u6761\u6570\u636e", flush=True)
        return 0, 0

    written = insert_data(records)
    last_ts = max(all_ts)
    last_dt = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc).astimezone(tz8)
    last_str = last_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(
        f"{symbol} \u8bf7\u6c42{n}\u6761\u6570\u636e \u6210\u529f\u5199\u5165{written}\u6761\u6570\u636e \u6700\u65b0\u65f6\u95f4\u4e3a{last_str}",
        flush=True,
    )
    return written, 0


def main() -> None:
    ensure_table()
    tz8 = timezone(timedelta(hours=8))
    interval = 3600 * 1000
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    end_ts = (now_ms // interval) * interval

    symbols = get_symbols_from_db()
    if not symbols:
        print("\u6ca1\u6709 instrument\uff0c\u8bf7\u68c0\u67e5 instruments \u8868\u3002", flush=True)
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

    print(f"\n\u603b\u5171\u5199\u5165 {total} \u6761\u6570\u636e\uff0c\u5931\u8d25 {failed} \u4e2asymbol", flush=True)


if __name__ == "__main__":
    main()
