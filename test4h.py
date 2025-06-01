#!/usr/bin/env python3
"""Compute consecutive 4h up candles for all symbols."""
import psycopg2
from config import secret_get

DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("DB_NAME", "ohlcv_4h"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}


def get_symbols():
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT symbol FROM ohlcv_4h")
    symbols = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return symbols



def fetch_closes(symbol: str, limit: int = 100):
    conn = psycopg2.connect(**DB_CFG)
    cur = conn.cursor()
    cur.execute(
        "SELECT close FROM ohlcv_4h WHERE symbol=%s ORDER BY time DESC LIMIT %s",
        (symbol, limit),

    )
    closes = [float(r[0]) for r in cur.fetchall()]
    cur.close()
    conn.close()
    return closes


def consecutive_up_count(closes):
    count = 0
    for i in range(len(closes) - 1):
        if closes[i + 1] < closes[i]:
            count += 1
        else:
            break
    if count:
        start = closes[count]
        end = closes[0]
        pct = (end - start) / start * 100 if start else 0.0
    else:
        pct = 0.0
    return count, pct


def main():
    symbols = get_symbols()
    results = []
    for sym in symbols:
        closes = fetch_closes(sym)
        if not closes:
            continue
        count, pct = consecutive_up_count(closes)
        results.append((sym, count, pct))
    results.sort(key=lambda x: x[1], reverse=True)
    top10 = results[:10]
    print(f"{'symbol':<10} {'count':>5} {'累计涨幅':>10}")
    for sym, c, pct in top10:
        print(f"{sym:<10} {c:>5} {pct:>9.2f}%")


if __name__ == "__main__":
    main()
