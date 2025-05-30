#!/usr/bin/env python3
import pandas as pd
import requests
from sqlalchemy import text
from datetime import datetime

from db import engine_ohlcv
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, get_proxy_dict


def ensure_table():
    sql = """
    CREATE TABLE IF NOT EXISTS monitor_levels (
        symbol TEXT PRIMARY KEY,
        start_ts BIGINT NOT NULL,
        end_ts BIGINT NOT NULL,
        p1 NUMERIC NOT NULL,
        alerted BOOLEAN NOT NULL DEFAULT FALSE
    );
    """
    with engine_ohlcv.begin() as conn:
        conn.execute(text(sql))


def send_message(text_msg: str) -> None:
    token = TELEGRAM_BOT_TOKEN
    chat_id = TELEGRAM_CHAT_ID
    if not token or not chat_id:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": chat_id, "text": text_msg}
    try:
        requests.post(url, json=payload, timeout=10, proxies=get_proxy_dict())
    except Exception as e:
        print("Failed to send telegram message:", e)


def check_prices():
    with engine_ohlcv.begin() as conn:
        df = pd.read_sql(
            "SELECT symbol, p1 FROM monitor_levels WHERE NOT alerted", conn
        )
        if df.empty:
            return
        alerts = []
        for _, row in df.iterrows():
            sym = row["symbol"]
            p1 = float(row["p1"])
            latest = conn.execute(
                text(
                    "SELECT close FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 1"
                ),
                {"s": sym},
            ).fetchone()
            if not latest:
                continue
            price = float(latest[0])
            if price > p1:
                alerts.append(f"{sym} price {price} > P1 {p1}")
                conn.execute(
                    text("UPDATE monitor_levels SET alerted=true WHERE symbol=:s"),
                    {"s": sym},
                )
        if alerts:
            send_message("\n".join(alerts))


def main():
    ensure_table()
    check_prices()


if __name__ == "__main__":
    main()
