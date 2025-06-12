#!/usr/bin/env python3
"""Fetch TronGrid events for specified contract and print new ones."""

import time
import traceback
from datetime import datetime, timezone, timedelta

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv

from config import secret_get, get_proxy_dict

load_dotenv()

DB_CFG = {
    "host": secret_get("DB_HOST", "127.0.0.1"),
    "port": secret_get("DB_PORT", "5432"),
    "dbname": secret_get("TRON_DB_NAME", "testtron"),
    "user": secret_get("DB_USER", "postgres"),
    "password": secret_get("DB_PASSWORD", ""),
}

API_URL = (
    "https://api.trongrid.io/v1/contracts/TR7NHqjeKQxGTCi8q8ZY4pL8otSzgjLj6t"
    "/events?only_confirmed=true&limit=50"
)

PROXIES = get_proxy_dict()
TZ8 = timezone(timedelta(hours=8))
LOG_FILE = "testtron.log"


def log_result(message: str) -> None:
    """Append a timestamped message to the log file."""
    ts = datetime.now(TZ8).strftime("%Y-%m-%d %H:%M:%S")
    try:
        with open(LOG_FILE, "a", encoding="utf-8") as f:
            f.write(f"{ts} - {message}\n")
    except Exception as exc:
        print("Failed to write log:", exc)


def ensure_table() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS tron_events (
        block_number BIGINT,
        block_timestamp BIGINT,
        caller_contract_address TEXT,
        contract_address TEXT,
        event_index INT,
        event_name TEXT,
        result JSONB,
        result_type JSONB,
        event TEXT,
        transaction_id TEXT,
        PRIMARY KEY(transaction_id, event_index)
    );
    """
    with psycopg2.connect(**DB_CFG) as conn, conn.cursor() as cur:
        cur.execute(sql)
        conn.commit()


def fetch_events() -> list[dict]:
    resp = requests.get(
        API_URL,
        headers={"Accept": "application/json"},
        timeout=30,
        proxies=PROXIES or None,
    )
    resp.raise_for_status()
    return resp.json().get("data", [])


def insert_events(events: list[dict]) -> list[dict]:
    inserted = []
    sql = """
    INSERT INTO tron_events(
        block_number, block_timestamp, caller_contract_address,
        contract_address, event_index, event_name,
        result, result_type, event, transaction_id
    ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT(transaction_id, event_index) DO NOTHING;
    """
    with psycopg2.connect(**DB_CFG) as conn, conn.cursor() as cur:
        for ev in events:
            cur.execute(
                sql,
                (
                    ev.get("block_number"),
                    ev.get("block_timestamp"),
                    ev.get("caller_contract_address"),
                    ev.get("contract_address"),
                    ev.get("event_index"),
                    ev.get("event_name"),
                    psycopg2.extras.Json(ev.get("result")),
                    psycopg2.extras.Json(ev.get("result_type")),
                    ev.get("event"),
                    ev.get("transaction_id"),
                ),
            )
            if cur.rowcount:
                inserted.append(ev)
        conn.commit()
    return inserted


def format_ts(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).astimezone(TZ8)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def process_once() -> None:
    try:
        events = fetch_events()
        if not events:
            log_result("no new result")
            return
        new_events = insert_events(events)
        if not new_events:
            log_result("no new result")
            return
        for e in new_events:
            ts_str = format_ts(e["block_timestamp"])
            tx_url = f"https://tronscan.org/#/transaction/{e['transaction_id']}"
            print(f"{ts_str}\n{e['event_name']}\n{tx_url}\n")
        log_result(f"stored {len(new_events)} event(s)")
    except Exception:
        err = traceback.format_exc()
        print("testtron error:\n" + err)
        log_result("error: " + err.replace("\n", " | "))


def main() -> None:
    ensure_table()
    while True:
        process_once()
        time.sleep(10)


if __name__ == "__main__":
    main()
