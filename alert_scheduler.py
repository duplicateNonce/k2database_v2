#!/usr/bin/env python3
"""Run price breakout and 4h up alerts every 15 minutes."""
import time
from datetime import datetime, timedelta
from monitor_bot import (
    ensure_table,
    ensure_up_tables,
    check_prices,
    check_up_alert,
    send_message,
)


def main() -> None:
    ensure_table()
    ensure_up_tables()
    send_message("alert scheduler started")
    # first run aligned to the next quarter hour
    def next_quarter(dt: datetime) -> datetime:
        dt = dt.replace(second=0, microsecond=0)
        minute = (dt.minute // 15 + 1) * 15
        if minute >= 60:
            dt = dt.replace(minute=0) + timedelta(hours=1)
        else:
            dt = dt.replace(minute=minute)
        return dt

    next_run = next_quarter(datetime.now())
    while True:
        now = datetime.now()
        if now >= next_run:
            check_prices()
            check_up_alert()
            next_run = next_quarter(now)
        sleep = min(120, (next_run - now).total_seconds())
        if sleep > 0:
            time.sleep(sleep)
        else:
            time.sleep(120)


if __name__ == "__main__":
    main()
