#!/usr/bin/env python3
"""Run price breakout and 4h up alerts every 15 minutes."""
import time
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
    send_message("4h 连涨警报开始运行")
    while True:
        start = time.time()
        check_prices()
        check_up_alert()
        # Sleep until next 15 minute slot
        elapsed = time.time() - start
        sleep_time = max(900 - elapsed, 1)
        time.sleep(sleep_time)


if __name__ == "__main__":
    main()
