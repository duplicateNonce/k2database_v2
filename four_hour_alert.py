#!/usr/bin/env python3
import time
from monitor_bot import ensure_up_tables, check_up_alert, send_message


def main() -> None:
    ensure_up_tables()
    send_message("4h 连涨警报开始运行")
    while True:
        check_up_alert()
        time.sleep(900)


if __name__ == "__main__":
    main()
