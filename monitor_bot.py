#!/usr/bin/env python3
import time
import pandas as pd
import requests
from sqlalchemy import text
from datetime import datetime, timezone

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


def send_message(text_msg: str, chat_id: str | int | None = None) -> None:
    token = TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        return
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": cid, "text": text_msg}
    try:
        requests.post(url, json=payload, timeout=10, proxies=get_proxy_dict())
    except Exception as e:
        print("Failed to send telegram message:", e)


def check_prices() -> None:
    """Alert when the latest price is higher than the saved P1."""
    with engine_ohlcv.begin() as conn:
        df = pd.read_sql("SELECT symbol, p1 FROM monitor_levels WHERE NOT alerted", conn)
        if df.empty:
            return
        alerts = []
        for _, row in df.iterrows():
            sym = row["symbol"]
            p1 = float(row["p1"])
            latest = conn.execute(
                text("SELECT close FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 1"),
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


def ba_command(chat_id: int) -> None:
    """Respond to /ba with top 5 symbols closest to P1."""
    try:
        with engine_ohlcv.begin() as conn:
            df = pd.read_sql("SELECT symbol, p1 FROM monitor_levels", conn)
            if df.empty:
                send_message("无 P1 数据", chat_id)
                return
            rows: list[tuple[str, float, float]] = []
            for _, row in df.iterrows():
                sym = row["symbol"]
                p1 = float(row["p1"])
                latest = conn.execute(
                    text("SELECT close FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 1"),
                    {"s": sym},
                ).fetchone()
                if not latest:
                    continue
                p2 = float(latest[0])
                diff_pct = abs(p2 - p1) / p1 * 100 if p1 else 0
                rows.append((sym, p2, p1, diff_pct))
            if not rows:
                send_message("无有效数据", chat_id)
                return
            rows.sort(key=lambda x: x[3])
            top5 = rows[:5]
            lines = [f"{r[0]} {r[1]:.4f} 与 P1 {r[2]:.4f} 相差 {r[3]:.2f}%" for r in top5]
            send_message("\n".join(lines), chat_id)
    except Exception as exc:
        send_message(f"/ba 执行失败: {exc}", chat_id)


def rsi_command(chat_id: int) -> None:
    """Return the 10 symbols with the lowest 4h RSI."""
    url = "https://open-api-v4.coinglass.com/api/futures/rsi/list"
    try:
        resp = requests.get(url, headers={"accept": "application/json"}, timeout=10, proxies=get_proxy_dict())
        data = resp.json()
        if data.get("code") != "0":
            raise RuntimeError(data.get("msg"))
        items = sorted(data.get("data", []), key=lambda x: x.get("rsi_4h", 0))[:10]
        lines = [f"{it['symbol']} {it['rsi_4h']:.2f} {it['current_price']}" for it in items]
        send_message("\n".join(lines), chat_id)
    except Exception as exc:
        send_message(f"/rsi 执行失败: {exc}", chat_id)


UP_STREAK = 4  # 连涨阈值


def aggregate_4h(df: pd.DataFrame) -> pd.DataFrame:
    df = df.set_index("dt").sort_index()
    counts = df["open"].resample("4H").count()
    complete = counts[counts == 16].index
    if complete.empty:
        return pd.DataFrame()
    o = df["open"].resample("4H").first().loc[complete]
    h = df["high"].resample("4H").max().loc[complete]
    l = df["low"].resample("4H").min().loc[complete]
    c = df["close"].resample("4H").last().loc[complete]
    v = df["volume_usd"].resample("4H").sum().loc[complete]
    res = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v})
    return res.reset_index().rename(columns={"dt": "start"})


def consecutive_up_count(df4h: pd.DataFrame) -> int:
    df4h = df4h.sort_values("start").reset_index(drop=True)
    count = 0
    for i in range(len(df4h) - 1, -1, -1):
        row = df4h.iloc[i]
        if row["close"] <= row["open"]:
            break
        if count > 0:
            later = df4h.iloc[i + 1]
            if row["close"] <= later["close"]:
                break
        count += 1
    return count


def ensure_up_tables() -> None:
    sql = """
    CREATE TABLE IF NOT EXISTS four_hour_up_history (
        symbol TEXT NOT NULL,
        ts BIGINT NOT NULL,
        count INTEGER NOT NULL,
        PRIMARY KEY(symbol, ts)
    );
    """
    with engine_ohlcv.begin() as conn:
        conn.execute(text(sql))


def check_up_alert() -> None:
    with engine_ohlcv.begin() as conn:
        syms = [row[0] for row in conn.execute(text("SELECT symbol FROM monitor_levels"))]
        if not syms:
            return
        for sym in syms:
            df = pd.read_sql(
                text(
                    "SELECT time, open, high, low, close, volume_usd FROM ohlcv "
                    "WHERE symbol=:s ORDER BY time DESC LIMIT 200"
                ),
                conn,
                params={"s": sym},
            )
            if df.empty:
                continue
            df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True)
            df4h = aggregate_4h(df)
            if df4h.empty:
                continue
            streak = consecutive_up_count(df4h)
            last_start = int(df4h["start"].iloc[-1].timestamp() * 1000)
            prev = conn.execute(
                text(
                    "SELECT count FROM four_hour_up_history WHERE symbol=:s ORDER BY ts DESC LIMIT 1"
                ),
                {"s": sym},
            ).fetchone()
            prev_count = prev[0] if prev else 0
            conn.execute(
                text(
                    "INSERT INTO four_hour_up_history(symbol, ts, count) "
                    "VALUES(:s, :ts, :c) "
                    "ON CONFLICT(symbol, ts) DO UPDATE SET count=excluded.count"
                ),
                {"s": sym, "ts": last_start, "c": streak},
            )
            if streak >= UP_STREAK and streak > prev_count:
                send_message(f"{sym} 4h 连涨 {streak} 根")


def history_command(chat_id: int) -> None:
    with engine_ohlcv.begin() as conn:
        df = pd.read_sql(
            text(
                "SELECT symbol, ts, count FROM four_hour_up_history ORDER BY ts DESC LIMIT 20"
            ),
            conn,
        )
    if df.empty:
        send_message("无历史记录", chat_id)
        return
    df["dt"] = pd.to_datetime(df["ts"], unit="ms", utc=True).dt.strftime("%m-%d %H:%M")
    lines = [f"{r.symbol} {r.dt} 连涨{r['count']}" for r in df.itertuples()]
    send_message("\n".join(lines), chat_id)


def fetch_updates(offset: int) -> list[dict]:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getUpdates"
    try:
        resp = requests.get(
            url,
            params={"timeout": 10, "offset": offset},
            timeout=15,
            proxies=get_proxy_dict(),
        )
        data = resp.json()
        if not data.get("ok"):
            return []
        return data.get("result", [])
    except Exception:
        return []


def handle_update(upd: dict) -> int:
    if "message" not in upd:
        return upd.get("update_id", 0)
    msg = upd["message"]
    text_msg = msg.get("text", "")
    chat_id = msg.get("chat", {}).get("id")
    if text_msg.startswith("/ba"):
        ba_command(chat_id)
    elif text_msg.startswith("/rsi"):
        rsi_command(chat_id)
    elif text_msg.startswith("/4h"):
        history_command(chat_id)
    return upd.get("update_id", 0)


def main() -> None:
    ensure_table()
    ensure_up_tables()
    send_message("监控机器人已启动")
    last_update = 0
    last_check = time.time()
    while True:
        if time.time() - last_check >= 900:
            check_prices()
            last_check = time.time()
        updates = fetch_updates(last_update + 1)
        for upd in updates:
            last_update = handle_update(upd)
        time.sleep(5)


if __name__ == "__main__":
    main()
