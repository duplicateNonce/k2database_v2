#!/usr/bin/env python3
import time
import pandas as pd
import requests
from sqlalchemy import text
from datetime import datetime, timezone

from pathlib import Path

from db import engine_ohlcv
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_ID,
    CG_API_KEY,
    get_proxy_dict,
)

# Symbols that should be skipped when checking alerts
IGNORED_SYMBOLS = {"USDCUSDT"}

# Directory used to store aggregated 4h candles
FOUR_H_CACHE_DIR = Path("data/cache/4h")


def ensure_table():
    sql1 = """
    CREATE TABLE IF NOT EXISTS monitor_levels (
        symbol TEXT PRIMARY KEY,
        start_ts BIGINT NOT NULL,
        end_ts BIGINT NOT NULL,
        p1 NUMERIC NOT NULL,
        alerted BOOLEAN NOT NULL DEFAULT FALSE
    );
    """
    sql2 = """
    CREATE TABLE IF NOT EXISTS ba_hidden (
        symbol TEXT PRIMARY KEY
    );
    """
    with engine_ohlcv.begin() as conn:
        conn.execute(text(sql1))
        conn.execute(text(sql2))


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


def ascii_table(df: pd.DataFrame) -> str:
    """Return a plain text table with ASCII borders."""
    headers = list(df.columns)
    rows = df.astype(str).values.tolist()
    widths = [max(len(h), *(len(r[i]) for r in rows)) for i, h in enumerate(headers)]

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    lines = [border]
    header_line = "| " + " | ".join(f"{h:{w}}" for h, w in zip(headers, widths)) + " |"
    lines.append(header_line)
    lines.append(border)
    for r in rows:
        line = "| " + " | ".join(f"{val:{w}}" for val, w in zip(r, widths)) + " |"
        lines.append(line)
    lines.append(border)
    return "\n".join(lines)


def check_prices() -> None:
    """Alert when the latest price is higher than the saved P1."""
    with engine_ohlcv.begin() as conn:
        df = pd.read_sql("SELECT symbol, p1 FROM monitor_levels WHERE NOT alerted", conn)
        if df.empty:
            return
        alerts = []
        for _, row in df.iterrows():
            sym = row["symbol"]
            if sym.upper() in IGNORED_SYMBOLS:
                continue
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
    """Respond to /ba with the 10 symbols most distant from P1."""
    try:
        with engine_ohlcv.begin() as conn:
            df = pd.read_sql("SELECT symbol, p1 FROM monitor_levels", conn)
            hide = pd.read_sql("SELECT symbol FROM ba_hidden", conn)
            hidden = set(hide["symbol"].tolist()) if not hide.empty else set()
            if df.empty:
                send_message("无 P1 数据", chat_id)
                return
            rows: list[tuple[str, float, float, float, float]] = []
            for _, row in df.iterrows():
                sym = row["symbol"]
                if sym.upper() in IGNORED_SYMBOLS or sym in hidden:
                    continue
                p1 = float(row["p1"])
                latest = conn.execute(
                    text("SELECT close FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 1"),
                    {"s": sym},
                ).fetchone()
                if not latest:
                    continue
                p2 = float(latest[0])
                diff_pct = (p2 - p1) / p1 * 100 if p1 else 0.0
                rows.append((sym, p2, p1, diff_pct))
            if not rows:
                send_message("无有效数据", chat_id)
                return
            rows.sort(key=lambda x: x[3])
            top10 = rows[:10]
            table = pd.DataFrame(
                [
                    {
                        "Symbol": r[0].replace("USDT", ""),
                        "现价": f"{r[1]:.4f}",
                        "区域最高价": f"{r[2]:.4f}",
                        "差值": f"{r[3]:+.2f}%",
                    }
                    for r in top10
                ]
            )
            msg = "```\n" + ascii_table(table) + "\n```"
            send_message(msg, chat_id)
    except Exception as exc:
        send_message(f"/ba 执行失败: {exc}", chat_id)


def removeba_command(chat_id: int, symbol: str) -> None:
    symbol = symbol.upper()
    try:
        with engine_ohlcv.begin() as conn:
            conn.execute(
                text(
                    "INSERT INTO ba_hidden(symbol) VALUES(:s) "
                    "ON CONFLICT DO NOTHING"
                ),
                {"s": symbol},
            )
        send_message(f"已隐藏 {symbol}", chat_id)
    except Exception as exc:
        send_message(f"/removeba 执行失败: {exc}", chat_id)


def addba_command(chat_id: int, symbol: str) -> None:
    symbol = symbol.upper()
    try:
        with engine_ohlcv.begin() as conn:
            conn.execute(text("DELETE FROM ba_hidden WHERE symbol=:s"), {"s": symbol})
        send_message(f"已恢复 {symbol}", chat_id)
    except Exception as exc:
        send_message(f"/addba 执行失败: {exc}", chat_id)


def rsi_command(chat_id: int) -> None:
    """Return the 10 symbols with the highest 4h RSI."""
    url = "https://open-api-v4.coinglass.com/api/futures/rsi/list"
    try:
        headers = {"accept": "application/json"}
        if CG_API_KEY:
            headers["CG-API-KEY"] = CG_API_KEY
        resp = requests.get(url, headers=headers, timeout=10, proxies=get_proxy_dict())
        data = resp.json()
        if data.get("code") != "0":
            raise RuntimeError(data.get("msg"))
        items = sorted(
            data.get("data", []), key=lambda x: x.get("rsi_4h", 0), reverse=True
        )[:10]
        table = []
        for it in items:
            pct = it.get("price_change_percent_4h", 0)
            table.append(
                {
                    "symbol": it.get("symbol"),
                    "RSI(4h)": f"{it.get('rsi_4h', 0):.2f}",
                    "现价": it.get("current_price"),
                    "4h涨跌幅": f"{pct:+.2f}%",
                }
            )
        df_t = pd.DataFrame(table, columns=["symbol", "RSI(4h)", "现价", "4h涨跌幅"])
        msg = "```\n" + ascii_table(df_t) + "\n```"
        send_message(msg, chat_id)
    except Exception as exc:
        send_message(f"/rsi 执行失败: {exc}", chat_id)


UP_STREAK = 2  # 连涨阈值


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


def load_4h_data(conn, symbol: str) -> pd.DataFrame:
    """Return cached 4h candles, updating from the database if needed."""
    FOUR_H_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = FOUR_H_CACHE_DIR / f"{symbol}.csv"
    df_cache = pd.DataFrame()
    next_ts = 0
    if path.exists():
        try:
            df_cache = pd.read_csv(path, parse_dates=["start"])
            if not df_cache.empty:
                last_start = df_cache["start"].iloc[-1]
                next_ts = int(pd.Timestamp(last_start).timestamp() * 1000) + 4 * 3600 * 1000
        except Exception:
            df_cache = pd.DataFrame()
    df = pd.read_sql(
        text(
            "SELECT time, open, high, low, close, volume_usd FROM ohlcv "
            "WHERE symbol=:s AND time >= :t ORDER BY time"
        ),
        conn,
        params={"s": symbol, "t": next_ts},
    )
    if not df.empty:
        df["dt"] = pd.to_datetime(df["time"], unit="ms", utc=True)
        new_4h = aggregate_4h(df)
        if not new_4h.empty:
            df_cache = pd.concat([df_cache, new_4h], ignore_index=True)
            df_cache.to_csv(path, index=False)
    return df_cache


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
            if sym.upper() in IGNORED_SYMBOLS:
                continue
            df4h = load_4h_data(conn, sym)
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


def four_hour_command(chat_id: int) -> None:
    """Aggregate all OHLCV data into 4h candles and send alerts."""
    with engine_ohlcv.begin() as conn:
        syms = [row[0] for row in conn.execute(text("SELECT symbol FROM monitor_levels"))]
        if not syms:
            send_message("无监控币种", chat_id)
            return
        lines: list[str] = []
        for sym in syms:
            if sym.upper() in IGNORED_SYMBOLS:
                continue
            df4h = load_4h_data(conn, sym)
            if df4h.empty:
                continue
            streak = consecutive_up_count(df4h)
            if streak >= UP_STREAK:
                lines.append(f"{sym} 4h 连涨 {streak} 根")
        if lines:
            send_message("\n".join(lines), chat_id)
        else:
            send_message("无符合条件的交易对", chat_id)


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
    if text_msg.startswith("/removeba"):
        parts = text_msg.split()
        if len(parts) >= 2:
            removeba_command(chat_id, parts[1])
        else:
            send_message("用法: /removeba SYMBOL", chat_id)
    elif text_msg.startswith("/addba"):
        parts = text_msg.split()
        if len(parts) >= 2:
            addba_command(chat_id, parts[1])
        else:
            send_message("用法: /addba SYMBOL", chat_id)
    elif text_msg.startswith("/ba"):
        ba_command(chat_id)
    elif text_msg.startswith("/rsi"):
        rsi_command(chat_id)
    elif text_msg.startswith("/4h"):
        four_hour_command(chat_id)
    return upd.get("update_id", 0)


def main() -> None:
    ensure_table()
    ensure_up_tables()
    send_message("monitor bot started")
    last_update = 0
    while True:
        updates = fetch_updates(last_update + 1)
        for upd in updates:
            last_update = handle_update(upd)
        time.sleep(5)


if __name__ == "__main__":
    main()
