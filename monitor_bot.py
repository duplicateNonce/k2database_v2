#!/usr/bin/env python3
import time
import pandas as pd
import requests
from sqlalchemy import text
from datetime import datetime, timezone
import unicodedata

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
FOUR_H_MEM_CACHE: dict[str, pd.DataFrame] = {}


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


def send_message(
    text_msg: str, chat_id: str | int | None = None, parse_mode: str | None = None
) -> None:
    """Send ``text_msg`` to Telegram using an optional parse mode."""

    token = TELEGRAM_BOT_TOKEN
    cid = chat_id or TELEGRAM_CHAT_ID
    if not token or not cid:
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {"chat_id": cid, "text": text_msg}
    if parse_mode:
        payload["parse_mode"] = parse_mode

    try:
        requests.post(url, json=payload, timeout=10, proxies=get_proxy_dict())
    except Exception as e:
        print("Failed to send telegram message:", e)


def _display_width(text: str) -> int:
    """Return the visual width of a string accounting for wide chars."""
    return sum(2 if unicodedata.east_asian_width(c) in "WF" else 1 for c in text)


def ascii_table(df: pd.DataFrame) -> str:
    """Return a plain text table with ASCII borders."""
    headers = list(df.columns)
    rows = df.astype(str).values.tolist()
    widths = [max(_display_width(h), *(_display_width(r[i]) for r in rows)) for i, h in enumerate(headers)]

    def pad(val: str, width: int) -> str:
        val = str(val)
        pad_len = width - _display_width(val)
        return val + " " * max(pad_len, 0)

    border = "+" + "+".join("-" * (w + 2) for w in widths) + "+"
    lines = [border]
    header_line = "| " + " | ".join(pad(h, w) for h, w in zip(headers, widths)) + " |"
    lines.append(header_line)
    lines.append(border)
    for r in rows:
        line = "| " + " | ".join(pad(val, w) for val, w in zip(r, widths)) + " |"
        lines.append(line)
    lines.append(border)
    return "\n".join(lines)


def check_prices() -> None:
    """Alert when price breaks above P1 with volume confirmation."""
    with engine_ohlcv.begin() as conn:
        df_levels = pd.read_sql(
            "SELECT symbol, p1, start_ts, end_ts FROM monitor_levels WHERE NOT alerted",
            conn,
        )
        if df_levels.empty:
            return

        syms = [s for s in df_levels["symbol"].tolist() if s.upper() not in IGNORED_SYMBOLS]
        if not syms:
            return

        labels = {
            r["instrument_id"]: r["labels"]
            for r in conn.execute(text("SELECT instrument_id, labels FROM instruments")).mappings()
        }

        # fetch latest price and volume for all symbols in one query
        sql = text(
            """
            SELECT o.symbol, o.time, o.close, o.volume_usd FROM ohlcv o
            JOIN (
                SELECT symbol, MAX(time) AS t
                FROM ohlcv
                WHERE symbol = ANY(:syms)
                GROUP BY symbol
            ) m ON o.symbol = m.symbol AND o.time = m.t
            """
        )
        df_latest = pd.read_sql(sql, conn, params={"syms": syms})
        latest_map = {r.symbol: r for r in df_latest.itertuples()}

        rows = []
        for _, lv in df_levels.iterrows():
            sym = lv.symbol
            last = latest_map.get(sym)
            if not last:
                continue
            p1 = float(lv.p1)
            price = float(last.close)
            if price <= p1:
                continue

            # find p1 time within saved range
            p1_row = conn.execute(
                text(
                    "SELECT time FROM ohlcv WHERE symbol=:s AND time BETWEEN :a AND :b "
                    "ORDER BY close DESC LIMIT 1"
                ),
                {"s": sym, "a": int(lv.start_ts), "b": int(lv.end_ts)},
            ).fetchone()
            p1_time = p1_row[0] if p1_row else lv.start_ts

            vol_df = pd.read_sql(
                text(
                    "SELECT volume_usd FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 96"
                ),
                conn,
                params={"s": sym},
            )
            vol_ma = vol_df["volume_usd"].astype(float).mean() if not vol_df.empty else 0.0
            cur_vol = float(getattr(last, "volume_usd", 0))
            vol_change = (cur_vol - vol_ma) / vol_ma * 100 if vol_ma else 0.0
            diff_pct = (price - p1) / p1 * 100 if p1 else 0.0

            lbl = labels.get(sym)
            if lbl is None:
                lbl_text = ""
            elif isinstance(lbl, list):
                lbl_text = "，".join(lbl)
            else:
                lbl_text = str(lbl)

            dt = (
                pd.to_datetime(p1_time, unit="ms", utc=True)
                .tz_convert("Asia/Shanghai")
                .strftime("%m.%d %H:%M")
            )

            rows.append(
                {
                    "标签": lbl_text,
                    "Symbol": sym.replace("USDT", ""),
                    "P1时间": dt,
                    "最新价格": f"{price:.4f}",
                    "差值%": f"{diff_pct:+.2f}%",
                    "量变化%": f"{vol_change:+.2f}%",
                }
            )

            conn.execute(
                text("UPDATE monitor_levels SET alerted=true WHERE symbol=:s"),
                {"s": sym},
            )

        if rows:
            rows.sort(key=lambda x: float(x["差值%"].rstrip("%")), reverse=True)
            df_table = pd.DataFrame(rows)
            msg = "```\n" + ascii_table(df_table) + "\n```"
            send_message(msg, parse_mode="Markdown")


def ba_command(chat_id: int) -> None:
    """Respond to /ba with the 10 symbols most distant from P1."""
    try:
        with engine_ohlcv.begin() as conn:
            df = pd.read_sql("SELECT symbol, p1 FROM monitor_levels", conn)
            hide = pd.read_sql("SELECT symbol FROM ba_hidden", conn)
            hidden = set(s.upper() for s in hide["symbol"].tolist()) if not hide.empty else set()
            labels = {
                r["instrument_id"]: r["labels"]
                for r in conn.execute(text("SELECT instrument_id, labels FROM instruments")).mappings()
            }
            if df.empty:
                send_message("无 P1 数据", chat_id)
                return
            rows: list[tuple[str, str, float, float, float]] = []
            for _, row in df.iterrows():
                sym = row["symbol"]
                u_sym = sym.upper()
                if u_sym in IGNORED_SYMBOLS or u_sym in hidden:
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
                lbl = labels.get(sym)
                if lbl is None:
                    lbl_text = ""
                elif isinstance(lbl, list):
                    lbl_text = "，".join(lbl)
                else:
                    lbl_text = str(lbl)
                rows.append((sym, lbl_text, p2, p1, diff_pct))
            if not rows:
                send_message("无有效数据", chat_id)
                return
            # 按照与 P1 的差值百分比从大到小排序
            rows.sort(key=lambda x: x[4], reverse=True)
            top10 = rows[:10]
            table = pd.DataFrame(
                [
                    {
                        "标签": r[1],
                        "Symbol": r[0].replace("USDT", ""),
                        "现价": f"{r[2]:.4f}",
                        "区域最高价": f"{r[3]:.4f}",
                        "差值": f"{r[4]:+.2f}%",
                    }
                    for r in top10
                ]
            )
            msg = "```\n" + ascii_table(table) + "\n```"
            send_message(msg, chat_id, parse_mode="Markdown")
    except Exception as exc:
        send_message(f"/ba 执行失败: {exc}", chat_id)


def removeba_command(chat_id: int, symbol: str) -> None:
    symbol = symbol.strip().upper()
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
    symbol = symbol.strip().upper()
    try:
        with engine_ohlcv.begin() as conn:
            conn.execute(text("DELETE FROM ba_hidden WHERE symbol=:s"), {"s": symbol})
        send_message(f"已恢复 {symbol}", chat_id)
    except Exception as exc:
        send_message(f"/addba 执行失败: {exc}", chat_id)


def rsi_command(chat_id: int) -> None:
    """Return the 10 symbols with the lowest 4h RSI."""
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
            data.get("data", []),
            key=lambda x: x.get("rsi_4h", 0),
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
        send_message(msg, chat_id, parse_mode="Markdown")
    except Exception as exc:
        send_message(f"/rsi 执行失败: {exc}", chat_id)


UP_STREAK = 2  # 连涨阈值


def aggregate_4h(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 15m candles into 4h bars aligned to 00:00/04:00/08:00..."""
    df = df.set_index("dt").sort_index()
    # Snap timestamps to 15m boundaries to avoid drift
    df.index = df.index.floor("15min")
    # Resample using a fixed origin so windows start at 00:00, 04:00, ...
    tz = df.index.tz
    if tz is not None:
        origin = pd.Timestamp("1970-01-01", tz=tz)
    else:
        origin = "epoch"
    rs = df.resample(
        "4H",
        label="left",
        closed="left",
        origin=origin,
        offset="0H",
    )

    counts = rs["open"].count()
    complete = counts[counts == 16].index
    if complete.empty:
        return pd.DataFrame()

    o = rs["open"].first().loc[complete]
    h = rs["high"].max().loc[complete]
    l = rs["low"].min().loc[complete]
    c = rs["close"].last().loc[complete]
    v = rs["volume_usd"].sum().loc[complete]
    res = pd.DataFrame({"open": o, "high": h, "low": l, "close": c, "volume": v})
    return res.reset_index().rename(columns={"dt": "start"})


def load_4h_data(conn, symbol: str) -> pd.DataFrame:
    """Return cached 4h candles, updating from the database if needed."""
    FOUR_H_CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = FOUR_H_CACHE_DIR / f"{symbol}.csv"
    df_cache = FOUR_H_MEM_CACHE.get(symbol)
    next_ts = 0
    if df_cache is None and path.exists():
        try:
            df_cache = pd.read_csv(path, parse_dates=["start"])
        except Exception:
            df_cache = pd.DataFrame()
    if df_cache is not None and not df_cache.empty:
        last_start = df_cache["start"].iloc[-1]
        next_ts = int(pd.Timestamp(last_start).timestamp() * 1000) + 4 * 3600 * 1000
    else:
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
    FOUR_H_MEM_CACHE[symbol] = df_cache
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


def fetch_4h_closes(conn, symbol: str, limit: int = 30) -> list[float]:
    """Return recent 4h closes from the ohlcv_4h table."""
    df = pd.read_sql(
        text(
            f"SELECT close FROM ohlcv_4h WHERE symbol=:s ORDER BY time DESC LIMIT {limit}"
        ),
        conn,
        params={"s": symbol},
    )
    return df["close"].astype(float).tolist()


def consecutive_up_from_closes(closes: list[float]) -> tuple[int, float]:
    """Return count of consecutive up closes and accumulated pct."""
    count = 0
    for i in range(len(closes) - 1):
        if closes[i + 1] < closes[i]:
            count += 1
        else:
            break
    pct = 0.0
    if count:
        start = closes[count]
        end = closes[0]
        pct = (end - start) / start * 100 if start else 0.0
    return count, pct


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
            closes = fetch_4h_closes(conn, sym)
            if len(closes) < 2:
                continue
            streak, _ = consecutive_up_from_closes(closes)
            latest_ts = conn.execute(
                text(
                    "SELECT time FROM ohlcv_4h WHERE symbol=:s ORDER BY time DESC LIMIT 1"
                ),
                {"s": sym},
            ).scalar()
            last_start = int(latest_ts) if latest_ts else 0
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
    """Reply with the top 10 symbols ranked by 4h up streak."""
    with engine_ohlcv.begin() as conn:
        syms = [r[0] for r in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_4h"))]
        if not syms:
            send_message("无4h数据", chat_id)
            return

        results: list[tuple[str, int, float]] = []
        for sym in syms:
            if sym.upper() in IGNORED_SYMBOLS:
                continue
            closes = fetch_4h_closes(conn, sym)
            if len(closes) < 2:
                continue
            cnt, pct = consecutive_up_from_closes(closes)
            if cnt:
                results.append((sym, cnt, pct))

        if not results:
            send_message("无有效数据", chat_id)
            return

        results.sort(key=lambda x: x[1], reverse=True)
        top10 = results[:10]
        table = pd.DataFrame(
            [
                {
                    "symbol": r[0].replace("USDT", ""),
                    "count": r[1],
                    "累计涨幅": f"{r[2]:.2f}%",
                }
                for r in top10
            ],
            columns=["symbol", "count", "累计涨幅"],
        )
        msg = "```\n" + ascii_table(table) + "\n```"
        send_message(msg, chat_id, parse_mode="Markdown")


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
        parts = text_msg.strip().split()
        if len(parts) >= 2:
            removeba_command(chat_id, parts[1])
        else:
            send_message("用法: /removeba SYMBOL", chat_id)
    elif text_msg.startswith("/addba"):
        parts = text_msg.strip().split()
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
