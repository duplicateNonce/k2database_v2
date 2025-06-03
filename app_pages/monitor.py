import streamlit as st
import pandas as pd
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text

from utils import safe_rerun

from db import engine_ohlcv
from config import TZ_NAME


IGNORED_SYMBOLS = {"USDCUSDT"}


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


def compute_p1(start_ts: int, end_ts: int) -> pd.DataFrame:
    with engine_ohlcv.begin() as conn:
        symbols = [
            row[0] for row in conn.execute(text("SELECT instrument_id FROM instruments"))
        ]
        records = []
        for sym in symbols:
            row = conn.execute(
                text(
                    "SELECT close, time FROM ohlcv "
                    "WHERE symbol=:s AND time BETWEEN :a AND :b "
                    "ORDER BY close DESC LIMIT 1"
                ),
                {"s": sym, "a": start_ts, "b": end_ts},
            ).fetchone()
            if not row:
                continue
            p1 = float(row.close)
            conn.execute(
                text(
                    """
                    INSERT INTO monitor_levels(symbol,start_ts,end_ts,p1,alerted)
                    VALUES(:sym,:start,:end,:p1,false)
                    ON CONFLICT(symbol) DO UPDATE
                      SET start_ts=excluded.start_ts,
                          end_ts=excluded.end_ts,
                          p1=excluded.p1,
                          alerted=false
                    """
                ),
                {"sym": sym, "start": start_ts, "end": end_ts, "p1": p1},
            )
            records.append({"symbol": sym, "p1": p1, "time": row.time})
    df = pd.DataFrame(records)
    if not df.empty:
        df["time"] = (
            pd.to_datetime(df["time"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
    return df


def render_monitor():
    ensure_table()
    st.header("Monitor")

    if "p1_locked" not in st.session_state:
        st.session_state["p1_locked"] = True
    locked = st.session_state["p1_locked"]

    lock_col1, lock_col2 = st.columns(2)
    with lock_col1:
        if locked:
            if st.button("解锁 P1"):
                st.session_state["p1_locked"] = False
        else:
            if st.button("锁定 P1"):
                st.session_state["p1_locked"] = True
    if locked:
        st.info("P1 已锁定，解锁后才能重新计算")

    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("开始日期", date.today() - timedelta(days=7))
        start_time = st.time_input("开始时间", time(0, 0))
    with col2:
        end_date = st.date_input("结束日期", date.today())
        end_time = st.time_input("结束时间", time(23, 59))

    if st.button("计算并保存 P1", disabled=locked):
        tz = dt_timezone(timedelta(hours=8))
        start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
        end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)
        df = compute_p1(start_ts, end_ts)
        if df.empty:
            st.warning("区间内无数据")
        else:
            st.dataframe(df)

    st.subheader("已保存的 P1")
    df_exist = pd.read_sql(
        "SELECT symbol, p1, start_ts, end_ts, alerted FROM monitor_levels", engine_ohlcv
    )
    if not df_exist.empty:
        df_exist["start_ts"] = (
            pd.to_datetime(df_exist["start_ts"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
        df_exist["end_ts"] = (
            pd.to_datetime(df_exist["end_ts"], unit="ms", utc=True)
            .dt.tz_convert(TZ_NAME)
            .dt.strftime("%Y-%m-%d %H:%M")
        )
    st.dataframe(df_exist)

    if not df_exist.empty:
        with st.expander("手动修改 P1", expanded=False):
            with st.form("edit_p1_form"):
                symbols = df_exist["symbol"].tolist()
                sel_sym = st.selectbox("选择标的", symbols)
                cur_p1 = float(df_exist.set_index("symbol").loc[sel_sym, "p1"])
                st.write(f"{sel_sym} 当前 P1: {cur_p1}")
                new_p1 = st.number_input("新 P1", value=cur_p1)
                sub = st.form_submit_button("更新", disabled=locked)
                if sub:
                    with engine_ohlcv.begin() as conn:
                        conn.execute(
                            text(
                                "UPDATE monitor_levels SET p1=:p, alerted=false WHERE symbol=:s"
                            ),
                            {"p": new_p1, "s": sel_sym},
                        )
                    safe_rerun()

    # ---- Manage hidden symbols ----
    with st.expander("管理隐藏的标的", expanded=False):
        with engine_ohlcv.begin() as conn:
            all_syms = [row[0] for row in conn.execute(text("SELECT instrument_id FROM instruments"))]
            hidden_df = pd.read_sql("SELECT symbol FROM ba_hidden", conn)
            hidden = hidden_df["symbol"].tolist() if not hidden_df.empty else []

        hide_sel = st.multiselect("隐藏标的", [s for s in all_syms if s not in hidden], key="hide_sel")
        if st.button("隐藏", key="hide_btn") and hide_sel:
            with engine_ohlcv.begin() as conn:
                for sym in hide_sel:
                    conn.execute(
                        text("INSERT INTO ba_hidden(symbol) VALUES(:s) ON CONFLICT DO NOTHING"),
                        {"s": sym},
                    )
            safe_rerun()

        unhide_sel = st.multiselect("取消隐藏", hidden, key="unhide_sel")
        if st.button("取消隐藏", key="unhide_btn") and unhide_sel:
            with engine_ohlcv.begin() as conn:
                for sym in unhide_sel:
                    conn.execute(text("DELETE FROM ba_hidden WHERE symbol=:s"), {"s": sym})
            safe_rerun()

    st.subheader("/ba 全量数据")
    if st.button("重新计算 /ba"):
        safe_rerun()
    df_ba = load_ba_data()
    if df_ba.empty:
        st.info("无有效数据")
    else:
        st.dataframe(df_ba, use_container_width=True)


def load_ba_data() -> pd.DataFrame:
    """Return /ba style ranking for all symbols."""
    with engine_ohlcv.begin() as conn:
        df_levels = pd.read_sql("SELECT symbol, p1 FROM monitor_levels", conn)
        hide = pd.read_sql("SELECT symbol FROM ba_hidden", conn)
        hidden = (
            set(s.upper() for s in hide["symbol"].tolist()) if not hide.empty else set()
        )
        labels = {
            r["instrument_id"]: r["labels"]
            for r in conn.execute(text("SELECT instrument_id, labels FROM instruments")).mappings()
        }

        rows: list[tuple[str, str, float, float, float]] = []
        for _, row in df_levels.iterrows():
            sym = row["symbol"]
            u_sym = sym.upper()
            if u_sym in IGNORED_SYMBOLS or u_sym in hidden:
                continue
            p1 = float(row["p1"])
            last = conn.execute(
                text("SELECT close FROM ohlcv WHERE symbol=:s ORDER BY time DESC LIMIT 1"),
                {"s": sym},
            ).fetchone()
            if not last:
                continue
            price = float(last[0])
            diff_pct = (price - p1) / p1 * 100 if p1 else 0.0
            lbl = labels.get(sym)
            if lbl is None:
                lbl_text = ""
            elif isinstance(lbl, list):
                lbl_text = "，".join(lbl)
            else:
                lbl_text = str(lbl)
            rows.append((sym, lbl_text, price, p1, diff_pct))
    if not rows:
        return pd.DataFrame()
    rows.sort(key=lambda x: x[4], reverse=True)
    df_res = pd.DataFrame(rows, columns=["symbol", "label", "price", "p1", "diff_pct"])
    df_res["symbol"] = df_res["symbol"].str.replace("USDT", "")
    df_res = df_res.rename(
        columns={
            "label": "标签",
            "symbol": "标的",
            "price": "现价",
            "p1": "区域最高价",
            "diff_pct": "差值%",
        }
    )
    df_res = df_res.round({"现价": 4, "区域最高价": 4, "差值%": 2})
    return df_res

