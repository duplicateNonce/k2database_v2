import streamlit as st
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.bottom_lift import analyze_bottom_lift
import pandas as pd
from query_history import add_entry, get_history
from utils import safe_rerun, short_time_range, update_shared_range
from result_cache import load_cached, save_cached


def render_bottom_lift_page():
    st.title("底部抬升筛选")

    # —— 历史记录 ——
    user = st.session_state.get("username", "default")
    history = get_history("bottom_lift", user)

    if "bl_load_params" in st.session_state:
        params = st.session_state.pop("bl_load_params")
        st.session_state["t1_date"] = date.fromisoformat(params["t1_date"])
        st.session_state["t1_time"] = time.fromisoformat(params["t1_time"])
        st.session_state["t2_date"] = date.fromisoformat(params["t2_date"])
        st.session_state["t2_time"] = time.fromisoformat(params["t2_time"])
        update_shared_range(
            st.session_state["t1_date"],
            st.session_state["t1_time"],
            st.session_state["t2_date"],
            st.session_state["t2_time"],
        )

    # 如果有共享时间范围，则作为默认值
    if "range_start_date" in st.session_state:
        st.session_state.setdefault("t1_date", st.session_state["range_start_date"])
    if "range_start_time" in st.session_state:
        st.session_state.setdefault("t1_time", st.session_state["range_start_time"])
    if "range_end_date" in st.session_state:
        st.session_state.setdefault("t2_date", st.session_state["range_end_date"])
    if "range_end_time" in st.session_state:
        st.session_state.setdefault("t2_time", st.session_state["range_end_time"])
    with st.sidebar.expander("历史记录", expanded=False):
        if history:
            labels = [
                short_time_range(
                    h["params"]["t1_date"],
                    h["params"]["t1_time"],
                    h["params"]["t2_date"],
                    h["params"]["t2_time"],
                )
                for h in history
            ]
            idx = st.selectbox(
                "选择记录",
                range(len(history)),
                format_func=lambda i: labels[i],
                key="bl_hist_select",
            )
            if st.button("载入历史", key="bl_hist_load"):
                st.session_state["bl_load_params"] = history[idx]["params"]
                safe_rerun()
        else:
            st.write("暂无历史记录")

    # 布局：左右两栏输入时间点
    col1, col2 = st.columns(2)
    with col1:
        t1_date = st.date_input(
            "T1 日期",
            value=st.session_state.get(
                "t1_date",
                st.session_state.get("range_start_date", date.today()),
            ),
            key="t1_date",
        )
        t1_time = st.time_input(
            "T1 时间",
            value=st.session_state.get(
                "t1_time",
                st.session_state.get("range_start_time", time(0, 0)),
            ),
            key="t1_time",
        )
    with col2:
        t2_date = st.date_input(
            "T2 日期",
            value=st.session_state.get(
                "t2_date",
                st.session_state.get("range_end_date", date.today()),
            ),
            key="t2_date",
        )
        t2_time = st.time_input(
            "T2 时间",
            value=st.session_state.get(
                "t2_time",
                st.session_state.get("range_end_time", time(23, 59)),
            ),
            key="t2_time",
        )

    # 可调参数：bars (窗口大小) 和 factor (放大因子)
    bars = st.number_input("± N 根 K 线 (bars)", min_value=1, value=4, step=1)
    factor = st.number_input("放大因子 (factor)", min_value=1.0, value=100.0, step=1.0)

    if st.button("运行分析"):
        # 合并日期时间并设定为 UTC+8 时区
        tz = dt_timezone(timedelta(hours=8))
        t1 = datetime.combine(t1_date, t1_time).replace(tzinfo=tz)
        t2 = datetime.combine(t2_date, t2_time).replace(tzinfo=tz)

        params = {
            "t1_date": t1_date.isoformat(),
            "t1_time": t1_time.isoformat(),
            "t2_date": t2_date.isoformat(),
            "t2_time": t2_time.isoformat(),
            "bars": bars,
            "factor": factor,
        }
        cache_id, df = load_cached("bottom_lift", params)
        if df is None:

            # 执行分析
            df = analyze_bottom_lift(t1, t2, bars=bars, factor=factor)
            if df.empty:
                st.warning("无符合条件的数据")
                return
            save_cached("bottom_lift", params, df)
        add_entry("bottom_lift", user, params, {"id": cache_id})
        update_shared_range(t1_date, t1_time, t2_date, t2_time)

        # 获取标签映射
        with engine_ohlcv.connect() as conn:
            result = conn.execute(text("SELECT instrument_id, labels FROM instruments"))
            labels_map = {instr_id: labels for instr_id, labels in result}

        # 在 DataFrame 中插入标签列
        df["标签"] = df.index.map(
            lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
        )

        # 格式化 L1_time 与 L2_time 为 MM-DD HH:MM
        df["L1_time"] = (
            pd.to_datetime(df["L1_time"], utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.strftime("%m-%d %H:%M")
        )
        df["L2_time"] = (
            pd.to_datetime(df["L2_time"], utc=True)
            .dt.tz_convert("Asia/Shanghai")
            .dt.strftime("%m-%d %H:%M")
        )

        # 重置索引，将 symbol 列加入 DataFrame
        df = df.reset_index()

        # 重排列顺序并重命名 symbol
        df = df[["标签", "symbol", "L1_time", "L1_low", "L2_time", "L2_low", "slope"]]
        df = df.rename(columns={"symbol": "代币名字"})

        # 按 slope 降序排序
        df = df.sort_values("slope", ascending=False).reset_index(drop=True)

        # 展示结果表格，动态调整高度以显示所有行
        row_height = 35  # approximate pixels per row
        height = row_height * (len(df) + 1)
        st.dataframe(df, use_container_width=True, height=height)
