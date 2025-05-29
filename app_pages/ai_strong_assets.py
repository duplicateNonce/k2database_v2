import streamlit as st
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from query_history import add_entry, get_history
from utils import safe_rerun, short_time_range, update_shared_range, format_time_col
from result_cache import load_cached, save_cached
import pandas as pd
from grok_search import live_search_summary, x_search_summary, bubble_market_summary


def render_ai_strong_assets_page():
    st.header("AI-强势标的")

    api_key = st.text_input("Grok API Key", type="password", key="ai_sa_api_key")

    # —— 历史记录侧边栏 ——
    user = st.session_state.get("username", "default")
    history = get_history("ai_strong_assets", user)

    if "ai_sa_load_params" in st.session_state:
        params = st.session_state.pop("ai_sa_load_params")
        st.session_state["ai_sa_start_date"] = date.fromisoformat(params["start_date"])
        st.session_state["ai_sa_start_time"] = time.fromisoformat(params["start_time"])
        st.session_state["ai_sa_end_date"] = date.fromisoformat(params["end_date"])
        st.session_state["ai_sa_end_time"] = time.fromisoformat(params["end_time"])
        update_shared_range(
            st.session_state["ai_sa_start_date"],
            st.session_state["ai_sa_start_time"],
            st.session_state["ai_sa_end_date"],
            st.session_state["ai_sa_end_time"],
        )

    # 如果有共享时间范围，则作为默认值
    if "range_start_date" in st.session_state:
        st.session_state.setdefault(
            "ai_sa_start_date", st.session_state["range_start_date"]
        )
    if "range_start_time" in st.session_state:
        st.session_state.setdefault(
            "ai_sa_start_time", st.session_state["range_start_time"]
        )
    if "range_end_date" in st.session_state:
        st.session_state.setdefault("ai_sa_end_date", st.session_state["range_end_date"])
    if "range_end_time" in st.session_state:
        st.session_state.setdefault("ai_sa_end_time", st.session_state["range_end_time"])
    with st.sidebar.expander("历史记录", expanded=False):
        if history:
            labels = [
                short_time_range(
                    h["params"]["start_date"],
                    h["params"]["start_time"],
                    h["params"]["end_date"],
                    h["params"]["end_time"],
                )
                for h in history
            ]
            idx = st.selectbox(
                "选择记录",
                range(len(history)),
                format_func=lambda i: labels[i],
                key="ai_sa_hist_select",
            )
            if st.button("载入历史", key="ai_sa_hist_load"):
                st.session_state["ai_sa_load_params"] = history[idx]["params"]
                safe_rerun()
        else:
            st.write("暂无历史记录")

    # 时间选择（UTC+8）
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期",
            value=st.session_state.get(
                "ai_sa_start_date",
                st.session_state.get(
                    "range_start_date", date.today() - timedelta(days=7)
                ),
            ),
            key="ai_sa_start_date",
        )
        start_time = st.time_input(
            "开始时间",
            value=st.session_state.get(
                "ai_sa_start_time",
                st.session_state.get("range_start_time", time(0, 0)),
            ),
            key="ai_sa_start_time",
        )
    with col2:
        end_date = st.date_input(
            "结束日期",
            value=st.session_state.get(
                "ai_sa_end_date",
                st.session_state.get("range_end_date", date.today()),
            ),
            key="ai_sa_end_date",
        )
        end_time = st.time_input(
            "结束时间",
            value=st.session_state.get(
                "ai_sa_end_time",
                st.session_state.get("range_end_time", time(23, 59)),
            ),
            key="ai_sa_end_time",
        )

    if st.button("计算区间指标", key="ai_sa_btn"):
        # 合并日期时间并转毫秒，内部计算使用 UTC+8 时区
        tz = dt_timezone(timedelta(hours=8))
        start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
        end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        params = {
            "start_date": start_date.isoformat(),
            "start_time": start_time.isoformat(),
            "end_date": end_date.isoformat(),
            "end_time": end_time.isoformat(),
        }
        cache_id, df = load_cached("ai_strong_assets", params)
        if df is None:

            # 拉取所有 symbol 及对应标签
            with engine_ohlcv.connect() as conn:
                symbols = [
                    row[0]
                    for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv"))
                ]
                result = conn.execute(text("SELECT instrument_id, labels FROM instruments"))
                labels_map = {instr_id: labels for instr_id, labels in result}

            records = []
            for symbol in symbols:
                try:
                    m = compute_period_metrics(symbol, start_ts, end_ts)
                except ValueError:
                    continue
                m["symbol"] = symbol
                records.append(m)

            if not records:
                st.warning("该区间内无数据")
                return

            df = pd.DataFrame(records)
            save_cached("ai_strong_assets", params, df)
        else:
            with engine_ohlcv.connect() as conn:
                result = conn.execute(text("SELECT instrument_id, labels FROM instruments"))
                labels_map = {instr_id: labels for instr_id, labels in result}

        add_entry("ai_strong_assets", user, params, {"id": cache_id})
        update_shared_range(start_date, start_time, end_date, end_time)

        # 转换最高/最低价时间戳为 UTC+8 并格式化为 MM-DD HH:MM
        df["max_close_dt"] = format_time_col(df["max_close_dt"])
        df["min_close_dt"] = format_time_col(df["min_close_dt"])

        # 计算百分比并四舍五入
        df["period_return (%)"] = (df["period_return"] * 100).round(2)
        df["drawdown (%)"] = (df["drawdown"] * 100).round(2)

        # 插入标签列
        df["标签"] = df["symbol"].map(
            lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
        )

        # 重排和重命名列，并按期间收益降序排序
        df = (
            df[
                [
                    "标签",
                    "symbol",
                    "first_close",
                    "last_close",
                    "period_return (%)",
                    "max_close",
                    "max_close_dt",
                    "min_close",
                    "min_close_dt",
                    "drawdown (%)",
                ]
            ]
            .sort_values("period_return (%)", ascending=False)
            .reset_index(drop=True)
        )

        df = df.rename(
            columns={
                "symbol": "代币名字",
                "first_close": "时间1",
                "last_close": "时间2",
                "period_return (%)": "期间收益",
                "max_close": "期间最高价",
                "max_close_dt": "最高价时间",
                "min_close": "期间最低价",
                "min_close_dt": "最低价时间",
                "drawdown (%)": "最大回撤",
            }
        )

        # Cache result for later display outside this callback
        st.session_state["ai_sa_df"] = df

    df = st.session_state.get("ai_sa_df")
    if df is not None:
        st.dataframe(df, use_container_width=True)

        if not df.empty:
            symbol = st.selectbox(
                "选择代币查看归因", df["代币名字"], key="ai_sa_symbol"
            )
            if st.button("搜索涨幅归因", key="ai_sa_search"):
                with st.spinner("搜索中..."):
                    s_date = st.session_state.get("ai_sa_start_date")
                    e_date = st.session_state.get("ai_sa_end_date")
                    try:
                        url, summary = x_search_summary(
                            symbol, s_date, e_date, api_key=api_key
                        )
                    except Exception as exc:
                        st.error(f"搜索失败: {exc}")
                    else:
                        st.markdown(f"[在 X 上查看结果]({url})")
                        st.write(summary)

            if st.button("市场整体描述", key="ai_sa_market"):
                with st.spinner("搜索中..."):
                    try:
                        summary = bubble_market_summary(api_key=api_key)
                    except Exception as exc:
                        st.error(f"搜索失败: {exc}")
                    else:
                        st.write(summary)
