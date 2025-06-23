import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from app_pages.price_change_by_label import (
    get_mappings,
    compute_period_metrics as label_compute,
)
from utils import (
    update_shared_range,
    safe_rerun,
    short_time_range,
    format_time_col,
    quick_range_buttons,
)
from query_history import add_entry, get_history
from result_cache import load_cached, save_cached
from app_pages.watchlist import load_watchlist, save_watchlist
from label_watchlist import load_label_watchlist, save_label_watchlist


def render_combined_page():
    st.title("综合分析")

    user = st.session_state.get("username", "default")
    history = get_history("combined_analysis", user)

    if "combo_load_params" in st.session_state:
        params = st.session_state.pop("combo_load_params")
        st.session_state["combo_start_date"] = date.fromisoformat(params["start_date"])
        st.session_state["combo_start_time"] = time.fromisoformat(params["start_time"])
        st.session_state["combo_end_date"] = date.fromisoformat(params["end_date"])
        st.session_state["combo_end_time"] = time.fromisoformat(params["end_time"])
        update_shared_range(
            st.session_state["combo_start_date"],
            st.session_state["combo_start_time"],
            st.session_state["combo_end_date"],
            st.session_state["combo_end_time"],
        )

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
                key="combo_hist_select",
            )
            if st.button("载入历史", key="combo_hist_load"):
                st.session_state["combo_load_params"] = history[idx]["params"]
                safe_rerun()
        else:
            st.write("暂无历史记录")

    # 日期时间输入，只需填写一次
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期",
            st.session_state.get(
                "combo_start_date",
                st.session_state.get("range_start_date", date.today() - timedelta(days=7)),
            ),
            key="combo_start_date",
        )
        start_time = st.time_input(
            "开始时间",
            st.session_state.get(
                "combo_start_time",
                st.session_state.get("range_start_time", time(0, 0)),
            ),
            key="combo_start_time",
        )
    with col2:
        end_date = st.date_input(
            "结束日期",
            st.session_state.get(
                "combo_end_date",
                st.session_state.get("range_end_date", date.today()),
            ),
            key="combo_end_date",
        )
        end_time = st.time_input(
            "结束时间",
            st.session_state.get(
                "combo_end_time",
                st.session_state.get("range_end_time", time(23, 59)),
            ),
            key="combo_end_time",
        )

    quick_range_buttons(
        "combo_start_date",
        "combo_start_time",
        "combo_end_date",
        "combo_end_time",
    )

    tz = dt_timezone(timedelta(hours=8))
    start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
    end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
    start_ts = int(start_dt.timestamp() * 1000)
    end_ts = int(end_dt.timestamp() * 1000)

    update_shared_range(start_date, start_time, end_date, end_time)

    # ---- Choose symbols to analyse ----
    with engine_ohlcv.connect() as conn:
        all_symbols = [row[0] for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))]

    sel_symbols = st.multiselect(
        "选择自选标的",
        all_symbols,
        default=st.session_state.get("combo_sel_symbols", load_watchlist()),
        key="combo_sel_symbols",
    )
    if st.button("保存自选标的", key="combo_save_sel"):
        save_watchlist(sel_symbols)
        st.success("已保存自选标的")


    # These parameters are needed for history logging below. They are
    # normally collected later from widgets, but may not be available when
    # running the strong assets section first. Fetch them from session
    # state with defaults so they are always defined.
    run_all = st.button("一键分析", key="combo_all_btn")

    history_params = {
        "start_date": start_date.isoformat(),
        "start_time": start_time.isoformat(),
        "end_date": end_date.isoformat(),
        "end_time": end_time.isoformat(),
    }
    history_extra = {}

    st.subheader("强势标的筛选")
    run_sa = st.button("计算强势标的", key="combo_sa_btn")
    if run_sa or run_all:
        sa_params = {
            "start_date": start_date.isoformat(),
            "start_time": start_time.isoformat(),
            "end_date": end_date.isoformat(),
            "end_time": end_time.isoformat(),
        }
        # Always compute using all symbols so the full list is available
        with engine_ohlcv.connect() as conn:
            symbols = [
                row[0]
                for row in conn.execute(text("SELECT DISTINCT symbol FROM ohlcv_1h"))
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
        sa_cache_id = save_cached("strong_assets", sa_params, df)

        history_extra["sa_id"] = sa_cache_id
        if not run_all:
            add_entry(
                "combined_analysis",
                user,
                history_params,
                {"sa_id": sa_cache_id},
            )
        df["max_close_dt"] = format_time_col(df["max_close_dt"])
        df["min_close_dt"] = format_time_col(df["min_close_dt"])
        df["period_return (%)"] = (df["period_return"] * 100).round(2)
        df["drawdown (%)"] = (df["drawdown"] * 100).round(2)
        df["标签"] = df["symbol"].map(
            lambda s: "，".join(labels_map.get(s, [])) if labels_map.get(s) else ""
        )
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
        start_label = start_dt.strftime("%m-%d %H:%M")
        end_label = end_dt.strftime("%m-%d %H:%M")
        df = df.rename(
            columns={
                "symbol": "代币名字",
                "first_close": start_label,
                "last_close": end_label,
                "period_return (%)": "期间收益",
                "max_close": "期间最高价",
                "max_close_dt": "最高价时间",
                "min_close": "期间最低价",
                "min_close_dt": "最低价时间",
                "drawdown (%)": "最大回撤",
            }
        )
        df["期间收益"] = df["期间收益"].map("{:.2f}%".format)
        st.session_state["combo_sa_df"] = df

    if "combo_sa_df" in st.session_state:
        df = st.session_state["combo_sa_df"]
        st.dataframe(df, use_container_width=True)
        watch_syms = st.session_state.get("combo_sel_symbols", [])
        if watch_syms:
            sub = df[df["代币名字"].isin(watch_syms)]
            if not sub.empty:
                st.subheader("自选标的表现")
                st.dataframe(sub, use_container_width=True)
            else:
                st.info("自选标的不在当前结果中")

    st.subheader("标签化涨跌幅")
    run_label = st.button("计算标签涨跌幅", key="combo_label_btn")
    if run_label or run_all:
        label_params = {
            "start_date": start_date.isoformat(),
            "start_time": start_time.isoformat(),
            "end_date": end_date.isoformat(),
            "end_time": end_time.isoformat(),
        }
        label_cache_id, df = load_cached("price_change_by_label", label_params)
        if df is None:
            df_map = get_mappings()
            symbols = df_map["symbol"].unique()
            recs = []
            for sym in symbols:
                ret, _ = label_compute(sym, start_ts, end_ts)
                if ret is None:
                    continue
                lbls = df_map.loc[df_map["symbol"] == sym, "label"]
                for lbl in lbls:
                    recs.append({"symbol": sym, "label": lbl, "return": ret})
            df = pd.DataFrame(recs)
            if df.empty:
                st.warning("无有效数据")
                return
            rmin, rmax = df["return"].min(), df["return"].max()
            low = (rmin * 100 // 5) * 5 / 100
            high = (rmax * 100 + 4) // 5 * 5 / 100
            bins = np.arange(low, high + 0.0001, 0.05)
            bucket_labels = [
                f"{int(l*100)}%~{int(u*100)}%" for l, u in zip(bins[:-1], bins[1:])
            ]
            df["bucket"] = pd.cut(df["return"], bins=bins, labels=bucket_labels, include_lowest=True)
            save_cached("price_change_by_label", label_params, df)

        history_extra["label_id"] = label_cache_id
        if not run_all:
            add_entry(
                "combined_analysis",
                user,
                history_params,
                {"label_id": label_cache_id},
            )

        rmin, rmax = df["return"].min(), df["return"].max()
        low = (rmin * 100 // 5) * 5 / 100
        high = (rmax * 100 + 4) // 5 * 5 / 100
        bins = np.arange(low, high + 0.0001, 0.05)
        bucket_labels = [
            f"{int(l*100)}%~{int(u*100)}%" for l, u in zip(bins[:-1], bins[1:])
        ]
        df["bucket"] = pd.cut(
            df["return"], bins=bins, labels=bucket_labels, include_lowest=True
        )
        grp = (
            df.groupby(["label", "bucket"])["symbol"]
            .nunique()
            .reset_index(name="count")
        )
        pivot = (
            grp.pivot(index="label", columns="bucket", values="count")
            .fillna(0)
            .astype(int)
        )
        pivot = pivot.loc[:, pivot.sum(axis=0) > 0]
        styled = pivot.style.format(lambda v: "" if v == 0 else v)

        stats = (
            df.groupby("label")["return"]
            .agg(["mean", "median"])
            .reset_index()
            .rename(columns={"label": "标签", "mean": "平均涨幅", "median": "中位数涨幅"})
        )
        stats["平均涨幅"] = stats["平均涨幅"].map("{:.2%}".format)
        stats["中位数涨幅"] = stats["中位数涨幅"].map("{:.2%}".format)
        st.session_state["combo_label_df"] = df
        st.session_state["combo_label_pivot"] = pivot
        st.session_state["combo_label_stats"] = stats
        if run_all:
            add_entry("combined_analysis", user, history_params, history_extra)

    if "combo_label_stats" in st.session_state:
        pivot = st.session_state["combo_label_pivot"]
        stats = st.session_state["combo_label_stats"]
        df = st.session_state["combo_label_df"]
        styled = pivot.style.format(lambda v: "" if v == 0 else v)
        st.dataframe(styled, use_container_width=True)
        st.dataframe(stats, use_container_width=True)

        options = stats["标签"].tolist()
        selected = st.multiselect("选择标签", options, key="combo_lbl_select")
        if selected:
            st.subheader("自选标签表现")
            sel = stats[stats["标签"].isin(selected)]
            st.dataframe(sel, use_container_width=True)

        watch_labels = load_label_watchlist()
        with st.expander("管理关注标签", expanded=False):
            add_label = st.selectbox(
                "添加标签",
                [l for l in options if l not in watch_labels],
                key="combo_add_label",
            )
            if st.button("添加标签", key="combo_add_label_btn"):
                if add_label and add_label not in watch_labels:
                    watch_labels.append(add_label)
                    save_label_watchlist(watch_labels)
                    st.success(f"已添加 {add_label}")
                    safe_rerun()
            if watch_labels:
                for lbl in watch_labels:
                    if st.button(f"删除 {lbl}", key=f"combo_del_label_{lbl}"):
                        watch_labels.remove(lbl)
                        save_label_watchlist(watch_labels)
                        safe_rerun()
            else:
                st.write("暂无关注标签")

        if watch_labels:
            sel_lbl = stats[stats["标签"].isin(watch_labels)]
            if not sel_lbl.empty:
                st.subheader("关注标签表现")
                st.dataframe(sel_lbl, use_container_width=True)
            else:
                st.info("关注标签未出现在当前结果中")

        for bucket in pivot.columns[::-1]:
            df_b = df[df["bucket"] == bucket]
            if df_b.empty:
                continue
            with st.expander(f"{bucket} （共 {df_b['symbol'].nunique()} 个标的）"):
                df_show = (
                    df_b.groupby("symbol")
                    .agg({
                        "label": lambda x: "，".join(sorted(set(x))),
                        "return": "first",
                    })
                    .reset_index()
                )
                df_show["return"] = df_show["return"].map("{:.2%}".format)
                st.dataframe(
                    df_show.sort_values(["return", "symbol"], ascending=[False, True]),
                    use_container_width=True,
                )

    if "combo_sa_df" in st.session_state:
        watch_syms = load_watchlist()
        with st.expander("管理关注标的", expanded=False):
            add_sym = st.selectbox(
                "添加标的",
                [s for s in all_symbols if s not in watch_syms],
                key="combo_add_sym",
            )
            if st.button("添加", key="combo_add_sym_btn"):
                if add_sym and add_sym not in watch_syms:
                    watch_syms.append(add_sym)
                    save_watchlist(watch_syms)
                    st.success(f"已添加 {add_sym}")
                    safe_rerun()
            if watch_syms:
                for sym in watch_syms:
                    if st.button(f"删除 {sym}", key=f"combo_del_sym_{sym}"):
                        watch_syms.remove(sym)
                        save_watchlist(watch_syms)
                        safe_rerun()
            else:
                st.write("暂无关注标的")

        if watch_syms:
            dfw = st.session_state["combo_sa_df"]
            sel = dfw[dfw["代币名字"].isin(watch_syms)]
            if not sel.empty:
                st.subheader("关注标的表现")
                st.dataframe(sel, use_container_width=True)
            else:
                st.info("关注标的未出现在当前结果中")
