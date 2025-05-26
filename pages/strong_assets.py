import streamlit as st
from datetime import datetime, date, time, timedelta, timezone as dt_timezone
from sqlalchemy import text
from db import engine_ohlcv
from strategies.strong_assets import compute_period_metrics
from query_history import add_entry, get_history
import pandas as pd

def render_strong_assets_page():
    st.header("区间收益 & 回调 列表（所有资产）")

    # —— 历史记录侧边栏 ——
    history = get_history("strong_assets")
    with st.sidebar.expander("历史记录", expanded=False):
        if history:
            labels = [h["time"] for h in history]
            idx = st.selectbox("选择记录", range(len(history)), format_func=lambda i: labels[i], key="sa_hist_select")
            if st.button("载入历史", key="sa_hist_load"):
                params = history[idx]["params"]
                st.session_state["sa_start_date"] = date.fromisoformat(params["start_date"])
                st.session_state["sa_start_time"] = time.fromisoformat(params["start_time"])
                st.session_state["sa_end_date"] = date.fromisoformat(params["end_date"])
                st.session_state["sa_end_time"] = time.fromisoformat(params["end_time"])
                st.experimental_rerun()
        else:
            st.write("暂无历史记录")

    # 时间选择（UTC+8）
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input(
            "开始日期", date.today() - timedelta(days=7), key="sa_start_date"
        )
        start_time = st.time_input(
            "开始时间", time(0, 0), key="sa_start_time"
        )
    with col2:
        end_date = st.date_input(
            "结束日期", date.today(), key="sa_end_date"
        )
        end_time = st.time_input(
            "结束时间", time(23, 59), key="sa_end_time"
        )

    if st.button("计算区间指标", key="sa_btn"): 
        # 合并日期时间并转毫秒，内部计算使用 UTC+8 时区
        tz = dt_timezone(timedelta(hours=8))
        start_dt = datetime.combine(start_date, start_time).replace(tzinfo=tz)
        end_dt = datetime.combine(end_date, end_time).replace(tzinfo=tz)
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts = int(end_dt.timestamp() * 1000)

        # 记录查询参数
        add_entry("strong_assets", {
            "start_date": start_date.isoformat(),
            "start_time": start_time.isoformat(),
            "end_date": end_date.isoformat(),
            "end_time": end_time.isoformat(),
        })

        # 拉取所有 symbol 及对应标签
        with engine_ohlcv.connect() as conn:
            symbols = [row[0] for row in conn.execute(text(
                "SELECT DISTINCT symbol FROM ohlcv"
            ))]
            result = conn.execute(text(
                "SELECT instrument_id, labels FROM instruments"
            ))
            labels_map = {instr_id: labels for instr_id, labels in result}

        records = []
        for symbol in symbols:
            try:
                m = compute_period_metrics(symbol, start_ts, end_ts)
            except ValueError:
                continue
            m['symbol'] = symbol
            records.append(m)

        if not records:
            st.warning("该区间内无数据")
            return

        # 组织 DataFrame
        df = pd.DataFrame(records)

        # 转换最高/最低价时间戳为 UTC+8 并格式化为 MM-DD HH:MM
        df['max_close_dt'] = pd.to_datetime(df['max_close_dt'], unit='ms', utc=True) \
                            .dt.tz_convert('Asia/Shanghai') \
                            .dt.strftime('%m-%d %H:%M')
        df['min_close_dt'] = pd.to_datetime(df['min_close_dt'], unit='ms', utc=True) \
                            .dt.tz_convert('Asia/Shanghai') \
                            .dt.strftime('%m-%d %H:%M')

        # 计算百分比并四舍五入
        df['period_return (%)'] = (df['period_return'] * 100).round(2)
        df['drawdown (%)'] = (df['drawdown'] * 100).round(2)

        # 插入标签列
        df['标签'] = df['symbol'].map(lambda s: '，'.join(labels_map.get(s, [])) if labels_map.get(s) else '')

        # 重排和重命名列，并按期间收益降序排序
        df = df[[
            '标签',
            'symbol',
            'first_close',
            'last_close',
            'period_return (%)',
            'max_close',
            'max_close_dt',
            'min_close',
            'min_close_dt',
            'drawdown (%)'
        ]].sort_values('period_return (%)', ascending=False).reset_index(drop=True)

        df = df.rename(columns={
            'symbol': '代币名字',
            'first_close': '时间1',
            'last_close': '时间2',
            'period_return (%)': '期间收益',
            'max_close': '期间最高价',
            'max_close_dt': '最高价时间',
            'min_close': '期间最低价',
            'min_close_dt': '最低价时间',
            'drawdown (%)': '最大回撤'
        })

        # 展示结果表格
        st.dataframe(df, use_container_width=True)

