import streamlit as st
from datetime import datetime, timedelta, time
from datetime import timezone as dt_timezone
from config import TZ_NAME
from strategies.strong_assets import analyze_strong_assets

def render_strong_assets_page():
    st.header("强势资产选股 V4")

    # 基准符号输入
    benchmark = st.text_input("基准符号 (benchmark)", value="", 
                              help="如 BTCUSDT", key="sa_benchmark")
    # 聚合参数
    agg = st.number_input(
        "聚合条数 (每条 = 15 分钟)", min_value=1, value=4, step=1, key="sa_agg"
    )

    # 时间范围
    start_date = st.date_input(
        "开始日期", datetime.now() - timedelta(days=1), key="sa_start_date"
    )
    start_time = st.time_input(
        "开始时间", time(0, 0), key="sa_start_time"
    )
    end_date   = st.date_input(
        "结束日期", datetime.now(), key="sa_end_date"
    )
    end_time   = st.time_input(
        "结束时间", time(23, 59), key="sa_end_time"
    )

    if st.button("执行选股", key="sa_btn"):
        if not benchmark:
            st.error("请填写基准符号")
            return

        # 组合 datetime 并转成 ms 时间戳
        start_dt = datetime.combine(start_date, start_time).replace(
            tzinfo=dt_timezone(timedelta(hours=8))
        )
        end_dt   = datetime.combine(end_date,   end_time).replace(
            tzinfo=dt_timezone(timedelta(hours=8))
        )
        start_ts = int(start_dt.timestamp() * 1000)
        end_ts   = int(end_dt.timestamp() * 1000)

        # 调用策略
        df_scores = analyze_strong_assets(benchmark, start_ts, end_ts, agg)
        if df_scores.empty:
            st.warning("无可用数据")
        else:
            st.dataframe(df_scores, use_container_width=True)
