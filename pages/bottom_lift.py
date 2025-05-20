import streamlit as st
from datetime import datetime
from strategies.bottom_lift import analyze_bottom_lift


def render_bottom_lift_page():
    st.title("Bottom Lift 分析")

    # 布局：左右两栏输入时间点
    col1, col2 = st.columns(2)
    with col1:
        t1_date = st.date_input("T1 日期", key="t1_date")
        t1_time = st.time_input("T1 时间", key="t1_time")
    with col2:
        t2_date = st.date_input("T2 日期", key="t2_date")
        t2_time = st.time_input("T2 时间", key="t2_time")

    # 可调参数：bars 和 factor
    bars = st.number_input("± N 根 K 线 (bars)", min_value=1, value=4, step=1)
    factor = st.number_input("放大因子 (factor)", min_value=1.0, value=100.0, step=1.0)

    if st.button("运行分析"):
        t1 = datetime.combine(t1_date, t1_time)
        t2 = datetime.combine(t2_date, t2_time)
        df = analyze_bottom_lift(t1, t2, bars=bars, factor=factor)
        st.dataframe(df)
