import streamlit as st
from datetime import datetime, time, timedelta
from strategies.bottom_lift import analyze_bottom_lift


def render_bottom_lift_page():
    st.header("底部抬升检测")

    t1_date = st.date_input("第一时间点日期", value=datetime.now().date())
    t1_time = st.time_input("第一时间点时间", value=time(0,0), key="bl_t1_time")
    t2_date = st.date_input("第二时间点日期", value=datetime.now().date())
    t2_time = st.time_input("第二时间点时间", value=time(0,0), key="bl_t2_time")
    factor = st.number_input("放大因子", min_value=1.0, value=100.0)

    if st.button("执行检测", key="bl_btn"):
        dt1 = datetime.combine(t1_date, t1_time)
        dt2 = datetime.combine(t2_date, t2_time)
        df = analyze_bottom_lift(dt1, dt2, factor)
        if df.empty:
            st.warning("无可用数据")
        else:
            st.dataframe(df)
