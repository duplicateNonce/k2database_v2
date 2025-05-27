import streamlit as st
from metrics.custom import load_saved_metrics, save_metrics


def render_metrics_editor():
    st.header("自定义指标编辑器")
    cms = load_saved_metrics()
    name = st.text_input("指标名称", key="met_name")
    expr = st.text_input("表达式", help="例如: current_price * volume_usd", key="met_expr")
    if st.button("保存", key="met_save"):
        if name and expr:
            cms[name] = expr
            save_metrics(cms)
            st.success("保存成功")
    if cms:
        st.subheader("已保存指标")
        for k, v in cms.items():
            st.write(f"- **{k}**: `{v}`")
