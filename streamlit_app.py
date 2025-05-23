import streamlit as st
from pages.overview import render_overview
from pages.history import render_history
from pages.metrics_editor import render_metrics_editor
from pages.ohlcv import render_ohlcv_page
from pages.strong_assets import render_strong_assets_page
from pages.bottom_lift import render_bottom_lift_page
from pages.long_short_analysis import render_long_short_analysis_page
from pages.label_assets import render_label_assets_page
from pages import rsi_data
from pages.hyperliquid_whale import render_hyperliquid_whale_page
from pages.price_change_ranking import render_price_change_page
from pages.price_change_by_label import render_price_change_by_label


PAGES = {
    "Overview": render_overview,
    "History": render_history,
    "Metrics Editor": render_metrics_editor,
    "OHLCV": render_ohlcv_page,
    "Strong Assets": render_strong_assets_page,
    "Bottom Lift": render_bottom_lift_page,
    "涨跌幅排行榜": render_price_change_page,
    "RSI Data": rsi_data.render_rsi_data_page, 
    "Hyperliquid 鲸鱼监控": render_hyperliquid_whale_page,
    "标签化涨跌幅": render_price_change_page,
}

PAGES["Long/Short Analysis"] = render_long_short_analysis_page
PAGES["Label Assets"] = render_label_assets_page
PAGES["价格&标签指标"] = render_price_change_by_label

def main():
    st.set_page_config(page_title="K2Database Monitor", layout="wide")
    choice = st.sidebar.radio("选择页面", list(PAGES.keys()))
    PAGES[choice]()

if __name__ == "__main__":
    main()
