import streamlit as st
from app_pages.overview import render_overview
from app_pages.ohlcv import render_ohlcv_page
from app_pages.ai_strong_assets import render_ai_strong_assets_page
from app_pages.bottom_lift import render_bottom_lift_page
from app_pages.label_assets import render_label_assets_page
# from app_pages.price_change_by_label import render_price_change_by_label
from app_pages.combined_analysis import render_combined_page
from app_pages.watchlist import render_watchlist_page
from app_pages.history_rank import render_history_rank
from app_pages.monitor import render_monitor
from app_pages.long_short_analysis import render_long_short_analysis_page
from app_pages.pct_change_rank import render_pct_change_rank_page
from app_pages.airank import render_ai_rank_page

from login import require_login, logout
from utils import safe_rerun

PAGES = {
    "Overview": render_overview,
    "OHLCV": render_ohlcv_page,
    "历史排名": render_history_rank,
    "百分化涨幅排名": render_pct_change_rank_page,
    "涨幅归因": render_ai_rank_page,
    "综合分析": render_combined_page,
    "AI-强势标的": render_ai_strong_assets_page,
    "底部抬升筛选": render_bottom_lift_page,
    # "标签化涨跌幅": render_price_change_by_label,
    "自选标的": render_watchlist_page,
    "Monitor": render_monitor,
    "多空持仓分析": render_long_short_analysis_page,
}
# 动态添加
PAGES["编辑标的标签"] = render_label_assets_page

def main():
    st.set_page_config(page_title="K2Database Monitor", layout="wide")
    if not require_login():
        return

    if st.sidebar.button("退出登录"):
        logout()

    choice = st.sidebar.radio("选择页面", list(PAGES.keys()))
    PAGES[choice]()

if __name__ == "__main__":
    main()
