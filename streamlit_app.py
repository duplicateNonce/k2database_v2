import streamlit as st
from app_pages.overview import render_overview
from app_pages.ohlcv import render_ohlcv_page
# from app_pages.strong_assets import render_strong_assets_page
# from app_pages.bottom_lift import render_bottom_lift_page
from app_pages.label_assets import render_label_assets_page
# from app_pages.price_change_by_label import render_price_change_by_label
from app_pages.combined_analysis import render_combined_page
from app_pages.watchlist import render_watchlist_page

# 使用 codex 分支中新加的登录凭证和 rerun 工具
from config import USER_CREDENTIALS
from utils import safe_rerun
import json
from uuid import uuid4
from pathlib import Path
import hashlib

PAGES = {
    "Overview": render_overview,
    "OHLCV": render_ohlcv_page,
    "综合分析": render_combined_page,
    # "强势标的筛选": render_strong_assets_page,
    # "底部抬升筛选": render_bottom_lift_page,
    # "标签化涨跌幅": render_price_change_by_label,
    "自选跟踪": render_watchlist_page,
    "自选标的": render_watchlist_page,
}
# 动态添加
PAGES["编辑标的标签"] = render_label_assets_page

FINGERPRINT_FILE = Path("data/fingerprints.json")


def load_fingerprints() -> dict:
    if FINGERPRINT_FILE.exists():
        try:
            return json.loads(FINGERPRINT_FILE.read_text())
        except Exception:
            pass
    return {}


def save_fingerprints(fp_dict: dict) -> None:
    FINGERPRINT_FILE.parent.mkdir(parents=True, exist_ok=True)
    FINGERPRINT_FILE.write_text(json.dumps(fp_dict))

def require_login() -> bool:
    """Login with optional fingerprint auto-login."""

    # If a device ID exists in localStorage but not in query params,
    # set the param and reload so Python can access it
    st.components.v1.html(
        """
        <script>
        (function() {
            const key = 'deviceId';
            const params = new URLSearchParams(window.location.search);
            const stored = window.localStorage.getItem(key);
            if (stored && !params.get('fp')) {
                params.set('fp', stored);
                window.location.search = params.toString();
            }
        })();
        </script>
        """,
        height=0,
    )

    # Try automatic login via fingerprint in query params
    params = st.query_params
    fp_param = params.get("fp")
    fingerprints = load_fingerprints()
    if fp_param:
        for name, fp in fingerprints.items():
            if fp == fp_param:
                st.session_state["logged_in"] = True
                st.session_state["username"] = name
                return True

    if st.session_state.get("logged_in"):
        return True

    st.title("登录")
    u = st.text_input("用户名")
    p = st.text_input("密码", type="password")
    if st.button("登录"):
        if USER_CREDENTIALS.get(u) == p:
            existing_fp = fingerprints.get(u)
            if existing_fp and existing_fp != fp_param:
                st.error("ERROR 01")
                return False
            if not existing_fp:
                raw_id = fp_param or uuid4().hex
                fp_param = hashlib.md5(raw_id.encode()).hexdigest()
                fingerprints[u] = fp_param
                save_fingerprints(fingerprints)
            st.session_state["logged_in"] = True
            st.session_state["username"] = u
            st.components.v1.html(
                f"""
                <script>
                const params = new URLSearchParams(window.location.search);
                params.set('fp', '{fp_param}');
                window.localStorage.setItem('deviceId', '{fp_param}');
                window.location.search = params.toString();
                </script>
                """,
                height=0,
            )
            st.stop()
        else:
            st.error("用户名或密码错误")
    return False

def main():
    st.set_page_config(page_title="K2Database Monitor", layout="wide")
    if not require_login():
        return

    if st.sidebar.button("退出登录"):
        st.session_state.clear()
        safe_rerun()

    choice = st.sidebar.radio("选择页面", list(PAGES.keys()))
    PAGES[choice]()

if __name__ == "__main__":
    main()
