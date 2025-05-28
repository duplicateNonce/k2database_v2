import json
from pathlib import Path
from uuid import uuid4

import streamlit as st

from config import USER_CREDENTIALS
from utils import safe_rerun

TOKEN_FILE = Path("data/tokens.json")


def _load_tokens() -> dict:
    if TOKEN_FILE.exists():
        try:
            return json.loads(TOKEN_FILE.read_text())
        except Exception:
            pass
    return {}


def _save_tokens(tokens: dict) -> None:
    TOKEN_FILE.parent.mkdir(parents=True, exist_ok=True)
    TOKEN_FILE.write_text(json.dumps(tokens))


def _ensure_token_param() -> str | None:
    """Return device token from query params or localStorage."""
    st.components.v1.html(
        """
        <script>
        (function() {
            const params = new URLSearchParams(window.location.search);
            let token = window.localStorage.getItem('deviceToken');
            if (!token) {
                token = Math.random().toString(36).substring(2) + Date.now().toString(36);
                window.localStorage.setItem('deviceToken', token);
            }
            if (params.get('tok') !== token) {
                params.set('tok', token);
                window.location.search = params.toString();
            }
        })();
        </script>
        """,
        height=0,
    )

    params = st.experimental_get_query_params()
    return params.get("tok", [None])[0]


def require_login() -> bool:
    token = _ensure_token_param()
    tokens = _load_tokens()

    # automatic login if token matches a stored user
    if token:
        for name, t in tokens.items():
            if t == token:
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
            existing = tokens.get(u)
            if existing and existing != token:
                st.error("ERROR 01")
                return False
            if not existing:
                token = token or uuid4().hex
                tokens[u] = token
                _save_tokens(tokens)
            st.session_state["logged_in"] = True
            st.session_state["username"] = u
            safe_rerun()
        else:
            st.error("用户名或密码错误")
    return False


def logout() -> None:
    st.session_state.clear()
    safe_rerun()
