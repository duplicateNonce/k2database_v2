import json
import hashlib
from pathlib import Path

import streamlit as st

from config import USER_CREDENTIALS
from utils import safe_rerun

SALT = "@@@"

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


def _compute_token(username: str, password: str) -> str:
    """Return MD5(username + password + SALT)."""
    return hashlib.md5(f"{username}{password}{SALT}".encode()).hexdigest()


def _store_token(token: str) -> None:
    """Save token into localStorage."""
    st.components.v1.html(
        f"<script>window.localStorage.setItem('deviceToken', '{token}');</script>",
        height=0,
    )


def _ensure_token_param() -> str | None:
    """Return device token from query params or localStorage."""
    st.components.v1.html(
        """
        <script>
        (function() {
            const params = new URLSearchParams(window.location.search);
            const token = window.localStorage.getItem('deviceToken');
            if (!token) return;
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
            token = _compute_token(u, p)
            tokens[u] = token
            _save_tokens(tokens)
            _store_token(token)
            st.session_state["logged_in"] = True
            st.session_state["username"] = u
            safe_rerun()
        else:
            st.error("用户名或密码错误")
    return False


def logout() -> None:
    st.session_state.clear()
    _store_token("")
    safe_rerun()
