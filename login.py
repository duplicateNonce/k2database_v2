import json
import uuid
from datetime import datetime
from pathlib import Path

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


def _generate_device_id() -> str:
    """Return a new random device identifier."""
    return uuid.uuid4().hex


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

    params = st.query_params
    return params.get("tok")


def require_login() -> bool:
    token = _ensure_token_param()
    tokens = _load_tokens()

    # automatic login if token matches a stored user
    if token:
        for name, info in tokens.items():
            if isinstance(info, dict):
                if info.get("device_id") == token:
                    st.session_state["logged_in"] = True
                    st.session_state["username"] = name
                    return True
            else:  # backward compatibility with old token format
                if info == token:
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
            if u in tokens:
                st.error("error 01")
            else:
                device_id = _generate_device_id()
                tokens[u] = {
                    "device_id": device_id,
                    "created": datetime.utcnow().isoformat(),
                }
                _save_tokens(tokens)
                _store_token(device_id)
                st.session_state["logged_in"] = True
                st.session_state["username"] = u
                safe_rerun()
        else:
            st.error("用户名或密码错误")
    return False


def logout() -> None:
    user = st.session_state.get("username")
    tokens = _load_tokens()
    if user and user in tokens:
        tokens.pop(user, None)
        _save_tokens(tokens)
    st.session_state.clear()
    _store_token("")
    safe_rerun()
