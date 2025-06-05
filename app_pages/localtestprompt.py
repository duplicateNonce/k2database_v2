import json
import time
from pathlib import Path
import streamlit as st

from prompt_manager import get_prompt, save_prompt, list_versions
from grok_api import ask_xai

CACHE_FILE = Path("data/localprompt_cache.json")


def _load_cache() -> dict:
    if CACHE_FILE.exists():
        try:
            return json.loads(CACHE_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_cache(cache: dict) -> None:
    CACHE_FILE.parent.mkdir(parents=True, exist_ok=True)
    CACHE_FILE.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _run_prompt(symbol: str, label: str, version: str, prompt: str) -> str:
    cache = _load_cache()
    hour_key = int(time.time() // 3600)
    entry = cache.get(symbol)
    if entry and entry.get("version") == version and entry.get("hour") == hour_key:
        return entry.get("answer", "")
    answer = ask_xai(prompt)
    cache[symbol] = {"version": version, "hour": hour_key, "answer": answer}
    _save_cache(cache)
    return answer


def render_localtestprompt_page() -> None:
    st.title("Prompt \u5c55\u793a\u6d4b\u8bd5")
    symbol = st.text_input("\u4ee3\u5e01", "BTCUSDT")
    label = st.text_input("\u6807\u7b7e", "")

    versions = list_versions("localtestprompt")
    if not versions:
        versions = ["v1"]

    col_a, col_b = st.columns(2)

    with col_a:
        ver_a = st.selectbox("Prompt A \u7248\u672c", versions, index=len(versions) - 1)
        _, default_a = get_prompt("localtestprompt", ver_a)
        prompt_a = st.text_area("Prompt A", value=default_a, height=200)
        if st.button("\u4fdd\u5b58A", key="save_a"):
            save_prompt("localtestprompt", ver_a, prompt_a)
        if st.button("\u8fd0\u884cA", key="run_a"):
            formatted = prompt_a.format(search_symbol=symbol.replace("USDT", ""), label=label or "无")
            ans = _run_prompt(symbol + "_A", label, ver_a, formatted)
            st.markdown(ans)

    with col_b:
        ver_b = st.selectbox("Prompt B \u7248\u672c", versions, index=len(versions) - 1, key="ver_b")
        _, default_b = get_prompt("localtestprompt", ver_b)
        prompt_b = st.text_area("Prompt B", value=default_b, height=200, key="prompt_b")
        if st.button("\u4fdd\u5b58B", key="save_b"):
            save_prompt("localtestprompt", ver_b, prompt_b)
        if st.button("\u8fd0\u884cB", key="run_b"):
            formatted = prompt_b.format(search_symbol=symbol.replace("USDT", ""), label=label or "无")
            ans = _run_prompt(symbol + "_B", label, ver_b, formatted)
            st.markdown(ans)
