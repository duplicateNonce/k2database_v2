import os
from dotenv import load_dotenv

try:
    import streamlit as st
except ModuleNotFoundError:
    st = None

# Load variables from .env, overriding existing environment
load_dotenv(override=True)


def secret_get(key: str, default: str = ""):
    """Retrieve configuration from env vars or st.secrets.

    Any value pulled from the environment will be expanded so that
    references like ``${VAR}`` are resolved using existing variables.
    """
    val = os.getenv(key)
    if val is not None and val != "":
        return os.path.expandvars(val)
    if st:
        try:
            if key in st.secrets:
                secret_val = st.secrets[key]
                if isinstance(secret_val, str):
                    secret_val = os.path.expandvars(secret_val)
                return secret_val
        except Exception:
            # If Streamlit can't load secrets (e.g., running outside Streamlit),
            # just fall back to the provided default.
            pass
    return default

# 通用环境配置
DB_USER = secret_get("DB_USER", "postgres")
DB_PASSWORD = secret_get("DB_PASSWORD", "")
DB_HOST = secret_get("DB_HOST", "127.0.0.1")
DB_PORT = secret_get("DB_PORT", "5432")

# 数据库名称：支持同时连接两个库
COIN_DB_NAME = secret_get("COIN_DB_NAME", secret_get("DB_NAME", "postgres"))
OHLCV_DB_NAME = secret_get("OHLCV_DB_NAME", COIN_DB_NAME)

# 业务所需 API Key
CG_API_KEY = secret_get("CG_API_KEY")

# 时区常量
TZ_NAME = "Asia/Shanghai"

# 登录凭据（从环境变量读取）
# 支持在 APP_USERS 中使用 "user:pass" 列表，逗号分隔
_multi = secret_get("APP_USERS")
if not _multi and st:
    try:
        if "app_users" in st.secrets:
            _multi = ",".join(f"{k}:{v}" for k, v in st.secrets["app_users"].items())
    except Exception:
        pass

USER_CREDENTIALS = {}
if _multi:
    for pair in _multi.split(","):
        pair = pair.strip()
        if ":" in pair:
            user, pwd = pair.split(":", 1)
            USER_CREDENTIALS[user.strip()] = pwd.strip()
else:
    # 兼容单用户模式
    u = secret_get("APP_USER")
    p = secret_get("APP_PASSWORD")
    if u and p:
        USER_CREDENTIALS[u] = p


def load_proxy_env() -> None:
    """Populate proxy related environment variables from configuration."""
    keys = (
        "ALL_PROXY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "all_proxy",
        "http_proxy",
        "https_proxy",
    )
    for key in keys:
        val = secret_get(key)
        if val:
            os.environ[key] = val


def get_proxy_dict() -> dict:
    """Return a requests ``proxies`` mapping derived from the environment."""
    http = os.getenv("http_proxy") or os.getenv("HTTP_PROXY")
    https = os.getenv("https_proxy") or os.getenv("HTTPS_PROXY")
    all_p = os.getenv("all_proxy") or os.getenv("ALL_PROXY")
    proxies = {}
    if all_p:
        proxies["http"] = all_p
        proxies["https"] = all_p
    else:
        if http:
            proxies["http"] = http
        if https:
            proxies["https"] = https
    return proxies


# Load proxy env vars on import so other modules just need to import config
load_proxy_env()

# Telegram configuration
TELEGRAM_BOT_TOKEN = secret_get("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = secret_get("TELEGRAM_CHAT_ID")
