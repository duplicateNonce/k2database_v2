import os
from dotenv import load_dotenv

load_dotenv()

# 通用环境配置
DB_USER     = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_HOST     = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT     = os.getenv("DB_PORT", "5432")

# 数据库名称：支持同时连接两个库
COIN_DB_NAME  = os.getenv("COIN_DB_NAME", os.getenv("DB_NAME", "postgres"))
OHLCV_DB_NAME = os.getenv("OHLCV_DB_NAME", COIN_DB_NAME)

# 业务所需 API Key
CG_API_KEY = os.getenv("CG_API_KEY")

# 时区常量
TZ_NAME = "Asia/Shanghai"

# 登录凭据（从环境变量读取）
# 支持在 APP_USERS 中使用 "user:pass" 列表，逗号分隔
_multi = os.getenv("APP_USERS")
if _multi:
    USER_CREDENTIALS = {}
    for pair in _multi.split(","):
        if ":" in pair:
            user, pwd = pair.split(":", 1)
            USER_CREDENTIALS[user] = pwd
else:
    # 兼容单用户模式
    u = os.getenv("APP_USER", "")
    p = os.getenv("APP_PASSWORD", "")
    USER_CREDENTIALS = {u: p} if u and p else {}
