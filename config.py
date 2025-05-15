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
