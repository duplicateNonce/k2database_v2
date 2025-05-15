from sqlalchemy import create_engine
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, COIN_DB_NAME, OHLCV_DB_NAME


def get_engine(db: str = "coin"):
    """
    获取对应数据库的 SQLAlchemy 引擎。
    db: 'coin' 或 'ohlcv'
    """
    name = COIN_DB_NAME if db == "coin" else OHLCV_DB_NAME
    url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{name}"
    return create_engine(url, pool_pre_ping=True)

# 预实例化两个引擎
engine_coin  = get_engine("coin")
engine_ohlcv = get_engine("ohlcv")
