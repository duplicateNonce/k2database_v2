import json
import hashlib
from pathlib import Path
from typing import Any, Dict, Tuple, Optional

import pandas as pd

CACHE_DIR = Path("data/cache")


def _hash_params(params: Dict[str, Any]) -> str:
    """Return a stable hash for a dict of parameters."""
    serial = json.dumps(params, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(serial.encode("utf-8")).hexdigest()


def load_cached(page: str, params: Dict[str, Any]) -> Tuple[str, Optional[pd.DataFrame]]:
    """Load cached DataFrame for page/params.

    Returns (cache_id, df or None).
    """
    cache_id = _hash_params(params)
    path = CACHE_DIR / page / f"{cache_id}.csv"
    if path.exists():
        try:
            df = pd.read_csv(path, index_col=0)
            return cache_id, df
        except Exception:
            pass
    return cache_id, None


def save_cached(page: str, params: Dict[str, Any], df: pd.DataFrame) -> str:
    """Save DataFrame to cache and return cache_id."""
    cache_id = _hash_params(params)
    path = CACHE_DIR / page / f"{cache_id}.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        df.to_csv(path, index=True)
    except Exception:
        pass
    return cache_id


def load_by_id(page: str, cache_id: str) -> Optional[pd.DataFrame]:
    """Load cached DataFrame by cache id."""
    path = CACHE_DIR / page / f"{cache_id}.csv"
    if path.exists():
        try:
            return pd.read_csv(path, index_col=0)
        except Exception:
            return None
    return None
