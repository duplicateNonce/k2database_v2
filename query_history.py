import json
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List

FILE_PATH = Path("data/query_history.json")


def _load() -> Dict[str, Dict[str, List[dict]]]:
    if FILE_PATH.exists():
        try:
            data = json.loads(FILE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, dict):
                return data
        except json.JSONDecodeError:
            pass
    return {}


def _save(history: Dict[str, Dict[str, List[dict]]]) -> None:
    FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
    FILE_PATH.write_text(json.dumps(history, ensure_ascii=False, indent=2), encoding="utf-8")


def add_entry(page: str, user: str, params: Dict[str, Any]) -> None:
    """Add a history entry for a page and user."""
    history = _load()
    if not isinstance(history, dict):
        history = {}

    page_hist = history.get(page, {})
    if not isinstance(page_hist, dict):
        page_hist = {}

    user_hist = page_hist.get(user, [])
    if not isinstance(user_hist, list):
        user_hist = []
    user_hist.insert(0, {
        "params": params,
        "time": datetime.now().isoformat(timespec="seconds")
    })
    page_hist[user] = user_hist[:20]
    history[page] = page_hist
    _save(history)


def get_history(page: str, user: str) -> List[dict]:
    """Return history list for a page and user."""
    history = _load()
    if not isinstance(history, dict):
        return []

    page_hist = history.get(page)
    if not isinstance(page_hist, dict):
        return []

    user_hist = page_hist.get(user)
    if isinstance(user_hist, list):
        return user_hist
    return []
