import json
from pathlib import Path

LABEL_WATCHLIST_FILE = Path("data/label_watchlist.json")


def load_label_watchlist() -> list[str]:
    if LABEL_WATCHLIST_FILE.exists():
        try:
            data = json.loads(LABEL_WATCHLIST_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return data
        except Exception:
            pass
    return []


def save_label_watchlist(lst: list[str]) -> None:
    LABEL_WATCHLIST_FILE.parent.mkdir(parents=True, exist_ok=True)
    LABEL_WATCHLIST_FILE.write_text(
        json.dumps(lst, ensure_ascii=False, indent=2), encoding="utf-8"
    )
