import json
from pathlib import Path
from typing import Tuple, List

PROMPT_FILE = Path("prompts.json")


def _load() -> dict:
    if PROMPT_FILE.exists():
        try:
            return json.loads(PROMPT_FILE.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save(data: dict) -> None:
    PROMPT_FILE.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def get_prompt(name: str, version: str | None = None) -> Tuple[str, str]:
    data = _load()
    versions = data.get(name, {})
    if not versions:
        return "", ""
    if version is None:
        # pick last version sorted lexicographically
        version = sorted(versions.keys())[-1]
    prompt = versions.get(version, "")
    return version, prompt


def save_prompt(name: str, version: str, text: str) -> None:
    data = _load()
    data.setdefault(name, {})[version] = text
    _save(data)


def list_versions(name: str) -> List[str]:
    data = _load()
    versions = data.get(name, {})
    return sorted(versions.keys())
