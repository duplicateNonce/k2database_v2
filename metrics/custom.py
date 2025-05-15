import json
from pathlib import Path
import pandas as pd  

CUSTOM_METRICS_FILE = Path.home() / ".coinmetrics_metrics.json"


def load_saved_metrics() -> dict:
    if CUSTOM_METRICS_FILE.exists():
        try:
            return json.loads(CUSTOM_METRICS_FILE.read_text())
        except Exception:
            return {}
    return {}


def save_metrics(metrics: dict) -> None:
    with open(CUSTOM_METRICS_FILE, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)


def eval_custom_metrics(df, sys_metrics, custom_metrics) -> pd.DataFrame:
    from utils import extract_cols
    import pandas as pd
    for name, expr in custom_metrics.items():
        cols = extract_cols(expr, df.columns)
        try:
            df[name] = pd.eval(expr, engine="python", parser="pandas", local_dict=df)
        except Exception:
            df[name] = None
    return df
