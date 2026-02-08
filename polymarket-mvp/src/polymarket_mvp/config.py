from pathlib import Path
import yaml


def load_config(path: str) -> dict:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(path)
    return yaml.safe_load(p.read_text())
