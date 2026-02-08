from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
from polymarket_mvp.models import RunState


def load_state(path: str, starting_cash: float) -> RunState:
    p = Path(path)
    if not p.exists():
        return RunState(cash_usd=starting_cash, positions=[])
    data = json.loads(p.read_text())
    return RunState.model_validate(data)


def save_state(path: str, state: RunState) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(state.model_dump_json(indent=2))


def append_event(path: str, event: dict) -> None:
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    event = {"ts": datetime.now(timezone.utc).isoformat(), **event}
    with p.open("a") as f:
        f.write(json.dumps(event) + "\n")
