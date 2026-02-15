import time
from polymarket_mvp.config import load_config
from polymarket_mvp.main import run_once, _ensure_ws_hook
from polymarket_mvp.sim.paper import init_state
from polymarket_mvp.utils.storage import save_state


def run_forever(config_path: str):
    cfg = load_config(config_path)
    # Reset ledger on restart only in paper mode.
    if str(cfg.get("app", {}).get("mode", "paper")).lower() == "paper":
        save_state(cfg["storage"]["state_path"], init_state(cfg))
    interval = float(cfg.get("app", {}).get("loop_seconds", 15))
    event_driven = bool(cfg.get("app", {}).get("event_driven", True))
    use_ws = bool(cfg.get("data", {}).get("use_clob_ws", True))
    min_cycle_seconds = float(cfg.get("app", {}).get("min_cycle_seconds", 0.2))
    last_ws_ts = 0.0

    while True:
        cycle_start = time.time()
        try:
            run_once(cfg)
            if use_ws:
                try:
                    st = _ensure_ws_hook().stats()
                    last_ws_ts = max(last_ws_ts, float(st.get("last_msg_ts") or 0.0))
                except Exception:
                    pass
        except Exception as e:
            from polymarket_mvp.utils.storage import append_event
            append_event(cfg["storage"]["events_path"], {"type": "loop_error", "error": str(e)})

        elapsed = time.time() - cycle_start
        if elapsed < min_cycle_seconds:
            time.sleep(min_cycle_seconds - elapsed)

        if event_driven and use_ws:
            try:
                last_ws_ts = _ensure_ws_hook().wait_for_update(last_ws_ts, timeout=interval)
                continue
            except Exception:
                pass

        time.sleep(interval)


if __name__ == "__main__":
    run_forever("config/default.yaml")
