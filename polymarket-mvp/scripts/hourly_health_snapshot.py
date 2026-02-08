#!/usr/bin/env python3
import json
import subprocess
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "data" / "events.jsonl"


def ps_count(pattern: str) -> int:
    cmd = f"ps aux | egrep '{pattern}' | egrep -v 'egrep|grep' | wc -l"
    out = subprocess.check_output(cmd, shell=True, text=True).strip()
    return int(out or 0)


def to_epoch(ts):
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, str):
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    return None


last = {}
btc_target_missing_1h = 0
btc_target_missing_markets_1h = set()
now = time.time()

if EVENTS.exists():
    with EVENTS.open() as f:
        # Keep this lightweight for hourly cron runs even when events.jsonl grows large.
        # We only need recent state, so tail the last ~5k events.
        recent_lines = deque(f, maxlen=5000)
    for line in recent_lines:
        try:
            e = json.loads(line)
        except Exception:
            continue

        t = e.get("type")
        if t in {
            "market_scan",
            "inefficiency_report",
            "weather_scan",
            "ws_usage",
            "market_groups",
            "strategy_snapshot",
        }:
            last[t] = e

        if t == "btc_target_missing":
            ts = to_epoch(e.get("ts"))
            if ts and now - ts <= 3600:
                btc_target_missing_1h += 1
                mid = str(e.get("market_id") or "")
                if mid:
                    btc_target_missing_markets_1h.add(mid)

loop_n = ps_count(r"polymarket_mvp\.loop")
dash_n = ps_count(r"polymarket_mvp\.dashboard")

print(f"loops={loop_n} dashboards={dash_n}")

ms = last.get("market_scan", {})
if ms:
    cands = ms.get("top_candidates") or []
    c = next(
        (x for x in cands if (x.get("signal") or "").upper() not in {"NO_OPPORTUNITY", "NO_TRADE"}),
        cands[0] if cands else {},
    )
    print(
        "market_scan",
        ms.get("ts"),
        c.get("market_id"),
        c.get("signal"),
        f"exec_sum={c.get('yes_no_exec_sum')}",
        f"hint_sum={c.get('yes_no_hint_sum')}",
        f"exec_edge={c.get('exec_edge_bps')}bps",
        f"theo_edge={c.get('theo_edge_bps')}bps",
    )

ir = last.get("inefficiency_report", {})
if ir:
    c = (ir.get("top") or [{}])[0]
    print(
        "inefficiency",
        ir.get("ts"),
        c.get("market_id"),
        f"gap={c.get('execution_gap_bps')}bps",
    )

wx = last.get("weather_scan", {})
if wx:
    print("weather_scan", wx.get("ts"), f"count={wx.get('count')}")

wu = last.get("ws_usage", {})
if wu:
    print(
        "ws_usage",
        wu.get("ts"),
        f"alive={wu.get('alive')}",
        f"tracked={wu.get('tracked_count')}",
        f"updates={wu.get('updates_applied')}",
    )

grp = last.get("market_groups", {})
if grp:
    btc = (grp.get("bitcoin") or [{}])[0]
    if btc:
        print(
            "btc_focus",
            grp.get("ts"),
            btc.get("market_id"),
            btc.get("signal"),
            f"model={btc.get('best_model')}",
            f"consensus={btc.get('model_consensus')}",
        )

snap = last.get("strategy_snapshot", {})
if snap:
    print(
        "strategy",
        snap.get("ts"),
        snap.get("market_id"),
        snap.get("winner_side"),
        f"open_positions={snap.get('open_positions')}",
    )

print(
    f"btc_target_missing_1h={btc_target_missing_1h}"
    f" unique_markets={len(btc_target_missing_markets_1h)}"
)
