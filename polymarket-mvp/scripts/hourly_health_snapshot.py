#!/usr/bin/env python3
import json
import subprocess
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "data" / "events.jsonl"

def ps_count(pattern: str) -> int:
    cmd = f"ps aux | egrep '{pattern}' | egrep -v 'egrep|grep' | wc -l"
    out = subprocess.check_output(cmd, shell=True, text=True).strip()
    return int(out or 0)

last = {}
if EVENTS.exists():
    for line in EVENTS.read_text().splitlines():
        try:
            e = json.loads(line)
        except Exception:
            continue
        t = e.get("type")
        if t in {"market_scan", "inefficiency_report", "weather_scan"}:
            last[t] = e

loop_n = ps_count(r"polymarket_mvp\.loop")
dash_n = ps_count(r"polymarket_mvp\.dashboard")

print(f"loops={loop_n} dashboards={dash_n}")

ms = last.get("market_scan", {})
if ms:
    c = (ms.get("top_candidates") or [{}])[0]
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
