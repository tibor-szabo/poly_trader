#!/usr/bin/env python3
import json
import subprocess
import time
from collections import Counter, deque
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


def age_minutes(ts):
    t = to_epoch(ts)
    if not t:
        return None
    return round((time.time() - t) / 60.0, 1)


last = {}
btc_target_missing_1h = 0
btc_target_missing_30m = 0
btc_target_missing_15m = 0
btc_target_missing_5m = 0
btc_target_missing_markets_1h = set()
btc_target_missing_market_counter_1h = Counter()
last_btc_target_missing_ts = None
loop_errors_1h = 0
adapter_errors_1h = 0
guardrails_1h = 0
now = time.time()

if EVENTS.exists():
    with EVENTS.open() as f:
        # Keep this lightweight for hourly cron runs even when events.jsonl grows large.
        # Tail a much larger window so high-frequency periods do not undercount
        # within-hour diagnostics (e.g., btc_target_missing bursts).
        recent_lines = deque(f, maxlen=100000)
    for line in recent_lines:
        try:
            e = json.loads(line)
        except Exception:
            continue

        ts = to_epoch(e.get("ts"))
        t = e.get("type")

        if ts and now - ts <= 3600:
            if t == "loop_error":
                loop_errors_1h += 1
            elif t == "adapter_error":
                adapter_errors_1h += 1
            elif t == "market_guardrail":
                guardrails_1h += 1

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
            if ts:
                if (last_btc_target_missing_ts is None) or ts > last_btc_target_missing_ts:
                    last_btc_target_missing_ts = ts
                if now - ts <= 3600:
                    btc_target_missing_1h += 1
                    if now - ts <= 1800:
                        btc_target_missing_30m += 1
                    if now - ts <= 900:
                        btc_target_missing_15m += 1
                    if now - ts <= 300:
                        btc_target_missing_5m += 1
                    mid = str(e.get("market_id") or "")
                    if mid:
                        btc_target_missing_markets_1h.add(mid)
                        btc_target_missing_market_counter_1h[mid] += 1

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
    age = age_minutes(ms.get("ts"))
    print(
        "market_scan",
        ms.get("ts"),
        f"age_min={age}",
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
    age = age_minutes(wu.get("ts"))
    stale = age is not None and age > 10
    print(
        "ws_usage",
        wu.get("ts"),
        f"age_min={age}",
        f"alive={wu.get('alive')}",
        f"tracked={wu.get('tracked_count')}",
        f"updates={wu.get('updates_applied')}",
        f"status={'STALE' if stale else 'OK'}",
    )

grp = last.get("market_groups", {})
if grp:
    btc = (grp.get("bitcoin") or [{}])[0]
    if btc:
        print(
            "btc_focus",
            grp.get("ts"),
            f"age_min={age_minutes(grp.get('ts'))}",
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
        f"age_min={age_minutes(snap.get('ts'))}",
        snap.get("market_id"),
        snap.get("winner_side"),
        f"open_positions={snap.get('open_positions')}",
    )

missing_ids = sorted(btc_target_missing_markets_1h)
print(
    f"btc_target_missing_1h={btc_target_missing_1h}"
    f" btc_target_missing_30m={btc_target_missing_30m}"
    f" btc_target_missing_15m={btc_target_missing_15m}"
    f" btc_target_missing_5m={btc_target_missing_5m}"
    f" unique_markets={len(missing_ids)}"
)
if missing_ids:
    print("btc_target_missing_market_ids", ",".join(missing_ids[:5]))
if btc_target_missing_market_counter_1h:
    top_mid, top_cnt = btc_target_missing_market_counter_1h.most_common(1)[0]
    share = round((top_cnt / max(1, btc_target_missing_1h)) * 100.0, 1)
    print("btc_target_missing_hotspot", top_mid, f"count={top_cnt}", f"share_pct={share}")
    top3 = ",".join(
        f"{mid}:{cnt}" for mid, cnt in btc_target_missing_market_counter_1h.most_common(3)
    )
    print("btc_target_missing_hotspots_top3", top3)
if last_btc_target_missing_ts is not None:
    age = round((now - last_btc_target_missing_ts) / 60.0, 1)
    ts_iso = datetime.fromtimestamp(last_btc_target_missing_ts, timezone.utc).isoformat()
    print("btc_target_missing_last_seen", ts_iso, f"age_min={age}")

print(
    "error_summary_1h",
    f"loop_error={loop_errors_1h}",
    f"adapter_error={adapter_errors_1h}",
    f"market_guardrail={guardrails_1h}",
)

# Lightweight health signal for discovery coverage reliability.
if btc_target_missing_1h >= 50:
    print("btc_discovery_health", "DEGRADED", f"missing_per_hour={btc_target_missing_1h}")
elif btc_target_missing_1h >= 20:
    print("btc_discovery_health", "WATCH", f"missing_per_hour={btc_target_missing_1h}")
else:
    print("btc_discovery_health", "OK", f"missing_per_hour={btc_target_missing_1h}")

burst = "BURST" if btc_target_missing_5m >= 5 else "STABLE"
print("btc_discovery_burst", burst, f"missing_5m={btc_target_missing_5m}")

if btc_target_missing_30m >= 10:
    health_30m = "HOT"
elif btc_target_missing_30m >= 5:
    health_30m = "ELEVATED"
else:
    health_30m = "OK"
print("btc_discovery_health_30m", health_30m, f"missing_30m={btc_target_missing_30m}")

if btc_target_missing_15m >= 8:
    health_15m = "HOT"
elif btc_target_missing_15m >= 4:
    health_15m = "ELEVATED"
else:
    health_15m = "OK"
print("btc_discovery_health_15m", health_15m, f"missing_15m={btc_target_missing_15m}")

missing_rate_30m = round(btc_target_missing_30m / 30.0, 2)
print("btc_discovery_missing_rate_30m", f"per_min={missing_rate_30m}")

if health_30m == "HOT":
    print("btc_discovery_action", "consider_narrowing_focus_or_refreshing_target_set")
