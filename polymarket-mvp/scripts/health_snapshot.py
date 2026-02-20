#!/usr/bin/env python3
"""Quick MVP health snapshot for hourly intel checks.

Reads local dashboard JSON and prints compact, actionable status:
- freshness (latest scan/data/WS)
- BTC 15m focus group summary
- top candidate signal metrics (exec_sum, hint_sum, exec_edge/theo_edge)
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from urllib.request import urlopen

URL = "http://127.0.0.1:8787/json"


def parse_ts(ts: str | None) -> datetime | None:
    if not ts:
        return None
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def age_s(ts: str | None) -> float | None:
    dt = parse_ts(ts)
    if not dt:
        return None
    return (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds()


def fmt_age(v: float | None) -> str:
    if v is None:
        return "n/a"
    return f"{v:.1f}s"


def main() -> None:
    payload = json.load(urlopen(URL, timeout=5))
    stats = payload.get("apiStats", {})

    latest_scan_ts = (stats.get("latestScan") or {}).get("ts")
    latest_groups_ts = (stats.get("latestGroups") or {}).get("ts")
    latest_data_ts = stats.get("latestDataTs")
    latest_ws_ts = stats.get("latestWsTs")

    print("=== MVP HEALTH SNAPSHOT ===")
    print(f"server_time: {payload.get('serverTime')}")
    print(
        "freshness:"
        f" scan={fmt_age(age_s(latest_scan_ts))}"
        f" groups={fmt_age(age_s(latest_groups_ts))}"
        f" data={fmt_age(age_s(latest_data_ts))}"
        f" ws={fmt_age(age_s(latest_ws_ts))}"
    )

    btc = (stats.get("latestGroups") or {}).get("bitcoin", [])
    print(f"btc_focus_count: {len(btc)}")
    for m in btc[:3]:
        print(
            f"BTC {m.get('market_id')}"
            f" sum={m.get('ask_sum_no_fees')}"
            f" spread={m.get('spread_sum')}"
            f" sig={m.get('signal')}"
            f" | {m.get('market_name')}"
        )

    btc_tick = stats.get("latestBtcTick") or {}
    chainlink = btc_tick.get("chainlink")
    binance = btc_tick.get("binance")
    if isinstance(chainlink, (int, float)) and isinstance(binance, (int, float)) and chainlink:
        delta_bps = ((binance - chainlink) / chainlink) * 10_000
        print(f"btc_tick: chainlink={chainlink:.2f} binance={binance:.2f} delta_bps={delta_bps:.1f}")

    opp = stats.get("latestOpportunitySeen") or {}
    print(f"latest_opportunities: {opp.get('count', 0)}")

    cands = (stats.get("latestScan") or {}).get("top_candidates", [])
    print(f"top_candidates: {len(cands)}")
    positive_exec = [c for c in cands if isinstance(c.get("exec_edge_bps"), (int, float)) and c.get("exec_edge_bps") > 0]
    positive_theo = [c for c in cands if isinstance(c.get("theo_edge_bps"), (int, float)) and c.get("theo_edge_bps") > 0]
    print(f"positive_edges: exec={len(positive_exec)} theo={len(positive_theo)}")
    for c in cands[:5]:
        print(
            f"CAND {c.get('market_id')} {c.get('side')}"
            f" exec_sum={c.get('yes_no_exec_sum')}"
            f" hint_sum={c.get('yes_no_hint_sum')}"
            f" exec_edge={c.get('exec_edge_bps')}"
            f" theo_edge={c.get('theo_edge_bps')}"
            f" sig={c.get('signal')}"
        )

    btc_cands = [
        c for c in cands
        if "bitcoin up or down" in str(c.get("market_name", "")).lower()
    ]
    print(f"btc_top_candidates: {len(btc_cands)}")
    for c in btc_cands[:3]:
        print(
            f"BTC_CAND {c.get('market_id')} {c.get('side')}"
            f" exec_sum={c.get('yes_no_exec_sum')}"
            f" hint_sum={c.get('yes_no_hint_sum')}"
            f" exec_edge={c.get('exec_edge_bps')}"
            f" theo_edge={c.get('theo_edge_bps')}"
            f" sig={c.get('signal')}"
            f" | {c.get('market_name')}"
        )

    state = payload.get("state") or {}
    last_error = state.get("last_error") or stats.get("lastError")
    print(f"last_error: {last_error}")


if __name__ == "__main__":
    main()
