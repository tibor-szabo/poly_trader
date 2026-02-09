#!/usr/bin/env python3
import json
import time
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

EVENTS = Path(__file__).resolve().parents[1] / "data" / "events.jsonl"
WINDOW_S = 3600


def to_epoch(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    return None


def main():
    now = time.time()
    cut = now - WINDOW_S
    opens, closes, guards = [], [], []

    with EVENTS.open() as f:
        for line in f:
            try:
                e = json.loads(line)
            except Exception:
                continue
            t = to_epoch(e.get("ts"))
            if not t or t < cut:
                continue
            typ = e.get("type")
            if typ == "paper_trade":
                if e.get("action") == "OPEN":
                    opens.append(e)
                elif "CLOSE" in str(e.get("action") or ""):
                    closes.append(e)
            elif typ == "market_guardrail":
                guards.append(e)

    pnl = sum(float(e.get("pnl_usd") or 0.0) for e in closes)
    wins = sum(1 for e in closes if float(e.get("pnl_usd") or 0.0) > 0)
    breakeven = sum(1 for e in closes if float(e.get("pnl_usd") or 0.0) == 0)
    losses = sum(1 for e in closes if float(e.get("pnl_usd") or 0.0) < 0)
    winrate = (wins / len(closes) * 100.0) if closes else 0.0

    by_side = Counter((e.get("side") or "-") for e in closes)
    by_model = Counter((e.get("model_open") or e.get("model") or "-") for e in closes)
    close_reasons = Counter((e.get("reason") or "-") for e in closes)

    side_pnl = defaultdict(float)
    model_pnl = defaultdict(float)
    reason_pnl = defaultdict(float)
    model_wins = defaultdict(int)
    model_trades = defaultdict(int)
    for e in closes:
        v = float(e.get("pnl_usd") or 0.0)
        side = (e.get("side") or "-")
        model = (e.get("model_open") or e.get("model") or "-")
        reason = (e.get("reason") or "-")
        side_pnl[side] += v
        model_pnl[model] += v
        reason_pnl[reason] += v
        model_trades[model] += 1
        if v > 0:
            model_wins[model] += 1

    # Re-entry / churn: open after close on same market within 10 minutes.
    closes_by_market = defaultdict(list)
    for e in closes:
        ts = to_epoch(e.get("closed_at") or e.get("ts"))
        if ts:
            closes_by_market[str(e.get("market_id") or "")].append(ts)
    for v in closes_by_market.values():
        v.sort()

    reentries = 0
    fast_reentries = 0
    hold_s = []
    closes_per_market = Counter(str(e.get("market_id") or "") for e in closes if e.get("market_id") is not None)
    for e in opens:
        mid = str(e.get("market_id") or "")
        ot = to_epoch(e.get("opened_at") or e.get("ts"))
        if not mid or not ot:
            continue
        prev = [x for x in closes_by_market.get(mid, []) if x < ot]
        if prev:
            dt = ot - prev[-1]
            if dt <= 600:
                reentries += 1
            if dt <= 180:
                fast_reentries += 1

    for e in closes:
        ot = to_epoch(e.get("opened_at"))
        ct = to_epoch(e.get("closed_at") or e.get("ts"))
        if ot and ct and ct >= ot:
            hold_s.append(ct - ot)

    avg_hold = (sum(hold_s) / len(hold_s)) if hold_s else 0.0

    out = {
        "window_minutes": 60,
        "counts": {"opens": len(opens), "closes": len(closes)},
        "pnl_usd": round(pnl, 4),
        "winrate_pct": round(winrate, 2),
        "wins": wins,
        "breakeven": breakeven,
        "losses": losses,
        "by_side": dict(by_side),
        "by_side_pnl": {k: round(v, 4) for k, v in side_pnl.items()},
        "by_model": dict(by_model),
        "by_model_pnl": {k: round(v, 4) for k, v in model_pnl.items()},
        "by_model_winrate_pct": {
            k: round((model_wins.get(k, 0) / max(1, n)) * 100.0, 2) for k, n in model_trades.items()
        },
        "close_reasons": dict(close_reasons),
        "close_reasons_pnl": {k: round(v, 4) for k, v in reason_pnl.items()},
        "churn": {
            "reentries_10m": reentries,
            "fast_reentries_3m": fast_reentries,
            "avg_hold_seconds": round(avg_hold, 2),
            "markets_with_multiple_closes": sum(1 for _, n in closes_per_market.items() if n > 1),
            "top_repeated_markets": [
                {"market_id": k, "closes": n} for k, n in closes_per_market.most_common(3) if n > 1
            ],
        },
        "guardrails_triggered": len(guards),
    }
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
