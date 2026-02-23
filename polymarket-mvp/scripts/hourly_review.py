#!/usr/bin/env python3
import argparse
import json
import time
from collections import Counter, defaultdict, deque
from datetime import datetime
from pathlib import Path

EVENTS = Path(__file__).resolve().parents[1] / "data" / "events.jsonl"
WINDOW_S = 3600
RECENT_LINES = 300_000

LOW_CONVERSION_PCT = 8.0
LOW_WS_OPP_SHARE_PCT = 10.0
SHORT_HOLD_SECONDS = 45.0


def to_epoch(v):
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        try:
            return datetime.fromisoformat(v.replace("Z", "+00:00")).timestamp()
        except Exception:
            return None
    return None


def parse_args():
    p = argparse.ArgumentParser(description="Summarize recent paper trading performance from events.jsonl")
    p.add_argument("--window-hours", type=float, default=1.0, help="Lookback window in hours (default: 1)")
    p.add_argument("--events", type=Path, default=EVENTS, help="Path to events.jsonl")
    p.add_argument(
        "--recent-lines",
        type=int,
        default=RECENT_LINES,
        help="Only parse last N lines for speed (default: 300000, use 0 for full file)",
    )
    return p.parse_args()


def iter_recent_lines(path: Path, recent_lines: int):
    if recent_lines <= 0:
        with path.open() as f:
            yield from f
        return
    q = deque(maxlen=recent_lines)
    with path.open() as f:
        for line in f:
            q.append(line)
    for line in q:
        yield line


def main():
    args = parse_args()
    now = time.time()
    window_s = max(60.0, float(args.window_hours) * 3600.0)
    cut = now - window_s
    opens, closes, partial_closes, guards = [], [], [], []
    event_type_counts = Counter()
    opportunity_seen_count = 0
    ws_opportunity_seen_count = 0
    btc_target_missing_count = 0
    btc_target_missing_markets = Counter()

    for line in iter_recent_lines(args.events, args.recent_lines):
        try:
            e = json.loads(line)
        except Exception:
            continue
        t = to_epoch(e.get("ts"))
        if not t or t < cut:
            continue
        typ = e.get("type")
        event_type_counts[str(typ or "-")] += 1
        if typ == "opportunity_seen":
            opportunity_seen_count += 1
        elif typ == "ws_opportunity_seen":
            ws_opportunity_seen_count += 1
        elif typ == "btc_target_missing":
            btc_target_missing_count += 1
            market_id = str(e.get("market_id") or "")
            if market_id:
                btc_target_missing_markets[market_id] += 1

        if typ == "paper_trade":
            action = str(e.get("action") or "")
            if action == "OPEN":
                opens.append(e)
            elif action == "CLOSE":
                closes.append(e)
            elif action == "PARTIAL_CLOSE":
                partial_closes.append(e)
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
    open_execution = Counter((e.get("open_execution") or "-") for e in opens)
    close_execution = Counter((e.get("close_execution") or "-") for e in closes)
    guardrail_reasons = Counter((e.get("reason") or "-") for e in guards)
    open_fallback_count = int(open_execution.get("open_limit_timeout_fallback", 0))

    side_pnl = defaultdict(float)
    model_pnl = defaultdict(float)
    reason_pnl = defaultdict(float)
    hard_stop_by_model = defaultdict(int)
    hard_stop_pnl_by_model = defaultdict(float)
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
        if reason == "hard_stop_25":
            hard_stop_by_model[model] += 1
            hard_stop_pnl_by_model[model] += v
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
    hold_pairs = []
    closes_per_market = Counter(str(e.get("market_id") or "") for e in closes if e.get("market_id") is not None)
    partials_per_market = Counter(str(e.get("market_id") or "") for e in partial_closes if e.get("market_id") is not None)
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
            h = ct - ot
            hold_s.append(h)
            hold_pairs.append((e, h))

    avg_hold = (sum(hold_s) / len(hold_s)) if hold_s else 0.0
    loss_hold = [h for e, h in hold_pairs if float(e.get("pnl_usd") or 0.0) < 0]
    win_hold = [h for e, h in hold_pairs if float(e.get("pnl_usd") or 0.0) > 0]

    opportunity_total = opportunity_seen_count + ws_opportunity_seen_count
    ws_opp_share_pct = (ws_opportunity_seen_count / max(1, opportunity_total)) * 100.0
    conversion_pct = (len(opens) / max(1, opportunity_total)) * 100.0

    review_flags = []
    issues = []
    if len(closes) < 5:
        review_flags.append("low_sample_size")
        issues.append(
            {
                "name": "low_sample_size",
                "severity": "high",
                "evidence": {
                    "closes": len(closes),
                    "opens": len(opens),
                    "window_hours": round(args.window_hours, 2),
                },
                "suggestion": "Increase candidate throughput or widen eligible universe until >=5 closes/4h before trusting PnL.",
            }
        )
    if len(opens) > 0 and (open_fallback_count / max(1, len(opens))) >= 0.5:
        review_flags.append("high_open_fallback_share")
    if (len(opens) + len(closes)) == 0 and (opportunity_seen_count + ws_opportunity_seen_count) > 0:
        review_flags.append("opportunities_without_trades")
    if btc_target_missing_count > 0:
        review_flags.append("btc_target_missing")

    if opportunity_total >= 10 and conversion_pct < LOW_CONVERSION_PCT:
        review_flags.append("low_opportunity_conversion")
        issues.append(
            {
                "name": "low_opportunity_conversion",
                "severity": "medium",
                "evidence": {
                    "opportunities": opportunity_total,
                    "opens": len(opens),
                    "conversion_pct": round(conversion_pct, 2),
                },
                "suggestion": "Loosen only the most restrictive gate (single parameter per run) and track conversion delta next hour.",
            }
        )

    if event_type_counts.get("ws_market_tick", 0) > 500 and ws_opp_share_pct < LOW_WS_OPP_SHARE_PCT:
        review_flags.append("ws_opportunity_underrepresentation")
        issues.append(
            {
                "name": "ws_opportunity_underrepresentation",
                "severity": "medium",
                "evidence": {
                    "ws_market_tick": int(event_type_counts.get("ws_market_tick", 0)),
                    "ws_opportunity_seen": ws_opportunity_seen_count,
                    "ws_opportunity_share_pct": round(ws_opp_share_pct, 2),
                },
                "suggestion": "Audit ws opportunity gating/thresholds; if intentional, add explicit metric guardrail so drops are visible.",
            }
        )

    if len(hold_s) > 0 and avg_hold <= SHORT_HOLD_SECONDS:
        review_flags.append("very_short_average_hold")
        issues.append(
            {
                "name": "very_short_average_hold",
                "severity": "low",
                "evidence": {
                    "avg_hold_seconds": round(avg_hold, 2),
                    "closes": len(closes),
                },
                "suggestion": "Review close reasons for premature exits; consider minimum-hold guard except for hard-stop exits.",
            }
        )

    hard_stop_count = int(close_reasons.get("hard_stop_25", 0))
    hard_stop_share_pct = (hard_stop_count / max(1, len(closes))) * 100.0

    buy_yes_closes = int(by_side.get("BUY_YES", 0))
    buy_yes_loss_share_pct = 0.0
    if buy_yes_closes > 0:
        buy_yes_losses = sum(1 for e in closes if (e.get("side") == "BUY_YES") and float(e.get("pnl_usd") or 0.0) < 0)
        buy_yes_loss_share_pct = (buy_yes_losses / buy_yes_closes) * 100.0
    buy_yes_pnl = float(side_pnl.get("BUY_YES", 0.0))
    if len(closes) >= 8 and buy_yes_closes >= 3 and buy_yes_pnl < 0 and buy_yes_loss_share_pct >= 66.0:
        review_flags.append("buy_yes_drawdown_concentration")
        issues.append(
            {
                "name": "buy_yes_drawdown_concentration",
                "severity": "high",
                "evidence": {
                    "buy_yes_closes": buy_yes_closes,
                    "buy_yes_pnl_usd": round(buy_yes_pnl, 4),
                    "buy_yes_loss_share_pct": round(buy_yes_loss_share_pct, 2),
                    "total_closes": len(closes),
                },
                "suggestion": "De-risk BUY_YES entries first (smaller size and/or stricter impulse threshold) before changing broader gates.",
            }
        )

    top_hard_stop_model = None
    top_hard_stop_count = 0
    if hard_stop_by_model:
        top_hard_stop_model, top_hard_stop_count = max(hard_stop_by_model.items(), key=lambda kv: kv[1])
    if len(closes) >= 3 and hard_stop_count == len(closes) and reason_pnl.get("hard_stop_25", 0.0) < 0:
        review_flags.append("hard_stop_dominance_early")
        issues.append(
            {
                "name": "hard_stop_dominance_early",
                "severity": "medium",
                "evidence": {
                    "hard_stop_closes": hard_stop_count,
                    "total_closes": len(closes),
                    "hard_stop_share_pct": round(hard_stop_share_pct, 2),
                    "hard_stop_pnl_usd": round(reason_pnl.get("hard_stop_25", 0.0), 4),
                    "top_hard_stop_model": top_hard_stop_model,
                    "top_hard_stop_model_count": int(top_hard_stop_count),
                },
                "suggestion": "Treat as early warning: tighten exploration entry quality and/or reduce exploration size until non-hard-stop exits appear.",
            }
        )

    if len(closes) >= 5 and hard_stop_count >= 3 and hard_stop_share_pct >= 50.0 and reason_pnl.get("hard_stop_25", 0.0) < 0:
        review_flags.append("hard_stop_dominance")
        issues.append(
            {
                "name": "hard_stop_dominance",
                "severity": "high",
                "evidence": {
                    "hard_stop_closes": hard_stop_count,
                    "total_closes": len(closes),
                    "hard_stop_share_pct": round(hard_stop_share_pct, 2),
                    "hard_stop_pnl_usd": round(reason_pnl.get("hard_stop_25", 0.0), 4),
                    "top_hard_stop_model": top_hard_stop_model,
                    "top_hard_stop_model_count": int(top_hard_stop_count),
                },
                "suggestion": "Reduce risk on the dominant hard-stop model first (smaller size or tighter entry confidence), then re-check next hour.",
            }
        )

    out = {
        "window_minutes": round(window_s / 60.0, 2),
        "counts": {"opens": len(opens), "closes": len(closes), "partial_closes": len(partial_closes)},
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
        "open_execution": dict(open_execution),
        "open_fallback_rate_pct": round((open_fallback_count / max(1, len(opens))) * 100.0, 2),
        "close_execution": dict(close_execution),
        "close_reasons": dict(close_reasons),
        "close_reasons_pnl": {k: round(v, 4) for k, v in reason_pnl.items()},
        "hard_stop_share_pct": round(hard_stop_share_pct, 2),
        "hard_stop_by_model": dict(hard_stop_by_model),
        "hard_stop_pnl_by_model": {k: round(v, 4) for k, v in hard_stop_pnl_by_model.items()},
        "churn": {
            "reentries_10m": reentries,
            "fast_reentries_3m": fast_reentries,
            "avg_hold_seconds": round(avg_hold, 2),
            "avg_hold_seconds_wins": round((sum(win_hold) / len(win_hold)), 2) if win_hold else 0.0,
            "avg_hold_seconds_losses": round((sum(loss_hold) / len(loss_hold)), 2) if loss_hold else 0.0,
            "markets_with_multiple_closes": sum(1 for _, n in closes_per_market.items() if n > 1),
            "top_repeated_markets": [
                {"market_id": k, "closes": n} for k, n in closes_per_market.most_common(3) if n > 1
            ],
            "top_partial_close_markets": [
                {"market_id": k, "partial_closes": n} for k, n in partials_per_market.most_common(3) if n > 1
            ],
        },
        "guardrails_triggered": len(guards),
        "guardrail_reasons": dict(guardrail_reasons),
        "opportunity_seen": {
            "scanner": opportunity_seen_count,
            "ws": ws_opportunity_seen_count,
            "total": opportunity_seen_count + ws_opportunity_seen_count,
        },
        "btc_target_missing": btc_target_missing_count,
        "btc_target_missing_per_hour": round(btc_target_missing_count / max(1e-9, args.window_hours), 2),
        "btc_target_missing_top_markets": [
            {"market_id": mid, "count": cnt}
            for mid, cnt in btc_target_missing_markets.most_common(5)
        ],
        "opportunity_to_trade_conversion_pct": round(conversion_pct, 2),
        "ws_opportunity_share_pct": round(ws_opp_share_pct, 2),
        "event_type_counts": dict(event_type_counts),
        "review_flags": review_flags,
        "issues": issues,
    }
    print(json.dumps(out, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
