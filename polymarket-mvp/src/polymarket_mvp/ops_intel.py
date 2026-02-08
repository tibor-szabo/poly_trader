import math
from typing import Dict, List

from polymarket_mvp.models import MarketSnapshot
from polymarket_mvp.engine.scoring import depth_aware_buy_prices


def _safe_mid(bid: float, ask: float) -> float:
    if bid > 0 and ask > 0:
        return (bid + ask) / 2.0
    return max(bid, ask, 0.0)


def build_market_radar(snapshots: List[MarketSnapshot], limit: int = 8) -> List[Dict]:
    rows: List[Dict] = []
    for s in snapshots:
        spread_yes = max(0.0, s.yes_ask - s.yes_bid)
        spread_no = max(0.0, s.no_ask - s.no_bid)
        spread_penalty = (spread_yes + spread_no) / 2.0

        # Penalize dead/wide books hard (the 0.98/0.98 style markets).
        dead_book_penalty = 0.0
        if spread_yes >= 0.9 and spread_no >= 0.9:
            dead_book_penalty += 55.0

        depth_score = min(50.0, math.log10(max(s.depth_usd, 1.0)) * 12.0)
        tightness_score = max(0.0, 100.0 * (1.0 - spread_penalty))
        score = round(depth_score + tightness_score - dead_book_penalty, 2)
        rows.append(
            {
                "market_id": s.market_id,
                "market_name": s.question,
                "score": score,
                "quality": "dead" if dead_book_penalty > 0 else ("weak" if spread_penalty > 0.2 else "tradable"),
                "depth_usd": round(s.depth_usd, 2),
                "spread_yes": round(spread_yes, 4),
                "spread_no": round(spread_no, 4),
                "yes_mid": round(_safe_mid(s.yes_bid, s.yes_ask), 4),
                "no_mid": round(_safe_mid(s.no_bid, s.no_ask), 4),
            }
        )
    rows.sort(key=lambda x: x["score"], reverse=True)
    return rows[:limit]


def build_inefficiency_report(snapshots: List[MarketSnapshot], fee_bps: float, slippage_bps: float, target_size_usd: float, limit: int = 8) -> List[Dict]:
    rows: List[Dict] = []
    for s in snapshots:
        yes_buy, no_buy = depth_aware_buy_prices(s, target_size_usd=target_size_usd)
        exec_sum = yes_buy + no_buy
        exec_edge_bps = (1.0 - exec_sum) * 10000.0 - fee_bps - slippage_bps

        theo_sum = None
        theo_edge_bps = None
        if s.yes_hint > 0 and s.no_hint > 0:
            theo_sum = s.yes_hint + s.no_hint
            theo_edge_bps = (1.0 - theo_sum) * 10000.0 - fee_bps - slippage_bps

        gap = None
        if theo_edge_bps is not None:
            gap = theo_edge_bps - exec_edge_bps

        rows.append(
            {
                "market_id": s.market_id,
                "market_name": s.question,
                "yes_no_exec_sum": round(exec_sum, 4),
                "exec_edge_bps": round(exec_edge_bps, 2),
                "yes_no_hint_sum": round(theo_sum, 4) if theo_sum is not None else None,
                "theo_edge_bps": round(theo_edge_bps, 2) if theo_edge_bps is not None else None,
                "execution_gap_bps": round(gap, 2) if gap is not None else None,
            }
        )

    rows.sort(key=lambda x: x.get("execution_gap_bps") if x.get("execution_gap_bps") is not None else -10**9, reverse=True)
    return rows[:limit]


def build_flow_watch(snapshots: List[MarketSnapshot], limit: int = 8) -> List[Dict]:
    rows: List[Dict] = []
    for s in snapshots:
        yes_mid = _safe_mid(s.yes_bid, s.yes_ask)
        no_mid = _safe_mid(s.no_bid, s.no_ask)
        imbalance = yes_mid - no_mid
        rows.append(
            {
                "market_id": s.market_id,
                "market_name": s.question,
                "yes_mid": round(yes_mid, 4),
                "no_mid": round(no_mid, 4),
                "mid_imbalance": round(imbalance, 4),
                "tag": "yes_pressure" if imbalance > 0.03 else ("no_pressure" if imbalance < -0.03 else "balanced"),
            }
        )
    rows.sort(key=lambda x: abs(x["mid_imbalance"]), reverse=True)
    return rows[:limit]
