from typing import List, Tuple
from polymarket_mvp.models import MarketSnapshot, Opportunity


def _as_float(x, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _bookwalk_buy_price(asks: List[dict], target_size_usd: float, fallback_price: float) -> float:
    remaining = max(0.0, target_size_usd)
    total_cost = 0.0
    total_qty = 0.0

    for lvl in asks:
        px = _as_float((lvl or {}).get("price"), 0.0)
        qty = _as_float((lvl or {}).get("size"), 0.0)
        if px <= 0 or qty <= 0:
            continue

        lvl_notional = px * qty
        take_notional = min(remaining, lvl_notional)
        take_qty = take_notional / px

        total_cost += take_notional
        total_qty += take_qty
        remaining -= take_notional
        if remaining <= 1e-9:
            break

    if total_qty <= 0:
        return fallback_price if fallback_price > 0 else 1.0

    avg_price = total_cost / total_qty

    # If book depth is insufficient for requested size, punish by using fallback for unfilled tail.
    if remaining > 0:
        fallback = fallback_price if fallback_price > 0 else 1.0
        total_cost += remaining
        total_qty += remaining / max(fallback, 1e-6)
        avg_price = total_cost / max(total_qty, 1e-9)

    return max(0.0, min(1.0, avg_price))


def effective_buy_prices(s: MarketSnapshot) -> Tuple[float, float]:
    yes_direct = s.yes_ask if s.yes_ask > 0 else 1.0
    no_direct = s.no_ask if s.no_ask > 0 else 1.0
    yes_via_parity = 1.0 - s.no_bid if s.no_bid > 0 else 1.0
    no_via_parity = 1.0 - s.yes_bid if s.yes_bid > 0 else 1.0
    yes_buy = max(0.0, min(1.0, min(yes_direct, yes_via_parity)))
    no_buy = max(0.0, min(1.0, min(no_direct, no_via_parity)))
    return yes_buy, no_buy


def depth_aware_buy_prices(s: MarketSnapshot, target_size_usd: float) -> Tuple[float, float]:
    yes_top, no_top = effective_buy_prices(s)

    yes_book = _bookwalk_buy_price(getattr(s, "yes_asks", []) or [], target_size_usd=target_size_usd, fallback_price=yes_top)
    no_book = _bookwalk_buy_price(getattr(s, "no_asks", []) or [], target_size_usd=target_size_usd, fallback_price=no_top)

    # Keep parity alternative as a floor if direct ask book is worse.
    yes_via_parity = 1.0 - s.no_bid if s.no_bid > 0 else 1.0
    no_via_parity = 1.0 - s.yes_bid if s.yes_bid > 0 else 1.0

    yes_exec = max(0.0, min(1.0, min(yes_book, yes_via_parity)))
    no_exec = max(0.0, min(1.0, min(no_book, no_via_parity)))
    return yes_exec, no_exec


def rank_candidates(snapshots: List[MarketSnapshot], cfg: dict) -> List[Opportunity]:
    out: List[Opportunity] = []
    fee_bps = float(cfg["scoring"]["fee_bps"])
    slippage_bps = float(cfg["scoring"]["slippage_bps"])
    target_size_usd = float(cfg["scoring"].get("target_size_usd", 20.0))

    for s in snapshots:
        if not s.accepting_orders:
            continue

        yes_buy, no_buy = depth_aware_buy_prices(s, target_size_usd=target_size_usd)

        edge_yes_bps = (0.5 - yes_buy) * 10000 - fee_bps - slippage_bps
        edge_no_bps = (0.5 - no_buy) * 10000 - fee_bps - slippage_bps

        base_size = min(50.0, max(10.0, s.depth_usd * 0.005))
        out.append(
            Opportunity(
                market_id=s.market_id,
                side="BUY_YES",
                edge_bps=round(edge_yes_bps, 2),
                expected_price=yes_buy,
                size_usd=base_size,
            )
        )
        out.append(
            Opportunity(
                market_id=s.market_id,
                side="BUY_NO",
                edge_bps=round(edge_no_bps, 2),
                expected_price=no_buy,
                size_usd=base_size,
            )
        )

    return sorted(out, key=lambda x: x.edge_bps, reverse=True)


def score_opportunities(snapshots: List[MarketSnapshot], cfg: dict) -> List[Opportunity]:
    min_edge = float(cfg["scoring"]["min_edge_bps"])
    return [c for c in rank_candidates(snapshots, cfg) if c.edge_bps >= min_edge]
