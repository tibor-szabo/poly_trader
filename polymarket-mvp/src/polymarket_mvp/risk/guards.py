from polymarket_mvp.models import Opportunity, Decision, RunState


def approve(op: Opportunity, state: RunState, cfg: dict) -> Decision:
    if op.size_usd > cfg["risk"]["max_notional_per_market_usd"]:
        return Decision(approved=False, reason="size_above_market_cap")
    if state.realized_pnl_usd <= -abs(cfg["risk"]["max_daily_loss_usd"]):
        return Decision(approved=False, reason="daily_loss_limit")
    if state.cash_usd < op.size_usd:
        return Decision(approved=False, reason="insufficient_cash")
    if len(state.positions) >= cfg["risk"]["max_open_markets"]:
        return Decision(approved=False, reason="max_open_markets")
    return Decision(approved=True, reason="ok")
