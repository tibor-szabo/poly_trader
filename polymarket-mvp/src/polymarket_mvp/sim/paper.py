from datetime import datetime, timezone
from polymarket_mvp.models import RunState, PaperPosition


def init_state(cfg: dict) -> RunState:
    return RunState(cash_usd=float(cfg["paper"]["starting_cash_usd"]), positions=[], closed_positions=[])


def open_position(state: RunState, market_id: str, market_name: str, side: str, entry_price: float, size_usd: float, model: str) -> PaperPosition:
    size = min(float(size_usd), float(state.cash_usd))
    if size <= 0 or entry_price <= 0:
        raise ValueError("invalid_open")
    qty = size / float(entry_price)
    pos = PaperPosition(
        market_id=market_id,
        market_name=market_name,
        side=side,
        status="open",
        size_usd=size,
        qty=qty,
        entry_price=float(entry_price),
        opened_at=datetime.now(timezone.utc).isoformat(),
        model=model,
    )
    state.cash_usd -= size
    state.positions.append(pos)
    return pos


def close_position(state: RunState, pos: PaperPosition, exit_price: float) -> float:
    if exit_price <= 0:
        raise ValueError("invalid_close")
    proceeds = float(pos.qty) * float(exit_price)
    pnl = proceeds - float(pos.size_usd)
    state.cash_usd += proceeds
    state.realized_pnl_usd += pnl
    pos.status = "closed"
    pos.exit_price = float(exit_price)
    pos.pnl_usd = float(pnl)
    pos.closed_at = datetime.now(timezone.utc).isoformat()
    state.positions = [p for p in state.positions if not (p.market_id == pos.market_id and p.opened_at == pos.opened_at)]
    state.closed_positions.append(pos)
    return pnl
