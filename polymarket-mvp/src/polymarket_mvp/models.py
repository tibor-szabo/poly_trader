from pydantic import BaseModel, Field
from typing import List, Optional


class MarketSnapshot(BaseModel):
    market_id: str
    token_id: str
    question: str = ""
    yes_bid: float
    yes_ask: float
    no_bid: float
    no_ask: float
    depth_usd: float
    accepting_orders: bool = True
    yes_hint: float = 0.0
    no_hint: float = 0.0
    yes_asks: List[dict] = Field(default_factory=list)
    no_asks: List[dict] = Field(default_factory=list)


class Opportunity(BaseModel):
    market_id: str
    side: str  # BUY_YES / BUY_NO
    edge_bps: float
    expected_price: float
    size_usd: float


class Decision(BaseModel):
    approved: bool
    reason: str


class PaperPosition(BaseModel):
    market_id: str
    market_name: str = ""
    side: str  # BUY_YES / BUY_NO
    status: str = "open"  # open / closed
    size_usd: float
    qty: float = 0.0
    entry_price: float
    exit_price: Optional[float] = None
    pnl_usd: Optional[float] = None
    opened_at: str = ""
    closed_at: Optional[str] = None
    model: str = ""  # model used to open
    close_model: Optional[str] = None  # model used to close


class RunState(BaseModel):
    cash_usd: float
    positions: List[PaperPosition] = Field(default_factory=list)
    closed_positions: List[PaperPosition] = Field(default_factory=list)
    realized_pnl_usd: float = 0.0
