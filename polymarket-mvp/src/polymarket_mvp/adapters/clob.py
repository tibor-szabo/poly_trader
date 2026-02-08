from __future__ import annotations
from typing import List, Optional
import httpx

from polymarket_mvp.models import MarketSnapshot
from polymarket_mvp.adapters.gamma import GammaMarketRef


class ClobAdapter:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.call_count = 0

    def reset_call_count(self):
        self.call_count = 0

    def _fetch_book(self, token_id: str) -> Optional[dict]:
        url = f"{self.base_url}/book"
        with httpx.Client(timeout=15.0) as client:
            self.call_count += 1
            r = client.get(url, params={"token_id": token_id})
            if r.status_code != 200:
                return None
            return r.json()

    @staticmethod
    def _best_ask(levels: list) -> float:
        if not levels:
            return 0.0
        vals = []
        for lvl in levels:
            try:
                px = float((lvl or {}).get("price", 0.0))
                if px > 0:
                    vals.append(px)
            except Exception:
                continue
        return min(vals) if vals else 0.0

    @staticmethod
    def _best_bid(levels: list) -> float:
        if not levels:
            return 0.0
        vals = []
        for lvl in levels:
            try:
                px = float((lvl or {}).get("price", 0.0))
                if px > 0:
                    vals.append(px)
            except Exception:
                continue
        return max(vals) if vals else 0.0

    @staticmethod
    def _depth_usd(levels: list, depth_n: int = 5) -> float:
        total = 0.0
        for lvl in levels[:depth_n]:
            try:
                total += float(lvl.get("price", 0.0)) * float(lvl.get("size", 0.0))
            except Exception:
                continue
        return total

    def fetch_snapshots(self) -> List[MarketSnapshot]:
        # kept for backward compatibility (demo mode fallback)
        return [
            MarketSnapshot(
                market_id="demo-market-1",
                token_id="token-yes-1",
                question="Demo market (fallback)",
                yes_bid=0.47,
                yes_ask=0.49,
                no_bid=0.51,
                no_ask=0.53,
                depth_usd=3500,
                accepting_orders=True,
            )
        ]

    def fetch_snapshots_from_refs(self, refs: List[GammaMarketRef]) -> List[MarketSnapshot]:
        out: List[MarketSnapshot] = []
        for ref in refs:
            yes_book = self._fetch_book(ref.yes_token)
            no_book = self._fetch_book(ref.no_token)
            if not yes_book or not no_book:
                continue

            yes_bid = self._best_bid(yes_book.get("bids", []))
            yes_ask = self._best_ask(yes_book.get("asks", []))
            no_bid = self._best_bid(no_book.get("bids", []))
            no_ask = self._best_ask(no_book.get("asks", []))

            if yes_ask <= 0 or no_ask <= 0:
                continue

            bid_depth = self._depth_usd(yes_book.get("bids", []), 3) + self._depth_usd(no_book.get("bids", []), 3)

            # Filter only truly dead books (near-1 asks + near-0 bids + tiny depth).
            # Keep thin-but-real books visible so scanner still reports why they are rejected.
            if yes_ask >= 0.985 and no_ask >= 0.985 and yes_bid <= 0.015 and no_bid <= 0.015 and bid_depth < 25:
                continue

            depth = self._depth_usd(yes_book.get("bids", []), 5) + self._depth_usd(no_book.get("bids", []), 5)

            out.append(
                MarketSnapshot(
                    market_id=ref.market_id,
                    token_id=ref.yes_token,
                    question=ref.question,
                    yes_bid=yes_bid,
                    yes_ask=yes_ask,
                    no_bid=no_bid,
                    no_ask=no_ask,
                    depth_usd=max(depth, ref.liquidity_num),
                    accepting_orders=ref.accepting_orders,
                    yes_hint=ref.yes_price_hint,
                    no_hint=ref.no_price_hint,
                    yes_asks=(yes_book.get("asks", []) or [])[:12],
                    no_asks=(no_book.get("asks", []) or [])[:12],
                )
            )
        return out
