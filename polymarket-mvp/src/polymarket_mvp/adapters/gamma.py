from __future__ import annotations
import json
from dataclasses import dataclass
from typing import List
from datetime import datetime, timezone
import httpx


@dataclass
class GammaMarketRef:
    market_id: str
    question: str
    yes_token: str
    no_token: str
    accepting_orders: bool
    liquidity_num: float
    yes_price_hint: float = 0.0
    no_price_hint: float = 0.0
    end_date: str = ""
    slug: str = ""
    resolution_source: str = ""
    event_start_time: str = ""


class GammaAdapter:
    def __init__(self, base_url: str):
        self.base_url = base_url.rstrip("/")
        self.call_count = 0

    def reset_call_count(self):
        self.call_count = 0

    def _counted_get(self, client: httpx.Client, url: str, **kwargs):
        self.call_count += 1
        return client.get(url, **kwargs)

    @staticmethod
    def _to_ref(m: dict) -> GammaMarketRef | None:
        try:
            token_ids = m.get("clobTokenIds")
            if isinstance(token_ids, str):
                token_ids = json.loads(token_ids)
            if not token_ids or len(token_ids) < 2:
                return None
            prices = m.get("outcomePrices")
            if isinstance(prices, str):
                try:
                    prices = json.loads(prices)
                except Exception:
                    prices = None
            yes_hint = float(prices[0]) if isinstance(prices, list) and len(prices) > 0 else 0.0
            no_hint = float(prices[1]) if isinstance(prices, list) and len(prices) > 1 else 0.0

            ev0 = {}
            try:
                if isinstance(m.get("events"), list) and m.get("events"):
                    ev0 = m.get("events")[0] or {}
            except Exception:
                ev0 = {}

            return GammaMarketRef(
                market_id=str(m.get("id")),
                question=str(m.get("question", "")),
                yes_token=str(token_ids[0]),
                no_token=str(token_ids[1]),
                accepting_orders=bool(m.get("acceptingOrders", True)),
                liquidity_num=float(m.get("liquidityNum") or 0.0),
                yes_price_hint=yes_hint,
                no_price_hint=no_hint,
                end_date=str(m.get("endDate") or ev0.get("endDate") or ""),
                slug=str(m.get("slug") or ev0.get("slug") or ""),
                resolution_source=str(m.get("resolutionSource") or ev0.get("resolutionSource") or ""),
                event_start_time=str(m.get("eventStartTime") or ev0.get("startTime") or ""),
            )
        except Exception:
            return None

    def fetch_active_market_refs(self, limit: int = 10, focus_keywords: list[str] | None = None) -> List[GammaMarketRef]:
        url = f"{self.base_url}/markets"
        params = {"active": "true", "closed": "false", "limit": str(limit)}
        with httpx.Client(timeout=15.0) as client:
            r = self._counted_get(client, url, params=params)
            r.raise_for_status()
            arr = r.json()

        refs: List[GammaMarketRef] = []
        kws = [k.lower() for k in (focus_keywords or []) if k]
        for m in arr:
            q = str(m.get("question", ""))
            slug = str(m.get("slug", ""))
            hay = (q + " " + slug).lower()
            if kws and not any(k.lower() in hay for k in kws):
                continue
            ref = self._to_ref(m)
            if ref:
                refs.append(ref)
        return refs

    def fetch_market_refs_by_slugs(self, slugs: list[str]) -> List[GammaMarketRef]:
        refs: List[GammaMarketRef] = []
        with httpx.Client(timeout=15.0) as client:
            for slug in slugs:
                if not slug:
                    continue
                r = self._counted_get(client, f"{self.base_url}/markets", params={"slug": slug})
                if r.status_code != 200:
                    continue
                arr = r.json()
                for m in arr:
                    ref = self._to_ref(m)
                    if ref:
                        refs.append(ref)
        return refs

    def fetch_market_refs_by_slug_prefixes(
        self,
        prefixes: list[str],
        limit: int = 200,
        active_only: bool = True,
    ) -> List[GammaMarketRef]:
        if not prefixes:
            return []
        params = {"limit": str(limit)}
        if active_only:
            params.update({"active": "true", "closed": "false"})

        with httpx.Client(timeout=20.0) as client:
            r = self._counted_get(client, f"{self.base_url}/markets", params=params)
            r.raise_for_status()
            arr = r.json()

        prefs = [p.lower() for p in prefixes if p]
        refs: List[GammaMarketRef] = []
        for m in arr:
            slug = str(m.get("slug", "")).lower()
            if not any(slug.startswith(p) for p in prefs):
                continue
            ref = self._to_ref(m)
            if ref:
                refs.append(ref)

        # Sort latest-ending first so rolling markets pick newest window.
        def _end_ts(mref: GammaMarketRef) -> str:
            return str(next((x.get("endDate") for x in arr if str(x.get("id")) == mref.market_id), ""))

        refs.sort(key=_end_ts, reverse=True)
        return refs

    def fetch_market_refs_by_generated_timeframe_slugs(
        self,
        prefixes: list[str],
        timeframe: str,
        bucket_seconds: int,
        windows: int = 8,
        lookback_windows: int = 8,
    ) -> List[GammaMarketRef]:
        # Generate rolling slugs like btc-updown-15m-<unix_ts> / btc-updown-5m-<unix_ts>.
        if not prefixes:
            return []

        now = int(datetime.now(timezone.utc).timestamp())
        base = (now // int(bucket_seconds)) * int(bucket_seconds)
        ts_candidates = [base + int(bucket_seconds) * k for k in range(-int(lookback_windows), int(windows) + 1)]

        slugs: list[str] = []
        tf = (timeframe or "").lower()
        for p in prefixes:
            lp = (p or "").lower()
            if tf not in lp:
                continue
            for t in ts_candidates:
                slugs.append(f"{p}{t}")

        return self.fetch_market_refs_by_slugs(slugs)

    def fetch_market_refs_by_generated_15m_slugs(self, prefixes: list[str], windows: int = 8) -> List[GammaMarketRef]:
        return self.fetch_market_refs_by_generated_timeframe_slugs(
            prefixes=prefixes,
            timeframe="15m",
            bucket_seconds=900,
            windows=windows,
            lookback_windows=8,
        )
