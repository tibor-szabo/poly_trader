from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass
class LiveOrderResult:
    ok: bool
    order_id: Optional[str] = None
    error: Optional[str] = None
    raw: Optional[dict] = None


class LiveExecutor:
    """Live Polymarket CLOB executor.

    Uses py-clob-client when available. Kept optional so paper mode works without
    wallet/signing dependencies installed.
    """

    def __init__(self, cfg: dict):
        self.cfg = cfg
        live = cfg.get("live", {})
        self.enabled = bool(live.get("enabled", False))
        self.dry_run = bool(live.get("dry_run", True))
        self.host = str(live.get("clob_host") or cfg.get("data", {}).get("clob_rest_base") or "https://clob.polymarket.com")
        self.chain_id = int(live.get("chain_id", 137))
        self.signature_type = int(live.get("signature_type", 1))
        self.default_order_type = str(live.get("order_type", "GTC")).upper()

        self._client = None
        self._imports_ok = False
        self._import_error = None

    def _ensure_client(self) -> Tuple[bool, Optional[str]]:
        if self._client is not None:
            return True, None
        if self.dry_run:
            return True, None

        try:
            from py_clob_client.client import ClobClient
        except Exception as e:
            self._import_error = f"py_clob_client_missing: {e}"
            return False, self._import_error

        key = os.getenv("POLYMARKET_PRIVATE_KEY", "").strip()
        funder = os.getenv("POLYMARKET_FUNDER", "").strip()
        api_key = os.getenv("POLYMARKET_API_KEY", "").strip()
        api_secret = os.getenv("POLYMARKET_API_SECRET", "").strip()
        api_passphrase = os.getenv("POLYMARKET_API_PASSPHRASE", "").strip()
        if not key:
            return False, "POLYMARKET_PRIVATE_KEY is missing"

        try:
            c = ClobClient(
                self.host,
                key=key,
                chain_id=self.chain_id,
                signature_type=self.signature_type,
                funder=funder or None,
            )
            # Prefer provided API creds; fallback to derive/create.
            if api_key and api_secret and api_passphrase:
                c.set_api_creds({
                    "key": api_key,
                    "secret": api_secret,
                    "passphrase": api_passphrase,
                })
            else:
                creds = c.create_or_derive_api_creds()
                c.set_api_creds(creds)
            self._client = c
            self._imports_ok = True
            return True, None
        except Exception as e:
            return False, f"clob_init_failed: {e}"

    def place(self, token_id: str, side: str, price: float, size: float, post_only: bool = False) -> LiveOrderResult:
        if not self.enabled:
            return LiveOrderResult(ok=False, error="live_disabled")

        if not token_id:
            return LiveOrderResult(ok=False, error="token_id_missing")
        if price <= 0 or size <= 0:
            return LiveOrderResult(ok=False, error="invalid_price_or_size")

        if self.dry_run:
            return LiveOrderResult(ok=True, order_id="dry_run", raw={
                "token_id": token_id,
                "side": side,
                "price": price,
                "size": size,
                "post_only": post_only,
            })

        ok, err = self._ensure_client()
        if not ok:
            return LiveOrderResult(ok=False, error=err)

        try:
            from py_clob_client.clob_types import OrderArgs, OrderType

            s = side.upper()
            order_type_name = "POST_ONLY" if post_only else self.default_order_type
            order_type = getattr(OrderType, order_type_name, getattr(OrderType, "GTC"))

            args = OrderArgs(
                token_id=token_id,
                price=float(price),
                size=float(size),
                side=s,
            )
            signed = self._client.create_order(args)
            resp = self._client.post_order(signed, order_type)

            oid = None
            if isinstance(resp, dict):
                oid = resp.get("orderID") or resp.get("id")
            return LiveOrderResult(ok=True, order_id=oid, raw=resp if isinstance(resp, dict) else {"resp": str(resp)})
        except Exception as e:
            return LiveOrderResult(ok=False, error=f"post_order_failed: {e}")
