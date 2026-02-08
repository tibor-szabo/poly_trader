import asyncio
import json
import threading
import time
from collections import deque
from typing import Callable, Dict, Iterable, Optional, Tuple

import websockets


class ClobWsHook:
    def __init__(self, url: str = "wss://ws-subscriptions-clob.polymarket.com/ws/market"):
        self.url = url
        self._asset_ids = set()
        self._best: Dict[str, Dict[str, float]] = {}
        self._last_msg_ts = 0.0
        self._lock = threading.Lock()
        self._cond = threading.Condition(self._lock)
        self._thread: Optional[threading.Thread] = None
        self._running = False
        self._needs_subscribe = False
        self._token_meta: Dict[str, Dict[str, str]] = {}
        self._on_tick: Optional[Callable[[dict], None]] = None
        self._last_emit_by_market: Dict[str, float] = {}
        self._market_tick_history: Dict[str, deque] = {}

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def subscribe_assets(self, asset_ids: Iterable[str]):
        changed = False
        with self._lock:
            for a in asset_ids:
                if a and str(a) not in self._asset_ids:
                    self._asset_ids.add(str(a))
                    changed = True
            if changed:
                self._needs_subscribe = True

    def get_best(self, asset_id: str) -> Tuple[Optional[float], Optional[float]]:
        with self._lock:
            row = self._best.get(str(asset_id))
            if not row:
                return None, None
            return row.get("bid"), row.get("ask")

    def set_on_tick(self, cb: Optional[Callable[[dict], None]]):
        self._on_tick = cb

    def set_token_meta(self, pairs: Iterable[dict]):
        with self._lock:
            for p in pairs:
                market_id = str(p.get("market_id", ""))
                market_name = str(p.get("market_name", ""))
                y = str(p.get("yes_token", ""))
                n = str(p.get("no_token", ""))
                if y:
                    self._token_meta[y] = {
                        "market_id": market_id,
                        "market_name": market_name,
                        "side": "yes",
                        "yes_token": y,
                        "no_token": n,
                    }
                if n:
                    self._token_meta[n] = {
                        "market_id": market_id,
                        "market_name": market_name,
                        "side": "no",
                        "yes_token": y,
                        "no_token": n,
                    }

    def stats(self):
        with self._lock:
            return {
                "asset_count": len(self._asset_ids),
                "tracked_count": len(self._best),
                "last_msg_ts": self._last_msg_ts,
                "alive": self._running,
            }

    def wait_for_update(self, after_ts: float, timeout: float = 1.0) -> float:
        with self._cond:
            if self._last_msg_ts > float(after_ts):
                return self._last_msg_ts
            self._cond.wait(timeout=max(0.05, float(timeout)))
            return self._last_msg_ts

    def get_market_metrics(self, window_seconds: int = 600) -> Dict[str, dict]:
        now = time.time()
        out: Dict[str, dict] = {}
        with self._lock:
            for mk, dq in self._market_tick_history.items():
                while dq and (now - dq[0][0]) > window_seconds:
                    dq.popleft()
                if not dq:
                    continue
                sums = []
                ys = []
                ns = []
                for x in dq:
                    try:
                        if isinstance(x, (list, tuple)):
                            if len(x) >= 4:
                                if x[1] is not None:
                                    ys.append(x[1])
                                if x[2] is not None:
                                    ns.append(x[2])
                                if x[3] is not None:
                                    sums.append(x[3])
                            elif len(x) >= 2:
                                # backward compatibility with old (ts, sum) tuples
                                if x[1] is not None:
                                    sums.append(x[1])
                    except Exception:
                        continue
                if not sums and not ys and not ns:
                    continue
                updates_per_min = (len(dq) * 60.0) / max(window_seconds, 1)
                sum_vol = (max(sums) - min(sums)) if sums else 0.0
                yes_vol = (max(ys) - min(ys)) if ys else 0.0
                no_vol = (max(ns) - min(ns)) if ns else 0.0
                out[mk] = {
                    "updates_per_min": updates_per_min,
                    "sum_volatility": sum_vol,
                    "yes_volatility": yes_vol,
                    "no_volatility": no_vol,
                    "ask_volatility": yes_vol + no_vol,
                    "last_sum": sums[-1] if sums else None,
                    "samples": len(dq),
                }
        return out

    def _run(self):
        asyncio.run(self._run_async())

    async def _run_async(self):
        while self._running:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20, close_timeout=10) as ws:
                    await self._send_subscribe(ws, full=True)
                    while self._running:
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=5)
                            self._on_message(msg)
                        except asyncio.TimeoutError:
                            pass
                        if self._take_subscribe_flag():
                            await self._send_subscribe(ws, full=False)
            except Exception:
                await asyncio.sleep(2)

    async def _send_subscribe(self, ws, full: bool):
        with self._lock:
            assets = list(self._asset_ids)
            self._needs_subscribe = False
        if not assets:
            return
        payload = {
            "assets_ids": assets,
            "custom_feature_enabled": True,
        }
        if full:
            payload["type"] = "MARKET"
        else:
            payload["operation"] = "subscribe"
        await ws.send(json.dumps(payload))

    def _take_subscribe_flag(self) -> bool:
        with self._lock:
            x = self._needs_subscribe
            self._needs_subscribe = False
            return x

    def _on_message(self, raw: str):
        try:
            obj = json.loads(raw)
        except Exception:
            return

        with self._cond:
            self._last_msg_ts = time.time()
            self._cond.notify_all()

        items = obj if isinstance(obj, list) else [obj]
        for it in items:
            if not isinstance(it, dict):
                continue
            et = str(it.get("event_type", "")).lower()
            if et == "best_bid_ask":
                aid = str(it.get("asset_id", ""))
                bid = _f(it.get("best_bid"))
                ask = _f(it.get("best_ask"))
                self._store(aid, bid, ask)
                continue

            if et == "book":
                aid = str(it.get("asset_id", ""))
                bids = it.get("bids", []) or it.get("buys", [])
                asks = it.get("asks", []) or it.get("sells", [])
                bid = max([_f(x.get("price")) for x in bids if _f(x.get("price")) > 0] or [0.0])
                ask_vals = [_f(x.get("price")) for x in asks if _f(x.get("price")) > 0]
                ask = min(ask_vals) if ask_vals else 0.0
                self._store(aid, bid, ask)
                continue

            if et == "price_change":
                for ch in (it.get("price_changes") or []):
                    aid = str(ch.get("asset_id", ""))
                    bid = _f(ch.get("best_bid"))
                    ask = _f(ch.get("best_ask"))
                    self._store(aid, bid, ask)

    def _store(self, aid: str, bid: float, ask: float):
        if not aid:
            return
        tick = None
        with self._lock:
            cur = self._best.get(aid, {})
            if bid > 0:
                cur["bid"] = bid
            if ask > 0:
                cur["ask"] = ask
            self._best[aid] = cur

            meta = self._token_meta.get(aid)
            if meta:
                y = self._best.get(meta.get("yes_token", ""), {})
                n = self._best.get(meta.get("no_token", ""), {})
                y_ask = y.get("ask")
                n_ask = n.get("ask")
                now_ts = time.time()
                mk = meta.get("market_id", "")
                if mk:
                    ask_sum = (float(y_ask) + float(n_ask)) if (y_ask is not None and n_ask is not None) else None
                    dq = self._market_tick_history.get(mk)
                    if dq is None:
                        dq = deque(maxlen=5000)
                        self._market_tick_history[mk] = dq
                    dq.append((now_ts, y_ask, n_ask, ask_sum))

                    if (now_ts - self._last_emit_by_market.get(mk, 0.0) >= 0.25):
                        self._last_emit_by_market[mk] = now_ts
                        tick = {
                            "market_id": mk,
                            "market_name": meta.get("market_name", ""),
                            "best_ask_yes": y_ask,
                            "best_ask_no": n_ask,
                            "ask_sum_no_fees": ask_sum,
                            "ws_asset_id": aid,
                            "ws_ts": now_ts,
                        }
        if tick and self._on_tick:
            try:
                self._on_tick(tick)
            except Exception:
                pass


def _f(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0
