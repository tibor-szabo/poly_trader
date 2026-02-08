import asyncio
import json
import threading
import time
from typing import Callable, Optional

import websockets


class BtcRtdsHook:
    def __init__(self, url: str = "wss://ws-live-data.polymarket.com"):
        self.url = url
        self._chainlink_price: Optional[float] = None
        self._binance_price: Optional[float] = None
        self._ts: float = 0.0
        self._running = False
        self._thread: Optional[threading.Thread] = None
        self._lock = threading.Lock()
        self._on_tick: Optional[Callable[[dict], None]] = None

    def start(self):
        if self._running:
            return
        self._running = True
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False

    def get(self):
        with self._lock:
            return {
                "chainlink": self._chainlink_price,
                "binance": self._binance_price,
                "ts": self._ts,
            }

    def set_on_tick(self, cb: Optional[Callable[[dict], None]]):
        self._on_tick = cb

    def _run(self):
        asyncio.run(self._run_async())

    async def _run_async(self):
        sub_msg = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": '{"symbol":"btc/usd"}',
                },
                {
                    "topic": "crypto_prices",
                    "type": "update",
                    "filters": '{"symbol":"btcusdt"}',
                },
            ],
        }

        while self._running:
            try:
                async with websockets.connect(self.url, ping_interval=20, ping_timeout=20) as ws:
                    await ws.send(json.dumps(sub_msg))
                    while self._running:
                        msg = await ws.recv()
                        self._on_msg(msg)
            except Exception:
                await asyncio.sleep(1.0)

    def _on_msg(self, raw: str):
        try:
            obj = json.loads(raw)
        except Exception:
            return
        payload = obj.get("payload") if isinstance(obj, dict) else None
        if not isinstance(payload, dict):
            return

        # Snapshot message may come as payload.data list without symbol; ignore.
        if isinstance(payload.get("data"), list):
            return

        sym = str(payload.get("symbol", "")).lower()
        try:
            px = float(payload.get("value"))
        except Exception:
            return
        tick = None
        with self._lock:
            if sym == "btc/usd":
                self._chainlink_price = px
            elif sym == "btcusdt":
                self._binance_price = px
            else:
                return
            self._ts = time.time()
            tick = {
                "chainlink": self._chainlink_price,
                "binance": self._binance_price,
                "ts": self._ts,
                "symbol": sym,
            }
        if tick and self._on_tick:
            try:
                self._on_tick(tick)
            except Exception:
                pass
