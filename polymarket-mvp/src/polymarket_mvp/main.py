import argparse
import re
import math
import random
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from rich import print

from polymarket_mvp.config import load_config
from polymarket_mvp.adapters.clob import ClobAdapter
from polymarket_mvp.engine.scoring import score_opportunities, rank_candidates, depth_aware_buy_prices
from polymarket_mvp.risk.guards import approve
from polymarket_mvp.sim.paper import open_position, close_position, close_fraction
from polymarket_mvp.utils.storage import load_state, save_state, append_event
from polymarket_mvp.adapters.gamma import GammaAdapter
from polymarket_mvp.ops_intel import build_market_radar, build_inefficiency_report, build_flow_watch
from polymarket_mvp.ws_hook import ClobWsHook
from polymarket_mvp.rtds_hook import BtcRtdsHook
from polymarket_mvp.execution.live import LiveExecutor


_WS_HOOK = None
_RTDS_BTC = None
_ALT_REFS_CACHE = []
_ALT_REFS_TS = 0.0
_BTC_TARGET_CACHE = {}
_BTC_TARGET_MISS_LAST = {}
_BTC_PRICE_CACHE = {}
_BTC_CURRENT_CACHE = {"ts": 0.0, "price": None}
_BTC_PRICE_CACHE_TTL_OK = 120.0
_BTC_PRICE_CACHE_TTL_MISS = 20.0
_BTC_PRICE_FORCE_REFRESH_SECONDS = 60.0
_BTC_SIGNAL_HISTORY = []
_MODEL_STATS = {
    "TA": {"trades": 0, "wins": 0, "pnl": 0.0},
    "LL": {"trades": 0, "wins": 0, "pnl": 0.0},
    "RG": {"trades": 0, "wins": 0, "pnl": 0.0},
    "BK": {"trades": 0, "wins": 0, "pnl": 0.0},
}
_LAST_CLOSE_TS = {}
_LAST_CLOSE_REASON = {}
_LAST_CLOSE_SIDE = {}
_LAST_CLOSE_PNL = {}
_EDGE_HIST = {}
_WINNER_HIST = {}
_MODEL_SIDE_HIST = {}
_PRICE_SRC_HIST = {"binance": [], "coinbase": [], "kraken": [], "bybit": []}
_PRICE_SRC_LAST = {"coinbase": 0.0, "kraken": 0.0, "bybit": 0.0}
_BINANCE_OPEN_PRICE_CACHE = {}
_FLIP_FAIL_STREAK = {}
_MARKET_LOCK_UNTIL = {}
_GLOBAL_OPEN_PAUSE_UNTIL = 0.0
_RECENT_FLIP_STOP_LOSS_TS = []
_PENDING_CLOSES = {}
_LIVE_EXECUTOR = None
_LAST_TA_SIG = {}
_LAST_OPEN_TS = 0.0


def _pos_key(pos) -> str:
    return f"{pos.market_id}|{pos.opened_at}|{pos.side}"


def _round_price(px: float, tick: float) -> float:
    if tick <= 0:
        return float(px)
    return round(round(float(px) / tick) * tick, 6)


def _build_close_order(side: str, row: dict, cfg: dict) -> dict:
    ex = cfg.get("execution", {})
    close_mode = str(ex.get("close_mode", "limit_first")).lower()
    tick = float(ex.get("tick_size", 0.001))
    improve_ticks = int(ex.get("close_limit_improve_ticks", 1))

    bid = float(row.get("best_bid_yes") or 0.0) if side == "BUY_YES" else float(row.get("best_bid_no") or 0.0)
    ask = float(row.get("best_ask_yes") or 0.0) if side == "BUY_YES" else float(row.get("best_ask_no") or 0.0)

    # Selling out of an existing BUY_YES/BUY_NO position.
    taker_px = bid if bid > 0 else ask
    if close_mode == "market":
        return {"mode": "market", "taker_price": taker_px, "limit_price": None, "bid": bid, "ask": ask}

    if bid > 0 and ask > 0 and ask >= bid:
        target = min(ask, bid + (improve_ticks * tick))
    elif ask > 0:
        target = ask
    else:
        target = bid
    target = _round_price(target, tick)
    return {"mode": "limit_first", "taker_price": taker_px, "limit_price": target, "bid": bid, "ask": ask}


def _resolve_limit_close(pos, close_reason: str, order: dict, cfg: dict):
    ex = cfg.get("execution", {})
    timeout_s = float(ex.get("close_limit_timeout_s", 20.0))
    reprice_s = float(ex.get("close_limit_reprice_s", 4.0))
    tick = float(ex.get("tick_size", 0.001))
    force_reasons = set(ex.get("close_force_taker_reasons", ["hard_stop_25", "resolved_loss_proxy", "flip_stop"]))

    key = _pos_key(pos)
    now_ts = datetime.now(timezone.utc).timestamp()
    bid = float(order.get("bid") or 0.0)
    ask = float(order.get("ask") or 0.0)
    taker_px = float(order.get("taker_price") or 0.0)
    limit_px = float(order.get("limit_price") or 0.0)

    if close_reason in force_reasons:
        _PENDING_CLOSES.pop(key, None)
        return (taker_px if taker_px > 0 else None), "close_force_taker", None

    st = _PENDING_CLOSES.get(key)
    if st is None:
        st = {
            "created_ts": now_ts,
            "last_reprice_ts": now_ts,
            "attempts": 1,
            "limit_price": limit_px,
            "reason": close_reason,
        }
        _PENDING_CLOSES[key] = st
    else:
        if (now_ts - float(st.get("last_reprice_ts") or 0.0)) >= reprice_s:
            st["attempts"] = int(st.get("attempts") or 0) + 1
            st["last_reprice_ts"] = now_ts
            if bid > 0:
                st["limit_price"] = _round_price(min(float(st.get("limit_price") or limit_px), bid + tick), tick)

    lp = float(st.get("limit_price") or 0.0)
    if bid > 0 and lp > 0 and bid >= lp:
        _PENDING_CLOSES.pop(key, None)
        return lp, "close_limit_fill", {"wait_s": round(now_ts - float(st.get("created_ts") or now_ts), 2), "attempts": st.get("attempts")}

    if (now_ts - float(st.get("created_ts") or now_ts)) >= timeout_s:
        _PENDING_CLOSES.pop(key, None)
        px = taker_px if taker_px > 0 else (bid if bid > 0 else ask)
        return (px if px > 0 else None), "close_limit_timeout_fallback", {"wait_s": round(now_ts - float(st.get("created_ts") or now_ts), 2), "attempts": st.get("attempts")}

    return None, None, {
        "wait_s": round(now_ts - float(st.get("created_ts") or now_ts), 2),
        "attempts": st.get("attempts"),
        "limit_price": lp,
        "best_bid": bid,
        "best_ask": ask,
    }


def _ensure_live_executor(cfg: dict) -> LiveExecutor:
    global _LIVE_EXECUTOR
    if _LIVE_EXECUTOR is None:
        _LIVE_EXECUTOR = LiveExecutor(cfg)
    return _LIVE_EXECUTOR


def _ensure_ws_hook() -> ClobWsHook:
    global _WS_HOOK
    if _WS_HOOK is None:
        _WS_HOOK = ClobWsHook()
        _WS_HOOK.start()
    return _WS_HOOK


def _parse_dt(s: str):
    try:
        return datetime.fromisoformat((s or "").replace("Z", "+00:00"))
    except Exception:
        return None


def _seconds_since_iso(s: str) -> float:
    dt = _parse_dt(s)
    if not dt:
        return 0.0
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return max(0.0, (datetime.now(timezone.utc) - dt.astimezone(timezone.utc)).total_seconds())


def _is_btc_ref(r) -> bool:
    q = (getattr(r, "question", "") or "").lower()
    sl = (getattr(r, "slug", "") or "").lower()
    hay = q + " " + sl
    return ("bitcoin" in hay) or ("btc" in hay)


def _ensure_btc_live_feed(events_path: str = None):
    global _RTDS_BTC
    if _RTDS_BTC is None:
        _RTDS_BTC = BtcRtdsHook()
        if events_path:
            _RTDS_BTC.set_on_tick(lambda t: append_event(events_path, {"type": "btc_price_tick", **t}))
        _RTDS_BTC.start()


def _btc_live_prices() -> tuple[Optional[float], Optional[float]]:
    chainlink_px = _BTC_CURRENT_CACHE.get("price")
    binance_px = None
    if _RTDS_BTC is not None:
        snap = _RTDS_BTC.get()
        chainlink_px = snap.get("chainlink") or chainlink_px
        binance_px = snap.get("binance")
        if chainlink_px is not None:
            _BTC_CURRENT_CACHE["price"] = chainlink_px
            _BTC_CURRENT_CACHE["ts"] = snap.get("ts") or _BTC_CURRENT_CACHE.get("ts")
    return chainlink_px, binance_px


def _chainlink_btc_current_price() -> Optional[float]:
    a, _ = _btc_live_prices()
    return a


def _update_btc_signal_history(chainlink_px: Optional[float], binance_px: Optional[float]):
    now = datetime.now(timezone.utc).timestamp()
    p = None
    if chainlink_px is not None and binance_px is not None:
        p = 0.4 * float(chainlink_px) + 0.6 * float(binance_px)
    elif chainlink_px is not None:
        p = float(chainlink_px)
    elif binance_px is not None:
        p = float(binance_px)
    if p is not None and p > 0:
        _BTC_SIGNAL_HISTORY.append({"t": now, "p": p, "cl": chainlink_px, "bi": binance_px})
    keep_after = now - 700
    while _BTC_SIGNAL_HISTORY and _BTC_SIGNAL_HISTORY[0]["t"] < keep_after:
        _BTC_SIGNAL_HISTORY.pop(0)


def _price_ago(sec: float) -> Optional[float]:
    if not _BTC_SIGNAL_HISTORY:
        return None
    now = _BTC_SIGNAL_HISTORY[-1]["t"]
    for x in reversed(_BTC_SIGNAL_HISTORY):
        if (now - x["t"]) >= sec:
            return float(x["p"])
    return float(_BTC_SIGNAL_HISTORY[0]["p"])


def _latest_signal_price(max_staleness_s: float = 120.0) -> Optional[float]:
    if not _BTC_SIGNAL_HISTORY:
        return None
    last = _BTC_SIGNAL_HISTORY[-1]
    now_ts = datetime.now(timezone.utc).timestamp()
    age = now_ts - float(last.get("t", 0.0))
    if age > max_staleness_s:
        return None
    p = float(last.get("p") or 0.0)
    return p if p > 0 else None


def _price_near_ts(ts: float, max_delta_s: float = 120.0) -> Optional[float]:
    if not _BTC_SIGNAL_HISTORY:
        return None
    best = None
    best_dt = 1e18
    for x in _BTC_SIGNAL_HISTORY:
        dt = abs(float(x.get("t", 0.0)) - float(ts))
        if dt < best_dt:
            best_dt = dt
            best = float(x.get("p"))
    if best is None or best_dt > max_delta_s:
        return None
    return best


def _binance_open_price_near_ts(ts: float, max_age_s: float = 7200.0) -> Optional[float]:
    now_ts = datetime.now(timezone.utc).timestamp()
    key = int(ts // 60)
    hit = _BINANCE_OPEN_PRICE_CACHE.get(key)
    if hit and (now_ts - float(hit.get("cached_ts", 0.0))) <= 1800.0:
        px = float(hit.get("price") or 0.0)
        return px if px > 0 else None

    ms = int(max(0.0, ts) * 1000)
    # Try a tight 1m window first, then a wider 5m fallback if Binance is jittery.
    attempts = [
        {"interval": "1m", "window_ms": 180000, "limit": 7, "timeout": 3.0},
        {"interval": "5m", "window_ms": 1800000, "limit": 12, "timeout": 3.5},
    ]
    for a in attempts:
        start_ms = max(0, ms - int(a["window_ms"]))
        end_ms = ms + int(a["window_ms"])
        try:
            r = httpx.get(
                "https://api.binance.com/api/v3/klines",
                params={
                    "symbol": "BTCUSDT",
                    "interval": str(a["interval"]),
                    "startTime": start_ms,
                    "endTime": end_ms,
                    "limit": int(a["limit"]),
                },
                timeout=float(a["timeout"]),
            )
            if r.status_code != 200:
                continue
            rows = r.json() or []
            best_px = None
            best_dt = 1e18
            for row in rows:
                if not isinstance(row, list) or len(row) < 2:
                    continue
                k_open_ms = int(row[0])
                dt = abs((k_open_ms / 1000.0) - ts)
                if dt < best_dt:
                    best_dt = dt
                    best_px = float(row[1])
            if best_px is None or best_dt > max_age_s:
                continue
            _BINANCE_OPEN_PRICE_CACHE[key] = {"price": float(best_px), "cached_ts": now_ts}
            return float(best_px)
        except Exception:
            continue
    return None


def _fetch_alt_price(source: str) -> Optional[float]:
    try:
        if source == "binance":
            r = httpx.get("https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT", timeout=1.5)
            if r.status_code == 200:
                return float((r.json() or {}).get("price"))
        elif source == "coinbase":
            r = httpx.get("https://api.exchange.coinbase.com/products/BTC-USD/ticker", timeout=1.5)
            if r.status_code == 200:
                return float(r.json().get("price"))
        elif source == "kraken":
            r = httpx.get("https://api.kraken.com/0/public/Ticker?pair=XBTUSD", timeout=1.5)
            if r.status_code == 200:
                obj = r.json().get("result", {})
                if isinstance(obj, dict) and obj:
                    k = next(iter(obj.keys()))
                    return float(obj[k]["c"][0])
        elif source == "bybit":
            r = httpx.get("https://api.bybit.com/v5/market/tickers?category=spot&symbol=BTCUSDT", timeout=1.5)
            if r.status_code == 200:
                lst = (((r.json() or {}).get("result") or {}).get("list") or [])
                if lst:
                    return float(lst[0].get("lastPrice"))
    except Exception:
        return None
    return None


def _push_src_hist(source: str, price: Optional[float]):
    if price is None or price <= 0:
        return
    now = datetime.now(timezone.utc).timestamp()
    arr = _PRICE_SRC_HIST.setdefault(source, [])
    arr.append({"t": now, "p": float(price)})
    keep = now - 120.0
    while arr and float(arr[0].get("t", 0.0)) < keep:
        arr.pop(0)


def _impulse_signal(source: str) -> dict:
    arr = _PRICE_SRC_HIST.get(source) or []
    if len(arr) < 8:
        return {"side": None, "bps_3s": 0.0, "bps_8s": 0.0, "source": source}
    now = arr[-1]
    p_now = float(now.get("p", 0.0))
    t_now = float(now.get("t", 0.0))
    if p_now <= 0:
        return {"side": None, "bps_3s": 0.0, "bps_8s": 0.0, "source": source}

    p3 = None
    p8 = None
    for x in reversed(arr):
        dt = t_now - float(x.get("t", 0.0))
        p = float(x.get("p", 0.0))
        if p <= 0:
            continue
        if p3 is None and dt >= 3.0:
            p3 = p
        if p8 is None and dt >= 8.0:
            p8 = p
            break
    if p3 is None or p8 is None:
        return {"side": None, "bps_3s": 0.0, "bps_8s": 0.0, "source": source}

    bps_3s = ((p_now - p3) / p3) * 10000.0
    bps_8s = ((p_now - p8) / p8) * 10000.0
    side = None
    if bps_3s >= 7.0 and bps_8s >= 10.0:
        side = "BUY_YES"
    elif bps_3s <= -7.0 and bps_8s <= -10.0:
        side = "BUY_NO"
    return {"side": side, "bps_3s": bps_3s, "bps_8s": bps_8s, "source": source}


def _compute_btc_signal() -> dict:
    if len(_BTC_SIGNAL_HISTORY) < 5:
        return {"p_up": 0.5, "lead_bps": 0.0, "rf": 0.0, "rs": 0.0, "sigma": 0.0001, "rsi_n": 0.0}
    now = _BTC_SIGNAL_HISTORY[-1]["t"]
    p_now = float(_BTC_SIGNAL_HISTORY[-1]["p"])
    p20 = _price_ago(20) or p_now
    p120 = _price_ago(120) or p_now
    rf = 0.0 if p20 <= 0 else math.log(p_now / p20)
    rs = 0.0 if p120 <= 0 else math.log(p_now / p120)

    rsi_window = [x for x in _BTC_SIGNAL_HISTORY if (now - x["t"]) <= 30]
    up = 0.0
    down = 0.0
    for i in range(1, len(rsi_window)):
        d = float(rsi_window[i]["p"]) - float(rsi_window[i - 1]["p"])
        if d > 0:
            up += d
        else:
            down += -d
    rsi = 50.0 if (up + down) <= 0 else (100.0 * up / (up + down))
    rsi_n = (rsi - 50.0) / 50.0

    vol_window = [x for x in _BTC_SIGNAL_HISTORY if (now - x["t"]) <= 60]
    rets = []
    for i in range(1, len(vol_window)):
        a = float(vol_window[i - 1]["p"])
        b = float(vol_window[i]["p"])
        if a > 0 and b > 0:
            rets.append(math.log(b / a))
    mean = (sum(rets) / len(rets)) if rets else 0.0
    sigma = math.sqrt(sum((x - mean) ** 2 for x in rets) / len(rets)) if rets else 0.0001

    last = _BTC_SIGNAL_HISTORY[-1]
    cl = last.get("cl")
    bi = last.get("bi")
    lead = ((float(bi) - float(cl)) / float(cl)) if (cl and bi and float(cl) > 0) else 0.0
    lead_bps = lead * 10000.0

    S = 1.8 * rf + 1.2 * rs + 0.6 * rsi_n + 0.8 * lead
    denom = 2.5 * max(sigma, 0.00008)
    z = max(-8.0, min(8.0, S / max(denom, 1e-6)))
    p_up = 1.0 / (1.0 + math.exp(-z))
    return {"p_up": p_up, "lead_bps": lead_bps, "rf": rf, "rs": rs, "sigma": sigma, "rsi_n": rsi_n}


def _compute_binance_ta_signal() -> dict:
    pts = []
    for x in _BTC_SIGNAL_HISTORY:
        bi = x.get("bi")
        if bi is None:
            bi = x.get("p")  # fallback when direct Binance tick is temporarily missing
        if bi is not None:
            try:
                v = float(bi)
                if v > 0:
                    pts.append({"t": float(x.get("t") or 0.0), "p": v})
            except Exception:
                continue
    if len(pts) < 25:
        return {"p_up": 0.5, "confidence": 0, "trend": 0.0, "mom": 0.0, "rsi_n": 0.0, "vol_z": 0.0}

    prices = [x["p"] for x in pts]
    now = pts[-1]["t"]

    def _ema(vals, n):
        a = 2.0 / (n + 1.0)
        e = float(vals[0])
        for v in vals[1:]:
            e = a * float(v) + (1.0 - a) * e
        return e

    ema_fast = _ema(prices[-12:], min(12, len(prices[-12:])))
    ema_slow = _ema(prices[-26:], min(26, len(prices[-26:])))
    trend = 0.0 if ema_slow <= 0 else (ema_fast - ema_slow) / ema_slow

    p20 = prices[-1]
    for x in reversed(pts):
        if (now - x["t"]) >= 20.0:
            p20 = x["p"]
            break
    p60 = prices[-1]
    for x in reversed(pts):
        if (now - x["t"]) >= 60.0:
            p60 = x["p"]
            break
    mom = 0.0 if p20 <= 0 else math.log(prices[-1] / p20)
    mom_slow = 0.0 if p60 <= 0 else math.log(prices[-1] / p60)

    up = 0.0
    down = 0.0
    look = prices[-20:]
    for i in range(1, len(look)):
        d = look[i] - look[i - 1]
        if d > 0:
            up += d
        else:
            down += -d
    rsi = 50.0 if (up + down) <= 0 else (100.0 * up / (up + down))
    rsi_n = (rsi - 50.0) / 50.0

    rets = []
    for i in range(1, len(prices[-40:])):
        a = float(prices[-40:][i - 1]); b = float(prices[-40:][i])
        if a > 0 and b > 0:
            rets.append(math.log(b / a))
    sig = math.sqrt(sum(r*r for r in rets) / len(rets)) if rets else 0.0001
    vol_ref = 0.00025
    vol_z = max(-2.0, min(2.0, (sig - vol_ref) / max(vol_ref, 1e-6)))

    score = 2.2 * trend + 1.6 * mom + 0.8 * mom_slow + 0.9 * rsi_n - 0.45 * max(0.0, vol_z)
    z = max(-8.0, min(8.0, score / max(0.0012, 4.0 * sig)))
    p_up = 1.0 / (1.0 + math.exp(-z))
    confidence = int(max(1, min(99, round(abs((p_up - 0.5) * 2.0) * 100))))
    return {
        "p_up": p_up,
        "confidence": confidence,
        "trend": trend,
        "mom": mom,
        "mom_slow": mom_slow,
        "rsi_n": rsi_n,
        "vol_z": vol_z,
        "sigma": sig,
    }


def _compute_binance_ta_signal_from_events(events_path: str) -> dict:
    try:
        p = Path(events_path)
        if not p.exists():
            return _compute_binance_ta_signal()
        with p.open("rb") as f:
            f.seek(0, 2)
            n = f.tell()
            f.seek(max(0, n - 220_000))
            blob = f.read().decode("utf-8", "ignore")
        pts = []
        for ln in blob.splitlines():
            if '"type": "btc_price_tick"' not in ln:
                continue
            try:
                o = json.loads(ln)
            except Exception:
                continue
            bi = o.get("binance")
            ts = o.get("ts")
            try:
                bv = float(bi)
                tv = float(ts)
            except Exception:
                continue
            if bv > 0:
                pts.append({"t": tv, "p": bv})
        if len(pts) < 25:
            return _compute_binance_ta_signal()

        # Temporarily evaluate TA on extracted sequence.
        bak = list(_BTC_SIGNAL_HISTORY)
        try:
            _BTC_SIGNAL_HISTORY.clear()
            for x in pts[-400:]:
                _BTC_SIGNAL_HISTORY.append({"t": x["t"], "p": x["p"], "bi": x["p"], "cl": None})
            return _compute_binance_ta_signal()
        finally:
            _BTC_SIGNAL_HISTORY.clear()
            _BTC_SIGNAL_HISTORY.extend(bak)
    except Exception:
        return _compute_binance_ta_signal()



def _bids_from_p(row: dict, p_up: float, margin: float = 0.006) -> tuple[Optional[float], Optional[float]]:
    ay = float(row.get("best_ask_yes") or 0.0)
    an = float(row.get("best_ask_no") or 0.0)
    by = float(row.get("best_bid_yes") or 0.0)
    bn = float(row.get("best_bid_no") or 0.0)
    if ay <= 0 or an <= 0:
        return None, None
    jy = p_up - margin
    jn = (1.0 - p_up) - margin
    if by > 0:
        jy = max(by, jy)
    if bn > 0:
        jn = max(bn, jn)
    jy = min(ay, max(0.0, jy))
    jn = min(an, max(0.0, jn))
    return round(jy, 4), round(jn, 4)


def _model_weight(name: str) -> float:
    s = _MODEL_STATS.get(name, {"trades": 0, "wins": 0, "pnl": 0.0})
    t = float(s.get("trades", 0) or 0)
    w = float(s.get("wins", 0) or 0)
    pnl = float(s.get("pnl", 0.0) or 0.0)
    winrate = (w + 1.0) / (t + 2.0)
    pnl_adj = math.tanh(pnl / 200.0) * 0.15
    return max(0.7, min(1.3, 0.8 + 0.4 * winrate + pnl_adj))


def _mc_target_probs(current: float, target: float, t_left_s: float, drift_per_s: float, sigma_per_s: float, paths: int = 800) -> tuple[float, float]:
    if current <= 0 or target <= 0:
        return 0.5, 0.5
    dt = 1.0
    n = max(1, int(min(900, t_left_s)))
    hit = 0
    close_above = 0
    mu = float(drift_per_s)
    sig = max(1e-8, float(sigma_per_s))
    sq = math.sqrt(dt)
    for _ in range(paths):
        p = current
        touched = False
        for _i in range(n):
            z = random.gauss(0.0, 1.0)
            p = p * math.exp((mu - 0.5 * sig * sig) * dt + sig * sq * z)
            if (p >= target):
                touched = True
        if touched:
            hit += 1
        if p >= target:
            close_above += 1
    return close_above / paths, hit / paths


def _history_push(hist: dict, key: str, val, maxlen: int = 12):
    arr = hist.get(key)
    if arr is None:
        arr = []
        hist[key] = arr
    arr.append(val)
    if len(arr) > maxlen:
        del arr[: len(arr) - maxlen]


def _model_compare(row: dict, signal: dict) -> dict:
    p_ta = max(0.02, min(0.98, float(signal.get("p_up", 0.5))))
    p_ll = max(0.02, min(0.98, 0.5 + 0.18 * max(-1.5, min(1.5, float(signal.get("lead_bps", 0.0)) / 35.0))))
    trend = abs(float(signal.get("rf", 0.0))) + abs(float(signal.get("rs", 0.0)))
    chop = float(signal.get("sigma", 0.0))
    w_trend = max(0.1, min(0.9, trend / max(trend + chop, 1e-6)))
    p_mr = max(0.02, min(0.98, 0.5 - 0.35 * float(signal.get("rsi_n", 0.0))))
    p_rg = max(0.02, min(0.98, w_trend * p_ta + (1 - w_trend) * p_mr))

    # Target/time anchor: probability of finishing above target increases with
    # (current-target) and decreases with less time + higher uncertainty.
    target = float(row.get("btc_target") or 0.0)
    current = float(row.get("btc_current") or row.get("btc_current_binance") or 0.0)
    end_ts = float(row.get("end_ts") or 0.0)
    now_ts = datetime.now(timezone.utc).timestamp()
    t_left = max(1.0, end_ts - now_ts) if end_ts > 0 else 900.0
    # Convert short-horizon return sigma to price sigma envelope.
    sigma_ret = max(float(signal.get("sigma", 0.00012)), 0.00005)
    sigma_price = max(current * sigma_ret * math.sqrt(max(5.0, t_left)), 8.0) if current > 0 else 50.0
    if target > 0 and current > 0:
        z_anchor = max(-8.0, min(8.0, (current - target) / sigma_price))
        p_anchor = 1.0 / (1.0 + math.exp(-z_anchor))
    else:
        p_anchor = 0.5

    # Monte-Carlo short-horizon probabilities.
    drift_per_s = float(signal.get("rf", 0.0)) / 20.0
    sigma_per_s = max(1e-6, float(signal.get("sigma", 0.00012)))
    p_close_mc, p_hit_mc = _mc_target_probs(current, target, t_left, drift_per_s, sigma_per_s, paths=700)

    ay = float(row.get("best_ask_yes") or 0.0)
    an = float(row.get("best_ask_no") or 0.0)
    by = float(row.get("best_bid_yes") or 0.0)
    bn = float(row.get("best_bid_no") or 0.0)
    sy = max(0.0, ay - by) if ay > 0 else 0.01
    sn = max(0.0, an - bn) if an > 0 else 0.01
    p_bk = max(0.02, min(0.98, 0.5 + 0.12 * (sn - sy)))

    ta = _bids_from_p(row, p_ta, 0.006)
    ll = _bids_from_p(row, p_ll, 0.006)
    rg = _bids_from_p(row, p_rg, 0.0065)
    bk = _bids_from_p(row, p_bk, 0.006)

    models = {"TA": ta, "LL": ll, "RG": rg, "BK": bk}
    probs = {
        "TA": p_ta,
        "LL": p_ll,
        "RG": p_rg,
        "BK": p_bk,
        "ANCHOR": p_anchor,
        "MC_CLOSE": p_close_mc,
    }
    weights = {k: _model_weight(k) for k in ["TA", "LL", "RG", "BK"]}
    # Anchor/MC get stronger near expiry.
    w_anchor = max(0.7, min(2.2, 1.9 - min(t_left, 900.0) / 900.0))
    w_mc = max(0.8, min(2.4, 2.0 - min(t_left, 900.0) / 900.0))
    weights["ANCHOR"] = w_anchor
    weights["MC_CLOSE"] = w_mc
    wsum = sum(weights.values()) or 1.0
    p_yes_ens = sum(probs[k] * weights[k] for k in probs.keys()) / wsum

    scored = []
    for name, p in probs.items():
        direction = "BUY_YES" if p >= 0.5 else "BUY_NO"
        strength = abs((2.0 * p) - 1.0)
        wgt = weights.get(name, 1.0)
        scored.append((strength * wgt, name, direction, strength))
    if not scored:
        return {"models": models, "probs": probs, "p_yes_ens": p_yes_ens, "p_hit_target": p_hit_mc, "best": "-", "side": None, "confidence": 0, "weights": {k: round(v, 3) for k, v in weights.items()}}
    scored.sort(reverse=True)
    _, best_name, side, base_strength = scored[0]
    agree = sum(1 for _, _, d, _ in scored if d == side) / len(scored)
    confidence = int(max(1, min(99, round((0.6 * base_strength + 0.4 * agree) * 100))))
    consensus = int(sum(1 for _, _, d, _ in scored if d == side))
    return {
        "models": models,
        "probs": probs,
        "p_yes_ens": p_yes_ens,
        "p_hit_target": p_hit_mc,
        "best": best_name,
        "side": side,
        "confidence": confidence,
        "consensus": consensus,
        "weights": {k: round(v, 3) for k, v in weights.items()},
    }


def _polymarket_btc_prices(event_start_iso: str, end_iso: str, variant: str = "fifteen") -> tuple[Optional[float], Optional[float]]:
    if not event_start_iso:
        return None, None
    key = f"{event_start_iso}|{end_iso}|{variant}"
    now = datetime.now(timezone.utc).timestamp()
    cached = _BTC_PRICE_CACHE.get(key)
    if cached:
        open_px, current_px, ts = cached
        age = now - float(ts or 0.0)
        ttl = _BTC_PRICE_CACHE_TTL_OK if (open_px is not None) else _BTC_PRICE_CACHE_TTL_MISS
        # Force at least one refresh per minute for rolling BTC windows.
        if age <= min(ttl, _BTC_PRICE_FORCE_REFRESH_SECONDS):
            return open_px, current_px

    open_px = None
    current_px = None
    try:
        r = httpx.get(
            "https://polymarket.com/api/crypto/crypto-price",
            params={
                "symbol": "BTC",
                "eventStartTime": event_start_iso,
                "endDate": end_iso,
                "variant": variant,
            },
            timeout=6.0,
        )
        if r.status_code == 200:
            obj = r.json()
            if isinstance(obj, dict):
                if obj.get("openPrice") is not None:
                    open_px = float(obj.get("openPrice"))
                # closePrice is current while market active, final close after completion.
                if obj.get("closePrice") is not None:
                    current_px = float(obj.get("closePrice"))
    except Exception:
        pass

    _BTC_PRICE_CACHE[key] = (open_px, current_px, now)
    return open_px, current_px


def _topic_bucket(question: str, slug: str) -> str:
    q = (question or "").lower()
    s = (slug or "").lower()
    hay = q + " " + s
    if "super bowl" in hay:
        return "super_bowl"
    if "nba" in hay:
        return "nba"
    if "nfl" in hay:
        return "nfl"
    if "election" in hay or "president" in hay:
        return "politics"
    if "fed" in hay or "cpi" in hay or "rate" in hay:
        return "macro"
    return "other"


def _infer_btc_target_from_text(question: str) -> Optional[float]:
    q = str(question or "")
    if not q:
        return None
    # Common forms: "$96,500", "96500", "96.5k".
    matches = re.findall(r"\$?\s*(\d{2,3}(?:,\d{3})+|\d{4,6}(?:\.\d+)?)\s*([kK])?", q)
    if not matches:
        return None
    vals = []
    for raw_s, k in matches:
        raw = str(raw_s or "").replace(",", "")
        if not raw:
            continue
        try:
            v = float(raw)
        except Exception:
            continue
        if k:
            v *= 1000.0
        if v >= 1000.0:
            vals.append(v)
    if not vals:
        return None
    return round(max(vals), 2)


def run_once(cfg: dict):
    global _GLOBAL_OPEN_PAUSE_UNTIL, _RECENT_FLIP_STOP_LOSS_TS, _LAST_OPEN_TS
    _ensure_btc_live_feed(cfg["storage"]["events_path"])

    clob = ClobAdapter(cfg["data"]["clob_rest_base"])
    gamma = GammaAdapter(cfg["data"]["gamma_base"])
    clob.reset_call_count()
    gamma.reset_call_count()
    state = load_state(cfg["storage"]["state_path"], float(cfg["paper"]["starting_cash_usd"]))
    strategy_cfg = cfg.get("strategy", {})

    snapshots = []
    try:
        refs = gamma.fetch_active_market_refs(
            limit=int(cfg["data"].get("max_markets", 10)),
            focus_keywords=cfg["data"].get("focus_keywords", []),
        )
        slug_refs = gamma.fetch_market_refs_by_slugs(cfg["data"].get("focus_slugs", []))
        prefixes = cfg["data"].get("focus_slug_prefixes", [])
        prefix_refs = gamma.fetch_market_refs_by_slug_prefixes(
            prefixes,
            limit=max(200, int(cfg["data"].get("max_markets", 10)) * 10),
            active_only=True,
        )
        # Slightly wider rolling window to reduce discovery gaps around 15m rollovers.
        generated_refs_15m = gamma.fetch_market_refs_by_generated_15m_slugs(prefixes, windows=16)
        generated_refs_5m = gamma.fetch_market_refs_by_generated_timeframe_slugs(
            prefixes,
            timeframe="5m",
            bucket_seconds=300,
            windows=24,
            lookback_windows=24,
        )
        generated_refs = generated_refs_15m + generated_refs_5m
        by_id = {r.market_id: r for r in refs}
        for r in slug_refs + prefix_refs + generated_refs:
            by_id[r.market_id] = r
        refs = list(by_id.values())

        # Rescue path: if focused discovery returns nothing, retry broad active markets
        # then keep only BTC/15m-ish names to avoid dead loop behavior.
        if not refs:
            broad = gamma.fetch_active_market_refs(
                limit=max(300, int(cfg["data"].get("max_markets", 30)) * 20),
                focus_keywords=[],
            )
            hints = ["btc", "bitcoin", "up or down", "15m", "15 min", "15-minute"]
            refs = [r for r in broad if any(h in (r.question or "").lower() for h in hints)][:20]

        btc_refs = refs[:]

        # Build second monitor group: non-BTC markets resolving within configurable horizon
        # and ranked by paired-leg arb proximity (YES ask + NO ask).
        global _ALT_REFS_CACHE, _ALT_REFS_TS
        now_ts = datetime.now(timezone.utc).timestamp()
        refresh_secs = int(cfg.get("data", {}).get("alt_group_refresh_seconds", 300))
        alt_target_n = int(cfg.get("data", {}).get("alt_group_size", 10))
        alt_horizon_days = int(cfg.get("data", {}).get("alt_group_horizon_days", 30))
        if (now_ts - _ALT_REFS_TS) > refresh_secs or not _ALT_REFS_CACHE:
            broad = gamma.fetch_active_market_refs(limit=700, focus_keywords=[])
            horizon = datetime.now(timezone.utc) + timedelta(days=alt_horizon_days)
            cands = []
            for r in broad:
                if _is_btc_ref(r):
                    continue
                dt = _parse_dt(getattr(r, "end_date", ""))
                if not dt:
                    continue
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt <= datetime.now(timezone.utc) or dt > horizon:
                    continue
                cands.append(r)
            cands.sort(key=lambda x: float(getattr(x, "liquidity_num", 0.0)), reverse=True)
            _ALT_REFS_CACHE = cands[: max(alt_target_n * 4, 30)]
            _ALT_REFS_TS = now_ts

        alt_refs = _ALT_REFS_CACHE[: max(alt_target_n * 3, 20)]

        # If no active focused markets, fall back to latest-by-prefix discovery
        # so rolling series (e.g., btc-updown-15m-*) stays visible.
        if not refs and prefixes:
            fallback_refs = gamma.fetch_market_refs_by_slug_prefixes(
                prefixes,
                limit=500,
                active_only=False,
            )
            refs = fallback_refs[:3]
            append_event(cfg["storage"]["events_path"], {
                "type": "focus_fallback",
                "reason": "no_active_focus_markets",
                "selected_market_ids": [r.market_id for r in refs],
            })

        # Combine BTC group + secondary under-7d group, dedup by market_id.
        by_mid = {r.market_id: r for r in (btc_refs + alt_refs)}
        refs = list(by_mid.values())

        use_ws = bool(cfg.get("data", {}).get("use_clob_ws", True))
        ws_hook = None
        if use_ws:
            ws_hook = _ensure_ws_hook()
            ws_hook.subscribe_assets([r.yes_token for r in refs] + [r.no_token for r in refs])
            ws_hook.set_token_meta([
                {
                    "market_id": r.market_id,
                    "market_name": r.question,
                    "yes_token": r.yes_token,
                    "no_token": r.no_token,
                }
                for r in refs
            ])
            def _on_ws_tick(tick: dict):
                append_event(cfg["storage"]["events_path"], {"type": "ws_market_tick", **tick})
                s = tick.get("ask_sum_no_fees")
                try:
                    if s is not None and float(s) <= 1.0:
                        append_event(cfg["storage"]["events_path"], {
                            "type": "ws_opportunity_seen",
                            "count": 1,
                            "items": [
                                {
                                    "market_id": tick.get("market_id"),
                                    "market_name": tick.get("market_name"),
                                    "best_ask_yes": tick.get("best_ask_yes"),
                                    "best_ask_no": tick.get("best_ask_no"),
                                    "ask_sum_no_fees": float(s),
                                }
                            ],
                        })
                except Exception:
                    pass

            ws_hook.set_on_tick(_on_ws_tick)

        snapshots = clob.fetch_snapshots_from_refs(refs)

        # WS override: replace best bid/ask with freshest websocket values when present.
        if use_ws and ws_hook:
            ws_updates = 0
            for s in snapshots:
                yb, ya = ws_hook.get_best(s.token_id)
                if yb is not None and yb > 0:
                    s.yes_bid = yb
                    ws_updates += 1
                if ya is not None and ya > 0:
                    s.yes_ask = ya
                    ws_updates += 1
            # no token id for NO side in snapshot model; infer from refs map
            no_token_by_market = {r.market_id: r.no_token for r in refs}
            for s in snapshots:
                nt = no_token_by_market.get(s.market_id)
                if not nt:
                    continue
                nb, na = ws_hook.get_best(nt)
                if nb is not None and nb > 0:
                    s.no_bid = nb
                    ws_updates += 1
                if na is not None and na > 0:
                    s.no_ask = na
                    ws_updates += 1

            append_event(cfg["storage"]["events_path"], {
                "type": "ws_usage",
                "enabled": True,
                "updates_applied": ws_updates,
                **ws_hook.stats(),
            })
    except Exception as e:
        append_event(cfg["storage"]["events_path"], {"type": "adapter_error", "source": "gamma_clob", "error": str(e)})

    if not snapshots:
        if cfg["data"].get("focus_keywords"):
            append_event(cfg["storage"]["events_path"], {
                "type": "market_scan_empty",
                "reason": "no_markets_for_focus_keywords",
                "focus_keywords": cfg["data"].get("focus_keywords", []),
            })
            print("[yellow]No focused live markets found; skipping cycle.[/yellow]")
            return
        snapshots = clob.fetch_snapshots()  # demo fallback only when no focus filter

    ranked = rank_candidates(snapshots, cfg)
    ops = score_opportunities(snapshots, cfg)

    fee_bps = float(cfg["scoring"]["fee_bps"])
    slippage_bps = float(cfg["scoring"]["slippage_bps"])

    # Ops Co-Founder outputs (v1)
    market_radar = build_market_radar(snapshots, limit=8)
    ineff = build_inefficiency_report(
        snapshots,
        fee_bps=fee_bps,
        slippage_bps=slippage_bps,
        target_size_usd=float(cfg["scoring"].get("target_size_usd", 20.0)),
        limit=8,
    )
    flow_watch = build_flow_watch(snapshots, limit=8)
    append_event(cfg["storage"]["events_path"], {"type": "market_radar", "count": len(market_radar), "top": market_radar})
    append_event(cfg["storage"]["events_path"], {"type": "inefficiency_report", "count": len(ineff), "top": ineff})
    append_event(cfg["storage"]["events_path"], {"type": "flow_watch", "count": len(flow_watch), "top": flow_watch})

    snap_by_market = {s.market_id: s for s in snapshots}
    question_by_market = {s.market_id: s.question for s in snapshots}

    top_payload = []
    min_edge = float(cfg["scoring"]["min_edge_bps"])
    max_exec_sum = float(cfg.get("execution", {}).get("max_exec_sum", 1.05))

    for c in ranked[:10]:
        s = snap_by_market.get(c.market_id)
        yes_no_sum = None
        hint_sum = None
        exec_edge_bps = None
        theo_edge_bps = None
        best_ask_yes = None
        best_ask_no = None
        ask_sum_no_fees = None
        ask_sum_with_fees = None
        arb_under_1_no_fees = None
        arb_under_1_with_fees = None

        if s:
            yb, nb = depth_aware_buy_prices(s, target_size_usd=float(cfg["scoring"].get("target_size_usd", 20.0)))
            yes_no_sum = yb + nb
            exec_edge_bps = (1.0 - yes_no_sum) * 10000 - fee_bps - slippage_bps

            best_ask_yes = s.yes_ask
            best_ask_no = s.no_ask
            ask_sum_no_fees = best_ask_yes + best_ask_no
            ask_sum_with_fees = ask_sum_no_fees + ((fee_bps + slippage_bps) / 10000.0)
            arb_under_1_no_fees = ask_sum_no_fees < 1.0
            arb_under_1_with_fees = ask_sum_with_fees < 1.0

            if s.yes_hint > 0 and s.no_hint > 0:
                hint_sum = s.yes_hint + s.no_hint
                theo_edge_bps = (1.0 - hint_sum) * 10000 - fee_bps - slippage_bps

        signal = "OPPORTUNITY" if arb_under_1_with_fees else ("WATCH" if arb_under_1_no_fees else "NO_OPPORTUNITY")
        top_payload.append({
            "market_id": c.market_id,
            "market_name": question_by_market.get(c.market_id, ""),
            "side": c.side,
            "edge_bps": c.edge_bps,
            "price": c.expected_price,
            "best_ask_yes": round(best_ask_yes, 4) if best_ask_yes is not None else None,
            "best_ask_no": round(best_ask_no, 4) if best_ask_no is not None else None,
            "ask_sum_no_fees": round(ask_sum_no_fees, 4) if ask_sum_no_fees is not None else None,
            "ask_sum_with_fees": round(ask_sum_with_fees, 4) if ask_sum_with_fees is not None else None,
            "arb_under_1_no_fees": arb_under_1_no_fees,
            "arb_under_1_with_fees": arb_under_1_with_fees,
            "yes_no_exec_sum": round(yes_no_sum, 4) if yes_no_sum is not None else None,
            "yes_no_hint_sum": round(hint_sum, 4) if hint_sum is not None else None,
            "exec_edge_bps": round(exec_edge_bps, 2) if exec_edge_bps is not None else None,
            "theo_edge_bps": round(theo_edge_bps, 2) if theo_edge_bps is not None else None,
            "signal": signal,
        })

    # Build grouped monitor payloads: BTC first, then secondary (<7d non-BTC).
    btc_ids = {r.market_id for r in btc_refs}
    alt_ids = {r.market_id for r in alt_refs if r.market_id not in btc_ids}

    row_by_market = {}
    for s in snapshots:
        ask_sum_no_fees = s.yes_ask + s.no_ask
        ask_sum_with_fees = ask_sum_no_fees + ((fee_bps + slippage_bps) / 10000.0)
        if ask_sum_with_fees < 1.0:
            signal = "OPPORTUNITY"
        elif ask_sum_no_fees < 1.0:
            signal = "WATCH"
        else:
            signal = "NO_OPPORTUNITY"
        spread_penalty = (s.yes_ask - s.yes_bid) + (s.no_ask - s.no_bid)
        quality_score = (float(s.depth_usd) + 1.0) / max(spread_penalty + 0.01, 0.01)
        row_by_market[s.market_id] = {
            "market_id": s.market_id,
            "market_name": s.question,
            "best_bid_yes": round(s.yes_bid, 4),
            "best_bid_no": round(s.no_bid, 4),
            "best_ask_yes": round(s.yes_ask, 4),
            "best_ask_no": round(s.no_ask, 4),
            "ask_sum_no_fees": round(ask_sum_no_fees, 4),
            "ask_sum_with_fees": round(ask_sum_with_fees, 4),
            "signal": signal,
            "depth_usd": round(float(s.depth_usd), 2),
            "spread_sum": round(spread_penalty, 4),
            "quality_score": round(quality_score, 2),
        }

    # 5m rescue: if REST snapshot misses a 5m BTC market, synthesize row from WS best asks.
    # This keeps latest 5m visible in primary group instead of disappearing.
    if use_ws and ws_hook:
        for rr in btc_refs:
            mid = str(rr.market_id)
            if mid in row_by_market:
                continue
            slug_l = str(getattr(rr, "slug", "") or "").lower()
            if "5m" not in slug_l:
                continue
            yb, ya = ws_hook.get_best(rr.yes_token)
            nb, na = ws_hook.get_best(rr.no_token)
            if not (ya and na and ya > 0 and na > 0):
                continue
            by = float(yb or 0.0)
            bn = float(nb or 0.0)
            ask_sum_no_fees = float(ya) + float(na)
            ask_sum_with_fees = ask_sum_no_fees + ((fee_bps + slippage_bps) / 10000.0)
            signal = "OPPORTUNITY" if ask_sum_with_fees < 1.0 else ("WATCH" if ask_sum_no_fees < 1.0 else "NO_OPPORTUNITY")
            spread_penalty = max(0.0, float(ya) - by) + max(0.0, float(na) - bn)
            quality_score = 0.0 if spread_penalty <= 0 else round(1.0 / spread_penalty, 2)
            row_by_market[mid] = {
                "market_id": mid,
                "market_name": str(getattr(rr, "question", "") or mid),
                "best_bid_yes": round(by, 4),
                "best_bid_no": round(bn, 4),
                "best_ask_yes": round(float(ya), 4),
                "best_ask_no": round(float(na), 4),
                "ask_sum_no_fees": round(ask_sum_no_fees, 4),
                "ask_sum_with_fees": round(ask_sum_with_fees, 4),
                "signal": signal,
                "depth_usd": 0.0,
                "spread_sum": round(spread_penalty, 4),
                "quality_score": quality_score,
            }

    # BTC group policy: always show the next 3 resolving BTC markets.
    now_dt = datetime.now(timezone.utc)
    btc_ref_by_id = {r.market_id: r for r in btc_refs}
    token_ids_by_market = {r.market_id: {"BUY_YES": r.yes_token, "BUY_NO": r.no_token} for r in btc_refs}
    btc_candidates = []
    for mid in btc_ids:
        if mid not in row_by_market:
            continue
        r = btc_ref_by_id.get(mid)
        dt = _parse_dt(getattr(r, "end_date", "") if r else "")
        if dt is None:
            continue
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        if dt < now_dt:
            continue
        btc_candidates.append((dt, row_by_market[mid]))

    btc_candidates.sort(key=lambda x: x[0])

    def _tf_bucket(rr: dict) -> str:
        mid = str(rr.get("market_id") or "")
        ref = btc_ref_by_id.get(mid)
        slug = str((rr.get("slug") if rr.get("slug") else (getattr(ref, "slug", ""))) or "").lower()
        q = str((rr.get("question") if rr.get("question") else (getattr(ref, "question", ""))) or "").lower()
        if "5m" in slug or "5 minute" in q or "5-minute" in q:
            return "5m"
        if "15m" in slug or "15 min" in q or "15-minute" in q:
            return "15m"
        return "other"

    btc_rows = []
    # Primary monitor set: latest 3x 15m + latest 1x 5m (if available).
    rows_15 = [row for _dt, row in btc_candidates if _tf_bucket(row) == "15m"]
    rows_5 = [row for _dt, row in btc_candidates if _tf_bucket(row) == "5m"]
    btc_rows.extend(rows_15[:3])
    if rows_5:
        btc_rows.append(rows_5[0])

    # Fill remaining slots from nearest-expiry BTC rows.
    target_total = 4
    if len(btc_rows) < target_total:
        for _dt, row in btc_candidates:
            if row in btc_rows:
                continue
            btc_rows.append(row)
            if len(btc_rows) >= target_total:
                break

    if len(btc_rows) < target_total:
        fb = [row_by_market[m] for m in btc_ids if m in row_by_market and row_by_market[m] not in btc_rows]
        fb.sort(key=lambda x: (x["ask_sum_with_fees"], -x["depth_usd"]))
        btc_rows.extend(fb[: max(0, target_total - len(btc_rows))])

    # Hard guarantee: always include one active 5m BTC market in primary group if available.
    has_5m = any(_tf_bucket(r) == "5m" for r in btc_rows)
    if not has_5m:
        now_u = datetime.now(timezone.utc)
        five_refs = []
        for rr in btc_refs:
            slug_l = str(getattr(rr, "slug", "") or "").lower()
            if "-5m-" not in slug_l:
                continue
            dt = _parse_dt(getattr(rr, "end_date", ""))
            if not dt:
                continue
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            if dt < now_u:
                continue
            five_refs.append((dt, rr))
        five_refs.sort(key=lambda x: x[0])
        for _dt, rr in five_refs:
            cand = row_by_market.get(str(rr.market_id))
            if cand is not None and cand not in btc_rows:
                btc_rows.append(cand)
                break

    # Keep any currently open markets visible so dashboard can mark-to-market PnL.
    open_market_ids = {str(p.market_id) for p in state.positions if str(getattr(p, "status", "open")) == "open"}
    for omid in open_market_ids:
        rr = row_by_market.get(omid)
        if rr and rr not in btc_rows:
            btc_rows.append(rr)

    # BTC metadata from Polymarket crypto-price endpoint (Chainlink-derived in market UI).
    for r in btc_rows:
        rr = btc_ref_by_id.get(r.get("market_id"))
        src = getattr(rr, "resolution_source", "") if rr else ""
        st = getattr(rr, "event_start_time", "") if rr else ""
        ed = getattr(rr, "end_date", "") if rr else ""
        if not st and ed:
            _ed = _parse_dt(ed)
            if _ed is not None:
                if _ed.tzinfo is None:
                    _ed = _ed.replace(tzinfo=timezone.utc)
                st = (_ed - timedelta(minutes=15)).isoformat()
        tfv = _tf_bucket(r)
        variant = "five" if tfv == "5m" else "fifteen"
        target_px, current_px = _polymarket_btc_prices(st, ed, variant=variant)
        chainlink_live, binance_live = _btc_live_prices()
        if binance_live is None:
            now_imp_ts = datetime.now(timezone.utc).timestamp()
            if now_imp_ts - float(_PRICE_SRC_LAST.get("binance", 0.0)) >= 1.0:
                _PRICE_SRC_LAST["binance"] = now_imp_ts
                bpx = _fetch_alt_price("binance")
                if bpx is not None:
                    binance_live = bpx
        if current_px is None:
            current_px = chainlink_live if chainlink_live is not None else binance_live
        if current_px is None:
            current_px = _latest_signal_price(max_staleness_s=180.0)

        mid = str(r.get("market_id"))
        st_dt = _parse_dt(st) if st else None
        if st_dt is not None and st_dt.tzinfo is None:
            st_dt = st_dt.replace(tzinfo=timezone.utc)

        # Fallbacks for missing target: cached by market_id, or infer from BTC ticks near start.
        if target_px is None and mid in _BTC_TARGET_CACHE:
            target_px = _BTC_TARGET_CACHE[mid]
        if target_px is None and st_dt is not None and datetime.now(timezone.utc) >= st_dt:
            st_ts = st_dt.timestamp()
            inferred = _price_near_ts(st_ts, max_delta_s=1200.0)
            if inferred is not None:
                target_px = inferred
            else:
                inferred_binance = _binance_open_price_near_ts(st_ts, max_age_s=7200.0)
                if inferred_binance is not None:
                    target_px = inferred_binance
                elif current_px is not None:
                    # Fallback when upstream openPrice is unavailable: lock first seen live price after start.
                    target_px = float(current_px)
        if target_px is None:
            # Last-resort parse from market text (e.g. "... above $96,500 ...").
            qtxt = getattr(rr, "question", "") if rr else ""
            target_px = _infer_btc_target_from_text(qtxt)
            if target_px is not None:
                append_event(cfg["storage"]["events_path"], {
                    "type": "btc_target_inferred_from_text",
                    "market_id": r.get("market_id"),
                    "target": round(float(target_px), 2),
                })
        if target_px is not None:
            _BTC_TARGET_CACHE[mid] = float(target_px)

        r["btc_price_source"] = src or "https://data.chain.link/streams/btc-usd"
        r["btc_target"] = round(target_px, 2) if target_px is not None else None
        if target_px is None:
            now_ts = datetime.now(timezone.utc).timestamp()
            prev_ts = float(_BTC_TARGET_MISS_LAST.get(mid) or 0.0)
            # Throttle noisy repeats while keeping visibility for real missing-target episodes.
            if now_ts - prev_ts >= 300.0:
                append_event(cfg["storage"]["events_path"], {
                    "type": "btc_target_missing",
                    "market_id": r.get("market_id"),
                    "event_start_time": st,
                    "end_date": ed,
                })
                _BTC_TARGET_MISS_LAST[mid] = now_ts
        r["btc_current"] = round(current_px, 2) if current_px is not None else None
        r["btc_current_binance"] = round(binance_live, 2) if binance_live is not None else None
        r["btc_target_start"] = st
        dt_end = _parse_dt(ed)
        if dt_end is not None:
            if dt_end.tzinfo is None:
                dt_end = dt_end.replace(tzinfo=timezone.utc)
            r["end_ts"] = dt_end.timestamp()

        _update_btc_signal_history(current_px, binance_live)
        sigm = _compute_btc_signal()
        model_engine = str(strategy_cfg.get("probability_engine", "hybrid")).lower()
        if model_engine == "binance_ta_v1":
            ta_sig = _compute_binance_ta_signal_from_events(cfg["storage"]["events_path"])
            global _LAST_TA_SIG
            if int(ta_sig.get("confidence", 0) or 0) <= 0 and _LAST_TA_SIG:
                ta_sig = dict(_LAST_TA_SIG)
            elif int(ta_sig.get("confidence", 0) or 0) <= 0:
                # Warm fallback: avoid all-zero UI/signals when Binance-only history is temporarily thin.
                p_f = max(0.02, min(0.98, float(sigm.get("p_up", 0.5))))
                ta_sig = {
                    "p_up": p_f,
                    "confidence": int(max(1, min(99, round(abs((p_f - 0.5) * 2.0) * 100)))),
                    "trend": float(sigm.get("rf", 0.0)),
                    "mom": float(sigm.get("rs", 0.0)),
                    "rsi_n": float(sigm.get("rsi_n", 0.0)),
                    "vol_z": 0.0,
                }
            else:
                _LAST_TA_SIG = dict(ta_sig)
            p_yes = max(0.02, min(0.98, float(ta_sig.get("p_up", 0.5))))
            side = "BUY_YES" if p_yes >= 0.5 else "BUY_NO"
            cmp = {
                "models": {"TA": _bids_from_p(r, p_yes, 0.006), "LL": None, "RG": None, "BK": None},
                "probs": {"TA_BINANCE": p_yes},
                "p_yes_ens": p_yes,
                "p_hit_target": p_yes if side == "BUY_YES" else (1.0 - p_yes),
                "best": "TA_BINANCE",
                "side": side,
                "confidence": int(ta_sig.get("confidence", 0)),
                "consensus": 1,
                "weights": {"TA_BINANCE": 1.0},
            }
            r["ta_trend"] = round(float(ta_sig.get("trend", 0.0)), 8)
            r["ta_mom"] = round(float(ta_sig.get("mom", 0.0)), 8)
            r["ta_rsi_n"] = round(float(ta_sig.get("rsi_n", 0.0)), 6)
            r["ta_vol_z"] = round(float(ta_sig.get("vol_z", 0.0)), 4)
        else:
            cmp = _model_compare(r, sigm)
        r["model_ta"] = cmp["models"].get("TA") if isinstance(cmp.get("models"), dict) else None
        r["model_ll"] = cmp["models"].get("LL")
        r["model_rg"] = cmp["models"].get("RG")
        r["model_bk"] = cmp["models"].get("BK")
        r["best_model"] = f"{cmp['best']}:{'UP' if cmp.get('side')=='BUY_YES' else 'DOWN'} {cmp.get('confidence',0)}%" if cmp.get("side") else "-"
        r["model_side"] = cmp.get("side")
        r["model_confidence"] = cmp.get("confidence", 0)
        r["model_consensus"] = cmp.get("consensus", 0)
        p_yes = float(cmp.get("p_yes_ens", 0.5))
        probs = cmp.get("probs") or {}
        r["p_yes_model"] = round(p_yes, 4)
        r["p_hit_target"] = round(float(cmp.get("p_hit_target", 0.5)), 4)
        r["p_anchor"] = round(float(probs.get("ANCHOR", 0.5)), 4)
        if r.get("end_ts"):
            r["t_left_s"] = max(0, int(float(r.get("end_ts")) - datetime.now(timezone.utc).timestamp()))
        r["p_no_model"] = round(1.0 - p_yes, 4)
        r["edge_yes"] = round(p_yes - float(r.get("best_ask_yes") or 0.0), 4)
        r["edge_no"] = round((1.0 - p_yes) - float(r.get("best_ask_no") or 0.0), 4)

    # Keep both 15m and 5m rows visible; some 5m markets may temporarily miss target metadata.
    # (We still compute target fallbacks when possible.)

    # Statistical relative-value scanner: 15m vs 5m implied-prob divergence.
    # Not risk-free arb; this flags unusually large dislocations only.
    strat_cfg_div = cfg.get("strategy", {})
    div_min = float(strat_cfg_div.get("tf_divergence_min", 0.12))
    fee_buffer = float(strat_cfg_div.get("tf_divergence_fee_buffer", 0.02))
    rows_15m = [r for r in btc_rows if _tf_bucket(r) == "15m" and r.get("p_yes_model") is not None]
    rows_5m = [r for r in btc_rows if _tf_bucket(r) == "5m" and r.get("p_yes_model") is not None]
    div_items = []
    for r5 in rows_5m:
        e5 = float(r5.get("end_ts") or 0.0)
        if e5 <= 0:
            continue
        best = None
        best_dt = 1e18
        for r15 in rows_15m:
            e15 = float(r15.get("end_ts") or 0.0)
            if e15 <= 0:
                continue
            dt = abs(e15 - e5)
            if dt < best_dt:
                best_dt = dt
                best = r15
        if not best:
            continue

        p5 = float(r5.get("p_yes_model") or 0.5)
        p15 = float(best.get("p_yes_model") or 0.5)
        div = p5 - p15
        abs_div = abs(div)
        if abs_div < div_min:
            continue

        edge_est = abs_div - fee_buffer
        if edge_est <= 0:
            continue

        # Positive div => 5m implies more YES than 15m context.
        suggestion = "LONG_5M_NO__LONG_15M_YES" if div > 0 else "LONG_5M_YES__LONG_15M_NO"
        div_items.append({
            "market_5m": r5.get("market_id"),
            "market_15m": best.get("market_id"),
            "name_5m": r5.get("market_name"),
            "name_15m": best.get("market_name"),
            "p_yes_5m": round(p5, 4),
            "p_yes_15m": round(p15, 4),
            "divergence": round(div, 4),
            "abs_divergence": round(abs_div, 4),
            "edge_est": round(edge_est, 4),
            "suggestion": suggestion,
            "end_gap_s": int(best_dt),
        })

    div_items.sort(key=lambda x: float(x.get("edge_est") or 0.0), reverse=True)
    append_event(cfg["storage"]["events_path"], {
        "type": "timeframe_divergence",
        "enabled": True,
        "min_divergence": div_min,
        "fee_buffer": fee_buffer,
        "count": len(div_items),
        "top": div_items[:5],
    })

    alt_ref_by_id = {r.market_id: r for r in alt_refs}
    alt_rows = [row_by_market[m] for m in alt_ids if m in row_by_market]

    ws_metrics = {}
    if use_ws:
        try:
            ws_metrics = _ensure_ws_hook().get_market_metrics(window_seconds=int(cfg.get("data", {}).get("alt_vol_window_seconds", 600)))
        except Exception:
            ws_metrics = {}

    min_tick_rate = float(cfg.get("data", {}).get("alt_min_updates_per_min", 3.0))
    vol_weight = float(cfg.get("data", {}).get("alt_vol_weight", 0.60))
    spread_cap = float(cfg.get("data", {}).get("alt_max_spread_sum", 0.12))

    scored = []
    for r in alt_rows:
        mk = str(r.get("market_id", ""))
        m = ws_metrics.get(mk, {})
        updates = float(m.get("updates_per_min", 0.0))
        vol = float(m.get("ask_volatility", 0.0))
        spread = float(r.get("spread_sum", 9.0))
        if updates < min_tick_rate:
            continue
        if spread > spread_cap:
            continue

        # lower is better for arb distance; convert to score.
        arb_dist = abs(float(r.get("ask_sum_no_fees", 9.0)) - 1.0)
        arb_score = max(0.0, 1.0 - min(arb_dist / 0.05, 1.0))
        vol_score = min(vol / 0.05, 1.0)
        activity_score = min(updates / 40.0, 1.0)
        # Volatility-first ranking, with activity second and arb proximity third.
        composite = vol_weight * vol_score + 0.25 * activity_score + max(0.0, 0.75 - vol_weight) * arb_score
        scored.append((composite, updates, -arb_dist, r))

    scored.sort(key=lambda t: (t[0], t[1], t[2]), reverse=True)
    alt_rows = [x[3] for x in scored]

    # Diversity cap so one theme doesn't dominate the panel.
    alt_limit = int(cfg.get("data", {}).get("alt_group_size", 10))
    per_topic_cap = int(cfg.get("data", {}).get("alt_group_topic_cap", 3))
    if alt_limit <= 0:
        alt_rows = []
    else:
        picked = []
        topic_counts = {}
        for r in alt_rows:
            rr = alt_ref_by_id.get(r.get("market_id"))
            topic = _topic_bucket(getattr(rr, "question", ""), getattr(rr, "slug", ""))
            if topic_counts.get(topic, 0) >= per_topic_cap:
                continue
            topic_counts[topic] = topic_counts.get(topic, 0) + 1
            picked.append(r)
            if len(picked) >= alt_limit:
                break
        alt_rows = picked

    alt_enabled = alt_limit > 0
    append_event(cfg["storage"]["events_path"], {
        "type": "market_groups",
        "bitcoin": btc_rows,
        "secondary": alt_rows,
        "secondary_note": (
            f"Non-BTC markets resolving within {alt_horizon_days} days, ranked by paired YES+NO arb proximity (sum toward <1)"
            if alt_enabled else
            "Secondary group disabled (BTC-only focus)"
        ),
        "counts": {"bitcoin": len(btc_rows), "secondary": len(alt_rows)},
    })

    append_event(cfg["storage"]["events_path"], {
        "type": "api_usage",
        "gamma_calls": gamma.call_count,
        "clob_calls": clob.call_count,
        "total_calls": gamma.call_count + clob.call_count,
        "snapshot_count": len(snapshots),
    })

    opportunities_seen = []
    for t in top_payload:
        s0 = t.get("ask_sum_no_fees")
        if s0 is not None and s0 <= 1.0:
            opportunities_seen.append({
                "market_id": t.get("market_id"),
                "market_name": t.get("market_name"),
                "best_ask_yes": t.get("best_ask_yes"),
                "best_ask_no": t.get("best_ask_no"),
                "ask_sum_no_fees": s0,
            })

    append_event(cfg["storage"]["events_path"], {
        "type": "opportunity_seen",
        "count": len(opportunities_seen),
        "items": opportunities_seen,
    })

    append_event(cfg["storage"]["events_path"], {
        "type": "market_scan",
        "snapshot_count": len(snapshots),
        "top_candidates": top_payload,
    })

    print(f"[bold]Snapshots:[/bold] {len(snapshots)} | [bold]Opportunities:[/bold] {len(ops)}")
    for c in ranked[:5]:
        s = snap_by_market.get(c.market_id)
        yes_no = 0.0
        hint_sum = 0.0
        exec_edge = None
        theo_edge = None
        if s:
            yb, nb = depth_aware_buy_prices(s, target_size_usd=float(cfg["scoring"].get("target_size_usd", 20.0)))
            yes_no = yb + nb
            exec_edge = (1.0 - yes_no) * 10000 - fee_bps - slippage_bps
            hint_sum = (s.yes_hint + s.no_hint) if (s.yes_hint > 0 and s.no_hint > 0) else 0.0
            if hint_sum > 0:
                theo_edge = (1.0 - hint_sum) * 10000 - fee_bps - slippage_bps

        best_ask_yes = s.yes_ask if s else 0.0
        best_ask_no = s.no_ask if s else 0.0
        ask_sum_no_fees = best_ask_yes + best_ask_no
        ask_sum_with_fees = ask_sum_no_fees + ((fee_bps + slippage_bps) / 10000.0)
        if ask_sum_with_fees < 1.0:
            signal = "OPPORTUNITY"
        elif ask_sum_no_fees < 1.0:
            signal = "WATCH"
        else:
            signal = "NO_OPPORTUNITY"

        name = question_by_market.get(c.market_id, "")
        short = (name[:56] + "...") if len(name) > 59 else name
        print(
            f"[cyan]CAND[/cyan] {c.market_id} {c.side} askY={best_ask_yes:.3f} askN={best_ask_no:.3f} "
            f"sum={ask_sum_no_fees:.3f} sum_fee={ask_sum_with_fees:.3f} sig={signal} | {short}"
        )

    # Model-driven BTC paper trading simulation / live bridge.
    app_mode = str(cfg.get("app", {}).get("mode", "paper")).lower()
    live_exec = _ensure_live_executor(cfg)
    live_enabled = app_mode == "live" and bool(cfg.get("live", {}).get("enabled", False))
    strategy_cfg = cfg.get("strategy", {})
    trade_cap = float(strategy_cfg.get("trade_cap_usd", 100.0))
    max_trade_cash_fraction = float(strategy_cfg.get("max_trade_cash_fraction", 0.10))
    max_open_positions = int(strategy_cfg.get("max_open_positions", 2))
    base_reentry_cooldown_s = float(strategy_cfg.get("base_reentry_cooldown_s", 120.0))
    flip_reentry_cooldown_s = float(strategy_cfg.get("flip_reentry_cooldown_s", 240.0))
    min_hold_for_flip_exit_s = float(strategy_cfg.get("min_hold_for_flip_exit_s", 20.0))
    min_hold_for_flip_stop_s = float(strategy_cfg.get("min_hold_for_flip_stop_s", 0.0))
    min_time_left_for_entry_s = float(strategy_cfg.get("min_time_left_for_entry_s", 90.0))
    flip_signal_conf_min = int(strategy_cfg.get("flip_signal_conf_min", 62))
    flip_stop_loss_pct = float(strategy_cfg.get("flip_stop_loss_pct", -0.12))
    buy_no_flip_stop_loss_pct = float(strategy_cfg.get("buy_no_flip_stop_loss_pct", -0.10))
    flip_stop_loss_lock_seconds = int(strategy_cfg.get("flip_stop_loss_lock_seconds", 480))
    global_flip_stop_pause_seconds = int(strategy_cfg.get("global_flip_stop_pause_seconds", 0))
    global_flip_stop_window_seconds = int(strategy_cfg.get("global_flip_stop_window_seconds", 1200))
    global_flip_stop_trigger_count = int(strategy_cfg.get("global_flip_stop_trigger_count", 2))
    normal_open_min_winner_stability = float(strategy_cfg.get("normal_open_min_winner_stability", 0.12))
    normal_open_buy_yes_min_winner_stability = float(strategy_cfg.get("normal_open_buy_yes_min_winner_stability", 0.30))
    normal_open_max_opposing_impulse_bps = float(strategy_cfg.get("normal_open_max_opposing_impulse_bps", 3.0))
    exploration_enabled = bool(strategy_cfg.get("exploration_enabled", True))
    exploration_idle_seconds = float(strategy_cfg.get("exploration_idle_seconds", 1800.0))
    exploration_min_confidence = int(strategy_cfg.get("exploration_min_confidence", 55))
    exploration_min_time_left_s = float(strategy_cfg.get("exploration_min_time_left_s", 120.0))
    exploration_edge_floor = float(strategy_cfg.get("exploration_edge_floor", 0.012))
    exploration_size_mult = float(strategy_cfg.get("exploration_size_mult", 0.35))
    model_side_stability_window = int(strategy_cfg.get("model_side_stability_window", 8))
    model_side_stability_min = float(strategy_cfg.get("model_side_stability_min", 0.55))
    model_side_stability_buy_yes_min = float(strategy_cfg.get("model_side_stability_buy_yes_min", 0.70))
    buy_yes_conf_floor = int(strategy_cfg.get("buy_yes_conf_floor", 52))
    buy_yes_consensus_floor = int(strategy_cfg.get("buy_yes_consensus_floor", 4))
    buy_yes_reentry_cooldown_mult = float(strategy_cfg.get("buy_yes_reentry_cooldown_mult", 1.20))
    buy_no_conf_floor = int(strategy_cfg.get("buy_no_conf_floor", 52))
    buy_no_consensus_floor = int(strategy_cfg.get("buy_no_consensus_floor", 4))
    buy_no_reentry_cooldown_mult = float(strategy_cfg.get("buy_no_reentry_cooldown_mult", 1.35))
    scalp_min_impulse_bps = float(strategy_cfg.get("scalp_min_impulse_bps", 9.0))
    scalp_buy_yes_min_impulse_bps = float(strategy_cfg.get("scalp_buy_yes_min_impulse_bps", scalp_min_impulse_bps))
    scalp_buy_no_min_impulse_bps = float(strategy_cfg.get("scalp_buy_no_min_impulse_bps", scalp_min_impulse_bps))
    scalp_min_edge = float(strategy_cfg.get("scalp_min_edge", 0.02))
    normal_min_edge = float(strategy_cfg.get("normal_min_edge", 0.025))
    reversal_min_edge = float(strategy_cfg.get("reversal_min_edge", 0.04))
    hard_stop_pct = float(strategy_cfg.get("hard_stop_pct", -0.15))
    open_fallback_max_spread = float(strategy_cfg.get("open_fallback_max_spread", 0.018))
    open_fallback_min_edge = float(strategy_cfg.get("open_fallback_min_edge", 0.03))
    min_entry_price = float(strategy_cfg.get("min_entry_price", 0.04))
    max_entry_price = float(strategy_cfg.get("max_entry_price", 0.96))
    open_map = {p.market_id: p for p in state.positions if p.status == "open"}

    impulse_source = str(strategy_cfg.get("impulse_source", "binance")).lower()
    cl_live, bi_live = _btc_live_prices()
    _push_src_hist("binance", bi_live)
    now_imp_ts = datetime.now(timezone.utc).timestamp()
    if impulse_source in {"coinbase", "kraken", "bybit"}:
        if now_imp_ts - float(_PRICE_SRC_LAST.get(impulse_source, 0.0)) >= 1.0:
            px = _fetch_alt_price(impulse_source)
            _PRICE_SRC_LAST[impulse_source] = now_imp_ts
            _push_src_hist(impulse_source, px)
    impulse = _impulse_signal(impulse_source)

    for r in btc_rows:
        # Trade only markets with known target BTC.
        if r.get("btc_target") is None:
            continue
        mid = str(r.get("market_id"))
        side = r.get("model_side")
        conf = int(r.get("model_confidence") or 0)
        consensus = int(r.get("model_consensus") or 0)
        best_model = str(r.get("best_model") or "-")
        if not side:
            continue

        ask_yes = float(r.get("best_ask_yes") or 0.0)
        ask_no = float(r.get("best_ask_no") or 0.0)
        open_pos = open_map.get(mid)

        edge_yes = float(r.get("edge_yes") or 0.0)
        edge_no = float(r.get("edge_no") or 0.0)
        open_edge = max(edge_yes, edge_no)

        btc_now = float(r.get("btc_current") or 0.0)
        btc_target = float(r.get("btc_target") or 0.0)
        t_left_s = max(0.0, float(r.get("t_left_s") or 0.0))
        # Recompute using wall-clock to avoid stale snapshot t_left_s near market expiry.
        end_ts_live = float(r.get("end_ts") or 0.0)
        t_left_open_s = (end_ts_live - datetime.now(timezone.utc).timestamp()) if end_ts_live > 0 else t_left_s
        t_left_open_s = max(0.0, t_left_open_s)
        winner_side = "BUY_YES" if btc_now >= btc_target else "BUY_NO"
        dist_bps = ((btc_now - btc_target) / btc_target * 10000.0) if btc_target > 0 else 0.0
        # Reversal belief from ensemble probability.
        p_yes = float(r.get("p_yes_model") or 0.5)
        p_hit = float(r.get("p_hit_target") or 0.5)
        _history_push(_EDGE_HIST, mid, {"ey": edge_yes, "en": edge_no})
        _history_push(_WINNER_HIST, mid, winner_side)
        _history_push(_MODEL_SIDE_HIST, mid, side, maxlen=max(3, model_side_stability_window))

        wh = _WINNER_HIST.get(mid, [])
        winner_stability = (sum(1 for x in wh if x == winner_side) / len(wh)) if wh else 0.0
        model_hist = _MODEL_SIDE_HIST.get(mid, [])
        model_side_stability = (sum(1 for x in model_hist if x == side) / len(model_hist)) if model_hist else 0.0

        # Reversal only when model disagrees, target hit chance is weak, and winner is unstable.
        reversal_belief = ((winner_side == "BUY_YES" and p_yes < 0.42) or (winner_side == "BUY_NO" and p_yes > 0.58)) and (p_hit < 0.45) and (winner_stability < 0.65)

        append_event(cfg["storage"]["events_path"], {
            "type": "strategy_snapshot",
            "market_id": mid,
            "side": side,
            "winner_side": winner_side,
            "distance_bps": round(dist_bps, 2),
            "reversal_belief": bool(reversal_belief),
            "winner_stability": round(winner_stability, 3),
            "model_side_stability": round(model_side_stability, 3),
            "p_hit_target": round(p_hit, 4),
            "confidence": conf,
            "consensus": consensus,
            "best_model": best_model,
            "edge_yes": edge_yes,
            "edge_no": edge_no,
            "t_left_open_s": round(float(t_left_open_s), 2),
            "open_positions": len(open_map),
            "flip_fail_streak": int(_FLIP_FAIL_STREAK.get(mid, 0) or 0),
            "market_locked": bool(datetime.now(timezone.utc).timestamp() < float(_MARKET_LOCK_UNTIL.get(mid, 0.0) or 0.0)),
            "recent_losing_buy_no": bool(
                str(_LAST_CLOSE_SIDE.get(mid, "") or "") == "BUY_NO"
                and float(_LAST_CLOSE_PNL.get(mid, 0.0) or 0.0) <= 0
                and (datetime.now(timezone.utc).timestamp() - float(_LAST_CLOSE_TS.get(mid, 0.0) or 0.0)) < 1800
            ),
        })

        # Open rule v3: trend-follow by default with persistence filter; reversal is rare.
        now_epoch = datetime.now(timezone.utc).timestamp()
        last_close_ts = float(_LAST_CLOSE_TS.get(mid, 0.0))
        last_close_reason = str(_LAST_CLOSE_REASON.get(mid, "") or "")
        last_close_side = str(_LAST_CLOSE_SIDE.get(mid, "") or "")
        last_close_pnl = float(_LAST_CLOSE_PNL.get(mid, 0.0) or 0.0)
        reentry_cooldown_s = flip_reentry_cooldown_s if last_close_reason in {"edge_flip_wrong_way", "edge_decay_stop", "flip_stop"} else base_reentry_cooldown_s
        if winner_side == "BUY_YES":
            reentry_cooldown_s *= buy_yes_reentry_cooldown_mult
        elif winner_side == "BUY_NO":
            reentry_cooldown_s *= buy_no_reentry_cooldown_mult
        # Extra churn brake: after a losing close on the same side, wait longer before re-entering.
        if winner_side == last_close_side and last_close_pnl <= 0:
            reentry_cooldown_s *= 1.35
        # Hard-stop losses are expensive; cool down that side materially longer.
        if last_close_reason == "hard_stop_25" and winner_side == last_close_side:
            reentry_cooldown_s = max(reentry_cooldown_s, 600.0)
        # Wrong-way exits indicate unstable read; force a longer cooldown before re-entry.
        if last_close_reason in {"against_winner_no_reversal", "edge_flip_wrong_way"}:
            reentry_cooldown_s = max(reentry_cooldown_s, 420.0)
        lock_until = float(_MARKET_LOCK_UNTIL.get(mid, 0.0) or 0.0)
        lock_ok = now_epoch >= lock_until
        global_pause_ok = now_epoch >= float(_GLOBAL_OPEN_PAUSE_UNTIL or 0.0)
        cool_ok = (now_epoch - last_close_ts) > reentry_cooldown_s and lock_ok and global_pause_ok
        open_side = winner_side
        required_edge = normal_min_edge
        if p_hit > 0.65 and winner_stability >= 0.7:
            required_edge *= 0.85
        if reversal_belief:
            open_side = "BUY_NO" if winner_side == "BUY_YES" else "BUY_YES"
            required_edge = reversal_min_edge

        eh = _EDGE_HIST.get(mid, [])
        last = eh[-5:]
        if open_side == "BUY_YES":
            persist = sum(1 for x in last if float(x.get("ey", 0.0)) > 0)
        else:
            persist = sum(1 for x in last if float(x.get("en", 0.0)) > 0)

        side_edge = edge_yes if open_side == "BUY_YES" else edge_no

        conf_floor = buy_no_conf_floor if open_side == "BUY_NO" else buy_yes_conf_floor
        consensus_floor = buy_no_consensus_floor if open_side == "BUY_NO" else buy_yes_consensus_floor
        # Side-specific loss cooloff: after a recent same-side loss, require stronger setup.
        if open_side == "BUY_NO" and last_close_side == "BUY_NO" and last_close_pnl <= 0 and (now_epoch - last_close_ts) < 1800:
            conf_floor += 4
            consensus_floor += 1
        elif open_side == "BUY_YES" and last_close_side == "BUY_YES" and last_close_pnl <= 0 and (now_epoch - last_close_ts) < 1800:
            conf_floor += 3
            consensus_floor += 1
        # Fast Binance impulse scalp: take movement direction, then exit quickly when edge decays.
        impulse_side = impulse.get("side")
        impulse_bps = float(impulse.get("bps_3s") or 0.0)

        # Avoid late contrarian flips when winner side is already stable.
        late_contrarian_block = (t_left_open_s < 240) and (winner_stability >= 0.70) and (open_side != winner_side)
        min_stability_floor = normal_open_buy_yes_min_winner_stability if open_side == "BUY_YES" else normal_open_min_winner_stability
        low_stability_block = winner_stability < min_stability_floor
        model_side_stability_floor = model_side_stability_buy_yes_min if open_side == "BUY_YES" else model_side_stability_min
        low_model_side_stability_block = model_side_stability < model_side_stability_floor
        impulse_against_open = (
            (open_side == "BUY_YES" and impulse_bps <= -abs(normal_open_max_opposing_impulse_bps))
            or (open_side == "BUY_NO" and impulse_bps >= abs(normal_open_max_opposing_impulse_bps))
        )

        normal_open_ok = open_pos is None and conf >= conf_floor and consensus >= consensus_floor and side_edge >= required_edge and persist >= 3 and len(open_map) < max_open_positions and cool_ok and t_left_open_s >= min_time_left_for_entry_s and (not late_contrarian_block) and (not low_stability_block) and (not low_model_side_stability_block) and (not impulse_against_open)

        impulse_edge = edge_yes if impulse_side == "BUY_YES" else edge_no
        scalp_impulse_req = scalp_buy_yes_min_impulse_bps if impulse_side == "BUY_YES" else scalp_buy_no_min_impulse_bps
        scalp_open_ok = open_pos is None and impulse_side in {"BUY_YES", "BUY_NO"} and abs(impulse_bps) >= scalp_impulse_req and impulse_edge >= scalp_min_edge and len(open_map) < max_open_positions and cool_ok and t_left_open_s >= max(75.0, min_time_left_for_entry_s)

        force_explore_open = False
        if exploration_enabled and open_pos is None and len(open_map) < max_open_positions and cool_ok:
            idle_s = now_epoch - float(_LAST_OPEN_TS or 0.0)
            if idle_s >= exploration_idle_seconds and t_left_open_s >= exploration_min_time_left_s and conf >= exploration_min_confidence:
                if max(edge_yes, edge_no) >= exploration_edge_floor:
                    force_explore_open = True

        if normal_open_ok or scalp_open_ok or force_explore_open:
            side = ("BUY_YES" if edge_yes >= edge_no else "BUY_NO") if force_explore_open else (impulse_side if scalp_open_ok else open_side)
            ask_open = ask_yes if side == "BUY_YES" else ask_no
            bid_open = float(r.get("best_bid_yes") or 0.0) if side == "BUY_YES" else float(r.get("best_bid_no") or 0.0)
            ex_cfg = cfg.get("execution", {})
            open_mode = str(ex_cfg.get("open_mode", "limit_first")).lower()
            tick = float(ex_cfg.get("tick_size", 0.001))
            improve_ticks = int(ex_cfg.get("open_limit_improve_ticks", 1))
            open_fallback_taker = bool(ex_cfg.get("open_limit_fallback_taker", True))
            spread_open = max(0.0, ask_open - bid_open) if (ask_open > 0 and bid_open > 0) else 0.0
            if open_mode == "market":
                entry = ask_open
                open_exec = "open_market"
            else:
                limit_open = _round_price(max(0.0, bid_open + (improve_ticks * tick)), tick) if bid_open > 0 else ask_open
                if ask_open > 0 and limit_open >= ask_open:
                    entry = ask_open
                    open_exec = "open_limit_fill"
                elif open_fallback_taker and ask_open > 0 and spread_open <= open_fallback_max_spread and side_edge >= open_fallback_min_edge:
                    entry = ask_open
                    open_exec = "open_limit_timeout_fallback"
                else:
                    entry = 0.0
                    open_exec = "open_limit_pending_skip"
                    if open_fallback_taker and ask_open > 0:
                        reason = "open_fallback_spread_too_wide" if spread_open > open_fallback_max_spread else "open_fallback_edge_too_low"
                        append_event(cfg["storage"]["events_path"], {
                            "type": "market_guardrail",
                            "market_id": mid,
                            "reason": reason,
                            "side": side,
                            "best_bid": round(float(bid_open), 4),
                            "best_ask": round(float(ask_open), 4),
                            "spread": round(float(spread_open), 4),
                            "max_spread": round(float(open_fallback_max_spread), 4),
                            "side_edge": round(float(side_edge), 4),
                            "min_side_edge": round(float(open_fallback_min_edge), 4),
                            "confidence": conf,
                            "consensus": consensus,
                            "model": str(best_model),
                        })

            # Confidence-weighted sizing; smaller size for scalp entries.
            size_mul = max(0.5, min(1.0, 0.5 + (conf / 100.0) * 0.6))
            if scalp_open_ok:
                size_mul = min(size_mul, 0.65)
            if force_explore_open:
                size_mul = min(size_mul, max(0.15, exploration_size_mult))
            cash_now = float(state.cash_usd)
            per_trade_cash_cap = max(1.0, cash_now * max_trade_cash_fraction)
            size_usd = min(trade_cap * size_mul, per_trade_cash_cap, cash_now)
            if force_explore_open:
                model_tag = f"EXPLORE:{side}:idle"
            else:
                model_tag = (f"SCALP:{impulse.get('source','src')}:{side}:{round(impulse_bps,1)}bps" if scalp_open_ok else best_model)
            entry_price_ok = (entry >= min_entry_price) and (entry <= max_entry_price)
            if (entry > 0) and (not entry_price_ok):
                append_event(cfg["storage"]["events_path"], {
                    "type": "market_guardrail",
                    "market_id": mid,
                    "reason": "entry_price_out_of_bounds",
                    "side": side,
                    "entry_price": round(float(entry), 4),
                    "min_entry_price": round(float(min_entry_price), 4),
                    "max_entry_price": round(float(max_entry_price), 4),
                    "confidence": conf,
                    "consensus": consensus,
                    "model": model_tag,
                })
                continue
            if entry > 0 and size_usd >= 1.0:
                live_open = None
                if live_enabled:
                    tok = (token_ids_by_market.get(mid) or {}).get(side)
                    qty = (size_usd / entry) if entry > 0 else 0.0
                    live_open = live_exec.place(
                        token_id=str(tok or ""),
                        side="BUY",
                        price=float(entry),
                        size=float(qty),
                        post_only=open_exec in {"open_limit_fill", "open_limit_pending_skip"},
                    )
                    append_event(cfg["storage"]["events_path"], {
                        "type": "live_trade",
                        "action": "OPEN_SUBMIT",
                        "market_id": mid,
                        "market_name": str(r.get("market_name") or mid),
                        "token_id": tok,
                        "side": side,
                        "price": round(float(entry), 4),
                        "qty": round(float(qty), 6),
                        "open_execution": open_exec,
                        "ok": bool(live_open.ok),
                        "order_id": live_open.order_id,
                        "error": live_open.error,
                    })
                    if not live_open.ok:
                        print(f"[red]LIVE OPEN FAILED[/red] {mid} {side} err={live_open.error}")
                        continue

                pos = open_position(
                    state,
                    market_id=mid,
                    market_name=str(r.get("market_name") or mid),
                    side=side,
                    entry_price=entry,
                    size_usd=size_usd,
                    model=model_tag,
                )
                pos.edge_entry = float(edge_yes if side == "BUY_YES" else edge_no)
                pos.edge_peak = pos.edge_entry
                open_map[mid] = pos
                _LAST_OPEN_TS = datetime.now(timezone.utc).timestamp()
                append_event(cfg["storage"]["events_path"], {
                    "type": "paper_trade",
                    "action": "OPEN",
                    "market_id": mid,
                    "market_name": pos.market_name,
                    "side": side,
                    "size_usd": round(pos.size_usd, 2),
                    "entry_price": round(pos.entry_price, 4),
                    "opened_at": pos.opened_at,
                    "model": model_tag,
                    "open_execution": open_exec,
                    "live_order_id": (live_open.order_id if live_open else None),
                    "confidence": conf,
                    "consensus": consensus,
                    "winner_side": winner_side,
                    "winner_stability": round(winner_stability, 3),
                    "model_side_stability": round(model_side_stability, 3),
                    "p_hit_target": round(p_hit, 4),
                    "impulse_bps_3s": round(impulse_bps, 2),
                    "edge_yes": round(edge_yes, 4),
                    "edge_no": round(edge_no, 4),
                })
                print(f"[green]OPEN[/green] {mid} {side} size=${pos.size_usd:.2f} price={pos.entry_price:.4f} exec={open_exec} model={model_tag} conf={conf} cons={consensus}")
            continue

        # Close policy v1: resolve proxy + tp ladder + stops + time decay + flip stop.
        if open_pos is not None:
            mark_price = ask_yes if open_pos.side == "BUY_YES" else ask_no
            if mark_price <= 0:
                continue

            entry = float(open_pos.entry_price or 0.0)
            u_pnl = ((mark_price - entry) / entry) if entry > 0 else 0.0
            if open_pos.side == "BUY_NO":
                # BUY_NO still marks against NO ask directly (same price-space), no inversion needed.
                u_pnl = ((mark_price - entry) / entry) if entry > 0 else 0.0

            now_ts = datetime.now(timezone.utc).timestamp()
            end_ts = float(r.get("end_ts") or 0.0)
            t_left = (end_ts - now_ts) if end_ts > 0 else 999999.0
            held_edge = edge_yes if open_pos.side == "BUY_YES" else edge_no
            opp_edge = edge_no if open_pos.side == "BUY_YES" else edge_yes
            flip = (side != open_pos.side) and conf >= flip_signal_conf_min
            against_winner = (open_pos.side != winner_side)

            peak = float(open_pos.edge_peak if open_pos.edge_peak is not None else (open_pos.edge_entry or held_edge or 0.0))
            peak = max(peak, held_edge)
            open_pos.edge_peak = peak

            close_reason = None
            close_frac = 0.0
            held_s = _seconds_since_iso(open_pos.opened_at)

            # Resolve proxy
            if mark_price >= 0.99:
                close_reason, close_frac = "resolved_win_proxy", 1.0
            elif mark_price <= 0.01:
                close_reason, close_frac = "resolved_loss_proxy", 1.0
            # hard stops
            elif u_pnl <= hard_stop_pct:
                close_reason, close_frac = "hard_stop_25", 1.0
            elif flip and held_s >= min_hold_for_flip_stop_s and u_pnl <= (buy_no_flip_stop_loss_pct if open_pos.side == "BUY_NO" else flip_stop_loss_pct):
                close_reason, close_frac = "flip_stop", 1.0
            # Fast scalp exits: enter on impulse, exit quickly after PM reaction.
            elif str(open_pos.model or "").startswith("SCALP:") and u_pnl >= 0.02:
                close_reason, close_frac = "scalp_take_quick", 1.0
            elif str(open_pos.model or "").startswith("SCALP:") and held_s >= 30:
                close_reason, close_frac = "scalp_timeout", 1.0
            elif str(open_pos.model or "").startswith("SCALP:") and held_edge < 0.004:
                close_reason, close_frac = "scalp_edge_faded", 1.0
            # mispricing goes wrong-way: cut/flip risk (after brief hold to reduce churn)
            elif held_s >= min_hold_for_flip_exit_s and held_edge <= -0.012 and opp_edge >= 0.025:
                close_reason, close_frac = "edge_flip_wrong_way", 1.0
            elif held_s >= min_hold_for_flip_exit_s and held_edge < 0.0 and u_pnl < 0:
                close_reason, close_frac = "edge_decay_stop", 1.0
            elif held_s >= min_hold_for_flip_exit_s and peak > 0 and held_edge < (0.45 * peak) and u_pnl > 0:
                close_reason, close_frac = "edge_trailing_stop", 1.0
            # if we're fighting current winner and no real reversal thesis, cut.
            elif against_winner and (not reversal_belief) and t_left_s < 300:
                close_reason, close_frac = "against_winner_no_reversal", 1.0
            # time exits
            elif t_left < 45:
                close_reason, close_frac = "time_lt_45s", 1.0
            elif t_left < 90 and u_pnl > 0:
                close_reason, close_frac = "time_lt_90s_bank", 1.0
            elif t_left < 180 and conf < 58:
                close_reason, close_frac = "time_lt_180s_low_conf", 1.0
            # take profit ladder (single-step per cycle)
            elif u_pnl >= 0.50:
                close_reason, close_frac = "tp_50", 1.0
            elif u_pnl >= 0.35 and not bool(getattr(open_pos, "tp35_taken", False)):
                close_reason, close_frac = "tp_35_half", 0.5

            close_fill_meta = None
            exit_price = None
            execution_tag = None
            if close_frac > 0:
                order = _build_close_order(open_pos.side, r, cfg)
                if order.get("mode") == "market" or close_frac < 1.0:
                    exit_price = float(order.get("taker_price") or 0.0)
                    execution_tag = "close_market"
                else:
                    exit_price, execution_tag, close_fill_meta = _resolve_limit_close(open_pos, close_reason, order, cfg)
                    if exit_price is None:
                        append_event(cfg["storage"]["events_path"], {
                            "type": "paper_trade",
                            "action": "CLOSE_PENDING",
                            "reason": close_reason,
                            "market_id": mid,
                            "market_name": open_pos.market_name,
                            "side": open_pos.side,
                            "model_open": open_pos.model,
                            "close_execution": order.get("mode"),
                            "meta": close_fill_meta,
                        })
                        continue
                if exit_price <= 0:
                    continue

            if close_frac > 0:
                live_close = None
                if live_enabled:
                    tok = (token_ids_by_market.get(mid) or {}).get(open_pos.side)
                    qty_close = float(open_pos.qty) * float(close_frac)
                    live_close = live_exec.place(
                        token_id=str(tok or ""),
                        side="SELL",
                        price=float(exit_price),
                        size=float(qty_close),
                        post_only=(execution_tag in {"close_limit_fill"}),
                    )
                    append_event(cfg["storage"]["events_path"], {
                        "type": "live_trade",
                        "action": "CLOSE_SUBMIT" if close_frac >= 1.0 else "PARTIAL_CLOSE_SUBMIT",
                        "reason": close_reason,
                        "market_id": mid,
                        "market_name": open_pos.market_name,
                        "token_id": tok,
                        "side": open_pos.side,
                        "price": round(float(exit_price), 4),
                        "qty": round(float(qty_close), 6),
                        "close_execution": execution_tag,
                        "ok": bool(live_close.ok),
                        "order_id": live_close.order_id,
                        "error": live_close.error,
                    })
                    if not live_close.ok:
                        print(f"[red]LIVE CLOSE FAILED[/red] {mid} {open_pos.side} err={live_close.error}")
                        continue

                open_pos.close_model = best_model
                open_pos.close_reason = close_reason
                open_model_name = str(open_pos.model or "").split(":", 1)[0]
                if close_frac >= 1.0:
                    _PENDING_CLOSES.pop(_pos_key(open_pos), None)
                    pnl = close_position(state, open_pos, exit_price)
                    _LAST_CLOSE_TS[mid] = datetime.now(timezone.utc).timestamp()
                    _LAST_CLOSE_REASON[mid] = close_reason
                    _LAST_CLOSE_SIDE[mid] = str(open_pos.side or "")
                    _LAST_CLOSE_PNL[mid] = float(pnl)
                else:
                    pnl = close_fraction(state, open_pos, exit_price, close_frac)
                    open_pos.pnl_usd = float((open_pos.pnl_usd or 0.0) + pnl)
                    if close_reason == "tp_35_half":
                        open_pos.tp35_taken = True

                # Model learning updates only on full close (clean attribution)
                if close_frac >= 1.0:
                    ms = _MODEL_STATS.get(open_model_name)
                    if ms is not None:
                        ms["trades"] = int(ms.get("trades", 0)) + 1
                        ms["wins"] = int(ms.get("wins", 0)) + (1 if pnl > 0 else 0)
                        ms["pnl"] = float(ms.get("pnl", 0.0)) + float(pnl)

                    # Guardrail: lock a market after repeated wrong-way flip exits with non-positive outcomes.
                    streak = int(_FLIP_FAIL_STREAK.get(mid, 0) or 0)
                    if close_reason == "edge_flip_wrong_way" and pnl <= 0:
                        streak += 1
                    elif close_reason in {"edge_trailing_stop", "tp_50", "tp_35_half", "time_lt_90s_bank", "resolved_win_proxy"} and pnl > 0:
                        streak = 0
                    else:
                        streak = max(0, streak - 1)
                    _FLIP_FAIL_STREAK[mid] = streak

                    if close_reason == "edge_flip_wrong_way" and pnl <= 0:
                        lock_s = 360
                        _MARKET_LOCK_UNTIL[mid] = max(
                            float(_MARKET_LOCK_UNTIL.get(mid, 0.0) or 0.0),
                            datetime.now(timezone.utc).timestamp() + lock_s,
                        )
                        append_event(cfg["storage"]["events_path"], {
                            "type": "market_guardrail",
                            "market_id": mid,
                            "reason": "single_flip_loss_cooloff",
                            "flip_fail_streak": streak,
                            "lock_seconds": lock_s,
                            "lock_until_ts": _MARKET_LOCK_UNTIL[mid],
                            "last_close_reason": close_reason,
                            "last_pnl_usd": round(float(pnl), 4),
                        })

                    # Hard-stop losses are costly; pause this market longer before trying again.
                    if close_reason == "hard_stop_25" and pnl <= 0:
                        lock_s = 720
                        _MARKET_LOCK_UNTIL[mid] = max(
                            float(_MARKET_LOCK_UNTIL.get(mid, 0.0) or 0.0),
                            datetime.now(timezone.utc).timestamp() + lock_s,
                        )
                        append_event(cfg["storage"]["events_path"], {
                            "type": "market_guardrail",
                            "market_id": mid,
                            "reason": "single_hard_stop_cooloff",
                            "flip_fail_streak": streak,
                            "lock_seconds": lock_s,
                            "lock_until_ts": _MARKET_LOCK_UNTIL[mid],
                            "last_close_reason": close_reason,
                            "last_pnl_usd": round(float(pnl), 4),
                        })

                    # Flip-stop loss usually means noisy direction change; cool off this market briefly.
                    if close_reason == "flip_stop" and pnl <= 0 and flip_stop_loss_lock_seconds > 0:
                        lock_s = flip_stop_loss_lock_seconds
                        _MARKET_LOCK_UNTIL[mid] = max(
                            float(_MARKET_LOCK_UNTIL.get(mid, 0.0) or 0.0),
                            datetime.now(timezone.utc).timestamp() + lock_s,
                        )
                        append_event(cfg["storage"]["events_path"], {
                            "type": "market_guardrail",
                            "market_id": mid,
                            "reason": "flip_stop_loss_cooloff",
                            "flip_fail_streak": streak,
                            "lock_seconds": lock_s,
                            "lock_until_ts": _MARKET_LOCK_UNTIL[mid],
                            "last_close_reason": close_reason,
                            "last_pnl_usd": round(float(pnl), 4),
                        })

                        # Cross-market churn brake: repeated flip-stop losses often signal regime whipsaw.
                        now_ts = datetime.now(timezone.utc).timestamp()
                        if global_flip_stop_pause_seconds > 0 and global_flip_stop_trigger_count > 0:
                            _RECENT_FLIP_STOP_LOSS_TS = [
                                t for t in _RECENT_FLIP_STOP_LOSS_TS
                                if (now_ts - float(t)) <= max(60, global_flip_stop_window_seconds)
                            ]
                            _RECENT_FLIP_STOP_LOSS_TS.append(now_ts)
                            if len(_RECENT_FLIP_STOP_LOSS_TS) >= global_flip_stop_trigger_count:
                                _GLOBAL_OPEN_PAUSE_UNTIL = max(
                                    float(_GLOBAL_OPEN_PAUSE_UNTIL or 0.0),
                                    now_ts + float(global_flip_stop_pause_seconds),
                                )
                                append_event(cfg["storage"]["events_path"], {
                                    "type": "market_guardrail",
                                    "market_id": "*",
                                    "reason": "global_flip_stop_cooloff",
                                    "lock_seconds": int(global_flip_stop_pause_seconds),
                                    "lock_until_ts": _GLOBAL_OPEN_PAUSE_UNTIL,
                                    "recent_flip_stop_losses": len(_RECENT_FLIP_STOP_LOSS_TS),
                                    "window_seconds": int(global_flip_stop_window_seconds),
                                    "last_close_reason": close_reason,
                                    "last_pnl_usd": round(float(pnl), 4),
                                })

                    if streak >= 2:
                        lock_s = min(900, 300 + (streak - 2) * 180)
                        _MARKET_LOCK_UNTIL[mid] = datetime.now(timezone.utc).timestamp() + lock_s
                        append_event(cfg["storage"]["events_path"], {
                            "type": "market_guardrail",
                            "market_id": mid,
                            "reason": "flip_streak_lockout",
                            "flip_fail_streak": streak,
                            "lock_seconds": lock_s,
                            "lock_until_ts": _MARKET_LOCK_UNTIL[mid],
                            "last_close_reason": close_reason,
                            "last_pnl_usd": round(float(pnl), 4),
                        })

                append_event(cfg["storage"]["events_path"], {
                    "type": "paper_trade",
                    "action": "CLOSE" if close_frac >= 1.0 else "PARTIAL_CLOSE",
                    "reason": close_reason,
                    "fraction": close_frac,
                    "market_id": mid,
                    "market_name": open_pos.market_name,
                    "side": open_pos.side,
                    "entry_price": round(entry, 4),
                    "exit_price": round(exit_price, 4),
                    "opened_at": open_pos.opened_at,
                    "closed_at": open_pos.closed_at if close_frac >= 1.0 else None,
                    "pnl_usd": round(pnl, 4),
                    "model_open": open_pos.model,
                    "model_close": best_model,
                    "close_execution": execution_tag,
                    "close_meta": close_fill_meta,
                    "live_order_id": (live_close.order_id if live_close else None),
                    "confidence": conf,
                    "held_edge": round(held_edge, 4),
                    "opp_edge": round(opp_edge, 4),
                })
                append_event(cfg["storage"]["events_path"], {"type": "model_stats", "stats": _MODEL_STATS})
                print(f"[magenta]{'CLOSE' if close_frac>=1 else 'PARTIAL'}[/magenta] {mid} {open_pos.side} reason={close_reason} exec={execution_tag} pnl=${pnl:.2f}")

    save_state(cfg["storage"]["state_path"], state)
    print(f"[bold]State[/bold] cash=${state.cash_usd:.2f} positions={len(state.positions)} pnl=${state.realized_pnl_usd:.2f}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--once", action="store_true")
    args = parser.parse_args()

    cfg = load_config(args.config)
    run_once(cfg)


if __name__ == "__main__":
    main()
