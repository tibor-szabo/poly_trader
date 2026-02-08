import argparse
import re
import math
from datetime import datetime, timedelta, timezone
from typing import Optional

import httpx
from rich import print

from polymarket_mvp.config import load_config
from polymarket_mvp.adapters.clob import ClobAdapter
from polymarket_mvp.engine.scoring import score_opportunities, rank_candidates, depth_aware_buy_prices
from polymarket_mvp.risk.guards import approve
from polymarket_mvp.sim.paper import open_position, close_position
from polymarket_mvp.utils.storage import load_state, save_state, append_event
from polymarket_mvp.adapters.gamma import GammaAdapter
from polymarket_mvp.ops_intel import build_market_radar, build_inefficiency_report, build_flow_watch
from polymarket_mvp.ws_hook import ClobWsHook
from polymarket_mvp.rtds_hook import BtcRtdsHook


_WS_HOOK = None
_RTDS_BTC = None
_ALT_REFS_CACHE = []
_ALT_REFS_TS = 0.0
_BTC_TARGET_CACHE = {}
_BTC_PRICE_CACHE = {}
_BTC_CURRENT_CACHE = {"ts": 0.0, "price": None}
_BTC_SIGNAL_HISTORY = []
_MODEL_STATS = {
    "TA": {"trades": 0, "wins": 0, "pnl": 0.0},
    "LL": {"trades": 0, "wins": 0, "pnl": 0.0},
    "RG": {"trades": 0, "wins": 0, "pnl": 0.0},
    "BK": {"trades": 0, "wins": 0, "pnl": 0.0},
}


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


def _model_compare(row: dict, signal: dict) -> dict:
    p_ta = max(0.02, min(0.98, float(signal.get("p_up", 0.5))))
    ta = _bids_from_p(row, p_ta, 0.006)
    z = max(-1.5, min(1.5, float(signal.get("lead_bps", 0.0)) / 35.0))
    ll = _bids_from_p(row, max(0.02, min(0.98, 0.5 + 0.18 * z)), 0.006)
    trend = abs(float(signal.get("rf", 0.0))) + abs(float(signal.get("rs", 0.0)))
    chop = float(signal.get("sigma", 0.0))
    w_trend = max(0.1, min(0.9, trend / max(trend + chop, 1e-6)))
    p_mr = max(0.02, min(0.98, 0.5 - 0.35 * float(signal.get("rsi_n", 0.0))))
    rg = _bids_from_p(row, max(0.02, min(0.98, w_trend * p_ta + (1 - w_trend) * p_mr)), 0.0065)
    ay = float(row.get("best_ask_yes") or 0.0)
    an = float(row.get("best_ask_no") or 0.0)
    by = float(row.get("best_bid_yes") or 0.0)
    bn = float(row.get("best_bid_no") or 0.0)
    sy = max(0.0, ay - by) if ay > 0 else 0.01
    sn = max(0.0, an - bn) if an > 0 else 0.01
    bk = _bids_from_p(row, max(0.02, min(0.98, 0.5 + 0.12 * (sn - sy))), 0.006)

    models = {"TA": ta, "LL": ll, "RG": rg, "BK": bk}
    scored = []
    for name, pair in models.items():
        if not pair or pair[0] is None or pair[1] is None:
            continue
        y, n = pair
        direction = "BUY_YES" if y >= n else "BUY_NO"
        strength = abs(y - n)
        wgt = _model_weight(name)
        scored.append((strength * wgt, name, direction, y, n, strength))
    if not scored:
        return {"models": models, "best": "-", "side": None, "confidence": 0, "weights": {k: round(_model_weight(k), 3) for k in models.keys()}}
    scored.sort(reverse=True)
    _, best_name, side, _, _, base_strength = scored[0]
    agree = sum(1 for _, _, d, _, _, _ in scored if d == side) / len(scored)
    confidence = int(max(1, min(99, round((0.6 * base_strength + 0.4 * agree) * 100))))
    return {
        "models": models,
        "best": best_name,
        "side": side,
        "confidence": confidence,
        "weights": {k: round(_model_weight(k), 3) for k in models.keys()},
    }


def _polymarket_btc_prices(event_start_iso: str, end_iso: str, variant: str = "fifteen") -> tuple[Optional[float], Optional[float]]:
    if not event_start_iso:
        return None, None
    key = f"{event_start_iso}|{end_iso}|{variant}"
    if key in _BTC_PRICE_CACHE:
        return _BTC_PRICE_CACHE[key]

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

    _BTC_PRICE_CACHE[key] = (open_px, current_px)
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


def run_once(cfg: dict):
    _ensure_btc_live_feed(cfg["storage"]["events_path"])

    clob = ClobAdapter(cfg["data"]["clob_rest_base"])
    gamma = GammaAdapter(cfg["data"]["gamma_base"])
    clob.reset_call_count()
    gamma.reset_call_count()
    state = load_state(cfg["storage"]["state_path"], float(cfg["paper"]["starting_cash_usd"]))

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
        generated_refs = gamma.fetch_market_refs_by_generated_15m_slugs(prefixes, windows=16)
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

    # BTC group policy: always show the next 3 resolving BTC markets.
    now_dt = datetime.now(timezone.utc)
    btc_ref_by_id = {r.market_id: r for r in btc_refs}
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
    btc_rows = [x[1] for x in btc_candidates[:3]]
    if len(btc_rows) < 3:
        fb = [row_by_market[m] for m in btc_ids if m in row_by_market and row_by_market[m] not in btc_rows]
        fb.sort(key=lambda x: (x["ask_sum_with_fees"], -x["depth_usd"]))
        btc_rows.extend(fb[: max(0, 3 - len(btc_rows))])

    # BTC metadata from Polymarket crypto-price endpoint (Chainlink-derived in market UI).
    for r in btc_rows:
        rr = btc_ref_by_id.get(r.get("market_id"))
        src = getattr(rr, "resolution_source", "") if rr else ""
        st = getattr(rr, "event_start_time", "") if rr else ""
        ed = getattr(rr, "end_date", "") if rr else ""
        target_px, current_px = _polymarket_btc_prices(st, ed, variant="fifteen")
        chainlink_live, binance_live = _btc_live_prices()
        if current_px is None:
            current_px = chainlink_live
        r["btc_price_source"] = src or "https://data.chain.link/streams/btc-usd"
        r["btc_target"] = round(target_px, 2) if target_px is not None else None
        r["btc_current"] = round(current_px, 2) if current_px is not None else None
        r["btc_current_binance"] = round(binance_live, 2) if binance_live is not None else None
        r["btc_target_start"] = st

        _update_btc_signal_history(current_px, binance_live)
        sigm = _compute_btc_signal()
        cmp = _model_compare(r, sigm)
        r["model_ta"] = cmp["models"].get("TA")
        r["model_ll"] = cmp["models"].get("LL")
        r["model_rg"] = cmp["models"].get("RG")
        r["model_bk"] = cmp["models"].get("BK")
        r["best_model"] = f"{cmp['best']}:{'UP' if cmp.get('side')=='BUY_YES' else 'DOWN'} {cmp.get('confidence',0)}%" if cmp.get("side") else "-"
        r["model_side"] = cmp.get("side")
        r["model_confidence"] = cmp.get("confidence", 0)

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

    append_event(cfg["storage"]["events_path"], {
        "type": "market_groups",
        "bitcoin": btc_rows,
        "secondary": alt_rows,
        "secondary_note": f"Non-BTC markets resolving within {alt_horizon_days} days, ranked by paired YES+NO arb proximity (sum toward <1)",
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

    # Model-driven BTC paper trading simulation.
    trade_cap = 100.0
    open_map = {p.market_id: p for p in state.positions if p.status == "open"}

    for r in btc_rows:
        mid = str(r.get("market_id"))
        side = r.get("model_side")
        conf = int(r.get("model_confidence") or 0)
        best_model = str(r.get("best_model") or "-")
        if not side:
            continue

        ask_yes = float(r.get("best_ask_yes") or 0.0)
        ask_no = float(r.get("best_ask_no") or 0.0)
        open_pos = open_map.get(mid)

        # Open rule: confident directional model, one open position per market.
        if open_pos is None and conf >= 30:
            entry = ask_yes if side == "BUY_YES" else ask_no
            size_usd = min(trade_cap, float(state.cash_usd))
            if entry > 0 and size_usd >= 1.0:
                pos = open_position(
                    state,
                    market_id=mid,
                    market_name=str(r.get("market_name") or mid),
                    side=side,
                    entry_price=entry,
                    size_usd=size_usd,
                    model=best_model,
                )
                append_event(cfg["storage"]["events_path"], {
                    "type": "paper_trade",
                    "action": "OPEN",
                    "market_id": mid,
                    "market_name": pos.market_name,
                    "side": side,
                    "size_usd": round(pos.size_usd, 2),
                    "entry_price": round(pos.entry_price, 4),
                    "opened_at": pos.opened_at,
                    "model": best_model,
                    "confidence": conf,
                })
                print(f"[green]OPEN[/green] {mid} {side} size=${pos.size_usd:.2f} price={pos.entry_price:.4f} model={best_model}")
            continue

        # Close rule: model flips with sufficient confidence.
        if open_pos is not None:
            flip = (side != open_pos.side) and conf >= 30
            if flip:
                exit_price = ask_yes if open_pos.side == "BUY_YES" else ask_no
                if exit_price > 0:
                    open_pos.close_model = best_model
                    pnl = close_position(state, open_pos, exit_price)
                    open_model_name = str(open_pos.model or "").split(":", 1)[0]
                    ms = _MODEL_STATS.get(open_model_name)
                    if ms is not None:
                        ms["trades"] = int(ms.get("trades", 0)) + 1
                        ms["wins"] = int(ms.get("wins", 0)) + (1 if pnl > 0 else 0)
                        ms["pnl"] = float(ms.get("pnl", 0.0)) + float(pnl)
                    append_event(cfg["storage"]["events_path"], {
                        "type": "paper_trade",
                        "action": "CLOSE",
                        "market_id": mid,
                        "market_name": open_pos.market_name,
                        "side": open_pos.side,
                        "entry_price": round(open_pos.entry_price, 4),
                        "exit_price": round(exit_price, 4),
                        "opened_at": open_pos.opened_at,
                        "closed_at": open_pos.closed_at,
                        "pnl_usd": round(pnl, 4),
                        "model_open": open_pos.model,
                        "model_close": best_model,
                        "confidence": conf,
                    })
                    append_event(cfg["storage"]["events_path"], {
                        "type": "model_stats",
                        "stats": _MODEL_STATS,
                    })
                    print(f"[magenta]CLOSE[/magenta] {mid} {open_pos.side} exit={exit_price:.4f} pnl=${pnl:.2f} model={best_model}")

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
