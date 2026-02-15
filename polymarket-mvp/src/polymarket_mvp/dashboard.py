import json
import mimetypes
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "data" / "state.json"
EVENTS = ROOT / "data" / "events.jsonl"
WEB = ROOT / "web"


def read_state():
    if not STATE.exists():
        return {"cash_usd": None, "positions": [], "realized_pnl_usd": 0}
    return json.loads(STATE.read_text())


def read_events(n=400):
    if not EVENTS.exists():
        return []

    # Efficient tail-read: avoid loading entire events.jsonl into memory on each request.
    chunk_size = 64 * 1024
    data = b""
    with EVENTS.open("rb") as f:
        f.seek(0, 2)
        end = f.tell()
        pos = end
        needed_lines = n + 5
        while pos > 0 and data.count(b"\n") < needed_lines:
            read_size = min(chunk_size, pos)
            pos -= read_size
            f.seek(pos)
            data = f.read(read_size) + data

    lines = data.decode("utf-8", errors="ignore").splitlines()[-n:]
    out = []
    for ln in lines:
        try:
            out.append(json.loads(ln))
        except Exception:
            pass
    return out


def parse_ts(ts: str):
    try:
        return datetime.fromisoformat(ts)
    except Exception:
        return None


def api_stats(events):
    now = datetime.now(timezone.utc)
    one_hour = now - timedelta(hours=1)
    one_day = now - timedelta(days=1)

    usage = [e for e in events if e.get("type") == "api_usage"]
    ws_usage = [e for e in events if e.get("type") == "ws_usage"]
    total = sum(int(e.get("total_calls", 0)) for e in usage)

    hour_total = 0
    day_total = 0
    latest_data = None
    latest_ws_msg = None

    for e in usage:
        dt = parse_ts(e.get("ts", ""))
        if not dt:
            continue
        if dt > one_hour:
            hour_total += int(e.get("total_calls", 0))
        if dt > one_day:
            day_total += int(e.get("total_calls", 0))
        if latest_data is None or dt > latest_data:
            latest_data = dt

    for e in ws_usage:
        x = e.get("last_msg_ts")
        try:
            if x:
                dt = datetime.fromtimestamp(float(x), tz=timezone.utc)
                if latest_ws_msg is None or dt > latest_ws_msg:
                    latest_ws_msg = dt
                if latest_data is None or dt > latest_data:
                    latest_data = dt
        except Exception:
            pass

    latest_scan = None
    latest_scan_with_data = None
    latest_op_seen = None
    latest_groups = None
    latest_btc_tick = None
    latest_ticks = {}
    for e in reversed(events):
        if e.get("type") in ("opportunity_seen", "ws_opportunity_seen") and latest_op_seen is None:
            latest_op_seen = e
        if e.get("type") == "market_groups" and latest_groups is None:
            latest_groups = e
        if e.get("type") == "btc_price_tick" and latest_btc_tick is None:
            latest_btc_tick = e
        if e.get("type") == "ws_market_tick":
            mk = str(e.get("market_id", ""))
            if mk and mk not in latest_ticks:
                latest_ticks[mk] = e
                try:
                    dt = datetime.fromtimestamp(float(e.get("ws_ts", 0)), tz=timezone.utc)
                    if latest_data is None or dt > latest_data:
                        latest_data = dt
                except Exception:
                    pass
            continue
        if e.get("type") != "market_scan":
            continue
        if latest_scan is None:
            latest_scan = e
        top = e.get("top_candidates") or []
        has_asks = bool(top) and (top[0].get("best_ask_yes") is not None) and (top[0].get("best_ask_no") is not None)
        if has_asks and latest_scan_with_data is None:
            latest_scan_with_data = e

    if latest_scan_with_data is not None:
        latest_scan = latest_scan_with_data

    def _patch_rows(rows):
        out = []
        for row in rows or []:
            r = dict(row)
            t = latest_ticks.get(str(r.get("market_id", "")))
            if t:
                y = t.get("best_ask_yes")
                n = t.get("best_ask_no")
                if y is not None:
                    r["best_ask_yes"] = y
                if n is not None:
                    r["best_ask_no"] = n
                if (r.get("best_ask_yes") is not None) and (r.get("best_ask_no") is not None):
                    s = float(r["best_ask_yes"]) + float(r["best_ask_no"])
                    r["ask_sum_no_fees"] = s
                    fee_bump = 0.0035
                    r["ask_sum_with_fees"] = s + fee_bump
                    r["signal"] = "OPPORTUNITY" if s < 1.0 else ("WATCH" if s <= 1.01 else "NO_OPPORTUNITY")
            out.append(r)
        return out

    if latest_scan and (latest_scan.get("top_candidates") or []):
        latest_scan = {**latest_scan, "top_candidates": _patch_rows(latest_scan.get("top_candidates", []))}

    if latest_groups:
        latest_groups = {
            **latest_groups,
            "bitcoin": _patch_rows(latest_groups.get("bitcoin", [])),
            "secondary": _patch_rows(latest_groups.get("secondary", [])),
        }
        if latest_btc_tick:
            cl = latest_btc_tick.get("chainlink")
            bn = latest_btc_tick.get("binance")
            bpatched = []
            for r in (latest_groups.get("bitcoin") or []):
                rr = dict(r)
                if cl is not None:
                    rr["btc_current"] = round(float(cl), 2)
                if bn is not None:
                    rr["btc_current_binance"] = round(float(bn), 2)
                bpatched.append(rr)
            latest_groups["bitcoin"] = bpatched

    latest_ticks_by_market = {}
    for mk, t in latest_ticks.items():
        latest_ticks_by_market[mk] = {
            "best_ask_yes": t.get("best_ask_yes"),
            "best_ask_no": t.get("best_ask_no"),
            "best_bid_yes": t.get("best_bid_yes"),
            "best_bid_no": t.get("best_bid_no"),
            "ws_ts": t.get("ws_ts"),
        }

    return {
        "apiLink": "https://clob.polymarket.com/book",
        "latestDataTs": latest_data.isoformat() if latest_data else None,
        "latestWsTs": latest_ws_msg.isoformat() if latest_ws_msg else None,
        "totalCalls": total,
        "lastHourCalls": hour_total,
        "lastDayCalls": day_total,
        "latestScan": latest_scan,
        "latestGroups": latest_groups,
        "latestBtcTick": latest_btc_tick,
        "latestOpportunitySeen": latest_op_seen,
        "latestTicksByMarket": latest_ticks_by_market,
    }


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return

        if self.path == "/events":
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()

            last_blob = None
            try:
                while True:
                    events = read_events(5000)
                    state = read_state()
                    stats = api_stats(events)
                    blob = json.dumps({"state": state, "apiStats": stats, "serverTime": datetime.now(timezone.utc).isoformat()})
                    if blob != last_blob:
                        self.wfile.write(f"data: {blob}\n\n".encode())
                        self.wfile.flush()
                        last_blob = blob
                    time.sleep(0.5)
            except Exception:
                return

        events = read_events(5000)
        state = read_state()
        stats = api_stats(events)

        if self.path == "/json":
            payload = {
                "state": state,
                "apiStats": stats,
                "serverTime": datetime.now(timezone.utc).isoformat(),
                "events": events[-60:],
            }
            body = json.dumps(payload).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        request_path = self.path.split("?", 1)[0]
        if request_path == "/":
            request_path = "/index.html"

        safe_path = request_path.lstrip("/")
        file_path = (WEB / safe_path).resolve()
        if file_path.is_file() and WEB.resolve() in file_path.parents:
            body = file_path.read_bytes()
            ctype = mimetypes.guess_type(str(file_path))[0] or "application/octet-stream"
            self.send_response(200)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        self.send_response(404)
        self.end_headers()


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8787), Handler).serve_forever()
