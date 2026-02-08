import json
import time
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / "data" / "state.json"
EVENTS = ROOT / "data" / "events.jsonl"


def read_state():
    if not STATE.exists():
        return {"cash_usd": None, "positions": [], "realized_pnl_usd": 0}
    return json.loads(STATE.read_text())


def read_events(n=400):
    if not EVENTS.exists():
        return []
    lines = EVENTS.read_text().strip().splitlines()[-n:]
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
                    events = read_events(800)
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

        events = read_events(800)
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

        html = """
<!doctype html>
<html>
<head>
  <meta charset='utf-8' />
  <meta name='viewport' content='width=device-width, initial-scale=1' />
  <title>Polymarket MVP</title>
  <style>
    body { font-family: Inter, Arial, sans-serif; margin: 18px; background:#0f1115; color:#e8eaed; }
    .card { border:1px solid #2a2f3a; border-radius:12px; padding:14px; margin-bottom:14px; background:#151922; }
    .grid { display:grid; grid-template-columns: repeat(7, minmax(120px, 1fr)); gap:10px; align-items:end; }
    .api-link { grid-column: 7 / 8; text-align:right; overflow:hidden; }
    .api-link .v { font-size:12px; white-space:nowrap; overflow:hidden; text-overflow:ellipsis; display:block; }
    .k { color:#9aa4b2; font-size:12px; } .v { font-size:16px; font-weight:700; }
    table { width:100%; border-collapse:collapse; }
    th, td { border-bottom:1px solid #2a2f3a; padding:8px; font-size:12px; text-align:left; }
    .ok { color:#55d38a; font-weight:700; } .no { color:#ff6b6b; font-weight:700; } .watch { color:#ffd166; font-weight:700; }
    .changed { color:#55d38a; font-weight:700; transition: color 0.25s ease; }
  </style>
</head>
<body>
  <div class='card'>
    <h3 style='margin:0 0 12px 0;'>API Monitor</h3>
    <div class='grid'>
      <div><div class='k'>Server time</div><div class='v' id='serverTime'>-</div></div>
      <div><div class='k'>Latest data</div><div class='v' id='latestData'>-</div></div>
      <div><div class='k'>Latest WS tick</div><div class='v' id='latestWs'>-</div></div>
      <div><div class='k'>Calls total</div><div class='v' id='callsTotal'>0</div></div>
      <div><div class='k'>Calls last hour</div><div class='v' id='callsHour'>0</div></div>
      <div><div class='k'>Calls last day</div><div class='v' id='callsDay'>0</div></div>
      <div class='api-link'><div class='k'>API Link</div><div class='v' id='apiLink'>-</div></div>
    </div>
    <div style='margin-top:10px' class='k'>Cash: <span id='cash'></span> | Positions: <span id='positions'></span> | Realized PnL: <span id='pnl'></span></div>
  </div>

  <div class='card'>
    <h3 style='margin:0 0 12px 0;'>Bitcoin Group (primary)</h3>
    <table>
      <thead>
        <tr>
          <th>Market</th>
          <th>Target BTC</th>
          <th>BTC now (Chainlink)</th>
          <th>BTC now (Binance)</th>
          <th>TA Y/N</th>
          <th>LeadLag Y/N</th>
          <th>Regime Y/N</th>
          <th>Book Y/N</th>
          <th>Best Model</th>
          <th>Best Ask YES</th>
          <th>Best Ask NO</th>
          <th>Sum (no fees)</th>
          <th>Sum (with fees)</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody id='rows'></tbody>
    </table>
  </div>

  <div class='card'>
    <h3 style='margin:0 0 12px 0;'>Secondary Group (&lt; 7d to resolution)</h3>
    <div class='k' id='secondaryMeta'>Auto-selected by liquidity + spread quality.</div>
    <table>
      <thead>
        <tr>
          <th>Market</th>
          <th>Best Ask YES</th>
          <th>Best Ask NO</th>
          <th>Sum (no fees)</th>
          <th>Sum (with fees)</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody id='rows2'></tbody>
    </table>
  </div>

  <div class='card'>
    <h3 style='margin:0 0 12px 0;'>Positions Log (paper simulation)</h3>
    <div class='k' id='oppMeta'>No positions yet.</div>
    <table>
      <thead>
        <tr>
          <th>Server time opened</th>
          <th>Market</th>
          <th>Status</th>
          <th>Side</th>
          <th>Qty</th>
          <th>Notional ($)</th>
          <th>Entry</th>
          <th>Exit</th>
          <th>Model open</th>
          <th>Model close</th>
          <th>PnL ($)</th>
        </tr>
      </thead>
      <tbody id='oppRows'></tbody>
    </table>
  </div>

<script>
const prevRowValues = {};
const btcHistory = [];

function cellWithChange(curr, prev) {
  const txt = (curr ?? '')
  if (prev === undefined) return `<td>${txt}</td>`;
  const changed = String(curr ?? '') !== String(prev ?? '');
  return `<td class='${changed ? "changed" : ""}'>${txt}</td>`;
}

function _clamp(x, lo, hi) { return Math.max(lo, Math.min(hi, x)); }

function updateBtcSignal(topRows) {
  const now = Date.now() / 1000;
  const row = (topRows || [])[0] || {};
  const cl = Number(row.btc_current ?? NaN);
  const bi = Number(row.btc_current_binance ?? NaN);
  let p = NaN;
  if (Number.isFinite(cl) && Number.isFinite(bi)) p = 0.4 * cl + 0.6 * bi;
  else if (Number.isFinite(cl)) p = cl;
  else if (Number.isFinite(bi)) p = bi;

  if (Number.isFinite(p) && p > 0) btcHistory.push({ t: now, p, cl, bi });
  while (btcHistory.length && (now - btcHistory[0].t) > 700) btcHistory.shift();

  const priceAgo = (sec) => {
    for (let i = btcHistory.length - 1; i >= 0; i--) {
      if ((now - btcHistory[i].t) >= sec) return btcHistory[i].p;
    }
    return btcHistory.length ? btcHistory[0].p : NaN;
  };

  if (btcHistory.length < 5) return { pUp: 0.5, leadBps: 0.0 };

  const pNow = btcHistory[btcHistory.length - 1].p;
  const p20 = priceAgo(20);
  const p120 = priceAgo(120);
  const rf = (Number.isFinite(p20) && p20 > 0) ? Math.log(pNow / p20) : 0;
  const rs = (Number.isFinite(p120) && p120 > 0) ? Math.log(pNow / p120) : 0;

  // RSI-ish on ~30s
  const rsiWindow = btcHistory.filter(x => (now - x.t) <= 30);
  let up = 0, down = 0;
  for (let i = 1; i < rsiWindow.length; i++) {
    const d = rsiWindow[i].p - rsiWindow[i - 1].p;
    if (d > 0) up += d; else down += (-d);
  }
  const rsi = (up + down > 0) ? (100 * up / (up + down)) : 50;
  const rsiN = (rsi - 50) / 50;

  const volWindow = btcHistory.filter(x => (now - x.t) <= 60);
  const rets = [];
  for (let i = 1; i < volWindow.length; i++) {
    const a = volWindow[i - 1].p, b = volWindow[i].p;
    if (a > 0 && b > 0) rets.push(Math.log(b / a));
  }
  const mean = rets.length ? rets.reduce((a, b) => a + b, 0) / rets.length : 0;
  const sigma = rets.length ? Math.sqrt(rets.reduce((a, x) => a + (x - mean) ** 2, 0) / rets.length) : 0.0001;

  let lead = 0;
  const last = btcHistory[btcHistory.length - 1];
  if (Number.isFinite(last.cl) && last.cl > 0 && Number.isFinite(last.bi)) {
    lead = (last.bi - last.cl) / last.cl;
  }
  const leadBps = lead * 10000;

  const S = 1.8 * rf + 1.2 * rs + 0.6 * rsiN + 0.8 * lead;
  const denom = 2.5 * Math.max(sigma, 0.00008);
  const z = _clamp(S / Math.max(denom, 1e-6), -8, 8);
  const pUp = 1 / (1 + Math.exp(-z));
  return { pUp, leadBps, rf, rs, sigma, rsiN };
}

function _bidsFromP(row, pUp, margin=0.006) {
  const ay = Number(row.best_ask_yes ?? NaN);
  const an = Number(row.best_ask_no ?? NaN);
  const by = Number(row.best_bid_yes ?? NaN);
  const bn = Number(row.best_bid_no ?? NaN);
  if (!Number.isFinite(ay) || !Number.isFinite(an)) return { yes:null, no:null };
  let jy = pUp - margin;
  let jn = (1 - pUp) - margin;
  if (Number.isFinite(by) && by > 0) jy = Math.max(by, jy);
  if (Number.isFinite(bn) && bn > 0) jn = Math.max(bn, jn);
  jy = _clamp(jy, 0, ay);
  jn = _clamp(jn, 0, an);
  return { yes: Math.round(jy*10000)/10000, no: Math.round(jn*10000)/10000 };
}

function computeJarvisBids(row, signal) {
  const ay = Number(row.best_ask_yes ?? NaN);
  const an = Number(row.best_ask_no ?? NaN);
  const by = Number(row.best_bid_yes ?? NaN);
  const bn = Number(row.best_bid_no ?? NaN);
  if (!Number.isFinite(ay) || !Number.isFinite(an)) return { yes: null, no: null, lead: 0 };

  const pUp = _clamp(Number(signal?.pUp ?? 0.5), 0.01, 0.99);
  const margin = 0.006;
  let jy = pUp - margin;
  let jn = (1 - pUp) - margin;

  if (Number.isFinite(by) && by > 0) jy = Math.max(by, jy);
  if (Number.isFinite(bn) && bn > 0) jn = Math.max(bn, jn);

  jy = _clamp(jy, 0, ay);
  jn = _clamp(jn, 0, an);
  return {
    yes: Math.round(jy * 10000) / 10000,
    no: Math.round(jn * 10000) / 10000,
    lead: Math.round(Number(signal?.leadBps ?? 0) * 100) / 100,
  };
}

function computeLeadLagBids(row, signal) {
  const z = _clamp((Number(signal?.leadBps ?? 0))/35.0, -1.5, 1.5);
  const p = _clamp(0.5 + 0.18*z, 0.02, 0.98);
  return _bidsFromP(row, p, 0.006);
}

function computeRegimeBids(row, signal) {
  const trend = Math.abs(Number(signal?.rf ?? 0)) + Math.abs(Number(signal?.rs ?? 0));
  const chop = Number(signal?.sigma ?? 0);
  const wTrend = _clamp(trend / Math.max(trend + chop, 1e-6), 0.1, 0.9);
  const pTrend = _clamp(Number(signal?.pUp ?? 0.5), 0.02, 0.98);
  const pMr = _clamp(0.5 - 0.35*Number(signal?.rsiN ?? 0), 0.02, 0.98);
  const p = _clamp(wTrend*pTrend + (1-wTrend)*pMr, 0.02, 0.98);
  return _bidsFromP(row, p, 0.0065);
}

function computeBookBids(row) {
  const ay = Number(row.best_ask_yes ?? NaN), an = Number(row.best_ask_no ?? NaN);
  const by = Number(row.best_bid_yes ?? NaN), bn = Number(row.best_bid_no ?? NaN);
  if (!Number.isFinite(ay) || !Number.isFinite(an)) return {yes:null,no:null};
  const sy = Number.isFinite(by) ? Math.max(0, ay - by) : 0.01;
  const sn = Number.isFinite(bn) ? Math.max(0, an - bn) : 0.01;
  const p = _clamp(0.5 + 0.12*(sn - sy), 0.02, 0.98);
  return _bidsFromP(row, p, 0.006);
}

function modelSummary(models) {
  const entries = Object.entries(models).map(([k,v]) => ({k, yes:Number(v?.yes ?? NaN), no:Number(v?.no ?? NaN)}))
    .filter(m => Number.isFinite(m.yes) && Number.isFinite(m.no));
  if (!entries.length) return {label:'-', score:0};
  const scored = entries.map(m => {
    const dir = m.yes >= m.no ? 'UP' : 'DOWN';
    const strength = Math.abs(m.yes - m.no);
    return {...m, dir, strength};
  });
  scored.sort((a,b) => b.strength - a.strength);
  const best = scored[0];
  const agree = scored.filter(x => x.dir === best.dir).length / scored.length;
  const conf = Math.round(_clamp((0.6*best.strength + 0.4*agree)*100, 1, 99));
  return { label: `${best.k}:${best.dir} ${conf}%`, score: conf };
}

function render(d) {
  const s = d.apiStats || {};
  document.getElementById('serverTime').textContent = d.serverTime ? new Date(d.serverTime).toLocaleTimeString() : '-';
  document.getElementById('apiLink').textContent = s.apiLink || '-';
  document.getElementById('latestData').textContent = s.latestDataTs ? new Date(s.latestDataTs).toLocaleString() : '-';
  document.getElementById('latestWs').textContent = s.latestWsTs ? new Date(s.latestWsTs).toLocaleTimeString() : '-';
  document.getElementById('callsTotal').textContent = s.totalCalls ?? 0;
  document.getElementById('callsHour').textContent = s.lastHourCalls ?? 0;
  document.getElementById('callsDay').textContent = s.lastDayCalls ?? 0;

  const st = d.state || {};
  document.getElementById('cash').textContent = st.cash_usd ?? '-';
  document.getElementById('positions').textContent = (st.positions || []).length;
  document.getElementById('pnl').textContent = st.realized_pnl_usd ?? '-';

  const rows = document.getElementById('rows');
  const rows2 = document.getElementById('rows2');
  const secondaryMeta = document.getElementById('secondaryMeta');
  rows.innerHTML = '';
  rows2.innerHTML = '';

  const groups = s.latestGroups || {};
  const hasGroups = Array.isArray(groups.bitcoin) || Array.isArray(groups.secondary);
  const top = hasGroups ? (groups.bitcoin || []) : [];
  const secondary = hasGroups ? (groups.secondary || []) : [];
  const btcSignal = updateBtcSignal(top);
  const priceByMarket = {};
  for (const r of top) {
    const mid = String(r.market_id || '');
    if (!mid) continue;
    const y = Number(r.best_ask_yes ?? NaN);
    const n = Number(r.best_ask_no ?? NaN);
    if (Number.isFinite(y) && Number.isFinite(n)) {
      priceByMarket[mid] = { yes: y, no: n };
    }
  }
  if (secondaryMeta) {
    secondaryMeta.textContent = hasGroups
      ? (groups.secondary_note || 'Auto-selected by liquidity + spread quality.')
      : 'Waiting for grouped market snapshot...';
  }

  for (const x of top) {
    const tr = document.createElement('tr');
    const sig = x.signal || '';
    const cls = sig === 'OPPORTUNITY' ? 'ok' : (sig === 'WATCH' ? 'watch' : 'no');
    const mk = String(x.market_id || x.market_name || '');
    const prev = prevRowValues[mk] || {};
    const jb = computeJarvisBids(x, btcSignal);
    const ll = computeLeadLagBids(x, btcSignal);
    const rg = computeRegimeBids(x, btcSignal);
    const bk = computeBookBids(x);
    const ms = modelSummary({ TA: jb, LL: ll, RG: rg, BK: bk });

    tr.innerHTML = `
      <td>${x.market_name || x.market_id || ''}</td>
      <td title='${x.btc_price_source || ''}'>${x.btc_target ?? ''}</td>
      <td>${x.btc_current ?? ''}</td>
      <td>${x.btc_current_binance ?? ''}</td>
      ${cellWithChange(`${jb.yes ?? ''}/${jb.no ?? ''}`, prev.model_ta)}
      ${cellWithChange(`${ll.yes ?? ''}/${ll.no ?? ''}`, prev.model_ll)}
      ${cellWithChange(`${rg.yes ?? ''}/${rg.no ?? ''}`, prev.model_rg)}
      ${cellWithChange(`${bk.yes ?? ''}/${bk.no ?? ''}`, prev.model_bk)}
      ${cellWithChange(ms.label, prev.best_model)}
      ${cellWithChange(x.best_ask_yes, prev.best_ask_yes)}
      ${cellWithChange(x.best_ask_no, prev.best_ask_no)}
      ${cellWithChange(x.ask_sum_no_fees, prev.ask_sum_no_fees)}
      ${cellWithChange(x.ask_sum_with_fees, prev.ask_sum_with_fees)}
      <td class='${cls}'>${sig}</td>
    `;
    rows.appendChild(tr);

    prevRowValues[mk] = {
      model_ta: `${jb.yes ?? ''}/${jb.no ?? ''}`,
      model_ll: `${ll.yes ?? ''}/${ll.no ?? ''}`,
      model_rg: `${rg.yes ?? ''}/${rg.no ?? ''}`,
      model_bk: `${bk.yes ?? ''}/${bk.no ?? ''}`,
      best_model: ms.label,
      best_ask_yes: x.best_ask_yes,
      best_ask_no: x.best_ask_no,
      ask_sum_no_fees: x.ask_sum_no_fees,
      ask_sum_with_fees: x.ask_sum_with_fees,
    };
  }

  for (const x of secondary) {
    const tr = document.createElement('tr');
    const sig = x.signal || '';
    const cls = sig === 'OPPORTUNITY' ? 'ok' : (sig === 'WATCH' ? 'watch' : 'no');
    const mk = `secondary:${String(x.market_id || x.market_name || '')}`;
    const prev = prevRowValues[mk] || {};
    tr.innerHTML = `
      <td>${x.market_name || x.market_id || ''}</td>
      ${cellWithChange(x.best_ask_yes, prev.best_ask_yes)}
      ${cellWithChange(x.best_ask_no, prev.best_ask_no)}
      ${cellWithChange(x.ask_sum_no_fees, prev.ask_sum_no_fees)}
      ${cellWithChange(x.ask_sum_with_fees, prev.ask_sum_with_fees)}
      <td class='${cls}'>${sig}</td>
    `;
    rows2.appendChild(tr);
    prevRowValues[mk] = {
      best_ask_yes: x.best_ask_yes,
      best_ask_no: x.best_ask_no,
      ask_sum_no_fees: x.ask_sum_no_fees,
      ask_sum_with_fees: x.ask_sum_with_fees,
    };
  }

  const oppRows = document.getElementById('oppRows');
  const oppMeta = document.getElementById('oppMeta');
  oppRows.innerHTML = '';
  const openPos = (st.positions || []).filter(p => p.status === 'open');
  const closedPos = (st.closed_positions || []).slice(-20).reverse();
  const items = [...openPos, ...closedPos];
  if (!items.length) {
    oppMeta.textContent = 'No positions yet.';
  } else {
    oppMeta.textContent = `Open: ${openPos.length} | Closed: ${(st.closed_positions || []).length}`;
    for (const x of items) {
      const tr = document.createElement('tr');
      const statusCls = x.status === 'open' ? 'watch' : 'ok';
      const qty = Number(x.qty ?? NaN);
      const notional = Number(x.size_usd ?? NaN);
      const entry = Number(x.entry_price ?? NaN);
      let exit = Number(x.exit_price ?? NaN);
      if (!Number.isFinite(exit) && x.status === 'open') {
        const pm = priceByMarket[String(x.market_id || '')] || {};
        exit = (x.side === 'BUY_YES') ? Number(pm.yes ?? NaN) : Number(pm.no ?? NaN);
      }
      let pnl = Number(x.pnl_usd ?? NaN);
      if (!Number.isFinite(pnl) && Number.isFinite(qty) && Number.isFinite(exit) && Number.isFinite(notional)) {
        pnl = (qty * exit) - notional;
      }
      const pnlCls = Number.isFinite(pnl) ? (pnl >= 0 ? 'ok' : 'no') : '';
      tr.innerHTML = `
        <td>${x.opened_at ? new Date(x.opened_at).toLocaleString() : ''}</td>
        <td>${x.market_name || x.market_id || ''}</td>
        <td class='${statusCls}'>${x.status || ''}</td>
        <td>${x.side || ''}</td>
        <td>${Number.isFinite(qty) ? qty.toFixed(4) : ''}</td>
        <td>${Number.isFinite(notional) ? notional.toFixed(2) : ''}</td>
        <td>${Number.isFinite(entry) ? entry.toFixed(4) : ''}</td>
        <td>${Number.isFinite(exit) ? exit.toFixed(4) : ''}</td>
        <td>${x.model || ''}</td>
        <td>${x.close_model || ''}</td>
        <td class='${pnlCls}'>${Number.isFinite(pnl) ? pnl.toFixed(2) : ''}</td>
      `;
      oppRows.appendChild(tr);
    }
  }
}

async function bootstrap() {
  const r = await fetch('/json', {cache:'no-store'});
  const d = await r.json();
  render(d);
}

function startSSE() {
  const es = new EventSource('/events');
  es.onmessage = (ev) => {
    try { render(JSON.parse(ev.data)); } catch (_) {}
  };
  es.onerror = () => {
    es.close();
    setTimeout(startSSE, 1000);
  };
}

bootstrap().then(startSSE).catch(() => setTimeout(() => { bootstrap().then(startSSE); }, 1000));
</script>
</body>
</html>
"""
        body = html.encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8787), Handler).serve_forever()
