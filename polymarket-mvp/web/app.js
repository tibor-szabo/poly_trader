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
      ${cellWithChange(x.p_yes_model, prev.p_yes_model)}
      ${cellWithChange(x.p_hit_target, prev.p_hit_target)}
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
      p_yes_model: x.p_yes_model,
      p_hit_target: x.p_hit_target,
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
  const edgeQaMeta = document.getElementById('edgeQaMeta');
  oppRows.innerHTML = '';
  const openPos = (st.positions || []).filter(p => p.status === 'open');
  const closedPos = (st.closed_positions || []).slice(-20).reverse();
  const items = [...openPos, ...closedPos];
  if (!items.length) {
    oppMeta.textContent = 'No positions yet.';
    if (edgeQaMeta) edgeQaMeta.textContent = '';
  } else {
    oppMeta.textContent = `Open: ${openPos.length} | Closed: ${(st.closed_positions || []).length}`;
    const cls = st.closed_positions || [];
    if (edgeQaMeta && cls.length) {
      const n = cls.length;
      const wins = cls.filter(x => Number(x.pnl_usd || 0) > 0).length;
      const avgPnl = cls.reduce((a,x)=>a+Number(x.pnl_usd||0),0)/Math.max(1,n);
      const withEdge = cls.filter(x => Number.isFinite(Number(x.edge_entry)));
      const avgEdge = withEdge.length ? (withEdge.reduce((a,x)=>a+Number(x.edge_entry||0),0)/withEdge.length) : NaN;
      edgeQaMeta.textContent = `Edge QA: trades=${n} winrate=${Math.round((wins/n)*100)}% avgPnL=${avgPnl.toFixed(2)} avgOpenEdge=${Number.isFinite(avgEdge)?avgEdge.toFixed(4):'-'}`;
    }

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
