# Polymarket MVP (Paper-First)

Jarvis/Tib MVP scaffold for a practical Polymarket scanner + paper executor.

## Goals
- Detect executable opportunities (net of fees/slippage)
- Simulate realistic execution (latency + delayed/unmatched states)
- Enforce hard risk controls
- Produce daily operator metrics

## Current status
- ✅ Project scaffold
- ✅ Config + domain models
- ✅ Stub adapters (CLOB/Gamma)
- ✅ Opportunity scoring skeleton
- ✅ Paper execution/risk skeleton
- ⏳ Wiring + live dry-run loop

## Quickstart
```bash
cd polymarket-mvp
python3 -m venv .venv
source .venv/bin/activate
pip install -e .
python -m polymarket_mvp.main --config config/default.yaml --once
```

## Execution style (paper engine)
- Open: `execution.open_mode=limit_first` by default (with optional taker fallback).
- Close: `execution.close_mode=limit_first` by default; uses limit repricing, then fallback-to-taker on timeout or emergency reasons.
- Emergency reasons are configurable via `execution.close_force_taker_reasons`.

## Structure
- `src/polymarket_mvp/adapters` data adapters
- `src/polymarket_mvp/engine` scoring/opportunities
- `src/polymarket_mvp/sim` paper execution + ledger
- `src/polymarket_mvp/risk` hard limits and breakers
- `src/polymarket_mvp/api` optional status output

## Safety
- Paper mode only by default
- No private keys required for current scaffold

## Operating Charter
- Co-founder operating model and legal boundaries: `OPERATING_CHARTER.md`
- Principle: propose first, execute safely, keep TT in control

## Secret safety
- Keep credentials only in local `.env` / `config/local*.yaml` (gitignored).
- Run `scripts/secret_check.sh` before push (requires `gitleaks`).

## Live trading (gated)
- Keep `app.mode: paper` by default.
- To enable live routing: set `app.mode: live` and `live.enabled: true`.
- Install signer client locally: `pip install py-clob-client`.
- Required local env vars (never commit):
  - `POLYMARKET_PRIVATE_KEY`
  - optional `POLYMARKET_FUNDER`
  - optional API creds (`POLYMARKET_API_KEY`, `POLYMARKET_API_SECRET`, `POLYMARKET_API_PASSPHRASE`)
- Use `live.dry_run: true` first to validate order paths without sending real orders.
