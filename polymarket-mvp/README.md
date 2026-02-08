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
