# SPEC — Wave 3: Unusual-OI "Smart Money" (DESIGN ONLY)
Status: design draft 2026-06-01. No build. Within current Polygon tier (no new paid feed).

## Reality (STEP-0 truth, locked)
Polygon Options Starter: /v3/trades + /v3/quotes = 403. So PRINT-LEVEL flow (per-print premium,
sweep vs block, at-ask vs at-bid aggressor) is **NOT buildable** — that stays a tier-upgrade
DECISION for the Admiral, NOT a build. What IS real: the snapshot's `day.volume` + `open_interest`
per contract (already aggregated in flow_gex.db `flow_aggregates` / `unusual_contracts`).

## What this spec builds (real, tier-compliant)
A ranked unusual-options-activity signal from the aggregate:
- **unusual = day.volume / OI ≥ threshold** (vol>OI ⇒ likely opening) AND notional ≥ $250K
  (notional = volume × vwap × 100). Already computed in `compute_flow_aggregate`.
- **Rank by notional**; per-underlying net call/put notional lean (bullish/bearish).
- **Opening proxy** (vol>OI) flagged as ESTIMATED; true confirm = next-day OI delta (persist daily
  OI, diff). 
- Emit top-N as shadow signals (agent='shadow-oi:<lean>'), refused at executor, scored by W0.

## Explicitly NOT in scope (flag to Admiral)
- Sweep vs block, at-ask/at-bid aggressor, per-print premium → require /v3/trades + /v3/quotes
  (403). Decision: upgrade Polygon tier (cost) OR accept aggregate-only. HM-DARKPOOL (true dark
  prints) remains separately deferred.

## Validation
Accrues forward; DSR≥0.95 ∧ PBO≤0.3 before execution proposed. Aggregate-OI is a weaker signal
than print-level — expect it to need more sample to clear.
