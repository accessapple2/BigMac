# SPEC — Wave 4: Regime-Conditional Routing (DESIGN ONLY)
Status: design draft 2026-06-01. No build. The capstone: only surface setups with edge in the
CURRENT regime. Depends on W0 (expectancy) + canonical GEX + validation gate.

## Core idea
W0 expectancy is currently sliced by setup × horizon. Wave 4 adds REGIME slicing: expectancy
conditioned on the live regime vector, and a router that surfaces a setup ONLY when its
conditional expectancy (in this regime) is positive AND graduated.

## Regime vector (3 axes)
1. **Gamma sign** (canonical GEX total_gex sign: long-gamma/stable vs short-gamma/trend) — and
   position vs flip/walls.
2. **VIX term-structure** (contango vs backwardation; spot VIX vs VIX3M) — risk-on vs stress.
3. **Time-of-day** (open / midday / power-hour / overnight-hold) — many edges are session-specific
   (W0 already showed premarket_gap is a 1-3d edge that decays).

## Routing table
For each (setup, horizon): store conditional expectancy_R per regime-bucket (gamma×VIX×ToD).
At decision time: compute the live regime vector → look up the setup's conditional expectancy in
that bucket → surface ONLY if (a) bucket expectancy_R > 0, (b) bucket sample ≥ threshold, (c) the
setup has GRADUATED (DSR/PBO). Otherwise suppress (don't trade an edge outside its regime).

## Why this matters (W0 evidence)
W0 already proved regime-dependence: Grade-B SELLs were 80% WR on SPY-up vs 40% on SPY-down;
premarket_gap edge is horizon/session-specific. A flat expectancy hides this. Regime-conditioning
is how you stop trading a setup in the regime where it bleeds.

## Build order (later)
Needs: enough W0 forward sample PER regime-bucket (sparse — buckets fragment the data). Start by
LOGGING the regime vector on every shadow signal now (cheap, observation-only) so the conditional
table accrues; build the router once buckets have ≥ threshold closed.

## Validation
Per (setup × regime-bucket) must clear DSR≥0.95 ∧ PBO≤0.3 — and bucketing makes n smaller, so
deflation is even more important here (more "trials" = more selection risk). PBO especially.
