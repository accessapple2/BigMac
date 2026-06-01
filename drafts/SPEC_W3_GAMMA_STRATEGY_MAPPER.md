# SPEC — Wave 3: Gamma → Strategy Mapper (DESIGN ONLY)
Status: design draft 2026-06-01. No build. Reads the CANONICAL GEX (engine/options_flow_gex via
/api/gex-snapshot) — the single source after HM-GEX-CANONICAL. Observation-only until validated.

## Inputs (canonical GEX, per underlying)
spot, total_gex (sign = regime), gamma_flip, call_wall, put_wall, per-strike net GEX. Daily snapshot
+ 15-min intraday cache.

## Mapping (regime → structure, strikes anchored to flip/walls)
- **Positive gamma, spot between put_wall and call_wall (pinned/stable):** → **iron condor** with
  short strikes AT the walls (put_wall / call_wall), longs beyond. Dealers defend the walls → range-bound.
- **Spot approaching call_wall from below (pos-gamma):** → **fade / short call spread** at the call
  wall (resistance; dealer selling into it). Short call ≈ call_wall, long above.
- **Negative gamma (spot below flip, trend-amplifying):** → **directional / debit** structures
  (long call/put or debit spread) in the trend direction; AVOID short-premium (dealers amplify moves).
- **Spot near gamma_flip:** transition zone — reduce size / no new condor (regime unstable).
- Strikes ALWAYS anchored to flip/walls (not fixed %), so the structure tracks the actual gamma map.

## Guards
- Only emit when GEX is fresh (W1 gate: gex source GREEN); skip on stale/RED.
- Min OI/liquidity at the chosen strikes (from the canonical per-strike data).
- Observation-only: emit as shadow signals (agent='shadow-gex:<structure>'), refused at the
  executor chokepoint, scored by W0 — same pattern as the deep_scan bridge.

## Validation
Each structure-type accrues forward; must clear DSR≥0.95 ∧ PBO≤0.3 before execution proposed.
