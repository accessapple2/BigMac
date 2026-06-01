# Spec summaries for green-light — W2 / W3 / W3 / W4 (2026-06-01)

One paragraph each. All are DESIGN-ONLY drafts; observation-first; gated behind the W0 validation
gate (DSR≥0.95 ∧ PBO≤0.30). Pick which to build. (Reminder from today: relative_strength's PBO leg
FAILS at 0.48 — so the gate is not yet cleared for the lead setup; W2's post-graduation sizing and
W4's routing both depend on at least one setup graduating.)

## W2 — Bracket Sizing (`SPEC_W2_BRACKET_SIZING.md`)
Attaches a risk-sized bracket (entry/stop/target + position size) to each signal so a graduated
setup can be sized sanely. Phase-1 is observation-only: compute + log the size on shadow signals and
score whether sizing improves realized R in W0 — it NEVER routes to execution. The ladder is
fixed-fractional (0.5–1.0% of equity per trade, shares = risk$/|entry−stop|, stop from
deep_scan.stop_price) pre-graduation, escalating to ≤0.25× fractional-Kelly off a setup's W0
expectancy only AFTER it clears the gate. **Build value:** low-risk, immediately useful as
observation (does sizing add R?), and it's the prerequisite for ever sizing a live trade. **Dependency:**
the Kelly leg needs a graduated setup (blocked by the PBO fail); the fixed-fractional observation
leg can start now. **Recommend: build the observation leg first — cheapest, unblocked.**

## W3 — Gamma → Strategy Mapper (`SPEC_W3_GAMMA_STRATEGY_MAPPER.md`)
Maps the canonical GEX regime (engine/options_flow_gex via /api/gex-snapshot — the single post-
consolidation source) to an options structure with strikes anchored to flip/walls: positive-gamma
pinned between walls → iron condor short at the walls; spot approaching call_wall → fade/short-call-
spread at resistance; negative-gamma (below flip) → directional/debit structures. Observation-only
until each structure-type accrues its own DSR/PBO. **Build value:** turns the GEX we already compute
into concrete structure proposals; natural shadow-scoring target. **Dependency:** canonical GEX
(shipped) + the validation gate per structure-type (each accrues from zero). **Recommend: build as
shadow proposer (logs structures, scores forward) — no execution; complements W3-OI.**

## W3 — Unusual-OI "Smart Money" (`SPEC_W3_UNUSUAL_OI_SMART_MONEY.md`)
A ranked unusual-options-activity signal built ONLY from data we already have under the current
Polygon tier: per-contract day.volume + open_interest in flow_gex.db (`flow_aggregates`/
`unusual_contracts`). unusual = volume/OI ≥ threshold (vol>OI ⇒ likely opening) AND notional ≥ $250K;
ranked by notional with a per-underlying net call/put lean. **Explicitly NOT in scope:** print-level
flow (per-print premium, sweep-vs-block, at-ask aggressor) — Polygon Starter returns 403 on
/v3/trades+/v3/quotes, so that's a tier-upgrade DECISION for the Admiral, not a build. **Build value:**
real, tier-compliant, cheap (data already collected); a new observation signal that can feed W0.
**Dependency:** none beyond the existing flow_gex collector. **Recommend: build — lowest external
risk of the four, no new feed, no execution.**

## W4 — Regime-Conditional Routing (`SPEC_W4_REGIME_CONDITIONAL_ROUTING.md`)
The capstone: slice W0 expectancy by a 3-axis live regime vector (gamma sign + position vs
flip/walls; VIX term-structure contango/backwardation; time-of-day) and a router that surfaces a
setup ONLY when its regime-conditional expectancy is positive AND the setup has graduated — otherwise
suppress (don't trade an edge outside its regime). **Build value:** highest — it's how edges get
deployed safely — but also highest dependency and the most selection risk: bucketing by regime
shrinks n per cell, so deflation/PBO matters MORE here (more "trials"). **Dependency:** W0 (shipped) +
canonical GEX (shipped) + VIX term-structure + at least one graduated setup. **Recommend: build LAST,
after ≥1 setup graduates and W2/W3 observation data exists — premature without a graduated edge to route.**

## Suggested build order (if approving a subset)
1. **W3 Unusual-OI** — unblocked, no new feed, pure observation. Safest first build.
2. **W2 fixed-fractional observation leg** — unblocked, answers "does sizing add R?".
3. **W3 Gamma Mapper** (shadow proposer) — uses shipped canonical GEX.
4. **W4 routing** — LAST; needs a graduated setup + VIX term-structure + W2/W3 data.
(All stay observation-only; nothing routes to the executor until the gate clears AND explicit go.)
