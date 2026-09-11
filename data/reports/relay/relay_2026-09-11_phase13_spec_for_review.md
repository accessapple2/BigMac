# Relay — Phase 1.3 spec, posted for review. NOT BUILT. 2026-09-11

Per your request: the spec as currently written in `docs/XO_PLAN_2026-09.md`
(lines 146–339), reproduced verbatim below — not summarized, not built.
Nothing in this document is new; it's the same text that's live in the
plan file right now, including the already-corrected 42-trade/71.4%
calibration baseline. Posting it here purely so it can be read in full,
in one place, without opening the plan doc.

**Quick map, since you asked specifically about these four things — all
present in the verbatim text below, not added here:**
- **The gate chain it targets**: `paper_trader.py::buy()`'s real chain,
  confirmed by the 30-day funnel — see the numbered list (items 1–6)
  under "The real gate chain."
- **Where sizing hooks into `buy()`**: the existing, currently-McCoy-
  unused `sizing_multiplier: float = 1.0` kwarg — see "Alpha-scaled
  sizing tier."
- **The fail-closed rule**: see "Confidence input to sizing — fails
  CLOSED on missing calibration data" — sizing (not
  `get_calibrated_confidence()` itself, which stays fail-open) sizes at
  base allocation, never the full 0.6+ tier, when a (regime, confidence-
  bucket) pair has no calibration evidence.
- **What it does NOT change**: explicitly called out inline — items 5–6
  of the gate chain (regime router, bench rating) are untouched; the six
  non-confidence `model_adjustments` types are untouched; `get_calibrated_
  confidence()`'s own fail-open behavior is unchanged (only what sizing
  does with a missing-evidence result changes); the 2026-09-10 Gate-7
  draft is explicitly superseded, not merged with this one.

---

## Verbatim, `docs/XO_PLAN_2026-09.md` lines 146–339

- **1.3 — ATR-scaled sizing in the gate, PLUS sizing/gate work re-scoped
  around McCoy's REAL gate chain (spec v2, 2026-09-11 — supersedes the
  2026-09-10 Gate-7-based draft below, still SPEC ONLY, not built).**

  **Why v2 exists:** the 2026-09-10 draft (kept below, struck through in
  spirit not in text, for history) targeted restoring `engine/
  crew_scanner.py` Gate 7 (`SNIPER_ALPHA_THRESHOLD`/`SNIPER_MIN_CONFIDENCE`).
  **A full trace on 2026-09-11 found Gate 7 governs a path
  (`_scan_single_agent()` via `SCAN_PAIRS`) that produced 272 decisions
  and 0 executed trades for McCoy in the 30 days before it went dormant
  2026-09-09 — not McCoy's real trading.** McCoy's actual trades flow
  through the Arena path (`ai_brain.py` -> `paper_trader.buy()`) and hit a
  completely different gate chain that Gate 7 never touches. Full trace:
  `data/reports/relay/relay_2026-09-11_mccoy_pipeline_trace_and_funnel.md`.
  This is the sixth premise this week that turned out wrong on inspection
  — the dry-dock halted for it, traced it, and this spec is the result.

  **ATR term (original scope, unchanged):** Today's stop/size math
  (`engine/stops.py`) is conviction-scaled but not volatility-scaled — two
  names at the same confidence get the same stop % regardless of how much
  they actually move. Adds an ATR term to the sizing gate so position size
  reflects the stock's own volatility, not just McCoy's confidence.

  **The real gate chain (`paper_trader.py::buy()`, confirmed by a 30-day
  funnel pull — 9,862 BUY signals -> 2 execution attempts -> 1 real
  trade):**
  1. `[HM-MARKET-CLOSED]` (before/after-hours/weekend) — 31.2% of all
     rejects. **Already fixed by Phase 1.2's cadence cut** (McCoy now
     only fires at 9:35 AM / 12:30 PM ET, both inside market hours) — this
     category should collapse to near-zero once 1.2 is live.
  2. `stale_signal` — 20.7% of rejects, the single largest remaining
     category. Not addressed by 1.2 or this spec; worth its own look
     before or alongside 1.3 if it's still large post-1.2.
  3. `engine.risk_manager.UNIVERSAL_MIN_CONVICTION` (currently **0.65**,
     `BEAR_MIN_CONVICTION=0.80` in bear regimes) — the REAL live
     confidence floor, not Gate 7's dormant 55. 11.4%+ of rejects
     (`LOW_CONVICTION: NN% below 65% minimum`, several buckets). **This is
     the correct target for the Sniper spec's `confidence >= 70%` intent**
     — raising `UNIVERSAL_MIN_CONVICTION` from 0.65 to 0.70 is the actual
     restoration, not touching Gate 7's constants.
  4. `engine.learning_engine.apply_learning()` — 11.1% of rejects
     ("Blocked by learning engine"). Real, live, currently applying a
     **flat `confidence_modifier=0.8` to every McCoy stated confidence**
     (frozen since 2026-07-08 — McCoy fell out of this pipeline's
     coverage 2026-07-09/13, see `docs/XO_BACKLOG.md`; root cause not yet
     found). Also enforces a regime filter, position-size cap, stop-loss
     override, a 3-losses-in-14-days ticker blacklist, and a cooldown —
     all still live for McCoy today. **See the coordination question
     below before touching this.**
  5. Regime router (`regime_mismatch` 9.4% + `REGIME-ROUTER: long_equity
     not approved in BEAR_CROSS` 5.4% = 14.8% combined) — already live,
     not part of this spec's changes.
  6. Bench rating (`BENCH: rating D (40/100)`, 3.0%) — already live, not
     part of this spec's changes.

  **Coordination question — DECIDED 2026-09-11 (Admiral). Spec below,
  NOT built yet.** `learning_engine.apply_learning()`'s `confidence_modifier`
  and `engine.calibration_map.get_calibrated_confidence()` are two
  independent confidence-adjustment mechanisms that don't know about each
  other today. **Decision: one adjustment, one owner, evidence-based.
  `learning_engine` keeps owning the actual gate/block/multiply
  enforcement (`apply_learning()` stays the single call site every
  decision passes through); `calibration_map` becomes its evidence
  source, replacing the LLM-guessed weekly number.** Concretely:

  1. **`model_scores.confidence_calibration` (currently `0.0` for every
     player, every row — see the 2026-09-11 pipeline-trace relay doc for
     why: `weekly_tuning_crew.py` prompts an LLM for it and the value
     never lands) gets populated by `calibration_map` instead.** A daily
     job (natural home: alongside `_flag_no_trade_active_players()`,
     `engine/crew/daily_review_crew.py`, same file just fixed for the
     coverage gap) computes, per active player with enough data, a
     summary score from `calibration_map.fit_binned_calibration()`'s
     output — proposed reduction: `100 - avg(abs(stated - realized) * 100
     across buckets with n >= MIN_BUCKET_N)`, i.e. 100 = perfectly
     calibrated, lower = more overconfident/underconfident. Exact
     reduction formula needs one more look before building (this is the
     one open detail in this spec), but the source is decided: empirical,
     from real hit-rate data, not an LLM guess.
  2. **`learning_engine.apply_learning()`'s step 1 ("Confidence modifier",
     `engine/learning_engine.py` lines 66-75) stops reading
     `model_adjustments` for `adjustment_type='confidence_modifier'` and
     reads the player's latest `model_scores.confidence_calibration`
     instead**, deriving a multiplier from it (proposed:
     `multiplier = confidence_calibration / 100.0` — a fully-calibrated
     player gets no adjustment, a badly-calibrated one gets shaved
     proportionally to how badly). Also needs one confirmation pass on
     the exact transform before building, but the data source and call
     site are decided.
  3. **`weekly_tuning_crew.py`'s LLM-prompted write of
     `confidence_calibration` (and any write to `model_adjustments` with
     `adjustment_type='confidence_modifier'`) is retired** — this is the
     "one owner" part; two things no longer both write the same signal.
     The other six `model_adjustments` types (`regime_filter`,
     `position_size`, `stop_loss`, ticker blacklist, `ghost_promotion_
     override`, `cooldown`) are untouched — those aren't confidence
     adjustments and don't conflict with this change.
  4. **Sizing (below) reads `calibration_map.get_calibrated_confidence()`
     directly, fail-closed, as already speced** — that path is unchanged
     by this decision; it's the *gate*-side confidence adjustment
     (`learning_engine`, fail-open) that's being re-sourced here, not the
     sizing-side one, and the two were already meant to be different
     (fail-open vs fail-closed) before this decision.

  Do not build any of this until `calibration_map` has materially more
  live data than it has today (see "First real calibration read,
  corrected" below) — and note that mechanism is fleet-wide with a
  `decision_audit.trade_fire`+`trade_id`+settled-`pnl` join requirement
  that currently sees only 3 rows *total, across every player combined*
  — waiting for it to accrue McCoy-specific data at any real rate is not
  a sound plan as that mechanism is designed today (see `docs/XO_
  BACKLOG.md`'s "calibration_map.py's trade_fire→trade_id join is too
  narrow" item). A summary score computed from one populated bucket and
  four empty ones is not evidence-based in the way this decision intends.

  **Alpha-scaled sizing tier (original April spec, restated) — wires into
  `paper_trader.py::buy()`'s existing, currently-unused-for-McCoy
  `sizing_multiplier: float = 1.0` kwarg** (confirmed 2026-09-11: the
  only live caller passing it explicitly today is `crew_scanner.py`'s
  `troi_caution_multiplier`; the Arena/McCoy path always calls `buy()`
  at the default 1.0). `composite_alpha >= 0.6` -> `sizing_multiplier=1.0`
  (full), `0.3–0.6` -> `sizing_multiplier=0.5` (half).

  **Confidence input to sizing — fails CLOSED on missing calibration data
  (unchanged principle from the 2026-09-10 draft):**
  `engine.calibration_map.get_calibrated_confidence()` deliberately fails
  OPEN (returns the raw stated confidence unchanged) when a regime/bucket
  has fewer than `MIN_BUCKET_N` observations — correct for a pass/fail
  gate, where "no evidence either way" shouldn't block a trade outright.
  It is **wrong for sizing**: letting unearned, uncalibrated confidence set
  position size in exactly the range with zero supporting data would let
  the sizing tier be driven by a number nobody has verified. Phase 1.3's
  sizing logic (not `get_calibrated_confidence()` itself, which stays
  fail-open) must apply its own rule on top: **when the relevant (regime,
  confidence-bucket) has no calibration evidence, size at the base
  allocation — never the 0.6+ full tier — regardless of how high the raw
  stated confidence is.**

  **Should the gate chain be loosened? No — measured, not assumed
  (2026-09-11).** Top 300 highest-confidence REJECTED McCoy BUYs, last 30
  days, forward return to next close (yfinance, close-to-close): mean
  +0.171%, **median -0.212%, win rate 45.8%** — at or slightly below a
  same-period SPY baseline (mean +0.034%, median -0.073%, win rate
  47.7%) on the two outlier-resistant metrics. Not the profile of a
  systematically profitable population being wrongly blocked. **This
  spec does not loosen any threshold** — `UNIVERSAL_MIN_CONVICTION`
  0.65->0.70 is a tightening, matching the original Sniper intent, and is
  supported by this measurement, not contradicted by it. Full detail:
  `data/reports/relay/relay_2026-09-11_forward_return_and_learning_engine.md`.

  **First real calibration read — WITHDRAWN AND CORRECTED 2026-09-11.**
  The original entry here (2026-09-10) cited "169 clean, non-placeholder
  McCoy trades... 78.6% hit rate... `scripts/print_calibration_curve.py`"
  as its source. **That citation is wrong and the number does not
  reproduce.** `print_calibration_curve.py` is a thin wrapper around
  `engine.calibration_map.load_calibration_data()` — a fleet-wide (no
  player filter), `decision_audit.trade_fire`-linked query that has never
  been capable of producing 169 rows for any single player: run live
  2026-09-11, it returns **3 rows total, across every player combined**.
  Whatever produced "169" on 2026-09-10 was not this function.

  A same-day reconciliation attempt (`relay_2026-09-11_B9_calibration_
  reconciliation.md`) tried to independently verify the number with a
  direct `trades.confidence`/`trades.realized_pnl` query and got 168/78.6%
  — close enough to 169/78.6% that it was reported as confirming the
  original finding. **That reconciliation was itself wrong**: the query
  had no `player_id='ollama-plutus'` filter, so it was scoring the whole
  fleet (minus 3 excluded players), not McCoy — of those 168 rows, McCoy
  contributed only 42; `neo-matrix` alone contributed more (52). The
  near-match to 169 was coincidence (McCoy being a meaningful but partial
  slice of a fleet-wide number), not a real confirmation.

  **Corrected, properly-scoped number, verified 2026-09-11**: McCoy-only,
  same `CLEAN_TRADES_WHERE` date floor, stated confidence 0.9–1.0:
  **n=42, realized hit rate 71.4%** (30/42 wins). Avg stated confidence in
  this bucket is still ~0.93 — so the overconfidence gap is **~22 points**,
  *wider* than the withdrawn 15-point figure, not narrower. The directional
  finding (McCoy is meaningfully overconfident at the top of his stated
  range) survives the correction and is if anything stronger; the specific
  numbers 169/78.6%/"15 points" are withdrawn and replaced by 42/71.4%/
  "~22 points." **The fail-closed sizing rule above is unaffected in
  substance** — a 42-trade sample with a 22-point gap argues for it at
  least as strongly as the withdrawn 169-trade figure did. Full
  correction detail: `relay_2026-09-11_calibration_finding_correction.md`.

  ---
  **2026-09-10 draft, superseded — kept for history, do not build against
  this:** targeted restoring `engine/crew_scanner.py` Gate 7
  (`SNIPER_ALPHA_THRESHOLD` 0.25->0.3, `SNIPER_MIN_CONFIDENCE` 55->70,
  plus a Signal Center grade >= B check). Wrong target — see "Why v2
  exists" above. The Signal Center grade >= B mechanism it wanted to add
  does already exist live, just elsewhere (`engine.crew_scanner.
  ollie_auto_check()`, a different pipeline entirely, not McCoy's).

---

## Status

**Not built. Nothing shipped against this spec.** Two open details flagged
inline (the exact `confidence_calibration` reduction formula, and the
exact `learning_engine` multiplier transform) still need a confirmation
pass before either is coded. The whole confidence-coordination piece
(items 1–4 under "Coordination question") stays held until `calibration_
map` has materially more data than the 3 fleet-wide rows it has today —
per the spec's own text, that's not a "wait a few days" condition given
how narrow the `trade_fire`→`trade_id` join is; see the backlog item it
references for the actual blocker.
