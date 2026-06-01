# SPEC — Per-advisor labeling on Captain's Portfolio (Dig 2) — HOLD for smoke

Status: design draft 2026-06-01. **No build yet** (frontend → Admiral browser smoke). The
GROK-vs-Kirk "contradiction" is a GENUINE FRESH disagreement, not a bug — so the fix is to
LABEL each advisor's call clearly, never silence one.

## What the data actually shows (read-only dig)
- **GROK** (LLM, `portfolio_advice` advisor='grok' via `/api/wb-team/advice`): FRESH today 17:39 —
  CEG **SELL**, LLY **TRIM**, COPX HOLD. Non-expired. Backend already deduped per (advisor,symbol)
  + expiry-filtered (`get_team_advice`); `_renderGrokPanel`/`pollGrokDiff` use `innerHTML=` (no dup).
- **Kirk-live** (rules, `/api/kirk/advisory` computed on-demand → the portfolio "Kirk" column via
  `window._kirkPositionMap`): HOLD. FRESH (endpoint computes live each call).
- **Kirk-log** (`kirk_advisory_log`, the W1 persisted source): STALE — last write 2026-05-18
  (producer dead); shows CEG=SELL. So Kirk even disagrees with itself (live HOLD vs log SELL).

Three distinct signals, today, by methodology — not stale, not a data bug.

## The change (frontend; dashboard/static/index.html)
1. **Label the source on every advice badge.** The portfolio "Kirk" column today shows the
   Kirk-live action with no provenance. Render each as a labeled chip:
   `Kirk-live: HOLD` · `GROK: SELL` · (optional) `Kirk-log: SELL ⚠ 14d stale`. Distinct colors
   per advisor; never collapse to one verdict.
2. **Show disagreement explicitly** rather than letting one overwrite the other — e.g. a small
   "advisors split (GROK SELL / Kirk HOLD)" tag on the row so the Captain sees the divergence.
3. **Stale-source badge.** Where the value derives from `kirk_advisory_log` (W1 RED), mark it
   `⚠ stale <age>` (reuse the W1 as_of/RED treatment) so a dead feed can't read as current.
4. **Single provenance helper** so each advisor's freshness/label is rendered consistently
   (avoid re-deriving in each renderer — the dup-prone pattern).

## Deferred / flagged (NOT in this spec)
- **10× duplication: PIN ONLY when `/api/portfolio/real` is non-empty.** It returned **0 positions**
  at dig time, so the duplicating surface can't be reproduced. Backend advice is clean, so the
  repetition is at the portfolio render layer (likely multi-account/multi-lot rows repeating a
  ticker+badge) — confirm against a populated feed before touching it.
- **FLAG (not fix): real portfolio is on the 2026-05-28 snapshot** — no fresh Schwab CSV imported
  since (schwab_snapshot is UNKNOWN/idle in W1). The cards/positions reflect 05-28 holdings, which
  is itself why the surface looks stale/empty. Importing a fresh CSV is a Captain action, not a code fix.

## Constraints
Frontend → ships only after Admiral hover/click smoke (HM-BJ.E4). No order path, no advisor
silenced. Labeling is additive (no data filtered out).
