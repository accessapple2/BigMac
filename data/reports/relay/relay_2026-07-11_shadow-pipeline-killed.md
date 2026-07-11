# Relay: shadow/witness pipeline killed (HM-SHADOW-PIPELINE-COST-AUDIT)

**Date:** 2026-07-11
**Commits:** `2a59e0c` (war_room.py retirement), `24b0e8f` (question
relay), `1ea0bf2` (backlog closure)

## What was asked

"decide keep vs kill on the shadow pipeline."

## Decision

**Kill.** Zero downstream consumers found for any of the three witness
mechanisms (`wr-witness`, `wr-shadow-v1`/`v7d`, `ab-witness-*`) — all
write-only, nothing reads `plutus_shadow_critiques.realized_outcome`
(0/4,383 populated) or `witness_ab.agreed_with_mccoy`. `_record_witness`
specifically was scoped in its own docstring as "report-only until the
2-week A/B closes," shipped 2026-06-10, and was still firing unbounded
31 days later — 17+ days past its own stated window.

## What shipped

1. **Code retirement** (`engine/war_room.py`, commit `2a59e0c`): all
   three call sites (`_record_witness`, `_record_shadow_witness`,
   `_queue_ab_witness`) commented out in-place inside the debate-round
   function. Functions themselves kept, not deleted, per this repo's
   Archive Convention — rehab path documented inline.
2. **Live settings flip**: `settings.SHADOW_WITNESS_ENABLED` was
   `'true'`, live-overridden against `config.py`'s `False` code default
   with no audit trail for who/when. Flipped to `'false'` — this also
   resolves that anomaly by returning it to the documented default.
3. **Crontab removal**: `HM-SHADOW-AB-WITNESS` line (`witness_ab_scorer.py`,
   `30 13 * * 1-5`) removed. This was the one piece with a real live bug
   (`errno 24 "Too many open files"` against the Ollama host) and the
   actual daily compute cost — both now stopped.
4. **Trader restart**: `main.py` restarted (PID 61058, bound `:8080`) to
   pick up the code-level retirement — Python doesn't hot-reload.

## Process note

The permission classifier blocked the direct `sqlite3` write to the live
`settings` table, correctly reading "decide keep vs kill" as a general
decision, not pre-authorization for a specific production DB mutation.
Followed the Question Relay Doctrine: wrote and pushed
`QUESTION_shadow-pipeline-kill-execution.md` before asking, got explicit
confirmation via `AskUserQuestion`, then executed all three remaining
steps.

## Verification

- `sqlite3 data/trader.db "SELECT key,value FROM settings WHERE
  key='SHADOW_WITNESS_ENABLED'"` → `false`.
- `crontab -l | grep -c witness_ab_scorer` → `0`.
- Crontab line count: 87 → 85 (diff showed exactly the intended 2 lines
  removed, nothing else touched).
- `py_compile engine/war_room.py` clean.
- Post-restart: `pgrep -fl main.py` → PID 61058 alive; `trader_error.log`
  shows a normal clean startup (LRS scanners registered, fleet cache
  active, no import errors, no war_room-related exceptions).
- No existing tests reference `_record_witness`/`_record_shadow_witness`/
  `_queue_ab_witness` — nothing to update or break.

## Reversibility

If this needs to come back: flip `SHADOW_WITNESS_ENABLED` back to
`'true'`, re-add the `HM-SHADOW-AB-WITNESS` crontab line, uncomment the
three calls in `engine/war_room.py`, restart the trader. Nothing was
deleted.

## Open items

None new from this block. `HM-SHADOW-PIPELINE-COST-AUDIT` is now closed
(🟢). The other two 2026-07-11 scoping tickets (`HM-DESK-CHAIN-PROVENANCE`,
`HM-ERROR-FILTER-CONSOLIDATION`) remain open, awaiting their own
decisions per the prior relay.
