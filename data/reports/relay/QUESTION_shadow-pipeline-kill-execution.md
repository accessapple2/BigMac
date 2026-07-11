# Question: execute the remaining kill steps for the shadow/witness pipeline?

**Date:** 2026-07-11
**Context:** Asked to decide keep vs. kill on `HM-SHADOW-PIPELINE-COST-AUDIT`
(wr-witness, wr-shadow-v1/v7d, ab-witness-*). Decision: **kill.** Zero
downstream consumers found for any of it (confirmed via the earlier
scoping pass), and `wr-witness` specifically was scoped as "report-only
until the 2-week A/B closes" (its own docstring, shipped 2026-06-10) but
was still running unbounded 31 days later with nothing reading its output.

**Already done (code-level, git-diffable, reversible):** retired all
three call sites (`_record_witness`, `_record_shadow_witness`,
`_queue_ab_witness`) in-place inside `engine/war_room.py`'s debate-round
function — commented out with a dated explanation and rehab path, per
this repo's Archive Convention (code kept, not deleted).

**Blocked by the permission classifier** — needs your explicit go-ahead
before I execute:

1. Flip the live `settings` table row `SHADOW_WITNESS_ENABLED` from
   `'true'` to `'false'` (matches `config.py`'s code default; this row
   was live-overridden against that default for an unknown reason — no
   audit trail exists in the `settings` table to say who/when).
2. Remove the `HM-SHADOW-AB-WITNESS` crontab line (`30 13 * * 1-5
   .../scripts/witness_ab_scorer.py`) — this is where the actual LLM
   compute + the live "too many open files" bug (found during scoping)
   are happening.
3. Restart the trader (`main.py` holds `war_room.py` in memory) so the
   code-level retirement in step 0 actually takes effect — Python doesn't
   hot-reload.

## Options

- **Proceed with all three (Recommended)** — completes the kill decision
  end-to-end; fully reversible (flip the flag back, re-add the cron line,
  uncomment the three calls, restart again).
- **Hold off** — leave the code-level retirement committed but inert
  (the calls are already commented out, so nothing changes behaviorally
  until a restart regardless), don't touch the live settings row or
  crontab yet.
