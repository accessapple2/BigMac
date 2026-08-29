# HM-STANDDOWN-SUCCESSOR-2026-08-29

**Supersedes:** the 2026-07-22 "final quiet-down" order (see
`~/.claude/projects/-Users-bigmac/memory/project_ollietrades_final_quietdown_2026-07-22.md`
for the original, full record — not duplicated here).

## Disposition

The 2026-07-22 stand-down was a deliberate, trip-departure pause — *"the
fleet owns bigmac through tomorrow's close and the trip quiet-down"*
(`docs/XO_BACKLOG.md`, filed 07-21), with an explicit **post-trip**
resumption always intended, not a permanent end to the project. Trading
itself resumed around 2026-08-24 (exact trigger not recorded). This
document formally closes out the remainder of that order's own "still
owed" reversal checklist, which had sat incomplete for five weeks.

**As of 2026-08-29, the fleet is deliberately restored to full operation.**

### The 23 jobs from the original stand-down — final disposition

**Revived (18), enabled + bootstrapped in `gui/501` today, no sudo
required:**
`universe-refresh`, `model-watcher`, `iv-backfill`, `danelfin-update`,
`enrichment-poller`, `ti-email-poller`, `uhura-watch`, `scotty`,
`nightly-backtest`, `nightly-regression`, `daily-watch`,
`hm-wr-dur-monday-check`, `morning-an2-observation`, `stale-trim-obs`,
`finetune-reminder`, `hm-signals-v2-monday-check`,
`hm-signals-v2-monday-check-verify`, `archer-briefing` — all confirmed
healthy right up until the 07-22 stand-down (log activity within 0-10
days of it), reactivated on the Admiral's explicit job-by-job review.
Every one of these now carries freshness coverage via
`scripts/hm_ops_sentinel.py::check_launchd_jobs_health` (drift-back-to-
disabled + staleness, ceilings scaled to each job's own calendar
cadence).

**Left disabled, investigate separately (3):** `ti-picks-watcher` (dead
since 05-14), `morning-cd-instr` (dead since 05-22), `crusher` (dead
since 04-26) — all three were already broken well before the 07-22
stand-down; reactivating them wouldn't restore anything, and whatever
actually broke them is a separate, older problem not addressed here.

**Code-retired, not reactivated (1):** `com.ollietrades.riker-synthesis`
— per CLAUDE.md, this was retired at the code level on 2026-06-24
(`main.py`'s scheduler for it was removed, not just paused); re-enabling
the launchd job would do nothing since the code path it fires no longer
exists.

**Orphan, removed from consideration (1):** `com.trademinds.crew` — a
disabled-override exists in launchd's records but no plist file backs it
(`~/Library/LaunchAgents/com.trademinds.crew.plist` does not exist).
Inert either way; nothing to reactivate.

**Deferred, not decided in this pass (2):** `com.trademinds.premarket`
(an older, independent scanner — not part of the Kirk-briefing pipeline,
last ran 07-22 04:00, same day as the stand-down) — flagged in the
original open question as needing its own explicit call, not bundled
into this successor order. `com.trademinds.signal-center` was already
separately reactivated 2026-08-28 (see the companion relay doc from that
date) — not part of this document's scope, noted for completeness.

18 + 3 + 1 + 1 + 2 = 25 — the extra two beyond the "23" figure are
`com.trademinds.crew` and `com.trademinds.premarket`, which were folded
into the original stand-down's disabled-jobs list but aren't
`com.ollietrades.*` fleet-automation jobs in the same sense as the other
23; see `QUESTION_fleet-standdown-reversal.md` for exactly how the
original count was assembled.

### Separately, not part of the stand-down but found the same day

The Ollama-serve plist regression (`OLLAMA_FLASH_ATTENTION`/
`OLLAMA_KV_CACHE_TYPE` added 2026-08-27, cutting live signal throughput
~15x) and `mlx-qwen3`'s unsupervised local server (dead since 07-18,
independent of the stand-down) are tracked in the companion post-mortem,
not here — they're real but unrelated root causes found while
investigating this one.

## What changes going forward

- No further action needed on the 18 revived jobs — they resume their
  normal calendar schedules and are now sentinel-covered.
- The 3 pre-dead jobs and `com.trademinds.premarket` remain open items,
  explicitly deferred, not silently dropped.
- If a future stand-down is ever ordered again, this document's
  disposition table is the reference for "what was running before,"
  so a future reversal doesn't have to reconstruct it from scratch.

**Source:** full investigation and live verification in
`data/reports/relay/relay_2026-08-29_2026-07-21-22-postmortem.md`.
