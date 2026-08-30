# Relay — 2026-08-30 — sentinel banner regression investigated, not where hypothesized

## Context

Admiral flagged 3 of tonight's revives as "failing under cron despite
passing manual verification," hypothesized a PYTHONPATH/venv/cwd import
issue in the ntfy migration. Ordered: read each log's real last lines
first (sentinel doctrine), fix invocation if that's the cause, fix
gex_collector, prune/repoint the 11 stale launchd entries, verify via
/classic after a real sentinel tick — and going forward, verify revives by
waiting for a real cron tick, not a manual run.

## What the evidence actually showed

The PYTHONPATH hypothesis didn't hold up. All three flagged logs'
mtimes predate tonight's revival work — the content is old, not a new
failure. Direct evidence: `origin_healthcheck.sh` is firing correctly
every 5 minutes right now (cross-checked `/api/status` hits in
`trader.log` against the cron schedule, exact match, all fast/healthy) —
it's silent-on-success by design, not broken. `q_dissent_watch.py` and
`uhura_agent.py` simply haven't had a real scheduled tick since revival
(weekday-only / already-past-05:30-today schedules). Directly tested the
PYTHONPATH hypothesis anyway under a fully stripped environment matching
real cron — no import failure, for either script.

## What was actually fixed

1. **Heartbeat logging** added to `origin_healthcheck.sh` and
   `q_dissent_watch.py` so silent-on-success runs leave distinguishable
   log content instead of freezing on stale pre-fix error text forever.
   Verified with a real cron tick (polled for the log mtime to change,
   caught the genuine 11:55:01 firing, confirmed the sentinel cleared it)
   — not a manual re-run.
2. **`hm-wr-dur-monday-check` retired** — its `StartCalendarInterval` is a
   one-shot hardcoded to 2026-07-20, confirmed via direct plist read.
   Never fires again. Same dead-one-shot class as
   `hm-signals-v2-monday-check`/`-verify` (retired earlier tonight),
   missed in that pass. Retired via `fleet_lifecycle.py`, removed from the
   sentinel's registry.
3. **`archer-briefing` repointed** — its registered log path
   (`archer_briefing.log`, the plist's stdout) is permanently empty;
   Python's `logging` module defaults to stderr. Repointed to
   `archer_briefing_err.log`, where the real content actually lands.
   **Found a real bug along the way**: the same stderr routing exposed
   that `engine/archer_morning_synthesis.py`'s ntfy send has been silently
   failing on every single run (`No module named 'engine'`, no `sys.path`
   setup for a direct-script-path launchd invocation) — caught by a broad
   except, logged as a mere warning, never actually delivered. Fixed with
   the standard `sys.path.insert(0, repo_root)` pattern.
4. **`uhura-watch` corrected, not touched** — it is NOT the same thing as
   tonight's `agents/uhura_agent.py` cron revival (different scripts,
   coincidental name overlap, same trap flagged in the original
   classification report). Left as-is in the registry — correctly
   scheduled, just not yet due.
5. **The other 8 stale launchd entries** — verified every plist path
   matches the registry and every schedule is legitimate and simply not
   yet due (4 fire later today, the rest Monday). No changes needed.

## Verification

`hm_ops_sentinel.py --dry-run`: launchd stale 11→9, cron broken 6→3,
`lifecycle_drift` stayed `{[], [], []}` throughout (no new drift
introduced by any change).

## Not achieved tonight (by design, not oversight)

Full zero on both banners requires ticks that are hours (today) to a full
day (Monday) away on their own real schedules — `q_dissent_watch.py`,
`uhura_agent.py`, `gex_collector.py`, and the 8 remaining launchd entries.
Per the new "verify by real tick" standard, none of these were
manually re-triggered to fake a clean state. Recommend a follow-up check
Monday afternoon once the weekday schedules have had a full cycle.
