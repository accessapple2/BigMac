# Relay — 2026-08-30 — quietdown revive/retire batch, executed

## Context

Follow-on to tonight's `HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION`. Admiral
picked dispositions for all 16 cron-scheduled scripts and ordered execution
one at a time with a clean-tick verification before each next step. Full
detail recorded in `docs/XO_BACKLOG.md` under
`HM-QUIETDOWN-FINAL-DISPOSITIONS`. Crontab backed up first:
`~/backups/cron/crontab.bak-20260830-090553-pre-revive-batch`.

## What shipped

**Revived (4):** `regime_refresh_runner.py`, `agents/uhura_agent.py`,
`fleet_realism_sweep_clean_window.py`, `scripts/eod_report.py` — all
manually clean-tick verified (uhura_agent did a full real EDGAR scan;
fleet_realism_sweep ran to completion, 22/22 agents; eod_report's "collision
with daily_report" premise was re-checked and found false — no actual doc
mismatch or file collision existed to fix).

**ntfy-migrated + revived (2):** `origin_healthcheck.sh`, `q_dissent_watch.py`
— both moved off raw `curl`/`urllib` calls to `engine.alert_channels.send_alert`
(DECOM-SILENCE/Pushover/rate-limit aware). Verified via real runs plus an
isolated failure-path test for origin_healthcheck.

**ntfy-migrated, not enabled (1):** `recall_refresh_run.sh` — migrated and
verified in isolation, left commented out pending the catch-up
characterization: 1,343/1,595 qualifying closed trades un-embedded, bounded
~3-run self-resolving catch-up once enabled (newest-500-per-run). Awaiting
go-ahead.

**Retired (3) + stays-retired (1):** `kimi_cut_watch.py` (purpose fulfilled),
`daily_report.py` (retired per directive, but the stated reason was checked
and corrected on record — no real collision with eod_report existed),
`iren_flip_watch.py` (Admiral dropped IREN alerts), `engine/fleet_auditor.py`
(stays retired, v2 direction).

**Stays dark, no ledger action (2):** `situation_report.py` (revisit ~09-06),
`ollama_prewarm.sh` (revisit post-09-04 un-aliasing).

**Sentinel repoint check:** no code change needed — already self-adapting
(launchd side reads the ledger live and skips retired/halted; cron side has
no hardcoded registry at all, just re-reads crontab). Verified the broken-cron
count dropped 6→4 post-fix. Two of the remaining four (`origin_healthcheck.sh`,
`q_dissent_watch.py`) won't clear from a clean/no-op run because they're
silent-on-success by design — flagged as a known limitation, not fixed
(would be new architecture).

## One process note

`eod_report.py`'s file-rename step was initially skipped by mistake (only
its crontab line got uncommented) — caught during the sentinel re-verification
pass when its manual test hit a real "No such file" error, fixed immediately,
then re-verified clean before moving on. Flagging since it's exactly the kind
of gap the "verify a clean tick before the next" instruction exists to catch.

## Not done this pass

`recall_refresh_run.sh` not enabled (pending review). Comment-only crontab
edits for `situation_report.py`/`ollama_prewarm.sh` (documentation accuracy,
not a lifecycle action). `ollie-machine` (ledger 114) untouched, unrelated to
this batch.
