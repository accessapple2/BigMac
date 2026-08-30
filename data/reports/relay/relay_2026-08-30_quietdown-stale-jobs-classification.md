# Relay — 2026-08-30 — quietdown stale-jobs classification (report-only)

## Context

Follow-on to tonight's earlier signals-v2 retire + FIFO-starvation scoping
work (commit `8bed0fc`). Separate directive: classify the 16 cron-scheduled
scripts still sitting as `*.quietdown-disabled-2026-07-22` (the leftover
dirty git state surfaced while checking the repo before that work — see
[[project_ollietrades_final_quietdown_2026-07-22]]). Explicit instruction:
**stand-down is over, but hold execution** — report only, no revives, no
code changes. Admiral picks from the report.

## What was asked

Two required columns per stale job:
1. **Current-writer check** — is this job's output already being produced
   by something else today? Seeded with two worked examples: `uhura/signal`
   written 2026-08-29 23:45 and `archer briefing` written 2026-08-30 06:25,
   despite both jobs' own logs being stale.
2. **Stale-ref screen** — grep each script for `olliemax`, direct Polygon
   calls, and per-run ntfy curls, all pre-dating the 2026-08-28
   429-remediation pass.

## What was found

Full table appended to `docs/XO_BACKLOG.md` under
`HM-QUIETDOWN-STALE-JOBS-CLASSIFICATION`. Highlights:

- **The two worked examples turned out to be two separate, already-revived
  `job`-type launchd entries** (`uhura-watch`, `archer-briefing`) —
  distinct from the 16 cron scripts in scope, and a genuine name-collision
  trap: `agents/uhura_agent.py` (in scope, SEC EDGAR 13F/insider intel,
  writes `institutional_holdings`/`institutional_signals`/`insider_trades`)
  is **not** the same "Uhura" as `/api/uhura/signal` (a live dashboard
  confluence-signal endpoint, `engine.uhura`, unrelated data). Confirmed
  `agents/uhura_agent.py`'s actual tables are stale since 2026-07-22 with
  no substitute — initially misread this as "already covered" before
  checking the underlying tables directly; correcting here rather than
  letting the wrong read stand.
- **11 of 16 fail the stale-ref screen** on ntfy alone — direct
  `urllib.urlopen()`/`curl` straight to `ntfy.sh`, bypassing the hardened
  `engine.alert_channels._send_ntfy()` (DECOM-SILENCE guard + 429 backoff)
  that the other 5 already route through.
- **2 of 16 are moot regardless of current-writer status:**
  `kimi_cut_watch.py` (the agent it watches, `ollama-kimi`, was already cut
  2026-06-19 — confirmed via `fleet_lifecycle_ledger`) and
  `riker_synthesis.py` (already formally retired 2026-08-29, code-level
  retirement dates to 2026-06-24 per CLAUDE.md — not actually part of this
  decision at all).
- **1 confirmed live bug the disabled automation would fix:**
  `regime_refresh_runner.py` — `regime_history` gets exactly one row per
  date, at/near market open, with zero intraday updates for the rest of
  the session, every day checked 08-27 through 08-30. This is the exact
  scheduler-starvation symptom the script was built to correct. Clean ntfy
  screen too — strongest candidate on the list.
- **A genuine structural bug found, not previously flagged:**
  `daily_report.py` and `eod_report.py` write to the identical output path
  (`drafts/DAILY_REPORT_<date>.md` + `daily_ledger.csv`) at two different
  cron times. Reviving both would silently recreate a collision that
  pre-dates the quietdown.
- **A dated decision that may have been silently missed:** both
  `door1_kill_gate_check.py` and `ollie_machine_kill_gate_check.py` existed
  to compute kill-gate verdicts dated **2026-07-24** — but the quietdown
  disabled both scripts **2026-07-22**, two days before either could fire
  even once at the decision date. Nothing in `XO_BACKLOG.md` records either
  verdict ever being rendered. Flagged as its own question, separate from
  the routine revive decision.
- **One dashboard bug surfaced as a side effect:** `dashboard/app.py`'s
  `/api/health-manifest` endpoint lazy-triggers `engine.fleet_auditor.
  run_audit()` when its cached manifest goes stale — but that import fails
  live right now (`ModuleNotFoundError`, confirmed by direct test) since
  the module is still renamed away. The self-heal path is silently broken
  independent of whether `fleet_auditor.py` itself gets revived.
- **One prior comment caught being wrong:** the crontab's 2026-08-28
  HM-429-REMEDIATION-D comment for `iren_flip_watch.py` claims the file "no
  longer exists on disk (deleted, not renamed)" — it's present, just
  renamed under the quietdown suffix. Whoever wrote that comment didn't
  check.

## Not done this pass

No files renamed back, no crontab lines uncommented, no code touched. Pure
classification. The Admiral picks candidates from the `XO_BACKLOG.md` table
before any revive proceeds.
