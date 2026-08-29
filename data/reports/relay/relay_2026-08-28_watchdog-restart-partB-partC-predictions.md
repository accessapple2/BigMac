# Relay — 2026-08-28 (later still) — watchdog restart, Part B fix, Part C investigation, predictions/signals forensics

## Context

Follow-on to `relay_2026-08-28_429-remediation-A-and-D.md` (same evening).
Explicit authorization for: (1) restart `watchdog.py` now, (2) implement
`red_alert_check()`'s backoff-with-retry (in scope), (3) investigate/design
(not build) a shared Alpaca pacer, (4) GO on the Part C Polygon investigation
(not build), (5) find why `predictions`/`signals`/`signal_outcomes` sources
are stale per signal-center's `/api/sources/health`.

## 1. `watchdog.py` restarted

Old PID 1231 (started 20:10:00, predated the 22:03:57 edit) killed; relaunched
via the exact command `watchdog_supervisor.sh` uses. New PID 23298. Verified:
clean startup log ("=== TradeMinds Watchdog started ==="), a real "Watchdog
Online" push sent `[200]`, and direct import against the live interpreter
confirms `_daily_count_ok`/`_content_dupe`/`NTFY_DAILY_CAP=200`/
`DEDUPE_WINDOW_SECS=1800` are genuinely loaded.

## 2. Part B — investigation findings + the one in-scope fix

**The 233-vs-829 discrepancy is a red herring pointing at the wrong root
cause.** Found a real 429 on an **80-symbol** batch — under both the
directive's "233" and Alpaca's actual documented 1000-symbols-per-call
limit. This is not a request-size problem. ~20 different modules
independently call Alpaca's data API with zero cross-module coordination;
their aggregate call *frequency* is what exceeds the free tier's per-minute
quota, not any single request's size. Chunking smaller (as literally
directed) wouldn't fix this and could make it worse (more requests against
an already-saturated shared budget).

Traced the actual unchunked/unretried call site: `volume_scanner.py::
red_alert_check()` — "1 API call for 50-150 symbols" per its own comment,
but real data shows it reaching 829 on a busy day. The two *other* Alpaca
snapshot call sites (`volume_baselines.py`, `volume_scanner.py`'s main
sweep) already chunk correctly at `SNAPSHOT_BATCH=1000` with spacing.

**Shipped** (in scope, per explicit authorization): `_fetch_snapshots()` —
the shared low-level function `red_alert_check()` and the main chunked sweep
both call — now retries on HTTP 429 with backoff (5s, 15s; never
immediate-retry), giving up gracefully after 3 total attempts. Fixes the
class for both callers, not just `red_alert_check()`. Verified with a
mocked-`requests` unit test: 2×429 then success returns correct data on the
3rd call.

**Not built** (design/investigate only, bring to review alongside Part C):
a shared cross-module Alpaca request pacer. ~20 callers, no shared budget
today — same architectural shape as the Part C Polygon problem. Flagging
that it exists and is real; not designing it in isolation from Part C's
design review.

## 3. Part C — Polygon BENCH-gate investigation (GO on investigation, nothing built)

Full findings + the actual `engine/` triage in
`QUESTION_polygon-rate-limiter-bench-gate-list.md` (same relay batch).
Headline: the directive's "12 engine modules + 5 scripts" undercounts the
real surface — **34 files** actually call `api.polygon.io`/`polygon_provider`
(15 `engine/`, 7 `scripts/`, 12 more across `swingdesk/`, `scrapers/`,
`strategies/`, `dashboard/app.py`, `signal-center/`, `mread_server.py`).
Triaged the 15 `engine/` files into can't-tolerate-stale-GEX (`gamma_context.py`,
`options_pricing.py`, `paper_trader.py`, `bk_orb_scanner.py`,
`squeeze_scanner.py`, `ollie_machine_universe.py`) vs. safe-to-cache
(`options_flow_gex.py` — explicitly observation-only by its own docstring,
`universe_refresh.py`, `ticker_context.py`/`ticker_names.py`,
`holly_intraday.py`, `total_portfolio.py`, `crew_dissent.py`). The other 19
files (scripts/ + everything outside engine/) not triaged to the same depth
this pass — `swingdesk/` in particular looks like it needs its own review
(`options_engine.py`, `shadow_autopilot.py` by name alone). Question doc asks
which scope to proceed on.

## 4. Predictions / signals / signal_outcomes forensics

Checked `/api/sources/health` directly (via `rtk proxy curl` — the default
`curl` gets RTK's token-optimized schema-stub response, not real data, for
this endpoint; worth remembering for next time).

**`predictions`** — frozen `as_of` 2026-07-22 20:05:44 (36 days). First
checked whether tonight's crontab work was the trigger: **confirmed no** —
none of the three lines touched tonight (HM-FORGE-PAGER, `hm_ops_sentinel.py`,
`iren_flip_watch.py`) reference predictions/auto-snapshot/port 9000 at all.
Traced the real writer: `watchdog.py::trigger_snapshot()` → POSTs to
signal-center's `/api/predictions/auto-snapshot` once daily at 4:05pm ET
(`check_daily_snapshot()`, called every cycle from `main()`'s loop — verified
it's actually wired in, not dead code). Checked signal-center's own access
log directly for this exact request: **last successful hit was 2026-07-22
13:15:11 local (= 20:05:44 UTC = 16:05:44 EDT, matching the trigger time
exactly) — zero POST attempts of any kind, successful or failed, since.**
Signal-center itself was demonstrably up and logging other traffic for
another five weeks after that (until the Aug 27 10:41 outage this whole
session started from), so signal-center's own downtime does not explain the
freeze — something stopped the *caller* from firing at all, specifically on
stand-down day, five weeks before signal-center itself actually went down.

Checked the current code for a bug that would explain this and found none —
`check_daily_snapshot()`'s logic is sound, has no market-day gate (fires any
calendar day), `_last_snapshot_date` is a plain in-memory guard that resets
on every restart, and `predictions_snapshot()` on the signal-center side has
no gate either. Git history shows this code unchanged since April 10 (no
`git log -S` hits for a later fix). **Honest conclusion: I can't pin down
with certainty why it stopped exactly on stand-down day given no other code
change is visible — most likely some now-untraceable interaction with
whatever else happened that day (possibly a watchdog.py restart into a
different, uncommitted local state that's since been overwritten, which
git history can't show). The practical fix is already in place**: watchdog.py
is freshly restarted tonight with the current (structurally correct) code,
and the real test is tomorrow's natural 4:05pm ET fire — will check
`logs/signal-center.log` for a fresh `auto-snapshot` hit after that time
and report back rather than assume it's fixed.

**`signal_outcomes`** — writer is `main.py::run_signal_scorecard()`, hourly
via `schedule.every(1).hours.do(...)`, throttled by an in-memory (not
persisted) `_market_throttle_last` dict. `main.py` was restarted tonight
(22:09) for unrelated reasons (429-remediation Parts A/D) — the throttle
state is now clean and the hourly schedule re-registers from the restart
time. No independent bug found; should self-resolve within the hour.

**`signals`** — writer is `ai_brain.py` (trader-resident) POSTing to
signal-center's `/api/signal` per generated signal, not a scheduled batch —
the `source_registry`'s "once-daily morning batch" description characterizes
the typical observed cadence, not a distinct cron trigger. Also rides along
with tonight's `main.py` restart. Lower investigative depth than
`predictions` (no forensic log trace attempted on `/api/signal` hits) — if
this is still stale after the next trading session, worth a proper trace
of *why* `ai_brain.py` didn't post Friday, same rigor as the predictions
trace, not assumed away by "the process restarted."

## Files/state touched this pass

- `engine/volume_scanner.py` — `_fetch_snapshots()` 429 retry/backoff.
  Committed with unit-test evidence in the commit message.
- `watchdog.py` — restarted (no further code change this pass beyond
  tonight's earlier commit).
- `main.py` — no new change; already restarted this evening for A/D.
- Two new relay docs (this file + the Part C question doc), committed
  together.
- No crontab changes this pass (tonight's earlier 3-line cleanup already
  covered and re-verified as unrelated to predictions).

## Open for next session / Admiral input

- Part C question doc — needs an answer on scope before any limiter code
  gets written.
- Part B's shared Alpaca pacer — same treatment, bring alongside Part C.
- Tomorrow ~4:05pm ET: check `logs/signal-center.log` for a fresh
  `auto-snapshot` 200 to confirm the predictions freeze is actually broken,
  not just theoretically fixed.
- Next trading session: check whether `signals`/`signal_outcomes` actually
  advanced, or whether `ai_brain.py`'s Friday miss needs its own forensic
  trace.
