# Relay: XO-DEPARTURE-HARDENING status check, proposals, and equity-curve fix (S6, work block 11)

**Date:** 2026-07-10
**Commit:** `b529e1e` (pushed to `exec-pipeline`)
**Prior context:** Captain asked "what's next on the backlog," then "check
XO-DEPARTURE-HARDENING, get me the current status," then "Yes, propose
builds for all three," then "check then Yes, look into the equity-curve
gap next."

## What was asked, in order

1. Status check on `XO-DEPARTURE-HARDENING` Phase 1 (4 items: service
   watchdog, health cron, DB/log hygiene, gate-day automation).
2. Propose builds for the three confirmed gaps.
3. Investigate a data-freshness issue surfaced while dry-running one of
   those proposals.

## Status check (re-verified against live crontab/scripts, not the 4-day-old ticket text)

- **Item 1 (watchdog): largely already built.** `watchdog.py` (kept
  alive by cron, reboot-survives) covers trader/bridge/signal-center/
  ollama/cloudflared with ntfy push. `origin_healthcheck.sh` adds a
  second independent layer checking actual HTTP response for bridge/
  signal-center/swingdesk/status_page.
- **Item 2 (health cron): partial.** Confirmed two real gaps: `tour_api`'s
  `/api/tour/health` checked nowhere; the Cloudflare-edge-cache
  staleness for `status.ollietrades.com` unmonitored (and its underlying
  fix still needs a Cloudflare dashboard change only the Captain can
  make — `HM-STATUS-PAGE-STALE-CACHE`).
- **Item 3 (DB/log hygiene): largely already built.** Daily DB backup
  with exact 7-day retention, off-host nightly backup, a freshness
  alarm, and weekly archived log rotation are all live in cron.
  Confirmed one gap: no automated disk-space alert (`vitals.sh` exists
  but isn't scheduled).
- **Item 4 (gate-day automation): not built.** Confirmed zero scripts or
  cron entries for the two dated gates (Door-1 G1-G4, ollie-machine).
  Corrected my own earlier assessment: the Aug 15/16 audition gate needs
  **no new script** — already computed and pushed daily via the existing
  `eod_report.py`.

## What shipped: four proposed scripts

All standalone, syntax-checked, dry-run-verified against live data.
**None wired into cron or the live `origin_healthcheck.sh` yet** —
pending sign-off before scheduling:

- `scripts/tour_api_restart.sh` — handles tour_api's self-respawn-loop
  architecture correctly (kills both the wrapper and the process; a
  plain kill would just get instantly re-spawned with the same wedge).
- `scripts/disk_space_alert.sh` — WARN 85%/ALARM 95%, one push per
  severity per day. Dry-ran clean (29% used, 40GB free).
- `scripts/door1_kill_gate_check.py` — G1-G4 per `OLLIETRADES_KILL_GATE.md`'s
  pre-committed criteria, compute-and-push only. G4 correctly reports
  inconclusive (no benchmark tracking exists — the doc allows this). G3
  scoped to `structure='csp'`, flagged as an explicit interpretation
  call since the doc's own SQL note doesn't state it. Dry-run: G1 PASS
  ($2,511.39 vs $500), G3 PASS (2.9% vs 20% threshold).
- `scripts/ollie_machine_kill_gate_check.py` — checks both `trades` and
  `options_trades`. Dry-run: zero trades, 14 days remaining.

## The equity-curve bug — found while dry-running, fixed, and it mattered

Door-1's G2 (risk gate) came back "no data" on the dry-run. Traced it:
`dashboard/app.py::_get_alpaca_equity_and_spy_raw()` calls Alpaca's
`portfolio/history` endpoint with `start=<season_start>` and no `period`.
**Verified directly against the live Alpaca API with four separate test
calls** (not guessed): omitting `period` makes the endpoint silently
default to a ~1-month window measured *from* `start`, not "start to
now." Proved it by moving `start` forward and watching the returned
window move with it. The account equity curve had been frozen at
2026-05-23 for 7+ weeks and would never self-heal.

**Also tested and ruled out the obvious "safe" fix** — a fixed period
like `"3M"` anchors to `start` the same way, so it would have resurfaced
the identical bug right around the real 2026-07-24 Door-1 verdict date.
Given the kill-gate doc's own rule ("if a gate is ambiguous on the day,
it fails"), that would have been a real risk to Door-1's validity on the
day it matters most, not a cosmetic bug.

**Fix:** `period="all"` (drop `start` entirely) — the only combination
that always reaches "now." Pre-season rows it returns are already
filtered out for free via the existing SPY-bars join. Live-verified
post-restart: `/api/account/equity-curve` now returns 42 dates through
today; the Door-1 script's G2 now resolves (PASS, 0.17% vs 0.55%
threshold) instead of "no data."

## Testing

- `tests/test_equity_curve_period_cap.py` (1 test): verifies the
  outbound Alpaca request uses `period=all` with no `start` param.
- Full suite: 986 passed, same 14 pre-existing + 2 unrelated
  date-boundary `test_m5_allocator.py` failures from earlier tonight
  (untouched module, confirmed unrelated at the time).
- `py_compile` clean on all touched/new files.
- Trader restarted, single-PID bind confirmed, zero orphans.

## Open items (carried forward, plus new)

1. **Wiring approval needed** for the four proposed scripts — none are
   scheduled yet.
2. `HM-STATUS-PAGE-STALE-CACHE` — still needs a Cloudflare dashboard
   change only the Captain can make.
3. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — on hold pending a live MLEG close.
4. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — needs a dedicated design session,
   zero current urgency.
5. The `options_books` stored-counter drift — still harmless, still out
   of scope.
