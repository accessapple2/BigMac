# Relay — OPS TRIAGE (2026-08-29)

Full directive: sentinel banners caught two real problems (signals_v2 FIFO
starvation, source-health watcher dead) plus a report-card sanity check.
Three items, all closed this session.

## 1. SIGNALS_V2 FIFO STARVATION — RESOLVED (option a: applied the fix)

- One-time cleanup: `scripts/hm_signals_v2_expire_dead_letter_20260829.py`
  (dry-run then `--apply`) archived 10,536 dead-letter rows (halt_mode !=
  'active' or past `stale_after`). 1,127 rows left untouched (all NULL
  `stale_after`, by design — "never-stale"). Snapshot:
  `data/backups/hm_signals_v2_expire_dead_letter_20260829_133052.ids`.
- Structural fix: `engine/events_bus_consumer.py::consume_pending_signals()`
  now does hybrid ordering — reserves up to 2 slots/batch for the OLDEST
  pending rows (`ORDER BY created_at ASC`), fills the rest newest-first as
  before. Prevents the newest-first-only queue from starving old rows
  forever under sustained load.
- `docs/XO_BACKLOG.md`'s `HM-SIGNALS-V2-STARVATION-RECURRENCE` entry
  updated NOT IMPLEMENTED → RESOLVED with before/after counts.
- Tests: `tests/test_events_bus_consumer_starvation_fix.py` (4 new tests).
- Commit `61126bb`, pushed.

## 2. SOURCE-HEALTH WATCHER — REVIVED, now self-watched

- Root cause confirmed: same 2026-07-22 stand-down rename that killed the
  GEX collector also renamed `scripts/source_health_watcher.py` to
  `.quietdown-disabled-2026-07-22`, leaving its cron entry pointing at a
  dead path. One root cause, two casualties, as suspected.
- Restored the script (`git checkout HEAD --`), removed the disabled copy.
- Fixed a real latent bug found while restoring: the NTFY fallback path's
  HTTP header used an em-dash (`—`) in the title, which isn't latin-1-
  encodable — every fallback POST raised `UnicodeEncodeError` before a
  request was ever sent (silent, since the primary `engine.alert_channels`
  path normally covers it). Changed to a plain hyphen.
- **No more unwatched watchers**: `hm_ops_sentinel.py` gets two new checks:
  - `check_source_health_watcher_heartbeat` — fires **RED_ALERT** (not
    WARNING) on stale/missing heartbeat, specifically to bypass
    DECOM-SILENCE. The existing in-process dead-man's-switch in `main.py`
    (`_bg_source_health_dms`) had been correctly firing WARNING for all 38
    days of the outage and going nowhere — same disease as the historical
    "alarm shares a failure mode with what it watches" lesson, just via a
    muted severity instead of a shared mechanism.
  - `check_cron_missing_scripts` — reads `crontab -l` directly, tails the
    single last non-empty line of every active entry's log for a
    file-not-found signature. Generalizes past the two watchers found by
    hand: scans all 35 active cron entries automatically. Live run finds
    exactly the 15 known-broken quietdown-disabled entries (14 unique
    scripts + `gex_collector.log`, which legitimately hasn't had a
    successful cron-triggered retry since Saturday's restore — pending the
    Polygon 403-entitlement fix, outside my control). Two false-positive
    iterations during dev (byte-window scan, then 2-line tail) both fixed
    before shipping — see commit message for detail.
- Verified live: cron entry active (`*/10 * * * *`), heartbeat 7.9 min old
  at write time, 16 sources checked.
- Fixed a real test regression this surfaced:
  `tests/test_hm_ops_sentinel_acks.py`'s end-to-end dispatch test only
  mocked 4 of `main()`'s now-7 checks, so the new
  `check_cron_missing_scripts` ran for real inside the test and leaked a
  genuine alert into a strict-equality assertion. Now mocks all 7.
- Full suite: 1102 passed, 9 pre-existing failures (unrelated
  quietdown-script/riker-ntfy fallout — same baseline as before this work).
- Commit `93e3006`, pushed. No trader restart needed (only cron-run scripts
  touched — `main.py`/`dashboard/app.py` untouched).

## 3. REPORT CARD SANITY CHECK — confirmed intended, one stale UI note fixed

**Question 1: is this a scoping bug or the intended small-sample S7 view?**
Confirmed intended. Live `calculate_rating()`:
- Capitol Trades: E, 0/2, -$116.89 — reproduces the reported numbers exactly.
- Neo: B, 29/0, +$46.18 — a FULL THROTTLE-tier rating (A/B → FULL THROTTLE
  per `lineup_advisor()`), matches the report.
- Uhura (`ollama-llama`): **currently N/A, 0 season-7 stock trades** (last
  persisted snapshot is from 2026-05-30, before season 7 even started —
  `calculate_rating()` doesn't persist N/A results). She is NOT currently
  FULL THROTTLE in live data; either trade-count drift since the report
  was written, or a stale read at the time. Flagging honestly rather than
  reconciling a moment I can't reproduce — not a bug either way, since an
  N/A agent is simply absent from lineup advice, not misclassified.

**Question 2: does the DISPLAY still exclude options while
`get_winning_models()` includes them?**
No — confirmed **no divergence**. Traced both call paths: `/api/ratings`
(the display table) calls `fleet_report_card()`; `get_winning_models()`
(engine/ollietrades_signal.py:125) also calls `fleet_report_card()`. Same
function, same `calculate_rating()` underneath, for both. This was already
true as of the earlier options-blindness fix (`7f34d71`) — there was never
a live scoping gap between the two, by construction (one shared function).

**Before/after diff of the options-inclusion fix**, requested explicitly:
Ran the OLD (stock-only) scoring logic against TODAY's live data alongside
the current (merged) logic, across all 78 active-fleet agents. **Zero
agents' ratings changed.** Root cause: no currently-active-fleet agent has
closed *any* options trade with `exit_date >= 2026-07-07` (the real-quotes
era boundary the fix reuses). The only real-quotes-era closed options rows
in the whole table belong to `shadow-qwen35-csp` (a shadow-book agent, not
in the tracked active fleet), `test-door1-regression` (a test harness), and
a `strategy:bull_spread_v1` label row — none map to a real active-fleet
`ai_players` row. The last real active-fleet options close was
`options-sosnoff` on 2026-07-04, three days before the era boundary. **The
fix is live and logically correct, but has had zero practical effect on
any current rating** — worth knowing, not something this triage was asked
to fix (a separate question of whether real-quotes-era options closes are
actually happening/being recorded for the active fleet, or whether options
positions are just sitting open, is out of scope here).

**Action taken**: the P&L\* column tooltip in `dashboard/static/index.html`
still read "stock trades only, current season" — factually wrong since the
options-inclusion fix (options ARE included, real-quotes-era only). Updated
the tooltip text to describe the actual current scope. Static HTML, no
process restart required.

## Verification summary

| Item | State | Live-verified |
|---|---|---|
| 1. signals_v2 starvation | RESOLVED | queue drained 10,536→1,127; hybrid ordering unit-tested |
| 2. source-health watcher | REVIVED + self-watched | cron active, heartbeat fresh, RED_ALERT + cron-audit checks live |
| 3. report card | CONFIRMED intended, no bug | traced both call paths to one shared function; diff shows 0 agents affected by earlier fix; stale tooltip fixed |

## Residual / not actioned this pass
- GEX collector (`gex_collector.log`) still shows as broken by the new
  cron-missing-scripts check — known, pending the Polygon 403-entitlement
  fix outside my control (unrelated to today's work).
- Real active-fleet options trades appear to have stopped closing since
  2026-07-04 (0 real-quotes-era closes across 78 active agents) — flagged
  above, not diagnosed or fixed; may be worth a dedicated look if the fleet
  is expected to be actively trading options right now.
- `drawdown_alert.log` intermittent "no positions loaded" — investigated
  earlier this session, `data/real_holdings.json` currently exists, likely
  a benign race with the sync script, not acted on further.
