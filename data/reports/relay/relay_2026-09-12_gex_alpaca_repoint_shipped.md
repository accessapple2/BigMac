# Relay — 2026-09-12. GEX repoint to Alpaca — SHIPPED.

Builds on `relay_2026-09-12_gex_repoint_scoping.md` (the read-only
investigation) — this is the actual build, approved with two conditions,
both addressed below. RULE #1 not implicated (no trades touched).

## What shipped

**1. Canonical repoint** (`engine/canonical_gex.py`) — `canonical_gex()`
now checks a new tier 0 first: `_alpaca_snapshot_fresh()`, which reads
`gex_calculator.py`'s Alpaca snapshot (`data/trader.db`'s `gex_snapshots`
table) and reshapes it into the same dict shape every other tier returns.
Polygon's three tiers (intraday cache, `flow_gex.db` daily row, live
compute) stay in place as an optional fallback if that subscription is
ever restored — not removed, just demoted, per the scoping doc's
recommendation.

**2. Condition 1 — wall-label fix, not an assertion** (`engine/
gex_scanner.py`): chose to fix the producer's naming rather than add a
raise, since the fix is a one-line change per site and eliminates the
failure mode structurally rather than just detecting it after the fact.
`gex_type = "call_wall" if m["net_gex"] > 0 else "put_wall"` (sign-based,
no positional constraint — the exact defect HM-DRYDOCK A1 named
2026-06-09) is now `"call_wall" if m["strike"] >= spot else "put_wall"`
(position-based) at both the primary and secondary magnet sites. Also
fixed `build_gex_prompt_section()`'s backwards text labels
(`"CALL WALL (support/pin)"` / `"PUT WALL (resistance/accelerator)"` →
`"(resistance)"` / `"(support)"`, matching `ready_room.py` and
`gex_calculator.py`'s existing convention) and the interpretation footer
that reinforced the same backwards framing. A strike below spot with
positive net GEX — the literal HM-DRYDOCK A1 shape — is now labeled
`put_wall` regardless of sign, proven with a regression test built on
exactly that input (`tests/test_gex_scanner_wall_labels.py`), which fails
against the old rule and passes against the new one (verified both ways
via `git stash`).

**3. Condition 2 — freshness gate on the new path, plus a real refresh to
back it.** Two things were needed, not just one, because of something
found mid-build:

- **The Alpaca `gex_snapshots` table was not actually being kept fresh.**
  `main.run_alpaca_gex_refresh`'s scheduler line was commented out since
  the 2026-05-31 canonical consolidation (`# DISABLED HM-GEX-CANONICAL`) —
  confirmed live before touching anything: the table's most recent
  pre-existing row was 2026-09-11, with earlier gaps of a day or more
  between writes, sourced from `ready_room.py`'s incidental direct call to
  `compute_gex_sync`, not any real schedule. Calling this "real-time" would
  have been a claim the system wasn't actually keeping. Fixed:
  `run_alpaca_gex_refresh()` had its old 4-fixed-times-a-day window logic
  removed (that cadence predates Alpaca being canonical and is too sparse
  for it) and now runs on the same 15-min RTH cadence as Polygon's
  equivalent refresher — both scheduled side by side in `main.py` now.
- **`canonical_gex()`'s tier-0 freshness bar** (`ALPACA_GEX_MAX_AGE_DAYS`,
  30 minutes — 2x the refresh interval, same margin convention as
  `HM-SCAN-LIVENESS-WATCHDOG`'s "2x tier cadence" from earlier today):
  a stale Alpaca row is rejected and falls through to the Polygon tiers
  (which will also fail, correctly, since that subscription is dead —
  ending in an honest `error` rather than silently serving old data).
  Every payload still carries `_asof`/`_src`, so the dashboard's existing
  `_gex_is_stale`/`age_days` greying (already wired from the 9/1 fix)
  applies to the new source automatically, no dashboard changes needed.
- **`risk_manager.py`'s direct read got the same discipline.** It has
  always read `gex_calculator.get_latest_snapshot("SPY")` directly (never
  through `canonical_gex`), and had *no* freshness check of its own — the
  exact gap that let it silently run on a day-plus-old row before this fix,
  even though it was never touching the frozen Polygon data. Now reuses
  the same `ALPACA_GEX_MAX_AGE_DAYS`/`snapshot_age_days()` from
  `canonical_gex.py` (imported, not redefined — the "single freshness gate"
  doctrine that module's own docstring already argues for) and skips the
  GEX-based sizing adjustment entirely on a stale row rather than sizing
  off it.

## Tests

`tests/test_gex_scanner_wall_labels.py` (new, 2 tests) — the position-vs-
sign labeling fix, plus the prompt-text label correction.
`tests/test_gex_freshness_gate.py` (extended, +5 tests) — the Alpaca
tier-0's threshold value, used-when-fresh, falls-through-when-stale,
falls-through-when-missing, and that the adapter doesn't corrupt
`call_wall`/`put_wall` values in transit. All 5 new tests plus the
existing 13 pass; the 2 new wall-label tests independently confirmed to
fail against the pre-fix code and pass against the post-fix code
(`git stash` both ways).

**Full suite, correct venv** (`.venv/bin/python3` — using bare `python3`
under-reports pass counts all day today because several test files import
`fastapi`/`alpaca`, only present in the project venv): 1200 passed, 13
failed — all 13 confirmed pre-existing and unrelated (ntfy IPv6, universe
filter, launchd sentinel, riker synthesis cron) via the same `git stash`
before/after diff used all day, zero new failures from this build.

## Verification — restarted, and the two specific reads confirmed live

Backup-first (`trader_prerestart_2026-09-12T183425.db`,
`integrity_check=ok`), market closed (Saturday, confirmed), restart PID
15763→25772, single-writer, orphan-free, clean `ALL SYSTEMS OPERATIONAL`
startup.

Called all three consumers directly against the live venv and the real
DB, post-restart, in the same pass, to confirm they agree on the same
numbers (not three disconnected checks that happen to coincide):

```
risk_manager.py's gate:      accepts row, age=21.6min < 30min threshold
                              call_wall=775.0  put_wall=750.0

providers/base.py's LLM
prompt injection:            "=== ALPACA GEX — SPY [21m old] ==="
                              Call Wall (Resistance): $775
                              Put Wall (Support): $750

canonical_gex() (the
repointed source):            _src=alpaca  call_wall=775.0  put_wall=750.0
                              canonical_gex_if_fresh(): accepted
```

All three read the identical repointed source and agree. **This is as far
as verification can go with the market closed** — no real order flow to
watch the gate act on, no live 15-min refresh cycle to observe (Alpaca's
own `refresh_alpaca_gex()` correctly no-ops on a closed market, confirmed
live: `RiskManager.is_market_hours()` gate inside it returned `[]` when
called directly).

## What Monday's premarket check should confirm

Not "does the code exist" (verified above) — whether the *live cadence*
actually holds under real market conditions:
1. `grep "Alpaca GEX: refreshed" logs/trader.log` shows repeated hits
   roughly every 15 minutes once premarket scanning starts.
2. `data/trader.db`'s `gex_snapshots` table (`source='alpaca'`) shows rows
   landing at that same cadence, not the old sporadic multi-hour gaps.
3. `risk_manager.py`'s GEX-based sizing adjustment fires (or doesn't) off
   a same-session row, not something from before the weekend.

Nothing above is a new code change — Monday's check is confirming the
schedule actually holds under real traffic, which a Saturday restart
can't fully prove on its own.
