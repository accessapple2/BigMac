# Relay — 2026-09-12. Trades-table UTC/local-date gate fix (full pass).

RULE #1 respected throughout: no `trades`/`battle_station_trades`/`crew_decisions`
row was deleted or rewritten. The only data change is additive — a new
`tz_bucket_suspect` column, set on exactly 61 pre-existing row ids, default
0 everywhere else. Every code change is gate/dedup logic; no historical
row's stored values changed.

## What shipped

**One canonical helper**, `engine.market_calendar.local_day_utc_bounds(d=None)`
— returns `(start_utc, end_utc)` as canonical `"YYYY-MM-DD HH:MM:SS"` UTC
strings bounding one Arizona calendar day (`d=None` = today). Arizona has no
DST, so this is exact with no seasonal edge case. Same consolidation doctrine
as `is_trading_day()` in the same file, applied to day *ranges* instead of
day *predicates*.

**15 call sites fixed across 10 files** — every one converted from an ad hoc
`date(<col>)=?` string match (bound to a Python-local `date.today()`/
`datetime.now()`) to `datetime(<col>) >= ? AND datetime(<col>) < ?` bound to
the helper's UTC range. `datetime()` (not raw string comparison) because two
different literal formats are live in this repo — SQLite `CURRENT_TIMESTAMP`'s
`"YYYY-MM-DD HH:MM:SS"` and `datetime.now(timezone.utc).isoformat()`'s
`"YYYY-MM-DDTHH:MM:SS.ffffff+00:00"` — and `datetime()` normalizes both
(verified empirically before relying on it).

The original 9 (8 sites + `battle_station_0dte`, as scoped):
- `engine/battle_station_0dte.py::_trades_today()` — the original finding.
- `engine/risk_manager.py` — fleet-wide bear-standdown cap (max 3/day, all
  agents) AND the per-player daily trade limit (both in this file).
- `engine/paper_trader.py` — redundant per-player limit re-check.
- `engine/trade_gateway.py` — a third independent per-agent daily-limit layer.
- `engine/m5_allocator.py` — once-per-day dedup.
- `engine/capitol_fund.py` — once-per-symbol-per-day BUY dedup.
- `engine/crew_scanner.py` — the duplicate hardcoded `capitol-trades` dedup.
- `engine/dayblade.py::get_dayblade_stats()` — display stats (lower severity,
  fixed anyway per "full trades-table scope").
- `main.py` — portfolio-summary `trades_today` (display, fixed anyway).

**6 more found while building the regression test**, fixed for the same
"no ad hoc `date()` string matching left anywhere in a gate" consistency,
not because they were separately requested:
- `engine/crew_scanner.py::_count_today_trades()` and a second
  `crew_decisions`-table `capitol-trades` signal-level dedup a few lines
  below it — both real gates, both local-naive, same table class as `trades`
  (execution-decision tables), not one of the deferred ~40 reporting sites.
  Fixing the second one also fixed a `NameError` bug I'd have otherwise
  introduced myself: it referenced `_cap_date` (an import I removed from the
  first dedup block above it in the same function, not realizing the name
  was shared) — caught by rerunning the regression scan before considering
  this done, not by inspection. Worth flagging as a reminder to actually run
  the check, not just trust the diff.
- Three `engine/crew_scanner.py` ollie-auto `traded_today` dedup sites and
  one `_run_spock_risk_alerts()` fleet P&L site — these were **already
  internally consistent** (`datetime.now(timezone.utc)` matching the UTC
  column, genuinely no bug), but still used the ad hoc `date()` string shape
  the directive said to eliminate everywhere. Converting them changes their
  "today" boundary from a UTC calendar day to an Arizona calendar day — a
  real semantic alignment (now the same "trading day" every other gate uses),
  not a no-op, but also not a bug fix since nothing was wrong before.

**Left alone, as scoped**: the ~40 other `date(col)=?` sites on non-execution
tables (briefings, journal entries, signal dedup, cost tracking, etc.) —
including two more `trades.executed_at` sites surfaced during this pass,
`engine/self_improvement.py:44` and `engine/ai_journal.py:34`, both read-only
summary/journal generators, not gates. Filed as the lower-priority sweep,
not touched.

## Regression test

`tests/test_trades_tz_gate_regression.py`, three tests:
1. **Pattern scan** (same shape as `scripts/ci/grep_gate_scan.py` — reuses
   its actual `_blank_prose()` comment/docstring stripper) over the 9 gate
   files: fails if `date(executed_at)=` or `date(timestamp)=` reappears
   anywhere in them, regardless of wording — catches the pattern, not the
   old instance. Explicitly scoped to these files, not the whole repo, so it
   doesn't fire on the deferred ~40.
2. **Helper correctness**: uses the real historical case (qwen3-8b-flash
   trade id 1780, an 8:55pm Arizona trade stored as UTC 2026-04-25 03:55:42)
   to assert it falls inside its own local day's bounds and outside the
   previous day's.
3. **The double-counting half**: asserts that same row falls outside the
   *next* local day's bounds too — the exact mechanism that let one evening
   trade get counted twice (missed today, credited to tomorrow).

Fixing `engine/m5_allocator.py::_traded_today()` broke one existing test
(`test_run_m5_rebalance_skips_if_already_traded_today`) that inserted a
trade relying on SQLite's real wall-clock `CURRENT_TIMESTAMP` default while
mocking `az_now()` to a fake date — the old code's real-wall-clock
`date.today()` coincidentally matched the real-wall-clock insert; the new
code correctly compares against the *mocked* day and no longer does.
Fixed the test to set `executed_at` explicitly inside the mocked day,
matching what a real trade in that scenario would look like. Full suite
diffed before/after (`git stash` isolation): **51 pre-existing failures
(unrelated modules — auth, GEX freshness, ntfy IPv6, universe filter, riker
synthesis cron, etc.) before this pass's edits, 46 after — the 5-line
difference is exactly the 2 m5_allocator tests this fix corrected plus the
3 new regression tests, zero new failures introduced.**

## Data flag

`trades.tz_bucket_suspect INTEGER DEFAULT 0` — added via `setup_db.py` (same
idempotent-column-add pattern as the existing `pnl_basis_invalid` flag,
runs every startup, no-ops once present) and backfilled once, directly,
on exactly the 61 previously-reported affected row ids (verified: exact set
match against the earlier report, `webull`'s 127 date-only-import artifacts
untouched, total row count unchanged at 2786, `PRAGMA integrity_check=ok`
on a fresh backup taken immediately before the restart below).

## Restart + verification

Backup-first: `data/backups/trader_prerestart_2026-09-12T172925.db`,
`integrity_check=ok`, confirmed 61 flagged rows present. Market closed
(Saturday, `is_trading_day()` confirmed False) — restart authorized per
standing outside-market-hours authorization. `trader_restart.sh`: single
writer, PID 15763, orphan-free, bound `:8080`. `trader_error.log`: clean
`SEASON 6.3 — SATURDAY STARTUP` → `ALL SYSTEMS OPERATIONAL — ENGAGE`, no
errors.

**Verified the fix is actually live**, not just committed: with the market
closed there's no real order flow to exercise the gates naturally, so I
called the fixed functions directly, in the live venv, against the real
`data/trader.db` — `battle_station_0dte._trades_today()`,
`m5_allocator._traded_today()`, `capitol_fund._already_bought_today()`,
`crew_scanner._count_today_trades()`, `dayblade.get_dayblade_stats()` — all
five executed cleanly and returned the correct zero/false values for a
market-closed Saturday, using the new range-bound query path (confirmed by
the regression test's pattern scan finding zero ad hoc `date()` matches left
in any of these files). This is the same "call the live function directly"
verification standard used earlier this session for the Phase 2 display
fixes, applied here since these are internal scheduler/gate functions with
no HTTP endpoint to hit.

## What this actually cost — confirmed vs. exposure-only

**The miscounting mechanism is confirmed and demonstrated with a real
number**, not just theoretical: `qwen3-8b-flash` (Worf), local Arizona day
2026-05-09 — the OLD query (`date(executed_at)='2026-05-09'`) returned
**1** (a phantom count from a leftover 2026-05-08 evening trade whose UTC
date rolled to 05-09); the NEW range-bound query correctly returns **0**
(zero real trades yet that Arizona day). That's a live gate input that was
objectively wrong, not a hypothetical.

**Whether that specific miscount — or any other among the 61 — actually
flipped an accept/reject decision: not confirmed, and I looked.**
`gate_reject_log` (the audit trail that records every `MAX_TRADES_REACHED`
rejection) only exists from **2026-05-26** onward. **55 of the 61 affected
trades predate it entirely** — there is no record to check either way for
those, so absence of a found incident there is not evidence of absence, it's
an audit gap. For the **6 that postdate it** (navigator ×4 on 2026-05-27,
ollie-auto ×2 on 2026-08-25), I queried `gate_reject_log` directly for
`MAX_TRADES_REACHED` entries for those two players around those dates:
**zero rows**. No confirmed wrong block found in the one window where it
was actually checkable.

One correction to the read-only report from this morning: I'd flagged the
`capitol-trades` SJM row (id 2316) as a plausible false-block candidate for
its dedup gate. Re-verified while building this fix and that's wrong — the
row is a **SELL**, and the dedup only guards `action='BUY'`, so it was never
in that gate's path regardless of the tz bug. Correcting it here rather than
letting it stand.

**Plain answer to the question that matters for prioritizing the rest**:
this is **exposure with a demonstrated mechanism, not a confirmed bad
historical outcome**. The bug was real, live, and provably fed at least one
gate a wrong number — but I found no evidence (and had the means to check
for only 6 of 61 instances) that it actually blocked or allowed a trade it
shouldn't have. Given the trades-table gates — the ones that cost capital —
show no confirmed incident despite a real, checkable mechanism, the ~40
remaining `date(col)=?` sites (all lower-stakes than execution gates) are
a reasonable lower priority to leave as filed rather than chase urgently.
