# Relay: XO-DEPARTURE-HARDENING remaining items — 1 shipped, 3 scoped

**Date:** 2026-07-11
**Commits:** `b20d6bb` (digest push code), `547b425` (shadow-pipeline +
error-filter scoping), `dc03c1b` (desk-chain-provenance scoping)

## What was asked

"Start the weekly digest push" (Phase 3 item 8), then mid-turn: "also
start on 1 through 4 scoping" — all four remaining open items from the
last status check: weekly digest push, HM-SHADOW-PIPELINE-COST-AUDIT,
HM-DESK-CHAIN-PROVENANCE, HM-ERROR-FILTER-CONSOLIDATION.

## What shipped

**1. Weekly digest push — built, tested, NOT cron-installed (propose-first).**
`scripts/weekly_digest.py`: sweep summary (latest clean-window fleet-realism
report, excluding `.INCOMPLETE_*` test files) + tuning results (reads back
`model_scores`/`model_adjustments` already persisted by the Sunday tuning
crew — never re-invokes it, since that makes LLM calls and unattended
automation here must stay deterministic) + audition clocks (delegates to
the existing `track_incumbent_auditions()`, already suspension-aware) +
30-day spend (deliberately includes ids outside `ai_players` — this is the
direct structural fix for the exact blind spot item 2 below investigates).
One `send_alert()` push, mirrors `eod_report.py`'s shape. 12 tests, dry-run
verified against real `trader.db`:

```
Weekly Digest — week of 2026-07-11
Sweep (fleet_realism_sweep_clean_20260705_065111.json): 5 scored (17 no-data), guarded P&L $235.80 across 56 trades
  Top: ollama-qwen3 ($1,047.55)  Bottom: capitol-trades ($-244.86)
Tuning: 17 models scored (avg 45.882), 74 adjustments saved
Audition options-sosnoff: SUSPENDED — 0/20 broker-executed
Audition qwen3-8b-flash: SUSPENDED — 0/20 broker-executed
30d spend: $52.28
  off-roster (not in ai_players): wr-shadow-v1=$22.10, wr-witness=$15.06, wr-shadow-v7d=$11.22, ab-witness-deepseek-r1=$0.72, ab-witness-gpt-oss=$0.66
```

**Proposed cron (not installed):** `0 23 * * 0` — after both the tuning
crew (~21:00-21:30 MST) and the sweep (22:10 kickoff) finish. Needs an
explicit go-ahead before `crontab -e`, same durability posture as every
other cron proposal in this doc.

## Three scoping investigations (parallel agents, read-only, no code changed)

**2. HM-SHADOW-PIPELINE-COST-AUDIT — verdict: not dead spend.** The
$50.24/30d flagged earlier is 100% historical. Cost has been **$0.00/day
for every wr-shadow-*/wr-witness call since 2026-07-06** — already fixed
under a separate 2026-06-29 cost-discipline pass, confirmed by direct SQL
(volume continues at ~200-230 calls/day/id, cost stays exactly zero). The
real open item is compute waste, not dollars: both pipelines write to
tables (`plutus_shadow_critiques`, `witness_ab`) that nothing downstream
ever reads. **Two new anomalies surfaced, flagged for you rather than
silently touched:** `SHADOW_WITNESS_ENABLED` is live-overridden to `true`
in the `settings` table against `config.py`'s `False` code default —
unknown who/when set that; and `witness_ab_scorer.log` shows a live
"too many open files" connection-failure bug against the Ollama host,
unrelated to cost.

**3. HM-DESK-CHAIN-PROVENANCE — both root causes traced, fix scoped
forward-only.** The 72% `signal_id` mislink rate re-confirmed exactly via
a fresh independent query (54/75 resolved links, 47/65 on a separate
sample). The intended write path looks correct in code, but the actual
corrupted rows (cross-player, cross-symbol, ~3 months apart, near-
sequential IDs) don't fit any visible write path — genuinely unresolved
which exact site produces them; only ~3% of trades even carry a
`signal_id` (added 2026-05-20), so this is narrow and recent, not
systemic historical corruption. `execution_status` never reaching
`EXECUTED` is fully root-caused: a hardcoded 5-player legacy whitelist
(`_EXECUTION_PORTFOLIO_BY_PLAYER`) gates the only code path that can set
it, and none of the players actually producing real fleet trades are on
that list — architecturally unreachable, not a string/timing bug. Fix
proposed for both is forward-only; explicitly recommended AGAINST a
backfill (same "retrospective join is a dead end" trap already documented
for `acted_by_fleet`). Recommended the two 2026-07-24 kill-gates filter on
`executed_at >= <fix-deploy-date>` once shipped, so they don't silently
inherit this blind spot on historical rows.

**4. HM-ERROR-FILTER-CONSOLIDATION — re-scoped bigger than originally
filed.** The `[LRS]`-blanket-exclusion bug in `daily_report.py` is real,
but it's currently masked by a separate, more severe bug: `trader_error.log`
lines have no per-line date prefix at all, so BOTH `eod_report.py`'s and
`daily_report.py`'s date filters silently fail to read that file's errors
by date. Confirmed live: `eod_report.py`'s "genuine errors" count today
is 100% from `trader.log`, zero contribution from `trader_error.log`
despite it being in `LOG_PATHS`. Recommended fix:
`engine/error_filter.py` (mirrors the existing
`engine/trades_filter.py::CLEAN_TRADES_WHERE` precedent), but flagged
that `daily_report.py`'s reported error count will jump visibly
(~0 → several hundred/day) once both the allowlist and date-filter bugs
are fixed together — needs sign-off before shipping, not a silent number
change.

## Testing / verification

- `weekly_digest.py`: `py_compile` clean, 12/12 tests pass, dry-run
  against real `trader.db` verified end-to-end.
- Full suite: 1045-1059 passed depending on run (same known flaky bbkc/
  m5_allocator/quality_gate/universe_filter/war_room set toggling pass/
  fail between runs, confirmed unrelated across many sessions this week —
  not a regression from this block's changes).
- The three scoping investigations are read-only research — no code was
  changed by them, only `docs/XO_BACKLOG.md`.

## Open items after this block

- Weekly digest cron still needs an explicit go-ahead to install.
- HM-DESK-CHAIN-PROVENANCE's mislink write-site trace needs its own
  dedicated follow-up session (the `execution_status` half is fully
  scoped and ready to implement whenever prioritized).
- HM-SHADOW-PIPELINE-COST-AUDIT needs a keep-vs-kill decision on the
  write-only shadow/witness pipelines, plus input on the two anomalies
  (config drift, connection-failure bug).
- HM-ERROR-FILTER-CONSOLIDATION needs sign-off on the visible error-count
  jump before the fix ships.
- `HM-STATUS-PAGE-STALE-CACHE` — still needs a Cloudflare dashboard change
  only you can make.
