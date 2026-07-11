# Relay: HM-ERROR-FILTER-CONSOLIDATION decided — KEEP, not FIX (bigger problem found underneath)

**Date:** 2026-07-11
**Commit:** `f255dab`

## What was asked

"decide keep vs fix on the error filter consolidation."

## Decision: KEEP. No code touched.

Went to implement the "shared `engine/error_filter.py`" fix the earlier
scoping pass recommended, and traced `trader_error.log`'s actual origin
before writing any code — same discipline as the desk-chain-provenance
decision earlier today.

**Found it's a bigger problem than a formatter tweak.**
`trader_error.log` is plain shell stderr redirection
(`scripts/trader_restart.sh:98`: `nohup ... >> trader.log 2>> trader_error.log`),
not a single Python `logging` handler with one controllable format
string. Every independent stderr-writing call site across the codebase —
`console.log`/`logger` calls with their own ad-hoc prefixes, plus raw
Python tracebacks that have no timestamp at all — lands in this file
verbatim, in whatever shape that call site happens to produce. There's
no single source to patch. Making it genuinely, correctly
date-filterable would mean auditing and standardizing many independent
logging call sites codebase-wide — a real multi-file project, not a
"consolidate a filter function" ticket.

**Practical impact of keeping as-is is low:**
- `daily_report.py`'s broken error count is archival-only — 10 PM cron,
  writes to `drafts/DAILY_REPORT_<date>.md` only, **no ntfy push**
  (confirmed via its own docstring and cron entry). Nobody's actively
  alerted by this number.
- `eod_report.py`'s actively-pushed count (the one that matters day to
  day) is unaffected by this decision either way — it already correctly
  counts everything from `trader.log`. Its own blind spot against
  `trader_error.log`-only errors has existed unnoticed this whole time
  regardless of today's decision, and fixing it requires the same bigger
  standardization work either way.

**Real follow-up identified, explicitly not actioned:** if
`trader_error.log`'s errors should ever become visible in the
actively-pushed `eod_report.py`, that's its own dedicated project
(standardize log line prefixes across the codebase's stderr-writing call
sites) — genuinely bigger than "error filter consolidation," worth
scoping separately if it becomes a priority, not something to rush under
a same-turn keep-vs-fix call.

## Verification

- `scripts/daily_report.py:33` (`ERROR_LOG = trader_error.log`), `:204-218`
  (`get_error_summary`), `:3-7` (docstring: `Cron: 0 22 * * 1-5`, no ntfy
  push referenced anywhere in the file).
- `scripts/trader_restart.sh:21,98` — confirmed the exact shell
  redirection that produces `trader_error.log`.
- `logs/trader_error.log` tail sampled directly — confirmed `HH:MM:SS
  [TAG] ...` format, no per-line date, consistent with the earlier
  scoping pass's finding.

## This closes the "1 through 4 scoping" batch

All four items from the original batch now have an explicit decision:

| Item | Decision |
|---|---|
| 1. Weekly digest push | **Shipped** (`scripts/weekly_digest.py`, cron proposed not installed) |
| 2. HM-SHADOW-PIPELINE-COST-AUDIT | **Killed** (code retired, live flag flipped, cron removed, trader restarted) |
| 3. HM-DESK-CHAIN-PROVENANCE | **Kept** (execution_status is correct-by-design; kill-gate urgency claim was false, corrected) |
| 4. HM-ERROR-FILTER-CONSOLIDATION | **Kept** (real underlying problem is bigger than scoped — no single log source to fix) |

## Open items

None new from this block. `HM-STATUS-PAGE-STALE-CACHE` remains the one
outstanding pre-flight item still needing a Cloudflare dashboard change
only you can make.
