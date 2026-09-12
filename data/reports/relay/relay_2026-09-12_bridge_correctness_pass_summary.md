# Relay — 2026-09-12. Bridge Correctness Pass — summary.

RULE #1 held throughout: nothing in this pass deleted, dropped, truncated,
or rewrote a trade or trader history row. Every change was display code,
a notification-text builder, a display-cache fallback rule, or a spec
document. Full detail lives in three companion docs — this one is the
index the Admiral asked for.

- `relay_2026-09-12_bridge_correctness_phase1_trace.md` — Phase 1, the
  read-only trace of all four contradictory-panel reports.
- `relay_2026-09-12_bridge_correctness_phase2_fixes.md` — Phase 2, the 11
  fixes shipped (8 originally listed + 3 more found while tracing Phase
  1), plus the restart/live-verification record.
- `docs/architecture/bridge-facts-endpoint-spec.md` — Phase 3, the
  `/api/bridge/facts` spec. **Not built** — written for review only.

## Data vs. display, at a glance

| # | Item | Verdict |
|---|---|---|
| 1 | Riker "bullish" vs. Tactical Display "BEAR_CROSS" | **Display/labeling.** Two real, correctly-computed regime classifiers on different timeframes (50/200MA vs. 8/21MA), never labeled as such. Riker's output never reaches any execution path — confirmed by grep, not assumed. |
| 2a | `_trades_today()` UTC/local date mismatch | **Live decision-path bug.** Gates real 0DTE order flow (`battle_station_0dte.py:367`). Flagged, not fixed — out of this pass's scope. |
| 2b | Battle Station history mislabeled as "today" | **Display.** Fixed + verified live in-browser. |
| 3 | Sector heatmap zero-filled | **Display/fallback-logic bug**, not genuinely empty data. Fixed + verified live (Finviz's real Saturday zero response was short-circuiting past a stale-disk fallback that had real data). |
| 4 | Riker mid-sentence text | **Display only** (`index.html`'s tokenizer-artifact strip regex, silent — not a stored-data truncation). Fixed. |
| 5 | Shadow CSP "DSR 0.99" on N=3 | **Display** — backend correctly returns the number, frontend wasn't applying `archive_harness`'s own N≥5 floor. Fixed + verified live. |
| 6 | Kirk Advisory "retired path" | **False premise, no bug.** `real_holdings.json` is the live canonical Schwab sync target, not retired; already weekend-staleness-guarded. Documented, not changed. |
| 7 | Hardcoded "Season 5" | **Display.** 3 real hardcodes found and fixed (bridge-v2 was already dynamic). |
| 8 | Leaderboard rank skip / wrong sort | **Rank skip: real display bug, fixed** (benched-player filter didn't decouple from the displayed rank). **Sort-direction: unreproduced** against live data across 3 render paths — documented as open, not fabricated a fix for. |
| 9 | Agent counts disagree 4 ways | **Mostly definitional, not a bug** — Systems Status's arithmetic matches the live DB exactly; the smaller leaderboard count is a deliberately curated subset. Real unification is Phase 3's job. |
| 10 | Archer briefing NUKZ x3 + mangled char | **Both real backend bugs, fixed**: no per-ticker dedup on institutional_signals; a blanket ASCII-strip mangling em-dashes that was protecting a hardcoded title never built from the affected text. |
| 11 | Consensus panel ~300 empty rows | **Display.** Confirmed live (316 tickers, 301 zero-vote). Fixed — shows populated rows plus a collapsed count. |
| 12 | Metals panel contradiction | **Display — field-name mismatch**, not missing data. `d.holdings` checked, real field is `d.positions`; header and expanded view were reading the same response the whole time. Fixed. |

## What shipped

11 fixes across `dashboard/app.py`, `dashboard/static/bridge-v2.html`,
`dashboard/static/index.html`, `engine/archer_morning_synthesis.py`,
`engine/premarket_scanner.py`. `py_compile` and `node --check` clean on
every file. Two of the riskier fixes verified live in-browser against
the running trader (bridge.ollietrades.com); the rest verified by calling
the live functions/endpoints directly against real data. Two backup-first,
single-writer-gated restarts this session (Admiral-authorized outside
market hours) — code is live in the running process, not just on disk.

## What's flagged, not fixed (by design — out of this pass's scope)

- **`_trades_today()`'s UTC/local date mismatch** (`engine/battle_station_0dte.py`) —
  gates real 0DTE order flow. This is a live decision-path bug; fixing it
  needs its own dedicated pass, not folded into a display-correctness
  sweep.
- **The leaderboard sort-direction claim** — could not reproduce against
  live data on any of 3 render paths checked. If it recurs, a screenshot
  with the active sort-button state would pin down which code path is
  actually firing.

## What Phase 3 would cost

Full estimate and phasing in `docs/architecture/bridge-facts-endpoint-spec.md`.
Short version: ~1 session for the endpoint + core facts (additive, low
risk), ~half a session for a pilot panel migration to validate the
pattern, then 2-4 more sessions spread out to migrate the rest,
prioritized by which panels this audit actually found wrong (regime,
metals, agent counts, sector data) rather than by convenience. Not
started — spec only, per instruction.

## Not touched (per explicit instruction)

GEX CBOE repoint, Phase 1.3, the un-alias leftovers, anything in the
trading decision path (including the one live bug this pass found and
flagged, item 2a above).
