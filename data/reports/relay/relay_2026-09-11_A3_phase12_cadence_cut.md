# Relay — A3: Phase 1.2 cadence cut built. 2026-09-11

## Required prerequisite: McCoy cadence-by-hour breakdown (was flagged "not yet built")

Measured directly from `decision_audit` (last 48h, `event_type='signal_emit'`,
`player_id='ollama-plutus'`): **4,191 signal_emits total, ~87-249/hour,
no flat pattern (real hour-to-hour variance).** Cross-checked against
`_SCAN_TIER2`'s actual configured interval (`_TIER2_INTERVAL = 7200s` = 2h,
confirmed in code, not just comment) and `run_scanner()`'s mechanism: each
tier-2 firing scans the **full active universe** (`get_active_universe()`,
600-900 symbols) via `arena.run_scan(_captured_stocks, ...)` — not a
per-symbol tier gate. That resolves the mystery the plan flagged ("tier
mechanism alone doesn't explain the observed cadence"): McCoy fires ~12
times/day (every 2h) and each firing produces on the order of 100-250 real
`signal_emits` (whichever symbols in the full universe clear whatever
internal pre-LLM filter `run_scan()` applies) — averaging to the observed
~2,095/day. This is the real number Phase 1.2's twice-daily-top-100 design
needs to beat, and confirms the design is sized about right: 2 firings/day
x 100-symbol cap ≈ 200, matching the target directly.

## Built

- `engine/mccoy_screen.py` (new) — `get_mccoy_screened_symbols(limit=100)`,
  no LLM calls, reuses existing infra rather than inventing a new signal:
  **Volume Radar** (`engine/volume_scanner.py::get_todays_volume_alerts`,
  the already-running 10,000-stock 15-min funnel, literally named "Volume
  Radar" in its own docstrings) + a **liquidity floor** + **regime**
  (`engine/regime_detector.py::detect_regime`, attached for context, not
  a hard filter).
- `main.py`: removed `ollama-plutus` from `_SCAN_TIER2` (stops the old
  every-2h/full-universe firing). Added `run_mccoy_screened_scan()`,
  scheduled `every(5).minutes` with the same slot-window pattern as
  `run_team_advisor()` (fires 9:35 AM ET pre-open, 12:30 PM ET midday) —
  deliberately NOT `schedule.every().day.at("HH:MM")`, which this
  codebase has already been bitten by three times (phase-drift on
  restart, see `run_kirk_advisory_job`/`run_cto_advisory` comments).

## Real false start, caught before shipping

First design intersected Volume Radar hits with `engine.universe.
get_active_universe()` for the "liquidity" leg — **live-verified this
produces 0/90 candidates.** `get_active_universe()` is a ~307-name
mega-cap-only pool ($5B+ cap, $100M+/day volume, built for long-hold
fleet coverage) with near-zero overlap against actual volume-radar hits
by construction — a volume-explosion scanner's whole point is to surface
names moving unusually today, which skews toward small/mid caps the
mega-cap floor excludes. Redesigned: liquidity floor applied directly on
Volume Radar's own fields instead (`price >= $1.00`, `dollar_volume >=
$1,000,000` — well above the observed p25 of ~$59K on live data, filters
real noise without re-imposing the mega-cap bar). Re-verified live:
34/100 candidates (early in the trading day, Volume Radar hasn't
accumulated a full day's hits yet — honest shortfall, not padded, matches
the module's documented ABSTAIN posture), real recognizable liquid names
(RWT, COO, FCX, SCCO, CIFR, AG, AU, SOXL/SOXS, ...).

## Not yet live

Trader is stopped (dry-dock) — this wiring takes effect on the next
restart (item D16). `main.py` compiles clean; `run_mccoy_screened_scan`
not exercised end-to-end against a running `arena` object tonight (would
require starting the trader, out of scope for dry-dock). Worth a live
smoke check at undock specifically for this function's first real fire.
