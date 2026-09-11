# Relay — McCoy's learning-engine coverage lapse: root-caused and fixed. 2026-09-11

## Root cause

`engine/crew/daily_review_crew.py::run_daily_review()` (scheduled Mon-Fri
1:15 PM MST) was gated entirely on **players who executed a trade that
specific day**: `_get_today_trades()` pulls from `trades`, `player_ids =
set(t["player_id"] for t in trades)`, and `model_scores` only ever gets a
row (previously) for a `player_id` inside that set. Not a filter, join,
or `halt_mode` bug — simpler and more structural: **the diagnostic tool
needs a trade to run, but McCoy's actual problem is that almost nothing
converts to a trade** (1 real trade in the last 30 days, per the earlier
funnel trace). He fell out of his own calibration pipeline for exactly
the reason the pipeline exists to catch.

**Confirmed this isn't McCoy-specific.** Live query, 8 `halt_mode='active'`
players, before the fix: only `desk-manual` had a `model_scores` row
newer than 2026-07-10. **`qwen3-8b-flash` (Worf) was equally stale**,
frozen at the identical 2026-07-09/10 date — same structural cause, a
second real LLM decision seat silently affected. (`capitol-trades` also
showed 07-10, though it's rules-based, not an LLM-confidence candidate in
the same sense.) `enterprise-computer`/`m5-allocator`/`options-sosnoff`/
`trade-desk` have never had a score at all — correctly, since they're
human-operated or non-LLM seats outside this pipeline's real scope.

## Fix — shipped, live-tested

`_flag_no_trade_active_players()` (new function, same file), called
**unconditionally** at the top of `run_daily_review()`, before the
`if not trades` skip gate — so a zero-fleet-trade day no longer also
skips this. For every `halt_mode='active'` player not in today's trade
set, checks for real `signals` activity today; if present, writes a
`model_scores` row that **carries forward their most recent real score
values** (never fabricates a new grade — there's nothing to grade
without a trade) with today's date, tagged in `data_window` as
`"no-trade-day, N signals, carried forward"` so it's never confused with
a genuine LLM-graded review. No LLM calls — pure DB read/write, zero
added cost or timeout risk to the existing crew.

**Live-verified** (ran directly, safe during dry-dock — doesn't touch
the trading path): flagged 2 players (`ollama-plutus`, `capitol-trades`)
who had signal activity today with zero trades. `ollama-plutus`'s
`model_scores` freshness: 2026-07-09 -> 2026-09-11.

**Honest gap, not silently claimed fixed: `qwen3-8b-flash` (Worf) is
still stale.** His real current activity happens through `portfolio_advice`
(the Advisory Team path, see the earlier pipeline-trace relay doc), not
`signals` — this fix's activity check only looks at `signals`, so Worf
doesn't get freshness-flagged by it. Would need the check broadened to
also cover `portfolio_advice` for advisory-role players; not done
tonight, flagged here rather than overclaimed.

## Not changed

`weekly_tuning_crew.py` (Sunday 9 PM MST) was not investigated the same
way — it has its own player-selection logic, not traced tonight. The
`model_scores.confidence_calibration` field's own separate problem
(always exactly `0.0` for every player, every row, despite being
LLM-prompted) is unrelated to this coverage gap and is addressed
separately in the Phase 1.3 confidence-coordination spec.
