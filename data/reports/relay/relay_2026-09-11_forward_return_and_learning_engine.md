# Relay — forward-return measurement + learning_engine investigation. 2026-09-11 (dry-dock)

Two questions requested before Phase 1.3 could be speced. Both answered;
spec follows in `docs/XO_PLAN_2026-09.md` (Phase 1.3 section, superseding
the prior Gate-7-based draft).

## Question 1: are the rejected high-confidence BUYs a goldmine or noise?

Top 300 highest-confidence REJECTED McCoy BUY signals, last 30 days
(confidence range 0.88-1.00, avg 0.93). `reference_price` mostly empty
(Phase 1.1 only started populating it 2026-09-09, most of this sample
predates that) — used close-to-close daily returns from yfinance instead:
anchor = last close at/before the signal's date, forward = next trading
day's close. 128 unique symbols, 297/300 resolved (3 had no forward day
available yet).

**Result: n=297, mean=+0.171%, median=-0.212%, win_rate=45.8%, stdev=2.67%.**

**SPY baseline over the same 45-day window, same methodology:**
mean=+0.034%, median=-0.073%, win_rate=47.7%.

The rejected population edges out SPY on mean (likely right-tail-driven —
best single case +10.0%, worst -10.3%, individual-stock variance is much
higher than an index's) but is **at or slightly below SPY on the two
more outlier-resistant metrics — median and win rate.** This is not the
profile of a systematically profitable population being wrongly
suppressed (that would show a win rate meaningfully above 50% and a
positive median, not a coin-flip-or-worse one).

**Verdict, per the decision framework given: this is closer to "gates are
earning their keep, fix is upstream at signal quality" than "loosen the
gates."** It doesn't cleanly land in either stated bucket (the rejected
population didn't clearly lose money either), but the honest reading is
that raw stated confidence — even at 0.88-1.00 — is not reliably
identifying good trades among the rejected set. That's consistent with,
and extends, the earlier calibration-map finding (93% stated confidence
-> 78.6% realized hit rate on *executed* trades): the overconfidence
problem shows up on both sides of the gate. **Phase 1.3 should not
loosen the gate chain on the strength of this population — if anything
it argues for tightening around calibrated (not raw) confidence.**

## Question 2: what is engine/learning_engine.py, and does it know about calibration_map.py?

**What it is — a real, currently-live, two-part system**, not something
obscure or dead:

1. **`apply_learning(player_id, trade_signal)`** — called from `ai_brain.py`
   before every execution attempt (the "Blocked by learning engine" 11.1%
   reject category from the funnel). Applies up to 7 adjustment types
   read from `model_adjustments` (one row per type, most-recent wins):
   `confidence_modifier` (multiplies stated confidence), `regime_filter`
   (blocks BUY/SELL in specific regimes), `position_size` (caps trade
   value as % of capital), `stop_loss` (overrides stop %), a **ticker
   blacklist** (3+ losses in 14 days from `daily_lessons`, blocks unless
   confidence >= 0.9), `ghost_promotion_override`, and `cooldown` (minimum
   minutes between trades).
2. **`get_learning_context(player_id)`** — injects a summary of recent
   lessons/score/active-adjustments directly into the model's own scan
   prompt — a separate, prompt-level feedback loop, not just a post-hoc
   filter.

**Data source**: `model_adjustments` + `daily_lessons` + `model_scores`,
written by `engine/crew/daily_review_crew.py` (daily) and `engine/crew/
weekly_tuning_crew.py` (weekly) — CrewAI-based automated review agents,
**both still scheduled in `main.py`** and genuinely still running (wrote
fresh rows for `desk-manual` on 2026-09-09, and a batch of
`audition_proposed` entries for several other players on 09-07).

**For McCoy specifically: stale.** Last `model_scores` row: **2026-07-09**.
Last `model_adjustments` row: **2026-07-13**. Over two months, while the
pipeline itself is demonstrably alive for other players right now — McCoy
has silently fallen out of its coverage. Root cause not investigated
tonight (would need to read `daily_review_crew.py`'s player-selection
logic) — flagging as its own finding, filed to `docs/XO_BACKLOG.md`.

**McCoy's currently-active adjustment, frozen since 2026-07-08:
`confidence_modifier = 0.8`** — every stated confidence gets multiplied
by 0.8 before anything downstream sees it (including `risk_manager.
UNIVERSAL_MIN_CONVICTION=0.65`), set by one LLM-judged weekly review two
months ago and never revisited since.

**Second dead-field finding**: `model_scores.confidence_calibration` is
supposed to hold a 0-100 LLM-judged calibration score
(`weekly_tuning_crew.py` prompts for it explicitly) — **every observed
row for every player is exactly `0.0`.** Same "field exists, never
populates" pattern as `daily_snapshot.master_grade` and `signal_history.
grade` found earlier tonight.

**Relationship to `engine/calibration_map.py` (built earlier this
dry-dock): genuinely different mechanisms, and they do NOT currently
know about each other.**

| | `learning_engine.apply_learning` | `calibration_map.get_calibrated_confidence` |
|---|---|---|
| Method | Flat multiplier, one LLM-judged number per player | Binned, empirical, stated-confidence -> realized-hit-rate, per regime |
| Source | Weekly LLM review (`weekly_tuning_crew`) | `decision_audit` + `trades`, computed offline |
| Live today | Yes (multiplier applied on every McCoy call) | No (built, not wired in) |
| Freshness | Frozen since 2026-07-08 for McCoy | Computed live from the last 30-90 days on each call |

**If Phase 1.3 wires `calibration_map` in without accounting for this,
McCoy's confidence gets adjusted twice, uncoordinated**: stated 0.93 ->
learning_engine's frozen x0.8 -> 0.744 -> calibration_map would then
apply its own correction computed from *historical* confidence values
that already had the 0.8 baked in for some of the sample and not others
(the multiplier changed over time, per the `model_adjustments` history:
0.8 since 07-08, different values before). That's a real double-counting
risk, not a hypothetical one. Phase 1.3's spec addresses this directly.
