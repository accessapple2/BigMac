# McCoy (ollama-plutus) Risk/Reward Diagnosis

**Status:** Analysis only. No live changes made. Parameter changes below are
proposals — owner sign-off required before any are applied.

**Agent identity confirmed:** `ollama-plutus` / "Dr. McCoy" (model tag
`plutus-v1`, currently aliased to `qwen3:8b` per the ongoing Ollama-alias
situation — attribution void until 2026-09-04, but that affects *which
model* gets credit, not the P&L math below, which is real dollars). Not to
be confused with the retired `mccoy-bps` (Bull Put variant, `halt_mode='full'`,
zero trades ever recorded).

**Analysis window:** her most recent 75 closed stock trades,
2026-05-15 → 2026-07-09 (season 6). This is the exact window that reproduces
the cited numbers to the penny — confirmed by brute-force search across
window sizes, not assumed:

| | 75-trade window (this report) | Ticket's cited figures |
|---|---|---|
| Win rate | 84.0% (63W / 12L) | 84% |
| Gross realized gain | $200.76 | $200.76 |
| Gross realized loss | -$535.83 | -$535.83 |
| Profit factor | 0.375 | 0.37 |

Her full all-time record is 209 raw transaction rows (49 BUY + 160 SELL,
matching the ticket's "209 trades") — but the full 160-trade history is
actually **net positive** ($4,388 gross win / -$580 gross loss, PF ≈ 7.6),
driven by four large May 2026 MU fills. The 0.37-PF picture is specifically
her *recent* run, not her lifetime record — worth knowing before reading
"profit factor 0.37" as her permanent character rather than a recent-window
symptom.

## Headline numbers

- **Avg win: $3.19. Avg loss: -$44.65. Ratio: 14.0x.**
- **Avg win %: +6.24%. Avg loss %: -11.05%** (only ~1.8x on a % basis —
  the dollar asymmetry is far more extreme than the % asymmetry, see
  position-sizing finding below).
- **Avg loss position size: $244.39 ($188.92 excluding the outlier below).
  Avg win position size: $68.90.** Losing trades are being sized **2.7-3.5x
  larger** than winning trades.

## Finding 1 — one trade is 68% of the entire loss column

**HIMS, 2026-06-12, -$366.12, -42.8%.** Entered 2026-06-01 at $1.55/share
(551.2976 sh, confidence=1.0), held **11 days**, exited at $0.886/share.

This is not a normal loss — every *other* loss in the sample (11 trades)
closed within a tight band, -8.04% to -8.29%. HIMS is a 5x-larger percentage
loss than the next-worst, and by itself accounts for $366.12 of the $535.83
total loss column (68%). **Exclude it and the picture changes completely:**
gross loss becomes -$169.71, profit factor becomes 200.76/169.71 ≈ **1.18**
— still not great (dollar win/loss is still lopsided, see Finding 3), but a
fundamentally different diagnosis than "systematically loses big." An 11-day
hold that ran to -42.8% without being cut looks like a stop-enforcement
gap on this specific position, not typical behavior for this agent.
**Recommend: pull the position-monitoring log for this exact position
(2026-06-01 through 2026-06-12) to determine whether a stop-loss order was
ever placed/checked for it, before treating this as representative.**

## Finding 2 — the other 11 losses cluster at -8.16% (σ=0.10%), not -12% or -18%

Every one of the 11 non-HIMS losses has **confidence=1.0** and is dated
2026-06-09 through 2026-07-09 — all well after the conviction-scaled stop
system's tier-floor fix (2026-05-25) and the `RiskManager.get_stop_loss_pct`
staticmethod-shadowing bug fix (`HM-AGENT-RULES-CONSOLIDATION`, 2026-07-04,
which had been silently applying an outlawed 8% stop instead of the
canonical `engine.stops.get_stop_loss_pct` tiers). Under the current,
documented logic, a confidence=1.0 trade should get the **18% stop tier**
(`engine.stops.get_stop_loss_pct`: ≥0.90 conviction → 0.18) — she's on the
`AI_SIGNAL_PLAYERS` allow-list, `CONVICTION_SCALED_STOPS_ENABLED=true` in
the live `.env`, and her `MODEL_GUARDRAILS` entry (added the same day as the
staticmethod fix, 2026-07-04) has no `stop_loss_pct` override that would
explain an 8% figure either.

A -8.16% ± 0.10% band across 11 different symbols over a month is far too
tight to be organic price action — it's a mechanical exit rule, just not
the one the code is documented to run. **This is the single highest-value
open question in this report: something is still cutting her positions at
~8%, not 18%, and I did not find the exact live code path in this
analysis-only pass.** Two concrete hypotheses to check first, in order of
likelihood:
1. **A restart-lag gap** — the 07-04 fix landed in the file but the live
   trader process may not have been restarted at the right moment for
   every trade in this window (matches a pattern found repeatedly
   elsewhere this session: correct code on disk, stale bytecode/state
   still running). The July 9 COST trade (5 days after the fix) still
   shows -8.04%, which argues against a clean fix-then-restart story.
2. **A different, uninvestigated exit path** — scaled-exit tiers, a
   trailing-stop mechanism, or something in the swing-gate/target-injection
   logic (`paper_trader.py` HM-AN2-TARGET-INJECTION, adjacent to the stop
   code read for this report) may be closing these positions before the
   conviction-scaled stop ever gets evaluated.

**Not something to fix by widening a parameter** — if the intended 18% tier
genuinely isn't reaching these trades, that's a wiring bug to trace, not a
number to retune.

## Finding 3 — wins are capped far below any target, and losers are sized bigger than winners

The 5 largest wins in the sample: +8.65%, +5.49%, +4.20%, +4.11%, +4.03%.
**None come close to a +24% target.** Combined with the average win
position ($68.90) being a third the size of the average loss position
($188.92-244.39), the dollar asymmetry (14x) is being driven by two
compounding effects, not one:

1. Winners are being taken very early (matches the scaled-exit tier system
   found and fixed elsewhere today — `engine/crew_scanner.py`'s
   `_tiers_triggered`/`_check_scaled_exits` — worth checking whether McCoy's
   winning exits are routing through that same tiered-partial-exit logic,
   which would explain profits capped in the 4-8% range regardless of a
   documented +24% target).
2. Winning and losing positions are not sized symmetrically — losers are
   consistently bigger stakes than winners in this sample.

**Recommend investigating #1 before touching any stop/target number** —
if scaled exits are clipping her winners at 4-8% by design (for every
agent, not just her), that's a global tier-table question, not McCoy-
specific. If it's McCoy-specific (e.g. a guardrail or allow-list gap),
that's the second thing to check.

## Tuning proposal (report proposes — owner disposes)

In priority order, since #1-2 are diagnostic gaps that should be closed
*before* any parameter is touched — retuning a stop/target percentage while
the actual live exit mechanism is still unidentified risks tuning the wrong
knob entirely:

1. **Trace why confidence=1.0 trades are exiting at ~8% instead of the
   documented 18% tier.** Highest leverage, zero risk to investigate (read-
   only log/code tracing). If it's a restart-lag or wiring gap, the fix is
   mechanical once found.
2. **Investigate the HIMS position specifically** — was a stop order ever
   placed? Did the monitoring loop skip this position for 11 days? This
   single trade is 68% of the reported loss column; understanding it
   (not necessarily "fixing" anything, since it may be an isolated
   incident) changes the diagnosis materially.
3. **Confirm whether McCoy's winning exits route through the scaled-exit
   tier system**, and if so, whether the tier thresholds (which cap her
   biggest win at 8.65%) are appropriate for a stock-trading agent with a
   documented +24% target, or whether the tiers were calibrated for a
   different trading style.
4. **Only after 1-3 are answered:** if the conclusion is that an 8% stop
   really is what's live and working as intended (not a bug), then the
   stop is simply too tight relative to a +24% target for an 84%-win-rate/
   14x-loss-asymmetry profile, and either the stop should widen toward the
   documented 18% tier, the position-sizing asymmetry (Finding 3) should be
   corrected so losers aren't systematically larger than winners, or both.
   No specific new percentage is proposed here — that's a decision for
   after 1-3 close the diagnostic gap, not before.

## Data notes

- All figures from `data/trader.db` `trades` table, `player_id='ollama-plutus'`,
  `action='SELL'`, `realized_pnl IS NOT NULL`. Zero rows flagged
  `known_contaminated`; zero rows have a `corrected_pnl` differing from
  `realized_pnl` — the underlying data is clean at face value.
- Hold-time analysis was only computed for the HIMS trade specifically
  (matched to its BUY row by symbol); a full hold-time distribution across
  all 75 trades was not computed in this pass (no direct BUY/SELL join key
  in the schema — would need heuristic per-trade matching, left as a
  follow-up if the diagnostic gaps above don't resolve the picture on
  their own).
