# HM-M5-BASELINE-ALLOCATOR — rules-based regime allocator control seat

**Date:** 2026-07-10 · **Status:** SHIPPED-DORMANT (Admiral activation required)
**Origin:** JPM AI-allocation note (Salopek et al., 2026-07-09, via Bloomberg) —
JPM benchmarked its AI agents against BOTH static 60/40 AND their own
rules-based regime model. OllieTrades had the first control arm
(benchmark.py 60/40 blend) but not the second. M-5 is the second.

## What it is

`engine/m5_allocator.py` — a zero-LLM deterministic seat (`m5-allocator`,
"M-5 Multitronic", provider `rule-based`) trading a two-ETF SPY/AGG book by
regime, $10k genesis, season 6. Named for the TOS multitronic computer:
a machine that trades on pure rules — fitting for the control arm.

## Allocation matrix (regime_history taxonomy → SPY weight, remainder AGG)

| Regime | SPY | AGG |
|---|---|---|
| BULL / BULL_CROSS / BULL_LOW_VOL | 80% | 20% |
| CAUTIOUS_BULL | 60% | 40% |
| EUPHORIC / CAUTIOUS_BEAR | 40% | 60% |
| BEAR / BEAR_CROSS | 20% | 80% |
| unknown / missing | 60% | 40% (loud log) |

Rebalance once per trading day (7:45–8:30 AZ window, capitol_fund pattern,
restart-safe dedup via trades table), and only when |actual − target| > 5pp.
Dust guard: no trade under $50 notional.

## Why it runs through the fleet gates (deliberate)

M-5 trades via `paper_trader.buy()/sell_partial()` — same regime-router veto,
same guardrails, same audit trail as every LLM seat. It measures the
allocation *policy inside the fleet's environment* (apples to apples with the
agents it benchmarks). The pure, ungated 60/40 curve remains benchmark.py's
job — two control arms, two layers.

Known interaction: the regime router's BEAR/BEAR_CROSS avoid-list blocks
long_equity buys, so M-5 can't rebalance equity UP inside a BEAR regime
(sells unaffected; bear target is the 20% floor anyway). Practical impact ~nil;
first BEAR→recovery regime flip unblocks. Documented in the module docstring.

## Safety posture

- Ships `is_paused=1` — **inert until Admiral runs:**
  `UPDATE ai_players SET is_paused=0 WHERE id='m5-allocator';`
- `can_trade_live=0`, paper only, forever. RULE #1 untouched.
- Fail-closed on seat-state read errors and dedup-check errors.
- Fail-loud (type+repr) everywhere else; degrade, don't crash. No silent catch.
- No model, no inference, no RAM — Free Models First trivially satisfied.

## The graduation question this seat answers

Every season: **did the LLM fleet beat M-5?** If not, the fleet's complexity
isn't earning its keep at the allocation layer — exactly the bar JPM set for
its own agents. Add M-5 to the leaderboard read alongside the SPY and 60/40
lines once benchmark pipeline is restored (see S6 findings 2026-07-10, P0).

## Files

- `engine/m5_allocator.py` (new)
- `main.py` (+9 lines: registration + 15-min scheduler tick, try-guarded)
- `drafts/HM-M5-BASELINE-ALLOCATOR.md` (this file)

## Verify before claiming (done on branch)

- `py_compile` both files — clean
- Matrix keys cross-checked against `engine/regime_router.py`'s
  REGIME_STRATEGY_MATRIX taxonomy — all present
- EUPHORIC weight (40%) matches the router's EUPHORIC `long_equity_max_pct`
- NOT verified from cloud (needs bigmac): live registration against trader.db,
  first scheduled tick, and a paper rebalance in the market window —
  smoke-test checklist for Scotty below.

## Scotty smoke-test checklist

1. `git apply` / merge branch `hm-m5-baseline-allocator`; restart trader.
2. Confirm `ai_players` row `m5-allocator` exists, `is_paused=1`.
3. `python3 -c "from engine.m5_allocator import current_target; print(current_target())"`
   → expect (0.80, 'BULL_CROSS') under current regime.
4. Admiral activates when satisfied; watch first rebalance in next AZ-morning
   window; verify trades tagged `m5-allocator,regime-rules` in decision audit.
