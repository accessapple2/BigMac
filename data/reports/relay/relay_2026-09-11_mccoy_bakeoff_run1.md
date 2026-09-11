# Relay — McCoy Rank bakeoff, first real measurement run. 2026-09-11

XO Priority 3, item 9: "Run the three-arm bakeoff: qwen3:8b, Fin-R1,
MiniMax-M3... No arm gets a seat on this run — it's measurement." First
real run complete. `run_id=20260911T212123Z-a04a50`, logged to
`data/trader.db:mccoy_bakeoff_log` (new, additive table — 30 rows this
run). Nothing executed a trade; nothing touched a live decision path.

## Setup

- 10 real symbols, top of today's actual McCoy screened list
  (`engine.mccoy_screen.get_mccoy_screened_symbols()`, 46/100 symbols
  found, regime BULL_TREND at screen time / BEAR_CROSS by call time —
  regime flipped mid-run, both logged per-row, not a bug).
- Same real prompt to all three arms per symbol: McCoy's actual persona +
  live market data (price, RSI/MACD/volume/SMA indicators, real news) +
  the fleet's exact format footer, built via `engine.providers.base.
  build_prompt()` with a `player_id='ollama-plutus'` builder so
  `MODEL_PERSONALITIES` resolves for real — not a synthetic/hardcoded
  prompt.
- Coordinated with olliemax first (`COORD_FROM_SCOTTY_mccoy_bakeoff.md`
  posted to `~/modelworks/fleet_checks/ollama_churn/` before running —
  GPUs were at 92%/84% util with 3 models resident at coordination time;
  ran sequentially, one call at a time, no objection received).
- New script: `scripts/mccoy_bakeoff_run.py` (`--n`, `--cost-cap`).

## Results

| Arm | n | ok | format_valid | avg wall | Notes |
|---|---|---|---|---|---|
| qwen3:8b | 10 | 10/10 | 10/10 | 9.41s | 4/10 responses flagged `REASONING-DIRECTION-CONFLICT` (base.py's own live sanity check — reasoning describes a bearish setup, action is BUY), 2/10 `INVALIDATION-IMPLAUSIBLE`. 9/10 calls = BUY, avg confidence 0.82 on those. |
| Fin-R1 | 10 | **0/10** | 0/10 | 30.01s (timeout, every call) | **Every single call hit the fleet's real production generate timeout** (`config.OLLAMA_GENERATE_TIMEOUT_S`, confirmed live-configured at 30s, no `.env` override) on a real ~15-18K-token McCoy prompt. Zero usable responses — not a format problem, a completion-time problem. |
| MiniMax-M3 | 10 | 10/10 | 10/10 | 12.04s | **10/10 = HOLD.** Never once recommended BUY across the same 10 real symbols qwen3:8b called BUY on 9/10 times. |

**MiniMax-M3 cost + token split (10 calls):** `in=51,634 visible_out=1,923
reasoning_out=10,471 cost_usd=$0.030363` (~$0.003/call). **84.5% of output
tokens were reasoning, not visible answer** — for every 1 visible token,
~5.4 reasoning tokens. Real cost is small in absolute terms but the
reasoning-to-visible ratio is the thing worth watching if this scales to
McCoy's actual twice-daily top-100 screened-scan volume (100 symbols x 2
slots/day x this ratio ≈ 20x this run's token volume).

## The headline finding: qwen3:8b vs. MiniMax-M3 diverge sharply in BEAR_CROSS

Same 10 real symbols, same real data, same regime (BEAR_CROSS by the time
most calls landed):

```
symbol   qwen3:8b (today's live McCoy model)    minimax-m3
ACVA     BUY  conf=0.72                          HOLD conf=0.30
ADBG     BUY  conf=0.82                          HOLD conf=0.30
AEO      BUY  conf=0.78                          HOLD conf=0.28
ATEC     BUY  conf=0.85                          HOLD conf=0.42
COO      BUY  conf=0.85                          HOLD conf=0.30
LMUB     BUY  conf=0.85                          HOLD conf=0.30
NAVN     BUY  conf=0.85                          HOLD conf=0.40
ORCU     BUY  conf=0.82                          HOLD conf=0.20
ORCX     BUY  conf=0.85                          HOLD conf=0.35
RWT      HOLD conf=0.45                          HOLD conf=0.35
```

qwen3:8b: 9/10 BUY, avg confidence 0.82 on those BUYs. MiniMax-M3: 10/10
HOLD, avg confidence 0.32. **This is directionally consistent with, and
now a second independent data point for, the McCoy-overconfidence finding
already on record this session** (42-trade/71.4% hit-rate calibration gap,
`relay_2026-09-11_calibration_finding_correction.md`) and with why
`UNIVERSAL_MIN_CONVICTION` is raised to 0.80 specifically in bear regimes
in the live gate today. 4 of those 9 qwen3:8b BUYs also independently
tripped the existing `REASONING-DIRECTION-CONFLICT` sanity check (its own
stated reasoning was bearish while its action was BUY) — not a bakeoff
artifact, a real quality signal from code already live in `base.py`.

**Not a conclusion, a data point.** n=10, one run, one (transitional)
regime reading. Not claiming MiniMax-M3 is "better" from this alone —
just that the divergence is real, large, and worth the calibration/
forward-return tracking this whole Phase 2 design exists to build toward.

## What this run does NOT score (honest gap, by design)

**Calibration** and **forward return per regime** both require future
price/outcome data this run cannot have yet. Every row logs `price` and
`regime` at call time specifically so a later pass can join against
forward price action once it accrues — this is a measurement
infrastructure piece, not a one-shot verdict. Phase 2's full design (top/
bottom-20 held basket, re-ranked daily, alpha vs SPY) is a multi-day
system; this is the first single-day data point toward it, not the system
itself.

## Fin-R1: a structural finding, not a tuning question

This isn't "Fin-R1 answered slowly" — it's "Fin-R1 never once finished
inside the timeout the real fleet actually runs under, on 10/10 real
prompts." Two honest framings, Admiral's call which applies:
1. Fin-R1 is not viable as a McCoy candidate under today's real 30s
   generate timeout, full stop — the earlier documented format/language
   concern (code-switching into Chinese, `mccoy_bakeoff_arms.py`'s
   comments) is now moot; it doesn't get far enough to test format.
2. The 30s timeout itself may be worth a separate look — it's
   shorter than this codebase's own history suggests it used to be
   (comments elsewhere reference a 180s value; `.env` has no
   `OLLAMA_GENERATE_TIMEOUT_S` override today, so the live fleet runs at
   the 30s code default). **Not touched or changed here** — a live
   production timeout is a separate, consequential decision with its own
   tradeoffs already discussed in this codebase's history (idle-timeout
   vs. wall-clock, HM-OLLIE-STALE-SOCKET); flagging as a real, possibly
   surprising fact, not fixing it as a side effect of a bakeoff script.

## Not built in this pass

- The top/bottom-20 held-basket daily system (Phase 2's full design) —
  this run is the "next piece" `mccoy_bakeoff_arms.py`'s docstring said
  wasn't built yet (single real measurement pass), not the recurring
  basket-tracking pipeline.
- `plutus-v1-real` (arm 5 in the plan doc) — not named in today's
  directive's "three-arm" framing, not run.
- Claude (arm 2, `xo_brief.py::call_claude`) — not named in today's
  directive either, not run.

Co-Authored-By: Claude Sonnet 5 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01PZs3iBLLgQUffpHn8yfJzi
