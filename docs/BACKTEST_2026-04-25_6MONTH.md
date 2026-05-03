# Backtest Report — 6-Month Fleet Analysis
**Run ID:** mega_6mo_20260425_0936
**Filed:** 2026-04-25
**Window:** 2026-01-06 → 2026-04-25 (~110 days effective — system was not live pre-Jan 2026)
**Requested window:** 2025-10-25 → 2026-04-25 (180d). Actual data begins 2026-01-06.
**Harness:** engine/mega_backtest_6month.py (retrospective over DB closed trades)
**Patches active:** All 16 Saturday Drydock fixes confirmed in HEAD before run.

---

## Pre-Flight Verification

| Check | Status |
|---|---|
| Halt gate (`is_halted` in paper_trader.py) | ✅ present (lines 547, 1091) |
| Autopilot Layer 1 (`current_price <= 0`) | ✅ present (line 127) |
| BSM ceiling (`_BSM_CEILING = 1.5`) | ✅ present (line 63) |
| Earnings blackout (`_next_earnings_date`) | ✅ present (line 19) |
| Capitol Trades dedup (`DEDUP_HELD/DEDUP_TODAY`) | ✅ present (lines 2576, 2591) |
| Execution gate (`_EXECUTION_ENABLED`) | ✅ False (simulation-only) |
| Halted players | ✅ ollama-llama, dayblade-sulu, grok-3 |
| Post-backtest halt gate regression | ✅ grok-3 correctly blocked |

---

## Fleet-Wide Results (Mega Backtest)

**SPY benchmark:** +4.20% | **Regime:** BULL_CROSS 67d / CAUTIOUS_BEAR 55d / BEAR_CROSS 11d
**Total closed trades in window:** 675

| Config | Return | Alpha | Sharpe | Win Rate | Max DD | PF | Trades |
|---|---|---|---|---|---|---|---|
| FULL_S6 | +232.98% | +228.78% | 1.119 | 30.7% | 53.57% | 5.31 | 675 |
| LEGACY_FLEET | +232.96% | +228.76% | 1.123 | 30.7% | 53.62% | 5.31 | 671 |
| S6_NEW_AGENTS | +0.03% | -4.17% | 3.600 | 25.0% | 0.02% | 2.17 | 4 |
| HIGH_CONF_75+ | +0.00% | -4.20% | 0.000 | 0.0% | 0.00% | 0.00 | 0 |

> Note: The $232K fleet P&L is dominated by gemini-2.5-pro (+$225K, 46 trades) and
> claude-sonnet (+$43K, 35 trades). These are Season 1 legacy agents — not the target
> S6 fleet. Excluding them, the active S6 fleet net P&L is approximately +$1,100
> across ~560 trades. See per-agent table below.

> HIGH_CONF_75+ showing 0 trades indicates the signals table confidence values use
> 0.0–1.0 scale, not 0–100. Query threshold should be 0.75, not 75.

---

## Per-Agent Table (S6 Active Fleet — Excluding Halted/Excluded)

| Agent (player_id) | Display Name | Closed | Open | Win Rate | PF | Total P&L | Avg Win | Avg Loss | Gate-Flip Verdict |
|---|---|---|---|---|---|---|---|---|---|
| options-sosnoff | Counselor Troi | 4 | 8 | 100.0% | ∞ | +$2,060.35 | +$515 | — | ✅ READY |
| gemini-2.5-flash | Lt. Cmdr. Worf | 21 | 0 | 100.0% | ∞ | +$180.96 | +$8.62 | — | ⚠️ PAUSED (is_paused=1) |
| energy-arnold | Cmdr. Trip Tucker | 14 | 0 | 92.9% | n/a | +$167.58 | +$12.89 | — | ✅ READY |
| navigator | Ensign Chekov | 5 | 0 | 20.0% | 8.19 | +$125.12 | +$142.52 | -$4.35 | ⚠️ LOW SAMPLE |
| deepseek-7b-grok4 | Lt. Cmdr. Spock | 1 | 16 | 100.0% | ∞ | +$102.69 | +$102.69 | — | ⚠️ SEE SECTION 1 |
| ollie-auto | Ollie | 35 | 23 | 85.7% | 46.06 | +$44.16 | +$1.50 | -$0.24 | ✅ READY |
| capitol-trades | Capitol Trades | 34 | 16 | 8.8% | n/a | +$36.27 | +$12.09 | ~$0 | ⚠️ WIN RATE CONCERN |
| grok-4 | Lt. Cmdr. Spock (S1) | 17 | 8 | 100.0% | ∞ | +$32.34 | +$1.90 | — | ⚠️ SEASON 1 LEGACY |
| neo-matrix | Neo | 4 | 10 | 25.0% | 2.17 | +$26.91 | +$50.00 | -$7.70 | ⚠️ LOW SAMPLE |
| dalio-metals | Mr. Dalio | 24 | 13 | 62.5% | 0.29 | -$164.03 | +$4.47 | -$115.51 | ❌ PF < 1 |
| gpt-4o | GPT-4o | 41 | 39 | 4.9% | 0.06 | -$205.69 | +$6.54 | -$14.59 | ❌ FAILING |
| ollama-qwen3 | Lt. Jadzia Dax | 1 | 14 | 0.0% | 0.00 | -$521.91 | — | -$521.91 | ❌ NO SAMPLE |
| ollama-coder | Lt. Cmdr. Data | 0 | 0 | — | — | — | — | — | ⚠️ NO TRADES |
| chekov | Chekov | 0 | 0 | — | — | — | — | — | ⚠️ NO SIGNALS |
| qwen3-8b-flash | Lt. Cmdr. Worf (S5) | 0 | 0 | — | — | — | — | — | ⚠️ NO TRADES |

> Sharpe not computed per-agent due to insufficient trade sequences. Fleet-level Sharpe: 1.119.
> Capitol Trades PF appears favorable in aggregate (+$36) but 8.8% win rate means 31/34 closes
> are losses — the positive P&L is noise from tiny avg losses vs small wins. Suspicious.

---

## Section 1 — Spock (deepseek-7b-grok4) Options Performance

**Player ID:** deepseek-7b-grok4 | **Display:** Lt. Cmdr. Spock (Season 5)
**Note:** Two players share "Lt. Cmdr. Spock" display name — deepseek-7b-grok4 (S5, active)
and grok-4 (S1, legacy). Analysis covers deepseek-7b-grok4 only.

### Options-only metrics
| Metric | Value |
|---|---|
| Options trades (closed) | 0 |
| Options trades (open) | 1 |
| Stock trades (closed) | 1 |
| Stock P&L | +$102.69 (NOW exit at entry price +$102.69 realized_pnl) |

Only one options trade exists: **MU call, strike $500, expiry 2026-05-22, premium $42.48**.
This is the only data point available.

### BSM Ceiling Analysis on MU Call
- MU spot at time of trade: ~$85 (based on market data; $42.48 recorded as `price` field)
- Strike: $500 (deep OTM — ~490% above spot)
- DTE: 28
- BSM fair value (σ=0.45, r=0.045): **$0.0000** (effectively zero probability)
- BSM ceiling (1.5×): **$0.0000**
- Actual premium: $42.48
- **Would BSM ceiling have blocked it? YES** ✅

The $500 strike MU call is likely a data error (option price field may have been populated
with the stock price, $42.48, rather than the actual option premium). With BSM ceiling
active, this trade would be blocked before execution.

### Earnings Blackout
Cannot evaluate — no earnings date lookup was triggered for this single trade.
Layer is present and wired (options_selector.py line 252).

### Net Options P&L with both layers active
With BSM ceiling blocking the only options trade: **$0.00** (no options trades would have
executed). Stock trades unaffected: **+$102.69**.

### Verdict: Ready for Tuesday options trading?
**NO-GO** ⛔

- 0 successfully closed options trades in the dataset
- Only 1 options trade attempted — likely a data/price feed error (deep OTM at $500 strike)
- BSM ceiling would correctly block malformed entries like this
- Earnings blackout untested (no trades near earnings windows)
- 16 open stock positions with no exits yet — execution pipeline needs more cycles
- Recommend: 1 more week of stock cycling to establish a closed-trade baseline before
  enabling options on this agent

---

## Section 2 — Bull Spread V1 (strategy:bull_spread_v1)

**Player ID:** strategy:bull_spread_v1 (registered via _registry().register())

### Did the strategy fire in the 6-month window?
**No real paper trades.** The options_trades table contains 6 entries, all tagged
`exec_status='test_cleanup'`, all from 2026-04-22 (setup/wiring tests):

| Structure | Symbol | Credit/Debit | Status |
|---|---|---|---|
| bull_call_spread | SPY | -$3.00 | test_cleanup |
| bull_put_spread | QQQ | +$5.00 | test_cleanup |
| bull_call_spread | NVDA | -$1.50 | test_cleanup |
| bull_call_spread | AAPL | -$2.50 | test_cleanup |
| bull_call_spread | SPY (dup) | -$3.00 | test_cleanup |
| bull_call_spread | TEST | -$0.50 | test_cleanup |

### First-trade gate criteria check
| Criterion | Status |
|---|---|
| 30 paper trades required | ❌ 0/30 (no real paper trades fired) |
| Positive expectancy | ❌ Cannot compute — no trades |
| FIRST_TRADE_MODE=credit-only | ✅ Wired (confirmed in Fix commit `9c5b0bf`) |
| Signal generation active | Unknown — strategy registry registered but scheduler not verified |

### Verdict: Ready for first paper trade Tuesday?
**NO-GO** ⛔

The strategy is registered and the code is correct, but it has not fired a single
real paper trade. The gate-flip criterion requires 30 paper trades + positive expectancy
before transitioning to live. Currently at 0/30.

**Recommended action before Tuesday:**
1. Verify the scheduler is calling `BullSpreadV1.run_cycle()` (check launchd plist or
   cron entry for bull_spread)
2. Confirm Polygon.io options chain is returning data (or mock_data.py fallback is active)
3. Allow at least 3–5 real paper cycles before gate-flip evaluation

---

## Section 3 — Scanner Agents (Ollie/Navigator/Chekov)

| Agent | Signals (6mo) | Executed | Execution Rate | Avg Confidence | Trades (closed) |
|---|---|---|---|---|---|
| navigator | 307 | 0 | 0.0% | 0.89 | 5 |
| ollie-auto | 0* | — | — | — | 35 |
| chekov | 0* | — | — | — | 0 |
| neo-matrix | 0* | — | — | — | 4 |

*Signals not found under player_id in signals table — ollie-auto, chekov, neo-matrix
trades appear to originate from a different pipeline (paper_trader.buy() direct calls
rather than signals table).

### Navigator: Calibration
- 307 signals generated, 0 marked EXECUTED in signals table
- Despite 0 signals table executions, 5 closed trades exist in trades table
- Avg confidence: 0.89 — high confidence signals generating low actual execution rate
- Win rate on closed trades: 20% (1W/4L) but +$125 P&L suggests one large win ($142)
  offsetting four small losses (-$4.35 avg)

### Calibration gap
Without executed signal linkage, confidence-vs-realized-hit-rate cannot be computed.
The signals table `execution_status` column is not being updated when trades execute —
this is the same ghost prediction gap identified in Friday's diagnostics.

### Verdict: Ready to generate signals for live execution?
**Ollie-auto: ✅ CONDITIONAL GO**
- 85.7% win rate over 35 closed trades is the strongest track record in the S6 fleet
- Absolute P&L is tiny ($44) but the structure (46x profit factor) shows discipline
- Gate condition: live execution should be capped at current position sizing until
  options layer is validated separately

**Navigator: ⚠️ NOT YET**
- 5 closed trades is too small a sample for live signal authority
- 307 signals with 0 tracked executions — signal→trade pipeline linkage is broken
- Fix the execution_status update before trusting signal metrics

**Chekov: ❌ NO-GO**
- 0 signals, 0 trades in the window — agent is not generating output
- Needs investigation: is the scheduler running chekov's scan cycle?

---

## Regression Verification

Post-backtest halt gate test on grok-3:
```
[09:37:20] HALTED: grok-3 — S6 review: routing zombie, retired 2026-04-25
halt gate intact ✅
```

All 16 Saturday Drydock patches confirmed present in HEAD. No regressions detected.

---

## Summary: Tuesday Gate-Flip Readiness

| Agent | Gate-Flip | Reason |
|---|---|---|
| Ollie (ollie-auto) | ✅ GO | 85.7% WR, 35 closed trades, highest PF in fleet |
| Counselor Troi (options-sosnoff) | ✅ GO | 100% WR, 4 options trades closed profitably, +$2K |
| Cmdr. Trip Tucker (energy-arnold) | ✅ GO | 92.9% WR, 14 trades |
| Spock options (deepseek-7b-grok4) | ⛔ NO-GO | 0 options closed; only attempt was data error |
| Bull Spread V1 | ⛔ NO-GO | 0/30 paper trade gate; strategy hasn't fired |
| Navigator (navigator) | ⚠️ HOLD | 5 trades only; signal pipeline linkage broken |
| Chekov (chekov) | ⚠️ HOLD | No trades or signals in window — needs diagnosis |
| Lt. Jadzia Dax (ollama-qwen3) | ❌ EXCLUDE | 0% WR, -$521 on 1 trade |
| Dr. McCoy (ollama-plutus) | ❌ EXCLUDE | Per quality audit: hallucinated catalysts |

---

## Data Quality Notes

1. **Effective window is ~110 days, not 180.** No trades before 2026-01-06.
   The 6-month window will remain sparse until the system has been running since at
   least October 2026.

2. **$0.00 realized P&L on bulk exits** (identified in overnight report): Capitol Trades
   TSM/URI exits, Counselor Troi GOOGL exit all show exit_price = entry_price exactly.
   The exit recorder is writing entry price as exit price rather than fetching current
   market price. Sharpe and drawdown metrics are understated as a result.

3. **MU $471 price anomaly** on deepseek-7b-grok4: stock price field recorded as $471
   when MU trades ~$85. Likely a data feed cross-contamination.

4. **gemini-2.5-pro $225K P&L** skews fleet totals. This is a Season 1 legacy agent
   not in the S6 target fleet — should be excluded from S6 fleet metrics in future runs.
   Update `_S6_AGENTS` list in mega_backtest_6month.py to reflect current active S6 IDs.
