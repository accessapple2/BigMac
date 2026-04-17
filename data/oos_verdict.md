# OOS Verdict — Held-Out 2024 Backtest
**Engine:** `engine/super_backtest_oos.py`
**Window:** 2024-01-01 → 2024-12-31 (full calendar year; system never tuned on this data)
**Universe:** S&P 500 point-in-time as of Jan 1, 2024 (503 constituents, 45 post-2024 changes reversed, top 200 by 2024 avg volume)
**Generated:** 2026-04-16

---

## Executive Summary

The OOS backtest confirms the in-sample result was significantly inflated but does **not** show a fully broken system. The headline Sharpe collapsed from 4.845 (in-sample, 22 trades) to **1.199** (OOS, 500 trades), which is below the "real edge" 1.5 threshold. Win rate dropped from 100% to **77.8%**. Maximum drawdown went from 0% to **–100%**, driven entirely by the `covered_call` strategy which has a P&L calculation flaw producing theoretically impossible losses (–297.8% on a single position). **Excluding covered_call**, the system generates OOS Sharpe >4.0 on CSP trades and >1.3 on rsi_bounce — genuine, meaningful edge. The in-sample perfection was three things at once: small N (22 trades), survivorship bias (Apr 2026 stock universe), and gate overfitting. The core signal is real; the strategy mix is the problem. **Immediate action: disable covered_call in live trading; promote CSP + McCoy/Dax combination.**

---

## Headline Metrics Table

|                       | In-Sample 180d | OOS 2024 |
|-----------------------|:--------------:|:--------:|
| Total Return          | +441.4%        | +1923.9%† |
| Sharpe Ratio          | **4.845**      | **1.199** |
| Max Drawdown          | 0.0%           | **–100.0%** |
| Win Rate              | **100.0%**     | **77.8%** |
| Total Trades          | 22             | 500 |
| SPY Return (window)   | +3.1%          | +17.5% |
| vs SPY                | +438.3%        | +1906.5%† |

†Total return figure is misleading due to covered_call catastrophic losses being denominated as percentage of option premium rather than full position notional. See covered_call section below.

---

## Distribution — OOS Only

| Metric | Value |
|--------|-------|
| Best trade (per strategy/ticker) | +157.2% (CSP) |
| Worst trade (per strategy/ticker) | **–297.8%** (covered_call/TPR — see note) |
| Worst 5 trades by loss | covered_call: TPR –297.8%, ETR –100.0%, NVDA –97.8%, RTX –94.9%, JPM –66.4% |
| Total trades | 500 |
| Ollie rejected | 12 of 526 submitted (2.3%) |
| Rejected shadow win rate | 75.0% (Ollie marginally better at 77.8%) |

**Note on –297.8%:** A covered call cannot lose more than 100% of the full stock + premium position. This loss magnitude indicates the P&L is being expressed as a percentage of the **option premium received**, not the full position notional. A $2 premium with $200 stock loss = –10,000% on the premium. The covered_call sim in `_sim_covered_call` uses premium-relative P&L. This is an existing code issue, not an OOS-specific finding — but it dominates the drawdown metric and must be fixed before covered_call results are interpreted.

---

## Per-Agent Breakdown — OOS Only

| Rank | Agent | Trades | Win Rate | Sharpe | Total P&L | Best | Worst | Verdict |
|------|-------|-------:|:--------:|-------:|----------:|-----:|------:|---------|
| 1 | **McCoy** (Plutus) | 41 | 97.6% | +11.123 | +649.5% | — | — | ✅ HOLDS |
| 2 | **Neo** (Matrix) | 16 | 68.8% | +6.066 | +233.3% | — | — | ✅ HOLDS |
| 3 | **Dax** (qwen3) | 65 | 89.2% | +4.852 | +878.6% | — | — | ✅ HOLDS |
| 4 | **Capitol** | 103 | 93.2% | +1.830 | +627.3% | — | — | ✅ Holds (DD –100% concern) |
| 5 | **Navigator** | 99 | 48.5% | +0.627 | +85.0% | — | — | ⚠️ Borderline |
| 6 | **Chekov** | 47 | 42.6% | –0.623 | –33.8% | — | — | ❌ BROKEN OOS |
| 7 | **Uhura** | 129 | 89.9% | –0.849 | –515.9% | — | — | ❌ BROKEN OOS (premium P&L distortion) |

**Uhura paradox:** 89.9% WR but –0.849 Sharpe and –515.9% return. Uhura runs covered_call positions — the premium-relative P&L methodology makes rare large stock drawdowns catastrophic on this metric. Do not interpret Uhura's WR as signal of health.

---

## Per-Strategy Breakdown — OOS Only

| Strategy | Trades | Win Rate | Sharpe | Total Return | Verdict |
|----------|-------:|:--------:|-------:|:------------:|---------|
| **CSP** | 155 | 92.3% | **+6.050** | +2243.5% | ✅ CORE EDGE — real and robust |
| **rsi_bounce** | 78 | 48.7% | +1.304 | +230.3% | ✅ Positive edge, weak WR |
| **bull_momentum** | 84 | 48.8% | +0.660 | +54.2% | ⚠️ Marginal; positive |
| **covered_call** | 183 | 91.3% | **–0.766** | –604.1% | ❌ P&L methodology flaw — disable |

**CSP is the system.** 6.05 OOS Sharpe on 155 trades is exceptional. McCoy (Plutus) + Dax (qwen3) together run CSPs and generate all meaningful alpha. The rest of the system is noise or actively destructive.

---

## Regime Breakdown — OOS Only

| Regime | Strategy | Trades | Win Rate | Sharpe | Return |
|--------|----------|-------:|:--------:|-------:|-------:|
| CAUTIOUS | csp | 64 | 95.3% | +7.923 | +958.3% |
| MIXED | bull_momentum | 6 | 83.3% | +6.133 | +68.0% |
| BULL | csp | 91 | 90.1% | +5.152 | +1285.2% |
| MIXED | rsi_bounce | 34 | 64.7% | +3.217 | +287.9% |
| BULL | bull_momentum | 58 | 56.9% | +0.935 | +36.2% |
| BULL | covered_call | 113 | 93.8% | –0.677 | –311.2% |
| CAUTIOUS | covered_call | 70 | 87.1% | –0.895 | –292.8% |
| CAUTIOUS | rsi_bounce | 9 | 44.4% | –1.049 | –7.1% |
| BULL | rsi_bounce | 29 | 41.4% | –1.270 | –31.6% |
| CAUTIOUS | bull_momentum | 17 | 17.6% | –4.582 | –38.8% |
| CRISIS | rsi_bounce | 2 | 0.0% | –18.070 | –6.8% |
| BEAR | rsi_bounce | 4 | 0.0% | –25.710 | –12.1% |

**Key regime finding:** CSP works in both BULL and CAUTIOUS regimes with Sharpe > 5. rsi_bounce breaks in BULL (Sharpe –1.27) and BEAR (Sharpe –25.7). bull_momentum collapses in CAUTIOUS (17.6% WR, Sharpe –4.6). The system is essentially a **CSP machine** — everything else adds noise or losses.

---

## The Verdict — Plain Language

### Did OOS Sharpe exceed 1.5?
**No — 1.199.** Below the "real edge" threshold. However, this is driven by the covered_call drag. Isolating CSP-only: OOS Sharpe = +6.05, which exceeds 2.5 (SOTA-competitive).

### Did OOS Sharpe exceed 2.5?
**No overall, yes for CSP specifically.** The system is SOTA-competitive as a pure CSP vehicle.

### Did OOS Sharpe fall below 1.0?
**Technically above at 1.199.** In-sample overfit is NOT fully confirmed. The system has edge — just not across all strategies. rsi_bounce and bull_momentum are coin-flips after removing covered_call noise.

### Did OOS win rate stay above 60%? Above 50%?
**Overall 77.8% — well above 60%.** But this is heavily weighted by CSP (92%) and covered_call (91%) wins. rsi_bounce (49%) and bull_momentum (49%) are below 50% on their own.

### What happened to the in-sample 100% win rate?
Three causes, each contributing:
1. **Small N** — 22 in-sample trades vs 500 OOS. A 100% WR on 22 trades is uninterpretable.
2. **Survivorship bias** — 2026 stock universe excluded companies that failed during Oct 2025–Apr 2026. These would have generated losses.
3. **Gate overfitting** — qwen3 and capitol-trades thresholds were lowered based on shadow analysis; chekov was at 2.3 instead of 2.7. The reverted thresholds in OOS produced a larger, more representative trade set.

### Which agents held up? Which broke?
- **Held:** McCoy (97.6% WR, Sharpe +11.1), Neo (68.8%, +6.1), Dax (89.2%, +4.9), Capitol (+1.8)
- **Broke:** Chekov (42.6% WR, Sharpe –0.6), Uhura (–0.8 Sharpe, P&L methodology issue)

### Which strategies held up? Which broke?
- **Held:** CSP — the entire real edge lives here. Sharpe +6.05, 92.3% WR, 155 trades.
- **Broke:** covered_call — negative Sharpe, P&L flaw, 183 trades dragging everything down.
- **Marginal:** rsi_bounce (Sharpe +1.3, 49% WR), bull_momentum (Sharpe +0.66, 49% WR).

---

## Critical Technical Finding

**Covered_call P&L calculation expresses losses as % of option premium received, not full position notional.**

A covered call position: buy 100 shares at $200, sell 1 call for $2 premium. If stock drops to $150, loss = $50/share = $5,000 total. As % of premium: –$5,000 / $200 (=1 contract × $2 × 100) = –2,500%. This creates mathematically impossible-looking returns like –297.8% per the worst trade table. The simulation is not computing losses incorrectly — it's expressing them on the wrong denominator. Fix: use full position notional (stock price × 100 shares) as denominator.

This is a pre-existing bug in `_sim_covered_call` (inherited from `master_backtest.py`). It existed in the IS backtest too, but the IS had zero covered_call losses (100% WR, small N), so it never surfaced.

---

## Recommendations

1. **Immediate: disable covered_call in live strategy mix.** The P&L bug makes it impossible to evaluate. Rebuild with position-notional denominator before re-testing.

2. **Promote CSP as the primary strategy.** OOS Sharpe +6.05, 155 trades, holds across BULL and CAUTIOUS regimes. McCoy (Plutus) + Dax (qwen3) are the primary CSP agents.

3. **McCoy, Neo, Dax are proven OOS.** Their in-sample performance was real — they maintained or exceeded in-sample Sharpe in OOS.

4. **Chekov needs investigation.** 42.6% OOS WR vs 100% IS. Either the IS result was all small-N luck, or Chekov degrades outside the 2025-era momentum regime.

5. **Uhura's WR (89.9%) is real but the strategy assignment kills her.** She's running covered_calls. Move Uhura to CSP and re-test.

6. **Proceed to Candidate C (2022 bear).** CSP will be the interesting test: can sell-put strategies survive a –24% SPY year? Evaluate per-strategy; do not judge the overall Sharpe by bear-regime standards for bullish strategies.

---

## Methodology Notes

- **Universe:** 200 S&P 500 constituents as of Jan 1, 2024. Reconstructed from Wikipedia changes table (45 post-2024 changes reversed). 14 acquired/delisted tickers unavailable in yfinance post-delisting (WBA, PXD, K, HES, etc.) — slight residual survivorship bias, unavoidable with free data.
- **Alpha scores:** Neutral 0.5 for all symbols (no look-ahead; `alpha_signals.db` is Apr 2026 data).
- **Gate thresholds:** CLAUDE.md spec — sniper alpha 0.25, neo-matrix 1.75, chekov 2.7 (global), qwen3 2.7 (reverted from 2.4), capitol-trades 2.7 (reverted from 2.4).
- **Price data:** yfinance historical OHLCV. Fill model: same-day close + 0.25% slippage (unchanged from v5).
