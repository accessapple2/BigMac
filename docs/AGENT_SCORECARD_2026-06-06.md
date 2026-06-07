# Agent Scorecard — 2026-06-07 03:47:32 UTC
Window: last 180d · 1098 closed trades · 26 agents · SR0(null)=2.4821
_Methodology: return-on-cost (realized_pnl / |entry*qty*mult|), no stop-distance R; DSR deflated across agent population; PBO via CSCV on daily returns_

## Ranked agents (best → worst, by DSR then Sharpe)

| # | Agent | Model | Closed | WR% | Sharpe | DSR | totR | avgR | P&L$ | MaxDD% |
|--:|-------|-------|------:|----:|------:|----:|----:|----:|-----:|-------:|
| 1 | energy-arnold | ministral-3:3b | 13 | 100.0 | +2.38 | 0.365 | +1.9 | +0.150 | +168 | 0 |
| 2 | gemini-2.5-flash | ministral-3:3b | 22 | 95.5 | +2.15 | 0.268 | +2.1 | +0.097 | +165 | -3 |
| 3 | cto-grok42 | qwen3:8b | 16 | 93.8 | +1.93 | 0.104 | +1.1 | +0.071 | +56 | -0 |
| 4 | neo-matrix | 8000 / Independent | 33 | 93.9 | +1.86 | 0.087 | +1.9 | +0.058 | +130 | -1 |
| 5 | options-sosnoff | qwen3:8b | 3 | 100.0 | +1.07 | 0.014 | +0.0 | +0.010 | +514 | 0 |
| 6 | grok-4 | deepseek-r1:7b | 17 | 100.0 | +1.73 | 0.002 | +0.7 | +0.041 | +32 | 0 |
| 7 | qwen3-8b-flash | qwen3:8b | 73 | 94.5 | +1.27 | 0.000 | +4.7 | +0.064 | +583 | -1 |
| 8 | ollama-qwen3 | ministral-3:3b | 109 | 89.9 | +1.07 | 0.000 | +5.0 | +0.046 | -130 | -18 |
| 9 | dayblade-sulu | qwen3:8b | 10 | 60.0 | +0.67 | 0.000 | +0.1 | +0.013 | -451 | -3 |
| 10 | capitol-trades | congress-copycat | 33 | 81.8 | +0.59 | 0.000 | +1.2 | +0.037 | -90 | -19 |
| 11 | ollie-auto | ollie | 62 | 69.4 | +0.50 | 0.000 | +0.9 | +0.015 | +39 | -7 |
| 12 | deepseek-7b-grok4 | qwen3:8b | 127 | 82.7 | +0.41 | 0.000 | +4.6 | +0.036 | -465 | -80 |
| 13 | ollama-plutus | plutus-v1 | 97 | 93.8 | +0.30 | 0.000 | +23.8 | +0.245 | +4,254 | -2 |
| 14 | dalio-metals | ministral-3:3b | 18 | 83.3 | +0.21 | 0.000 | +0.7 | +0.037 | -255 | -60 |
| 15 | navigator | qwen3:8b | 43 | 55.8 | +0.08 | 0.000 | +0.2 | +0.005 | +88 | -22 |
| 16 | gpt-o3 | ministral-3:3b | 9 | 44.4 | +0.08 | 0.000 | +0.0 | +0.002 | -3,045 | -3 |
| 17 | ollama-local | gemma3:4b | 49 | 32.7 | -0.19 | 0.000 | -0.1 | -0.003 | -12,316 | -18 |
| 18 | claude-sonnet | ministral-3:3b | 26 | 7.7 | -0.45 | 0.000 | -0.3 | -0.014 | -3,629 | -30 |
| 19 | claude-haiku | qwen2.5-coder:7b | 35 | 22.9 | -0.50 | 0.000 | -0.4 | -0.011 | -2,365 | -33 |
| 20 | ollama-llama | qwen3:8b | 33 | 18.2 | -0.51 | 0.000 | -0.3 | -0.009 | -5,536 | -26 |
| 21 | gemini-2.5-pro | qwen3:14b | 28 | 10.7 | -0.61 | 0.000 | -0.2 | -0.008 | -11,030 | -21 |
| 22 | gpt-4o | ministral-3:3b | 17 | 11.8 | -0.64 | 0.000 | -0.4 | -0.023 | -206 | -34 |
| 23 | ollama-kimi | ministral-3:3b | 22 | 13.6 | -0.67 | 0.000 | -0.5 | -0.023 | -1,368 | -40 |
| 24 | grok-3 | qwen3:14b | 45 | 24.4 | -0.76 | 0.000 | -0.5 | -0.011 | -5,030 | -39 |
| 25 | ollama-deepseek | deepseek-r1:14b | 25 | 20.0 | -1.61 | 0.000 | -0.2 | -0.007 | -3,492 | -17 |
| 26 | dayblade-0dte | options-s2 | 133 | 5.3 | -3.03 | 0.000 | -0.9 | -0.007 | -3,781 | -61 |

## Per-model rollup

| Model | Agents | Closed | WR% | totR | P&L$ |
|-------|------:|------:|----:|----:|-----:|
| plutus-v1 | 1 | 97 | 93.8 | +23.8 | +4,254 |
| 8000 / Independent | 1 | 33 | 93.9 | +1.9 | +130 |
| ollie | 1 | 62 | 69.4 | +0.9 | +39 |
| deepseek-r1:7b | 1 | 17 | 100.0 | +0.7 | +32 |
| congress-copycat | 1 | 33 | 81.8 | +1.2 | -90 |
| qwen2.5-coder:7b | 1 | 35 | 22.9 | -0.4 | -2,365 |
| deepseek-r1:14b | 1 | 25 | 20.0 | -0.2 | -3,492 |
| options-s2 | 1 | 133 | 5.3 | -0.9 | -3,781 |
| qwen3:8b | 7 | 305 | 74.8 | +10.5 | -5,211 |
| ministral-3:3b | 8 | 236 | 66.9 | +8.5 | -8,300 |
| gemma3:4b | 1 | 49 | 32.7 | -0.1 | -12,316 |
| qwen3:14b | 2 | 73 | 19.1 | -0.7 | -16,060 |

## Per-strategy (asset-class proxy; strategy_id unlogged)

| Strategy | Agents | Closed | WR% | P&L$ |
|------|------:|------:|----:|-----:|
| stock | 19 | 805 | 75.5 | -15,174 |
| option | 7 | 293 | 12.6 | -31,985 |

**Population PBO (CSCV): 0.0284** (OK; n_splits=12870, agents=18)

## Notes
- **DSR gate (≥0.95):** 0 agents clear it — NONE. After multiple-testing deflation across 26 agents (SR0_null=2.4821), no track record is statistically robust yet — expected at ~150 days / modest n. Top raw-DSR: energy-arnold=0.3654, gemini-2.5-flash=0.2682, cto-grok42=0.1042.
- **Clear winners (top Sharpe):** energy-arnold, gemini-2.5-flash, cto-grok42. McCoy (ollama-plutus) leads on R/WR (+23.8 totR, 93.8% WR) though mid-Sharpe (variance from a few big wins).
- **Clear losers (Sharpe < −0.5):** ollama-llama, gemini-2.5-pro, gpt-4o, ollama-kimi, grok-3, ollama-deepseek, dayblade-0dte. dayblade-0dte (5.3% WR, −3.03) and ollama-local (−$12.3k) are the standouts to halt/review.
- **⚠ Suspect P&L (excluded-worthy):** none — large $ inconsistent with the (clipped) return series ⇒ likely unflagged option-multiplier/writeback artifacts. Their **Sharpe/DSR/WR are still valid** (return-based, clipped); only their P&L$ and the per-model $ rollup are distorted.
- 126 `known_contaminated` trades excluded. Ranking is by DSR→Sharpe (robust to the $ outliers).