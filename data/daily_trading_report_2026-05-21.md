# 📊 OllieTrades — Daily Trading Report

**Date:** 2026-05-21 (Thursday) · **Regime:** `BULL_CROSS` (SPY $742.71, size_modifier=1.0) · **Trader PID:** 76910 (uptime since 14:07 AZ)

---

## 🎯 Headline numbers

| Metric | Value |
|---|---|
| Total trades executed | **17** (10 BUY / 7 SELL) |
| Closes with realized PnL | 7 |
| Day realized PnL | **+$11.97** |
| Win rate (closes) | **6/7 = 85.7%** |
| Unique players acting | 2 (`ollie-auto`, `navigator`) |
| Unique symbols traded | 12 |
| Best trade | `navigator` EA SELL → **+$7.90** |
| Worst trade | `ollie-auto` SR SELL → **−$5.86** (Alpaca-routed) |
| Alpaca-paper equity arc | $101,591.35 → **$101,533.46** (−$57.89, −0.057%) |
| Cash on Alpaca | $94,409.20 |

---

## 🤖 Per-player breakdown

| Player | Trades | Buys | Sells | Closes | Wins | Losses | PnL | WR |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| `navigator` | 2 | 1 | 1 | 1 | 1 | 0 | **+$7.90** | 100.0% |
| `ollie-auto` | 15 | 9 | 6 | 6 | 5 | 1 | **+$4.07** | 83.3% |
| `neo-matrix` | 0 | — | — | — | — | — | — | (silent) |
| `super-agent` | 0 | — | — | — | — | — | — | (silent) |

**Notable absence:** `neo-matrix` (HM-AN2.3 live-fire agent) emitted 0 signals + 0 trades today. Per `project_hm_wr_cycle_rca` WR cycles have been silent since 14:24 AZ on 2026-05-20 across two restarts — still mystery-bound, today extends the dormancy. 13 carry-over open positions across the fleet untouched.

---

## 📜 Chronological trade tape (UTC)

```
07:16  ollie-auto  BUY  TKR  2.0511 @ $117.20   simulated
07:16  ollie-auto  BUY  XHB  2.3694 @ $98.92    simulated
14:39  ollie-auto  BUY  F    6.4579 @ $13.27    simulated
15:19  ollie-auto  SELL FBT  0.4064 @ $211.33   simulated     +$0.92
15:19  ollie-auto  SELL TKR  1.0255 @ $120.44   simulated     +$3.32
16:04  ollie-auto  BUY  JGRO 0.8984 @ $96.68    simulated
16:04  ollie-auto  BUY  SR   0.9261 @ $92.91    ALPACA-PAPER
16:34  ollie-auto  BUY  HD   0.2782 @ $306.38   simulated
17:17  ollie-auto  SELL F    3.2290 @ $13.64    simulated     +$1.19
17:17  ollie-auto  SELL HD   0.1391 @ $316.75   simulated     +$1.44
17:31  navigator   SELL EA   1.5000 @ $206.68   simulated     +$7.90  ★ best
18:17  navigator   BUY  PLD  3.0000 @ $140.37   simulated
19:01  ollie-auto  SELL FCX  2.0039 @ $62.31    simulated     +$3.06
19:01  ollie-auto  SELL SR   0.9261 @ $80.40    ALPACA-PAPER  −$5.86  ★ worst
19:29  ollie-auto  BUY  T    4.5846 @ $25.34    simulated
20:22  ollie-auto  BUY  RL   0.3212 @ $357.15   ALPACA-PAPER
20:28  ollie-auto  BUY  RL   0.3022 @ $374.90   ALPACA-PAPER  (add-on)
```

**SR scratch case** (only Alpaca-routed loss): BUY $92.91 → SELL $80.40 over 3h → −$5.86 realized. Captured via `alpaca_order_id` on both legs — this is the writeback-fix path working as designed (broker order ID present, real fill prices propagated).

**RL position-add**: ollie-auto opened 0.3212 @ $357.15, then averaged up at $374.90 (+$17.75/sh later) — weighted avg in `positions.avg_price` = $365.75. Position sits open.

---

## 📈 Open positions snapshot (51 total across fleet)

| Book | Open positions | Total basis |
|---|---:|---:|
| Alpaca paper (mirror book) | 20 | $7,008.43 |
| Enterprise Computer (physical metals tracking) | 2 | $8,119.64 |
| `capitol-trades` | 3 | $2,562.28 |
| `neo-matrix` | 3 | $2,367.27 |
| `ollama-plutus` (McCoy/CSP) | 1 | $2,188.19 |
| `ollama-qwen3` | 3 | $1,615.84 |
| `qwen3-8b-flash` (Worf scout) | 3 | $1,552.55 |
| `ollie-auto` | **8** ★ | $992.80 |
| `deepseek-7b-grok4` (Spock scout) | 1 | $664.34 |
| `navigator` | 5 | $437.73 |
| `cto-grok42` | 1 | $250.96 |
| `dalio-metals` | 1 | $77.36 |

★ `ollie-auto` carry-over: RGA (from 2026-05-20) + 7 new today (TKR, XHB, F, JGRO, HD, T, RL).

---

## 🚦 Gate behavior

- **Signal volume today:** 6 signals total (all from `deepseek-7b-grok4`, all **REJECTED** with `LOW_CONVICTION`)
  - SO 61% < 75% · URI 54% < 75% · CPAY 61% · LLY 61% · COST 61% · XOM 61%
  - Pattern: raw conf 0.75-0.85 → post-gate meta-conviction drops to 54-61% → rejected. This is the gate downgrade mechanism flagged in `project_hm_decision_support_observability_audit` (the +24-point delta is what the new `decision_audit` table will expose once shipped).
- **`MAX_TRADES_REACHED` (ollie-auto 15-cap):** 18 firings today, all post-15:45 UTC (08:45 AZ) — cap binding, scoring discarded after-hours
- **Grade-B Fleet Gate:** 0 firings (no SPY-down regime → gate was OPEN all day, consistent with `BULL_CROSS`+1.0 size_mod)

---

## ⚠️ Infrastructure events

| Marker | Count today | Notes |
|---|---:|---|
| `[WR-PROVIDER-TIMEOUT]` | **349** | Pre-fix — driven by `navigator` (78) + `ollama-kimi` (32) named timeouts plus 239 unnamed. `ollama-kimi` was halted exit_only at 14:51 UTC; remaining timeouts pre-halt or pre-restart |
| `[WR-PROVIDER-DUR]` | 316 | Normal cycle telemetry |
| `[WR-BUDGET-EXCEEDED]` | 0 | Layer 2a v1 not yet tripping (budget 925s) |
| `[WR-STALL]` | 0 | No stall alarms (cycles continue to fire silently per RCA mystery) |
| `[OLLAMA-CANCEL]` | 0 | Expected — fix shipped today (PR #70) but trader runs pre-fix bytecode until next restart |
| `[CHEKOV-CONF]` / `[CHEKOV-REENTRY-BLOCK]` | 0 | Activations dormant until Chekov fires (none today in this regime) |

**Restarts today:** 06:26 AZ · 13:07 AZ · 14:07 AZ (Item 1 of close-session paste — activated Chekov PRs #58 + #59 + writeback fixes #68 + #69 on disk).

---

## 🔧 Code shipped today

| Ship | Commit | Status |
|---|---|---|
| HM-CHEKOV-SL-COOLDOWN | `51bfddf` | Merged earlier |
| HM-CHEKOV-CONF-CALIBRATION (PR #58) | `671a87c` | Merged + active post-14:07 restart |
| HM-CHEKOV-REENTRY-GUARD (PR #59) | `2de353e` | Merged + active post-14:07 restart |
| HM-SLOW-FUNDAMENTALS Phase 1 (PR #60) | `5ade725` | Merged earlier; unused until Phase 2 |
| HM-CIC-OPEN-DESYNC (PR #62) | `5e37b67` | Merged earlier |
| HM-CIC-PANEL-UNPARK | `6a4541c` | Merged earlier |
| HM-TRADES-PRICE-WRITEBACK-FIX (PR #68) | `45e8533` | Merged this session |
| HM-POSITIONS-AVG-PRICE-WRITEBACK (PR #69) | `5db26e3` | Merged this session |
| HM-WR-CANCEL-ON-TIMEOUT (PR #70) | `7983dd1` | Merged this session |
| HM-BULK-PRICES-FIXTURE-FIX (PR #71) | `bb19f8d` | Merged this session |
| HM-SLOW-FUNDAMENTALS Phase 2 | `190b3b2` | **Awaiting merge** (branch `hm-slow-fundamentals-phase-2`) |

---

## 📝 Recap & takeaways

1. **Quiet trading day, no drama.** +$11.97 day realized PnL, 6W/1L closes, two players acting. The Alpaca-paper equity arc moved sideways (−0.057%); all wins came from small `ollie-auto` scalps + one clean `navigator` EA exit.

2. **`neo-matrix` silent since 2026-05-20 14:24 AZ.** Today extends the dormancy — `project_hm_wr_cycle_rca` Phase 1 ruled out 9 candidate causes; Phase 2 instrumentation (`schedule.jobs` dump + 60s heartbeat) banked but not deployed. Priority HIGH.

3. **Sniper Squad essentially muted today.** Only `deepseek-7b-grok4` emitted (6 signals, 0% pass-rate) — gate downgrade math (raw 0.85 → meta-conv 0.61) rejected everything. `qwen3-8b-flash` produced nothing.

4. **Writeback fixes active for SR + RL.** SR scratch loss (−$5.86) and RL averaged-up position both have `alpaca_order_id` populated — Phase 1 of the price-truth migration (trades-row writeback) is observably working on new Alpaca-routed trades.

5. **Code velocity high.** Five PRs shipped today via this session alone (#68 #69 #70 #71 + pending #72). Phase 2 endpoint rewire awaiting merge — will activate at next restart and collapse `/api/{trendlines,patterns,pattern-alerts,channels}` from stale-cache-bound to <5s warm.

6. **Pending Captain follow-ups:**
   - Merge `hm-slow-fundamentals-phase-2` PR
   - Restart trader to activate PRs #70, #71, #72 simultaneously
   - After one clean WR cycle proves no `[WR-PROVIDER-TIMEOUT]` on ministral-3:3b → reactivate `dalio-metals` + `ollama-kimi` per banked SQL in `project_hm_wr_ollama_queue_starvation`
   - HM-WR-CYCLE-RCA Phase 2 instrumentation still queued
   - HM-DECISION-SUPPORT-OBSERVABILITY v1 (`decision_audit` table) — would explain today's 24-point gate-downgrade math

---

*Report generated 2026-05-21 from read-only queries against `data/trader.db` + `logs/trader.log`. No state changes made.*
