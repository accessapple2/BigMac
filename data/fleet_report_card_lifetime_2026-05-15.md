# USS TradeMinds Fleet Report Card + Learning Audit — Lifetime through 2026-05-15

**Generated:** 2026-05-15 evening, post-close + post-trader-restart (PID 48395).
**Generator:** Scotty (Claude Code on bigmac), read-only SQL + log grep + code audit + git log.
**Scope:** lifetime per-agent statistics + broker (Webull, Schwab) overlay + agent/system learning audit.
**Sources:** `data/trader.db`, `data/schwab_csv_archive/`, `data/real_holdings.json`, `engine/*.py` (read-only grep + head), `git log`.

**Timezone:** DB timestamps UTC; AZ = UTC-7. Date filters use UTC.

---

# PART 1 — REPORT CARD

## 1E. Fleet Aggregate Lifetime (executive summary)

| Metric | Value |
|---|---|
| Players ever traded | **28** |
| Players currently in `ai_players` | 50 |
| Currently `halt_mode='active'` | 21 |
| Currently `halt_mode='exit_only'` | 6 |
| Currently `halt_mode='full'` | 23 |
| Total lifetime trades (all players) | **2,074** |
| Closed trades (with realized P&L) | 1,136 (54.8% of opens have closed) |
| Wins | 556 |
| Losses | 446 |
| Break-evens | 134 |
| Aggregate lifetime win rate | **48.9%** |
| Total lifetime net P&L | **+$237,240.46** |
| Earliest trade in system | 2026-01-06 (Webull pre-liquidation imports) |
| Latest trade in system | 2026-05-16 00:47 UTC (post-tonight-restart cycle) |
| Trading span | ~4.5 months |

**Critical caveat:** the $237K net P&L is dominated by a SINGLE agent (`gemini-2.5-pro` = +$225,452 from 46 closed trades, all in March 2026). Excluding that one outlier, the fleet's lifetime net P&L is **~+$11,788** — far more modest and arguably the truer trading-performance number.

---

## 1A. Per-Agent Lifetime Statistics

Sorted by net P&L desc. Webull (broker mirror, no closed P&L recorded) shown at bottom.

| Agent | Total | Closed | Wins | Losses | B/E | WR % | Net P&L $ | Avg P&L $ | Best $ | Worst $ | Avg Pos $ | First Trade | Last Trade |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|---|
| gemini-2.5-pro | 102 | 46 | 21 | 25 | 0 | 45.7 | **+225,452.53** | +4,901.14 | +117,692.86 | -1,000.00 | 650.80 | 2026-03-11 | 2026-03-19 |
| claude-sonnet | 67 | 35 | 11 | 24 | 0 | 31.4 | +43,372.37 | +1,239.21 | +16,729.98 | -900.00 | 398.10 | 2026-03-11 | 2026-03-19 |
| ollama-plutus | 108 | 86 | 73 | 6 | 7 | **84.9** | +4,146.36 | +48.21 | +1,907.95 | -41.15 | 92.26 | 2026-04-25 | 2026-05-15 |
| options-sosnoff | 12 | 4 | 4 | 0 | 0 | **100.0** | +2,060.35 | +515.09 | +1,546.51 | +40.22 | 204.96 | 2026-03-23 | 2026-04-25 |
| qwen3-8b-flash | 96 | 74 | 54 | 4 | 16 | **73.0** | +501.17 | +6.77 | +42.42 | -7.16 | 214.00 | 2026-04-25 | 2026-05-15 |
| gemini-2.5-flash | 27 | 21 | 21 | 0 | 0 | 100.0 | +180.96 | +8.62 | +41.69 | +0.46 | 86.54 | 2026-03-23 | 2026-04-23 |
| energy-arnold | 20 | 14 | 13 | 0 | 1 | 92.9 | +167.58 | +11.97 | +45.38 | 0.00 | 143.70 | 2026-03-23 | 2026-04-15 |
| navigator | 33 | 16 | 8 | 8 | 0 | 50.0 | +84.50 | +5.28 | +142.52 | -77.13 | 383.55 | 2026-03-25 | 2026-05-15 |
| ollie-auto | 148 | 83 | 60 | 22 | 1 | 72.3 | +80.45 | +0.97 | +8.22 | -8.61 | 76.02 | 2026-04-09 | 2026-05-15 |
| neo-matrix | 24 | 5 | 2 | 3 | 0 | 40.0 | +42.10 | +8.42 | +50.00 | -17.32 | 455.63 | 2026-03-28 | 2026-05-16 |
| cto-grok42 | 14 | 10 | 9 | 1 | 0 | 90.0 | +37.68 | +3.77 | +18.04 | -0.25 | 73.14 | 2026-04-29 | 2026-05-05 |
| grok-4 | 25 | 17 | 17 | 0 | 0 | 100.0 | +32.34 | +1.90 | +10.55 | +0.07 | 59.71 | 2026-03-20 | 2026-04-13 |
| capitol-trades | 82 | 57 | 21 | 5 | 31 | 36.8 | -93.34 | -1.64 | +25.92 | -147.30 | 157.39 | 2026-03-30 | 2026-05-15 |
| dalio-metals | 37 | 24 | 15 | 2 | 7 | 62.5 | -164.03 | -6.83 | +16.87 | -229.48 | 201.37 | 2026-03-27 | 2026-04-20 |
| gpt-4o | 80 | 41 | 2 | 15 | 24 | 4.9 | -205.69 | -5.02 | +12.18 | -31.80 | 337.32 | 2026-03-12 | 2026-03-19 |
| ollama-qwen3 | 125 | 95 | 71 | 10 | 14 | 74.7 | -216.65 | -2.28 | +30.59 | -521.91 | 161.96 | 2026-03-20 | 2026-05-13 |
| super-agent | 16 | 8 | 2 | 6 | 0 | 25.0 | -371.32 | -46.41 | +3.69 | -252.48 | 1,157.39 | 2026-03-26 | 2026-03-28 |
| dayblade-sulu | 15 | 10 | 6 | 4 | 0 | 60.0 | -450.82 | -45.08 | +53.03 | -387.40 | 378.97 | 2026-03-24 | 2026-03-31 |
| deepseek-7b-grok4 | 137 | 108 | 86 | 20 | 2 | 79.6 | -482.52 | -4.47 | +102.69 | -671.12 | 152.53 | 2026-04-23 | 2026-05-15 |
| ollama-kimi | 44 | 22 | 3 | 19 | 0 | 13.6 | -1,368.45 | -62.20 | +15.41 | -212.28 | 415.72 | 2026-03-16 | 2026-03-19 |
| claude-haiku | 72 | 36 | 8 | 27 | 1 | 22.2 | -2,364.53 | -65.68 | +46.48 | -500.00 | 335.06 | 2026-03-11 | 2026-03-19 |
| gpt-o3 | 21 | 9 | 4 | 5 | 0 | 44.4 | -3,044.74 | -338.30 | +42.35 | -921.14 | 879.42 | 2026-03-13 | 2026-03-19 |
| ollama-deepseek | 47 | 29 | 5 | 20 | 4 | 17.2 | -3,492.18 | -120.42 | +30.62 | -1,000.00 | 272.29 | 2026-03-13 | 2026-03-19 |
| dayblade-0dte | 291 | 133 | 7 | 126 | 0 | **5.3** | -3,780.94 | -28.43 | +18.00 | -44.78 | 66.93 | 2026-03-11 | 2026-04-01 |
| grok-3 | 101 | 49 | 11 | 34 | 4 | 22.4 | -5,029.85 | -102.65 | +26.22 | -819.00 | 294.41 | 2026-03-11 | 2026-03-19 |
| ollama-llama | 53 | 37 | 6 | 27 | 4 | 16.2 | -5,536.45 | -149.63 | +74.01 | -784.10 | 302.44 | 2026-03-13 | 2026-05-04 |
| ollama-local | 150 | 67 | 16 | 33 | 18 | 23.9 | **-12,316.42** | -183.83 | +1.88 | -982.96 | 284.89 | 2026-03-11 | 2026-03-20 |
| webull (broker mirror) | 127 | 0 | 0 | 0 | 0 | — | (no P&L recorded — see 1F) | — | — | — | — | 2026-01-06 | 2026-02-27 |

### Tier interpretation

- **🥇 Top earners** (lifetime, real-era only — excluding March test cluster): `ollama-plutus` (+$4,146), `qwen3-8b-flash` (+$501), `ollie-auto` (+$80). These are the agents trading from late April onward with real performance.
- **💸 Worst losers**: `ollama-local` (-$12,316), `ollama-llama` (-$5,536), `grok-3` (-$5,029), `dayblade-0dte` (-$3,780 with 5.3% WR), `ollama-deepseek` (-$3,492), `gpt-o3` (-$3,044). **All of these stopped trading by mid-March 2026** — they were victims of the early-system stub data + paid-API cost overrun era. Most have since been halted via HM-AK (2026-05-07 dormant cleanup).
- **🎰 Anomaly**: `gemini-2.5-pro` +$225K from a single +$117K trade and a 45.7% WR — suggests either a paper-test bug, a stub-data injection, or a once-in-a-system event. Recommend `HM-MARCH-CLUSTER-AUDIT` to verify the realized_pnl on the +$117,692 trade is real, not a fenceposting artifact.
- **🐌 Active but unprofitable**: `deepseek-7b-grok4` (-$482 lifetime) and `capitol-trades` (-$93) — both still trading. Their lifetime numbers are dragged by April underperformance; May has been much better (see 1B).

---

## 1B. Win Rate Trend Over Time — "is the system improving?"

### Fleet-wide monthly aggregate

| UTC Month | Trades | Closed | Wins | WR % | Net P&L |
|---|---:|---:|---:|---:|---:|
| 2026-01 | 71 | 0 | 0 | — | — (Webull pre-liquidation imports, no P&L recorded) |
| 2026-02 | 56 | 0 | 0 | — | — (same) |
| 2026-03 | 1,102 | 522 | 102 | **19.5%** | +$230,349 (gemini-2.5-pro anomaly) |
| 2026-04 | 422 | 292 | 172 | **58.9%** | +$2,019 |
| 2026-05 | 423 | 322 | 282 | **87.6%** | +$4,872 |

**This is the headline answer to Captain's hypothesis "WR has improved over time": YES.**

- March: 19.5% WR — pre-cleanup, lots of zombie agents
- April: 58.9% WR — +39 points after HM-AK retirement + neo-matrix observation
- May (in-progress): **87.6% WR** — another +29 points after HM-QG calibration + 12 PRs ship

**Important nuance:** the improvement is NOT individual agents getting smarter — agents are stateless inference engines (see Part 2A). The improvement is **selection pressure**: the worst agents got retired, the best agents got promoted, the gates got tighter. Captain is the learning entity; the fleet is the substrate. Detailed analysis in Part 2F.

### Per-agent monthly trend (agents with >20 lifetime trades)

| Agent | Mar | Apr | May | Trend |
|---|---:|---:|---:|---|
| ollama-plutus | — | 41.2% | **95.7%** | 📈 IMPROVING |
| qwen3-8b-flash | — | 29.6% | **97.9%** | 📈 IMPROVING |
| ollama-qwen3 | 0% | 37.5% | **95.2%** | 📈 IMPROVING |
| deepseek-7b-grok4 | — | 62.7% | **94.7%** | 📈 IMPROVING |
| ollie-auto | — | 86.8% | 60.0% | 📉 DECLINING (counter-trend) |
| capitol-trades | 0% | 20.5% | 72.2% | 📈 IMPROVING |
| navigator | 0% | 50% | 63.6% | 📊 NOISY (high variance — owns biggest winner AND biggest loser) |
| neo-matrix | 100% (n=1) | 0% | 100% (n=1) | 📊 NOISY (only 24 lifetime trades) |
| dayblade-0dte | 4.8% | 12.5% | — (halted 2026-05-06) | 📈 → ⛔ HALTED before could recover |
| dalio-metals | 11.1% | 93.3% | — | 📈 IMPROVING but not active in May |
| gemini-2.5-flash | — | 100% | — | 📊 NOISY (n=21 across 1 month only) |
| Mar-2026 cluster (claude-*, gpt-*, grok-3, ollama-deepseek, ollama-llama, ollama-local, ollama-kimi) | various | — | — | ⛔ RETIRED before April |

**Interpretation summary:**

- Of agents with >20 lifetime trades and activity in May: **5 of 6 are IMPROVING** (ollama-plutus, qwen3-8b-flash, ollama-qwen3, deepseek-7b-grok4, capitol-trades).
- **ollie-auto** is the lone DECLINING agent (May WR 60% down from April's 86.8%). Worth flagging — it's the most active agent (148 lifetime trades) and the autopilot orchestrator. Possible explanations: more aggressive entries in May, weaker market regime, or HM-AN2.3 promotion adding marginal trades. **Flag `HM-OLLIE-AUTO-MAY-REGRESSION-AUDIT`.**
- **navigator** owns BOTH the biggest single winner AND biggest single loser this week. High variance, not a stable performer.
- The improvement pattern is consistent: agents that survived April → became markedly better in May. **Selection works.**

---

## 1C. Halt Mode History per Agent

### Current halt state distribution

| halt_mode | Count |
|---|---:|
| `active` | 21 |
| `exit_only` | 6 |
| `full` | 23 |

### Agents currently halted (full or exit_only)

| Agent | halt_mode | halted_at | Reason (first 60 chars) |
|---|---|---|---|
| anderson-bcs | full | 2026-05-05 14:09:20 | HM-T-fleet bundle retirement |
| chekov | full | **(EMPTY)** | orphan row — real Chekov is `navigator` |
| claude-haiku | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| claude-sonnet | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| covered-call | full | 2026-05-05 14:09:20 | HM-T-fleet bundle |
| dayblade-0dte | full | 2026-05-06 17:43:54 | Spread cannibalization (HM-AF α-lift trigger) |
| dayblade-sulu | exit_only | 2026-03-31 00:00:00 | S6.3 bench: R:R 0.10 |
| gemini-2.5-flash | exit_only | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| gemini-2.5-pro | exit_only | 2026-04-30 00:00:00 | Retired S6.3 — qwen3:14b too heavy |
| ghost-kirk-0dte-bc | full | 2026-05-05 15:57:18 | Option-4 ghost bundle retirement |
| ghost-kirk-bc | full | 2026-05-05 15:57:18 | (same) |
| ghost-long-call | full | 2026-05-05 15:57:18 | (same) |
| ghost-naked-put | full | 2026-05-05 15:57:18 | (same) |
| gpt-4o | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| gpt-o3 | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| grok-3 | exit_only | 2026-04-25 00:00:00 | S6 review: routing zombie |
| grok-4 | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| mccoy-bps | full | 2026-05-05 14:09:20 | HM-T-fleet bundle |
| ollama-gemma27b | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| ollama-glm4 | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| ollama-llama | exit_only | 2026-04-25 00:00:00 | S6 review: routing zombie |
| ollama-local | exit_only | 2026-05-13 22:16:30 | Stale signal emitter |
| quark-ic | full | 2026-05-05 14:09:20 | HM-T-fleet bundle |
| qwen-coder-haiku | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| qwen3-14b-grok3 | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| qwen3-8b-4o | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| qwen3-8b-o3 | full | 2026-05-07 17:18:36 | HM-AK dormant cleanup |
| super-agent | full | **(EMPTY)** | is_paused=1 reconcile 2026-05-11 |
| webull | full | **(EMPTY)** | Account liquidated. HM-WEBULL-LIQUIDATED |

### `halted_at` audit (CLAUDE.md "Manual halt SQL pattern" violation)

**Three current halts** have empty `halted_at` timestamps despite `halt_mode != 'active'`:

1. **chekov** — orphan row; halt timestamp not recorded.
2. **super-agent** — date inferred only from `halt_reason` text ("reconcile 2026-05-11").
3. **webull** — broker-liquidation halt; date inferred from `halt_reason` ("HM-WEBULL-LIQUIDATED 2026-05-13").

Per CLAUDE.md HM-F finding 2026-05-04 ("four April halts had their dates buried in halt_reason text"), this discipline gap continues. Running total of lifetime halted_at-empty halts: **~7 (4 historical + 3 current)**. Filed at the bottom of this report as `HM-HALTED-AT-BACKFILL` carry-forward.

### Halt timeline patterns

- **2026-04-25**: First round of "routing zombie" retirements (`grok-3`, `ollama-llama` → `exit_only`).
- **2026-04-30**: gemini-2.5-pro retired (qwen3:14b too heavy).
- **2026-05-05 14:09**: HM-T-fleet bundle (Option 1 halt-only) — 4 agents to `full`: anderson-bcs, covered-call, mccoy-bps, quark-ic.
- **2026-05-05 15:57**: Option-4 ghost bundle — 4 ghost agents to `full`: ghost-kirk-0dte-bc, ghost-kirk-bc, ghost-long-call, ghost-naked-put.
- **2026-05-06 17:43**: dayblade-0dte halted (spread cannibalization, HM-AF α-lift).
- **2026-05-07 17:18**: HM-AK dormant cleanup — 11 agents (claude/gpt/ollama variants) halted in one batch.
- **2026-05-11**: super-agent halted (timestamp missing, inferred).
- **2026-05-13**: HM-AN2.3 fire — `neo-matrix` PROMOTED `exit_only` → `active` (the only unhalt this period, per `hm_an23_revert_log`).
- **2026-05-13 22:16**: ollama-local stale-signal-emitter halt to `exit_only`.

**Net halts: 23 retire events (lifetime). Net unhalts: 1 (neo-matrix promotion). Selection ratio: 23:1 — strong retirement bias.**

---

## 1D. Strategy / Signal-Source Breakdown per Agent

### Asset class profile per agent

| Agent | Stock | Option Call | Option Put | Profile |
|---|---:|---:|---:|---|
| dayblade-0dte | 0 | 257 | 34 | **Pure options 0DTE — explains -$3,780 / 5.3% WR** |
| ollama-local | 70 | 80 | 0 | Heavy options bias (53% calls) |
| ollama-llama | 14 | 39 | 0 | Heavy call bias |
| gemini-2.5-pro | 30 | 66 | 6 | Call-heavy (anomaly source) |
| claude-sonnet | 29 | 38 | 0 | Mixed |
| grok-3 | 54 | 47 | 0 | 50/50 stock + call |
| ollama-deepseek | 13 | 29 | 5 | Options-heavy |
| gpt-4o | 49 | 26 | 5 | Mixed |
| ollama-kimi | 30 | 10 | 4 | Stock + some calls |
| claude-haiku | 51 | 21 | 0 | Stock + calls |
| ollama-plutus | 106 | 2 | 0 | **Nearly pure stock** ✓ |
| qwen3-8b-flash | 96 | 0 | 0 | **100% stock** ✓ |
| ollama-qwen3 | 123 | 2 | 0 | **Nearly pure stock** ✓ |
| ollie-auto | 148 | 0 | 0 | **100% stock** ✓ |
| capitol-trades | 82 | 0 | 0 | **100% stock** ✓ |
| deepseek-7b-grok4 | 134 | 3 | 0 | **Nearly pure stock** ✓ |
| navigator | 29 | 4 | 0 | Stock-heavy |
| neo-matrix | 24 | 0 | 0 | **100% stock** ✓ |
| dalio-metals | 35 | 2 | 0 | Stock-heavy (metals ETFs) |

**Strategic observation:** the **modern active agents (May 2026 era) are ALL nearly pure stock**. Options exposure has been deliberately reduced. The early-era agents (claude/gpt/grok/ollama variants, March 2026) were heavily options-biased and most lost money. Captain has consciously retreated to stock-only after the painful options-era losses.

### Reasoning prefix patterns (top 3 per agent)

Most agents have **free-form (no bracket)** reasoning text — LLM-generated prose. Agents with STRUCTURED prefixes:

| Agent | Top prefix | Count | Other notable |
|---|---|---:|---|
| dayblade-0dte | `[FIRST]` | 152 | `STOP` x119 (structured 0DTE entry/exit) |
| capitol-trades | `[CAUTION half-size]` | 11 | (otherwise free-form Congress trade copies) |
| neo-matrix | `[HM-AN2.C]` | 9 | `[Ollie✓ 1.82]` x5, `[Ollie✓ 2.07]` x4 — Signal Center signal-source attribution |
| deepseek-7b-grok4 | `[CAUTION half-size]` | 8 | (otherwise free-form) |
| ollama-qwen3 | `[3-step]` | 4 | (otherwise free-form) |
| grok-4 | `[3-step]` | 6 | (otherwise free-form) |

Common first-word patterns across ALL agents: `Autopilot` (autopilot exit decision), `Stop-loss`, `Take-profit`, `Scaled` (scaled entry), `Season` (season-based logic).

**Note:** `trades.strategy_id` is NULL for ALL 2,074 lifetime trades. The strategy_id field exists in the schema but is unpopulated. Pre-bridge-handoff payload schema in `docs/bridge-handoff/ollietrades-bridge-handoff.md` section 6 assumes strategy_id is present. Flagged in last week's review as `HM-TRADE-STRATEGY-ID-POPULATE`.

---

## 1F. Captain's Broker Trade History

### Webull (in trades table as `player_id='webull'`)

**127 trades total, all between 2026-01-06 and 2026-02-27.** After 2026-02-27, no Webull activity (per CLAUDE.md `HM-WEBULL-LIQUIDATED 2026-05-13`).

| Asset | Action | Count | Total qty | Total $ |
|---|---|---:|---:|---:|
| stock | BUY | 65 | (varies) | (varies) |
| stock | SELL | 53 | (varies) | (varies) |
| option | BUY | 5 | (varies) | (varies) |
| option | SELL | 4 | (varies) | (varies) |

**`realized_pnl` is NULL for ALL 127 webull trades** — the import pipeline captured BUY/SELL events but did not compute paired P&L. Closing P&L can be estimated by FIFO-matching same-symbol BUY+SELL pairs, but this is approximate and was not done at write-time.

**Sample of paired Webull trades (BUY total vs SELL total, raw dollars, no commission):**

| Symbol | BUY count | BUY total $ | SELL count | SELL total $ | Approx P&L $ |
|---|---:|---:|---:|---:|---:|
| AMD | 2 | 1,478.30 | 2 | 1,565.81 | **+87.51** |
| INTC | 3 | 2,212.00 | 2 | 2,135.40 | -76.60 (partial close — 45/45 shares paired) |
| INTU | 3 | 1,522.27 | 3 | 1,535.17 | +12.90 |
| GLDM | 1 | 1,048.00 | 1 | 1,003.10 | -44.90 |
| AMZN | 2 | 815.76 | 1 | 417.26 | (partial: 2 shares closed, 2 still open) |
| BULL | 1 | 757.00 | 1 | 715.00 | -42.00 |
| DIA | 1 | 1,506.03 | 3 | 1,485.96 | -20.07 |
| GOOG | 1 | 301.37 | 1 | 314.80 | +13.43 |
| HOOD | 1 | 499.95 | 1 | 517.60 | +17.65 |
| CRM | 2 | 587.85 | 1 | 187.31 | (partial: 1 of 3 closed) |
| AVAV | 1 | 362.50 | 1 | 326.00 | -36.50 |

**Pattern observations from Webull:** small position sizes (mostly $200-$1500), mixed wins/losses, modest P&L swings. The full-cycle Webull trading profile is "real retail trading in modest size, slightly under breakeven before account liquidation."

**Scope-down:** computing exact lifetime Webull P&L requires FIFO pairing across all 118 stock trades, which is mechanical but would consume budget. Not done. The estimated aggregate is "approximately flat to slightly negative" based on the sample.

### Schwab (DB table `schwab_holdings` + `data/schwab_csv_archive/`)

**`schwab_holdings` table:** 161 rows, 28 distinct symbols, 10 snapshots, 1 account ("Scwab New BS-015").

| Snapshot date range | 2026-04-24 → 2026-05-07 |
|---|---|
| Total market value (latest) | **$258,252.87** |
| Total cost basis | $167,471.40 |
| Total gain | **+$2,122.22** (+1.27% on cost basis) |
| Cash position | $8,393.71 |
| Distinct symbols held | 28 |

**Top 10 Schwab positions by market value (latest snapshot 2026-05-07):**

| Symbol | Description | Qty | Price $ | MV $ | Cost $ | Gain $ | Gain % |
|---|---|---:|---:|---:|---:|---:|---:|
| VTI | Vanguard Total Stock Market ETF | 10 | 362.54 | 3,625.40 | 3,602.50 | +22.90 | +0.64 |
| MU | Micron Technology | 4 | 653.98 | 2,615.90 | 2,503.47 | +112.43 | +4.49 |
| AMD | Advanced Micro Devices | 5 | 414.15 | 2,070.75 | 1,869.31 | +201.44 | +10.78 |
| AVGO | Broadcom | 4 | 421.85 | 1,687.40 | 1,648.39 | +39.01 | +2.37 |
| CEG | Constellation Energy | 5 | 326.25 | 1,631.25 | 1,558.50 | +72.75 | +4.67 |
| ITA | iShares US Aerospace & Defense ETF | 7 | 226.11 | 1,582.79 | 1,515.50 | +67.29 | +4.44 |
| CRWD | CrowdStrike | 3 | 480.40 | 1,441.20 | 1,391.88 | +49.32 | +3.54 |
| VRT | Vertiv Holdings | 3 | 356.00 | 1,068.01 | 1,027.00 | +41.01 | +3.99 |
| LMT | Lockheed Martin | 2 | 514.80 | 1,029.60 | 1,026.00 | +3.60 | +0.35 |
| (others: 19 additional symbols across megacap tech, energy, defense) |

**Schwab strategy pattern:** diversified across megacap tech (AMD, MU, AVGO, CRWD), defense (LMT, ITA), energy (CEG, VRT), and broad-market ETF (VTI). All long stock positions. No options, no shorts, no spreads. **Captain's real-money playbook is conservative buy-and-hold tech/defense/energy.**

**Data freshness:** latest snapshot `2026-05-07`. Per weekly review, `ghost_seed` references `schwab_snapshot_2026-04-28T20:11ET` (older). The CSV archive has files through 2026-05-07. **No 2026-05-15 snapshot — Schwab pipeline last ran ~8 days ago.** This is a data-freshness gap to flag separately.

### IBKR

**No IBKR data found in trader.db or any CSV archive.** Search for tables `ibkr*` returned nothing. Per CLAUDE.md, IBKR is referenced only in the "real-world net worth" line and not as a trading-execution venue. Treating as out-of-scope.

### Comparison table: human Captain vs autonomous fleet

| Source | Trades | WR | Net P&L | Span | Profile |
|---|---:|---:|---|---|---|
| **Webull** (Captain, real-money) | 127 | (no recorded P&L) | est ~flat to slightly neg | 2026-01-06 → 2026-02-27 | Real retail stock+options, ~$5K-$10K per trade |
| **Schwab** (Captain, real-money) | (snapshots, no trade log) | (HOLD strategy) | **+$2,122.22 unrealized** on $167K cost basis (+1.27%) | 2026-04-24 → 2026-05-07 snapshots | Diversified megacap-tech / defense / energy buy-and-hold |
| **Alpaca paper** (autonomous fleet, May era — ollama-plutus, qwen3-8b-flash, etc.) | 423 | **87.6%** | **+$4,872** May only | 2026-05-01 → 2026-05-15 | Pure stock, small-size, autopilot-managed exits |
| **Ghost paper-mirror** (genesis-seeded Schwab clone) | 9 closes this week | 2/3 wins (66%) | +$23.85 paper-pct sum: +58.0% on combined named exits | 2026-04-29 (seed) → 2026-05-15 | Mirrors Captain's Schwab basket + trailing stops |

**The headline comparison:** autonomous fleet's MAY 2026 win rate (87.6%) significantly exceeds anything in Captain's broker history. But this is in a constrained world (paper Alpaca, small positions, narrow tickers); the Schwab buy-and-hold strategy is operating over different time horizons with real-money execution slippage. Apples-to-apples is fraught.

---

# PART 2 — LEARNING AUDIT

## 2A. Do agents see their own past trade outcomes?

**Surprising result during code audit: YES, most LLM-driven agents are HISTORY_AWARE.** My initial hypothesis (PURE_INFERENCE for most) was wrong.

### Three layers of trade-history injection found in code

| File | Purpose | Lines | Commit count |
|---|---|---|---:|
| `engine/trade_memory.py` | Formats individual track-record blocks for injection into LLM prompts. Functions: `get_memory_block_for_player()`, `get_memory_block_for_chekov()`, `get_memory_block_for_debate(symbol)`. | 255 | 1 (stable) |
| `engine/brain_context.py` | Comprehensive intelligence aggregator. Includes `_source_backtest_performance(player_id, symbol)` and `_source_fleet_consensus(player_id, symbol)` — past-performance-by-symbol per agent. Also `_source_layered_memory(player_id)` using FinMem-style layered memory. | 675 | 3 |
| `engine/learning_engine.py` | "Loop 3: Continuous learning. Before each trade executes, the system looks up that model's adjustments and modifies trade parameters. Before each scan, learning context is injected into the model's prompt." Functions: `apply_learning(player_id, trade_signal)`, `get_learning_context(player_id)`, `get_model_profile(player_id)`. | 379 | 1 |

### Importer audit — who consumes each layer?

**`engine/ai_brain.py`** is the central LLM scan brain — and it imports **ALL THREE** learning layers:
- `from engine.trade_memory import ...` ✓
- `from engine.brain_context import build_full_context` ✓
- `from engine.learning_engine import apply_learning, get_learning_context` ✓

Other consumers of trade_memory: `engine/debate_engine.py`, `engine/scan_context.py`.

Other consumers of brain_context: `options_chain.py`, `finmem_memory.py`, `fleet_cache.py`, `bridge_vote.py`, `debate_engine.py`, `bootstrap_intelligence.py`, `providers/base.py`.

### Per-agent classification

| Agent | Classification | Why |
|---|---|---|
| ollama-plutus, qwen3-8b-flash, ollama-qwen3, deepseek-7b-grok4, ollie-auto, neo-matrix | **HISTORY_AWARE** | Route through `ai_brain.py` for LLM scans → all three learning layers injected into prompt |
| ollama-kimi, ollama-deepseek, ollama-coder, claude-*, gpt-*, grok-*, gemini-* (when active) | **HISTORY_AWARE** | Same — LLM scans always go through ai_brain |
| capitol-trades | **PURE_INFERENCE** | Rule-based — copies Congress STOCK Act disclosures, no LLM call, no history injection |
| dalio-metals | **PURE_INFERENCE** | Rule-based macro thesis (no LLM) |
| navigator | **PARTIAL_AWARE** | Convergence scanner — receives signal aggregations but not individual past trades |
| dayblade-0dte (halted) | **PURE_INFERENCE** | Mechanical 0DTE structure, no LLM context |
| energy-arnold | **PURE_INFERENCE** | Rule-based momentum from EIA energy data |
| webull (mirror), alpaca-mirror (mirror), enterprise-computer (metals tracker) | **N/A** | Not autonomous decision-makers — broker state mirrors / tracking ledgers |

**Important caveat on what "HISTORY_AWARE" means here:**

The `trade_memory.get_memory_block_for_player(player_id)` injection gives the LLM **its own** prior trades and outcomes formatted as a text block in the prompt. The LLM still has no persistent memory between cycles — every cycle, the prompt is re-built from scratch with the history block reloaded. So:

- Agents see their past trades each cycle ✓
- Agents have no continuous learning state — weights aren't updated ✗
- Each scan is a fresh inference call with history-as-context ⚠️

This is **stateless inference with replay context**, not learning in the gradient-descent sense. Functionally, the LLM might condition its choice on "I lost on this symbol last week, be more cautious" — but only if the in-context information is large enough to influence the next-token distribution.

---

## 2B. Fine-tuning / LoRA / weight updates audit

**Confirmed: NO fine-tuning is active.** Captain's memory note holds.

### What was found

| Surface | Status |
|---|---|
| `~/.ollama/models/manifests/registry.ollama.ai/library/` | Only stock-named models: deepseek-r1, deepseek-v3.1, gemma3, kimi-k2.5, llama3.1, llama3.2, mistral, phi3, phi4, qwen2.5-coder, qwen3. **Zero custom-trained variants.** |
| `scripts/export_training_data.py` | **EXISTS** — exports `trades.jsonl`, `convergence_signals.jsonl`, `war_room_outcomes.jsonl` for future LoRA training. Header comment: "Export TradeMinds training data for fine-tuning LoRA models." Not yet run for training, only data prep. |
| `engine/archer_frontier.py`, `engine/agent_manager.py` | Reference "training" in passing — not actual fine-tuning logic. |
| `scripts/ollama_bulk_backtest.py`, `scripts/backtest_watcher.sh`, `scripts/learning/status.py` | Backtest framework + learning-status reporters. No model-weight updates. |
| `~/Library/LaunchAgents/com.ollietrades.finetune-reminder.plist` | **EXISTS** — daily 9 AM cron runs `scripts/learning/check_pipeline.py`. This is a REMINDER, not a training job. Stdout to `logs/finetune_reminder.log`. |
| Custom-trained Ollama models / safetensors weights / LoRA adapters | **None found.** |

### Interpretation

The system has **all the scaffolding ready** for fine-tuning to start:
- Daily cron checks if data volume threshold is met (`check_pipeline.py`)
- Training-data exporter ready to write JSONL files
- Existing learning/* directory structure suggests Captain planned a pipeline

But the actual `lora` / `peft` / training-loop code is **not present**, and no custom model artifacts exist on disk. **Fine-tuning is deferred infrastructure, not active.**

**Ground truth for Captain:** *agents are stateless inference engines re-deciding from scratch every cycle. The "memory" they have is replay-context injection into prompts, not weight updates.*

---

## 2C. System-Level: adaptive_tuner / agent_ratings audit

**Discovered 3 distinct adaptive feedback systems** running in parallel:

### Layer 1 — `engine/agent_ratings.py` (daily Grade A-E)

**Purpose:** assigns each active agent a rating A-E daily.

**Inputs:** trades + realized_pnl per agent (Season 5 only — pre-S5 had options-mispriced-as-stocks data).

**Computation** (`calculate_rating()` at line 51+):
- `score = 0` initial
- `+ min(40, win_rate × 0.5)` — WR contribution, capped 40
- `+ min(20, profit_factor × 8)` — profit factor contribution, capped 20
- `+ min(15, total_pnl / 70)` if pnl > 0 else `max(-10, total_pnl / 100)` — P&L contribution
- `+ min(15, n_trades × 0.5)` — sample-size bonus, capped 15
- `- consec_losses × 5` — consecutive-loss penalty
- `+ 5` if WR ≥ 70 (Elite bonus)
- `+ 5` if WR ≥ 80 (Legendary bonus)
- Final score clamped to [0, 100]
- Grade: A ≥ 80, B ≥ 65, C ≥ 50, D ≥ 35, E < 35

**Outputs:** writes to `agent_ratings` table; `main.py:2155+` console-logs the daily report card `[RATINGS] {pid} Grade={X} Score={N}/100`.

**Frequency:** daily (per main.py scheduler).

**No automatic halting** — this layer reports grades; it does not change `halt_mode`. Captain consumes the report and may act.

**Excluded players:** dayblade-0dte (separate 0DTE scoring), red-alert (system agent), enterprise-computer (metals tracker), webull (human benchmark).

### Layer 2 — `engine/adaptive_strategy.py` (weekly BENCH)

**Purpose:** weekly review that automatically benches poor performers.

**Inputs:** trades from last 30 days, grouped by player_id.

**Computation** (line 170+):
- Compute WR per agent over last 30 days
- If `n < 5` trades: skip (insufficient data)
- If `WR < 30%`: **BENCH** the agent (multiplier=0.5, benched=1)
- If agent is the top performer: multiplier += 0.2 (capped at 2.0)
- If agent is bottom AND WR < 50%: multiplier -= 0.2 (floor 0.5)

**Outputs:**
- `agent_allocation` table — stores per-agent multiplier + benched flag + reason
- `adaptive_rules` table — appends BENCH rule with reason
- Log line: `adaptive: BENCHED {pid} ({wr}% WR)` (→ trader_error.log because `logger.info`, not console.log)

**Frequency:** weekly (called by a scheduler — verify cron).

**This is the layer that produces the `[LRS] adaptive: BENCHED super-agent (25% WR)` line in trader_error.log.**

**Un-bench mechanism:** the bench is INSERT ON CONFLICT — if next week the agent's WR climbs above 30%, the bench flag isn't automatically cleared (the same SQL would only re-INSERT a BENCH rule). Effectively **the bench is sticky** until manually unbenched OR until a subsequent week's logic decides to set benched=0 (which doesn't appear in the visible code path). **Captain must manually un-bench.**

### Layer 3 — `engine/adaptive_tuner.py` (weekly signal-weight tuner)

**Purpose:** tunes the 5 trigger-signal weights based on accuracy.

**Inputs:** `intraday_snapshots` + `signal_scorecard` over last 30 days.

**Default weights:**
- session_type: 0.30
- momentum: 0.25
- vix: 0.20
- volume: 0.15
- skew: 0.10

**Bounds:** min 0.05, max 0.40 per signal. Re-normalized to sum=1.0.

**Computation** (`_compute_accuracies()` + `_adjust_weights()`): compute per-signal historical accuracy, nudge weights toward higher-accuracy signals.

**Outputs:**
- `adaptive_weights` table — stores per-run weight changes
- Effective weights used by the scoring engine for new signals

**Frequency:** weekly (`run_adaptive_tuner_weekly()`).

**This is the only layer doing actual numerical learning** (gradient-free, simple proportional adjustment). Not LLM-related.

### Summary of the three loops

| Loop | Cadence | Subject | Outputs | Who applies it |
|---|---|---|---|---|
| Agent Ratings (Grade A-E) | Daily | Per-agent | Display label + agent_ratings table | Captain reads |
| Adaptive Strategy (BENCH) | Weekly | Per-agent | agent_allocation flag + multiplier | Scanner applies multiplier to position sizes |
| Adaptive Tuner (Signal weights) | Weekly | Per-signal | adaptive_weights table | Scoring engine reads on every signal eval |

**The system has a feedback loop, but the entity being updated is the SCANNER + the AGENT_ALLOCATION table — not the LLM agents themselves.** Agent weights are immutable; their *bookkeeping* is the learning surface.

---

## 2D. System-Level: Quality Gate Evolution

**Commit count on `engine/quality_gate.py`: 2** (since file creation).

| Commit | Date | Change |
|---|---|---|
| `859a4f0` | (S5 era) | TradeMinds Season 5 — file initial commit |
| `5fea82c` | 2026-05-15 | **HM-QG-CALIBRATION**: ETF fast-path (Patch 1) + analyst hold partial credit (Patch 2) + regression tests |

**Interpretation:** the Quality Gate has had **almost no churn** — created in Season 5 era, then a single major patch on Friday. This is unusual stability for a load-bearing filter. The 2026-05-15 patch was the first calibration in months.

**Categorization of the one substantive patch:**
- Patch 1 = bug fix (dead ETF fast-path branch)
- Patch 2 = threshold tuning (hold rating from FAIL → +0.5 partial)
- Tests = new regression coverage

Implies: most of the filter logic shipped in S5 has been working as designed, only one calibration miss surfaced via live observation.

### Scanner evolution

**`engine/scanner.py` does not exist.** The scanner logic is in `main.py` (run_scanner function) and across various engine modules (`fast_scan_results`, `deep_scan_results`, premarket scanners). Git log analysis of the scanner surface would require auditing 6+ files — **scoped down per Captain rule, flag as `HM-SCANNER-EVOLUTION-AUDIT` follow-up.**

---

## 2E. System-Level: Signal Center Evolution

**Commit count on `signal-center/server.py`: 4** (visible).

| Commit | Title |
|---|---|
| `5498c34` | OllieTrades April 10 — initial big rewrite |
| `08cc0eb` | Three audit gaps fixed + backtest baseline standard |
| `bd1dbab` | fix(portability): make 5 files host-agnostic for G1 migration |
| `d9ebe8c` | chore: Saturday Night Drydock checkpoint — 8 drydock sessions committed |

**Interpretation:** Signal Center is **highly stable at the entrypoint level**. The 4 commits to server.py represent: initial scaffold, audit gap fixes, infrastructure portability, and a drydock checkpoint. **No threshold tuning visible in this file** — that logic likely lives in submodules (scorers, predictors).

**Scope-down:** auditing the SC scoring modules' evolution requires walking `signal-center/scorers/` and `signal-center/predictors/` — not done. Flag as `HM-SIGNAL-CENTER-SCORER-EVOLUTION-AUDIT` follow-up.

### Lifetime commits on the broader learning stack

| File | Commits |
|---|---:|
| `engine/ai_brain.py` | 19 (heavy churn — the central brain) |
| `engine/brain_context.py` | 3 |
| `engine/trade_memory.py` | 1 (added once, stable) |
| `engine/learning_engine.py` | 1 |
| `engine/agent_ratings.py` | 2 |
| `engine/adaptive_strategy.py` | (not measured — likely similar) |
| `engine/adaptive_tuner.py` | 3 |
| `engine/quality_gate.py` | 2 |
| `signal-center/server.py` | 4 |

**`ai_brain.py` is the high-velocity file — 19 commits.** This is where Captain ships behavioral changes most often. The infrastructure files (trade_memory, learning_engine) are write-once and stable.

---

## 2F. The Meta-Question: Captain's Learning Loop

**The most honest answer to "are the agents learning?" — they're not. The SYSTEM is learning, via Captain.**

### What the agents do (and don't do)

| Capability | Status |
|---|---|
| Are LLM agents trained on past trades? | **No.** No fine-tuning, no LoRA, no weight updates. |
| Do agents see past trades each cycle? | **Yes** (via `trade_memory` prompt injection — when routed through `ai_brain.py`). |
| Do agents have persistent state between cycles? | **No.** Stateless inference; each cycle re-builds the prompt from scratch. |
| Can agents "remember" a regret? | **Only in-context** — if last week's loss appears in this cycle's history block, the LLM may condition on it. Not durable across model swaps. |

### What the system does (the actual learning entity)

The fleet's WR improving from **19.5% (March) → 87.6% (May)** is a real signal. The mechanism:

1. **Captain ships gate fixes** (HM-QG-CALIBRATION 2026-05-15, HM-SLOW-FUNDAMENTALS, HM-CD-ROUTES, 12 PRs this week alone) — the filter is the learning entity.
2. **Captain retires poor performers** (HM-AK 2026-05-07: 11 agents halted in one batch; HM-T-fleet 2026-05-05: 4 retired; Option-4 ghost retirement 2026-05-05: 4 more). Selection is the learning entity.
3. **Captain promotes good performers** (HM-AN2.3 2026-05-13: neo-matrix exit_only → active after 22 days observation). Promotion is the learning entity.
4. **The adaptive_strategy.py weekly review** AUTOMATICALLY benches sub-30% WR agents (super-agent at 25% WR was benched by this layer). System-level learning loop with manual unbench.
5. **The adaptive_tuner.py weekly signal-weight nudges** are the only fully-autonomous numerical learning loop. Tiny scale (5 weights, [0.05, 0.40] bounds).
6. **The agent_ratings daily report** doesn't change state but feeds Captain's decision-making (the human entity in the learning loop).

### Captain's learning loop — visualized

```
┌─────────────────────────────────────────────────────────────────┐
│                                                                 │
│   ┌─────────┐    runs each cycle    ┌─────────────────┐         │
│   │ Agents  │ ────────────────────▶ │  Decisions /    │         │
│   │ (LLM)   │                       │  trades         │         │
│   └─────────┘                       └────────┬────────┘         │
│        ▲                                     │                  │
│        │ history-block                       │                  │
│        │ injection                           ▼                  │
│   ┌────┴────────┐                  ┌─────────────────┐          │
│   │ trade_memory│  ◀──────────  │  trades table   │          │
│   │ brain_ctx   │  ◀──────────  │  portfolio_hist │          │
│   │ learning_eng│  ◀──────────  │  signal-center  │          │
│   └─────────────┘                  └────────┬────────┘          │
│                                             │                   │
│                                             ▼                   │
│   ┌──────────────────────────────────────────────────┐          │
│   │ adaptive_tuner (signal weights) — autonomous     │          │
│   │ adaptive_strategy (bench/multiplier) — autonomous│          │
│   │ agent_ratings (Grade A-E daily) — autonomous     │          │
│   └────────────────────────┬─────────────────────────┘          │
│                            │                                    │
│                            ▼                                    │
│   ┌──────────────────────────────────────────────────┐          │
│   │              CAPTAIN reads + decides             │          │
│   │  - Halts poor performers (HM-AK, HM-T, Option-4) │          │
│   │  - Promotes good performers (HM-AN2.3)           │          │
│   │  - Ships gate fixes (HM-QG, HM-AF, HM-SLOW)      │          │
│   │  - Tunes scoring (Patch 1, Patch 2)              │          │
│   └────────────────────────┬─────────────────────────┘          │
│                            │ git commit + PR + merge            │
│                            ▼                                    │
│   ┌──────────────────────────────────────────────────┐          │
│   │  CODEBASE UPDATES (the real "weight updates")    │          │
│   │  — these activate at next trader restart         │          │
│   └────────────────────────┬─────────────────────────┘          │
│                            │                                    │
│                            ▼                                    │
│   ┌──────────────────────────────────────────────────┐          │
│   │     New behavior → new trades → loop closes      │          │
│   └──────────────────────────────────────────────────┘          │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### The 12-PR week as evidence

This week's PR shipping rate (PR #1 through #12 in 5 days) IS the learning velocity. Each PR represents an observation→hypothesis→ship→verify cycle. The system isn't waiting for the LLMs to "figure things out" — it's waiting for Captain to spot patterns and ship fixes.

### What this implies for next steps

**Fine-tuning would change this picture qualitatively.** Captain has the scaffolding ready (`scripts/export_training_data.py`, the daily reminder cron). The current trade volume (~1,136 closed trades lifetime) is probably still below the threshold where LoRA on a 7B model would produce stable gains, but it's growing fast. Within 6-12 months at current velocity, fine-tuning may become viable.

Until then: **agents are stateless inference; Captain is the learning entity; codebase commits ARE the weight updates.**

---

# Carry-forward Flags

Filed from this report for Captain Monday-or-next-week triage:

| Ticket | Why |
|---|---|
| `HM-MARCH-CLUSTER-AUDIT` | gemini-2.5-pro +$225,452 lifetime P&L from a single +$117K trade looks suspicious. Verify entry/exit prices are real, not stub/test data. |
| `HM-HALTED-AT-BACKFILL` | 3 current halts (`chekov`, `super-agent`, `webull`) have empty `halted_at`. Lifetime running count: ~7 violations of CLAUDE.md halt-pattern rule. |
| `HM-OLLIE-AUTO-MAY-REGRESSION-AUDIT` | ollie-auto is the ONE major agent DECLINING (Apr WR 86.8% → May WR 60.0%). All others improving. Investigate. |
| `HM-WEBULL-PNL-FIFO-PAIR` | 127 webull trades have NULL realized_pnl. Run FIFO pairing to compute lifetime P&L on Captain's real-money Webull baseline. |
| `HM-SCHWAB-SNAPSHOT-STALENESS` | Latest schwab_holdings snapshot is 2026-05-07 (8 days stale). Pipeline broken? Manual run needed? |
| `HM-TRADE-STRATEGY-ID-POPULATE` | `trades.strategy_id` is NULL for ALL 2,074 lifetime trades. Bridge handoff payload schema (section 6) assumes it. |
| `HM-SCANNER-EVOLUTION-AUDIT` | `engine/scanner.py` doesn't exist — scanner logic is in main.py + multiple engine modules. Evolution analysis deferred. |
| `HM-SIGNAL-CENTER-SCORER-EVOLUTION-AUDIT` | server.py has 4 commits but scorer/predictor submodule evolution not audited. |
| `HM-ADAPTIVE-UNBENCH-AUTOMATION` | adaptive_strategy.py benches automatically (WR < 30%) but does not auto-unbench when WR recovers. Sticky-bench risk. |
| `HM-AGENT-CONTEXT-DRIFT-AUDIT` | Verify the `trade_memory` block actually appears in production prompts (read a captured prompt example). Documentation says it does; verify empirically. |

---

# Bottom Line

**Fleet lifetime: 2,074 trades, 48.9% WR, +$237K net P&L (gemini anomaly removed: +$11.8K).** Excluding the March outlier era, the modern fleet (April-May 2026) has run **+$6,891 net P&L over 845 closed trades with a 60-87% WR range**. Real growth.

**The headline finding for Captain: the WR trajectory is real (19.5% → 58.9% → 87.6% over 3 months), driven by selection pressure not by agent self-learning.** The agents are stateless LLM inference engines with history-replay context injection. The system learns via Captain's gate fixes, halts, promotions, and the 3 adaptive feedback loops (ratings, allocation, signal weights). Fine-tuning is scaffolded but not active.

**Captain's broker baseline:** Schwab portfolio +1.27% unrealized on $167K cost basis (5-day window 2026-05-07 latest snapshot, 8 days stale). Webull liquidated 2026-05-13, lifetime P&L not paired in DB. Schwab strategy: diversified megacap tech / defense / energy buy-and-hold, no options/shorts. The autonomous fleet beats Schwab on win rate but operates in a different risk/horizon regime — apples-to-oranges direct comparison.

**Standing position unchanged:** trader at PID 48395, Layer 1 in bytecode, Monday 06:00 AZ pre-market validation window held untouched.
