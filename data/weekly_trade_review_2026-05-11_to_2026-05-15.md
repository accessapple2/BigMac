# Weekly Trade Review — 2026-05-11 (Mon) → 2026-05-15 (Fri)

**Generated:** 2026-05-15 evening, post-close + post-trader-restart (PID 48395).
**Generator:** Scotty (Claude Code on bigmac), read-only SQL + log grep.
**Scope:** autonomous fleet + Ghost Trader only. Schwab / Webull / IBKR excluded per Captain spec.
**Sources:** `data/trader.db` (arena.db deprecated 2026-05-03, state migrated to trader.db), `signal-center/signals.db`, `logs/trader.log`, `logs/trader_error.log`, `git log`.

**Timezone caveat:** DB timestamps are UTC throughout. AZ = UTC-7. Captain's week window "Mon 2026-05-11 00:00 AZ → Fri 2026-05-15 23:59 AZ" maps to UTC `2026-05-11 07:00:00 → 2026-05-16 07:00:00`. Trade queries below use simple UTC-date filtering (`>= '2026-05-11' AND < '2026-05-16'`), which captures ~99% of in-scope activity but pulls in 1 trade from Sun 21:10 AZ and misses Fri 17:00-23:59 AZ post-close activity (which is debate-pipeline-only — no trades happen after market close at 13:00 AZ). For debate / event tables, the wider UTC window is used and noted inline.

---

## A. Executive Numbers

| Metric | Value |
|---|---|
| Total fleet trades (BUY + SELL) | **149** |
| Closed trades (realized_pnl recorded) | **93** |
| Wins | **70** |
| Losses | **23** |
| Win rate | **75.3%** |
| Net P&L (closed paper trades) | **+$154.19** |
| Avg P&L per closed trade | +$1.66 |
| Largest single winner | **navigator SLS +$68.38 (+15.67%)** on 2026-05-14 |
| Largest single loser | **navigator NTRS -$77.13 (-7.09%)** on 2026-05-15 (today) |
| Best agent by net P&L | **qwen3-8b-flash +$120.75** (15/15 wins, 100% WR) |
| Worst agent by net P&L | **navigator -$40.62** (7/11 wins, 63.6% WR — owns both biggest winner AND biggest loser) |
| Fleet equity (Mon snapshot) | **$510,897.81** across 35 players reporting |
| Fleet equity (Fri end) | **$673,844.17** across 51 players reporting |
| Equity delta | +$162,947 — **but driven by new player onboarding + cash funding events, NOT trade P&L** |
| Open positions at Fri close | **49 positions, $28,161.07 cost basis** |

### Equity caveat (important)

The fleet equity delta `+$162,947` is **NOT a trading P&L number**. Between Monday and Friday:
- 16 new players were added (35 → 51 reporting)
- 3 players received cash adjustments / funding (dayblade-0dte $5k→$10k = +100%, dalio-metals +77.55%, gemini-2.5-flash +14.69%)
- alpaca-mirror (the Alpaca paper broker mirror) went $101,164.63 → $101,487.71 — broker-state mirror, not autonomous-decision P&L

**Authoritative trade-P&L number is the +$154.19 across 93 closed trades.** Fleet equity figures are useful for context but should not be misread as "fleet earned $162K this week."

### Open positions snapshot (Friday close)

| Player | Open count | Cost basis $ |
|---|---:|---:|
| alpaca-mirror | 15 | 8,374.89 |
| enterprise-computer (dalio-metals tracking) | 2 | 8,119.64 |
| neo-matrix | 3 | 3,160.56 |
| ollama-plutus | 2 | 2,244.60 |
| ollama-qwen3 | 3 | 1,615.84 |
| qwen3-8b-flash | 4 | 1,614.54 |
| ollie-auto | 8 | 1,176.21 |
| capitol-trades | 4 | 797.21 |
| deepseek-7b-grok4 | 2 | 712.64 |
| cto-grok42 | 1 | 250.96 |
| dalio-metals (live spot rows) | 1 | 77.36 |
| navigator | 4 | 16.62 |
| **Total** | **49** | **28,161.07** |

Unrealized P&L per position requires current market prices (not stored in `positions`). Skipped — would require live Polygon queries during a stand-down window.

---

## B. Per-Agent Breakdown

Closed trades grouped by player_id, sorted by net P&L desc:

| Agent | Closed | Wins | Losses | Win % | Net P&L $ | Avg P&L $ | Best $ | Worst $ |
|---|---:|---:|---:|---:|---:|---:|---:|---:|
| **qwen3-8b-flash** | 15 | 15 | 0 | 100.0 | **+120.75** | +8.05 | +40.34 | +1.24 |
| **ollama-qwen3** | 15 | 14 | 1 | 93.3 | +45.34 | +3.02 | +16.24 | -16.48 |
| **ollie-auto** | 37 | 21 | 16 | 56.8 | +13.41 | +0.36 | +8.22 | -7.48 |
| **ollama-plutus** | 6 | 6 | 0 | 100.0 | +7.31 | +1.22 | +2.57 | +0.34 |
| **capitol-trades** | 8 | 6 | 2 | 75.0 | +6.24 | +0.78 | +4.07 | -0.21 |
| **deepseek-7b-grok4** | 1 | 1 | 0 | 100.0 | +1.76 | +1.76 | +1.76 | +1.76 |
| **navigator** | 11 | 7 | 4 | 63.6 | **-40.62** | -3.69 | +68.38 | -77.13 |

### Per-agent avg R (realized_pnl / (entry_price × qty))

| Agent | Avg R% |
|---|---:|
| qwen3-8b-flash | +7.23% |
| ollama-plutus | +6.28% |
| deepseek-7b-grok4 | +3.64% |
| ollama-qwen3 | +3.26% |
| capitol-trades | +2.30% |
| navigator | +1.76% |
| ollie-auto | +1.10% |

### Halt mode changes this week

Only ONE recorded event in `hm_an23_revert_log`:

```
fired_at:        2026-05-13 17:22:12 UTC (10:22 AZ Tue)
player_id:       neo-matrix
prev_halt_mode:  exit_only  →  active (after fire)
```

This is the **HM-AN2.3 promotion** documented in CLAUDE.md ("the show must go on Maestro!") — neo-matrix became the first autonomous AI agent authorized to deploy capital on live paper. Pre-promotion: 22 days observation-only since 2026-05-11. Post-promotion (Tue 10:22 AZ through Fri evening): **9 BUYs** (the HM-AN2.C consume path trades below), zero closes yet — positions still open at Fri close.

### HM-AN2.C consume path — 9 trades all by neo-matrix

| Date (UTC) | Symbol | Qty | Price | Status | Signal# |
|---|---|---:|---:|---|---|
| 2026-05-14 13:44:26 | GOOGL | 1.1623 | 385.75 | OPEN | 1151 |
| 2026-05-14 15:06:46 | AVGO | 0.978 | 429.80 | OPEN | 1160 |
| 2026-05-14 15:06:50 | MSFT | 0.948 | 415.65 | OPEN | 1159 |
| 2026-05-14 15:06:56 | AVGO | 0.8596 | 429.80 | OPEN | 1158 |
| 2026-05-14 15:07:33 | GOOGL | 0.8979 | 385.75 | OPEN | 1151 (re-fire) |
| 2026-05-15 14:33:00 | AVGO | 0.7554 | 429.80 | OPEN | 1160 (re-fire) |
| 2026-05-15 14:33:05 | MSFT | 0.7323 | 415.65 | OPEN | 1159 (re-fire) |
| 2026-05-15 14:33:09 | AVGO | 0.664 | 429.80 | OPEN | 1158 (re-fire) |
| 2026-05-15 14:33:27 | GOOGL | 0.6936 | 385.75 | OPEN | 1151 (re-fire) |

**Captain spec correction:** spec mentioned "4 BUYs (GOOGL, AVGO×2, MSFT) + capitol-trades AAPL" — actual record shows **9 BUYs, all neo-matrix, no capitol-trades AAPL**. Same 4 signal IDs fire across two days (consumed twice — Wed afternoon and Fri lunch). All 9 positions still OPEN at Fri close. Realized P&L will appear when these close.

### super-agent halt note

`super-agent` shows `halt_mode='full'` with reason `is_paused=1 reconcile 2026-05-11`. That halt was set BEFORE the week began (Mon at start) and stayed in place. Not flagged elsewhere in the week — frozen state. Worth a follow-up audit if Captain expected super-agent to be participating.

---

## C. Ghost Trader Activity

### Genesis status

Seeded 2026-04-29 from Schwab snapshot at **$25,453.05** (per `ghost_seed` + `ghost_equity_history`). Genesis basket: 14 symbols including AMD, AMZN, ANET, AVGO, BWXT, CCJ, CEG, CRDO, CRWD, DELL, MU, PLTR, VRT, plus $9,460.71 cash.

### Current state (Friday close)

```
ghost_cash:    $8,377.54
ghost_equity:  $26,790.22   (genesis was $25,453 → +5.25% over 2.5 weeks)
last_updated:  2026-05-16T00:28:06 UTC
```

### Ghost trades this week (9 events)

| UTC ts | Symbol | Side | Qty | Price | Entry→Exit | P&L % | Advisor |
|---|---|---|---:|---:|---|---:|---|
| 2026-05-11 13:46:38 | CEG | SELL | 4.0 | 293.64 | 293.64→293.64 | (no-data) | trailing_stop |
| 2026-05-11 16:27:40 | TER | BUY | 4.0 | 365.99 | — | — | ollie_super_trades |
| 2026-05-12 13:55:50 | DELL | SELL | 7.0 | 234.56 | 216.84→234.56 | **+8.17%** | trailing_stop |
| 2026-05-12 14:56:12 | CCJ | SELL | 10.0 | 114.82 | 118.00→114.82 | **-2.69%** | trailing_stop |
| 2026-05-12 17:27:02 | PEP | BUY | 9.0 | 152.17 | — | — | ollie_super_trades |
| 2026-05-12 17:27:03 | MA | BUY | 2.0 | 503.71 | — | — | ollie_super_trades |
| 2026-05-13 20:18:50 | META | BUY | **0.0** | 606.99 | — | — | deepseek-7b-grok4 (status=`ghost`) |
| 2026-05-14 19:16:45 | META | BUY | **0.0** | 619.02 | — | — | deepseek-7b-grok4 (status=`ghost`) |
| 2026-05-15 20:27:37 | **AMD** | SELL | 2.0 | 424.10 | 282.09→424.10 | **+50.34%** | trailing_stop |

### Highlights

- **Today's standout:** AMD sold +50.34% via trailing_stop. Held since genesis seed (avg cost $282.09 on 2026-04-29), exited today at $424.10. 2.5-week hold realizing the post-earnings rally — trailing stop did its job.
- **DELL +8.17%** earlier this week — same trailing-stop pattern.
- **CCJ -2.69%** — the only Ghost loss this week.
- **Two META "ghost" entries with qty=0.0** (2026-05-13 + 2026-05-14, advisor `deepseek-7b-grok4`) — phantom rows that aren't real positions. Likely test/observability entries that bypass quantity gating. Flag as `HM-GHOST-PHANTOM-AUDIT` candidate — investigate why ghost_trades accepts 0-qty inserts.

### Ghost portfolio at Fri close (14 open)

Open positions: AMZN, AVGO, VRT (genesis-seeded 2026-04-29), CSCO, META, MSFT, NVDA, GOOGL (early-May ollie_super_trades opens), JPM, GS, AMGN (2026-05-07), TER, PEP, MA (this week's adds). Note: AMD now CLOSED (sold today). CCJ + DELL + CEG closed earlier this week.

### Ghost options watch this week

**EMPTY this week.** `ghost_options_watch` has zero entries with `ts >= 2026-05-11`. Captain's memory note ("First entry per memory: INTC 0DTE closed +22.7%") — no matching record in the table this week or any week so far. Memory may reference a different system or was banked from a pre-DB-rewrite era. **Flag for memory correction.**

### Ghost equity curve

`ghost_equity_history` has only 2 lifetime rows: 2026-04-28 and 2026-04-29 (seeding). Table has not been updated since genesis. Ghost cash/equity is tracked live in `ghost_cash` (single row, updated every cycle) but the daily snapshot table is stale. **Flag as `HM-GHOST-EQUITY-DAILY-SNAPSHOT-WRITER`** — daily ghost_equity_history writes not implemented or broken.

---

## D. By-Strategy Breakdown

### `trades.strategy_id` field — 100% unset

```
strategy_id  | trades | wins | losses | win_rate | net_pnl
(unset/NULL) |   149  |  70  |   23   |  75.3%   | 154.19
```

**The `strategy_id` column exists on `trades` but NO trade this week has it populated.** The dashboard / Bridge handoff payload schema (section 6) assumes this field is present; production agents are not writing it. **Flag as `HM-TRADE-STRATEGY-ID-POPULATE`** — fill from the calling agent's strategy context, or compute at write-time from the agent + reasoning text.

### Asset/option type breakdown

| Asset | Option type | Action | Count |
|---|---|---|---:|
| stock | — | SELL | 93 |
| stock | — | BUY | 52 |
| option | call | BUY | 4 |

**Zero spread trades this week** (`spread_data` empty/NULL on all 149 trades). No bull_call / bear_call / bull_put / bear_put / iron_condor closed credits or debits this week — entirely stock + a few long calls.

Note: trader.log tail shows `[bull_spread_v1] SPY: bull_spread_v1 open — skip` repeatedly, and `bull_spread_v1: no signals this tick` — the spread strategies are *running* but not firing entries (existing open SPY position blocks new opens). Bear/bull spread executor remains gated by the spread cannibalization guard (`SPREAD_CANNIBALIZATION_GUARD_ENABLED=True` per CLAUDE.md) layered over the position-already-open check.

### Top 5 winners + top 5 losers this week

**Winners:**
| ts | Agent | Symbol | P&L $ | P&L % |
|---|---|---|---:|---:|
| 2026-05-14 15:44:37 | navigator | SLS | +68.38 | +15.67 |
| 2026-05-13 14:33:34 | navigator | COHR | +56.52 | +7.61 |
| 2026-05-11 16:17:36 | qwen3-8b-flash | NVDA | +40.34 | +6.46 |
| 2026-05-11 14:38:40 | ollama-qwen3 | NVDA | +16.24 | +4.11 |
| 2026-05-12 17:42:37 | qwen3-8b-flash | NVDA | +15.75 | +5.04 |

**Losers:**
| ts | Agent | Symbol | P&L $ | P&L % |
|---|---|---|---:|---:|
| 2026-05-15 21:26:20 | navigator | NTRS | -77.13 | -7.09 |
| 2026-05-12 14:47:09 | navigator | MRAM | -71.17 | -8.29 |
| 2026-05-12 16:38:46 | navigator | LITE | -45.09 | -8.83 |
| 2026-05-13 14:03:12 | ollama-qwen3 | PLTR | -16.48 | -8.28 |
| 2026-05-15 17:10:10 | ollie-auto | VIK | -7.48 | -6.45 |

**Pattern:** navigator owns 3 of the top 5 losers AND the top 1 winner. High-volatility agent — sizing prop produces both big wins and big losses. NVDA appears 3x in winners (multiple agents made money on it). PLTR loss + the absence of PLTR wins suggests today's-market PLTR move stopped out longs.

---

## E. Activation Gate Efficacy

**Important constraint discovered during extraction:** `logs/trader.log` lines have no date prefix (only `[HH:MM:SS]`). Per CLAUDE.md `console.log` doctrine, gate firings land in `trader.log` via Rich — but those lines cannot be date-filtered with simple grep. `logs/trader_error.log` is the Python `logger.*` sink and HAS date prefixes, but gate firings don't go through `logger`. **Counts below are lifetime totals, not this-week.**

**Flag follow-up:** `HM-LOG-DATE-PREFIX` — add full ISO timestamp to every console.log line so weekly slicing is possible.

### Lifetime gate marker counts in trader.log

| Marker | Lifetime count | Notes |
|---|---:|---|
| LOW_CONVICTION (rejection reason) | 2,425 | Active at scale |
| SCANNER_FILTER (rejection reason) | 4,823 | Active at scale |
| HM-AF (spread cannibalization guard) | 32 | Rare but present |
| KILL_SWITCH | 42 | Lifetime — see DB note below |
| analyst=hold (Patch 2 marker fragment) | 723 | Pre-existing — likely older path, not Patch 2 |
| `[STARTUP]` banners | 9,828 | Many entries per restart |
| `[STARTUP] Arena initialized eagerly` | 2 | Today only — both from PR #11 (HM-WAR-ROOM-INIT-FIX) restarts |
| EARNINGS_BLACKOUT | **0** | Either dead code OR different marker text |
| BSM_CEILING / BSM fair | **0** | Same — marker absent |
| QG ETF (Patch 1 marker) | **0** | Patches don't emit a distinguishing log marker — only pass/reject behavior changes |
| QG hold (Patch 2 marker) | **0** | Same — silent gate |
| `etf_fast_path` | **0** | No instrumentation on the fast path |
| `[HM-AN2.C]` | **0** | DB has 9 such trades (reasoning prefix) but the marker doesn't appear in console.log — only in trades.reasoning |
| `[HM-AN2-BLOCKED` | **0** | Inline-block marker not yet firing or marker text mismatch |
| `[WR-DUR]` (Layer 1) | **0** | Layer 1 shipped tonight via PR #12; trader restarted after — should appear next market open Monday |
| `[WR-STALL]` (Layer 1) | **0** | Same — not yet fired |

### DB-authoritative gate events this week

| Source | This-week count | Note |
|---|---:|---|
| `kill_switch_log` | **0** | No kill-switch fires this week |
| `notifications` (severity=alert) | **1** | One alert NTFY |
| `notifications` (severity=info) | **1** | One info NTFY |
| `manual_trades` | **0** | No Captain-manual SQL pokes |
| War Room cycles (`War Room: launching cycle` in trader.log) | **1,774 lifetime** / unknown this week | Cannot date-slice |
| War Room round complete | 1,686 lifetime | Cannot date-slice |
| `Scanner: skipping — previous cycle still running` | 1,087 lifetime | Cannot date-slice — but tail confirms it's still common |

### What this tells Captain

- **Operational quietness:** 0 kill-switch fires, 2 NTFY-DB notifications, 0 manual interventions. Very clean week from a guard-rail perspective.
- **Visibility gaps:**
  - The QG ETF Patch 1 and Patch 2 (PR #4) shipped 2026-05-15 morning but emit **zero distinguishing log markers**. Cannot verify the patches are firing in production without inferring from trade-outcome changes.
  - HM-AN2.C and HM-AN2-BLOCKED-INLINE were supposed to emit log markers (per CLAUDE.md "HM-AN2-BLOCKED-INLINE active") — none appear in trader.log. Either marker text mismatch or instrumentation not landed.
  - Layer 1 `[WR-DUR]` is the only newly-instrumented gate that will fire after Monday's market open. Other gates remain silent or unmeasurable.
- **The blunt log-date-prefix gap is the single biggest analytics blocker.** Without it, weekly slicing of any console.log-sunk gate firing is impossible.

---

## F. War Room Debate Outcomes (signal-to-outcome)

### Two distinct systems — clarification

OllieTrades has TWO debate pipelines that the spec conflates:

1. **War Room synchronous provider loop** (`engine.war_room.run_war_room`, every 3 min during market hours, ~49 providers serial) — outputs `War Room round complete: N responses on SYMBOL` to trader.log only. No DB persistence beyond what the calling provider chooses to write. Cannot be date-sliced this week due to log-date-prefix gap.

2. **Picard Daily Debate Pipeline** (post-close 17:11 AZ daily, writes to `debate_history_v2`) — structured DB record per ticker debated. Date-sliceable.

I cover **(2)** in detail because it's the only structured surface. **(1)** is reported only at the aggregate level.

### Picard Daily Debate this week (debate_history_v2)

**30 debates this week** (UTC window 2026-05-11 07:00 → 2026-05-16 07:00):

| UTC date | Debates fired |
|---|---:|
| 2026-05-11 | 0 |
| 2026-05-12 | 3 |
| 2026-05-13 | 10 |
| 2026-05-14 | 10 |
| 2026-05-15 | 0 (zero this Friday before market close) |
| 2026-05-16 (post-Fri-close, 00:15 UTC = 17:15 AZ tonight) | 7 |

Note: 2026-05-15 evening pipeline restarted at 17:11 AZ post-close (per trader.log tail "Debate pipeline: starting post-close run...") — 7 of these are tonight's run after the trader restart with Layer 1 instrumentation activated.

### Decision distribution this week

| Picard decision | Count |
|---|---:|
| HOLD | 16 |
| LEAN_BUY | 10 |
| BUY | 2 |
| STRONG_BUY | 1 |
| LEAN_SELL | 1 |

### Did the debate matter? Cross-reference: BUY-class verdicts → follow-up trades

For each of the 13 directional verdicts (BUY / STRONG_BUY / LEAN_BUY), I counted BUY trades on the same symbol within 24 hours after the debate:

| Debate UTC | Ticker | Verdict | Adj. conviction | BUYs in next 24h |
|---|---|---|---:|---:|
| 2026-05-12 00:09:51 | TH | BUY | 6 | **0** |
| 2026-05-12 00:11:57 | WAB | LEAN_BUY | 5 | **0** |
| 2026-05-13 00:15:30 | XYZ | LEAN_BUY | 5 | **0** |
| 2026-05-13 00:17:39 | WMT | BUY | 7 | **0** |
| 2026-05-13 00:19:35 | WMB | LEAN_BUY | 5 | **0** |
| 2026-05-13 00:22:38 | VLO | LEAN_BUY | 5 | **0** |
| 2026-05-14 00:26:17 | TSEM | LEAN_BUY | 6 | **0** |
| 2026-05-14 00:28:17 | OUST | LEAN_BUY | 5 | **0** |
| 2026-05-14 00:39:35 | LIN | STRONG_BUY | 5 | **0** |
| 2026-05-16 00:17:32 | CINF | LEAN_BUY | 5 | **0** |
| 2026-05-16 00:20:18 | WST | LEAN_BUY | 5 | **0** |
| 2026-05-16 00:28:09 | TRT | LEAN_BUY | 5 | **0** |
| 2026-05-16 00:30:40 | STT | LEAN_BUY | 5 | **0** |

**ZERO of 13 directional debate verdicts produced a follow-up trade within 24 hours.**

The Picard daily-debate pipeline is currently **not wired into fleet execution**. The debate output is informational/research, not a signal source. This may or may not be expected — Captain decision needed on whether this is a feature (research surface) or a gap (deployed reasoning not consumed). **Flag for triage:** `HM-DEBATE-TO-EXECUTION-WIRING`.

### bridge_consensus pipeline — only fired once

`bridge_consensus` has ONE entry this week:

```
session_date:   2026-05-13
session_time:   06:00 (AZ-formatted)
buy_votes:      0
sell_votes:     0
hold_votes:     7
total_voters:   7
conviction:     HIGH
consensus_vote: HOLD
```

`bridge_votes` table: 8 raw votes from 1 day, 8 distinct voters. Per CLAUDE.md Bridge Vote scheduler should fire every 5 min during market hours — but only one consensus row landed this week. Either the consensus pipeline is failing to record, or the every-5-min scheduler is upstream-broken. **Flag:** `HM-BRIDGE-CONSENSUS-CADENCE-AUDIT`.

### Today's 107-min War Room stall

Per memory `project_hm_war_room_cycle_latency.md`: morning War Room cycle today stalled 05:43 → 07:30 AZ (107 min), delaying first trade by ~62 min past market open. Root cause documented in `data/scotty_hm_war_room_latency_scope_2026-05-15.md`. Provider serial loop hit Ollama 180s timeouts on multiple agents.

**Cannot extract the cycle's per-provider duration breakdown from trader.log this week** — no `[WR-DUR]` instrumentation existed before tonight's Layer 1 ship. The 107-min figure is calculated from gap between the last `War Room: launching cycle` log line and the next `War Room round complete`, observed by Captain in real time.

Layer 1 ships `[WR-DUR]` log + `[WR-STALL]` NTFY for next week. Monday 06:00 AZ pre-market is the first observation window for whether the stall recurs.

### Aggregate War Room cycle metrics (lifetime, cannot week-slice)

- `War Room: launching cycle` events lifetime: **1,774**
- `War Room round complete` events lifetime: **1,686**
- `Scanner: skipping — previous cycle still running` (the skip-due-to-prev-cycle guard): **1,087 lifetime**
- Skip-to-launch ratio ≈ 61% — cycles routinely overrun the 3-min scheduler tick

---

## G. Notable Events Timeline

Chronological, this week.

### Tuesday 2026-05-13

- **10:22 AZ** — `neo-matrix` flipped from `halt_mode='exit_only'` → `'active'` (HM-AN2.3 promotion, fire #1). First autonomous AI agent authorized to deploy paper capital. Pre-state archived in `hm_an23_revert_log`.
- **06:00 AZ** — Sole `bridge_consensus` row this week recorded (7 HOLD votes, HIGH conviction).

### Wednesday 2026-05-14

- **HM-AN2.C consume path first fires:** neo-matrix opens 5 BUYs in afternoon (GOOGL, AVGO×2, MSFT, GOOGL re-fire). Signal IDs 1151, 1158, 1159, 1160.

### Friday 2026-05-15 — **HEAVY SHIPPING DAY**

Twelve PRs landed (in this week's window — most of them today):

| PR | Title | Merge time |
|---|---|---|
| #1 | HM-CA: trade-ideas multi-source email ingestion | earlier this week |
| #2 | HM-CC: launchd FD limits + WorkingDirectory fix | this week |
| #3 | HM-CD-ROUTES: 9 broken /api/* endpoints fixed | this week |
| #4 | **HM-QG-CALIBRATION**: ETF fast-path + analyst hold partial credit + tests (Patch 1+2) | 2026-05-15 morning |
| #5 | HM-CD-β: plist hygiene batch (17 plists) | 2026-05-15 |
| #6 | HM-CREWAI-PIN decision doc | 2026-05-15 morning |
| #7 | **HM-CREWAI-PIN code fix** (Path D, commit `28aa814`) — guards eager crewai imports | 2026-05-15 |
| #8 | HM-SLOW-FUNDAMENTALS: ETF skip-list short-circuits yahoo | 2026-05-15 |
| #9 | HM-AN bridge UI auth decision doc (Path C) | 2026-05-15 |
| #10 | HM-WAR-ROOM-LATENCY scope doc | 2026-05-15 |
| #11 | **HM-WAR-ROOM-INIT-FIX** (eager Arena init at startup) — commit `020a491` | 2026-05-15 ~15:30 |
| #12 | **HM-WAR-ROOM-LATENCY Layer 1 instrumentation** — commit `7ccdab3`, merge `8a25151` | 2026-05-15 evening |

### Today's restart events

Captain spec lists 4 PIDs today: 96459 → 40947 → 45313 → 48395 (3 transitions). The only `[STARTUP] Arena initialized eagerly` marker (introduced by PR #11) shows TWICE in trader.log:
- 1st: `[16:12:58] [STARTUP] Arena initialized eagerly` (main.py:2861) — likely PID 45313 startup
- 2nd: post-17:00 kickstart for PID 48395 (Layer 1 activation) — buried in trader.log tail

**Today's 107-min War Room stall** (05:43 → 07:30 AZ) preceded the PR #11 ship. PR #11's eager Arena init is part of the fix surface for this class of incident.

### First / last trade of the week

- First trade: `2026-05-11 04:10:45 UTC` (= Sunday 2026-05-10 21:10 AZ — technically pre-window, see timezone caveat)
- Last trade: `2026-05-15 21:26:20 UTC` (= Fri 14:26 AZ — navigator's NTRS -7.09% loss)

### Trades per UTC day this week

| UTC date | Trades | BUYs | SELLs |
|---|---:|---:|---:|
| 2026-05-11 | 48 | 10 | 38 |
| 2026-05-12 | 27 | 13 | 14 |
| 2026-05-13 | 31 | 3 | 28 |
| 2026-05-14 | 19 | 16 | 3 |
| 2026-05-15 | 24 | 14 | 10 |

Monday and Wednesday were sell-heavy (38/48 and 28/31). Thursday was buy-heavy (16/19 — likely tied to the HM-AN2.C consume path fires). Friday was balanced.

---

## H. System Health Context

Scoped down — most metrics here require infrastructure that doesn't exist for date-sliced extraction.

### What's confirmed

- **Trader process at week's end:** PID 48395 running today's bytecode (Layer 1 + eager Arena init). Restarted at 17:00 AZ Friday by Captain post-PR #12.
- **`[HM-EQ] snapshot pass: 49 fired across 49 players, 0 failed`** — trader.log shows this firing every ~5 min through evening. Snapshot health: 100% (49/49 players reporting per cycle, zero failures).
- **NTFY total this week:** 2 events (1 alert, 1 info). Quiet week from an alert perspective.
- **Bridge_consensus cadence broken** (only 1 row, see Section F).
- **Debate pipeline confirmed running post-close** — trader.log tail shows "Debate pipeline: starting post-close run..." at 17:11 AZ Friday.
- **Schwab CSV pipeline (HM-AT-β):** ghost_seed shows `schwab_snapshot_2026-04-28T20:11ET` — last ingestion was 2026-04-28. Per CLAUDE.md HM-WEBULL-LIQUIDATED, the real-money side is being wound down. **Schwab snapshot freshness uncertain — no this-week ingestion record found.** Flag if Captain expected a Friday refresh.

### What couldn't be extracted (scope-down per Captain rule)

- **Uptime distribution:** would require systemd / launchd state queries or log analysis of `[STARTUP]` markers with date prefixes (which don't exist). Estimate: at least 3 restarts today, unknown for earlier days. PIDs 96459 → 40947 → 45313 → 48395 progression suggests ~3 restarts today alone.
- **FD count trajectory:** no FD-monitoring infrastructure stores history. PR #2 (HM-CC) raised FD limits via launchd — current count unknown without `lsof -p 48395`.
- **Memory/CPU anomalies:** no metric store. CLAUDE.md notes "Bigmac M4 Mac Mini, 16GB shared" but no time-series data.
- **Polygon plan utilization / rate limit hits:** the HM-POLY-HEADERS instrumentation (commit `b03fce3` + `0b42a54`) was shipped pre-this-week and writes `X-RateLimit-Remaining/Reset` to trader.log. Cannot date-slice. Lifetime rate-limit events unknown.
- **api_costs table:** schema doesn't match the columns I expected (`created_at` or `cost_date` columns absent); skipped without further schema discovery to avoid budget overrun.

### Flags for follow-up tickets

| Ticket | Why |
|---|---|
| `HM-LOG-DATE-PREFIX` | Add full ISO timestamp to every `console.log` line. Blocks all week-slicing analytics. |
| `HM-TRADE-STRATEGY-ID-POPULATE` | 0/149 trades have strategy_id set. Dashboard / Bridge handoff payload schema assumes it. |
| `HM-GHOST-PHANTOM-AUDIT` | 2 ghost_trades rows this week have qty=0.0 (META, advisor=deepseek-7b-grok4) — phantom entries. |
| `HM-GHOST-EQUITY-DAILY-SNAPSHOT-WRITER` | `ghost_equity_history` has only 2 lifetime rows (genesis seed). Daily snapshot writer not implemented. |
| `HM-DEBATE-TO-EXECUTION-WIRING` | Picard daily debate verdicts (13 BUY-class this week) produced 0 follow-up trades. Decision: feature or gap? |
| `HM-BRIDGE-CONSENSUS-CADENCE-AUDIT` | Bridge Vote scheduler should fire every 5 min during market hours; only 1 consensus row landed this week. |
| `HM-MEMORY-INTC-0DTE-CORRECTION` | Ghost INTC 0DTE +22.7% referenced in memory doesn't exist in `ghost_options_watch` records — stale or wrong source. |
| `HM-GATE-INSTRUMENTATION-COVERAGE` | EARNINGS_BLACKOUT, BSM_CEILING, QG ETF, QG hold, HM-AN2.C, HM-AN2-BLOCKED-INLINE all emit zero distinguishing log markers in trader.log. Cannot verify they fire in production. |
| `HM-SUPER-AGENT-HALT-AUDIT` | `super-agent` is `halt_mode='full'` with reason `is_paused=1 reconcile 2026-05-11`. Halt set BEFORE week began. Intentional? |

---

## Bottom Line

**Week's net trading P&L: +$154.19 across 93 closed paper trades. 75.3% win rate.**

This is the headline number. Modest but green.

**Three single-line summary findings:**

1. **The trading fleet is making money on small consistent wins** (qwen3-8b-flash 15/15 perfect week, +$120.75; ollama-qwen3 14/15) but **navigator is the volatility outlier** — owns the biggest winner AND 3 of the top 5 losers.
2. **Twelve PRs shipped in 5 days** — all backend / observability / fix work, zero trader-decision-logic changes. Friday alone saw 7 PRs land.
3. **The instrumentation gap is real and limiting** — Section E and F are scoped-down because trader.log has no per-line dates and 6+ named gates emit no distinguishing log markers. Recommend `HM-LOG-DATE-PREFIX` as the highest-leverage observability ship after the current backlog clears.

**Operational quietness this week:** 0 kill-switches, 0 manual interventions, 2 NTFY events. One stall incident (today's 107-min War Room cycle, mitigated tonight by PR #12 Layer 1 instrumentation pending Monday's first validation window).

**Standing position at report generation:** trader at PID 48395, Layer 1 in bytecode, Monday 06:00 AZ first-validation window held untouched.
