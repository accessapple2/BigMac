# OllieTrades — Ground Rules for Claude Code

## Project Context
OllieTrades is an autonomous AI paper trading system running on bigmac (Mac Mini M4, 16GB RAM). Research project — not manual trading. Multi-agent fleet trading via Alpaca paper account.

## Broker Accounts (real-world, as of 2026-04-17)
- **Alpaca (paper)** — the ONLY account the fleet trades against. All agents (McCoy/Dax/Neo options, Capitol equities, etc.) fire signals here. This is the research surface and stays the research surface.
- **Schwab (real cash, options-enabled)** — opened 2026-04-17 to eventually replace Webull for real-money options. **OUT OF THE FLEET LOOP.** No agent, scanner, or bridge may route signals to Schwab. Stays dormant until an agent demonstrates ≥3 months of live-Alpaca OOS Sharpe matching or exceeding its backtest baseline, at which point the Admiral manually reviews a promotion proposal.
- **Webull** — being wound down; no new OllieTrades wiring. Dashboard's `section-webull` internal id stays (to avoid the 50+ ref rename) but label migrates to "Starfleet" per existing TODO.
- **Promotion gate (paper → real Schwab):** explicit Admiral approval per agent, documented in this file alongside the live-performance numbers that justify it.

## SACRED DATA RULES (non-negotiable)
- NEVER delete, drop, or truncate `trader.db`, `arena.db`, or `tractor.db`
- NEVER run `rm -rf` on `~/ollietrades` or `~/autonomous-trader`
- Always archive or rename instead of deleting
- Ask before any destructive filesystem operation

## Manual halt SQL pattern

When halting a player via direct SQL (no programmatic halt path exists today),
always include `halted_at` and update **both** `halt_mode` and `is_halted`:

<!-- HM-S-docs 2026-05-04: SQL pattern updated to drop is_halted column (removed by HM-B); drawdown-halt section rewritten after HM-S investigation found agent_state table never existed -->

```sql
UPDATE ai_players
   SET halt_mode  = 'exit_only',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '[YYYY-MM-DD] [reason]'
 WHERE id = 'X';
```

**Single source of truth: `halt_mode`.** HM-A migrated all production read paths
from `is_halted` to `halt_mode != 'active'`, and HM-B (2026-05-04, commit `9256890`)
dropped the `is_halted` column from `ai_players` entirely. Valid `halt_mode` values:
`active`, `exit_only`, `full` (CHECK constraint enforced).

**Drawdown-halt mechanism (do NOT confuse with manual halt above):**
The 20% drawdown auto-halt lives in `engine/risk_manager.py::check_drawdown()`. It
reads `portfolio_history` and computes `(peak - current) / peak >= max_drawdown_pct`
(default 0.20) every cycle, called from `engine/ai_brain.py:817`. The halt is
**transient** — recomputed each cycle, no flag table. To "unhalt" a drawdown-halted
agent, the only natural path is recovery to a new peak; manual injection of a
higher peak row in `portfolio_history` would also work but is not a designed
escape hatch.

There is **no `agent_state` table** in any DB. A vestigial reference in
`agents/post_earnings_drift.py::is_halted()` queries the phantom table but is
wrapped in `try/except → False`, so it's functionally inert. PED's actual
protection is `self.gated = True` (paper-only). HM-S investigation 2026-05-04
documented this; cleanup queued as HM-S-code (low priority — see XO_BACKLOG).

**Why `halted_at` is mandatory:** there is no schema default and no trigger.
Forgetting it leaves the timestamp NULL, which audit #6A flagged when the four
April halts had their dates buried in `halt_reason` text. HM-F (2026-05-04)
investigated whether to enforce this via a helper or trigger; finding was that
no programmatic halt paths exist, so the runbook is the right place.

To unhalt: same UPDATE pattern, `halt_mode='active'`, leave `halted_at` and
`halt_reason` as historical record (do not clear).

## Dashboard Rules
- Dashboard is served from `dashboard/static/index.html` on port 8080
- ALL dashboard edits target that single file — do not create new HTML files unless explicitly asked
- `main.py` is the entry point; it imports `from dashboard.app import app` and runs uvicorn on 8080

## RAM Discipline (16GB shared across Ollama, Docker, Tractor Beam, OllieTrades)
- Prefer `qwen3.5:9b` over larger models
- `qwen3:30b` is rejected — too slow for this box
- Avoid loading full datasets into memory; stream or chunk
- `0xroyce/plutus` is the finance-trained model used for Jim Simons' quant role

## Free Models First (cost doctrine, set 2026-04-16)
- All agents default to FREE models — local Ollama or no-CC-required cloud free tiers
- Paid models are FORBIDDEN unless the Admiral approves the spend, per agent
- Approved paid exceptions: **(none actively running; Polygon.io Options Starter $29/mo is approved-in-principle but not activated as of 2026-04-16 — see Pending TODOs)**
- When proposing a model swap, show: model name, RAM cost, why it's orthogonal to existing fleet, and any free-tier rate limits
- Rule of thumb: if two agents would run the same family (e.g. two LLaMA-derivatives), pick a different lineage (Qwen, DeepSeek-R1, Phi-4, Gemma) for real orthogonality

## Git & Deployment
- Pause before `git push` — Steve runs those manually (VPN must be off)
- Commit messages should reference the season (currently S6) and agent name when relevant

## Backtest Rule
- Always run ALL agents in backtests, never a subset
- Never cite in-sample (IS) numbers without the matching OOS figure

## Fleet Roster (S6.3, post-OOS-validation)

### Active 4 — Voters (live paper trading)
| Rank | Name    | Strategy / Type                              | Model                         | OOS Sharpe |
|-----:|---------|----------------------------------------------|-------------------------------|-----------:|
| 1    | McCoy   | CSP options seller — high-VIX regime         | 0xroyce/plutus (Plutus-3B)    | +11.1      |
| 2    | Neo     | Rule-based premium/GEX pattern detector      | Deterministic (no LLM)        |  +6.1      |
| 3    | Dax     | CSP options seller — low-VIX regime          | qwen3:8b                      |  +4.9      |
| 4    | Capitol | Congressional STOCK Act copy-trader          | Data feed (no LLM)            |  +1.8      |

### Bench 4 — Ghost Trading (signals recorded, no real trades, scored monthly)
| Name    | Strategy / Type                                              | Model                  |
|---------|--------------------------------------------------------------|------------------------|
| Uhura   | SEC EDGAR 13F + Form 4 institutional veto                    | llama3.1               |
| Aladdin | BlackRock iShares ETF flow + BII macro signals               | Rule-based (no LLM)    |
| Spock   | Premium second opinion on McCoy's ambiguous high-VIX CSPs    | deepseek-r1:7b (local) |
| Picard  | Weekly strategic thesis → modifies Ollie's regime table      | Gemma3 4B (local)      |

### Gates & Coordination (non-voters)
- Ollie (`ollie-auto`) — quality gate, OllieScore ≥ 2.0 to approve
- Tractor Beam (`tractor-beam`) — tiebreaker only, not a full voter
- Riker (`riker-xo`) — XO synthesis/alerts, fires every 10 min

### Retired (muted, code preserved per sacred-data rule)
- Chekov — momentum agent, threshold raised to 5.0 per spec. REHAB PATH: `git show 859a4f0:engine/chekov_autotrade.py` extracts S5 version; ghost-trade S5 vs current for 30 days, promote the better one.
- Navigator — convergence aggregator, archive candidate once Chekov decision lands
- Worf (Gemini Flash), Seven (Gemini Pro) — no defined edge, cost burn. Archive.

### Elder Council — Long-Horizon Agents (monthly/quarterly/annual cadence)
Patient investors. Not voters on the short-term Active 4 signals. Scored on 6-month rolling basis, not daily.

| Name    | Horizon | Strategy / Type                                              | Model            |
|---------|---------|--------------------------------------------------------------|------------------|
| Sarek   | 5 year  | Quality compounders + dividend aristocrats; monthly rebalance | qwen3:8b         |
| Janeway | 10 year | Innovation S-curves + moat leaders; quarterly review          | phi3:mini        |
| Surak   | 20 year | Secular themes (energy, AI, demographics); annual rebalance   | gemma3:4b        |

### Swing Desk — RETIRED 2026-05-04
Scaffolded but never wired. `agents/kirk.py` + `agents/pike.py` archived to `archive/retired/2026-05-04-kirk-swing-desk/`. Per audit #6A Problem B (`docs/AUDIT_6_INVESTIGATION_2026-05-04.md`): manual swing-trading workflow no longer applies since the fleet shifted to autonomous Alpaca-paper-only. The active "Kirk" (`engine/kirk_advisory.py` + `engine/kirk_grok_advisor.py`, daily Webull-style advisor writing to `kirk_advisory_log`) is unrelated and remains live.


### Metals Command (4-quadrant: Projections · News · Reports · Recommendations)
Physical holdings tracked as header widget above the quadrant grid. ETFs tracked: GLD, SLV, COPX, GDX, SIL, PPLT, PALL, REMX, URA. Spot: GC=F, SI=F, HG=F, PL=F, PA=F.

| Name           | Quadrant        | Source / Model                                   |
|----------------|-----------------|--------------------------------------------------|
| Dalio (existing)| Projections    | Macro thesis (rule-based, no LLM)                |
| Scotty          | News           | Kitco/LBMA/Reuters + FinGPT sentiment (gemma3:4b) |
| (rule-based)    | Reports        | USGS + ETF flows + 13F miner changes             |
| O'Brien         | Recommendations| Synthesizes quadrants → buy/hold/trim (deepseek-r1:7b, shared with Spock) |

### Gates & Coordination (non-voters)
- Ollie (`ollie-auto`) — quality gate, OllieScore ≥ 2.0 to approve
- Tractor Beam (`tractor-beam`) — tiebreaker only, not a full voter
- Riker (`riker-xo`) — XO synthesis/alerts, fires every 10 min

### Retired (muted, code preserved per sacred-data rule)
- Chekov — momentum agent, threshold raised to 5.0 per spec. REHAB PATH: `git show 859a4f0:engine/chekov_autotrade.py` extracts S5 version; ghost-trade S5 vs current for 30 days, promote the better one.
- Navigator — convergence aggregator, archive candidate once Chekov decision lands
- Worf (Gemini Flash), Seven (Gemini Pro) — no defined edge, cost burn. Archive.
- Grok-4 / Troi-as-Webull-advisor — replaced 2026-04-16 by Kirk (qwen3:8b) + Pike (mistral:7b) on Starfleet portfolio per Free Models First.

### Utility (not traders)
- Data (`ollama-coder`, qwen2.5-coder:7b) — strategy review / code tasks

### Out of Scope (separate tracks)
- Sulu, Dayblade-0dte — day-trading / 0dte strategies (separate track)

## Duplicate Role Policy
- **Healthy duplication** (keep): McCoy+Dax both run CSP but on different VIX regimes. Capitol+Aladdin+Uhura-EDGAR all "smart money" but orthogonal data sources (retail Congress / institutional ETF / 13F). Verify McCoy-Dax trade overlap stays <60% quarterly.
- **Bad duplication** (consolidate): Momentum cluster (Neo/Chekov/Navigator) — Neo owns it now. Cloud-LLM cluster (Spock/Worf/Seven) — consolidated to Spock only, then Spock moved local on 2026-04-16 (deepseek-r1:7b) per Free Models First.

## Season 6.3 Config (current)
- Tractor Beam = tiebreaker (not full voter)
- Gate thresholds: neo-matrix 1.75, chekov 5.0 (muted per spec), sniper alpha 0.25
- Target signal conversion: 3–5%
- IS 180-day baseline: 100% WR, Sharpe 4.845 (OVERFIT — see OOS)
- **OOS 2024 clean baseline:** Sharpe **2.692**, WR 65.8%, 456 trades, all strategies beat SPY (+17.5%)
- CSP dominates: OOS Sharpe +6.05 across BULL and CAUTIOUS regimes

## Pending TODOs
- **Polygon.io Options Starter ($29/mo)** — APPROVED IN PRINCIPLE (2026-04-16), not yet activated. When activated: powers Neo (real-time GEX/chain) + McCoy/Dax (precise greeks for CSP entries). First paid exception under Free Models First doctrine.
- Build Elder Council agents (Sarek 5yr, Janeway 10yr, Surak 20yr) — stub strategy modules + DCA paper-trade logic
- Build Metals Command quadrant agents (Scotty news, O'Brien recommendations); upgrade `section-metals` to 4-quadrant grid with spot + ETF tracking
- Rename dashboard `section-webull` label → "Starfleet" (keep internal id to avoid 50+ ref breakage)
- Ghost-trading experiments for Bench 4:
  - Uhura-EDGAR: 60-day ghost run, promote to Active if Sharpe > Capitol's
  - Aladdin: wire iShares ETF flow → paper-trade sector rotation signals
  - Spock-R1: `ollama pull deepseek-r1:7b`; fire only on McCoy's ambiguous high-VIX CSPs; 60-day A/B vs McCoy-alone. R1's reasoning traces are orthogonal to Plutus's finance-corpus priors
  - Picard: convert weekly briefing from advisory-only into Ollie regime-table modifier
- Chekov rehab: extract S5 version, ghost-trade S5 vs current for 30 days
- Candidate C (2022 bear) OOS backtest — running now
- Research TradingAgents / FinMem integration (FinGPT sentiment blended in S6.3 — see commit 7ebabb6)

## Recently Shipped (S6.3, no longer pending)
- Lt. Uhura SEC EDGAR agent (commits ad9d832, 4c78d04) — institutional veto wired into trade gateway
- Covered_call P&L bug fix (commit 14689a7) — denominator corrected to position notional
- CAUTIOUS rsi_bounce disabled (commit e799d07)
- Plutus-3B upgrade for McCoy (commit 3721c33)
- FinGPT news sentiment blended into alpha signals (commit 7ebabb6)

## 2026-04-23 Drydock Part 2 — Major Overhaul

24 fixes applied in one session. Gate held `_EXECUTION_ENABLED=False` throughout.
Alpaca paper ~$79,716 equity. No live exposure.

### Highlights
- **5 rogue qwen3:14b callers routed to Ollie Box**: neo-matrix (config.py),
  engine/premarket_scanner.py, engine/research_caller.py, crew/agents.py
  (5 CrewAI roles → ChatOllama with OLLIE_URL), engine/chart_analyzer.py
- **dashboard/app.py 3-path Gemini fallback → Ollie Box** (lines 4969/5025/5348):
  google-provider players now fall back to OLLIE_URL, not OLLAMA_URL (bigmac)
- **5 Elder Council + Swing Desk agents** (surak, janeway, kirk, pike, sarek):
  `_OLLAMA_URL` fallback changed `localhost:11434` → `192.168.1.166:11434` (Ollie)
- **2 zombie players deactivated** via `is_active=0` in ai_players table:
  `ollama-llama` (deepseek-r1:14b, 9.7GB — was loading on bigmac) and
  `grok-3` (qwen3:14b — retired per S6.3, not in AI_PLAYERS). Sacred trade/
  signal/cost data (145 trades, 5025 signals, 2924 cost rows) fully preserved.
- **Spock dedup TOCTOU race fixed**: `engine/crew_scanner.py::_save_spock_alert`
  rewritten as atomic `INSERT ... WHERE NOT EXISTS`. Eliminates hourly duplicate
  risk alerts when scanner plist and main.py fire simultaneously.
- **Webull sync disabled** (account liquidated), webull positions zeroed
- **Costs page `[object Object]` fixed**: `dashboard/static/index.html:10976`
  now reads `fvp.free.total_pnl` / `fvp.paid.total_pnl` — renders correctly

### RAM Result
94% peak → 45% mid-session → 1.8GB active healthy after all fixes landed.
Root cause: multiple sprint-era agent paths all defaulted to localhost:11434.

### Architecture Note
Each sprint added agent paths that defaulted to localhost:11434. Tonight was
surgical mop-up. Permanent fix is a single config-level OLLAMA_URL default
(pointing at Ollie) applied at process start — future sprint.

### Open Items (next session)
- `/api/wheel/status` intermittent 500 (`dashboard/app.py:7592`)
- iv_history day 3 verification at 9:45 MST tomorrow
- Chrome extension final cleanup (Profile 5 re-install check)
- Alert ACK hygiene — 5 Neo consecutive-loss alerts still unacknowledged in DB
- Ghost scorecard calibration via `/api/signals/scorecard` (`server.py:2104`)
  before Tuesday gate-flip
- Alpha threshold tuning decision for bull_spread_v1 first trade

## Archive Convention
- Retired agents: keep code in `engine/` (muted via threshold), DO NOT delete
- If file must be moved, use `agents/_archive/` with date suffix
- Document retirement reason + rehab path in this file, not just the commit message
- This supports the "iterate to the next Top 4" feedback loop — no known-good code is lost

## 2026-04-25 — Saturday Drydock — 14 fixes

Gate held `_EXECUTION_ENABLED=False` throughout. Alpaca paper account untouched.

### Category 1 — Kill switch architecture (3 fixes)
- **is_halted gate in `buy()`** (`paper_trader.py:~542`): SQL check before every buy; returns None if halted
- **is_halted gate in `sell()`** (`paper_trader.py:~1083`): same pattern
- **Zombies halted in DB**: `ollama-llama` + `grok-3` → `is_halted=1`, `halt_reason` set
- **Audit finding**: `is_active`, `is_paused`, and `crew_role` are all decorative in the execution path.
  `is_halted` is now the **only working per-player kill switch**. Document this before any new agent is wired.

### Category 2 — Autopilot $0 P&L bleed (2 fixes)
- **Layer 1** (`autopilot.py:126`): skip RSI trim when `current_price=0`; was falling back to `avg_price` and creating phantom $0-P&L exits
- **Layer 2** (`main.py:~1095`): widen `prices` dict from `WATCH_STOCKS` only → all symbols in open positions; off-watchlist holdings (CBRL, FDX, FMAO etc.) now get real prices before autopilot runs

### Category 3 — Latent fallback bombs defused (2 fixes)
- **`_try_db_fallback` option filter** (`market_data.py:~487`): added `AND asset_type='stock'` to both SQL queries; was returning option premiums ($42.48 MU call) as stock prices
- **BUY_CALL fail-closed** (`paper_trader.py:~1364`): `buy_price` init changed from `price` (stock price!) to `None`; guard added — if no option data returns, skip trade instead of buying at stock price

### Category 4 — Operational cleanup (7 fixes)
- **Capitol Trades dedup** (`crew_scanner.py:~2569`): `_process_rules_player()` now checks existing position and `action LIKE 'BUY%'` DB guard before submitting; root cause of FMAO triple-entry (15:21/15:28/15:31 UTC Apr 24 — crew_scanner has no `_done_today`, fires every 2 min)
- **NTFY topic renames** (4 files): `"Ollie-Alert-35"` → `"ollietrades-admin"` (`riker_synthesis.py`, `fleet_auditor.py`, `alert_channels.py`); `"Ollie-Alert-55"` → `"ollietrades-crew"` (`ntfy.py`)
- **`"Riker Fleet Alert"` → `"Fleet Alert"`** (`riker_synthesis.py:197`)
- **ntfy tags `"trademinds"` → `"ollietrades"`** (6 occurrences in `alert_channels.py`)
- **BSM ceiling** (`options_selector.py`): `_bsm_call`/`_bsm_put` via `math.erf` (no scipy); rejects if market premium > 1.5× BSM fair value. MU 500C 27DTE: fair $12.45, Spock paid $42.48 (3.4×) — would block
- **Earnings blackout** (`options_selector.py`): replaces dead logs-only block; blocks if earnings within 3d of today OR ±5d of expiry; fast-path through `data/earnings_cache.json` (1ms), yfinance fallback, fail-open on errors

### RAM Result
Bigmac Ollama clean throughout — nothing loaded at EOD. All heavy models on Ollie Box.

### Open Items (carry forward)
- `/api/wheel/status` intermittent 500 (`dashboard/app.py:7592`) — low priority
- 5 Neo consecutive-loss alerts — already `acknowledged=1` in DB (pre-cleared)
- iv_history Day 4: Mon Apr 28 @ 9:45 MST — verify 10/10 recorded
- iv_history Day 5: Tue Apr 29 @ 9:45 MST — verify 10/10 recorded; gate-flip review after
- Ghost scorecard calibration via `/api/signals/scorecard` before Tuesday gate-flip

## Archive Convention
- Retired agents: keep code in `engine/` (muted via threshold), DO NOT delete
- If file must be moved, use `agents/_archive/` with date suffix
- Document retirement reason + rehab path in this file, not just the commit message
- This supports the "iterate to the next Top 4" feedback loop — no known-good code is lost

## 2026-05-03 — Fleet Reality Reconciliation

Read-only audit of every fleet member against running code, DB state, scheduler entries, and signal volumes. No agents halted or modified. Source: `/tmp/scotty_session_2026-05-03/fleet_reality_2026-05-03.md`.

### State Flag Semantics (audit extension to 2026-04-25 finding)
- `is_halted` is a **trade-execution gate ONLY** (`paper_trader.py:547, 1091`). **It does NOT gate signal emission.** Halted players still emit signals (compute waste): verified ollama-llama emitted 947 signals between its halt date 2026-04-25 and 2026-05-02. To fully retire a player, also remove its scheduler entry OR add an `is_halted` check in the signal-emission path.
- `is_active`, `is_paused`, `crew_role` confirmed still decorative.

### DayBlade reality (override "Out of Scope" note above)
- `dayblade-0dte` (T'Pol/plutus): `is_halted=0`, `is_active=1` — scheduler ACTIVE (`main.py:2554` every 5 min). **No signals since 2026-04-07** (26 days idle as of audit). Empirical pause, not formal halt. Has explicit weekend-standby gate — Option A restart logs show `[STARTUP] DayBlade: standby (weekend)` on Sun May 3 20:07 MST.
- `dayblade-sulu`: `is_halted=1` since 2026-03-31 (R:R 0.10 dormancy).
- Monday market-hours verification protocol queued at `/tmp/scotty_session_2026-05-03/dayblade_monday_exit_verification.md`.

### Battle Station — dormant via missing inputs
- `main.py` schedulers ACTIVE: `run_battle_station_monitor` every 2 min (`main.py:2573`), `run_morning_briefing` daily 06:00 (`main.py:2564`), `run_opening_range` daily 06:45 (`main.py:2572`).
- launchd feeders that supplied data are **absent** (`launchctl list | grep battle` → 0 entries; no `com.trademinds.battle*` plists). Feeders never re-added after Phase 2 cleanup.
- Net effect: code-level battle-station calls execute, downstream-data they expect is empty.

### signal_scorecard — silent writer (16-col schema, 0 rows)
- Schema present in `trader.db`. Empty since creation.
- Writer never wired (April 7 Alpha Engine plan unfinished). No active code contains `INSERT INTO signal_scorecard`.
- Blocks ghost scorecard calibration (B5 in carry-forward TODOs) and gate-flip review.

### mlx-qwen3 — fully active
- `is_active=1`, `is_halted=0`. 965 signals over last 30d, MAX 2026-05-02. Confirmed in fleet, not zombie.
- Note: `ai_players.model_id` for mlx-qwen3 row shows `phi3:mini`, but `config.py:128` defines provider=mlx with model `mlx-community/Qwen3-8B-4bit`. Drift is non-blocking (runtime reads config, not DB) — cosmetic cleanup queued.

### energy-arnold — high-volume noise generator
- Model: `qwen3:8b` via Ollama. `is_active=1, is_halted=0`. **9,632 total signals**.
- Confidence distribution is bimodal: AVG 0.258, **6,643 at conf=0.0 (69%), 1,209 at conf=1.0 (13%)** — mostly noise + occasional over-confident outputs.
- bridge_voter wiring: 216 votes total in `bridge_votes` table, MAX `created_at` = 2026-05-01 13:01:23 — wired but stopped collecting 2 days ago. Investigation needed.
- IMPROVE decision pending parser investigation (per fleet reality doc) — not retired this round.

## Pending TODOs (additions from 2026-05-03 reconciliation)
- Backtest 8 orphaned options strategies in `engine/options_agents.py` (zero `main.py` refs) — wire/retire decision blocks Sunday Deep Dive Phase 4.
- Wire `signal_scorecard` writer OR remove from Alpha Engine plan — currently blocks B5.
- Investigate why `bridge_votes` collection stalled 2026-05-01 13:01.
- Add signal-emission gate (in addition to execution gate) for fully-retired players: ollama-llama, grok-3, possibly dayblade-0dte.
- Reconcile DB `ai_players.model_id` ↔ `config.py` model drift (~25 rows; cosmetic; one migration script).
- Retire legacy convergence scanner (`engine/strategies.py` scan_strategies path). Convergence-write path silently dead since 2026-04-07 — see `/tmp/scotty_session_2026-05-03/legacy_scanner_triage.md`. Coordinate with retiring 8 readers in `/tmp/scotty_session_2026-05-03/oq3_strategy_signals_readers.md`.

## Workflow
- Propose edits and ask for approval before applying
- For multi-file changes, show the plan first, then apply incrementally
