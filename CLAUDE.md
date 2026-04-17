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

### Swing Desk (3–10 day holds, Starfleet portfolio advisory)
| Name    | Role                                                         | Model       |
|---------|--------------------------------------------------------------|-------------|
| Kirk    | Holly Swing Advisor — primary swing calls, ghost-traded first | qwen3:8b    |
| Pike    | Swing backup / second-opinion veto on Kirk's ambiguous setups | mistral:7b  |

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
- Build Swing Desk agents (Kirk = qwen3:8b, Pike backup = mistral:7b) — ghost-trade swing setups for 30 days before promoting
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

## Archive Convention
- Retired agents: keep code in `engine/` (muted via threshold), DO NOT delete
- If file must be moved, use `agents/_archive/` with date suffix
- Document retirement reason + rehab path in this file, not just the commit message
- This supports the "iterate to the next Top 4" feedback loop — no known-good code is lost

## Workflow
- Propose edits and ask for approval before applying
- For multi-file changes, show the plan first, then apply incrementally
