# FLEET-ROSTER.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Fleet Roster (S6.3, post-OOS-validation)

### Active 4 — Voters (live paper trading)
| Rank | Name    | Strategy / Type                              | Model                         | OOS Sharpe |
|-----:|---------|----------------------------------------------|-------------------------------|-----------:|
| 1    | McCoy   | CSP options seller — high-VIX regime         | plutus-v1 (Plutus, finance)   | +11.1      |
| 2    | Neo     | Rule-based premium/GEX pattern detector      | Deterministic (no LLM)        |  +6.1      |
| 3    | Dax     | CSP options seller — low-VIX regime          | ministral-3:3b                |  +4.9 ⚠️   |
| 4    | Capitol | Congressional STOCK Act copy-trader          | Data feed (no LLM)            |  +1.8      |

> ⚠️ **Dax (ministral-3:3b): +4.9 = CSP low-VIX BACKTEST, not live.** Live seat runs stock
> scalps — zero CSPs — realized Sharpe 0.99, +$48.41, 93.9% win over 33 closes (post-2026-05-15).
> High-win/fat-tail micro-scalper (one −$40.68 erases ~45 wins). Role-divergence flagged
> 2026-06-15; structural review deferred post-trip.

### Bench 4 — Ghost Trading (signals recorded, no real trades, scored monthly)
| Name    | Strategy / Type                                              | Model                  |
|---------|--------------------------------------------------------------|------------------------|
| Uhura   | SEC EDGAR 13F + Form 4 institutional veto                    | qwen3:8b (exit_only)   |
| Aladdin | BlackRock iShares ETF flow + BII macro signals               | Rule-based (no LLM)    |
| Spock   | Premium second opinion on McCoy's ambiguous high-VIX CSPs    | qwen3:8b (HM-CN truth-up 2026-05-17 — deepseek-r1:7b in original plan was never installed) |
| Picard  | Weekly strategic thesis → modifies Ollie's regime table      | Gemma3 4B (local)      |

**Uhura UI surfaces (HM-UHURA-VISIBILITY, 2026-06-04).** Uhura runs as TWO
orthogonal systems, now both surfaced in the dashboard:
- **v1 — institutional Form-4** (`agents/uhura_agent.py`, 05:30 AZ daily →
  `institutional_signals` in trader.db). Endpoint `GET /api/uhura/institutional?limit=N`
  (MAX-scan_date, ordered STRONG_SELL→STRONG_BUY→SELL→BUY; commit `7e7eb74`). UI:
  **"Top Insider Reads"** card in the Congress/Capitol section + an explanatory
  tooltip on the per-ticker Riker-consensus "Insider" (Uhura) "—" cells (the
  universe is mega-cap-focused, so most watchlist tickers show "—" — that's
  data-correct, not dead wiring).
- **v2 — 7-source confluence** (`engine/uhura.py`, the 86% / 4-of-7 filter).
  Endpoint `GET /api/uhura/signal` (120s cached). UI: **"Market Mood Pill"** in
  the Tactical Display header (`#uhura-mood-pill`, click-to-expand reasoning +
  signal_votes) alongside the standalone `section-uhura-signal` page. Pill logs
  one `[uhura-mood-pill] mounted` then `console.debug` "refreshed" on the 120s
  tick (idempotent mount logging, P2.1 `a00b4eb`).
- Frontend lands in `dashboard/static/index.html` (commits `690dc98` + `a00b4eb`),
  browser-smoke-PASS per Frontend Ship Rule. All four touch points read-only.
- **Parked:** literal per-ticker column-header rename "Uhura"→"Insider" deferred —
  that view is an S/D/U badge flex-list, not a labeled column; tooltip shipped instead.

### Sniper Squad — Active Scouts (signal generation, route via Ollie gate)
Active scouts firing signals into the Sniper Mode trial, in `PROTECTED_AGENTS`
(roster-locked). Sniper Mode is a proving-ground role of `ollie-auto`, not a
separate flag — see `docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md` §6.

> **Worf (`qwen3-8b-flash`) BENCHED S6.1 (−0.36%) — 2026-05-29 reconcile.** Moved
> to `ADVISORY_CREW` (bridge-vote only); last emitted 2026-05-07. Removed from
> `_SCAN_TIER2` (main.py) + `SNIPER_AGENTS` (proving_ground.py). Kept
> `ai_players` **active** — required for WR bridge-voting (`war_room.py` skips
> `halt_mode!='active'`/`is_active=0`/`is_paused=1`). The sole live LLM scout is
> now `deepseek-7b-grok4` (Spock). **Review next genuine BEAR cycle (not a bull
> cross)** — see XO_BACKLOG `review-2026-06-04`.

**Proving Ground trial formalization (HM-PROVING-GROUND-FORMALIZE-V2 2026-05-25):**
- **Duration:** 60 days (Day 60 = 2026-06-09); forced go/no-go at Day 60
- **Dedicated NTFY topic:** `ollietrades-proving-ground`
- **Exit criteria in `engine/proving_ground.py::ship_kill_evaluator`:**
  - SHIP: `go_count >= 5/6` AND `max_drawdown <= -15%` simultaneously for 10
    consecutive days
  - KILL: `max_drawdown > -15%` past Day 60 OR `go_count < 3/6` for 10 days
    OR trade-count collapse >50% over 10-day rolling
  - WARNING: `go_count 3/6 or 4/6` for 5+ days (awareness only)
- **No auto-ship / no auto-kill:** Admiral makes final call via
  `scripts/proving_ground_admiral.py --ship` / `--kill` with `--confirm`.

| Player ID            | Star Trek role | Strategy / Type                                | Model                | Recent volume |
|----------------------|----------------|------------------------------------------------|----------------------|---------------|
| `deepseek-7b-grok4`  | Spock          | Role #1: RSI-bounce scout (DETERMINISTIC — no LLM) | qwen3:8b (for #2/debate roles) | 10–15 sigs/day |
| `qwen3-8b-flash`     | Worf           | **BENCHED S6.1 (−0.36%)** — bridge-vote only (ADVISORY_CREW); review next bear cycle | qwen3:8b (local)     | 0 since 2026-05-07 |

### Backtest Pool — Deliberate OFF (cost-doctrine, KEEP wired)
5 paid LLM agents intentionally `halt_mode='full'` so they don't burn API
charges — but remain wired (`fallback_model` populated) so Admiral can A/B
test LLM lineages later or revive any single one without code changes.
**NOT zombies. Do NOT retire.**

| Player ID       | Provider | Fallback model     |
|-----------------|----------|--------------------|
| `grok-4`        | xAI      | `deepseek-r1:7b`   |
| `claude-haiku`  | Anthropic| `qwen2.5-coder:7b` |
| `claude-sonnet` | Anthropic| `qwen3:8b`         |
| `gpt-4o`        | OpenAI   | `qwen3:8b`         |
| `gpt-o3`        | OpenAI   | `deepseek-r1:7b`   |

### Gates & Coordination (non-voters)
- Ollie (`ollie-auto`) — quality gate, OllieScore ≥ 2.0 to approve. **Also the
  Sniper Mode role-holder.**
- Tractor Beam — tiebreaker only, not a full voter. (no `tractor-beam` ai_players row exists; it's a coordination role in code)
- Riker — XO synthesis/alerts, fires every 10 min. (no `riker-xo` ai_players row exists; Riker is the scheduled job `run_riker_synthesis` at `main.py:4226`)

### Retired (muted, code preserved per sacred-data rule)
- Chekov — momentum agent, threshold raised to 5.0. REHAB PATH:
  `git show 859a4f0:engine/chekov_autotrade.py` extracts S5 version;
  ghost-trade S5 vs current for 30 days.
- Navigator — convergence aggregator, archive candidate.
- Worf (Gemini Flash), Seven (Gemini Pro) — no defined edge, cost burn.
- **dayblade-sulu** — TOGGLE-OFF since 2026-03-31 (R:R 0.10 dormancy).
  `halt_mode='exit_only'`, `is_paused=1`, zero trades in last 30 days.
- Grok-4 / Troi-as-Webull-advisor — replaced 2026-04-16 by Kirk (qwen3:8b) +
  Pike (mistral:7b) on Starfleet portfolio per Free Models First.

### Zombie Candidates — preserved per sacred-data rule, listed for future audits

13 rows at `halt_mode='full'` with no cost-doctrine angle. Rows stay forever,
code preserved, no DROP:
`anderson-bcs`, `covered-call`, `mccoy-bps`, `ghost-kirk-0dte-bc`,
`ghost-kirk-bc`, `ghost-long-call`, `ghost-naked-put`, `ollama-gemma27b`,
`ollama-glm4`, `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`,
`qwen3-8b-o3`. `dayblade-0dte` is separately `halt_mode='full'` from the
2026-05-06 spread cannibalization operational halt.

### Elder Council — Long-Horizon Agents (monthly/quarterly/annual cadence)
| Name    | Horizon | Strategy / Type                                              | Model            |
|---------|---------|--------------------------------------------------------------|------------------|
| Sarek   | 5 year  | Quality compounders + dividend aristocrats; monthly rebalance | qwen3:8b         |
| Janeway | 10 year | Innovation S-curves + moat leaders; quarterly review          | phi3:mini        |
| Surak   | 20 year | Secular themes (energy, AI, demographics); annual rebalance   | gemma3:4b        |

### Metals Command (4-quadrant: Projections · News · Reports · Recommendations)
ETFs tracked: GLD, SLV, COPX, GDX, SIL, PPLT, PALL, REMX, URA. Spot: GC=F,
SI=F, HG=F, PL=F, PA=F.

| Name           | Quadrant        | Source / Model                                   |
|----------------|-----------------|--------------------------------------------------|
| Dalio          | Projections    | Macro thesis (rule-based, no LLM)                |
| Scotty         | News           | Kitco/LBMA/Reuters + FinGPT sentiment (gemma3:4b) |
| (rule-based)   | Reports        | USGS + ETF flows + 13F miner changes             |
| O'Brien        | Recommendations| Synthesizes quadrants → buy/hold/trim (model TBD via HM-BN.2 bakeoff) |

### Utility / Out-of-Scope
- Data (`ollama-coder`, qwen2.5-coder:7b) — strategy review / code tasks
- Sulu, Dayblade-0dte — day-trading / 0dte (separate track)
- Swing Desk (Kirk, Pike) — RETIRED 2026-05-04, archived to
  `archive/retired/2026-05-04-kirk-swing-desk/`. The active "Kirk"
  (`engine/kirk_advisory.py` + `engine/kirk_grok_advisor.py`) is unrelated
  and remains live.

### Fleet count truth (live DB)
**21 active**, 6 `exit_only`, 45 `full` (as of 2026-06-01; DB-verified). `alpaca-mirror`,
`mlx-qwen3`, `red-alert` season-1 carryovers are now `halt_mode='full'` (no
longer active).
