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
- Dashboard is served from `dashboard/static/index.html` on port 8080.
  Verified empirically 2026-05-08 (`docs/DASHBOARD_DOCTRINE_2026-05-08.md`):
  FastAPI `/` route at `dashboard/app.py:9464-9467` returns
  `FileResponse(_static_dir + "/index.html")` and the only `StaticFiles`
  mount is `dashboard/static/`. The Vite tree at `dashboard/frontend/`
  is unwired experimental code — its `dist/` is never mounted.
- ALL dashboard edits target that single file — do not create new HTML files unless explicitly asked
- `main.py` is the entry point; it imports `from dashboard.app import app` and runs uvicorn on 8080

## Network Bindings
- **Trader dashboard (port 8080)**: bound network-wide; reachable on LAN + via Cloudflare tunnel `bridge.ollietrades.com`.
- **Signal Center (port 9000)**: currently bound to `127.0.0.1` from pre-2FA legacy posture. HM-AW (`docs/XO_BACKLOG.md`) tracks reopening to network now that 2FA TOTP + multi-user auth (Captain, Bonnie observer, Dad charts) are in place. SSH tunnel required today for non-bigmac browser access.
- **Two distinct auth layers** (do not conflate): browser users → 2FA TOTP + RBAC at the Signal Center server layer; automation/scripts → SSH keys + bigmac OS account. Both are valid; they protect different surfaces.

## RAM Discipline (16GB shared across Ollama, Docker, Tractor Beam, OllieTrades)
- Prefer `qwen3.5:9b` over larger models
- `qwen3:30b` is rejected — too slow for this box
- Avoid loading full datasets into memory; stream or chunk
- `0xroyce/plutus` is the finance-trained model used for Jim Simons' quant role

## Free Models First (cost doctrine, set 2026-04-16)
- All agents default to FREE models — local Ollama or no-CC-required cloud free tiers
- Paid models are FORBIDDEN unless the Admiral approves the spend, per agent
- Approved paid exceptions: **Polygon Stocks Starter + Options Starter ($29 + $29/mo) ACTIVE as of 2026-05-12.** Primary data source for candles (HM-CB) + options (HM-CA chain). Alpaca + yfinance are fallbacks.
- When proposing a model swap, show: model name, RAM cost, why it's orthogonal to existing fleet, and any free-tier rate limits
- Rule of thumb: if two agents would run the same family (e.g. two LLaMA-derivatives), pick a different lineage (Qwen, DeepSeek-R1, Phi-4, Gemma) for real orthogonality

## Git & Deployment
- Scotty handles `git push` + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` + verify inline. No Captain handoff (workflow updated 2026-05-11).
- Commit messages should reference the season (currently S6) and agent name when relevant

## Frontend Ship Rule (added 2026-05-12, HM-BJ.E4 lesson)
Non-trivial frontend JS changes require a **manual browser hover/click smoke test** before declaring shipped. `node --check` and `py_compile` catch syntax errors but NOT runtime IIFE/closure throws or DOM-binding regressions. Re-attempts after a revert MUST include browser-test as a required closure phase, not optional.

## Daemon Lifecycle Rule (added 2026-05-12, HM-EQ lesson)
Background daemons must bind to **process lifecycle** (module-level startup + explicit invocation in `main.py`), NEVER lazy-instantiated module state coupled to a scan-cycle or agent-spawn path. Standalone import-tests can pass while live production never fires — verify the live execution path with a log heartbeat before declaring shipped. HM-EQ daemon went 128h silent because the Arena-coupled spawn never fired; commit `54881bb` moved it to module-level.

## HM-AM Scope (added 2026-05-12, HM-CLOSE-GAP W1.1)
Total Portfolio = **real-world net worth only** (Schwab + Webull + IBKR + physical metals). EXCLUDES Alpaca paper trading book — that's a separate research/strategy-validation surface and must not co-mingle with real-world capital reporting. The two-book bridge policy (see below) governs how the books communicate without mixing.

## Backtest Rule
- Always run ALL agents in backtests, never a subset
- Never cite in-sample (IS) numbers without the matching OOS figure

<!-- HM-I-β: two-book bridge policy formalized 2026-05-05 per Admiral decision -->
## Architecture: Two-Book Bridge Policy (Option β, established 2026-05-05)

**OllieTrades operates two separate books, by design.** Source: HM-I investigation (`docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`); Admiral decision 2026-05-05.

### The two books
1. **Internal AI fleet book** — `positions` table for all `player_id != 'webull'`. Research / calibration. The legacy fleet (ollama-plutus, qwen3-8b-flash, deepseek-7b-grok4, ollama-qwen3, energy-arnold, capitol-trades, gemini-2.5-flash, etc.) writes here. **Never forwards to Alpaca.**
2. **Alpaca paper book** — Alpaca's live broker state, mirrored locally as the `webull` player's positions. Real-on-broker activity for routed players + spread strategies.

### What routes to Alpaca
- The **routed players** in `engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER`:
  - `super-agent` → Alpaca Paper (portfolio id=1)
  - `ollie-auto` → Alpaca Paper (portfolio id=1)
  - `neo-matrix` → Neo Matrix (portfolio id=7)
  - `dalio-metals` → Enterprise Computer (portfolio id=5, physical-metals tracker, `route_mode=tracking`, log-only)
- The **spread strategies** (post-gate-flip 2026-05-04, `_EXECUTION_ENABLED=True`):
  - `bull_call_spread_v1`, `bear_put_spread_v1`, `executor` — these route via `engine/alpaca_options.py::execute_options_signal`, a **third forward path** that bypasses the player-keyed routing table entirely.

### What stays internal
Every other player. The 9+ active legacy-fleet agents emit signals and trades into the `positions` table only. Their entries never reach Alpaca paper.

### How forwarding gates work
`engine/paper_trader.py::_forward_to_alpaca` (line 216) is gated on `route["route_mode"] == "trading"` at all three call sites:
- BUY (line 1015) — gated
- full-SELL (line 1167) — gated
- partial-SELL (line 1300) — gated as of 2026-05-05 commit `d06c33c` (HM-I Option ε)

Players whose mapped portfolio resolves to `route_mode=paper` (default) or `route_mode=tracking` (Enterprise Computer) never forward to the broker.

### Why two books, not one
- Spread strategies and routed players need real broker state for honest execution paths.
- Legacy fleet is research / calibration (per CLAUDE.md OOS validation: McCoy +11.1 Sharpe, etc.) — separating their book from the broker preserves test isolation.
- Shorts and futures (GC=F, SI=F) live in the internal book naturally; Alpaca paper can't accept futures.
- Legacy fleet halt/retirement decisions don't pollute the broker state.

### Naming discipline
- "Arena Paper" = the default unmapped routing destination (no DB row; `route_mode=paper`). Most legacy-fleet agents land here.
- "Alpaca Paper" = `portfolios.id=1`, the actual broker connection.
- These are **different things despite similar names**. Future dashboard work will make this visually distinct (HM-I-β followup, deferred).

### Open followups (HM-I-β, deferred to future sessions)
- **Item 2:** Dashboard naming pass — visually distinguish "Arena Paper" panels from "Alpaca Paper" panels.
- **Item 3:** Split `webull` player's dual role (human Webull benchmark + Alpaca mirror) into two distinct player_ids (`webull` + `alpaca-mirror`).
- **Item 5:** Daily reconciliation report — replaces the ε canary, surfaces internal-vs-Alpaca drift via NTFY when thresholds exceeded.

<!-- HM-U: -->
## Error Handling Posture (established 2026-05-05)

**OllieTrades exception-handling posture, established after HM-Z (commit 306dcf6) and HM-AA (commit a9d0649) surfaced two silent-failure cases in 12 hours.** Source: HM-U discussion (commit c12db55); Admiral decision 2026-05-05.

### Background — what HM-Z and HM-AA exposed

**HM-Z BTO bug** ran latent for weeks, then surfaced ~24 hours after gate-flip and produced 4 ghost rows in `options_trades` before discovery. Root cause: `engine/alpaca_options.py` used `PositionIntent.BTO` instead of `PositionIntent.BUY_TO_OPEN`. The `AttributeError("BTO")` was caught by a bare `except Exception`, logged as `"submit_spread error: BTO"` (single-token, deceptively API-looking), and the caller treated the failed submit as a successful position open.

**HM-AA empty-body errors** at `alpaca_options.py:254` produced log lines like `"submit_single error: "` — empty after the colon. Some exception with no `.args` raised. Caught by bare `except`. Logged as nothing. Discovery required Admiral diagnostic.

Both share three properties:
1. Bare `except Exception as e` catching code-bugs and ops-errors uniformly
2. `f"...error: {e}"` logging assuming `str(e)` is informative — it often isn't
3. No NTFY alerting; failures piled up silently in the log file

### Posture

**1. Bare `except Exception` is acceptable when the handling correctly accommodates unknown failures.**

Bare `except Exception` is *not* the bug. The BTO bug was a bug because the handling (return error dict; caller misread it) was wrong, not because the bare except existed. Some places legitimately want broad catch — per-agent cycles where one agent's crash shouldn't take down the fleet, for example.

When you write `except Exception as e:`, ask: "if `e` is a programming error (AttributeError, ImportError, NameError) instead of an operational error (APIError, ConnectionError), does my handler do the right thing?" If the handler treats those identically and that's wrong, narrow the except.

**2. Error logs capture type + repr, not just str.**

```python
# Avoid (information density too low — was the BTO and empty-body shape):
except Exception as e:
    console.log(f"foo error: {e}")

# Prefer:
except Exception as e:
    console.log(f"foo error: {type(e).__name__}: {e!r}")
```

`{type(e).__name__}` surfaces the exception class. `{e!r}` is `repr(e)` which usually includes the str representation but with class context preserved. Together they make every error log self-explanatory without requiring a debug session.

**3. NTFY on first occurrence per error class per day for architecture-class paths.**

Architecture-class paths require NTFY:
- Every broker-submit code path (`submit_*` functions in `alpaca_options.py`, future similar in webull/IBKR adapters)
- `halt_mode` writes (anything that transitions an `ai_players` row's halt state)
- Position-of-record writes (anything that mutates `positions`, `options_trades`, or sync destinations)

Threshold: first occurrence of an error class within a 24h window NTFYs. Subsequent occurrences of the same class within the window suppress (avoid alert fatigue). New class within the window = new NTFY. Window resets at midnight local.

Non-architecture paths (legacy fleet signal cycles, Polygon timeouts, Ollama timeouts, weather-style transient noise) do NOT NTFY. They log only.

**4. Going-forward, not retroactive.**

The codebase has hundreds of `except Exception as e:` blocks. A retroactive audit is not in scope. **The posture applies to:**
- Every new code change that touches exception handling
- Every code path that produces an error during normal operation that we discover and need to investigate

Old code stays as-is until something naturally touches it. The posture propagates through the codebase as natural maintenance occurs.

### Cross-references

- HM-Z fix: commit 306dcf6 (BTO/STO → BUY_TO_OPEN/SELL_TO_OPEN, six sites in alpaca_options.py)
- HM-AA narrow-strict: commit a9d0649 (line 254 enrichment for submit_single)
- HM-AA-extension (this commit): line 297 enrichment for submit_spread, applying posture principle 2 to the original BTO incident site
- 6 parallel error-log sites in alpaca_options.py (lines 49/105/157/216/334/377/383) deferred to a future HM-AA-broad session if pattern proves itself useful

## Ghost Tracking Architecture (two systems, established HM-BC 2026-05-11)

**OllieTrades has TWO ghost-tracking systems with orthogonal concerns.** They share a SQL table name (`ghost_trades`) but live in different DBs with different schemas. Source: HM-BC investigation; Admiral decision 2026-05-11.

### The two ghost systems

| | `engine/ghost_scoring.py` | `engine/ghost_trades.py` |
|---|---|---|
| **Purpose** | Signal-center agent win-rate scorecard | OllieTrades fleet decision log + missed-opportunity stats |
| **Storage DB** | `data/ghost_trades.db` (own DB) | `data/trader.db` (shared, sacred) |
| **Reads from** | `signal-center/signals.db::trade_signals` (BUY signals, conf ≥ 70) | n/a — writer-only on this side |
| **Written by** | Own pipeline: `capture_new_signals()` + `check_outcomes()` | `ai_brain.py::log_ghost_trade` (HOLD>0.6) + `ghost_advisor.py` (BUY/SELL) |
| **Outcome resolver** | Live Alpaca bars + signal-center `signal_outcomes` | Computed at SELL time vs `ghost_portfolio.avg_cost` |
| **Agent universe** | signal-center agents (`etf_regime_trader`, `danelfin_ai`, `chekov`, `navigator`, …) | OllieTrades fleet (`ollie_super_trades`, `trailing_stop`, `kirk_advisory_log`, …) |
| **Dashboard endpoints** | `/api/ghost/scorecard`, `/api/ghost/trades`, `/api/ghost/refresh` | `/api/ghost-trades`, `/api/ghost-trades/stats` |
| **UI panel** | `section-ghost-scorecard` (index.html L9594) | (consumed by older `/api/ghost-trades*` callers) |

### Why two systems

They were not designed as a pair — they were added at different points to solve different problems and happened to land on the same name. The signal-center scoring pipeline (`ghost_scoring`) predates the trader.db decision-log (`ghost_trades`); both are kept because both render real, distinct UI value.

### Naming discipline

- `ghost_scoring.py` (renamed from `ghost_trader.py` in HM-BC.2) is the **signal-center pipeline**.
- `ghost_trades.py` (the HM-AZ/HM-BB module) is the **trader.db decision log**.
- "Ghost trades" the user-facing concept can mean either depending on which dashboard panel is in view — the API path is the disambiguator. `/api/ghost/*` = scoring. `/api/ghost-trades*` = decision log.

### Do not consolidate

The two modules export non-overlapping functions, read different DBs, and serve different agent universes. A previous attempt to treat one as stale (the HM-BB closure note misread the singular file as obsolete) would have silently broken the live `section-ghost-scorecard` panel. **Future cleanup attempts must verify both UI panels still render before touching either module.**

### Cross-references

- HM-AZ ghost-trades rewrite: commit 8f65a87 (trader.db query rewrite)
- HM-BB ghost-trades schema enrichment: commits 8fc1aeb / 3d9f5ee (entry_price/confidence/exit_price/pnl_pct on trader.db.ghost_trades)
- HM-BC.1: `data/ghost_trades.db` restored from `.legacy_lean_2026-05-11` (784 rows)
- HM-BC.2: commit 5fa8a57 (`ghost_trader.py` → `ghost_scoring.py` rename)
- HM-BC.3: `com.ollietrades.ghost-trader.plist` archived to `~/Library/LaunchAgents/_archive/`

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

### Sniper Squad — Active Scouts (signal generation, route via Ollie gate)
**Added 2026-05-08 to align with `dashboard/app.py:1431+1439` toggle reality** (Phase 3 toggle infra map). These are the active scouts firing signals into the Sniper Mode trial; both are in `PROTECTED_AGENTS` (roster-locked) and emit signals daily. Sniper Mode is a 30-day proving-ground role of `ollie-auto`, not a separate sub-mode flag — see `docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md` §6.

| Player ID            | Star Trek role | Strategy / Type                                | Model                | Recent volume |
|----------------------|----------------|------------------------------------------------|----------------------|---------------|
| `deepseek-7b-grok4`  | Spock          | RSI-bounce scout for Sniper Mode trial         | deepseek-r1:7b (local)| ~178 sigs/day |
| `qwen3-8b-flash`     | Worf           | Sniper Mode scout (was Gemini Flash, repointed Apr 16) | qwen3:8b (local)| ~25 sigs/day |

### Backtest Pool — Deliberate OFF (cost-doctrine, KEEP wired)
**Added 2026-05-08 to align with toggle reality.** These 5 paid LLM agents are intentionally `halt_mode='full'` so they do NOT burn API charges — but they remain wired (`fallback_model` populated) so the Admiral can A/B test LLM lineages later or revive any single one without code changes. **They are NOT zombies. Do NOT retire or remove from `ai_players`.**

| Player ID       | Provider | Fallback model     | Why off                            |
|-----------------|----------|--------------------|------------------------------------|
| `grok-4`        | xAI      | `deepseek-r1:7b`   | xAI charges; deepseek covers slot  |
| `claude-haiku`  | Anthropic| `qwen2.5-coder:7b` | API cost                           |
| `claude-sonnet` | Anthropic| `qwen3:8b`         | API cost                           |
| `gpt-4o`        | OpenAI   | `qwen3:8b`         | API cost                           |
| `gpt-o3`        | OpenAI   | `deepseek-r1:7b`   | API cost                           |

### Gates & Coordination (non-voters)
- Ollie (`ollie-auto`) — quality gate, OllieScore ≥ 2.0 to approve. **Also the Sniper Mode role-holder** — Sniper Mode IS the trial wrapper around ollie-auto's gate (NOT a separate flag, NOT a sub-mode toggle). Saturday 2026-05-09 KILL is `halt_mode='full'` on this row.
- Tractor Beam (`tractor-beam`) — tiebreaker only, not a full voter
- Riker (`riker-xo`) — XO synthesis/alerts, fires every 10 min

### Retired (muted, code preserved per sacred-data rule)
- Chekov — momentum agent, threshold raised to 5.0 per spec. REHAB PATH: `git show 859a4f0:engine/chekov_autotrade.py` extracts S5 version; ghost-trade S5 vs current for 30 days, promote the better one.
- Navigator — convergence aggregator, archive candidate once Chekov decision lands
- Worf (Gemini Flash), Seven (Gemini Pro) — no defined edge, cost burn. Archive.
- **dayblade-sulu** — TOGGLE-OFF (deliberate) since 2026-03-31 (R:R 0.10 dormancy). Confirmed by Phase 3 toggle infra audit: `halted_at='2026-03-31'`, `halt_mode='exit_only'`, `is_paused=1`, **zero trades in last 30 days** — halt holding cleanly. Not a zombie, not a halt-gap. Code preserved.

### Zombie Candidates — Future Cleanup (NOT scheduled, no DROP)
**Added 2026-05-08 from Phase 3 toggle infra map §4.** These 14 rows are at `halt_mode='full'` with retirement reasons that have no cost-doctrine angle (i.e. they are NOT in the Backtest Pool above). Per sacred-data rule: rows stay forever, code preserved, no DROP. Listed here so future audits don't re-discover them.

| Player ID              | Reason class                                  |
|------------------------|-----------------------------------------------|
| `anderson-bcs`         | HM-T-fleet bundle 2026-05-05 (Option 1 halt-only)|
| `covered-call`         | HM-T-fleet bundle 2026-05-05                  |
| `mccoy-bps`            | HM-T-fleet bundle 2026-05-05                  |
| `quark-ic`             | HM-T-fleet bundle 2026-05-05                  |
| `ghost-kirk-0dte-bc`   | Option-4 ghost bundle 2026-05-05              |
| `ghost-kirk-bc`        | Option-4 ghost bundle 2026-05-05              |
| `ghost-long-call`      | Option-4 ghost bundle 2026-05-05              |
| `ghost-naked-put`      | Option-4 ghost bundle 2026-05-05              |
| `ollama-gemma27b`      | HM-AK 2026-05-07 dormant cleanup (no activity 7d) |
| `ollama-glm4`          | HM-AK 2026-05-07 dormant cleanup              |
| `qwen-coder-haiku`     | HM-AK 2026-05-07 dormant cleanup              |
| `qwen3-14b-grok3`      | HM-AK 2026-05-07 dormant cleanup              |
| `qwen3-8b-4o`          | HM-AK 2026-05-07 dormant cleanup              |
| `qwen3-8b-o3`          | HM-AK 2026-05-07 dormant cleanup              |

`dayblade-0dte` is `halt_mode='full'` from the 2026-05-06 spread cannibalization operational halt — separate decision per HM-AF flag-lift review, not in the zombie set.

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
- ~~**Polygon.io Options Starter ($29/mo)** — APPROVED IN PRINCIPLE~~ — **ACTIVATED 2026-05-12.** Polygon Stocks Starter (HM-CB) + Options Starter (HM-CA) both live. Primary candles + options-chain source. First paid exception under Free Models First doctrine, executed.
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
- bridge_voter wiring: 248 votes total in `bridge_votes` table, MAX `created_at` = 2026-05-08 13:08:41 — collection resumed since 2026-05-03 audit. Bimodal-confidence parser investigation remains open (see `data/scotty_proposals/energy_arnold_fix_proposal.md`).
- IMPROVE decision pending parser investigation (per fleet reality doc) — not retired this round.

## Pending TODOs (additions from 2026-05-03 reconciliation)
- Backtest 8 orphaned options strategies in `engine/options_agents.py` (zero `main.py` refs) — wire/retire decision blocks Sunday Deep Dive Phase 4.
- Wire `signal_scorecard` writer OR remove from Alpha Engine plan — currently blocks B5 (signal_scorecard still 0 rows as of 2026-05-10).
- Add signal-emission gate (in addition to execution gate) for fully-retired players: ollama-llama, grok-3, possibly dayblade-0dte.
- Reconcile DB `ai_players.model_id` ↔ `config.py` model drift (~25 rows; cosmetic; one migration script).
- Retire legacy convergence scanner (`engine/strategies.py` scan_strategies path). Convergence-write path silently dead since 2026-04-07 — see `/tmp/scotty_session_2026-05-03/legacy_scanner_triage.md`. Coordinate with retiring 8 readers in `/tmp/scotty_session_2026-05-03/oq3_strategy_signals_readers.md`.

<!-- Day-2-lessons: 2026-05-05 -->
## Lessons (2026-05-05 Day 2)

### Logging sink split — `trader.log` vs `trader_error.log`

OllieTrades logs to two files with different sinks:

| File | Sink | What goes here |
|---|---|---|
| `logs/trader.log` | Rich `console.log(...)` calls | Per-cycle agent output, strategy ticks, market data, formatted user-facing log lines |
| `logs/trader_error.log` | Python `logger.info / .warning / .error` calls | Structured Python logging — **including `engine.alert_channels` NTFY dispatch logs** |

**Implication for investigations:** when checking whether a NTFY actually fired, search `trader_error.log` for entries like:
- `[LRS] Alert dispatched [warning/{alert_type}]: {message}`
- `[LRS] ntfy sent [200]: ?? TradeMinds {Level}`

Searching only `trader.log` will miss NTFY firings entirely — the system POSTs at HTTP 200 and produces them correctly; they just land in the other file.

This was learned the hard way during the 2026-05-05 reconciliation drift investigation (commit `ec81add`) when the investigation initially classified Thread D ("HM-V NTFY didn't fire") as a real bug before discovering 5 NTFY events were happily landing in `trader_error.log` at HTTP 200.

### Alert rate-limit semantics — in-memory only

`engine.alert_channels._rate_state` is an in-memory dict scoped to the running process. It is **NOT** persisted to the `settings` table.

**Consequence:** the `rate_limit_secs=86400` parameter that HM-U callers pass means "first occurrence per error class **per process lifetime**", not "first occurrence per error class per 24h wall-clock." Service restarts reset the dedup state.

On heavy-restart days, each unique `alert_type` can fire up to N times where N = restart count. 2026-05-05 (Day 2 of gate-flip soak) had 4 restarts, so HM-U's submit-error alert classes fired up to 4× instead of the intended 1×.

For daily-cadence canary alerts (e.g., Item 5 reconciliation drift detection — fires once per day at 13:30), this is fine: the canary fires once and the in-memory state holds within that window.

For HM-U error-class alerts on heavy-restart days, the policy is diluted but not broken. Each restart produces at most one fresh fire per alert_type. Acceptable today; not yet load-bearing.

**Future option (not yet shipped):** persist `_rate_state` to the `settings` table to make rate-limiting survive restarts. Documented here so the actual semantics are clear.

## Feature Flags

Feature flags live in `config.py` as module-level constants. Engine modules import lazily inside the gated function (`from config import FLAG_NAME`). Flipping a flag requires a service restart: `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

### Active flags

| Flag | Default | Purpose | Reversal |
|------|---------|---------|----------|
| `SPREAD_CANNIBALIZATION_GUARD_ENABLED` | `True` | HM-AF-α 2026-05-06 (emergency halt). Halts P1 (`engine/battle_station.py::monitor_active_options`), P2 (`engine/alpaca_options.py::close_all_options`), and P3 (`engine/dayblade.py` post-trade close, belt-and-braces). HM-AF-β 2026-05-06 added inner-layer `engine/options_utils.is_spread_leg(symbol)` filter at all three sites; HM-AF-γ 2026-05-06 added wrong-side-of-book correction (sign-aware close in `_auto_close` via `qty_signed`). Once HM-AF-β has soaked **24h in production**, the flag CAN be set to `False` to re-enable legitimate options closes — leg-aware skip provides the protection. **Lifting requires a separate Phase 4 decision; do not auto-lift.** | Set `False` in `config.py` + restart |

## Workflow
- Propose edits and ask for approval before applying
- For multi-file changes, show the plan first, then apply incrementally


## Lessons Banked 2026-05-13

**logger.info vs console.log**: Rich Console writes to `logs/trader.log`; stdlib `logger.info()` writes to `logs/trader_error.log`. The two sinks are SEPARATE despite "trader_error" misleading-name. Any new HM-* instrumentation must use `console.log()` for trader.log routing, or scripts grepping for the data must explicitly target trader_error.log. Surfaced when HM-CD-instr + HM-AN2 lines were invisible (cron scripts grepped wrong file). Fixed by HM-LOG-CHANNEL commit 8d7a607.

**HM-CD-migrate doctrine — Ollama keep_alive**: Universal `keep_alive: "30s"` (legacy 16GB constraint) forces full model reload on every call — ollama-coder measured 207s wall, 90%+ in model swap. Fixed by per-model `_HM_CD_KEEP_ALIVE` lookup: high-frequency models (qwen3:8b=7 agents, qwen2.5-coder:7b=2 agents) get 30m residency; alpha-squad rotation gets 10m; rare models keep 30s default. Originally banked as "Ollie Box 32GB has headroom for 2-3 concurrent 7B-class models" — CORRECTED 2026-05-13: actual hardware is **RTX 5060 with 8GB VRAM** (surfaced via nvidia-smi during HM-CD-MIGRATE-GPU-RECOVERY). qwen2.5-coder:7b alone uses 4.59GB resident; realistic budget is ONE 7B model fully resident at a time, possibly a second with partial offload. Doctrine logic (per-model keep_alive lookup) remains correct; the VRAM-headroom premise was wrong. Commit 999984a (logic).

**Diagnostics first**: HM-CD-migrate ALMOST became a Polygon data migration based on stale assumptions. Real cause (model swap) emerged only when ollama-coder logs were read in context. HM-CD-instr instrumentation (logger.info → trader_error.log) was the savior — without it, blind Polygon migration would have shipped 0 perf improvement. Reinforces XO rule: diagnostics first, theorize second.

## Lessons Banked 2026-05-13 (afternoon)

**HM-WATCHDOG sweep complete**: All HM-* anchored instrumentation across the codebase now uses `console.log()` consistently. Files normalized: engine/crew_scanner.py (7 sites, HM-LOG-CHANNEL commit 8d7a607), main.py (1 site, HM-WATCHDOG commit 5d6ce29), engine/battle_station.py (3 HM-AF sites, HM-WATCHDOG-2). When adding new HM-* observability lines, default to `console.log()` not `logger.info()`.

**HM-AN2 BLOCKED enhancement**: BLOCKED log line now pulls `paper_trader._last_rejection.get(player_id)` inline so morning_an2_observation cron output is self-contained — no need to cross-reference paper_trader log lines.

**HM-BP-FOLLOW-UP parked**: 5+ gemini-2.5-pro trades from 2026-03-12 have entry_price corruption (entry ~$10, exit ~$210 for mega-caps). Reject filter handles compute; data cleanup deferred pending root-cause investigation. See data/scotty_hm_bp_followup_*.md.

**Diagnostics-first reinforcement**: Today's morning ship arc proved this discipline twice in production. Both HM-CD-migrate and HM-BP would have shipped wrong fixes without the discovery phase. RULE: never modify production code paths before reading current behavior via grep + sqlite + log inspection.

## Lessons Banked 2026-05-13 (evening — HM-CONSOLE-INIT)

**`logger.* → console.log` flips need module-level `Console()` init verified before commit.** This morning's automated sweep (HM-LOG-CHANNEL `8d7a607`, HM-WATCHDOG `5d6ce29`, HM-WATCHDOG-2 `10eb1e7`) flipped `logger.info/warning` calls to `console.log` in `crew_scanner.py` and `battle_station.py` without verifying these modules imported `Rich Console`. They didn't. Every flipped call raised `NameError: name 'console' is not defined` at runtime.

**The bug ran silently for ~12 hours** because `run_scan_cycle` wraps the body in `try/except` that logs `"Scan cycle crashed (will retry next cycle): name 'console' is not defined"` and moves on. Cycles "succeeded" from the scheduler's view but never finished emitting their HM-CD-instr lines. Combined with the same-day Ollie Box GPU outage, the fleet was running 2× degraded all day.

**Discovery path:** the bug only surfaced when the trader was restarted (commit `6bfa53f` deployed; restart fired). Fresh process loaded clean bytecode and immediately threw the NameError again on the first cycle. Without that restart it would have stayed invisible — the existing PID had been throwing the error every cycle but the masked errors didn't NTFY.

**Doctrine going forward:**

1. **`logger.*` → `console.log` flips are not safe defaults.** Before flipping, verify the target module has `from rich.console import Console` and `console = Console()` at module scope. Ship `ruff`/`pyflakes` as part of the sweep, OR runtime-smoke each touched module with `python -c "import engine.X; engine.X.console.log('test')"` before commit.

2. **`py_compile` is not enough.** It catches syntax errors but NOT undefined-name errors. Runtime smoke required for any cross-module symbol change. (Same lesson as Frontend Ship Rule 2026-05-12, applied to backend.)

3. **`try/except Exception` swallowing programming errors is the silent-failure pattern.** Per HM-Z/HM-AA doctrine in CLAUDE.md (2026-05-05): bare except is OK if the handler accommodates unknown failures. For `run_scan_cycle`, the handler logs "will retry next cycle" — that's fine for ops errors (timeout, API failure), but a `NameError` is a code bug that should at least NTFY. Consider distinguishing programming-error subclasses (`AttributeError`, `NameError`, `ImportError`) from operational errors and NTFY-ing the first class.

4. **Restart-then-verify is mandatory after any logger/console flip.** The "trader restart deferred until natural maintenance window" pattern (used today for HM-LOG-CHANNEL, HM-WATCHDOG, HM-WATCHDOG-2) creates a window where the buggy bytecode runs invisibly. Smoke-restart in a verify window, OR push the restart synchronously with the commit.

**Fix shipped:** commit `ef1c02c` added the canonical Console init block (`from rich.console import Console` + `console = Console()`) to both files. Trader restarted on the fix. HM-CD-instr cycle walls collapsed from 220-408s (CPU + crashes) to 80s (GPU + clean), per-agent walls from 60-207s to 2-3s.

## Account State Banked 2026-05-13

**HM-WEBULL-LIQUIDATED**: webull real-money account has been liquidated. Historical
trades remain in `trades` table as record. Account state in `ai_players` set to
halt_mode='full' with descriptive halt_reason. Positions table zeroed out for webull
(no open positions remain). Any future query/script that iterates real-money accounts
must exclude webull or check halt_mode='active'. Previous memory note "Webull ~$6.6K
= monitor only" is superseded by this liquidation.

## Banked 2026-05-13 (late afternoon)

**HM-STALE-TRIM-OBS-V2 query fix**: V1 used trades-table arithmetic (BUY without matching exact-qty SELL = stale) which produced false-positives on partial scale-outs. V2 anchors on positions table (source of truth) joined to trades for first-BUY context. Also filters to halt_mode='active' players only, excluding halted zombies. Lesson: **positions table is canonical for "is position open"; trades arithmetic is not equivalent.**

**ollama-local halted**: Stale signal emitter pattern. halt_mode was 'active' but no trades in 53 days; signals continued but all gate-rejected. Set to exit_only pending audit of which gate (mandate/confidence/sizing) is the rejection source. File HM-FLEET-REJECTION-AUDIT for next session if other emitters show same pattern.



## HM-AN2.3 FIRED 2026-05-13 evening (Captain go: "the show must go on Maestro!")

**neo-matrix flipped to halt_mode='active'.** First autonomous AI agent on
the fleet authorized to deploy capital on live (paper) trades.

**Posture:**
- Auto-traded only (paper money on Alpaca)
- 5 trades/day cap (MAX_TRADES_PER_DAY)
- ALLOCATION_POLICY_EXEMPT (no fixed sizing constraint)
- WARNING_ONLY_PLAYERS (some risk warnings don't block)
- HM-AN2-BLOCKED-INLINE active (gate reasons logged inline)
- Starting equity: ~$7,173.69 cash, zero positions, 100% cash
- Trader uptime at fire: 41:52+, running clean main.py (post-decorator-move)

**Pre-state archived** in hm_an23_revert_log table with fired_at timestamp.

**Revert command:**
```sql
UPDATE ai_players SET halt_mode='exit_only',
                      halt_reason='Reverted HM-AN2.3 ' || datetime('now','localtime')
WHERE id='neo-matrix';
```

**First fire window:** next regular scan cycle that produces an HM-AN2 candidate.
Market closed at fire-time; first real test is tomorrow's open (06:30 AZ).

**22 days of observation-only preceded this:** halt_mode='exit_only' since
2026-05-11, ~60 HM-AN2 candidates observed safely-blocked, all gate-reason
patterns documented via HM-AN2-BLOCKED-INLINE.

**Caveat banked:** if battle_station_monitor cadence drift (913s vs 120s
target, observed at 16:52) is symptomatic of broader scheduler issues,
neo-matrix's HM-AN2.C consume path could be affected. Monitor.


## HM-BK Phase 1 LOADED 2026-05-13 evening

**Movers poller plist loaded.** com.ollietrades.movers-poller running on
5-minute cadence, self-gates to market hours (09:30–16:00 ET Mon-Fri).

**Off-hours behavior:** invocations exit cheaply (no Polygon calls, no
DB writes). First real fire window: tomorrow's open 06:30 AZ.

**Phase 2 deferred (Captain-attended):**
- signal_center wiring as tier='mover'
- Dashboard /movers route (memory rule #27 — needs browser smoke)
- mcap + optionable enrichment via /v3/reference/tickers (nightly cadence)

**To unload (single command):**
```bash
launchctl unload ~/Library/LaunchAgents/com.ollietrades.movers-poller.plist
```
