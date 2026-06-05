# OllieTrades — Ground Rules for Claude Code

> **Historical narratives, sprint logs, and shipped/superseded sections** moved
> to `docs/CLAUDE-archive-2026-05.md` on 2026-05-27 per HM-CLAUDE-MD-TRIM. This
> file contains active doctrine only.

## Project Context
OllieTrades is an autonomous AI paper trading system running on bigmac (Mac
Mini M4, 16GB RAM). Research project — not manual trading. Multi-agent fleet
trading via Alpaca paper account.

## Broker Accounts (real-world, as of 2026-04-17)
- **Alpaca (paper)** — the ONLY account the fleet trades against. All agents
  (McCoy/Dax/Neo options, Capitol equities, etc.) fire signals here. This is
  the research surface and stays the research surface.
- **Schwab (real cash, options-enabled)** — opened 2026-04-17 to eventually
  replace Webull for real-money options. **OUT OF THE FLEET LOOP.** No agent,
  scanner, or bridge may route signals to Schwab. Stays dormant until an agent
  demonstrates ≥3 months of live-Alpaca OOS Sharpe matching or exceeding its
  backtest baseline; Admiral manually reviews promotion proposal.
- **Webull** — being wound down; no new OllieTrades wiring. Dashboard's
  `section-webull` internal id stays (to avoid the 50+ ref rename) but label
  migrates to "Starfleet" per existing TODO.
- **Promotion gate (paper → real Schwab):** explicit Admiral approval per agent,
  documented in this file alongside the live-performance numbers that justify it.

## SACRED DATA RULES (non-negotiable)
- NEVER delete, drop, or truncate `trader.db`, `arena.db`, or `tractor.db`
- NEVER run `rm -rf` on `~/ollietrades` or `~/autonomous-trader`
- Always archive or rename instead of deleting
- Ask before any destructive filesystem operation

## Manual halt SQL pattern

When halting a player via direct SQL (no programmatic halt path exists today),
always include `halted_at` and set `halt_mode`:

```sql
UPDATE ai_players
   SET halt_mode  = 'exit_only',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '[YYYY-MM-DD] [reason]'
 WHERE id = 'X';
```

**Single source of truth: `halt_mode`.** HM-A migrated all production read
paths from `is_halted` to `halt_mode != 'active'`, and HM-B (2026-05-04, commit
`9256890`) dropped the `is_halted` column from `ai_players` entirely. Valid
`halt_mode` values: `active`, `exit_only`, `full` (CHECK constraint enforced).

**Drawdown-halt mechanism (do NOT confuse with manual halt above):**
The 20% drawdown auto-halt lives in `engine/risk_manager.py::check_drawdown()`.
Reads `portfolio_history`, computes `(peak - current) / peak >= max_drawdown_pct`
(default 0.20) every cycle, called from `engine/ai_brain.py:962`. The halt is
**transient** — recomputed each cycle, no flag table. To "unhalt" a
drawdown-halted agent, the natural path is recovery to a new peak.

There is **no `agent_state` table** in any DB. A vestigial reference in
`archive/retired/2026-05-04-post-earnings-drift/post_earnings_drift.py::is_halted()`
(PED retired 2026-05-04) queries the phantom table but is wrapped in
`try/except → False`, so it's functionally inert. PED's actual protection is
`self.gated = True` (paper-only).

**Why `halted_at` is mandatory:** no schema default, no trigger before
2026-05-19. Forgetting it left NULL timestamps that audit #6A flagged. As of
HM-HALTED-AT-BACKFILL+ENFORCE (PR #15, 2026-05-19), INSERT/UPDATE triggers
auto-fill `halted_at` for `halt_mode != 'active'` rows, so the manual UPDATE
above is belt-and-braces — the triggers backstop it.

To unhalt: same UPDATE pattern, `halt_mode='active'`, leave `halted_at` and
`halt_reason` as historical record (do not clear).

## Dashboard Rules
- Dashboard is served from `dashboard/static/index.html` on port 8080. Verified
  empirically 2026-05-08 (`docs/DASHBOARD_DOCTRINE_2026-05-08.md`): FastAPI `/`
  route returns `FileResponse(_static_dir + "/index.html")` and the only
  `StaticFiles` mount is `dashboard/static/`. The Vite tree at
  `dashboard/frontend/` is unwired experimental code — its `dist/` is never
  mounted.
- ALL dashboard edits target that single file — do not create new HTML files
  unless explicitly asked.
- `main.py` is the entry point; it imports `from dashboard.app import app` and
  runs uvicorn on 8080.

## Network Bindings
- **Trader dashboard (port 8080)**: uvicorn binds `127.0.0.1` (loopback only);
  LAN clients reach via Cloudflare tunnel at `bridge.ollietrades.com`.
  Network-wide bind would require separate auth review at the uvicorn bind
  `main.py:3250`.
- **Signal Center (port 9000)**: bound to `127.0.0.1` from pre-2FA legacy
  posture. HM-AW (`docs/XO_BACKLOG.md`) tracks reopening to network now that
  2FA TOTP + multi-user auth (Captain, Bonnie observer, Dad charts) are in
  place. SSH tunnel required today for non-bigmac browser access.
- **Two distinct auth layers** (do not conflate): browser users → 2FA TOTP +
  RBAC at Signal Center server layer; automation/scripts → SSH keys + bigmac
  OS account. Both valid; protect different surfaces.

## RAM Discipline (post-MSI-migration 2026-05-20)
- **bigmac (Mac Mini M4, 16GB RAM)** — runs FastAPI trader, dashboard,
  schedulers, signal center. Ollama is NO LONGER co-located here.
- **Ollie Max (`olliemax.home.local`, 192.168.1.168, RTX 5080 16GB VRAM)** —
  sole Ollama host. Budget: TWO 7–8B-class models fully co-resident (~10–12GB;
  live `/api/ps` 2026-05-28 showed qwen3:8b 5.98GB + ministral-3:3b 4.62GB =
  10.6GB resident together). A 14B fits solo but TWO 14B cannot co-reside in
  16GB → 14B-vs-14B rotation still swaps. **(Corrected 2026-05-28 HM-AUDIT-T0:**
  prior "RTX 5060 8GB / one 7B fits" was WRONG — it drove HM-WR-VRAM-THRASHING's
  premise + the navigator "too big for 8GB" swap, both now suspect; keep_alive/
  batching fixes still help, only the scheduling *rationale* changes. 16GB
  confirmed via live /api/ps; exact model per XO audit.) **SSH-gap RESOLVED
  2026-05-31:** passwordless `ssh bigmac@192.168.1.168` works (provisioned this
  day; verified `echo OK`). Ollama store on .168 = `/usr/share/ollama/.ollama/
  models` (service user; manifests/blobs world-readable, no sudo for reads;
  `ollama rm`/`list` work as bigmac via the API). The prior "SSH-to-Ollie-Max
  key gap" note is STALE.
- **Preferred local workhorse:** `qwen3:8b` (7 active agents share it per
  HM-CD `_HM_CD_KEEP_ALIVE` lookup).
- `qwen3:30b` rejected — too slow for this GPU (latency, not a VRAM-fit issue).
- Avoid loading full datasets into memory; stream or chunk.
- McCoy (CSP) runs on **`plutus-v1`** — the finance-trained model resolved via
  `ai_players.ollama-plutus` (`config.py:175`, "McCoy's finance brain"). Doc
  previously asserted McCoy=`0xroyce/plutus`; corrected to plutus-v1 (doc-vs-
  reality drift, HM-MODEL-RETIRE pre-check C 2026-05-31). The `0xroyce/plutus:
  latest` tag is unwired (its only ref, `dayblade-0dte`, is halt_mode='full',
  inert) and is **slated for retirement under HM-MODEL-RETIRE** (host `ollama
  rm` pending manual execution). Re-pullable from HF if ever needed.

## Free Models First (cost doctrine, set 2026-04-16)
- All agents default to FREE models — local Ollama or no-CC-required cloud
  free tiers.
- Paid models are FORBIDDEN unless the Admiral approves the spend, per agent.
- Approved paid exceptions: **Polygon Stocks Starter + Options Starter
  ($29 + $29/mo) ACTIVE as of 2026-05-12** for REST candles (HM-CB) + options
  (HM-CA chain). **NOTE 2026-05-27:** Starter does NOT include WebSocket
  trades — see `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`. Realtime tick
  stream pivoted to Alpaca IEX (free with paper account) per Phase 2.5
  (`drafts/HM-OLLIE-EVENT-TAPE-V2-REALTIME.md`).
- When proposing a model swap, show: model name, RAM cost, why it's orthogonal
  to existing fleet, free-tier rate limits.
- Rule of thumb: if two agents would run the same family (e.g. two
  LLaMA-derivatives), pick a different lineage (Qwen, DeepSeek-R1, Phi-4,
  Gemma) for real orthogonality.

### local_redirect flag (HM-CN Phase 2, 2026-05-17)
Some `config.AI_PLAYERS` entries declare `"provider": "openai"` for
legacy/branding reasons (Codex Prime, Codex Scout, GPT-4o, GPT-o3) but should
never actually hit paid APIs. The `"local_redirect": True` flag instructs
`engine/agent_routing.py::build_all_providers()` to construct an `OllamaProvider`
against `ai_players.model_id` despite the declared provider. Routes inference
to Ollie Box (Free) while preserving the agent's identity. Adding new
paid-API agents that must be locally redirected: declare `provider="openai"`
+ set `local_redirect=True` + ensure `ai_players.model_id` points to a free
local Ollama model.

## Git & Deployment
- Scotty handles `git push` + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`
  + verify inline. No Captain handoff (workflow updated 2026-05-11).
- Commit messages should reference the season (currently S6) and agent name
  when relevant.
- Trader runs under `.venv/bin/python3` (3.14); restart via `./scripts/trader_restart.sh` or `zsh scripts/trader_restart.sh` (it's `#!/bin/zsh` — the `&!` detach is zsh-only; **do NOT prepend `bash`**, and never a hand-rolled `venv` nohup — venv=3.9 crashes on PEP 604).

## Frontend Ship Rule (added 2026-05-12, HM-BJ.E4 lesson)
Non-trivial frontend JS changes require a **manual browser hover/click smoke
test** before declaring shipped. `node --check` and `py_compile` catch syntax
errors but NOT runtime IIFE/closure throws or DOM-binding regressions.
Re-attempts after a revert MUST include browser-test as a required closure
phase, not optional.

## Daemon Lifecycle Rule (added 2026-05-12, HM-EQ lesson)
Background daemons must bind to **process lifecycle** (module-level startup
+ explicit invocation in `main.py`), NEVER lazy-instantiated module state
coupled to a scan-cycle or agent-spawn path. Standalone import-tests can pass
while live production never fires — verify the live execution path with a log
heartbeat before declaring shipped. HM-EQ daemon went 128h silent because the
Arena-coupled spawn never fired; commit `54881bb` moved it to module-level.

## LaunchAgent Reboot Lifecycle (added 2026-05-23)

On this macOS box, `launchctl bootstrap gui/$UID <plist>` fails with "Domain
does not support specified action" when run over SSH — and RunAtLoad does not
fire at boot without a logged-in Aqua session. Verified for both
`com.trademinds.trader` and `com.trademinds.tunnel` on 2026-05-23. The plists
are correct; the boot-time activation is what's broken.

**Fallback in production:** `@reboot` cron entry calling a wrapper script that
detaches via `nohup … &!`. Used by:
- `scripts/trader_reboot_start.sh` (com.trademinds.trader, commit `44ec7e3`)
- `scripts/signal_center_reboot_start.sh` (signal-center Flask app, commit `169e714`)
- `scripts/cloudflared_reboot_start.sh` (cloudflared tunnel for
  bridge.ollietrades.com)

Drawback: no KeepAlive respawn on crash. Acceptable for stable long-running
services. cloudflared has its own multi-edge connection retry logic so
transient connection drops self-heal; only a full process crash would require
manual intervention.

A LaunchDaemon under `/Library/LaunchDaemons/` would be the apple-canonical
fix (system domain, runs at boot independent of GUI), but requires sudo +
Full Disk Access on the Terminal app. Deferred. HM-CLOUDFLARED-LAUNCHDAEMON
in `drafts/HM-CLOSET-POWER-PASTE.md` ITEM 4 plans the migration.

### Scheduler-owned jobs — keep parallel launchd plists archived

When a job is registered in the in-process `schedule` library inside `main.py`
(Riker XO at `main.py:4226`, the daemons at module scope), a parallel
`~/Library/LaunchAgents/com.ollietrades.*-cron.plist` that hits the same
endpoint is a **double-fire footgun**. The in-process scheduler tick is the
authoritative path; the launchd cron only causes contention (duplicate
signal-center hits, duplicate NTFY, duplicate DB writes).

**Audit pattern when in doubt:** for any
`~/Library/LaunchAgents/com.ollietrades.*-cron.plist`, grep `main.py` for the
equivalent `schedule.every(...).do(<job>)` registration. If both exist,
archive the plist — `main.py` wins.

**Schwab watcher launchd→cron migration (2026-05-28, HM-SCHWAB-WATCHER-CRON).**
The Schwab CSV watcher (`scripts/schwab_csv_watcher.sh`, scans `inbox/` every
60s) and its 48h staleness alarm (`scripts/schwab_cadence_check.py`, daily
06:30) ran ONLY via launchd plists (`com.ollietrades.schwab-{watcher,cadence}`).
Those plists do NOT auto-load at boot here — the recurring **`launchctl
bootstrap gui/$UID` "Domain does not support specified action" over SSH +
RunAtLoad-needs-Aqua-session** failure mode (see "LaunchAgent Reboot Lifecycle"
above). Both went silent after a reboot; the real-world portfolio pipeline
froze 2026-05-23→05-28 undetected. **Fix:** migrated both to crontab
(`* * * * *` watcher, `30 6 * * *` cadence) — cron survives reboot here, same
as the trader/signal-center/cloudflared `@reboot` wrappers. Plists archived to
`archive/launchagents_2026-05-28/`; cron is now the SOLE trigger. **Rule: any
service that must survive reboot belongs on cron, not a bare launchd plist, on
this box.**

### Alarms must not share a failure mode with what they watch (2026-05-28)
The Schwab staleness alarm existed specifically to "prevent recurrence of the
11-day silent-gap incident" — but it ran on the SAME launchd mechanism as the
watcher it backstopped. When launchd-at-boot failed, BOTH died: the data froze
AND the alarm that should have caught it was equally silent. **A monitor that
shares a failure mode with its target provides no defense.** Defense-in-depth:
run the alarm on a DIFFERENT mechanism than the thing it alarms about (e.g. a
staleness check inside the always-on trader process, or an external uptime
monitor), so one infrastructure failure can't take out both the function and
its watchdog. NOTE: the 2026-05-28 cron fix still has watcher + alarm on the
SAME cron — shared-fate remains; cross-mechanism relocation tracked in XO_BACKLOG.

**Second instance, same day (HM-PUSH-UNBLOCK):** `git push` failed silently for
**87 commits** because nothing monitors push health independently of the push
pipeline — surfaced only reactively when a push during HM-AUDIT-T0 hit the
large-file limit. Same disease: no independent watchdog. **SHIPPED 2026-05-29:
HM-PUSH-HEALTH-MONITOR** — `scripts/git_push_health_check.py`, daily cron
(`0 20 * * *`, NOT launchd), runs `git fetch` + `git rev-list --count
origin/main..HEAD` and NTFYs `ollietrades-admin` if local is >5 ahead OR if
fetch itself fails (can't-reach-origin is also push-health). Independent of the
push pipeline by construction. Two instances in one day → this is the
program's recurring blind spot: **build the monitor on a different mechanism than
the thing it watches, every time.**

### Restart-then-verify (HM-CONSOLE-INIT doctrine, 2026-05-13)
The "trader restart deferred until natural maintenance window" pattern creates
a window where buggy bytecode runs invisibly. Smoke-restart in a verify
window, OR push the restart synchronously with the commit. `py_compile` is
not enough — catches syntax errors but NOT undefined-name errors. Runtime
smoke required for any cross-module symbol change.

## HM-AM Scope (added 2026-05-12, HM-CLOSE-GAP W1.1)
Total Portfolio = **real-world net worth only** (Schwab + Webull + IBKR +
physical metals). EXCLUDES Alpaca paper trading book — separate
research/strategy-validation surface, must not co-mingle with real-world
capital reporting. The two-book bridge policy below governs how the books
communicate without mixing.

## Backtest Rule
- Always run ALL agents in backtests, never a subset
- Never cite in-sample (IS) numbers without the matching OOS figure

## Architecture: Two-Book Bridge Policy (Option β, established 2026-05-05)

**OllieTrades operates two separate books, by design.** Source: HM-I
investigation (`docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`); Admiral
decision 2026-05-05.

### The two books
1. **Internal AI fleet book** — `positions` table for all `player_id != 'webull'`.
   Research / calibration. The legacy fleet (ollama-plutus, qwen3-8b-flash,
   deepseek-7b-grok4, ollama-qwen3, energy-arnold, capitol-trades,
   gemini-2.5-flash, etc.) writes here. **Never forwards to Alpaca.**
2. **Alpaca paper book** — Alpaca's live broker state, mirrored locally as the
   `webull` player's positions. Real-on-broker activity for routed players +
   spread strategies.

### What routes to Alpaca
- The **routed players** in `engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER`:
  - `super-agent` (Mr. Anderson) → Alpaca Paper (portfolio id=1) — **HALTED
    `halt_mode='full'` since 2026-05-11** (`is_paused=1` reconcile; last actually
    traded **2026-03-28**, 16 lifetime trades, 0 open positions). The routing
    entry persists but is INERT while halted — this row is the routing map, NOT
    a claim that Anderson is live. No recurring cost (api_costs empty; crewai
    pinned/not loaded). Reviving = an Admiral decision. (Doc truth-up 2026-05-31,
    HM-SUPER-AGENT-VERIFY — was silently listed as a live router for 10+ weeks.)
  - `ollie-auto` → Alpaca Paper (portfolio id=1)
  - `neo-matrix` → Neo Matrix (portfolio id=7) — flipped to `halt_mode='active'`
    2026-05-13 (HM-AN2.3, "the show must go on Maestro!")
  - `dalio-metals` → Enterprise Computer (portfolio id=5, physical-metals
    tracker, `route_mode=tracking`, log-only)
- The **spread strategies** (post-gate-flip 2026-05-04, gated on `player_id in OPTIONS_PLAYERS` at `engine/alpaca_options.py:711` inside `execute_options_signal` (line 688)):
  - `bull_call_spread_v1`, `bear_put_spread_v1`, `executor` — route via
    `engine/alpaca_options.py::execute_options_signal`, a third forward path
    that bypasses the player-keyed routing table.

### What stays internal
Every other player. The 9+ active legacy-fleet agents emit signals and trades
into the `positions` table only. Their entries never reach Alpaca paper.

### How forwarding gates work
`engine/paper_trader.py::_forward_to_alpaca` (line 244) is gated on
`route["route_mode"] == "trading"` at all three call sites:
- BUY (line 1546), full-SELL (line 1850), partial-SELL (line 2045 — gated as
  of 2026-05-05 commit `d06c33c` per HM-I Option ε).

Players whose mapped portfolio resolves to `route_mode=paper` (default) or
`route_mode=tracking` (Enterprise Computer) never forward to the broker.

### Why two books, not one
- Spread strategies and routed players need real broker state for honest
  execution paths.
- Legacy fleet is research / calibration — separating their book from the
  broker preserves test isolation.
- Shorts and futures (GC=F, SI=F) live in the internal book naturally;
  Alpaca paper can't accept futures.
- Legacy fleet halt/retirement decisions don't pollute the broker state.

### Naming discipline
- "Arena Paper" = the default unmapped routing destination (no DB row;
  `route_mode=paper`). Most legacy-fleet agents land here.
- "Alpaca Paper" = `portfolios.id=1`, the actual broker connection.
- **Different things despite similar names.** Future dashboard work will make
  this visually distinct (HM-I-β followup).

## SUPER_MAX Wave Program (W0–W4, shadow-first; added 2026-06-03)

Staged research→graduation pipeline. Every wave is OBSERVATION-FIRST: candidate edges
emit as SHADOW, accrue forward, and graduate to execution ONLY on the validation gate
+ explicit Admiral go. Execution stays OFF until both are satisfied.

### Graduation gate (load-bearing)
No setup is proposed for execution until it clears DSR ≥ 0.95 AND PBO ≤ 0.30
(strategies/validation.py) on FORWARD (true-OOS) sample — never raw Sharpe, never
in-sample alone. PBO needs a non-degenerate config universe (a 2-setup matrix is a
coin-flip artifact). W4 bucketing shrinks n → PBO matters MORE, not less.

### Shadow boundary (hard)
Shadow signals carry agent='shadow-bridge:<setup>' and are excluded from execution by
construction: (1) the only trade_signals→buy consumer (neo-matrix) is exit_only, and
(2) a defensive guard skips agent_name.startswith('shadow'). They exist for W0 forward
scoring only; emission never forwards to Alpaca.

### The waves
- W0 — forward-scoring substrate: signal_outcomes (signals.db) ⟷ trade_signals 1:1 by
  signal_id; expectancy_engine.score_backlog() = stop-first R @1/3/5/10d, IS/OOS split.
  Lead: relative_strength (in-sample DSR ✓, n=444). PBO = 0.6348 FAIL (genuinely fragile,
  confirmed 2026-06-01 on 144-config decorrelated grid — do not graduate). bull_flag +
  rsi_bounce + rsi_divergence accruing. No-level signals are unscoreable.
- W1 — source health: engine.source_gate + /api/sources/health (signal-center :9000) →
  RED-first grid (signal-center/index.html). Auto-quarantine tracker is report-only
  (AUTO_QUARANTINE_ENABLED default-off); independent cron watcher
  (scripts/source_health_watcher.py, NTFY topic ollietrades-admin) is the live alerter.
- W2 — bracket sizing: SPEC only (drafts/). Gated AFTER graduation. Fixed-fractional →
  ≤0.25× Kelly; observation-only sizing log, never buy().
- W3 — gamma mapper + unusual-OI: SPECs only. Canonical GEX = /api/gex-snapshot (trader
  :8080, SPY/QQQ); unusual-OI from flow_gex.db flow_aggregates. Print-level flow is
  Polygon-tier-blocked (403) — an Admiral cost decision, not a build.
- W4 — regime-conditional routing (capstone): signal_regime sidecar (signals.db) stamps
  gamma_sign (from the GEX regime LABEL, not raw total_gex sign) × VIX term (vix_monitor)
  × time-of-day on every shadow signal via signal_bridge._stamp_regime. Router built only
  once per-(setup × regime-bucket) sample clears the gate.

### Build discipline (this program specifically)
Verify-before-build: every wave spec assumed a dependency that was stale, already-built,
or mis-shaped (W1 grid already shipped; 'iso' ts_format unsupported by source_gate; gamma
sign inverted vs the engine label; expectancy_engine can't read context_json). Confirm
CURRENT code/DB state via SQL/grep/live-probe before scoping any wave.

## Error Handling Posture (established 2026-05-05)

After HM-Z (BTO bug, commit 306dcf6) and HM-AA (empty-body errors, commit
a9d0649) surfaced two silent-failure cases in 12 hours, the posture is:

**1. Bare `except Exception` is acceptable when the handling correctly
accommodates unknown failures.** Bare except is *not* the bug. The BTO bug
was a bug because the handling (return error dict; caller misread it) was
wrong. Some places legitimately want broad catch — per-agent cycles where
one agent's crash shouldn't take down the fleet. When you write `except
Exception as e:`, ask: "if `e` is a programming error (AttributeError,
ImportError, NameError) instead of an operational error (APIError,
ConnectionError), does my handler do the right thing?" If the handler treats
those identically and that's wrong, narrow the except.

**2. Error logs capture type + repr, not just str.**
```python
# Avoid:
except Exception as e:
    console.log(f"foo error: {e}")
# Prefer:
except Exception as e:
    console.log(f"foo error: {type(e).__name__}: {e!r}")
```

**3. NTFY on first occurrence per error class per day for architecture-class
paths.** Architecture-class paths:
- Every broker-submit code path (`submit_*` in `alpaca_options.py`, future
  webull/IBKR adapters)
- `halt_mode` writes (anything transitioning an `ai_players` row's halt state)
- Position-of-record writes (mutations to `positions`, `options_trades`,
  sync destinations)

Threshold: first occurrence of an error class within a 24h window NTFYs.
Subsequent same-class occurrences within window suppress (avoid alert
fatigue). New class within window = new NTFY. Window resets at midnight.

**Caveat (Day-2 lesson 2026-05-05):** `engine.alert_channels._rate_state` is
in-memory per process. `rate_limit_secs=86400` means "first per error class
per process lifetime", not per 24h wall-clock. Service restarts reset dedup.
Persist-to-settings deferred.

Non-architecture paths (legacy fleet signal cycles, Polygon timeouts, Ollama
timeouts, transient noise) do NOT NTFY. Log only.

**4. Going-forward, not retroactive.** Hundreds of `except Exception as e:`
blocks exist. Posture applies to new code changes that touch exception
handling and to paths where we discover and investigate a real error. Old
code stays as-is until naturally touched.

## Ghost Tracking Architecture (two systems, established HM-BC 2026-05-11)

**OllieTrades has TWO ghost-tracking systems with orthogonal concerns.** They
share a SQL table name (`ghost_trades`) but live in different DBs with
different schemas.

| | `engine/ghost_scoring.py` | `engine/ghost_trades.py` |
|---|---|---|
| **Purpose** | Signal-center agent win-rate scorecard | OllieTrades fleet decision log + missed-opportunity stats |
| **Storage DB** | `data/ghost_trades.db` (own DB) | `data/trader.db` (shared, sacred) |
| **Reads from** | `signal-center/signals.db::trade_signals` (BUY signals, conf ≥ 70) | n/a — writer-only |
| **Written by** | Own pipeline: `capture_new_signals()` + `check_outcomes()` | `ai_brain.py::log_ghost_trade` (HOLD>0.6) + `ghost_advisor.py` (BUY/SELL) |
| **Outcome resolver** | Live Alpaca bars + signal-center `signal_outcomes` | Computed at SELL time vs `ghost_portfolio.avg_cost` |
| **Agent universe** | signal-center agents (etf_regime_trader, danelfin_ai, chekov, navigator, …) | OllieTrades fleet (ollie_super_trades, trailing_stop, kirk_advisory_log, …) |
| **Dashboard endpoints** | `/api/ghost/scorecard`, `/api/ghost/trades`, `/api/ghost/refresh` | `/api/ghost-trades`, `/api/ghost-trades/stats` |

### Naming discipline
- `ghost_scoring.py` (renamed from `ghost_trader.py` in HM-BC.2) = the
  signal-center pipeline.
- `ghost_trades.py` (the HM-AZ/HM-BB module) = the trader.db decision log.
- "Ghost trades" user-facing term can mean either — API path is the
  disambiguator. `/api/ghost/*` = scoring. `/api/ghost-trades*` = decision log.

### Do not consolidate
Two modules export non-overlapping functions, read different DBs, serve
different agent universes. A previous attempt to treat one as stale (HM-BB
closure note misread the singular file as obsolete) would have silently
broken the live `section-ghost-scorecard` panel. **Future cleanup must verify
both UI panels still render before touching either module.**

## Fleet Roster (S6.3, post-OOS-validation)

### Active 4 — Voters (live paper trading)
| Rank | Name    | Strategy / Type                              | Model                         | OOS Sharpe |
|-----:|---------|----------------------------------------------|-------------------------------|-----------:|
| 1    | McCoy   | CSP options seller — high-VIX regime         | plutus-v1 (Plutus, finance)   | +11.1      |
| 2    | Neo     | Rule-based premium/GEX pattern detector      | Deterministic (no LLM)        |  +6.1      |
| 3    | Dax     | CSP options seller — low-VIX regime          | qwen3:8b                      |  +4.9      |
| 4    | Capitol | Congressional STOCK Act copy-trader          | Data feed (no LLM)            |  +1.8      |

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

## Duplicate Role Policy
- **Healthy duplication** (keep): McCoy+Dax both run CSP but on different VIX
  regimes. Capitol+Aladdin+Uhura-EDGAR all "smart money" but orthogonal data
  sources (retail Congress / institutional ETF / 13F). Verify McCoy-Dax trade
  overlap stays <60% quarterly.
- **Bad duplication** (consolidate): Momentum cluster (Neo/Chekov/Navigator) —
  Neo owns it now. Cloud-LLM cluster (Spock/Worf/Seven) — consolidated to
  Spock only, then Spock moved local 2026-04-16.

## Season 6.3 Config (current)
- Tractor Beam = tiebreaker (not full voter)
- Gate thresholds: neo-matrix 1.75, chekov 5.0 (muted), sniper alpha 0.25
- Target signal conversion: 3–5%
- IS 180-day baseline: 100% WR, Sharpe 4.845 (OVERFIT — see OOS)
- **OOS 2024 clean baseline:** Sharpe **2.692**, WR 65.8%, 456 trades, all
  strategies beat SPY (+17.5%)
- CSP dominates: OOS Sharpe +6.05 across BULL and CAUTIOUS regimes

## Feature Flags

Feature flags live in `config.py` as module-level constants. Engine modules
import lazily inside the gated function (`from config import FLAG_NAME`).
Flipping a flag requires a service restart:
`launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

### Active flags

| Flag | Default | Purpose | Reversal |
|------|---------|---------|----------|
| `SPREAD_CANNIBALIZATION_GUARD_ENABLED` | `False` (LIFTED 2026-05-28) | HM-AF-α 2026-05-06 (emergency halt). Halts P1 (`engine/battle_station.py::monitor_active_options`), P2 (`engine/alpaca_options.py::close_all_options`), P3 (`engine/dayblade.py` post-trade close). HM-AF-β added inner-layer `engine/options_utils.is_spread_leg(symbol)` filter at all three sites; HM-AF-γ added wrong-side-of-book correction in `_auto_close` via `qty_signed`. **Phase 4 decision 2026-05-28: LIFTED** — containment moved to the cannibalization source (dayblade-0dte / T'Pol) being `halt_mode='full'`. Re-enable in `config.py` if cannibalization recurs. | Set `True`/`False` in `config.py` + restart |

## strategy_signals (convergence scanner data plane)

`strategy_signals` is the canonical table for multi-strategy convergence
candidates feeding Phase 1/2/2.5 of HM-OLLIE-LIVE-SCANNER and the live event
tape. Columns: `ticker`, `strategy_name`, `confidence`, `entry_price`,
`stop_price`, `target_price`, `created_at`, `scan_date`. Read paths:

- `dashboard/app.py::api_scanner_convergence()` — `/api/scanner/convergence`
  tier-counts strategies over a 90-min window. Tier 1: ≥5 strategies. Tier 2:
  4. Tier 3: 3.
- `dashboard/app.py::api_scanner_events()` and `api_scanner_events_realtime()`
  — decorate events with `in_scanner_tier` from the same 90-min window.
- `engine/event_tape.py::_scanner_tier_for()` — Phase 2.5 detector
  cross-reference.

**Legacy convergence scanner — NOT dead (corrected 2026-05-28, HM-CLAUDE-STALENESS):**
The earlier "scan_strategies write path silently dead since 2026-04-07" note is
**STALE/WRONG**. `engine/strategies.py::scan_strategies` is LIVE: the Navigator
endpoints `/api/navigator/strategies/scan` and `/api/navigator/scan-now`
(dashboard/app.py) call it with default `save=True` → writes `strategy_signals`,
which is read back by `/api/navigator/convergence`, `get_todays_signals`,
`event_tape`, `trade_cards_api`, `tick_recorder`. Do NOT retire it without first
deprecating those Navigator endpoints (a product decision) + a coordinated
reader migration. Verified live during WAVE 4 (2026-05-28).

**Window doctrine:** the canonical convergence window is 90 minutes. Match it
when adding new readers — windows shorter than 30 min miss premarket scan
batches; longer than 120 min mixes regimes and produces stale tier flags.

## Logging Sink Split (trader.log vs trader_error.log)

OllieTrades logs to two files with different sinks:

| File | Sink | What goes here |
|---|---|---|
| `logs/trader.log` | Rich `console.log(...)` calls | Per-cycle agent output, strategy ticks, market data, formatted user-facing log lines |
| `logs/trader_error.log` | Python `logger.info / .warning / .error` calls | Structured Python logging — including `engine.alert_channels` NTFY dispatch logs |

**Implication for investigations:** when checking whether a NTFY actually
fired, search `trader_error.log` for entries like:
- `[LRS] Alert dispatched [warning/{alert_type}]: {message}`
- `[LRS] ntfy sent [200]: ?? TradeMinds {Level}`

Searching only `trader.log` will miss NTFY firings — they POST at HTTP 200
and produce correctly; they just land in the other file.

## Doctrine Lessons (distilled from sprint sessions)

### Multi-path scanning is implicit resilience — preserve it deliberately (2026-05-29)
When the arena scan stalls (`run_scan` holds `_scan_lock` unboundedly, §C), the
`crew_scanner` keeps producing signals (`sig#` advances) — the fleet doesn't go
signal-dark. Two independent scan paths (arena/`_SCAN_TIER` + `crew_scanner`) mean
one can hang without total signal-flow loss. This redundancy wasn't designed as
fault-tolerance but functions as it. **Preserve it deliberately:** don't
consolidate the two scan paths into one for "simplicity," and when fixing the §C
stall (HM-RUN-SCAN-WATCHDOG) keep `crew_scanner` independent of the arena lock.
Same family as "alarms must not share a failure mode" — independent paths survive
independent failures.

### Measurement-instrument bugs: boundary-isolate before reporting rates (2026-05-29)
The analysis tooling keeps biting us as badly as the bugs. Two instances: **date-less
log lines** (2026-05-29 — `trader_error.log` `[LRS]` lines carry HH:MM:SS but no date, so
`grep | uniq -c` by hour silently aggregates *multiple days* into one bucket → the "30-53/hr
drift baseline" was multi-day-per-bucket, ~6× the true single-night rate); and **rich-console
wrapping** (2026-05-28 — `console.log` wraps long lines, so naïve `grep`/`wc` of wrapped
output miscounts). **Rule: any time-window rate analysis MUST explicitly state the
day-boundary verification (how the post-restart/today segment was isolated — line offset,
restart marker, contiguous-ascending-timestamp block) BEFORE reporting a rate.** A rate
without a stated boundary method is suspect. Verify the instrument, not just the result.

### Trace EVERY sub-fetch to its leaf before fixing a multi-fetch block (2026-05-29)
When a hang localizes to a block that bundles multiple sub-fetches, trace **every**
fetch to its leaf — or instrument to distinguish them — **before** committing to a fix.
Don't stop at the first HTTP call and assume. HM-RUN-SCAN-WATCHDOG Loop 5B localized the
stall to `build_scan_context`'s `ctx:catalyst` block, traced it to the Finnhub
`/calendar/earnings` call, and shipped a cache+deadline on `_fh_get` — but the real hang
was two levels deeper (`get_earnings_countdown`'s per-symbol Alpha Vantage enrichment +
`get_trending_tickers`'s per-symbol Yahoo loop), so the fix didn't move the needle. The
**no-deadline-trip test** (ctx:catalyst still hung 200s+ with zero `TOTAL-deadline` logs ⇒
the hang is NOT in `_fh_get`) caught the mis-attribution — but only **after** a restart
cycle. **Cost of one wrong-fix restart cycle > cost of one extra instrumentation pass to
confirm the cause.** When a block fans out, split-marker first (same family as
[[boundary-isolate]] and layered-bottleneck). Related: external-fetch-discipline bug class
(unbounded per-item external fetch, cold cache, every agent re-pays) — now at 5 instances
(Loop 1 Yahoo indicators, Loop 3, 5B Finnhub calendar, earnings AV enrichment, trending Yahoo).

### Code comments document INTENT — divergence from implementation is a signal (2026-05-29)
When implementation diverges from a comment, investigate — it's code drift, a wrong
comment, or a design that was never realized. `build_scan_context`'s "Build shared scan
context once per model" comment vs the per-agent-call reality (every one of 19 agents
rebuilds all 14 blocks, re-paying market-wide fetches) is example #1: the comment captured
the *intended* design; the code never implemented the sharing. Aligning code to its own
documented intent (cache the market-wide blocks once per cycle) is the highest-leverage fix.

### Verify the MODEL of the system before designing a fix against it (2026-05-29)
The design model you carry of "how the code is shaped" is itself a verify-before-fix
surface. HM-RUN-SCAN-WATCHDOG Loop 5D proposed "cache 12 market-wide blocks once / 3
agent-specific fresh per agent" — a model assuming every agent computes a unique context.
Reading the actual code showed the real variation is only 3-4 *profile* branches
(energy-arnold / options-model / chekov-gate), so the correct cache key is `(cycle,
profile)` (~4 builds/cycle), not per-block. The right cache key follows the **actual
variation pattern, not the proposed model**. Confirm the model reflects the code before
designing structure against it — same family as [[trace-every-subfetch-to-leaf]] and
boundary-isolate.

### Restart verification: confirm the LISTENER died, not "something on the port" (2026-05-29)
`lsof -ti tcp:8080` returns **every** process with a connection to the port — clients and
child connections too, not just the LISTEN owner. Killing a transient client leaves the real
trader running on **old bytecode** — a *silent restart failure* (the relaunch can't bind, so
it exits, and the stale process keeps serving). Near-miss 2026-05-29: killed PID 22601 (a
client) thinking it was the trader; listener 26349 survived on Loop-5D bytecode; the new
process bound in "2s" (suspiciously fast) — the tell that nothing actually restarted. Caught
by re-checking the LISTEN owner. **Correct pattern:** target the listener specifically —
`lsof -tiTCP:8080 -sTCP:LISTEN` — and verify post-restart that the **new PID differs from the
old listener PID** AND the bind took the normal ~40s (a ~2s bind means the old process never
died). **Restart success = new-PID-bound + old-PID-gone, verified — not assumed.** Same class
as "deployed ≠ working" / [[kickstart-after-backend-merge]]: *restarted ≠ new bytecode active.*
Verify the state transition; don't assume the command did what you intended.

**ESCALATION 2026-05-29 — orphan traders (HM-RESTART-ORPHAN-PREVENTION):** killing only the LISTENER
isn't enough. A process can **free the listener but keep running its scan loop** as an orphan. Today
two traders ran in parallel 2.6h (PID 29543 from 15:15 + the live listener) — the orphan ran OLD code,
double-scanned, and polluted the shared `trader.log` with stale phase lines that made a *correct* fix
look failed → a multi-restart phantom chase. **"Port freed" ≠ "process dead."** Restart MUST: (1) kill
ALL `main.py` procs (`pkill -f main.py` — note the binary is `Python` capitalized, so `grep
python.*main.py` MISSES it; match `main.py`), and (2) confirm **single-writer** via `lsof
logs/trader.log` (exactly one Python PID) BEFORE trusting any post-restart measurement. A shared log
with two writers is a measurement-instrument bug that survives every boundary trick.

### When you can't validate a content filter, bound QUANTITY not content (2026-05-30)
A scan/screen that's too big has two fix shapes: (a) a **content filter** (keep the "relevant" subset)
or (b) a **quantity bound** (process N per cycle, rotate, full coverage over time). Choose by whether
you can VALIDATE the content criteria. The §C floor (McCoy/Dax CSP sellers analyzing all 307) looked
like a content-screen problem — until verify-before-fix found there was **nothing to validate against**:
they'd signaled on ~all 312 symbols (no selectivity to learn from) AND there was no cached
options/IV universe to screen on (a real CSP screen would need the per-symbol fetches we'd just killed).
**A content filter you can't validate risks silent PERMANENT alpha loss** (you drop a profitable name
and never know). **A quantity bound (bounded-rotation) loses nothing** — every name still scans, just
rotated across cycles — and for a SLOW strategy (CSP hold-to-expiry) temporal coverage (~12h full sweep)
is fine; you don't need every name every cycle. Rule: **no known-good-set + no cached universe to filter
on ⇒ bound the quantity, don't guess the content.** (Rotation cursor pattern already exists:
`_ALPHA_PAIR_IDX` in crew_scanner.py — but persist the offset to `settings` so restarts don't re-scan
the head and starve the tail.)

Full session narratives in `docs/CLAUDE-archive-2026-05.md`. Rules below are
load-bearing today.

### §C-arc lessons: deletion, stale paths, and spikes-mask-floors (2026-05-29/30)
Three lessons from the multi-day HM-RUN-SCAN-WATCHDOG arc, consolidated:
- **The cheapest fix to an expensive operation is sometimes deleting it.** §C's biggest infer cost
  (deepseek scanning 307 via LLM) wasn't a perf bug — it was a *path that shouldn't run at all*
  (Spock was converted to deterministic `spock_rules`; the arena LLM was a leftover). Before
  optimizing HOW an expensive path runs, ask whether it should run. deepseek + ollama-coder were both
  free deletions (zero coverage loss).
- **Role-conversions leave stale parallel paths — and they bite repeatedly.** When an agent is
  converted (LLM→deterministic, scanner→bench), the OLD path is often left wired. Bitten ~6× this arc:
  deepseek + ollama-coder (arena LLM leftover after rules-conversion), Worf in 3 roster lists, navigator
  (re-homed to trade-only, old `tractor_beam→save_signal` emitter dropped → dead `signals`). **When you
  convert an agent's role, enumerate + remove ALL old paths, don't just add the new one.** Same family as
  [[agent-state-must-reconcile-across-all-sources]].
- **Spikes mask the floor — fixing a hang reveals the next, slower bottleneck.** §C had layered causes:
  the loud spikes (catalyst 540s, indicators 552s, quote_summary) masked a quiet legitimately-long
  *floor* (analyze-all-307). Each spike-fix unmasked the next layer. **The soak metric (zero HELD>60s) is
  the true closure signal, not "fixed cause X"** — and a remaining "stall" may be a legitimate-but-slow
  floor (fix by bounding/reducing work), not a hang (fix by bounding the call). Distinguish them before
  reaching for a deadline.

### Agent state must reconcile across ALL sources (HM-WORF-DRIFT-RECONCILE, 2026-05-29)
When an agent's state lives in N sources, **all N must reconcile or the system
lies to future-session diagnostics.** Worf (`qwen3-8b-flash`) was benched S6.1
(−0.36%) but still appeared "active" in 6 places — `ADVISORY_CREW` (correct),
`_SCAN_TIER2` (stale), `SNIPER_AGENTS` (stale), `ai_players` active (correct —
load-bearing), the WR provider rotation (correct), and the Fleet Roster doc
("~25 sigs/day", stale). A morning diagnostic wrongly read "in WR rotation +
active" as healthy. **`ADVISORY_CREW` is canonical for benched-but-keeping-for-
bridge-vote agents** = no individual scanning, but `ai_players` stays `active`
(+`is_active=1`,`is_paused=0`) because `war_room.py` skips
`halt_mode!='active'`/`is_active=0`/`is_paused=1` — so an "active" row is
*required* to keep the bridge vote, NOT drift. Deeper bench (no bridge vote
either) = `exit_only`/`is_paused=1` (Uhura, Sulu). Before "fixing" an agent's
`ai_players` state, check whether WR/scan paths depend on it. **Worf reconcile
CLOSED 2026-05-29:** all 3 residual scanner memberships removed —
`_SCAN_TIER2` (main.py, only `ollama-plutus`+`ollama-qwen3` remain),
`SNIPER_AGENTS` (proving_ground.py), and `RULES_SCANNERS` (crew_scanner.py, the 3rd
location, found in the fleet-review sweep). The earlier "Uhura/Troi/Trip still in
`_SCAN_TIER2`" note was itself stale (inverse drift — doc lagged code): live
`main.py:236` had already pruned them. **Lesson:** a roster-drift sweep must enumerate
ALL membership lists (`_SCAN_TIER2`, `SNIPER_AGENTS`, `RULES_SCANNERS`, `ADVISORY_CREW`,
`ai_players`) in one pass — Worf lived in 3 of them and took 2 sweeps to fully clear.

### Diagnostics first (HM-CD-migrate, HM-BP, 2026-05-13)
Never modify production code paths before reading current behavior via
`grep + sqlite + log inspection`. HM-CD-migrate almost shipped as a Polygon
migration based on stale assumptions; real cause (Ollama model swap on every
call) emerged only when ollama-coder logs were read in context. HM-CD-instr
instrumentation was the savior.

### Console init verification (HM-CONSOLE-INIT, 2026-05-13)
`logger.* → console.log` flips are NOT safe defaults. Before flipping, verify
the target module has `from rich.console import Console` and `console =
Console()` at module scope. Runtime-smoke each touched module with
`python -c "import engine.X; engine.X.console.log('test')"` before commit.
`py_compile` catches syntax errors but NOT undefined-name errors.

Cycles can "succeed" from the scheduler's view while never finishing emitting
their lines, if `try/except` swallows the `NameError`. Consider
distinguishing programming-error subclasses (`AttributeError`, `NameError`,
`ImportError`) from operational errors and NTFY-ing the first class.

### Positions table is canonical (HM-STALE-TRIM-OBS-V2, 2026-05-13)
For "is position open" queries, anchor on `positions` table joined to
`trades` for context. Trades-table arithmetic (BUY without matching exact-qty
SELL = stale) produces false-positives on partial scale-outs. Always filter
to `halt_mode='active'` players to exclude halted zombies.

### Ollama keep_alive per-model lookup (HM-CD-migrate, 2026-05-13)
Universal `keep_alive: "30s"` (legacy 16GB constraint) forces full model
reload on every call. Per-model `_HM_CD_KEEP_ALIVE` lookup: high-frequency
models (qwen3:8b=7 agents, qwen2.5-coder:7b=2 agents) get 30m residency;
alpha-squad rotation 10m; rare models keep 30s default. Hardware reality is
RTX 5080 with 16GB VRAM (corrected 2026-05-28 HM-AUDIT-T0 — NOT the "8GB"
previously recorded here; live /api/ps showed 10.6GB co-resident); budget is
two 7–8B models co-resident, but 14B-vs-14B rotation still swaps.

### Three-book broker reconciliation (2026-05-20)
Verify ALL three books (real-money / Alpaca paper / fleet) before declaring
exposure resolved. NVDA "closed on Webull" hid two errors: Webull was
liquidated 5/13 (nothing to close) AND Alpaca paper still held 12.34 sh
ghost. Cross-check pattern: query all three independently, reconcile.

### Frontend Ship Rule (HM-BJ.E4, 2026-05-12)
See "Frontend Ship Rule" section above. Browser hover/click smoke is
mandatory for non-trivial JS changes — repeated here because it's the most
commonly skipped rule.

### Verify data-source tier before locking spec (HM-LESSON, 2026-05-27)
For any spec that names an external API/streaming source as "available",
verify the actual tier capability BEFORE locking the spec. A live probe of
auth + first subscribe is a 60-second test that prevents downstream rework.
Polygon Stocks Starter ($29) was assumed to include WS trades; live probe
found it does not. Pivot to Alpaca IEX cost zero $ but one module rewrite.
See `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`.

## Account State

- **HM-WEBULL-LIQUIDATED (2026-05-13)**: webull real-money account
  liquidated. Historical trades remain in `trades` table as record. Account
  state in `ai_players` set to `halt_mode='full'` with descriptive
  halt_reason. Positions table zeroed for webull. Any future query that
  iterates real-money accounts must exclude webull or check
  `halt_mode='active'`. Previous "Webull ~$6.6K = monitor only" note is
  superseded.

## Drift Catalog 2026-05-17

10 distinct drift classes caught in a single audit-first session. Captured
here so the doc can be trusted without re-verifying each session.

1. **Model assignment silent bypasses** — main.py per-call overrides shadow
   `ai_players.model_id` for ~3 routed players; DB truth ≠ runtime truth.
2. **Hidden bypasses from config.AI_PLAYERS-scope-limited side-by-side** —
   HM-CN audit only walked `config.AI_PLAYERS`; main.py and crew_scanner
   overrides outside that scope went undetected for months.
3. **Role-vs-reality gaps** — Spock documented as LLM second-opinion
   (qwen3:8b) but Sniper Role #1 is rule-based RSI-bounce (no LLM call); Troi
   options wheel blocked by Bridge gate despite role.
4. **Dead-code gates** — `PAID_MODEL_IDS` guard unreachable for cto-grok42
   path; the model swap never traversed the gate that would have blocked it.
5. **Fleet-config-vs-reality** — 5.8 Ollie migration assumed mistral:7b moved
   to Ollie Box; reality was bigmac. Pin → routing decisions based on false
   state.
6. **Docs-vs-bind reality** — CLAUDE.md claimed port 8080 "bound network-wide"
   but `main.py:3250` pins to `127.0.0.1`. LAN reachability is Cloudflare
   tunnel only. (Fixed in network bindings section above.)
7. **debate_engine.py is NOT CrewAI** — any doc describing
   `engine/debate_engine.py` as CrewAI-orchestrated is wrong. Reality: plain
   `asyncio` + `aiohttp` + Ollama HTTP.
8. **war_room_debates vs debate_history_v2 table confusion** —
   `/api/war-room/debate-history` serves `war_room_debates` (Captain
   Ask-Arena, ~1,447 rows). 12-agent structured debates live in
   `debate_history_v2` (~272 rows), served by `/api/war-room/debates/recent`.
9. **Journal "gitignored per convention" — wasn't** —
   `data/scotty_journal_*.md` was treated as gitignored-by-convention but
   `.gitignore` had no matching pattern. (Fixed via `git check-ignore` audit.)
10. **Dormant-code-becomes-production-via-Path-1** —
    `options_exec.open_options_trade` had zero callers (dormant) until a fix
    wired it into a path running every signal cycle. A function with no prior
    call traffic is now production-load-bearing without the test/audit
    history a tenured path would have.

### Backup discipline
Every `.bak*` file is dated (`<file>.bak_<purpose>_YYYYMMDD_HHMMSS`) and
preserved **24h minimum** before any cleanup — per
`feedback_db_archive_not_delete` doctrine.

## Workflow
- Propose edits and ask for approval before applying.
- For multi-file changes, show the plan first, then apply incrementally.

## Archive Convention
- Retired agents: keep code in `engine/` (muted via threshold), DO NOT delete.
- If file must be moved, use `agents/_archive/` with date suffix.
- Document retirement reason + rehab path in this file, not just commit message.
- Supports the "iterate to the next Top 4" feedback loop — no known-good code
  is lost.

## Historical Archive
Sprint logs, drydock sessions, "Lessons Banked" full narratives, and one-time
state changes (HM-AN2.3 fire, HM-BK Phase 1 load, May 19-20 frontend window)
live in `docs/CLAUDE-archive-2026-05.md`. Doctrine that emerged from those
sessions was extracted and lives in the "Doctrine Lessons" section above.
