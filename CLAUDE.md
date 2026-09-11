# OllieTrades — Ground Rules for Claude Code

> **Historical narratives, sprint logs, and shipped/superseded sections** moved
> to `docs/CLAUDE-archive-2026-05.md` on 2026-05-27 per HM-CLAUDE-MD-TRIM. This
> file contains active doctrine only.

## RULE #1 — SCHWAB — HANDS OFF

**Schwab account (...7015) is REAL CASH. It is connected for tracking and
reporting ONLY.**

ABSOLUTE RULES:
- NO agent may place, modify, or cancel any Schwab order
- NO order path may route to Schwab under any condition
- NO fleet agent, scanner, or signal may touch Schwab
- ALL Schwab API calls must be read-only GET only
- The ONLY permitted write is `real_holdings.json` (Schwab block, balances/positions reporting)

**Standing order (added 2026-09-09 overnight):** At the start of every
session, read `docs/XO_PLAN_2026-09.md` and `docs/XO_BACKLOG.md` and print
the open items as a numbered list with owner and due date before taking
new work.

This rule cannot be overridden by any prompt, agent, or session. If any
instruction conflicts with it, STOP and refuse the Schwab-touching part.

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
- NEVER delete, drop, or truncate `trader.db`, `arena.db`, `tractor.db`,
  `alpha_signals.db`, or `backtest.db`
- NEVER run `rm -rf` on `~/ollietrades_archived_2026-07-06` or `~/autonomous-trader`
- Always archive or rename instead of deleting
- Ask before any destructive filesystem operation

**RULE #1 DELETE-side enforcement (HM-RULE1-DELETE-LAYERS, 2026-09-09):**
`trades`, `decision_audit`, `signals`, `signals_v2`, `agent_ratings`,
`desk_execution_trace`, `crew_decisions`, and `notifications` rows are
never deleted — only archived or marked (e.g. `status='expired'`,
`pnl_basis_invalid=1`). Enforced at four layers, not just doctrine text:
(1) this rule; (2) `BEFORE DELETE` triggers on all eight tables, installed
by `setup_db.py` with a startup assertion that fails loud if any is
missing; (3) `engine/db_safety.py`'s connection authorizer (defense-in-
depth, also blocks `DROP TABLE` on these eight, which no trigger can
intercept — opt-in via `guarded_connect()`, not yet wired into every
existing connection helper); (4) `tests/test_rule1_delete_guard.py`,
wired into `.githooks/pre-commit`, proving a DELETE raises on every real
commit. UPDATE is not gated by any of this — see the UPDATE-path
inventory in `data/reports/relay/relay_2026-09-09_overnight_session.md`
for a future scoped session on that side.

**Status detail per DB (HM-OLLIETRADES-FOLDER-DISPOSITION, 2026-07-06):**
- `trader.db` — live, active, main.py-resident. The canonical DB.
- `arena.db` (`data/arena.db`) — 0 bytes, deprecated. Kept only because the
  sacred-data doctrine is "never delete," not "never let empty things exist."
- `tractor.db` — **archived-frozen**, not live. Lived at
  `~/ollietrades/tractor_beam/tractor.db` (last written 2026-04-14; the
  `tractor_beam` process has been dead since 2026-04-17, pid 8272 not
  running). The live Tractor Beam tiebreaker functionality is **in-repo**
  today (`engine/strategies.py`, `engine/crew_scanner.py`, `engine/
  phaser_lock.py`, `engine/reveille.py`) — this old DB is historical record
  only. `~/ollietrades` itself was archive-renamed to
  `~/ollietrades_archived_2026-07-06` (nothing deleted); a pre-rename backup
  tarball is checksummed on Ollie at `~/bigmac-backups/archive/
  ollietrades_final_2026-07-06.tar.gz`. `tractor.db`'s nightly offhost
  replication (`scripts/offhost_backup.sh`) and remote integrity-check both
  stopped treating it as a live target the same day — see that script's
  header comment for detail.
- `alpha_signals.db` / `backtest.db` (`data/`) — live, added to this list
  2026-07-06 (previously undocumented despite being real, non-trivial DBs).

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

### Future considered epic: submit-time manual-halt gate (NOT built)
Speculative/unbuilt design archived to [`docs/CLAUDE-archive-2026-05.md`](docs/CLAUDE-archive-2026-05.md).

## Dashboard Rules
- **Corrected 2026-07-05 (HM-DESK-SCOPE):** Dashboard root (`/`, port 8080) now
  serves `dashboard/static/bridge-v2.html` — `FileResponse(_static_dir + "/bridge-v2.html")`
  (`dashboard/app.py:13708`, HM-DIRECTIVE-2026-07-01 Deck3 #18, Admiral-approved
  2026-07-02). `dashboard/static/index.html` (v1, full engineering console) moved
  to `/classic`, per TWO-TIER BRIDGE DOCTRINE below — NOT retired. The prior
  2026-05-08 verification of `index.html`-at-root is superseded by this move;
  `docs/DASHBOARD_DOCTRINE_2026-05-08.md` is historical, not current. The Vite
  tree at `dashboard/frontend/` remains unwired experimental code — its `dist/`
  is still never mounted.
- ALL dashboard edits target `bridge-v2.html` (root) or `index.html` (`/classic`)
  per which tier you're changing — do not create new HTML files unless
  explicitly asked.
- `main.py` is the entry point; it imports `from dashboard.app import app` and
  runs uvicorn on 8080.

## Network Bindings
Port/host binding reference moved to [`docs/runbooks/network-bindings.md`](docs/runbooks/network-bindings.md).

## RAM Discipline
Post-MSI-migration RAM discipline moved to [`docs/runbooks/ram-discipline.md`](docs/runbooks/ram-discipline.md). Rule: respect model co-residency limits; don't overcommit VRAM.

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

## Scotty Model & Operating Conventions (HM-HELM, 2026-06-15)
How the Claude Code agent (Scotty) runs — distinct from the FREE-models fleet doctrine above.
- **Model routing:** working default = **Sonnet 4.6** (`model: sonnet` in `~/.claude/settings.json`).
  Opus is NOT faster and costs ~1.7x Sonnet / ~5x Haiku, and Sonnet ≈ Opus on directive execution
  (SWE-bench ~79.6 vs ~80.8). **Opus reserved for directives XO tags `OPUS`** in the ticket header
  (architectural / open-ended). Routine execution = Sonnet. Switch per-session with `/model opus`.
  (Deferred: mechanical subagents — research/verify — should be spawned `model: haiku`.)
- **Plan mode for AD-HOC asks:** for non-directive/off-script requests, propose before acting
  (the "## Workflow" propose-first rule). Directives are already the plan — execute them directly.
- **Long runs go `run_in_background`:** training, backtests, embeds, full backtests — never block
  the foreground; poll/notify on completion.
- **Permissions:** `Write`/`Edit` are scoped to `~/autonomous-trader/**`, `~/.claude/**`, `/tmp/**`
  (no blanket `Write(*)`); `Bash`/`Read` stay broad (HM-SHIELDS guards commands). Out-of-scope
  writes are classifier-screened, not pre-approved. Revert = restore `Write(*)`.

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

## Cron Edit Safety Rule (added 2026-09-09, HM-CRON-EMPTY-PIPE-INCIDENT)
**Never edit the live crontab with `crontab -l | ... | crontab -`.** If the
middle command fails or produces no output (a broken `sed` pattern, a bad
quote, a line-wrap mangling a pasted multi-line command), the pipe still
succeeds end-to-end and `crontab -` installs an **empty crontab** — silently
deleting every job (trader keepalive, watchdog, backups, health checks, all
of it). This happened for real 2026-09-09 during a routine rotate_logs.sh
schedule change: a wrapped/mangled `sed` command errored with no stdout, and
the empty output got installed as the new crontab, wiping all 170 lines.
Recovered from a pre-edit backup file that happened to exist.

**Required pattern for any cron edit, no exceptions:**
1. Dump the live crontab to a file: `crontab -l > /path/to/backup.txt`
2. Copy it to a working file and `sed`/edit **the file**, not a pipe:
   `cp backup.txt new.txt && sed -i '' 's|...|...|' new.txt`
3. `diff backup.txt new.txt` — confirm only the intended line(s) changed.
4. Count-guard: `wc -l backup.txt new.txt` — line counts must match (or
   differ by exactly the expected added/removed lines). A big unexplained
   drop means something ate the file — stop, don't install.
5. Only then: `crontab new.txt` (a file argument, never a bare `crontab -`
   fed by a pipe whose upstream could silently fail empty).

Keep the backup file until the new crontab is confirmed live and correct.

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

**UPDATE 2026-07-10 — NOT deferred, already shipped for three services.**
Found live during a departure-hardening pre-flight test (kill-and-verify):
`/Library/LaunchDaemons/` has real, working, system-domain LaunchDaemons —
`com.trademinds.cloudflared.plist` (dated 2026-06-11, `KeepAlive=true`,
`RunAtLoad=true`), `com.trademinds.swingdesk.plist` (2026-06-17, same
pattern, its own comment says "Replaces the @reboot cron — REMOVE that
cron so both don't fight for port 8889" — confirmed done, no swingdesk
`@reboot` line exists), and `com.trademinds.statuspage.plist` (2026-07-05,
matches the note already in `scripts/status_page_restart.sh`). System
LaunchDaemons run at boot independent of GUI login (no Aqua-session
requirement, unlike the `gui/$UID` LaunchAgents above) — this genuinely
is the apple-canonical fix, and it's live for these three. Verified
live: killed cloudflared directly, a replacement process appeared in the
**same second** (`KeepAlive` respawn, not any of this repo's own
watchdog/cron mechanisms — `watchdog.py`'s 60s sweep never even
registered the outage, it was already gone). Confirmed `bridge.
ollietrades.com` serving traffic again immediately after.

**Still on the cron+nohup fallback (no LaunchDaemon yet):** trader
(`main.py`), signal-center, `watchdog.py` itself. These still rely on
`HM-TRADER-KEEPALIVE`/`origin_healthcheck.sh` (5-min cron) and
`watchdog.py`'s own 60s-interval strike system for detection+recovery —
materially slower than the three LaunchDaemon-protected services above.

**Known residual cleanup, not yet done:** the old `@reboot
cloudflared_reboot_start.sh` cron line is still present and now
redundant (the LaunchDaemon handles this) — harmless (the script has a
`pgrep -x cloudflared` dup-guard) but worth removing for clarity in a
future pass.

`HM-CLOUDFLARED-LAUNCHDAEMON` in `drafts/HM-CLOSET-POWER-PASTE.md` ITEM 4
described this as a planned migration — it's done for these three
services; not yet attempted for trader/signal-center/watchdog.

### Scheduler-owned jobs
launchd plist archival + scheduler-ownership detail moved to [`docs/runbooks/reboot-lifecycle.md`](docs/runbooks/reboot-lifecycle.md).

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
large-file limit. Same disease: no independent watchdog. **SHIPPED 2026-05-29,
FIXED 2026-06-28: HM-PUSH-HEALTH-MONITOR** — `scripts/git_push_health_check.py`,
daily cron (`0 20 * * *`, NOT launchd), runs `git fetch` + `git rev-list --count
@{u}..HEAD` (current branch's own upstream, NOT hardcoded `origin/main` — the
2026-06-28 fix; the original `origin/main..HEAD` form false-alarmed on a fully-
pushed feature branch that legitimately diverges from `main`) and NTFYs
`ollietrades-admin` if local is >5 ahead of its own upstream OR if fetch itself
fails (can't-reach-origin is also push-health). No upstream configured → skips
silently (exit 0), not a false alarm either. Independent of the push pipeline
by construction. Two instances in one day → this is the program's recurring
blind spot: **build the monitor on a different mechanism than the thing it
watches, every time.**

### Restart-then-verify (HM-CONSOLE-INIT doctrine, 2026-05-13)
The "trader restart deferred until natural maintenance window" pattern creates
a window where buggy bytecode runs invisibly. Smoke-restart in a verify
window, OR push the restart synchronously with the commit. `py_compile` is
not enough — catches syntax errors but NOT undefined-name errors. Runtime
smoke required for any cross-module symbol change.

## Physical Power Infrastructure — Shelly Plugs (HM-SHELLY-PREP-V2, 2026-07-01)

Four Shelly Plug US, local-API only (cloud disabled), power-loss-restore
default **ON** (survives a real outage without a human present to switch
things back on).

| Plug     | IP            | Powers          | Role          | Watchdog |
|----------|---------------|-----------------|---------------|----------|
| bigmac   | 192.168.1.245 | bigmac Mac Mini | manual only   | none (see `HM-SHELLY-WATCHDOG`, post-trip) |
| olliemax | 192.168.1.246 | olliemax GPU box| manual only   | none (see `HM-SHELLY-WATCHDOG`, post-trip) |
| allo     | 192.168.1.244 | Allo router     | self-watchdog | `scripts/shelly_net_watchdog.js`, on-device |
| starlink | (RV/GL-MT3000 network) | Starlink Mini AC adapter | self-watchdog | `scripts/shelly_net_watchdog.js`, on-device (identical script to allo) |

**Wiring:**
- Home: `Wall → UPS → Shelly → device` (bigmac, olliemax, Allo router all
  ride through the UPS before the Shelly stage).
- RV: `Inverter → Shelly → Starlink AC adapter` (no UPS in the RV chain —
  the inverter is the buffer).

**Doctrine:**
- **Cloud disabled on all four** — local RPC/API only, no dependency on
  Shelly's cloud service being reachable.
- **Power-loss-restore = ON on all four** — after a real outage, everything
  comes back without a human present, rather than staying dark until someone
  manually flips it.
- **Watchdog auto-cycling is for network gear ONLY** (Allo router, Starlink
  Mini) — `scripts/shelly_net_watchdog.js` runs on-device on those two plugs
  only. Network gear has no stateful DB to corrupt; worst case of an
  auto-cycle is a clean reboot.
- **NEVER self-cycling on DB hosts.** bigmac and olliemax stay **manual-only**
  (`scripts/plug_cycle.sh {bigmac|olliemax} {status|off|on|cycle}`, a human
  runs it deliberately) — forcing power off a box mid-write risks corrupting
  `trader.db`/`signals.db`, and no on-device watchdog can safely quiesce a DB
  before cutting its own power. Auto-cycle for boxes is a real design problem
  (cross-box monitoring, conservative unresponsive-thresholds, accepting
  residual DB-crash risk mitigated by backups rather than pretending a clean
  quiesce is achievable) — sketched, not built, in `HM-SHELLY-WATCHDOG`
  (`docs/XO_BACKLOG.md`, POST-TRIP).
- `scripts/plug_cycle.sh` is zsh, not bash — macOS's stock `/bin/bash` is 3.2
  (no associative-array support), same reason `scripts/trader_restart.sh`
  is zsh. Has two safety rails: refuses off/cycle against the host it's
  running ON (hostname match — verified the real hostname of the bigmac box
  is `Steves-Mac-mini`, not literally "bigmac"), and requires `--confirm` for
  `allo` off/cycle since cutting the router likely cuts the script's own
  network path mid-command.

## HM-AM Scope (added 2026-05-12, HM-CLOSE-GAP W1.1)
Total Portfolio = **real-world net worth only** (Schwab + Webull + IBKR +
physical metals). EXCLUDES Alpaca paper trading book — separate
research/strategy-validation surface, must not co-mingle with real-world
capital reporting. The two-book bridge policy below governs how the books
communicate without mixing.

## Backtest Rule
- Always run ALL agents in backtests, never a subset
- Never cite in-sample (IS) numbers without the matching OOS figure

## Architecture: Two-Book Bridge Policy
Full policy moved to [`docs/architecture/two-book-bridge.md`](docs/architecture/two-book-bridge.md).
Summary: Option β two books; internal book + broker book; forwarding gates decide what routes to the broker. See doc for routing rules + naming discipline.

## SUPER_MAX Wave Program
Full W0–W4 program moved to [`docs/SUPER_MAX.md`](docs/SUPER_MAX.md).
Load-bearing: graduation gate + hard shadow boundary are documented there.

## Error Handling Posture
Full posture + Avoid/Prefer code examples moved to [`docs/DOCTRINE.md`](docs/DOCTRINE.md).
**Rule (load-bearing):** no silent catch; handle async errors explicitly; bounded I/O timeouts; degrade, don't crash.

## Ghost Tracking Architecture
Two-system ghost-tracking detail moved to [`docs/architecture/ghost-tracking.md`](docs/architecture/ghost-tracking.md). Do not consolidate the two systems (see doc).

## Fleet Roster
Full roster (active/bench/sniper/elder/metals/retired) moved to [`docs/FLEET-ROSTER.md`](docs/FLEET-ROSTER.md).
**Live counts are authoritative via the SessionStart primer (`data/trader.db` `ai_players`), not a static list here.**

**Dated waypoint (2026-07-01, HM-FULL-AUDIT + HM-CLOSEOUT Item 2):**
active/exit_only/halt/total drifted from a prior 22/6/45/73 baseline to
15/9/55/79. **RESOLVED — legitimate season churn, NOT a defect.** No halt_reason
anywhere in `ai_players` mentions drawdown/evaluator/error-loop/auto — every
transition traces to a deliberate, documented event:
- **2026-06-07** scorecard-driven cull → `full`: ollama-local, dayblade-0dte,
  ollama-deepseek (3 seats)
- **2026-06-19/20** Door 1 kill-gate cut (`docs/XO_BACKLOG.md` / kill gate
  G1-G4) → `full`: qwen3-8b-sonnet, qwen3-14b-pro, deepseek-7b-grok4,
  ollama-kimi, dalio-metals, ollama-coder (6 seats); → `exit_only`: navigator,
  ollie-auto, ollama-qwen3 (3 seats)
- **2026-06-24** Picard/Riker retirement (briefing + synthesis jobs retired,
  agents INSERT'd as new `full`/benched rows, not transitioned from active)
- **+3 new active seats** since baseline: sell-the-news (06-06), archer
  (06-06), q-witness (06-07); **+1 new exit_only seat**: guardian-of-forever
  (06-12, inserted exit_only by design, never active)
No dated snapshot of the original 22/6/45/73 baseline was found in-repo, so the
exact day it was measured (and thus the precise "7 left active" figure) can't
be pinned further — but every observed transition above is fully accounted for
by known decisions. `HM-ORPHAN-SEATS` (11 `ai_players` seats referencing Ollama
models absent from olliemax) remains separately open — all 11 already sit
within the 55 `halt='full'` count, so they're dormant and not part of this
churn.

**Crontab baseline (2026-07-01):** 34 active lines (prior reference ~29/31).
+3 today: `scripts/db_snapshot.sh` (20:15 MST), `scripts/backup_freshness_check.sh`
(20:45 MST), `scripts/rotate_logs.sh` (weekly Sun 05:00 MST). `scripts/offhost_backup.sh`
rescheduled in place 20:00→20:30 MST (same line count, time changed) — see
HM-BACKUP-SPINE-2026-07-01 in `docs/XO_BACKLOG.md` for the backup-spine work
this baseline reflects.

**Updated 2026-07-06 evening:** live-counted `crontab -l` at **41 active
lines** — well past the 07-01 baseline of 34, more drift than just tonight's
2 additions (`scripts/hm_gex_daily_collect.py` at 13:05 AZ, `HM-GEX-COLLECTOR-
2026-07-06`, and `scripts/hm_ops_sentinel.py` at */5, `HM-OPS-SENTINEL-
2026-07-06` — the latter already present before tonight's session, not
newly added here). The gap between 34 and 41 minus these 2 (`iren_flip_watch.py`,
various backup/health cron additions through the week) hasn't been reconciled
line-by-line against the 07-01 list — flagging the real count is higher than
naive "34+2" math would suggest, rather than asserting a false-precision
36. A full re-audit is a separate task, not done as part of this one-liner.

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
Schema + data-plane detail moved to [`docs/data-planes.md`](docs/data-planes.md).

## Logging Sink Split (trader.log vs trader_error.log)
Detail moved to [`docs/runbooks/logging.md`](docs/runbooks/logging.md).

## Doctrine Lessons
Distilled sprint lessons moved to [`docs/DOCTRINE.md`](docs/DOCTRINE.md) (titles indexed there). Load on demand; not needed every session.

## Account State

- **HM-WEBULL-LIQUIDATED (2026-05-13)**: webull real-money account
  liquidated. Historical trades remain in `trades` table as record. Account
  state in `ai_players` set to `halt_mode='full'` with descriptive
  halt_reason. Positions table zeroed for webull. Any future query that
  iterates real-money accounts must exclude webull or check
  `halt_mode='active'`. Previous "Webull ~$6.6K = monitor only" note is
  superseded.

## Drift Catalog 2026-05-17
Historical drift snapshot archived to [`docs/CLAUDE-archive-2026-05.md`](docs/CLAUDE-archive-2026-05.md).

## Workflow
- Propose edits and ask for approval before applying.
- For multi-file changes, show the plan first, then apply incrementally.

## Fleet Lifecycle Doctrine (added 2026-08-29, HM-FLEET-LIFECYCLE)
Full doctrine + drift enforcement detail: [`docs/FLEET_LIFECYCLE.md`](docs/FLEET_LIFECYCLE.md).
**Rule (load-bearing):** every agent/job state change (retire, bench,
shadow, halt, revive) goes through `scripts/fleet_lifecycle.py <action>
<name> --reason "..."` — never a raw `UPDATE ai_players` or
`launchctl disable`/`enable` by hand. The tool atomically writes a dated
order doc, applies the live change, and records it in the
`fleet_lifecycle_ledger` table (which the sentinel reads to know what
*should* be running and to catch drift when it isn't). Built specifically
so a future "why is X off?" is one lookup (`scripts/fleet_lifecycle.py
status <name>`), not the multi-hour forensic reconstruction the
2026-07-22 stand-down required five weeks later. See the doctrine doc for
why: manual edits still work mechanically but produce no record and will
surface as a `sentinel_lifecycle_drift` finding on the next sentinel
cycle.

## Archive Convention
- Retired agents: keep code in `engine/` (muted via threshold), DO NOT delete.
- If file must be moved, use `agents/_archive/` with date suffix.
- Document retirement reason + rehab path in this file, not just commit message.
- Supports the "iterate to the next Top 4" feedback loop — no known-good code
  is lost.

### Sulu DayBlade persona — RETIRED 2026-07-04 (HM-AGENT-RULES-CONSOLIDATION)
`dayblade-sulu` carried two irreconcilable identities (AGENT-RULES-REVIEW-
2026-07-03.md Inconsistency #6): `engine/providers/base.py`'s persona said
intraday DayBlade (-3% hard stop, 15min-2hr holds, close everything by
3:45 ET, no overnight) while `crew_specialization.py`'s CREW_MANIFEST /
AGENT_STRATEGIES mandate says S6.3 Iron Condor King (21-45 DTE multi-week
spreads — the literal opposite). Admiral decision: Iron Condor King is
canonical. The DayBlade persona text is retired in-place inside
`base.py` (commented block immediately above the live `"dayblade-sulu"`
entry, not deleted) — restore only with explicit Admiral approval.

**Not done in this pass (ticketed, see `docs/XO_BACKLOG.md`):** the
"DayBlade" label and intraday-specific exemptions/assumptions are threaded
through ~15 other files (`main.py`'s EOD options sweep, `paper_trader.py`'s
sizing/circuit-breaker/long-only exemptions, `crew_scanner.py`,
`super_backtest_v4.py`, `weekend_backtest.py`, etc.) — some of that code may
already be functionally correct for an options/spread trader and just
mislabeled "DayBlade" from before the S6.3 pivot; some may not be. `Sulu` is
currently `halt_mode='exit_only'` (no new entries), so none of this is live-
executing today. A full sweep is a separate, larger effort than a persona-
text fix and needs its own review pass before touching behavior.

## Historical Archive
Sprint logs, drydock sessions, "Lessons Banked" full narratives, and one-time
state changes (HM-AN2.3 fire, HM-BK Phase 1 load, May 19-20 frontend window)
live in `docs/CLAUDE-archive-2026-05.md`. Doctrine that emerged from those
sessions was extracted and lives in the "Doctrine Lessons" section above.

## Verify before claiming
- Run the check and read its real output before saying "done"/"passed".
- Read the actual code/schema/DB before asserting a fact about it;
  a grep hit is not confirmation (e.g. "nomic" was really "economic").
- Honest abstention > false completion: say "unverified" rather than guess.
- On retraction, state what was wrong and why.

## Relay Doctrine (standing rule, 2026-07-10)
At the end of every completed work block or directive, **before declaring
done**: write `data/reports/relay/relay_<date>_<topic>.md` summarizing what
shipped, what's open, and live-verification results — then commit and push
it together with the work. Cloud Claude fetches these directly; no
terminal copy/paste handoff. Applies to every session, not just
P&L-reconciliation-style investigations. A "work block" is any unit of
work that ends in a commit — if there's nothing to commit, there's nothing
to relay.

## Question Relay Doctrine (standing rule, 2026-07-10)
Whenever about to ask the Admiral a decision question with options
(AskUserQuestion or equivalent — any point where the assistant would stop
and present choices): **first** write the full question and every option
verbatim to `data/reports/relay/QUESTION_<topic>.md`, commit and push it,
**then** show the question in-session and wait for the answer. Applies to
every such question, not just push/permission incidents. If the push
itself is blocked (e.g. the same permission classifier that triggered the
question in the first place), say so plainly when presenting the
question rather than silently skipping the relay step.

## 2026-06-24 Structural Changes (canon)

### Gamma grounding (commit c8c021d, DAY0-gamma-grounding)
- `engine/gamma_context.py` live — native GEX from Polygon (call wall / put
  wall / gamma flip / net GEX / regime label). Injected into War Room
  `generate_hot_take` and `_record_witness` context blocks.
- Stamps `gex_snapshots` table + parquet sidecar. Bridge shows walls/flip/
  regime live — supersedes the Picard weekly narrative layer.

### Cloudflare tunnel — 4 routes, all gated by bridge-allow CF Access
- Remote config v11. Routes: `bridge→:8080`, `signal→:9000`,
  `swingdesk→:8889`, `tour→:8088`.
- Policy: `bridge-allow` (3 emails: superapple@duck.com,
  supersteveav@gmail.com, bonstenv@gmail.com; 730h session).
- **⚠ TODO**: `signal.ollietrades.com` previously CF-Access-UNGATED (remote
  config predated Access app). Verify/enforce Access gate on signal separately.

### bigmac-local Ollama — RETIRED
- Fleet inference consolidated to Ollie Max (192.168.1.168:11434).
- Dead-service health probes removed from `main.py`, `dashboard/app.py`,
  `engine/fleet_auditor.py`. `OLLAMA_LOCAL_URL` / `BIGMAC_OLLAMA` refs gone.
- `config.py:OLLAMA_LOCAL_URL` constant retained for reference; do not reuse.

### Picard briefing job — RETIRED (agent retained, benched on-deck slot 1)
- `engine/picard_strategy.py` → `engine/_parked/picard_strategy_retired_2026-06-24.py`.
- `run_picard_briefing` scheduler removed from `main.py`.
- `picard_briefings` table and all data preserved. Picard agent INSERT'd into
  `ai_players` (halt_mode='full', crew_role='benched', on-deck slot 1).
- Briefing layer made redundant by live GEX/regime display on Bridge.

### Riker XO synthesis job — STOOD DOWN (agent retained, benched on-deck slot 2)
- `run_riker_synthesis` scheduler + `_riker_startup` removed from `main.py`.
- `riker_synthesis` removed from `_GATE_SOURCES` in `engine/consensus.py`
  (was display-only, non-blocking; removal prevents stale-gate noise).
- `rikers_log` table and all data preserved. Riker agent INSERT'd into
  `ai_players` (halt_mode='full', crew_role='benched', on-deck slot 2).
- Narrative chain (Picard→Riker) made redundant by Bridge live intel layer.

### FUTURE / parked (do not action until 30-day run stable)
- **AI helmet control layer** (LOW PRIORITY): heads-up gaze/voice/click
  command surface on the cockpit — offensive paper-exec, RULE #1 holds.
  North-star for cockpit design; build cockpit changes helmet-aware.
- **Gemini 3.5 Flash as 'Data'** horse in the model sleeve (model-upgrade
  pipeline). On-deck when helmet layer scoped.

## TWO-TIER BRIDGE DOCTRINE

- **bridge-v2** = lean daily-driver (display + P0 safety controls).
- **bridge v1** = full engineering console (backtest, screener, Greeks,
  learning, alerts, deep panels). NOT retired — intentional second tier.
- Both bridges are windows onto the SAME backend `/api/*` state. Neither
  caches or computes authoritative state locally. They must never disagree.
- Any safety control (kill-switch, halt, autopilot) on EITHER bridge acts
  on the real fleet and both reflect true state.
- v1 retirement is OFF the table under two-tier. Re-decide only if the
  deep panels migrate to v2 deliberately.

## ALPHA READ — STATE AS OF 2026-06-29 ~20:35
- Evaluator FIXED + LIVE (fill_price→price + acted-join timestamp norm).
  Draining 11k backlog at 200/cycle/30min — full ~20h out.
- FIRST SIGNAL (≈5% drained, DIRECTIONAL ONLY, do not bank):
  bk_avwap +7.89% avg_fwd_1d, bk_box +2.46%. Positive + plausible.
  TRUSTWORTHY read = after full drain, NOT at partial sample.
- acted_by_fleet structurally ~0 (2,179 obs tickers vs 16 traded/7d).
  Retrospective join is a DEAD END. Fix = emit-time 'acted' tagging
  (stamp obs when fleet trades it). FORWARD-ONLY build. QUEUED, not built.
- by_grade all null = pre-fix rows; A/B grades populate forward only.
- OPEN: (1) emit-time acted tagging  (2) measurement-health→ntfy RED
  thresholds  (3) stale "on next restart" copy in evaluator endpoint.
- Report card: Performance = D↑ (do NOT upgrade until full drain + clean read).

## CARRIER DOCTRINE
### Bridge-v2 as the Deployed Strike Group · NOW-Edge Action Layer
**Established 2026-06-29 · Operates under, and subordinate to, the Two-Tier Bridge Doctrine**

---

### I. THE FORCE STRUCTURE

**OllieTrades is the Navy.** The standing force — the fleet of agents, the scanners, the data sources, the doctrine, the deep engineering console (bridge v1), the entire apparatus that runs whether or not anyone is watching. It is built for endurance, depth, and measurement.

**Bridge-v2 is the deployed Carrier Strike Group.** Forward, light, fast. It does not duplicate the Navy — it projects it. Its job is to take *live contact* — an alert, a data hit, a NOW-edge moment — and make it legible, followable, and (when earned) actionable, in the window where the edge is still fresh.

The Navy wins wars by being everywhere, always. The Carrier wins engagements by being *present at the contact* with a complete picture and a fast decision loop. OT needs both. v1 is everywhere-always. v2 is present-at-contact.

---

### II. THE KILL CHAIN (THE BUILD LADDER)

The build maps to the US targeting cycle — **F2T2EA: Find, Fix, Track, Target, Engage, Assess.** Each rung delivers standalone value. You climb only as far as the mission and the data justify. v1 is untouched at every rung.

**Rung 1 — FIND · *The Actionable Alert (the crumb trail)***
An alert stops being a dead-end ping and becomes a **sensor contact**. It deep-links into v2, focused on the ticker that fired — source, timestamp, freshness, live chart. No action. Just: *follow the trail, see what needs seeing.*
Cost: near-zero — wiring, not logic. Value: transforms every alert from "something happened" into "here is the contact, look."

**Rung 2 — FIX / TRACK · *Context-on-Arrival (the sensor picture)***
The focused view pulls the surrounding NOW-edge automatically: the congress/insider hit, the volatility spike, the options flow, the news that triggered it, the fleet's current read. This is the **Combat Information Center** picture for one contact — everything you need to orient, assembled before you ask.
Value: the live moment becomes *legible*. The "why now" is answered on arrival.

**Rung 3 — TARGET · *Debate & Review in Place (the firing solution)***
From the contact view: query the war room, eyeball the chart, pull the scorecard. The decision surface, live, in the moment. The carrier builds a firing solution before it launches anything.
Value: decision-quality at contact speed — no hop to another console, no stale context.

**Rung 4 — ENGAGE → ASSESS · *The Paper Sortie (gated)***
For contacts that earn it: a one-tap **paper-only** fire from the alert, kill-switch-guarded, logged as its own distinct execution source. Then **Assess** — the sortie feeds straight back into the measurement loop.
The payoff: firing from the alert *stamps the observation at fire-time* — which IS the emit-time `acted` tagging that the retrospective join could never deliver. **The carrier doesn't just act on edge; it generates the acted-data the Navy needs to grade itself.** Rung 4 back-solves the measurement dead-end from the action side.

> **Value at every rung.** Even if Rung 4 never ships, Rungs 1–3 make OT vastly more actionable on their own. The action is the last 10%; the legibility is the 90%.

---

### III. CONTACT CLASSIFICATION (ALERT TIERING)

Not every alert is a contact. The tier decides whether an alert *tells* you something or *invites* you to a contact — this is what keeps the carrier light instead of drowning you in pings.

- **INFORMATIONAL** (system status, measurement-health RED, infra) → report only. No trail, no carrier treatment.
- **ACTIONABLE** (bk_avwap / UHURA / congress / insider / options-flow / volatility contacts) → full crumb trail. These get the kill chain.

Only contacts where genuine NOW-edge exists earn a sortie path.

---

### IV. RULES OF ENGAGEMENT (NON-NEGOTIABLE)

1. **RULE #1 holds absolutely — paper only.** The carrier fires *paper* sorties. The real book stays Navy-side, on the sidelines, until measured edge earns deployment. Schwab remains read-only.
2. **Kill-switch guards every sortie path.** No engage rung exists without the emergency stop already wired (it is — P0 controls live on v2).
3. **v1 is untouched.** The Navy's engineering console keeps all its depth. Two-tier doctrine governs: both bridges are windows onto the *same* backend truth — no bifurcation, ever.
4. **Slow is smooth.** Climb the ladder one rung at a time. Each rung proves out before the next is built.

---

### V. DEPLOYMENT AUTHORITY (THE GATE)

**Rungs 1–3 build freely** — they carry zero execution risk and pure actionability upside.

**Rung 4 (ENGAGE) is GATED on the alpha read.** A source earns a carrier sortie path only when **full-drain forward-return data proves it carries edge that holds.** No source fires on a partial sample. (The +7.89% bk_avwap first-read is a 5%-drained directional signal — explicitly *not* deployment authority.)

**The alpha instrument is the deployment authority.** The Navy measures; the measurement clears the carrier to launch. Action waits for proof — that is the entire reason the real book is on the sidelines, expressed as a build rule.

---

### VI. ONE-LINE STATEMENT OF INTENT

*OllieTrades is the standing fleet. Bridge-v2 is the deployed carrier. Alerts are contacts. The kill chain turns a contact into a legible, reviewable, and — when the data has earned it — actionable paper sortie, in the window the edge is still live. The Navy proves the edge; the carrier projects it.*


## Ollama Model Aliases (2026-08-25, updated 2026-08-27)

On bigmac, the ollama tags `plutus-v1`, `ministral-3:3b`, `qwen3:4b` (as of
2026-08-27), and `qwen2.5-coder:7b` (manually aliased ~2026-08-27 07:50,
`ollama cp qwen3:8b qwen2.5-coder:7b`, to unblock `ollama-coder`/Data,
which had no model pulled at all before this) are **aliases of `qwen3:8b`**
(all five share model ID `500a1f067a9f`) — they are not distinct models.
This was discovered 2026-08-25 and causes model-swap thrashing on the 16GB
unified-memory budget when different logical "providers" that expect
different models actually round-trip the same weights in and out of VRAM.
**STILL LIVE as of 2026-09-10 — REVISIT-BY: 2026-09-17.** The original
"void until Friday, 2026-09-04" framing below is a dead-man's-switch that
already tripped once (same class as HM-OPS-SENTINEL-DOC-REVISIT-2026-09-10's
`situation_report.py`/`ollama_prewarm.sh` finding, `docs/XO_BACKLOG.md`):
2026-09-04 passed with the aliasing fully intact, and nobody re-checked
until this date. **Re-verified live 2026-09-10 by digest** (not just
model size) against olliemax's `/api/tags`: `plutus-v1`,
`plutus-v1:latest`, `ministral-3:3b`, `qwen3:4b`, and `qwen2.5-coder:7b`
all still share digest `f112024b4d65a1ec6a84...` — byte-identical to
`qwen3:8b`. Per-provider War Room attribution stays **void, ongoing, no
end date** until a fresh digest check shows otherwise — a win/loss
attributed to "plutus-v1", "ministral-3:3b", "qwen3:4b", or
"qwen2.5-coder:7b" is still really qwen3:8b under a different name. Only
`ollama-plutus` (McCoy) is currently `halt_mode='active'` among the
alias-affected `ai_players` seats — everything else on this list is
`full`. **One concrete consequence found 2026-09-10:** McCoy's only trade
on/after the 2026-07-19 alias-era boundary (`relay_2026-09-09_plutus-v1-
archaeology-and-season-dsr.md`'s "Alias-era flag" section) is the single
row dated 2026-09-03 in `trades` — genuinely alias-era-contaminated,
unlike Season 6's full trade range (predates 07-19, already cleared by
that relay doc). Season 6 needed no correction; this one row does, if
McCoy's Season 7 scorecard cites it as real Plutus-v1 performance.

The real finance-tuned model is preserved separately as `0xroyce/plutus`
(a distinct ID, family llama, digest `0bf0c307b6a4a673f8ee...`) — that is
the one to reference for actual Plutus-branded finance-model behavior, not
`plutus-v1`. The restored fine-tune itself is `plutus-v1-real` (digest
`06148c3401a6d74bab0c...`, family qwen2) — also genuinely distinct, see
`docs/XO_PLAN_2026-09.md` Phase 2 arm 5.

**Downstream fix, DO NOT ACTION UNTIL A FRESH DIGEST CHECK CONFIRMS
UN-ALIASING — a target date alone is not sufficient evidence, see above:**
`engine/providers/ollama_provider.py`'s `_QWEN3_ALIAS_MODEL_IDS` set
(`{"plutus-v1", "plutus-v1:latest", "ministral-3:3b", "qwen2.5-coder:7b"}`)
extends the qwen3 thinking-mode suppression (`payload["think"] = False`)
to cover these aliases, since War Room/McCoy/Data calls under these names
route to the same qwen3:8b weights and would otherwise leak `<think>`
tokens. **Once the roster gets real, distinct models back** (confirmed by
digest, not by calendar), `plutus-v1` becomes a Llama-based model and
`qwen2.5-coder:7b` becomes the real (non-thinking) Qwen2.5-Coder — sending
`think:false` to a non-thinking model can error on some Ollama versions.
Shrink `_QWEN3_ALIAS_MODEL_IDS` back down (or remove it) at that point —
`qwen2.5-coder:7b` in particular MUST come out once the real coder model
is pulled, since it was never a thinking model to begin with.

## THREE distinct "v1" Plutus models — confirm digest before any seat change (2026-09-10)

`plutus-v1` is heavily overloaded on olliemax. As of 2026-09-10 there are
**three separate models with "v1" in the name**, and seating the wrong one
puts an unbenchmarked model into production. Confirmed live via
`/api/tags` (digest is authoritative — size alone is not, see the alias
section above for why):

| Tag | Digest | Family / size | What it actually is |
|---|---|---|---|
| `plutus-v1:latest` | `f112024b4d65a1ec6a84...` | qwen3, 8.2B | The qwen3:8b alias (see above) — what McCoy (`ollama-plutus`) runs **today**. NOT a Plutus fine-tune at all. |
| `plutus-v1-real:latest` | `06148c3401a6d74bab0c...` | qwen2, 7.6B, size 4683075467 | The May build recovered from the X9 salvage 2026-09-09 — the restored HM-PLUTUS-V5-WIN checkpoint (`docs/XO_PLAN_2026-09.md` Phase 2 arm 5). |
| `plutus-v1-may27:latest` | `d413dbe9839060cea51f...` | qwen2, 7.6B, size 4683075306 | A **different** May build — the one June's evaluation actually benchmarked. Confirmed genuinely distinct from `plutus-v1-real` by both digest and byte size despite matching family/param count. |

**Before any seat change (config.py, `ai_players.model_id`, or a bakeoff
arm) that names anything starting `plutus-v1`, re-confirm which digest is
meant** — do not assume from the tag name alone, and do not trust a
target date or a prior comment (see the alias section above for why that
already went wrong once). Full history and additional digests: `~/
modelworks/CLAUDE.md` on olliemax (not in this repo — a separate working
tree on that host).

**June's evaluation verdicts, corrected 2026-09-10:**
- **"v6 tied v1" is withdrawn.** That tie came from `scripts/plutus_v6/
  bakeoff_judge.py`'s verdict checker substantially just matching the
  words "stop loss" in both critiques, not a real quality judgment.
- **"v1 beat v6-eval 98-80" is marked not-reproduced, not wrong.** It came
  from the LLM judge (`bakeoff_judge.py`/`bakeoff_gen.py`, both repointed
  off bigmac-local Ollama tonight, see the hardcoded-host-sweep relay
  report) — fresh samples score 87-91, not 98-80. The original number
  isn't proven false, just not reproducible from a fresh run; don't cite
  98-80 as a settled result either direction.

## Ollama Service: com.ollama.serve is the ONLY real service (2026-08-27)

**Ground truth, confirmed live 2026-08-27:** the actual Ollama server on
bigmac is the system LaunchDaemon **`com.ollama.serve`**
(`/Library/LaunchDaemons/com.ollama.serve.plist`, root-owned, dated
2026-07-01). It always wins port 11434 over Homebrew's
`homebrew.mxcl.ollama` LaunchAgent, because a system LaunchDaemon starts
before login and respawns faster on `KeepAlive`. **This fact was lost once
on 2026-07-01 (when `com.ollama.serve` was created) and cost three days
(2026-08-25 → 2026-08-27) of editing `homebrew.mxcl.ollama`'s plist — an
inert config that never actually served a single request.** Any future
Ollama tuning MUST target `com.ollama.serve`, not the Homebrew plist. This
also obsoletes the "bigmac-local Ollama — RETIRED" note earlier in this
file (2026-06-24) insofar as it implied no local bigmac Ollama serving
happens — `com.ollama.serve` has been serving locally on bigmac the whole
time, independent of that retirement decision about fleet routing.

- Plist: `/Library/LaunchDaemons/com.ollama.serve.plist`
- Backup: `com.ollama.serve.plist.bak-20260827`
- Full `EnvironmentVariables` set as of 2026-08-27: `OLLAMA_KEEP_ALIVE=-1`,
  `OLLAMA_CONTEXT_LENGTH=16384`, `OLLAMA_MODELS=/Users/bigmac/.ollama/models`,
  `OLLAMA_HOST=127.0.0.1:11434`, `OLLAMA_MAX_LOADED_MODELS=1`,
  `OLLAMA_FLASH_ATTENTION=1`, `OLLAMA_KV_CACHE_TYPE=q8_0`.
- `homebrew.mxcl.ollama` was disabled 2026-08-27 (`launchctl bootout` +
  `brew services stop ollama`) specifically so it stops crash-looping
  against a port it can never win — it is not a live competitor anymore,
  but if it's ever re-enabled it will lose the race again, silently.
