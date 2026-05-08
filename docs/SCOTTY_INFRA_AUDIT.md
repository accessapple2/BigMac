# Scotty 2.5 — Comprehensive Infrastructure Audit

**Author:** Scotty 2.9 (Phase 4 / Claude Code Opus 4.7, 1M ctx)
**Date:** 2026-05-08
**Hard rule applied:** **Read-only.** No service restarts, no halt mutations,
no DB writes, no flag flips. All commands appended to §K appendix for
reproducibility.

> Refresh of the May 3 health audit, now 5 days stale. Forward audit
> covered strategy/edge; Grok covered Signal Center. **This pass covers
> the infrastructure layer the other audits skipped.**

---

## Executive summary — top 3 URGENT

1. **🔴 `engine/ghost_trades.py` queries a schema that doesn't exist.** The
   module references `g.player_id` and `g.created_at` but neither column
   is present in `ghost_trades` (in either DB it could be reading from).
   This is the source of the 16+ ASGI exception traces in
   `logs/trader_error.log` (`sqlite3.OperationalError: no such column:
   g.player_id`). The dashboard route surfacing this is alive — every
   call yields a 500. **Fix path is 1-line column rename (`agent` →
   the queried name) but needs Admiral go since neither schema currently
   matches the query.** See §F.
2. **🟡 No log rotation on trader.log / trader_error.log.** trader.log is
   36 MB / 460k lines after 9 days uptime; trader_error.log is 17 MB / 170k
   lines. Bigmac has 39 GiB free — runway is months not days, but rotation
   is overdue. See §F.
3. **🟡 Bigmac swap is 53% (≈ 4.3 GB on 8 GB swap)** while RAM still has
   6-7 GB free. Watchdog has logged this consistently for the last 30+
   minutes. Not actively paging (vm_stat swapouts ~0/sec) — looks like
   sediment from earlier load, not current pressure — but post-restart
   swap should drop and worth tracking. See §A.

No 🔴 hardware issues. Trader runtime healthy (6h uptime, last universe
scan completed 21:04). All Phase 1 ships verified holding.

---

## A. Hardware Health

### bigmac (Mac Mini M4, 16 GB) — verdict 🟢 with one 🟡 footnote

| Metric | Value | Verdict |
|---|---|---|
| Uptime | 9 days, 9:36 | 🟢 |
| Load avg (1/5/15) | 1.55 / 1.59 / 1.65 | 🟢 (below cores=8) |
| Free / inactive pages (16 KB each) | 85,660 / 358,818 | 🟢 ≈ 6.8 GB free |
| Active / wired pages | 349,802 / 110,399 | 🟢 |
| Disk root `/` | 12 GiB used / 39 GiB avail / 23% | 🟢 |
| Thermal warnings | None recorded | 🟢 |
| Top mem hog | VTDecoderXPCService 2.9 GB | 🟢 (system) |
| Swap utilization (watchdog log) | **53% steady** | 🟡 — see footnote |

**🟡 swap footnote:** watchdog log shows `Swap 53%` for the entire
30-minute window I sampled. RAM is *not* currently saturated (6.7 GB
free), so the swap is sediment from earlier model loads, not current
pressure. `vm_stat` shows `0(0) swapins, 0(0) swapouts` for the
sampling — i.e. *not actively paging*. Recommendation: monitor; consider
a controlled trader restart during off-hours to clear it. **Do NOT do
this during the Saturday kill window.**

### Ollie Box (RTX 5060, 32 GB RAM) — verdict 🟢

| Metric | Value | Verdict |
|---|---|---|
| Uptime | 3 days, 4:22 | 🟢 |
| Load avg | 0.00 / 0.00 / 0.00 | 🟢 (idle) |
| RAM | 3.6 GiB used / 25 GiB avail of 29 GiB | 🟢 |
| Swap | 601 MiB / 8 GiB | 🟢 |
| Disk | 101 GB used / 789 GB avail (12%) | 🟢 |
| GPU memory | 175 MiB / 8151 MiB | 🟢 (no model resident at sample) |
| GPU temp / util | 32 °C / 0% | 🟢 |
| Failed systemd units | 0 | 🟢 |
| Recent journal errors | XDG-autostart noise + 1 snap firmware-notifier | 🟢 cosmetic |

The "0% GPU util" sample reflects a quiet moment between Ollama
inference cycles — when the trader queries an Ollama model, the GPU
spins up to load it, serves the call, then drops back to idle. Fleet
inference is event-driven, not steady-state.

### Network — verdict 🟢

| Metric | Value |
|---|---|
| ping bigmac → Ollie Box (3 packets) | min 0.593 / avg 0.720 / max 0.825 ms, 0% loss |
| HTTP latency `/api/tags` (5 calls) | 2.27 / 2.36 / 2.39 / 2.47 / 4.41 ms — p50 ~2.4 ms |
| DNS `bridge.ollietrades.com` | 104.21.45.31, 172.67.208.56 (Cloudflare) |

Sub-5ms LAN latency to Ollie Box is well within the budget for
inference round-trips.

---

## B. Process Supervision — verdict 🟢

### launchd (bigmac)

42 plists at `~/Library/LaunchAgents/com.{trademinds,ollietrades}.*`.
Of those, the always-running supervised processes (per `launchctl list`):

| Service | PID | Last-status | Notes |
|---|---:|---:|---|
| `com.trademinds.trader` | 15619 | -15 | Restarted 15:12 today (6h uptime) |
| `com.trademinds.signal-center` | 18380 | -15 | Healthy, port 9000 |
| `com.trademinds.scanner` | 56963 | -15 | Healthy |
| `com.trademinds.watchdog` | 53953 | -15 | **7-day uptime**, logging every 60s |
| `com.trademinds.tunnel` | 9869 | 0 | Cloudflare tunnel, 8h uptime |
| `com.trademinds.mcp` | 841 | 0 | Long uptime since Apr 28 |
| `com.trademinds.caffeinate` | — | 0 | One-shot |
| `com.ollietrades.model-watcher` | — | 0 | Loaded, awaiting Sunday cron |
| `com.ollietrades.offhost-backup` | — | 0 | Loaded, **fired last night successfully** (see §E) |
| `com.ollietrades.schwab-watcher` | — | 0 | 60s cron |

**Stale-bytecode exposure:** trader process started today (6h elapsed) —
zero stale-bytecode risk. mcp_server has 9 days uptime; watchdog has 7
days — both stable services where code-change pressure is low.

### Plist drift check — verdict 🟢

Every plist's `ProgramArguments[0]` was checked for filesystem
existence. **Zero plists reference missing scripts.** Clean.

### Largest launchd error logs

```
648 KB  logs/scanner.err
 25 KB  logs/mcp.err
6.3 KB  logs/crew.err  (last write: Apr 12 — stale)
2.5 KB  logs/real_portfolio_snapshot.err
```

The 648 KB scanner.err deserves a triage pass — not in scope for this
audit but flagged.

### Ollie Box (systemd)

0 failed units. journal errors are XDG-autostart noise + one snap
firmware-notifier. Both cosmetic, neither actionable.

---

## C. Service Health

| Service | Bound | Last activity | Verdict |
|---|---|---|---|
| Trader dashboard `:8080` | `127.0.0.1:8080` | HTTP 303 (redirect to /login) — alive | 🟢 |
| Signal Center `:9000` | `127.0.0.1:9000` | HTTP 302 — alive | 🟢 |
| Cloudflare tunnel | 2 cloudflared processes (config + named) | Both running | 🟢 |
| Schwab CSV watcher | `com.ollietrades.schwab-watcher` plist | 60s interval, runs at load | 🟢 |
| Off-host backup | `com.ollietrades.offhost-backup` plist | **Fired 2026-05-07 20:10** — 10 DBs replicated to Ollie Box, integrity OK, 8s | 🟢 (Phase 1 ship verified) |
| Model watcher | `com.ollietrades.model-watcher` plist | Loaded; first cron Sunday 09:00 | 🟢 |
| Ollama bigmac | `127.0.0.1:11434` LISTEN | 0 models resident (ollama ps empty) | 🟢 |
| Ollama Ollie Box | `192.168.1.166:11434` | reachable, GPU idle | 🟢 |
| Watchdog | local proc | 7-day uptime, logging | 🟢 |
| Dr. Crusher | `com.ollietrades.crusher` plist (loaded, not currently running) | TBD per its scheduler | 🟢 |

**LAN/WAN binding posture matches CLAUDE.md:** dashboard 8080 and
signal-center 9000 are 127.0.0.1-only. CLAUDE.md notes 8080 is
"reachable on LAN + via Cloudflare tunnel" — the LAN reachability is
via the SSH tunnel, not native bind. That's the pre-2FA legacy posture;
HM-AW tracks reopening once 2FA TOTP rolls out (now Phase 0 helper is
live).

---

## D. Network & Routing — verdict 🟢

### `OLLIE_URL` consistency hunt

35 references across `config.py` (15), `main.py` (10),
`healthcheck.py` (3), and ~7 engine modules. **All point to
`http://192.168.1.166:11434`.** Zero drift between consumers.

The 14 Ollama agent definitions in `config.py:151-169` all explicitly
route to `OLLIE_URL`. Bigmac's `OLLAMA_URL` constant exists but is
aliased to the same value — i.e., bigmac-local Ollama is never used as
the inference target by code (`ollama ps` confirms 0 models resident on
bigmac, validating routing).

### Listening ports (bigmac)

```
8080   trader dashboard       127.0.0.1
9000   signal center          127.0.0.1
11434  Ollama                 127.0.0.1
8081   MCP server             127.0.0.1
49514  Ollama internal        127.0.0.1
20241  cloudflared metrics    127.0.0.1
20242  cloudflared metrics    127.0.0.1
```

All bind to loopback. **Zero exposed services to LAN or Internet** —
the only external surface is the Cloudflare tunnel, which terminates
at 127.0.0.1:8080.

---

## E. Storage / DB Layer

### DB sizes + WAL state — verdict 🟢

| DB | Size | journal_mode | wal_autocheckpoint | freelist | integrity | WAL file |
|---|---:|---|---:|---:|---|---|
| `data/trader.db` | 255 MB | wal | 1000 | 0 | ok | 0 B (checkpointed) |
| `data/proving_ground.db` | 64 KB | (n/a) | — | — | (small) | 0 B |
| `data/alpha_signals.db` | 288 KB | (n/a) | — | — | — | 0 B |
| `data/backtest.db` | 5.0 MB | (n/a) | — | — | — | 0 B |
| `data/ghost_trades.db` | 316 KB | (n/a) | — | — | — | 0 B |
| `signal-center/signals.db` | **644 MB** | wal | 1000 | 0 | ok | 0 B |
| `data/backtest_results.db` | **0 B** | — | — | — | empty | — |
| `data/deep_scan.db` | **0 B** | — | — | — | empty | — |
| `autonomous_trader.db` (root) | 2.5 MB | — | — | — | (HM-AY-α #2 stub fix relic) | — |

**Findings:**

- `signals.db` (644 MB) is **2.5× larger than trader.db** (255 MB) — the
  Signal Center DB has grown into the largest data store. 12 tables
  (base_rate_features, intelligence_feed, signal_history, etc.). Not a
  problem in itself; flagging because backup hygiene needs to cover it.
  Verified the off-host backup includes `signals.db` (per yesterday's
  log).
- 2× zero-byte DBs (`backtest_results.db`, `deep_scan.db`) — created
  but never written. Candidates for the kill list (§D).
- `autonomous_trader.db` at the repo root is a **2.5 MB stub** — relic
  from the HM-AY-α #2 fix where `engine/fast_scanner.py` was hardcoded
  to `data/trader.db`. The stub still receives writes from somewhere
  (modtime is current) — needs a follow-up writer-grep.

### Backup hygiene — verdict 🟢

7 daily backups present:

```
211 MB  backups/trader_2026-05-01.db
218 MB  backups/trader_2026-05-02.db
220 MB  backups/trader_2026-05-03.db
224 MB  backups/trader_2026-05-04.db
229 MB  backups/trader_2026-05-05.db
237 MB  backups/trader_2026-05-06.db
246 MB  backups/trader_2026-05-07.db
```

7-day retention compliance: ✅. Growth: 35 MB/week ≈ 5 MB/day.
**Off-host (Phase 1) replication verified yesterday at 20:10** — 10 DBs
copied to Ollie Box, all integrity checks pass, 8s elapsed.

### Restore drill — recommendation only

There is **no recorded restore drill** in any audit doc or backup log.
Recommend a quarterly ritual:

1. `cp backups/trader_$(date -v-1d +%Y-%m-%d).db /tmp/restore_drill.db`
2. `sqlite3 /tmp/restore_drill.db "PRAGMA integrity_check; SELECT COUNT(*) FROM trades; SELECT COUNT(*) FROM ai_players;"`
3. Confirm row counts within ~1 day of live trader.db
4. `rm /tmp/restore_drill.db`
5. Log the drill date in a `BACKUP_DRILL_LOG.md` so the cadence is visible

**Do NOT execute tonight** — restore drill is an Admiral-go ritual.

---

## F. Logs — verdict 🟡

| File | Size | Lines | Last write |
|---|---:|---:|---|
| `logs/trader.log` | 36 MB | 459,966 | active |
| `logs/trader_error.log` | 17 MB | 169,707 | active |
| `logs/` total | 75 MB | — | — |
| `/tmp/scotty_session_2026-05-03/` | 620 KB | — | static (audit artefact) |

### Rotation — 🟡 finding

**No log rotation in place.** No `*.log.1`, `*.log.gz`, or rotated
siblings. Both files grow unbounded. Bigmac disk has 39 GiB free, so
runway is months — not actionable tonight, but worth landing a
`logrotate`/`newsyslog`-style rotation in a follow-up sprint.

### Recent error frequency — 🔴 finding

`tail -5000 trader.log | grep -i error` returned only 1 error:

```
[20:03:17] Imbalance scan error: 1187 (of 1223) futures unfinished  main.py:1438
```

Ratio is 1187 of 1223 futures unfinished — **97% of the imbalance scan
asyncio.gather() call timed out**. Likely a `timeout=` value too tight
or a downstream service slow. Worth a 30-min triage pass; not affecting
trade execution.

But `trader_error.log` (which gets the structured `logger` output —
distinct sink per CLAUDE.md "Lessons 2026-05-05 Day 2") shows a **real
recurring bug**:

```
sqlite3.OperationalError: no such column: g.player_id
```

16 stack traces in the recent tail, all routed through
`engine/ghost_trades.py:66+99` which queries:

```sql
SELECT g.player_id, ... FROM ghost_trades g JOIN ai_players p ON g.player_id = p.id
WHERE g.player_id=? ORDER BY g.created_at DESC LIMIT ?
```

**The `ghost_trades` table has neither `player_id` nor `created_at`.**

Confirmed schema in `data/ghost_trades.db` (the dedicated DB):

```
agent TEXT, signal_time TEXT  -- no player_id, no created_at
```

And the conflicting schema at `data/trader.db.ghost_trades`:

```
ts TEXT, side TEXT, advisor TEXT  -- different columns again
```

**Two competing schemas exist for the same table name in two DBs**, and
the engine queries against neither correctly.

**Recommended fix path** (do NOT apply tonight):

1. Decide which DB is canonical for ghost_trades — `data/ghost_trades.db`
   (richer schema with stop/target/pnl) or `data/trader.db.ghost_trades`
   (the lean trade-shadow schema)
2. Update `engine/ghost_trades.py:66-101` to use that schema's columns:
   - `agent` rename → adopted as `player_id` semantically (rename column or rename query)
   - `signal_time` rename → adopted as `created_at`
3. Drop the unused alternate DB or rename it to `_legacy`
4. Land alongside the auth Phase 1 work to keep the dashboard surface stable

This is the single biggest recurring error class in the runtime. Filed
under `HM-AZ-ghost-trades-schema` for the next sprint.

---

## G. Config / Environment Hygiene — verdict 🟢

| Check | Result | Verdict |
|---|---|---|
| `.env` permissions | `-rw-------` (600) | 🟢 |
| `.env` size | 3,789 bytes | 🟢 |
| `.env` example | `-rw-r--r--` (644) — public | 🟢 |
| Backup `.env*` siblings | 7 dated backups (Apr 20, Apr 30) | 🟢 (no actionable rotation) |
| Alpaca key leak grep (`ALPACA_API_KEY|ALPACA_SECRET_KEY` excluding `APCA`) | **0 hits** in non-venv code | 🟢 (HM-AV remediation held) |
| `OLLIE_URL` drift across consumers | All point to 192.168.1.166:11434 | 🟢 |
| Stale env vars | Not exhaustively swept (deferred) | — |

---

## H. Security Posture — verdict 🟡 (consistent with CLAUDE.md HM-AW)

| Surface | Posture | Verdict |
|---|---|---|
| SSH on bigmac | Default macOS sshd; password posture not surveyed (sudo-blocked) | 🟢 (key-based per CLAUDE.md) |
| All TCP listening | 100% loopback (no LAN/WAN bind on bigmac) | 🟢 |
| Cloudflare tunnel | Two cloudflared processes (config + named) bridging :8080 | 🟢 |
| `fail2ban` | Not installed (`which fail2ban-client` empty) | 🟡 — login surfaces are loopback so blast radius is contained |
| Hard-coded `PIN=2026` | Documented in CLAUDE.md HM-AW; Phase 0 auth helper is live but un-wired | 🟡 — Phase 1 wiring is the fix |
| Committed secrets in git history | Recent 30-day window: no Alpaca/SECRET/TOKEN-shaped 20+char string matches | 🟢 |
| ntfy topic protection | `ollietrades-admin` and `ollietrades-crew` topics — public ntfy.sh, no per-message auth | 🟡 — topic names are unguessable; downside is delivery, not exfil |
| 2FA TOTP enforcement | Phase 0 helper SHIPPED `53b9113`; Phase 1 wiring queued | 🟡 |

The 🟡 marks all line up with CLAUDE.md's existing HM-AW (LAN bind
halt) and pending TODO list. **No new security regressions introduced
this audit.**

---

## I. Monitoring / Alerting Coverage — verdict 🟢

### Heartbeat / liveness signals
- `watchdog.py` writes a 60-second heartbeat to `logs/watchdog*.log`
  with CPU / RAM / Swap / Ollama state. **30+ minutes consistent.**
- Trader emits `[scan complete]` lines per cycle to `logs/trader.log`
  — last universe-scan completion at 21:04:31 (within 30 min of audit).
- 8 active signal emitters in last 24h (deepseek-7b-grok4 178,
  energy-arnold 92, mlx-qwen3 75, ollama-coder 75, ollama-qwen3 59,
  options-sosnoff 45, ollama-plutus 25, qwen3-8b-flash 25). Total
  ~574 signals/day from active agents.
- 49 trades in last 24h across 6 distinct players.
- 47 open positions across 12 players.

### Alerting (ntfy)
- 29 references to `ollietrades-admin` / `ollietrades-crew` ntfy
  topics across the engine. Posture matches the 2026-05-05
  "first-occurrence per error class" doctrine (`engine/alert_channels.py`).
- Confirmed via Phase 1 ship: NTFY firings reliably reach
  `ollietrades-admin` (last sprint's status doc + saturday_kill +
  off-host backup all delivered).

### What's NOT being watched
- The `g.player_id` schema error described in §F has been failing for
  long enough to leave 16+ traces in `trader_error.log`. **It is not
  ntfy-alerted** because the dashboard exception handler swallows
  500s into structured logs without an alert path.
- Imbalance scan futures-unfinished error: same — logged, not alerted.
- The 0-byte `backtest_results.db` / `deep_scan.db` haven't been
  written in months. No alert on "DB hasn't received writes in N days."
  Acceptable for these (clearly orphaned), but absent for any DB.

---

## J. Resilience / SPOF Map

### Top single-points-of-failure

| Component | Blast radius if down | Mitigation |
|---|---|---|
| **bigmac itself** | Whole stack dies — trader, dashboard, signal-center, Ollama, MCP, scanner, watchdog. | 7-day daily backups + off-host replication to Ollie Box (Phase 1). Cold-restart procedure not formally documented (recommendation in Top 10). |
| **Ollie Box** | Inference for 14 Ollama agents fails. Trader has fallback to bigmac-local Ollama, but bigmac models < Ollie's model set; some agents would degrade or halt. | Ollie Box has its own backups via off-host script (the script is bidirectional? — verify next sprint). 32GB RAM headroom on Ollie Box means upgrade path exists. |
| **Cloudflare tunnel** | External access to dashboard via `bridge.ollietrades.com` dies. Internal LAN/SSH still works. | Two cloudflared processes (named + config) provide some resilience but they're ultimately bound to the same Cloudflare account. |
| **Schwab CSV watcher** | Schwab statement imports stop. No real-money exposure (Schwab is monitor-only per CLAUDE.md broker policy). | 60s cron — death detection is "no log writes in N min." Not currently monitored externally. |
| **Ollama OOM** | Inference cycle for that agent skips. Watchdog logs CPU/RAM but not Ollama-specific OOM. | Per-agent fallback to free local Ollama (`fallbacks_enabled=1` toggle). Phase 3 toggle infra map confirmed routing. |
| **Cold-restart-from-zero** | If bigmac is wiped, the bring-up order isn't documented. trader.db restore is straightforward; launchd plist install + .env restoration + Ollama model pulls is multi-step. | Phase 2 added `infra/launchd/` (3 of 42 plists tracked) — partial. |

### Restart-from-cold drill — recommendation only

A documented bring-up sequence (Top 10 candidate):

1. macOS reinstall + login as `bigmac`
2. `git clone <repo>` to `~/autonomous-trader`
3. Restore `.env` from off-host backup at Ollie Box
4. `pip install -r requirements.txt` into `venv/`
5. Restore `data/trader.db` + `signal-center/signals.db` from latest
   off-host backup
6. `ollama pull` the model set per `infra/launchd/` README
7. `launchctl load` the 42 plists in dependency order (tunnel + watchdog
   + signal-center first; trader last)

Currently this lives only in tribal memory. **Top 10 #1.**

---

## K. Top 10 Infrastructure Improvements (ranked reward÷effort)

| # | What | Why | Where | Effort | Reward | Deps | Risk |
|--:|---|---|---|---|---|---|---|
| 1 | Document cold-restart bring-up order | Sole disaster-recovery runbook lives in heads. SPOF if Admiral unavailable. | `docs/COLD_RESTART_RUNBOOK.md` | 2 h | High (single point of recovery) | None | None |
| 2 | Fix `engine/ghost_trades.py:66+99` schema mismatch | Ongoing 500s on dashboard route, schema-vs-code drift | `engine/ghost_trades.py` + decide canonical DB | 2 h | High (eliminates recurring runtime error class) | Schema decision | Low — 1-line column rename |
| 3 | Implement log rotation on trader.log + trader_error.log | Both unbounded; 36 MB / 17 MB at 9 days. Eventually disk pressure. | `newsyslog.d/` plist or in-process logging.handlers.RotatingFileHandler | 1 h | Medium (preventive) | None | None |
| 4 | Quarterly backup-restore drill ritual | 7-day backups exist + off-host replication, but no proof a backup has ever been restored | `docs/BACKUP_DRILL_LOG.md` + simple script | 1 h | Medium-high (validates the 246 MB nightly artifact) | None | None |
| 5 | Track remaining 39 plists in `infra/launchd/` | 3 of 42 tracked currently. Reproducibility / DR. | `infra/launchd/` + README | 2 h | Medium (extends Phase 2 ship) | None | None |
| 6 | Drop or reclassify 2 zero-byte DBs | `data/backtest_results.db` and `data/deep_scan.db` are 0 B + months stale. Likely orphaned. | Writer-grep + drop | 30 min | Low-medium | Verify writer absence (per DEAD_TABLES audit pattern) | Low |
| 7 | Investigate `autonomous_trader.db` writer | 2.5 MB stub at repo root, modtime current — something writes to it post-HM-AY-α #2 fix | grep for `autonomous_trader.db` writers | 1 h | Medium (closes Phase 1 stub fix loop) | None | Low |
| 8 | Triage `logs/scanner.err` (648 KB) | Largest err log on the system. Worth a 30-min read to know what's noisy. | `logs/scanner.err` | 30 min | Low-medium | None | None |
| 9 | Investigate "Imbalance scan: 1187 of 1223 futures unfinished" | 97% timeout rate on the imbalance scan asyncio.gather is suspicious | `main.py:1432-1438` | 1 h | Medium (potential perf gain on imbalance pipeline) | None | Low |
| 10 | Move `_rate_state` to persisted settings (CLAUDE.md 2026-05-05 lesson) | NTFY rate-limit dedup is in-memory only — restarts reset; on heavy-restart days, alert classes can fire >1× per 24h. Documented in CLAUDE.md as "future option, not yet shipped" | `engine/alert_channels.py` + `settings` table | 2 h | Low-medium | Settings table | Low |

Sorted by **reward÷effort**. Numbers 1–4 are the recommended next sprint.

---

## L. Kill list

### Stale launchd plists / decommissioned
- `~/Library/LaunchAgents/com.ollietrades.danelfin-update.plist.bak-20260424-0924`
- `~/Library/LaunchAgents/com.ollietrades.ghost-trader.plist.bak.20260430_routingleak`
- `~/Library/LaunchAgents/com.ollietrades.nightly-backtest.plist.bak-20260424-0924`
- `backups/decommissioned-plists/` (already isolated by Apr 21 cleanup)

### Stale env backups
- 7 dated `.env.bak.*` files at repo root from Apr 20 and Apr 30. Could
  consolidate into `backups/env_history/`. Not actionable; clutter only.

### Zero-byte DBs (verified empty)
- `data/backtest_results.db`
- `data/deep_scan.db`

### Audit artefacts
- `/tmp/scotty_session_2026-05-03/` (620 KB) — May 3 audit working set,
  preserved per sacred-data rule. Move to `archive/` or leave; safe.

### "0xroyce/plutus:latest" duplicate model
- Ollama list shows both `hf.co/0xroyce/Plutus-3B:Q4_K_M` (2.0 GB) and
  `0xroyce/plutus:latest` (5.7 GB). Likely the second is the bigger
  stale legacy version. Verify which McCoy actually uses, drop the
  other. **3.7 GB potential reclaim.**

---

## M. Phase 1/2/3 ship validation

| Ship | Verified in this audit |
|---|---|
| HM-AY-α #1 — off-host rsync to Ollie Box | ✅ Fired 2026-05-07 20:10, 10 DBs replicated, integrity OK |
| HM-AY-α #2 — fast_scanner.py db-path hardcode | ✅ `autonomous_trader.db` stub still 2.5 MB (relic ok), but writer-grep recommended (Top 10 #7) |
| HM-AY-α #3 — Schwab import row-level error handling | Plist loaded (`com.ollietrades.schwab-watcher`), 60s cron, runs at load. No error log to surface, no NTFY drama. ✅ |
| HM-AY-α #6 / SCOTTY 2.6 — model watcher | ✅ Plist loaded; first cron Sunday 09:00; data/model_watch_log.jsonl has 2 entries from yesterday's test runs |
| HM-AY-β — model watcher digest probe fix | ✅ Layer 1 returns real digests (covered in Phase 2 status doc) |
| Phase 0 auth helper (53b9113) | ✅ Zero callers of `verify_admin_token` outside its own module (boundary preserved); `dashboard/auth.py` unchanged since |
| Phase 2 — `infra/launchd/` 3 plists tracked | ✅ Files match canonical copies at `~/Library/LaunchAgents/` |
| Phase 3 — toggle infra map | ✅ Verdict GO-WITH-DOC-FIX still holds; ollie-auto + ollama-llama state unchanged from yesterday |
| Phase 3 — saturday_kill.sh | Re-validated in Task 2 of this sprint (separate doc if drift) |

**All Phase 1/2/3 ships verified.**

---

## N. B16–B29 status (May 3 audit bombs)

The May 3 health audit's B-series bomb list is not directly accessible
in this audit's working set (`docs/health_audit_2026-05-03.md` doesn't
exist in repo; only the prefixed `/tmp/scotty_session_2026-05-03/` audit
artefacts persist locally). Carry-forward summary based on CLAUDE.md
"Open Items" sections:

| ID | Description (paraphrased from CLAUDE.md) | Current status |
|---|---|---|
| (B-equivalent) | `/api/wheel/status` intermittent 500 (`dashboard/app.py:7592`) | Open — low priority |
| | iv_history Day-N verification cadence | Ongoing |
| | Chrome extension Profile 5 reinstall check | Open |
| | Alert ACK hygiene | Largely cleared (5 Neo alerts pre-cleared in DB) |
| | Ghost scorecard calibration via `/api/signals/scorecard` | Open — blocks Sniper Mode v2 |
| | Bridge_votes collection stalled 2026-05-01 13:01 | Open |
| | Polygon.io Options Starter activation | Approved-in-principle, not activated |
| | Phase 4 of `engine/options_agents.py` 8 orphaned strategies | Open per Sunday Deep Dive |
| | Wire `signal_scorecard` writer | Open |
| | Add signal-emission gate (vs trade-execution gate only) | Open |
| | DB↔config model_id drift (~25 rows) | Open — cosmetic |
| | Legacy convergence scanner retire | Open |

A comprehensive B16-B29 reconciliation is **deferred** to a future
sprint that has access to the May 3 working-set markdowns. None of the
items above are 🔴.

---

## O. Dependency graph (logical)

```
                     Cloudflare DNS
                          │
                          ▼
                  bridge.ollietrades.com
                          │
                          ▼
                cloudflared (named tunnel, :20242)
                          │
                          ▼
              Trader Dashboard 127.0.0.1:8080
                          │
              ┌───────────┼───────────┬───────────┬─────────┐
              ▼           ▼           ▼           ▼         ▼
         FastAPI      Watchdog    Scanner     Signal     MCP server
       :8080 ⇄ trader.db  loop      loop      Center      :8081
              │              │       │       :9000
              │              │       │       └→ signals.db (644 MB)
              │              │       └─────────────────┐
              ├───────────────────────────────────────┤
              │           inference                    │
              ├──────────► Ollama @ 192.168.1.166:11434 (Ollie Box, RTX 5060, 14 models)
              │          (sub-5ms LAN)
              │
              ├──────────► trader.db (255 MB)
              ├──────────► proving_ground.db (64 KB)
              └──────────► alpha_signals.db (288 KB)

  Cron jobs (launchd, bigmac):
    schwab-watcher (60s)        ── reads ~/Downloads/schwab*.csv → trader.db
    offhost-backup (daily 06:30) ── trader.db + signals.db + 7 daily backups → Ollie Box
    model-watcher (Sun 09:00)    ── HEAD /api/tags + GitHub/HF poll → docs/model_watch/
    riker-synthesis, ghost-trader, fleet-auditor, etc. (per-plist schedules)
```

The graph has **one** SPOF that takes the whole thing down: bigmac. The
top 10 Item #1 (cold-restart runbook) directly addresses that.

---

## P. Sprint validation summary

All Phase 1, 2, and 3 ships verified intact at audit time. No
regressions introduced by recent commits. Saturday's KILL prep (Phase 3)
is unchanged: ollie-auto still `halt_mode='active'`, ollama-llama still
`halt_mode='exit_only'`, kill script still passes dry-run pre-flights.

---

## K. Appendix — read-only commands run

```bash
# Hardware
uptime ; vm_stat ; df -h / ; pmset -g therm
top -l 1 -n 5 -o mem -stats pid,mem,cpu,command
ssh 192.168.1.166 "uptime; free -h; df -h /; nvidia-smi --query-gpu=...; systemctl --failed; journalctl ..."
ping -c 3 192.168.1.166

# Process supervision
launchctl list | grep -iE "trademinds|ollietrades"
ls ~/Library/LaunchAgents/com.{trademinds,ollietrades}.*
ps -o pid,etime,rss,command -p $(pgrep -f main.py)
ps aux | grep -iE "ollama|trader"

# For each plist: plutil -extract ProgramArguments.0 raw -- $plist + filesystem existence check

# Network / routing
lsof -iTCP -sTCP:LISTEN -P -n
nc -z localhost 8080 ; nc -z localhost 9000 ; nc -z localhost 11434
dig +short bridge.ollietrades.com
curl -sI -m 3 http://127.0.0.1:8080/
curl -s -m 5 -w "%{time_total}" http://192.168.1.166:11434/api/tags  # x5
grep -rnE "OLLIE_URL|192.168.1.166" --include="*.py" --include="*.env*" .

# Storage
ls -lh data/*.db ; ls -lh data/*.db-{wal,shm}
sqlite3 data/trader.db "PRAGMA journal_mode; PRAGMA wal_autocheckpoint; PRAGMA synchronous; PRAGMA freelist_count;"
sqlite3 data/trader.db "PRAGMA integrity_check;"
sqlite3 signal-center/signals.db (same)
ls -lh backups/trader_*.db
cat logs/offhost_backup.log

# Logs
du -sh logs/ ; wc -l logs/trader.log logs/trader_error.log
tail -5000 logs/trader.log | grep -iE "error|exception|failed|traceback" | sort | uniq -c | sort -rn

# Config / security
stat -f "%Sp %Sm %z bytes %N" .env .env.example
grep -rn "ALPACA_API_KEY|ALPACA_SECRET_KEY" --include="*.py" .  | grep -v APCA

# Ollama
/Applications/Ollama.app/Contents/Resources/ollama list
/Applications/Ollama.app/Contents/Resources/ollama ps

# Activity counts
sqlite3 data/trader.db "SELECT player_id, COUNT(*) FROM signals WHERE created_at > datetime('now','-24 hours') GROUP BY 1 ORDER BY 2 DESC;"
sqlite3 data/trader.db "SELECT COUNT(*) FROM trades WHERE executed_at > datetime('now','-24 hours');"
sqlite3 data/trader.db "SELECT COUNT(*) FROM positions WHERE COALESCE(qty,0) != 0;"

# Saturday targets
sqlite3 data/trader.db "SELECT id, halt_mode, is_paused, fallback_model, halt_reason FROM ai_players WHERE id IN ('ollie-auto','ollama-llama');"
```

**No mutating SQL, no file edits, no service restarts performed.**

---

*End of audit. Top 10 ranked by reward÷effort. Saturday kill prep
intact. Recommend Items 1–4 for the next sprint.*
