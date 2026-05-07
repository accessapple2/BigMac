# OllieTrades Ops Log
Automated audit trail for DB operations.

## Backfilled (2026-05-02 audit forensics)
- 2026-04-30T17:01:06 | schwab_sync | backup=trader.db.pre_schwab_sync_20260430_170106 | unknown bytes (CSV import — 0 new rows written, schwab_holdings unchanged at 38)
- 2026-04-30T17:35:25 | learning_cycle | backup=trader.db.pre_learning_20260430_173525 | unknown bytes (nightly daily_lessons run — 18 rows added, kirk_advisory_log +4)

## Live entries
- 2026-05-03T06:00:03 | daily_backup | backup=trader_2026-05-03.db | 225336KB

## 2026-05-03 ~20:05 MST — Production deploy: tier-2 fix + 14 pending commits

Commit deployed: 721b2fa (fix: tier-2 spread tiebreaker — migrate strategy_signals → signals; add missing persist)
Plus 13 prior commits ahead of origin from Saturday + today's work.

Pre-restart state:
- PID 84968, uptime ~4h 17m, B15 fix verified holding (0 OLLIE_URL errors post-startup)
- Backup: backups/trader.db.pre_tier2_deploy_20260503_200351 (220.4 MB)
- All 3 _EXECUTION_ENABLED gates False (executor.py:22, bull_call:63, bear_put:63)

Restart action: launchctl kickstart -k gui/$(id -u)/com.trademinds.trader

Expected post-restart:
- New PID assigned
- All 14+ commits effective in production (tier-2 + Saturday/Sunday backlog)
- B15 fix continues holding (zero OLLIE_URL errors expected)
- tier-2 tiebreaker now reads signals table (verification Monday market hours)

Rollback if needed:
- git revert HEAD (reverts tier-2 commit 721b2fa)
- launchctl kickstart -k gui/$(id -u)/com.trademinds.trader (restart on reverted code)

Refs: /tmp/scotty_session_2026-05-03/OPTION_A_DEPLOY_DIRECTIVE.md
Refs: /tmp/scotty_session_2026-05-03/tier2_landmine_fix_proposal.md (Section I, Admiral verdicts)
- 2026-05-05T06:00:05 | daily_backup | backup=trader_2026-05-05.db | 234968KB
- 2026-05-06T06:00:02 | daily_backup | backup=trader_2026-05-06.db | 242832KB
- 2026-05-07T06:00:06 | daily_backup | backup=trader_2026-05-07.db | 251812KB
- 2026-05-07: HM-AO closed as already-shipped — bug fixed in 86bb32b (Apr 24). Same-class bug pivoted to HM-AO-β (scripts/ollie_backtest_*.py).
- 2026-05-07 09:30: HM-AS diagnosed. battle_station_monitor cadence median 2:01 (on target); p95 5:07; tail driven by single-threaded schedule.run_pending() blocking on slow jobs. Architectural, not bug. 80% fire-rate recovery preserves α-lift evidence integrity. HM-AS-β (10-min observability log when interval >180s) queued for post-soak.

## 2026-05-07 10:00 — HM-AT TCC diagnosis + manual GUI fix path

The Schwab CSV watcher (`com.ollietrades.schwab-watcher`) appeared dormant 2026-05-06 and 2026-05-07. Initial theory was launchd fast-exit throttle: commit `e8b7f9e` (fix: HM-AT prevent launchd throttle in schwab_csv_watcher) added defensive `sleep 11` to script end.

**Revised diagnosis 2026-05-07**: `launchctl print gui/$(id -u)/com.ollietrades.schwab-watcher` confirmed `runs = 7`, `last exit code = 0` — the agent IS launching every 60s as designed. The real cause is **macOS TCC denying the launchd audit session access to `~/Downloads/`**. Manual runs (SSH/Terminal) inherit the Full Disk Access grant from the host app; launchd's audit session does not. The `nullglob` setting in `scripts/schwab_csv_watcher.sh` swallows the empty-glob expansion silently — every cycle exits clean with exit 0 and no log trace of the failure. Manual SSH-launched runs successfully processed all 6 backlogged CSVs (Apr 30 → May 7 06:16) during diagnosis, archive count 2 → 13.

**Manual fix path** (Admiral, GUI step):

1. Open System Settings → Privacy & Security → Full Disk Access.
2. Toggle ON for `/bin/bash` (use `+` to add if not listed; navigate to `/bin/bash`).
3. Reload the agent:
   ```
   launchctl bootout gui/$(id -u)/com.ollietrades.schwab-watcher
   launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ollietrades.schwab-watcher.plist
   ```
4. Verify within 90s by dropping a Schwab CSV in `~/Downloads/` and confirming it lands in `data/schwab_csv_archive/` and an entry appears in `logs/schwab_watcher.log`.

**Recovery on macOS update or TCC reset**: same GUI step. Document any TCC reset events here for pattern visibility.

**Defense-in-depth note**: `e8b7f9e` (`sleep 11`) is retained — harmless overhead and protects against the throttle theory if it ever becomes a compounding factor.

**Backlog**: HM-AT-β tracks migration of watcher to `~/autonomous-trader/inbox/` to eliminate TCC dependency entirely (post-soak).

## 2026-05-07 — HM-AT-β shipped: watcher migrated off ~/Downloads/ to ~/autonomous-trader/inbox/

Ship reason: GUI fix path for HM-AT (Full Disk Access grant) is unavailable — bigmac is a headless Mac Mini M4 with SSH-only access. HM-AT-β escalated from post-soak to immediate.

Changes:
- `scripts/schwab_csv_watcher.sh` — `WATCH_DIR` moved from `/Users/bigmac/Downloads` to `$HOME/autonomous-trader/inbox`
- `scripts/import_schwab_csv.py` — `DOWNLOADS` constant renamed to `INBOX`, repointed to `REPO_ROOT/inbox`; `--latest` glob and error message follow
- `docs/SCHEMA.md` — `schwab_holdings` table notes updated to reflect new watch dir
- `docs/XO_BACKLOG.md` — Schwab Workflow section updated with new path + scp command; HM-AT-β marked **SHIPPED**
- `.gitignore` — `inbox/*` ignored, `inbox/.gitkeep` tracked
- New empty dir: `~/autonomous-trader/inbox/.gitkeep`

**NEW WORKFLOW for Admiral** (PowerShell on Bonnie laptop, replaces browser-save-to-Downloads):
```
scp "C:\Users\Bonnie\Downloads\Sc*Position*.csv" bigmac@192.168.1.248:~/autonomous-trader/inbox/
```

The launchd watcher polls `inbox/` every 60s (StartInterval=60) and processes on the next tick. Verification: log entries land in `logs/schwab_watcher.log`; CSV moves to `data/schwab_csv_archive/`; NTFY fires to topic `ollietrades-admin`.

**Defense-in-depth retained**: `e8b7f9e` (`sleep 11`) stays — harmless overhead.

**Recovery**: `git revert <this-commit-sha>` + `launchctl bootout gui/$(id -u)/com.ollietrades.schwab-watcher && launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.ollietrades.schwab-watcher.plist`.

## 2026-05-07 — HM-AK fleet roster cleanup (12 dormant agents halted)

Diagnosis revealed 12 ai_players rows with `halt_mode='active' AND is_active=1` but **zero trades, signals (or fixed-pool 25-sig bootstrap), zero war_room posts** in the last 7 days. Two cohorts:

- **Paid-API zombies (6)** — should have been benched under Free-Models-First (CLAUDE.md 2026-04-13): `claude-haiku`, `claude-sonnet`, `gpt-4o`, `gpt-o3`, `grok-4`, `gemini-2.5-flash`. None of them showed any api_costs entries, signals, trades, or war_room activity over 7 days.
- **Dormant Ollama (6)** — scaffolded but never promoted, all show exactly 25 fixed-pool sigs and zero trades/posts: `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3`, `ollama-glm4`, `ollama-gemma27b`.

Side benefit: halting these 12 eliminates **all three duplicate display name conflicts** (`Lt. Cmdr. Worf` × 3, `Lt. Cmdr. Spock` × 2, `Qwen3 14B Pro` × 2) — the active iteration now contains only the canonical agent for each name.

**Action:** halt_mode UPDATE per CLAUDE.md halt SQL pattern.
- 11 with no open positions → `halt_mode='full'`
- `gemini-2.5-flash` had 2 open positions → `halt_mode='exit_only'` (allows close-out)

**Halt-mode breakdown:** before 37/9/4 (active/full/exit_only) → after **25/20/5** (12 moved). Total ai_players rows unchanged at 50.

**SQL artifact:** `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` (committed for reproducibility + rollback).

**Rollback:**
```sql
UPDATE ai_players SET halt_mode='active', halted_at=NULL, halt_reason=NULL
 WHERE halt_reason LIKE 'HM-AK 2026-05-07%';
```

**Architectural note (deferred):** the active scan loops at `main.py:1991`, `engine/risk_radar.py:168`, `engine/autopilot.py:63` use `WHERE is_active=1` and do **not** filter by `halt_mode`. Per-trade halt gates downstream block actual execution, but the iteration loops still touch all halted rows. HM-AK does not address this — sized for HM-AK-β if/when prioritized.

**No service restart required** — halt_mode is read fresh per CLAUDE.md ("halt_mode is now the only working per-player kill switch", read by `engine/halt_gate.py` per request).

## 2026-05-07 — HM-AQ Captain decision: broaden WATCH_STOCKS to market-cap+volume universe

**Decision (Captain):** WATCH_STOCKS expands from 20 manually-curated mega-cap tickers to ~500-800 dynamically-refreshed tickers matching:
- Market cap ≥ $5B
- Daily $ volume (20-day avg) ≥ $50M
- Refresh cadence: weekly (Sunday pre-Monday-open)
- Refresh source: **Polygon screener API** (Polygon Options Starter $29/mo activation under HM-AQ-β; first paid exception under Free-Models-First doctrine, approved-in-principle 2026-04-16)

**Risks acknowledged by Captain:** dashboard noise, scan-loop slowdown across 12+ iteration sites (per HM-AU audit), more spread attempts on lower-liquidity options *only if* HM-AQ-γ ships.

**Catches:** all 6 missed movers from 2026-05-07 morning (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%) would have been in coverage under the new criteria.

**Scope split:**
- WATCH_STOCKS broadens (this decision).
- Spread-strategy universes (`TIER_1+TIER_2` in `bull_spread_v1.py` / `bull_call_spread_v1.py` / `bear_put_spread_v1.py`) stay at 10 tickers. Spread quality > coverage on illiquid options. Expansion deferred as **HM-AQ-γ** (marker, not active queue).

**Implementation queued as HM-AQ-β** (4-8 h Scotty):
- `engine/universe_refresh.py` (Polygon screener integration)
- launchd weekly refresh (`com.ollietrades.universe-refresh`)
- Storage migration (`config.py:WATCH_STOCKS` → DB table or refreshed file)
- Polygon Options Starter $29/mo activation (first paid exception under Free-Models-First)
- Audit/retest of 12+ dashboard iteration sites for rate-limit + latency impact
- Soak window before promoting to live

**Canonical reference:** `docs/UNIVERSE.md` (created in this commit).

**No code changes in this entry.** Doc-only Captain decision logging.

## 2026-05-07 — HM-AR audit: earnings_universe DEPRECATED + path map docs

Diagnosis surfaced that the original HM-AR ticket framed three independent earnings paths as a single system. The audit untangles them:

1. **Options blackout (LIVE, safety-critical)** — `engine/options_selector.py::_next_earnings_date` → `data/earnings_cache.json` + yfinance fallback. Independent of any SQLite table.
2. **`main.py:679 run_earnings_universe_inject()` (LIVE)** — daily 06:00 AZ scheduler. Despite the function name, writes to `scan_universe`, NOT `earnings_universe`. Naming-drift artifact.
3. **`engine/earnings_injector.py` + `earnings_universe` table (DEAD ORPHAN)** — writer + reader exist, but no external caller; docstring claims a 06:00 AZ schedule that was never wired. Table empty since creation.

**Classification:** DEPRECATED. No safety regression — path 1 (the safety-critical one) is intact and independent.

**Documented:**
- `docs/EARNINGS.md` (new) — three-path map, full audit findings
- `docs/SCHEMA.md` — `earnings_universe` row updated to point at audit
- `docs/XO_BACKLOG.md` — HM-AR marked AUDITED+DOCUMENTED, HM-AR-β cleanup ticket queued

**Recommended cleanup (HM-AR-β, ~15 min Scotty):** archive `engine/earnings_injector.py` → `archive/retired/2026-05-07-earnings-injector/`; rename `run_earnings_universe_inject` → `run_earnings_scan_inject` in `main.py` to fix the naming-drift lie. Leave the empty `earnings_universe` table in place (sacred-data rule).

**No code changes in this entry.** Doc-only audit.
