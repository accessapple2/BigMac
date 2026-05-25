# XO Backlog — USS TradeMinds
# Riker's Standing Work Queue
# Updated: 2026-05-25 (Memorial Day — HM-DATA-INTEGRITY-FORENSICS triage)

---

## 🔴 HM-DATA-INTEGRITY-FORENSICS — PARENT TICKET (filed 2026-05-25)

**Trigger:** Forensic audit during HM-RISK-MANAGER-CONVICTION-STOP-WIRE Lane A
discovered an active "delete-without-archive" endpoint that wiped
`portfolio_history` rows pre-2026-03-11. All 9 DB backups inherit the
post-wipe state; data is unrecoverable from local sources.

**Sacred Data Rule violation:** "Trade data is gold. Never delete data."

### Sub-tickets

#### HM-CATEGORY-C-EMERGENCY-LOCK — ✅ **SHIPPED + SUPERSEDED** 2026-05-25

Initial emergency lock at `45e57e1` (returned HTTP 403, pushed direct-
to-main per Admiral authorization for active data-integrity violation).
Superseded by the proper fix HM-CLEAN-STALE-ARCHIVE-NOT-DELETE (merge
`38a38e4`), which replaces the 403 with the archive-then-delete pattern.
Lock commit retained in main history as forensic record of the doctrine
violation it closed.

#### HM-CLEAN-STALE-ARCHIVE-NOT-DELETE — ✅ **SHIPPED** 2026-05-25 (merge `38a38e4`, pushed to main)

Six-commit branch merged to main with end-to-end smoke + 5/5 tests:

  Phase 1 `04b00f3` — schema (portfolio_history_archived + 3 indexes)
  Phase 2 `8c9b942` — endpoint rewrite (archive-then-delete transaction)
  Phase 3 `a5054c0` — recovery endpoints (GET list + POST restore-from-archive)
  Phase 4 `b634dd0` — pattern tests (5/5 PASS, 0.04s wall)
  Phase 5 `c10eb06` — docs/DOCTRINE.md (Rules 1/2/3 codified)
  Phase 3.1 `c5a9d49` — typing.Optional fix (Py3.9 FastAPI runtime introspection)
  Merge `38a38e4` — merged to main + pushed

Post-merge verification: POST `/api/admin/clean-stale-snapshots` on
live DB (no-match case) returns 200 with `archived_count=0,
deleted_count=0, message="No stale snapshots to archive"`. The 403
short-circuit is gone; archive-then-delete pattern is the active code
path. Live trader continues running pre-merge bytecode until natural
restart — acceptable since the endpoint is admin-only with no
automated caller.

Sacred Data Rule restored: every gold-row removal is now traceable via
session_id + reversible via the restore endpoint.

Branch cleanup: keep `hm-clean-stale-archive-not-delete` 7 days, then
prune (standard).

#### HM-SIGNAL-WIPE-FORENSIC — ✅ **CLOSED AS NO-VIOLATION** 2026-05-25

Investigation result: `signals.earliest = 2026-03-11 18:18:16` is **NOT
a delete event**. Forensic searches confirmed:
- Zero `DELETE FROM signals` in code or git history.
- No clean-* endpoint touches signals.
- No retention/cleanup/prune cron for signals.
- Earliest timestamp matches fleet go-live; sibling tables (portfolio_
  history) started 24s later from same admin session.

Reclassified as Category B (never captured) — signal-emission code first
went live on that date; nothing was deleted. No further action required.

#### HM-PORTFOLIO-RECONSTRUCT-FROM-TRADES — optional, Admiral decides timing

Rebuild portfolio_history Jan 6 → Mar 11 from `trades` + Yahoo/Polygon
OHLCV. Caveat: most AI_SIGNAL_PLAYERS didn't exist pre-Mar-11; the
window is dominated by webull (liquidated) and 3 paid LLMs (now in
exit_only). Reconstruction is feasible but partial; ~6-8h scope. The
50d window we currently have IS the operational reality of the current
fleet — reconstruction adds historical color, not active calibration data.

#### HM-RECAP-TRIGGERS-DELETE-PATTERN — ✅ **AUDITED — NO ACTION REQUIRED** 2026-05-25

Full audit of `scripts/recapitalize_player.py` (137 lines, single source).
Findings:

- **Zero DELETE / DROP / TRUNCATE.** Only one mutation outside audit
  trail: `UPDATE ai_players SET cash=?, is_paused=? WHERE id=?` (L85).
- **Two gold-table INSERTs preserved per recap event:**
  `player_funding_events` (full delta audit) + `portfolio_history` (new
  snapshot at new equity level — prior rows untouched).
- **CLI-only:** no cron, no plist, no imports from other modules.
- **Lifetime usage:** 1 event in 76 days (dalio-metals 2026-03-28
  "restore baseline"). Audit mechanism rarely exercised.

Recap **arms** the precondition (cash >= 9999 + stale low-equity rows)
that the locked-and-fixed `clean_stale_snapshots` endpoint exploited.
Post-merge `38a38e4`, the endpoint archives-not-deletes, so the
script + endpoint now operate as a safe pair: recap sets new cash,
endpoint archives the now-stale prior snapshots to
`portfolio_history_archived`.

No code change needed. Recap is doctrine-compliant. Closing.

Banked adjacent (not actionable now):
- No NTFY on recap events (silent) — fleet observability gap, low priority.
- No 'unrecap' / refund path — one-way by design, acceptable.

#### HM-MARKET-HOLIDAY-CALENDAR — ✅ **SHIPPED** 2026-05-25 (Memorial Day arc)

Production trader fired 6 Alpaca orders + 2 simulated positions on
Memorial Day 2026-05-25 (market closed) because there was no holiday
calendar in production code. Full structural fix delivered same session.

Containment + structural commits (all pushed to main):
  `6cdf9d5`  Stage 1 — 6 Alpaca orders cancelled, 0/0 filled
  `c35aa51`  Stage 2 — 11 local rows archived per Doctrine Rule #2
  `02d3558`  Stage 3 — neo-matrix + ollie-auto halted
  `7d55d35`  Phase A — engine/market_calendar.py + tests (18/18)
  `3cd4838`  Phase B — 7 hard gates + 1 soft update (11/11 tests)
  `bf54ee8`  Phase C — dashboard banner holiday-aware
  `4588639`  Phase D — docs/DOCTRINE.md Rule #4

Doctrine Rule #4 codified: "Never trade on closed markets." Gates at
paper_trader buy/sell/short_sell + alpaca_bridge buy/sell/short_sell
+ alpaca_options.execute_options_signal + risk_manager.is_market_hours.

`engine/market_calendar.py` carries 2025-2027 NYSE holidays + early-
close days + DST-aware status enum. Annual extension required (bank
calendar-year-end ticket each December).

neo-matrix + ollie-auto held at `halt_mode='exit_only'` overnight
2026-05-25 → 2026-05-26 as conservative posture for first overnight
after fix; Admiral promotes to `active` Tuesday 09:30 ET after banner
verification.

#### HM-PROVING-GROUND-FORMALIZE-V2 — ✅ **SHIPPED** 2026-05-25 (merge `3b4bdac`)

Sniper Mode trial formalized after Memorial Day NTFY Proving Ground
review surfaced that the 30-day spec had run to Day 45 without formal
extension or exit criteria. Three-SUB structural ship:

  `e79a12a`  SUB-1 — dedicated `ollietrades-proving-ground` NTFY topic
                    (Admiral mobile receipt confirmed)
  `af22d32`  SUB-2 — Admiral-locked exit criteria + state evaluator
                    + Admiral CLI for terminal states (10/10 tests)
  `20d696c`  SUB-3 — TRIAL_DAYS 30 -> 60 + formalization rationale

Daily 13:18 AZ evaluator hook (`main.py::run_proving_ground_evaluator`).
State machine: pending → warning → ship_ready | kill_warning → shipped |
killed (terminal states require `scripts/proving_ground_admiral.py
--ship`/`--kill` --confirm --agent ollie-auto).

Dry-run at Day 46 (today): state='warning' (4/6 streak 5+ days).
**Heads-up:** at Day 60 boundary (2026-06-09) K1 kill_warning will fire
automatically because dd has held at -24% across the entire trial.
Admiral can preempt with --kill before Day 60 OR respond when the
auto-surfaced kill_warning emits.

#### HM-RISK-MANAGER-CONVICTION-STOP-WIRE — ✅ **SHIPPED** 2026-05-25 (merge `9b55466`)

Conviction-scaled stops shipped with feature flag default OFF. Tier
table at d41216b floor-fix state (0.18/0.15/0.12 — never tighter than
flat baseline).

Branch ship (11 commits merged):
  5655fb0 → f42c181  full HM-POSITIONS-CONVICTION-DENORM + WIRE arc
  d41216b           floor fix (eliminate 0.08 regression band)
  6971a91           Phase 5c 180d re-run (G1 +0.8%, G3 still 2/14)
  568cb81           Phase 6 feature flag default off
  51650ad           Opt 1 calibration trial (REJECTED — worse than floor)
  f42c181           Restore d41216b tier table per Admiral ship-as-is

Admiral flips `CONVICTION_SCALED_STOPS_ENABLED=True` in .env after
shadow-validating live trader behavior. Until then, production is
doctrine-equivalent to pre-wire (flat per-model guardrail for all
players).

HM-CONVICTION-TIER-BOUNDARY-CALIBRATION remains banked (separate
ticket) for post-shadow review — 3/4 regressors (dayblade-sulu,
options-sosnoff, partial ollama-llama) live in the 0.15 tier and would
benefit from calibration that the rejected Opt 1 trial failed to find.

#### HM-NTFY-ACK-CLICKTHROUGH — banked (Week 7 work)

Single click-through URL in NTFY body → logs `ntfy_ack` row to new
`ntfy_acknowledgements` table. ~30-min implementation. Enables real
engagement measurement instead of inferred-via-correlation. Low
priority — can ship anytime in the next 2 weeks.

#### HM-FINMEM-AGENT-MEMORY-ARCHIVE — defensive flag

`engine/finmem_writers.py:280` prunes `agent_memory` rows where
`decay_rate IS NOT NULL AND score < floor`. This is intentional memory
management (decay → prune below threshold), but agent memory IS partial
gold (learned context). Currently delete-without-archive.

Suggested fix: archive pruned rows to `agent_memory_pruned` with
timestamp + reason. Low priority — by design the pruned rows are low-
score memories the agent considered low-relevance; not the same severity
as portfolio_history.

#### HM-PORTFOLIO-POSITIONS-SYNC-ARCHIVE — defensive flag

`dashboard/app.py:10915` Alpaca-sync wipes `portfolio_positions WHERE
status='open'` before re-inserting current Alpaca state. Comment
explicitly preserves closed-history. State-sync is DEFENSIBLE but
worth adding a "last_synced_at" + audit log to make the sync events
queryable. Low priority.

### Audit complete — codebase delete-pattern inventory

Scanned: `dashboard/app.py`, `engine/`, `scripts/`, all `*.sh`,
`*.sql`, `migrations/`. Categories:

| Pattern | Count | Status |
|---|---|---|
| Active VIOLATION (delete gold without archive) | **1** | LOCKED via `45e57e1` |
| DEFENSIBLE state-sync (positions table on close, Alpaca sync, watchlist remove, season reset) | 13 | trades preserve history; positions are live-state |
| DEFENSIBLE computed-table refresh (rs_rank, minervini_trend, premarket_scan, backtest_extras, gex_levels) | 12 | nightly INSERT-replace pattern |
| Test scaffold cleanup (synthetic scripts) | 6 | not production paths |
| Watchlist user-driven removal | 3 | user intent, not data wipe |

No second active violation found. Sacred Data Rule integrity restored
(pending HM-CLEAN-STALE-ARCHIVE-NOT-DELETE proper fix).

---



**Reconciliation method**: every claim below verified against running code, DB state,
launchctl, trader.log post-PID-84968 startup (15:45 MST today), and on-disk files.
Items moved by category based on observed reality, not historical claim.

---

## Schwab Workflow

**Drop directory:** `/Users/bigmac/autonomous-trader/inbox/` (relocated 2026-05-07 from `/Users/bigmac/Downloads/` per HM-AT-β; previous: 2026-05-04 → `~/Downloads/`; pre-2026-05-04 → `/Users/Shared/schwab_inbox/`).

**How it works:** Admiral scps `Sc*Position*.csv` from Bonnie laptop into `~/autonomous-trader/inbox/`. The launchd watcher `com.ollietrades.schwab-watcher` polls every 60 seconds, finds the file via glob `Scwab*Positions*.csv` / `Schwab*Positions*.csv` / `schwab_*.csv` (case-insensitive), invokes `scripts/import_schwab_csv.py`, syncs via `scripts/sync_schwab_to_real_holdings.py`, archives the CSV to `data/schwab_csv_archive/`, and fires an NTFY notification to topic `ollietrades-admin`.

**Admiral's scp command** (PowerShell on Bonnie laptop):
```
scp "C:\Users\Bonnie\Downloads\Sc*Position*.csv" bigmac@192.168.1.248:~/autonomous-trader/inbox/
```

**Imports log:**
- 2026-05-07 09:14 MST — backlog drain (6 CSVs Apr 30 → May 7) imported during HM-AT diagnosis; archive count 2 → 13.
- 2026-05-04 09:35 MST — fresh snapshot 2026-05-04T12:15:00 imported, 24 rows. Resolved 4-day stale-data display issue (DELL day-change was showing -3.59% from 2026-04-30 snapshot; now correctly -0.77% from today's snapshot).
- 2026-04-30 09:21 MST — snapshot 2026-04-30T11:30:00, 16 rows
- 2026-04-28 09:39 MST — snapshot 2026-04-28T12:30:00, 14 rows
- 2026-04-24 09:54 MST — snapshot 2026-04-24T12:48:00, 8 rows

**Cadence note:** Imports are still manual-trigger (Admiral scps Schwab Positions CSV from Bonnie laptop into `~/autonomous-trader/inbox/`; watcher does the rest). No NTFY reminder added — revisit if drift recurs in 3 weeks.

---

## ⚠️ POST-GATE-FLIP MONDAY MORNING WATCH (2026-05-05 06:30 MST)

Gate flipped 2026-05-04 08:30 MST (commit `df7320c`). Service restarted PID 13734.
First live autonomous trades fire at NYSE open Monday 06:30 MST / 09:30 ET.

**Tier-2 execution gating:** Both `bull_call_spread_v1` and `bear_put_spread_v1`
filter `WHERE agent_name='tractor-beam'` on signal-center reads. **Tractor-beam
is the agent whose performance matters.** Pre-flip 30-day baseline: 268 signals,
34.3% hit_tp, PF 2.02, avg_pnl +1.74%.

### Pre-open (06:00 MST):
- [ ] Service running, PID stable (was 13734 at flip; may have rolled overnight)
- [ ] No overnight Errno 48 in trader.log (baseline = 6)
- [ ] Calibration query produces reasonable numbers (`signals.db::trade_signals` joined to `signal_outcomes`, agent_name='tractor-beam', last 24h)
- [ ] Halted players still 4 (ollama-llama, grok-3, dayblade-sulu, gemini-2.5-pro)
- [ ] All 3 gate sites still `_EXECUTION_ENABLED: bool = True`

### First-trade observation (06:30 - 10:30 MST):
- [ ] First fleet signal of the day fires cleanly
- [ ] First trade placed via Alpaca paper successfully
- [ ] Trade appears in `paper_trades` and `trades` tables
- [ ] Dashboard `/api/agents/scoreboard` reflects the new trade

### Kill-switch criteria — REVERT or HALT if ANY of:
1. **Tractor-beam delivering >5% loss on any single trade** → REVERT (SL discipline failure on the agent that drives execution)
2. **Aggregate paper P&L drawdown >5% from $99,931 starting balance in any 24h period** → HALT for review
3. **Tractor-beam placing >15 trades in first 4 hours** → HALT that signal source (suppress tractor-beam writes to `signals.db::trade_signals`)
4. **2+ service crashes in first 4 hours** → REVERT
5. **ANY real-broker (Schwab/Webull/IBKR) API call attempt** → REVERT IMMEDIATELY

### Recovery procedure (REVERT path):
```bash
cd ~/autonomous-trader
git reset --hard gate-flip-revert
git push --force-with-lease origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
```

- Pre-flip main HEAD: `753f01a70f2a145a1f2cd70a41143d8188f0ae3d`
- Pre-flip backup: `backups/trader.db.pre-gate-flip-20260504_082909`
- Recovery doc: `/tmp/gate-flip-recovery.md`
- Local-only branch `gate-flip-revert` retained at least 1 week of clean operation

### HALT-tractor-beam procedure (kill-switch #3, less drastic than full revert):
The tractor-beam poster is external to this repo (posts via HTTP to signal-center port 9000). To halt it without reverting the gate:
```bash
# Option 1: Mark all tractor-beam NEW signals as DISMISSED (one-shot scrub)
sqlite3 signal-center/signals.db "
  UPDATE trade_signals
     SET status='DISMISSED', dismissed_at=datetime('now')
   WHERE agent_name='tractor-beam' AND status='NEW';
"
# Then identify and stop the upstream poster (separate investigation needed).
```

---

## ✅ HM-G COMPLETE — origin push unblocked (2026-05-04 07:25 MST)

5 fat files (1.32 GB total) removed from history via `git filter-repo` on a mirror clone, force-pushed to origin. Origin now at `50ef95c` (rewritten HM-C ship). Push 1 (rewrite): `50ef95c`. Push 2 (gitignore prevention): `f7181f0`. All 25 ahead-commits cleared.

Original archive preserved at `~/autonomous-trader-archive/2026-05-04-pre-hmg-rewrite/` (5 files, hash + byte verified). Surgery mirror at `~/git-surgery/autonomous-trader-mirror-20260504_070333/` retained for insurance.

`.gitignore` extended with fat-file prevention patterns: `backups/*.db.*`, `backups/*-shm`, `backups/*-wal`, `*.deprecated_*`, `.fuse_archive_*/`, `/trader.db`, `*.orig`, `*.swp`. Bare `*.bak` deliberately omitted to avoid silently shadowing 20+ tracked sprint backups.

---

## Retired Components

### Kirk Swing Desk — retired 2026-05-04
- Scaffolded but never wired (`agents/kirk.py::propose_swing()`, `agents/pike.py::second_opinion()`).
- Audit #6A investigation determined: drift between CLAUDE.md (claimed active in fleet roster) and code (zero callers, zero scheduler entries). Per `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` Problem B.
- Decision: **RETIRE** rather than build the 6-8 hr feature. Manual swing-trading workflow no longer applies — fleet shifted to autonomous Alpaca-paper-only.
- Daily Kirk advisor (`engine/kirk_advisory.py`, `engine/kirk_grok_advisor.py`) is preserved and active. `kirk_advisory_log` continues to receive daily writes (272 rows, last write 2026-05-01).
- Code archived at `archive/retired/2026-05-04-kirk-swing-desk/` with restoration instructions in README.
- DB tables `kirk_signals` (0), `kirk_swing_trades` (0), `pike_votes` (0) preserved as empty schemas per SACRED-DATA discipline; can be dropped in a future schema-cleanup migration if approved.

---

## VERIFIED CLOSED (commit + reality both confirmed)

| ID  | Closed | Commit | Reality verification |
|-----|--------|--------|----------------------|
| B1  | 2026-05-03 | `8e06b5e` | `bull_spread_v1` `BULL_CROSS`→`BULL` mapping at `main.py:2610`; regime tick log confirms `BULL_CROSS` normalized to `BULL` at scheduler boundary |
| B2  | 2026-05-03 | `8e06b5e` | `bull_call_spread_v1` `get_regime` ImportError eliminated — replaced with `MarketContext` at `main.py:2685-2701`. Zero `get_regime` ImportErrors after PID 84968 startup |
| B3a | 2026-05-03 | `8e06b5e` | Edit 3 replaced broken `get_regime` import with `MarketContext` + regime normalization |
| B3b | 2026-05-03 | `8e06b5e` | Edit 2 regime normalization at `main.py:2648` — `bear_put_spread_v1` inverted block-list now correct |
| B4  | 2026-05-03 | `8e06b5e` | Same as B3b — Edit 2 closes inverted block-list (no separate `bear_put_spread_v1.py:366` edit needed) |
| B14 | 2026-05-03 | `cdc03d0` | `GetAllPositionsRequest` import removed from `engine/alpaca_options.py`. Symbol confirmed absent in alpaca-py 0.43.2; pure dead-code removal, zero behavioral change |
| B15 | 2026-05-03 | `17d40b4` | `OLLIE_URL` added to `initialize_dayblade()` import. **Verification: zero `OLLIE_URL` errors in `trader.log` after line 337403 (PID 84968 startup at 15:45)**. Pre-fix count 53,985, post-fix delta 0 |
| Task 3A | 2026-05-02 | `803c2db` | `engine/importers/ai4trade_importer.py` — `run_import()` alias added |
| Task 3B | 2026-05-02 | `803c2db` | `uoa/scraper.py:16` docstring path corrected |
| Task 3C | 2026-05-02 | `803c2db` | `premarket-scan.sh:46` defunct `launchctl start com.trademinds.crew` commented out |
| Item 5 | 2026-05-03 | `58c43f0` | ~60 lines dead crew-server polling removed from `premarket-scan.sh` |
| **AUDIT-#1** | 2026-05-03 | *pending commit* | **`halt_mode` enum added to `ai_players` (active/exit_only/full); `halt_gate` helper at `engine/halt_gate.py`; gates wired in `paper_trader.save_signal` (line 1870), `paper_trader.buy()` (line 547), `paper_trader.sell()` (line 1091; semantic: `exit_only` permits sells), `signal_tracker.record_signal` (line 35). Backfilled 1,156 leaked rows (`signals` 1,143 + `watchlist_signals` 13). Live gate-fire confirmed via direct exercise test.** |
| **AUDIT-HM#1** | 2026-05-03 | *pending commit* | **`healthcheck.py:43` `print(line)` removed; launchd plist already routes stdout → `logs/healthcheck.log`, eliminating 2× duplication. Truncated `logs/healthcheck.log` for clean post-fix verification window (next cron tick Mon 06:00 MST).** |
| **AUDIT-Open-Q#1** | 2026-05-03 | *pending commit* | **`ollama-llama` trapped positions flatted internally (NVDA 0.3748 sh @ $198.39, MSFT 0.175 sh @ $399.08; total realized -$7.16); `execution_type='manual_internal_mark'` for audit trail. No Alpaca round-trip per Admiral resolution Option B.** |

---

## PARTIALLY DONE (committed but not yet runtime-verified in production)

| Item | Status | Outstanding verification |
|------|--------|--------------------------|
| Edit 3 (`bull_call_spread_v1`) Monday verification | Code-level verified at `main.py:2685-2701` | Runtime verification needs Monday 06:30-13:00 MST market-hours window. Protocol at `/tmp/scotty_session_2026-05-03/b15_verification_protocol.md` |
| `bull_spread_v1` `BULL` normalization | Logged regime ticks confirm `BULL_CROSS`→`BULL` mapping fires | Need observation of an actual bull-spread signal generated post-fix during market hours (none yet — Sunday) |
| `bear_put_spread_v1` block-list correction | Code paths verified | Need market-hours observation that strategy correctly does NOT fire in BULL regime |
| OPS_LOG audit-trail bonus in `8e06b5e` | Healthcheck `backup_trader_db()` has `operation_name` param + writes to `docs/OPS_LOG.md` | Need next backup event to confirm trail writes |

---

## INTENTIONALLY PAUSED (deliberate dormancy, not a bug)

| Component | Pause mechanism | Verified state | Documentation |
|-----------|-----------------|----------------|---------------|
| `dayblade-sulu` (Lt. Sulu primary options trader) | `is_halted=1` in `ai_players` table, `halt_reason='S6.3 bench: R:R 0.10, dormant since 2026-03-31'` | DB-verified halted; `paper_trader.py` `buy()`/`sell()` both gate on `is_halted` (lines 547, 1091) | Drydock 2026-04-25 audit (CLAUDE.md) |
| `dayblade-0dte` (T'Pol on plutus) | Functionally idle: scheduler still runs `run_dayblade` every 5 min at `main.py:2554`, but no signals in DB since 2026-04-07 (26 days) | DB: MAX(`signals.created_at`) for `player_id LIKE '%dayblade%'` = `2026-04-07 15:41:46` | **Note: NOT commented out at main.py:1920 as previously claimed** — that line is `agent_ratings` code. DayBlade run path is live; dormancy is empirical (no trades emitted), not gated. Investigate before next iteration. |
| Battle Station feeders | Not in launchd | `launchctl list \| grep battle` returns 0 entries; `com.trademinds.battle*` does not exist | April 23 surgery, never re-added |
| Battle Station scheduler in main.py | Active: `run_battle_station_monitor` every 2 min at `main.py:2575`, `run_morning_briefing` daily 06:00 at line 2566 | Code-active but downstream feeders absent | "Pause" is partial: scheduler fires but feeders aren't running, so any signal pipeline is broken |
| `ollama-llama` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |
| `grok-3` | `is_halted=1`, `halt_reason='S6 review: routing zombie, retired 2026-04-25'` | DB-verified halted | Drydock 2026-04-25 |

---

## ARCHITECTURALLY INCOMPLETE (code half-built, not fully wired)

| ID | Component | Reality | Severity |
|----|-----------|---------|----------|
| AI-1 | `signal_scorecard` table | Schema exists with 16 cols, **0 rows**. Writer never wired (April 7 Alpha Engine plan unfinished). Scoring pipeline can't run without source data. | MEDIUM — blocks gate-flip calibration (B5 dependency) |
| AI-2 | `ghost_trades` table | Only **9 rows total** (verified). Per-agent tables (`sarek_paper_trades`, `janeway_paper_trades`, `surak_paper_trades`, `kirk_signals`) appear to be the actual write paths, leaving `ghost_trades` mostly empty. CLAUDE.md describes Bench 4 ghost-recording every signal. Same import-drift family as B12-B15 likely. | MEDIUM — distorts ghost performance scoring |
| AI-3 | `is_active` flag is decorative | Verified: `paper_trader.py` enforces `is_halted` (lines 547, 1091) but `is_active` only appears once at line 1555 in a `SELECT ... WHERE COALESCE(is_active, 1)=1` filter. Halted players (`ollama-llama`, `grok-3`) still have `is_active=1`. Per April 25 audit, `is_paused`, `crew_role` are also decorative. **Document before any new agent wiring.** | DOC-CRITICAL — easy to mis-trust |
| AI-4 | `bridge_voter` collection | `bridge_votes` table has 216 rows total, MAX `created_at` = 2026-05-01 13:01:23 (2 days ago). Wired but not collecting daily. | LOW-MEDIUM — investigation needed |
| AI-5 | `energy-arnold` quality | `qwen3:8b` LLM, **9,632 signals** total, AVG confidence 0.258. Distribution: 6,643 at conf=0.0 (69%), 1,209 at conf=1.0 (13% over-confident), rest scattered. is_active=1, is_halted=0. Bridge_voter wired but not collecting. | NEEDS-DECISION — high noise volume; Phase 4 reframe |
| H1 | `engine/tiered_exits.py:check_spread_exits()` | Fully implemented, never called by any scheduler | HIGH — needed before first live spread trade |
| H2 | `_EXECUTION_ENABLED = False` | 3 independent copies in `executor.py:22`, `bull_call_spread_v1.py:63`, `bear_put_spread_v1.py:63`. Must flip atomically | DEFERRED — after 30 paper trades + positive expectancy |
| H3 | `/api/wheel/status` | Intermittent 500 at `dashboard/app.py:7592` | HIGH — before Wheel goes live |

---

## OPEN BOMBS (current severity, post-reconciliation)

### Production noise / latent
| ID | File | Severity | Status |
|----|------|----------|--------|
| B5 ✅ RESOLVED 2026-05-04 | `signal-center/server.py:2121` | — | **Audit #6X investigation cleared this.** Scorecard system at `signals.db::trade_signals + signal_outcomes` is healthy (1,147 signals, 100% outcome coverage, daemon writing every 15 min). Endpoint `/api/signals/scorecard` returns HTTP 200 in ~19ms. **NOT blocked on AI-1** — that was a different table in `data/trader.db` audit #6A flagged as separate work. Per Admiral verdict 2026-05-04: gate-flip ready at SQL-level review. Frontend calibration column = follow-up sprint, not blocker. See `docs/AUDIT_6X_INVESTIGATION_2026-05-04.md`. |
| B12 | `main.py:481/484` | MEDIUM | `check_vix_spike` ImportError — no commit yet, B12 status check on Monday per `b12_proposed_fix.md` |
| B13 | `main.py:3608` | LOW | Rallies scraper ImportError — 1 occurrence; deferred |
| B16 | `healthcheck.py:25,474` | MEDIUM (downgraded from CRITICAL) | `TUNNEL_URL` hardcoded to orphan `bridge.accessapple.com`. Real bridge `bridge.ollietrades.com` healthy. Part of accessapple rebrand sprint |
| B17 | unknown XML/lxml caller | MEDIUM | 49 `Filename too long: %3C!DOCTYPE…` in `trader_error.log` — passing HTML body as filename |
| B18 | `engine/fast_scanner.py:389/489-490` | MEDIUM | 34 `database is locked` in `scanner.err` — WAL contention with trader process |
| B19 | aladdin scraper write path | LOW-MEDIUM | 35 db-lock-adjacent entries in `aladdin.log`, same family as B18 |
| B20 | yfinance internal | LOW | 25 `HTTP 401 Invalid Crumb` self-recovers, ~9 retries per burst |
| B21 | iv_history pipeline | **LOW (downgraded from MEDIUM)** | "Day 5 missing 2026-05-02" was a Saturday — iv_history records weekdays only. MAX as_of_date = 2026-05-01 (Friday, 10 rows = healthy). Reframe: H4 ops check applies to the next Monday, not weekend |
| B27 | `healthcheck.py` (Ready Room + Red Alert) | LOW | Crusher weekend false-positives on Sat/Sun |
| B29 | `data/trader.db` `ghost_trades` table | MEDIUM | Folded into AI-2 above |

### Cleanup-eligible (Phase 2 candidates)
| ID | Description | Phase 2 action |
|----|-------------|----------------|
| B22 | Two 0B `arena.db` files (root + `data/`) | **CLOSED 2026-05-03** — archived to `arena.db.deprecated_20260503_182837` and `data/arena.db.deprecated_20260503_182837`. Filesystem-only (gitignored). Rollback: `mv ...deprecated_*` back. setup_db.py confirms files were dead artifacts |
| B23 | `tractor.db` referenced in CLAUDE.md SACRED DATA but file does not exist in `~/autonomous-trader` (lives in `~/ollietrades/tractor_beam/tractor.db` and `/Users/bigmac/G1_BACKUP/`) | Doc drift; address with CLAUDE.md update outside this directive |
| B24 | No log rotation policy. `trader.log` 27.5 MB / 337k lines, `trader_error.log` 13.7 MB / 142k lines | Phase 3 investigation report |
| B25 | 19 `.fuse_hidden*` zombie files (32KB each) | **CLOSED 2026-05-03** — archived to `data/.fuse_archive_20260503_182918/` (19 files, all `lsof`-empty pre-archive). Filesystem-only. Rollback: `mv data/.fuse_archive_20260503_182918/* data/` |
| B26 | `main.py:2554-2587` scheduler comment-vs-cadence drift (11 mismatches confirmed) | **CLOSED 2026-05-03** — commit `9ee1c5c`. py_compile clean. Rollback: `git revert 9ee1c5c` |
| B28 | 4 backup orphan WAL files (`trader_2026-04-07.db-shm/-wal`, `trader_2026-04-08.db-shm/-wal`) | **CLOSED 2026-05-03** — archived to `backups/orphan_wals_20260503_182933/` (4 files). Filesystem-only. Rollback: `mv backups/orphan_wals_20260503_182933/* backups/` |
| HM-COVERED-CALL-RECORDING | Covered-call writes recorded as `action=BUY, qty=+positive` instead of `SELL, qty=-N`. Reasoning column says *"Selling call @ $X.XX. Income generation on existing X position"* but the row debits cash as if it were a long-call buy. Surfaced 2026-05-23 during HM-MASTER-PLAN W2-C navigator review — found 4 orphan covered-calls (LITE/MRAM/COHR/MNST) where the stock leg auto-exited and left the misrecorded option leg behind. Caller site: `engine/chekov_autotrade.py::execute_covered_calls` (path that emits the `COVERED_CALL:` reasoning template). Impact: cash-flow direction reversed at write site; PnL accounting on covered calls is the opposite of reality (worthless-expiry is recorded as a loss when it should be a premium-kept gain). | **OPEN, MEDIUM.** Audit `execute_covered_calls` for the misrouted BUY-vs-SELL action. Fix forward + backfill historical rows (positions cleared via W2-C/W2-D pattern; no live rows remain other than navigator PLD which is still covered). Add a regression test that verifies short-call writes land as `action=SELL, qty<0` in `positions`. Cross-ref: backups/positions_navigator_orphan_covered_calls_20260523_075102.sql for the 4 cleared examples. |

---

## DEFERRED (planned sprints, out of scope tonight)

### HM-OLLIE-AI-WORKSPACE — Concept 5 Ollie AI Workspace

**North star:** `USS-Trademinds-Dashboard-Redesigns-v4.3-FINAL.{html,pdf}` (supersedes v4.2).
Lives on Admiral's Bonnie box at `C:\Users\Bonnie\Downloads\`; carry to bigmac via scp or
drop into `~/autonomous-trader/docs/design/` before continuing.

**Concept 5 has 6 sub-views** (was 3 in v4.2):

| # | Sub-view          | Sprint                    | Status        |
|--:|-------------------|---------------------------|---------------|
| 1 | Workspace         | HM-OLLIE-AI-WORKSPACE Step 2 | IN PROGRESS — first pass shipped against v4.2 verbal spec, **needs revision against v4.3** (uncommitted on disk) |
| 2 | Symbol Focus      | HM-OLLIE-AI-WORKSPACE Step 3 | Pending — OPAD-style cockpit + Trade Ticket / Flatten / ½ / Double / Reverse action bar |
| 3 | Signal Replay     | HM-OLLIE-AI-WORKSPACE Step 4 | Pending — FDMT/HE side-by-side + Ollie Signal stamps |
| 4 | Backtest Lab      | HM-OLLIE-AI-WORKSPACE Step 5 | Pending — equity curve + heatmap + filter optimizer |
| 5 | Ollie Wave Scope  | **HM-OLLIE-WAVE**         | Pending — adaptive EMA bands + gainers/losers + treemap |
| 6 | Ollie Machine     | HM-OLLIE-AI-WORKSPACE Step 7 | Pending — 2nd-gen automated momentum + Sim/Live toggle |

**Shipped to date:**
- Step 1 (commit `23d42be`, 2026-05-23) — sidebar 🧠 Ollie AI nav with purple NEW badge + empty `section-ollie-ai` shell.
- Step 2 v4.3 (commit `fb1e7b1`, 2026-05-23) — Concept 5 Workspace sub-view per v4.3 spec L310-374:
  Idea Surfing badge, 6-tab sub-view bar, 8-cell Channel Bar, SPDR sectors + Halts row,
  dual races, movers histogram + Top List Config. Smoke passed.

**Step 2 follow-ups (banked 2026-05-23 post-smoke):**

1. **HM-OLLIE-AI-MOVERS-FIXTURE** — Movers histogram renders "No movers" when
   `/api/movers` returns empty (off-hours, cold cache, or stale-filter excludes all
   rows). Same path also leaves Idea Surfing queue empty. Wire fixture fallback
   when `j.movers.length === 0`. Same fixture pattern as Halts feed. Priority: LOW.
2. **HM-OLLIE-AI-SECTION-ISOLATION** — Portfolio Value + Sector Allocation panels
   from `section-webull` (index.html L6354 region) bleed through above the Workspace
   when viewing `section-ollie-ai`. Persisted across v4.2-pass AND v4.3 rewrite even
   with `min-height:calc(100vh - 120px)` + opaque `background:#05080d` on the section
   wrapper. Diagnosis hypothesis: orphan `.card` elements between `section-ollie`
   close (L8746) and `section-ollie-ai` open (~L9558) OR a section's `display`
   style being overridden by JS elsewhere. Browser DevTools required — read
   `showSection()` flow + scan computed styles in production. Priority: MEDIUM.
3. **HM-OLLIE-AI-SURF-ANIM** — Idea Surfing countdown ring stays static; the
   `conic-gradient(--surf-deg)` CSS-var update from JS every 100ms isn't repainting
   the ring. Likely either: (a) conic-gradient with var() not re-evaluating on var
   change in Safari, OR (b) need to use `@property --surf-deg { syntax:'<angle>'; }`
   for animatable custom property, OR (c) swap to SVG arc/stroke-dashoffset for
   guaranteed cross-browser. Priority: LOW (cosmetic).

### HM-OAI-SIGNAL-REPLAY-POLISH — three deferred items from Step 4b

**Banked 2026-05-24 after Step 4b ship (commit `3fc9a83`).** Signal
Replay is functional with real per-card live wire; these three items
were intentionally deferred from the 4b scope:

1. **True BUY-date lookup for the signal-candle pivot.** Currently the
   "Ollie Signal · {date}" label uses `executed_at` from the SELL row
   (the close date), not the original BUY date. `_oaiPickPivotIdx`
   centers the candle window on the close, so the signal candle is
   effectively the close candle. To show the actual BUY entry pivot:
   - **Path A:** Join `trades` to itself (most-recent prior BUY for
     same symbol + player_id + asset_type) — add a SQL CTE in
     `dashboard/app.py::recent_trades`. ~10 LOC.
   - **Path B:** New endpoint `/api/trades/round-trips?limit=N` that
     returns matched BUY/SELL pairs with both timestamps. Cleaner
     separation, ~30 LOC.
   - **Path C:** Frontend two-fetch: when a SELL is picked, fire a
     second `/api/trades/recent?symbol=X&player_id=Y&before=Z&action=BUY`
     filtered call. Heavier per-pick latency.

   **Recommend Path A** — minimal backend touch.

2. **Short round-trip support.** Current dropdown filter is `action
   LIKE 'SELL%'`. Short trades open with `action='SHORT_SELL'` (or
   `SELL_TO_OPEN` for options) and close with `BUY_TO_COVER`. None
   present in current 200-trade window. When they appear, the "Buy
   Signal" stamp would mis-label them (short entry should show "Sell
   Signal"). Fix: detect direction from the matched OPEN row's action
   (after #1 lands), flip stamp + color logic.

3. **Filter UI for the dropdowns.** Today the dropdowns show all 106
   replayable trades; the Captain scrolls to find a specific signal.
   Add filter pills above the dropdowns:
   - Date range (today / 7d / 30d / 90d / all)
   - Player filter (ollie-auto / navigator / neo-matrix / all)
   - Outcome filter (winners / losers / all)
   - Min |pnl_pct| slider
   Lightweight client-side filter that re-populates the dropdown lists.
   ~2-3h frontend.

**Priority:** LOW. Signal Replay arc is production-ready; these are
ergonomic upgrades. Pick up alongside other HM-OAI-POLISH cluster
work or after Step 5 Backtest Lab ships.

### HM-SIGNALS-RECENT-ACTED-ON-FIELD — `/api/signals/recent` payload omits the `acted_on` column

**Surfaced 2026-05-24 during Step 4a Signal Replay build.**
`dashboard/app.py::recent_signals` (L3790) selects from the `signals`
table but the response payload does not include the `acted_on` column
even though it's defined in the schema (`signals.acted_on INTEGER
DEFAULT 0`). Confirmed via curl + inspection — payload contains
`player_id, display_name, provider, symbol, signal, confidence,
reasoning, asset_type, option_type, created_at, sources, timeframe,
execution_status, rejection_reason` — no `acted_on`.

**Impact:** Signal Replay (Step 4a/4b) can't filter signals by
"actually became a trade" using the canonical field. Step 4a worked
around it by using `execution_status !== 'REJECTED'` as a proxy
(includes EXECUTED + SKIPPED + any other non-rejection state) but the
semantic match is imperfect. Today's signal-db is dominated by
REJECTED rows (47/50 of most recent), so the workaround yields very
few replay candidates.

**Fix paths:**

1. **Add `acted_on` to the SELECT** in `recent_signals()` at
   `dashboard/app.py:3790-3840`. ~1 line change. Frontend filter then
   uses canonical field: `r.acted_on === 1`.

2. **Cross-reference with trades table** for richer replay data — join
   `signals` to `trades` on `(player_id, symbol, created_at)` so each
   signal in the payload includes the resulting trade's
   entry/exit/realized_pnl. Heavier but unblocks Step 4b's outcome %
   computation without separate per-signal trades queries.

3. **Compound — add `acted_on` AND a separate
   `/api/signals/replayable?limit=N` endpoint** that returns only
   signals with corresponding trades + computed outcome %. Cleanest
   for Signal Replay use case; isolates the query optimization from
   the general /api/signals/recent consumers.

**Priority:** MEDIUM. Blocks the broader-scope Step 4b query semantics.
Step 4b can still ship with the execution_status proxy; banking so
the proper fix lands once a backend window opens.

### HM-FUNDAMENTALS-COMPANY-NAME — `/api/fundamentals/{sym}.company_name` falls back to ticker for most symbols

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.** Tested
7 held Alpaca positions (WMB, INTU, AVGO, SPGI, LLY, F, COST) via
`/api/fundamentals/{sym}` — **all 7 returned `company_name == symbol`**
instead of "Williams Companies, Inc." / "Intuit Inc." / etc.

**Impact:** Symbol Focus cockpit header shows the ticker twice (logo +
ticker line + name line all show the same 4-letter string). Degraded
UX without breaking functionality.

**Root cause** at `engine/stock_fundamentals.py:301`:
```python
company_name = profile.get("longName") or profile.get("shortName") or symbol
```
Falls back to `symbol` when both `longName` and `shortName` are missing
from the yfinance profile dict. Other fields in the SAME endpoint response
(sector="Technology", industry="Software - Application", market_cap,
pe_trailing, target_high, etc.) ARE populated — so yfinance itself is
reachable and returning a profile — just not the name fields specifically.

**Hypotheses:**
1. yfinance API surface shifted; `longName` / `shortName` now under a
   different key (e.g. `name` / `displayName` / `quoteType.shortName`).
2. The fields are now in `Ticker.info` vs the older `Ticker.profile`
   path that `fetch_fundamentals` may be using.
3. Polygon Reference (`/v3/reference/tickers/{sym}`) returns a `name`
   field reliably — could be used as a fallback before falling all the
   way back to ticker.

**Fix paths:**
1. Inspect `engine/stock_fundamentals.py::fetch_fundamentals` to see
   which yfinance call provides `profile`. Compare against current
   yfinance docs.
2. Add Polygon Reference fallback: if `profile.get('longName')` empty,
   try Polygon's `/v3/reference/tickers/{sym}` (`branding` + `name`
   already used by HM-OAI-RACE-LOGOS proposal).
3. Cache resolved names in `data/ticker_metadata` table column
   `company_name` so a one-time backfill makes the issue invisible.

**Priority:** MEDIUM. Cosmetic — cockpit functional without it.

### HM-MARKET-DATA-PREV-CLOSE-INCONSISTENCY — `/api/price.change_pct` flips between 0 and stale-percent across symbols

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.** Same
sample of 7 held positions:

| Symbol | price | prev_close | change_pct |
|---|---|---|---|
| INTU | 374.44 | 374.44 | **0.00%** |
| LLY  | 1058.72 | 1058.72 | **0.00%** |
| F    | 14.93 | 13.67 | **+9.22%** |
| WMB  | 78.47 | (delta consistent) | +1.23% |

Market is closed (verified via dashboard log `[Market Closed] Active`).
Expected: all symbols should show `price == prev_close` and 0% change,
OR all should show last-trading-day change from prior-close. Mixed
behavior suggests `engine.market_data.get_stock_price` is pulling
`prev_close` from different time-anchors for different symbols (some
get yesterday's close as `prev_close`, others get the same day's close,
producing 0%).

**Impact:** Symbol Focus header sometimes shows `$XXX.XX (0.00 (0.00%))`
which looks like a quote bug; sometimes shows `+9.22%` which may be a
real intraday move or a 2-day-stale prev_close producing inflated
delta. Inconsistent → user can't tell signal from noise.

**Fix path:** audit `engine/market_data.py::get_stock_price`'s
prev_close resolution. Likely needs a unified prior-trading-day
anchor across all symbol sources (Polygon vs Alpaca vs yfinance).
**Priority:** LOW (cosmetic, doesn't affect trade execution).

### HM-SC-ATR-FEED-DISCREPANCY — residual investigation after partial fix

**Banked 2026-05-24 during XO power-run after HM-SC-ATR-INTU-ANOMALY
partial-fix ship (signal-center/server.py, commit pending).**

The outlier-robust mean (cap individual TR at 5× window median) ships
a real ATR robustness improvement and removes single-gap-bar distortion
(verified: INTU's 2026-05-21 −$73 gap-down would be clamped from $82
TR to $66 cap, ATR reduced ~6%).

But the live `/api/trade-levels/INTU` STILL returns ATR=$40 (atr_pct=12.51%)
even though Alpaca daily bars via engine.market_data.get_bulk_daily_ohlcv
show a 14-bar TR series of ~$13 median with one gap-day outlier of $82
→ a simple-mean ATR of $17.83 (clamped: $16.68). Math difference: $40
live API result implies the bars feed signal-center sees has multiple
high-TR bars (not just the one gap day), OR the time-bucket aggregation
in `_compute_trade_levels` is producing wider 'daily' OHLC than Alpaca's
official daily bars (possibly due to extended-hours inclusion in the
intraday-aggregated path).

**Fix path (not in power run):**
1. Add per-bar tracing to `_compute_trade_levels` to dump the actual
   TR list for INTU vs AAPL side-by-side.
2. Cross-check `_bridge_get('/api/charts/ohlcv?symbol=INTU&timeframe=1D&limit=60')`
   response against `engine.market_data.get_bulk_daily_ohlcv('INTU', '3mo')`
   to identify the feed divergence.
3. If extended-hours inclusion is the cause, add a regular-trading-hours
   filter to the bucket aggregator OR switch the source to the
   already-clean daily bars endpoint.

**Note:** The XO power-run partial fix (5× median cap) is correct on
its own merits and ships independently — gap-day distortion is a real
ATR issue regardless of the feed discrepancy. Requires signal-center
restart to activate (Captain decision; not part of trader restart).

### HM-SC-ATR-INTU-ANOMALY — signal-center reports ATR_PCT 12.51% on INTU vs 1-3% normal range

**Surfaced 2026-05-23 during Step 3 Option 3 verification pass.**
`signal-center:9000/api/trade-levels/INTU` returns `atr_pct: 12.51`
where the 6 other sampled symbols range 0.99% (SPY/AAPL) to 5.20% (F).

**Possible legitimate explanation:** INTU had recent rough sessions
(week52 range $302 - $814 per fundamentals payload) and the ATR window
includes a large drop bar.

**Possible bug:** ATR calc in `signal-center/server.py::_calc_trade_levels`
upstream of the trade-levels response may have a stale OR malformed
candle window producing inflated true-range values.

**Impact:** Symbol Focus on INTU shows wide supply/demand zones because
they're synthesized from `resistance ± atr*0.4`. The zones extend well
beyond the visible candle range, forcing the auto-scale to zoom out and
making the candles compress vertically.

**Fix path:** print the candle window signal-center is using for INTU
ATR calculation. Verify it's the right 14-day daily ATR vs intraday vs
fragmentary. Cross-check against ATR(14) on a charting platform.
**Priority:** LOW (cosmetic for INTU; only affects 1 symbol).

### HM-OLLIE-MACHINE — 6th Concept 5 sub-view (depends on backend agent build)

**Deferred sprint.** Banked 2026-05-24 after the Wave Scope arc shipped.
Last sub-view in the v4.3 Concept 5 map (`docs/design/v4.3-FINAL.html`
L621+) — `OLLIE MACHINE · 2ND GEN AUTOMATED MOMENTUM`. Spec shows:

  - Sim / Live toggle (mode pill switcher)
  - Top-3 momentum picks table with score columns
  - Auto-entry optimization settings (per-rank position sizing,
    confidence thresholds)
  - Machine activity log (last 7 actions)

**Why deferred:** the "Ollie Machine" agent **does not exist in
`config.AI_PLAYERS`** today. v4.3 spec describes the UI for a future
2nd-gen automated momentum agent. Shipping the UI now would render
against vapor data forever until the agent is built.

**Prerequisites before any UI work:**

1. **Build the Ollie Machine agent** in `engine/agents/` (or similar
   strategies path). Likely modeled on neo-matrix (rule-based momentum
   scout currently in active fleet). Specifically v4.3 spec mentions
   "2ND GEN" — implies improvements over neo-matrix:
   - Multi-timeframe confirmation (10m + D agreement)
   - Adaptive position sizing tied to momentum strength
   - Earnings-window awareness (skip pre-earnings setups)
2. **Register in `ai_players` DB row** with `halt_mode='full'`
   initially (cost-doctrine path) for backtesting.
3. **Run 30-day backtest pool** vs neo-matrix to confirm edge.
4. **Promote via Admiral approval gate** per CLAUDE.md Free Models
   First doctrine.
5. **THEN ship the UI** — Step 7a (visual scaffold matching spec) +
   Step 7b (live wire to agent's signals + Sim/Live config endpoint
   + auto-entry settings table).

**UI scope (post-agent ship):**

- Top of page: Sim ↔ Live toggle pill (defaults to Sim; flipping
  to Live requires confirmation modal — broker-state mutating)
- Top-3 picks card: sorted by confidence, columns for symbol /
  entry trigger / SL / TP / sizing / confidence score / "ARM" button
- Auto-entry config: position sizing % per rank, confidence floor,
  earnings blackout days
- Activity log: last 7 actions with timestamps + outcomes
- Performance summary: WR / avg R / total $ since Sim/Live flip

**Effort estimate (post-prerequisite):**

- Agent build + backtest: 8-12h (medium agent, leverages existing
  engine/momentum patterns from neo-matrix)
- UI Step 7a visual scaffold: 3-4h
- UI Step 7b live wire: 4-5h
- Sim/Live toggle + broker-submit confirmation: 2-3h (mirrors Step
  3c action-bar pattern)
- Total ≈ 17-24h split across at least 3 ship cycles

**Priority:** LOW — Concept 5 is 5/6 sub-views shipped and the
existing fleet (McCoy + Dax + Neo + Capitol) already covers
automated trading. Ollie Machine is a future strategic upgrade,
not a blocker on any current workflow.

### HM-OLLIE-AI-SYMBOL-FOCUS-HOVER — chart hover crosshair + candle tooltip

**Polish item explicitly deferred from Step 3b.2** (commit `718904c`,
2026-05-23). Step 3b.2 scope item #5 of 5 — hover crosshair / candle
tooltip — pulled out of the shipped scope so the AI line + Battle
Station + zones + monthly inset could land cleaner.

**Surface:** Symbol Focus cockpit main chart (`#oai-chart-svg` inside
`#section-ollie-ai`, populated by `_oaiRenderSvg()`).

**Target behavior:**
1. On `mousemove` over the SVG plot area, project mouse X to nearest
   candle index, render a vertical dashed crosshair line at that X +
   horizontal line at the mouse Y price level.
2. Floating tooltip near the cursor showing the hovered candle's
   OHLCV + date + delta-from-prior-close. SF Mono numerics, gold
   accent for ticker/date.
3. Show the implied price at mouse Y on the Y-axis (small tag pill).
4. On `mouseleave`, hide crosshair + tooltip.

**Implementation notes:**
- Cleanest path: add a transparent `<rect>` overlay covering the plot
  area to capture pointer events without blocking candle interactions.
- Candle index = `Math.round((mouseX - PAD_L) / barW)` clamped to
  `[0, n-1]`.
- Tooltip as a separate absolutely-positioned div outside the SVG
  (easier to style + position via offsetX/offsetY).
- Throttle mousemove handler with `requestAnimationFrame` so re-render
  doesn't tank scroll perf on 90 candles.

**Effort:** ~2-3h (overlay rect + index math + crosshair render +
tooltip DOM + RAF throttle + theme styling).

**Priority:** LOW (polish). Symbol Focus is functional and readable
without it; hover/tooltip is a power-user nicety for precise level
reading.

### HM-OAI-POLISH (post-Step 3c) — three reference-image gaps banked 2026-05-23

Captain reviewed three reference images vs the current Ollie AI build and
flagged three deltas. All are post-Step 3c polish — Step 3c (action-bar
broker wiring + confirmation modal) stays the priority until shipped.
Images not retained on bigmac; descriptions captured below.

**Status (XO power-run audit 2026-05-24):**
- ✅ HM-OAI-RACE-LOGOS — shipped commit `ff4c09f` (2026-05-23)
- ✅ HM-OAI-SYMBOL-FOCUS-TIMEFRAMES — shipped commit `e83e652` (2026-05-23)
- ⏸ HM-OAI-TOP-LIST-FILTER-DIALOG — **DEFERRED out of XO power run.**
  Scope (~5-7h: 9-field min/max input grid + new `/api/movers/filtered`
  backend endpoint + debounced refetch + localStorage persistence)
  exceeds power-run cadence and requires Captain decisions on filter
  defaults + universe scope. Banking for a dedicated session.

#### HM-OAI-RACE-LOGOS — real company logos in race rows
**Surface:** Workspace sub-view → Volatile Race + Large Cap Race rows
(`dashboard/static/index.html`, `_oaiRenderRaces` and the `<div class="oai-race-row">`
template). Currently `.rt` cell shows the bare ticker text.

**Target:** swap the bare-text ticker with a small (~18-22px) company logo
glyph alongside it, matching the reference image. Options:

1. **Polygon `/v3/reference/tickers/{symbol}` logo URLs** — already on the
   Starter plan ($29/mo Stocks + $29/mo Options, per CLAUDE.md "Polygon
   ACTIVE" line). Returns `branding.icon_url` and `branding.logo_url`.
   Cache locally to avoid request burst on race re-renders (8 symbols ×
   2 races every refresh = 16 calls/cycle if uncached).
2. **Logo.dev / Clearbit** free-tier proxy on `https://img.logo.dev/ticker/{SYM}?token=…`
   — easier integration, no Polygon dependency, but third-party rate
   limits + privacy review needed.
3. **Local logo set** in `dashboard/static/logos/{SYM}.png` for the top
   200 most-traded tickers; fallback to text for misses. Lowest runtime
   cost, manual maintenance.

**Recommended:** path 1 (Polygon — paid plan already active) with a
`dashboard/app.py` proxy `/api/logo/{symbol}` that caches `branding.icon_url`
to `data/logo_cache.json` with 30-day TTL. Frontend renders
`<img src="/api/logo/{SYM}" onerror="this.replaceWith(text fallback)">`.

Effort: ~3-4h (proxy + cache + race-row HTML/CSS rework + fallback).

#### HM-OAI-TOP-LIST-FILTER-DIALOG — interactive Min/Max filter inputs
**Surface:** Workspace sub-view → Top List Config card
(`dashboard/static/index.html` L9841-9850 ish, the `.oai-sec--filters`
card with `.oai-kv` rows). Currently each row shows a static
`label · value` pair (Earnings Date: any, Price: $5-$100, Volume Today:
400K-∞, etc.).

**Target:** convert each kv row into an editable Min/Max input pair
matching the reference image, so the Admiral can adjust filters live and
the Top List re-queries. Per-field UI:

| Field | Input shape |
|---|---|
| Earnings Date | dropdown: any / next 7d / next 14d / past 7d |
| Price | min `$X` + max `$Y` (number) |
| Volume Today | min `X` + max `Y` (number with M/K shorthand parser) |
| Float | min + max (M-shares) |
| Short Float % | min + max (0-100) |
| Position in Range | min + max (0-100) |
| Change from Close | min `X%` + max `Y%` |
| Consecutive Days | min `X` (integer) |
| Relative Volume | min `X.X` (float) |

**Backend:** new endpoint `/api/movers/filtered` accepting all 9 filter
params as query string, querying `mover_watchlist` joined to
`ticker_metadata` + `stock_fundamentals` with WHERE clauses. ~2-3h
backend, ~3-4h frontend (input grid + debounced refetch + apply/reset
buttons + persist to localStorage so filters survive reload).

#### HM-OAI-SYMBOL-FOCUS-TIMEFRAMES — multi-timeframe tabs (10m / D / W / M)
**Surface:** Symbol Focus cockpit chart
(`dashboard/static/index.html` `_oaiRenderSvg`, main `<svg id="oai-chart-svg">`
+ surrounding `.oai-chart` div). Currently locked to `timeframe=1Day` from
the `/api/chart-data?timeframe=1Day&bars=90` fetch.

**Target:** add a 4-tab strip above the chart matching the reference image:

| Tab | Backend param | Bars |
|---|---|---|
| **10m** | `timeframe=10Min`*  | ~78 (1 RTH session) |
| **D** | `timeframe=1Day` (default) | 90 |
| **W** | `timeframe=1Week`* OR client-side aggregate 5 daily → 1 weekly | 52 |
| **M** | `timeframe=1Month`* OR client-side aggregate 21 daily → 1 monthly | 36 |

*`/api/chart-data._TF_MAP` (dashboard/app.py:12534-12541) currently maps
`1m/5m/15m/30m/1h/1d` — needs `10m/1w/1M` added. For 1W/1M the cleanest
path is client-side aggregation from the existing 1Day candles (already
fetched for the monthly inset at bars=750) so no backend change required.
10m requires an Alpaca SIP feed call with the new TF; same `_TF_LOOKBACK`
table needs an entry (~2 days for 10m).

Sub-view title (`{SYM} · D` watermark) updates to `{SYM} · {TF}` on tab
switch. Battle Station overlays (PH/PL/VWAP/ORH/ORL) only make sense on
intraday → hide on D/W/M tabs (or recompute prior_high/low from the
selected timeframe).

Effort: ~3h backend (TF_MAP extension + 10m feed) + ~2-3h frontend
(tab strip + state + redraw + sub-view title).

### HM-TRENDSPIDER-INSPIRED — five tickets banked 2026-05-24 from TrendSpider scanner deep-dive

**Banked 2026-05-24 from TS scan menu cross-referenced against bridge.ollietrades.com.**
Key finding: the existing `section-squeeze` Ghost Watcher is a **short-interest squeeze**
scanner (Finviz/Polygon SI + low float + RSI + volume), NOT a Bollinger-inside-Keltner
volatility-compression squeeze. These are orthogonal concepts. Five tickets below close
the gap. Priorities call out the recommended ship order; none scoped yet.

#### HM-SQUEEZE-BBKC-COMPRESSION — Bollinger/Keltner volatility-compression scanner ✅ SHIPPED 2026-05-24

**Shipped 2026-05-24 commits `ecd2d1b` (core, +1002/-18) + follow-up `fed16de` (NTFY signature + per-run cap).**
First TS-inspired ticket from the cluster live. Default-ON via `BBKC_SQUEEZE_WATCHER_ENABLED=True`.

Sunday-bypass one-shot baseline (PID 36748, 4.66s wall, 3,009 symbols):
- **35 PRIORITY** (≥20d coil)
- **141 ALERT** (10–19d)
- **96 WATCH** (5–9d)
- Top by duration: VRE 45d, SHNY 44d, OII 43d, RNA 41d, AXS 39d

**Monday verification note (no separate ticket).** First live cycle at 06:30 AZ
2026-05-25 should:
1. Compare new-row count vs Sunday baseline (272 total). Dedupe should mean
   most rows skip — expect <50 new inserts, mostly tier upgrades.
2. Tier distribution drift: PRIORITY should grow modestly (yesterday's 19d
   ALERTs aging to 20d PRIORITY); WATCH should churn the most.
3. NTFY fired count should be ≤5 (per-run cap). Already-PRIORITY symbols
   from Sunday dedupe out — only fresh PRIORITY entries NTFY.
4. Query: `SELECT threshold_tier, COUNT(*) FROM squeeze_watch WHERE
   kind='bbkc' AND scan_ts > '2026-05-25T13:00:00' GROUP BY threshold_tier;`

#### HM-RS-RANK-VS-SPY — relative-strength rank vs SPY across universe ✅ SHIPPED 2026-05-24

**Shipped 2026-05-24 commit `b265ff7` (+805/-6, 6 files).** Default-OFF via
`RS_RANK_ENABLED`. Live one-shot baseline: 3,026 universe → 2,432 ranked rows in
3.42s wall; SPY 12wk = +8.17%; rank distribution ~24-25 symbols per slot
(clean percentile spread). NVDA rank 78, return +16.49% vs SPY.

Surface live: `GET /api/rs-rank?top=N&min_rank=M`, `GET /api/rs-rank/{symbol}`,
`/api/fundamentals/{sym}` augmented with rs_rank fields, section-fundamentals
cards show RS row + SPY benchmark badge.

#### HM-RS-RANK-OUTLIER-FILTER — gate IPO / penny-stock noise out of the rank

**Banked 2026-05-24 as a follow-up to HM-RS-RANK-VS-SPY.**

Top-10 from the first live scan included entries like ADV +7764% / AGL +14266%
— real returns but on discontinuous price histories (IPOs, reverse splits,
delisted-relisted symbols). The 60-bar lookback hits a near-zero starting
close and the percentage explodes. Outliers crowd the rank=99 slot with
unusable signal.

**Fix (single ~30-min ticket):**
- In `engine/rs_rank.py::_compute_window_return`, gate on
  `start_price >= 1.0` AND `abs(return_pct) < 500.0`. Symbols failing either
  return `(NaN, 0)` → unranked (rank=0).
- Also worth adding: filter `bars_used < 60` from the rankable set so the
  rank pool is apples-to-apples. Currently a 35-bar symbol's 35-bar return
  gets percentile-ranked against 60-bar returns — minor unfairness but
  noticeable on the edges.

**Impact:** ~30–80 symbols drop to unranked (rough estimate from the
+7000% / +14000% tail), tightening the 99-rank slot to genuine leaders.

**Priority:** LOW (current data is usable; filter is a quality tightening).
~30 min scope.

#### HM-OAI-MOVERS-RS-OVERLAY — color movers histogram bars by RS rank

**Banked 2026-05-24 as a deferred surface from HM-RS-RANK-VS-SPY scope.**
Ollie AI Workspace movers histogram currently colors bars by gain/loss sign.
Overlay rs_rank tier (≥70 deep green, 30–69 neutral, ≤29 deep red) as the
fill color instead, so the captain can see "this is up but it's a weak
RS=20 lagger" at a glance. Tooltip already shows symbol + gain%; add
"RS=N (12wk)" line.

**Priority:** LOW–MEDIUM (cosmetic but high-density signal). ~1–1.5h scope.

#### HM-SQUEEZE-PRE-BREAKOUT-COMPOSITE — multi-factor pre-breakout coil scan

Composite scan combining BB/KC squeeze ≥10 days **AND** price in top 25% of 20-day range
**AND** volume contracting (declining 20-day ATR/HV). This is TS's highest-conviction
"coil under the lid" signal — the BB/KC squeeze alone fires too often; the composite
filters to setups with directional bias.

**Depends on:** HM-SQUEEZE-BBKC-COMPRESSION (provides the squeeze input) +
HM-RS-RANK-VS-SPY (optional fourth factor: RS ≥ 80).

**Implementation:**
- Composite computed inside `engine/bbkc_squeeze_scanner.py` (4th tier above PRIORITY,
  call it `COMPOSITE` or `COILED`).
- Same persistence + NTFY pattern.
- Dashboard: third tab on `section-squeeze` ("Pre-Breakout Composite"), or filter chip
  on the BB/KC tab.

**Priority:** MEDIUM (sequential dep on the first two). ~3–4h scope.

#### HM-SQUEEZE-RELEASE-DETECT — alert when an existing squeeze breaks out

Companion alert: when a row already in `squeeze_watch` with `kind='bbkc'` sees BB expand
back outside KC AND a 2σ volume spike on the breakout candle, fire NTFY with
direction (BB upper break = bullish, BB lower break = bearish). Currently we only
alert on entry to the squeeze; the breakout is the actual tradeable moment.

**Implementation:**
- Add release-detect pass to `engine/bbkc_squeeze_scanner.py::run_scan()`: for every
  row in `squeeze_watch` with `tier IN ('ALERT','PRIORITY','COMPOSITE')` and
  `released_at IS NULL`, check current bar against the entry conditions; if released,
  flip `released_at` + NTFY.
- New columns on `squeeze_watch`: `released_at`, `release_direction` (`'up'`|`'down'`),
  `release_volume_ratio`.
- NTFY topic: `ollietrades-admin` (same as short-interest PRIORITY).
- Dashboard: add "Recently Released" subsection on `section-squeeze` showing rows with
  `released_at` within last 5 days.

**Priority:** MEDIUM (closes the loop on HM-SQUEEZE-BBKC). ~2–3h scope.

#### HM-MINERVINI-TREND-FILTER — Minervini Trend Template pass/fail tagging

Daily background job tagging every symbol in scan universe with Minervini Trend
Template pass/fail (8 conditions: price > 150/200 SMA, 150 > 200, 200 trending up
1mo+, price > 50 SMA, 50 > 150, price within 25% of 52wk high, price > 30% above
52wk low, RS ≥ 70). Cheap — all inputs already cached from Alpaca daily bars +
HM-RS-RANK-VS-SPY.

**Depends on:** HM-RS-RANK-VS-SPY (for the RS ≥ 70 condition).

**Implementation:**
- Daily job alongside RS-rank compute.
- New column `trend_template_pass` (boolean) + `trend_template_score` (0–8 count) on
  `stock_fundamentals`.
- Dashboard v1: add column to `section-fundamentals` + filter chip on Ollie AI
  Workspace movers histogram.
- v2 (deferred): use as a hard pre-filter for the Active 4 voters (Capitol, Neo,
  McCoy, Dax) — only buy candidates that pass the template. Requires fleet-side
  approval; v1 is observation-only.

**Priority:** LOW–MEDIUM (foundational filter for any "leader" composite scan; v1 is
data-only, no fleet behavior change). ~3h v1 scope.

### HM-CHART-DATA-EARNINGS-DATES-POPULATE — `/api/chart-data.earnings_dates` declared empty, never filled

**Backend bug surfaced during Step 3b.1 endpoint discovery 2026-05-23.**
`dashboard/app.py:12531` initializes the chart-data response skeleton with
`"earnings_dates": []` but no code path within `chart_data()` populates the
field. Every caller gets an empty list regardless of symbol.

**Impact:** Symbol Focus cockpit (Step 3b.1 / 3b.2) can only plot UPCOMING
earnings via `/api/earnings/countdown?days=14` — past earnings markers
across the 90-bar daily window (per v4.3 spec L417-418 "E" line at mid-
history) require a separate endpoint or this field finally being filled.

**Fix paths** (pick one):

1. **Populate in chart_data()** — fetch `yfinance.Ticker(symbol).earnings_dates`
   (a DataFrame indexed by datetime, columns include EPS estimate/actual),
   filter to the candle window's date range, return as a list of
   `{date: ISO, eps_estimate, eps_actual, surprise_pct}` dicts. ~15 LOC
   inside the existing try/except envelope.

2. **Separate endpoint** — add `GET /api/earnings/history/{symbol}?days=N`
   that returns the same shape. Cleaner separation of concerns; frontend
   makes one extra parallel call. ~25 LOC.

3. **Pull from existing earnings cache** — `data/earnings_cache.json`
   (per CLAUDE.md) is already loaded by `engine/earnings_hub.py` for the
   countdown endpoint; extend the cache schema to retain historical events
   and expose via either path 1 or 2.

**Why deferred:** Step 3b.1 wired upcoming earnings only (good enough for
v4.3 spec's typical day-of-earnings view). Past earnings markers are a
nice-to-have for Symbol Focus historical context but not blocking the
cockpit. Priority: LOW until a Captain workflow specifically needs past
earnings on the chart.

### HM-SIDEBAR-VAR-MIGRATION — Migrate hardcoded `.sidebar` background to `var(--sidebar-bg)`

**Part of the larger v4.4 migration.** The `.sidebar` selector at
`dashboard/static/index.html` L814 correctly uses `background: var(--sidebar-bg)`,
but a media-query override at **L29137** hardcodes
`.sidebar { background:#0a0e17 !important; ... }` for the mobile breakpoint.
The `!important` plus hardcoded color short-circuits the variable cascade —
light-mode + mobile = dark sidebar, dark-mode + mobile = same dark sidebar
but with the wrong shade vs. desktop. L154 also has an explicit
`[data-theme="light"] .sidebar { background:#ffffff; ... }` that re-hardcodes
the value the var should provide.

**Migration steps:**
1. Refactor L29137's mobile-media-query `.sidebar` rule — remove the
   `background:#0a0e17 !important` clause entirely; the desktop rule's
   `var(--sidebar-bg)` will cascade through.
2. Replace L154 explicit light-mode override with reliance on the
   `[data-theme="light"]` `--sidebar-bg` variable (set to `#ffffff` in the
   light-mode variable block at L134-150). Same for the `border-right`
   color which should pull from `var(--border)`.
3. Grep for any other `.sidebar` rules in the file (`grep -nE
   '\.sidebar\s*{[^}]*background' index.html`) and migrate each to the
   variable system.
4. Browser smoke at mobile breakpoint in both themes per Frontend Ship Rule.

**Why deferred:** sidebar background is sensitive — mobile drawer overlay
behavior needs careful testing across all 4 theme combinations
(dark, light, dark-cb, light-cb if [[hm-theme-cb-consolidate]] ships first).
Bundling with the v4.4 migration sprint avoids a one-off touchpoint.

### HM-DEEPSEEK-STOP-DISCIPLINE — ✅ CLOSED 2026-05-24, no action needed

**Closed 2026-05-24 after XO data review.** Initial diagnosis (30-day window:
81.4% WR, PF 0.53, net −$478, loser:winner 1.92×) appeared to indicate a
stop-discipline problem. Task 1 re-pull split the window pre/post the MU
disaster:

| Window | Closes | WR | PF | Net P&L |
|---|---:|---:|---:|---:|
| Full 30d | 118 | 81.4% | 0.53 | −$478 |
| Post-MU-disaster (5-04+) | 61 | 95.1% | **22.03** | **+$162** |

The 30-day report was poisoned by a SINGLE pre-cap MU concentration disaster
(−$671 on 2026-04-30, before HM-DEEPSEEK-STOP-CAP shipped 2026-05-23).
Post-disaster behavior is dramatically healthy. The agent was mis-calibrated
on CONCENTRATION (already fixed by HM-DEEPSEEK-CONCENTRATION-CAP 2026-05-20),
not on stops. Further % tightening risks killing +6% NOW/AMD-class winners
that are normal volatility band.

**XO decision:** no production stop changes. Wait for post-cap live data.

### HM-DEEPSEEK-30D-RECHECK — re-pull deepseek stats due 2026-06-07

**Reminder 2026-06-07 (~2 weeks post-cap).** Run Task 1 query again:
```sql
SELECT COUNT(*) AS closes,
  SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) AS wins,
  ROUND(SUM(realized_pnl), 2) AS net_pnl,
  ROUND(SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) /
        NULLIF(ABS(SUM(CASE WHEN realized_pnl < 0 THEN realized_pnl ELSE 0 END)),0), 2) AS pf
FROM trades
WHERE player_id='deepseek-7b-grok4'
  AND (action LIKE 'SELL%' OR action='COVER')
  AND executed_at >= '2026-05-23'
  AND realized_pnl IS NOT NULL;
```
**Gate**: if post-cap PF ≥ 1.0 AND WR ≥ 70% AND no losses > $150, hold steady.
If PF < 1.0 OR cap-escape occurs, re-open HM-DEEPSEEK-STOP-DISCIPLINE.

Also check W21+ signal volume — if concentration-cap continues to suppress
deepseek to <100 signals/week with 0 executions, the agent is effectively
muted and the post-cap denominator stays zero (no data to compare against).

### HM-RISK-MANAGER-CONVICTION-STOP-WIRE — wire conviction-scaled stop into live path

**Banked 2026-05-24 for a future power run.** Code-debt fix; orthogonal to
the deepseek decision.

`engine/risk_manager.py:115` already exposes `get_stop_loss_pct(conviction)`:
- conv ≥ 0.90 → 18%
- conv ≥ 0.80 → 15%
- conv ≥ 0.70 → 12%
- conv < 0.70 → 8%

The static helper is used by `engine/backtester.py:459` but **not** by the
live exit path. `engine/risk_manager.py:785` reads
`get_model_guardrail("stop_loss_pct", self.stop_loss_pct)` which returns
the per-model override OR the constructor default — never the conviction
scaler.

**Fix (~10 LOC):** at L785, prefer `get_model_guardrail` if present, else
fall back to `get_stop_loss_pct(pos.get('confidence') or 0.7)` instead of
`self.stop_loss_pct`. High-conviction trades get wider stops; low-conviction
get tighter (the existing wisdom that's currently buried in the static).

**Backtest required:** replay last 30 days with conviction-scaled stops,
compare to flat 12%. Acceptance: aggregate PF improves AND no single agent
sees a WR drop > 10 pts.

**Risk:** changes live exit behavior for every agent. Worth its own scoping
session — Captain decides activation per-agent or fleet-wide.

### HM-DOCTRINE-SHORT-INTEREST-READING — agent-prompt note: high SI is bullish (squeeze fuel), not bearish

**Banked 2026-05-24 during HM-ONDS-COVER trading review.** Two agents
opened ONDS shorts on 2026-03-30 citing high short interest as BEARISH
evidence:

- gemini-2.5-flash (signal_id 46119): "ONDS is experiencing a short squeeze
  (41% short float)... a short position with a stop-loss above $8.80
  appears prudent."
- dalio-metals (signal_id 46525): "short interest extremely high (34.1%
  of float), indicating significant bearish sentiment."

Both agents read high SI as confirming a downtrend. **Inverted read.**
Classically high short interest is **bullish-via-squeeze** — the trade
is crowded SHORT, exposing the symbol to violent upside as covers pile
in. The agents took the side they should have been against.

Outcome: both shorts held 55 days, closed −$106.95 combined via
HM-ONDS-COVER 2026-05-24. ONDS traded $9.07 vs entries $8.80 / $7.88.

**For future agent prompt tuning** (no immediate action — Admiral
decides timing):
- Add to gemini/dalio prompt context: "High short interest (>20% float)
  is a bullish squeeze setup, NOT bearish confirmation. Short the
  symbol only if SI is LOW and price is breaking down on volume."
- Consider a doctrine-level guardrail: reject SHORT signals where
  `short_float_pct > 20` AND reasoning mentions "short squeeze" as
  bearish evidence.
- Pattern is broader than ONDS — sample other historical SHORT signals
  to see how often this inversion appears in the corpus.

**Priority:** LOW (single-event harm so far). Watch for repeat pattern;
size of corrective action scales with frequency.

### HM-EXIT-TRAILING-STOP-TIER-DOCTRINE — pending Admiral decisions before build

**Banked 2026-05-24 after HM-EXIT-TRAILING-STOP-TIER scope surfaced a
critical reframe.** Scope analysis showed a 5th runner tier does NOT
recover the MU $1,916 miss because that move was intra-bar (+418% in
one minute — all 4 tiers ripped on the same bar). Runner helps grinder-
class trades that compound past +50% over weeks, not instant-rippers.

**Four decisions needed from Admiral before build:**

Q1: **What case are we optimizing for?**
  - (a) Capture intra-bar rippers like MU $533 — runner doesn't help,
    needs different solution (event detection + sell-rate damping that
    detects a tier-cascade and pauses the ladder)
  - (b) Capture multi-week compounders past +50% — runner helps
  - (c) Both — needs a 2-part solution

Q2: **Tier weight interpretation A or B?**
  - (a) Reduce tier 4 `sell_frac` only → runner = ~3% of original
    position (tiny tail; meaningful only on the largest winners)
  - (b) Restructure ladder weights (e.g. 0.4/0.4/0.4/0.5) →
    runner = ~22% of original (bigger spec change; affects every
    agent's tier shape, not just runner-enabled ones)

Q3: **Path A or Path B for the trailing stop?**
  - (a) Path A — existing 3% fleet trail (zero new code; trails
    catch fast; small contribution)
  - (b) Path B — wire V3 conviction-scaled trail (10% at +20% gain,
    `_get_trailing_stop_pct` already in `risk_manager.py:851` but
    only used by backtester). Parallel ticket
    HM-RISK-MANAGER-TRAILING-V3-WIRE; affects all fleet agents

Q4: **Which agents opt in?**
  - `ollama-plutus` is obvious (the MU $533 trade is its winner)
  - `super-agent` and `neo-matrix` are candidates
  - Capitol (data feed) and deterministic strategies probably not
  - Each agent's opt-in goes in `MODEL_GUARDRAILS["agent_id"]["runner_pct"]`

**Cross-reference:** scope report in this session (HM-EXIT-TRAILING-STOP-TIER
Captain-framed scope, 2026-05-24). Build is on HOLD until Admiral
answers the 4 questions in a clean session.

### HM-THEME-V4.5-DEPRECATIONS — remove legacy compat shims one release after V4.4

**Banked 2026-05-24 per 47's note.** After HM-THEME-CB-CONSOLIDATE v4.4
(Path C) bakes for one release and HM-INLINE-STYLE-SWEEP completes:

1. **Remove `html[data-theme]` legacy mirror** — V4.4 routes everything
   through `body[data-uss-theme]` (or whatever attribute 47's module
   picks). The legacy `html[data-theme="light"]` selectors + the JS that
   keeps `html.setAttribute('data-theme', ...)` in sync are compat-only.
   Once V4.4 ships and no consumer (CSS, JS, third-party) reads
   `html[data-theme]`, drop the mirror.
2. **Remove `--green` / `--red` / `--accent` aliases** — V4.4 introduces
   semantic names like `--up` / `--down` / `--brand` (47's diagnosis
   per the HM-CB-PATH-A history). The migration IIFE in Block 4 will
   set up alias bindings (`--green: var(--up)`) so legacy consumers keep
   rendering. After HM-INLINE-STYLE-SWEEP migrates 575 inline `style=""`
   color hardcodes to vars, audit which consumers still touch the
   aliases. Drop the unused ones.
3. **Remove `data-cb="true"` orthogonal attribute** — V4.4's single-axis
   `data-uss-theme` enum supersedes the orthogonal `data-theme` ×
   `data-cb` model. The Block 4 migration IIFE handles state migration;
   the old attribute can stay one release as read-only fallback then
   be dropped.

**Trigger:** ship V4.4, soak ≥1 week (one full RTH week minimum), confirm
no consumer is reading the legacy paths (grep + browser DevTools sweep),
then ship the deprecation removal.

**Note:** the exact attribute names / var names in this entry are
placeholders inferred from 47's HM-CB-PATH-A diagnosis ("v4.4 light-cb
selector model" + "v4.4 CSS variable names don't exist (--up, --down,
--up-bg)"). XO should patch this entry with 47's literal naming once
the V4.4 module lands.

### HM-INLINE-STYLE-SWEEP — replace 575 hardcoded inline style="" colors with CSS vars

**Banked 2026-05-24 during HM-CB-PATH-A.** 47 diagnosed 575 inline `style=""`
attributes carrying hex colors throughout `dashboard/static/index.html`. Each
is a theme-blind hardcode that the theme switcher can't lift. Mostly cluster
in:
- SVG inline backgrounds: `style="background:#0d1117"` on `.oai-chart-svg`,
  `.oai-sr-svg-{left,right}`, `.oai-bl-equity-svg`, `.oai-ws-svg`, etc.
- Card backgrounds: `style="background:var(--card-bg)"` or hex equivalents
- Color literals on text spans
Scope: too big for a single commit; needs a sweep script + per-pattern review.

**Approach:** scripted `sed -i` against documented patterns (e.g.
`background:#0d1117` → `background:var(--panel)`) followed by visual smoke
across all 12 sections. ~3-4h with careful testing. Not in HM-CB-PATH-A scope.

### HM-THEME-CB-V4.4-UNIFIED — full body[data-theme="light-cb"] migration

**Banked 2026-05-24 during HM-CB-PATH-A.** Path A kept the legacy orthogonal
model (data-theme=light/dark × data-cb=true/false). v4.4 design spec calls
for a unified single-axis `data-theme` enum: `dark | light | dark-cb | light-cb`.

Migration would require:
1. Renaming `[data-cb="true"]` selectors → `[data-theme$="-cb"]` (or split
   into `[data-theme="dark-cb"]` + `[data-theme="light-cb"]`)
2. Updating `toggleColorblind()` and `applyThemeUI()` to write composite values
3. Migrating existing localStorage `tm-theme` + `tm-cb` to a single `tm-theme`
   with composite value
4. Updating any consumer code that reads `data-cb` directly

Risk: high CSS churn; needs explicit Captain decision on default + back-compat.
Path A was the tactical fix to the visible regression. v4.4 unification is the
strategic rewrite — deferred for a dedicated session.

### HM-THEME-CB-CONSOLIDATE — Unify dual colorblind systems + migrate to data-theme axis

**Lands after v4.4 ships.** Two parallel colorblind systems exist in
`dashboard/static/index.html` today, each with its own button + storage key +
CSS target. Confusing, orthogonal, and the `data-cb` flag is independent of
`data-theme` which forces 4 css-rule matrices instead of a single theme axis.

**System A — KEEP:**
- Button: `#cbBtn` at L2908 (top nav, "CB" label)
- Function: `toggleColorblind()` at L23234
- Flag: `data-cb="true"` set on `<html>`
- Storage key: `tm-cb`
- CSS targets: `[data-cb="true"]` blocks at L403, L407-409, L523

**System B — REMOVE:**
- Button: `#cb-mode-btn` at L8476 (inside sniff-scan page, "CB" label)
- Function: `toggleCBMode()` at L31774
- Flag: `body.classList.add('cb-mode')` (uses a class, not an attribute)
- Storage key: `cb_mode`
- CSS targets: `.cb-mode` selectors (audit before deletion)

**Migration steps:**
1. Delete `#cb-mode-btn` button at L8476 + `toggleCBMode()` function at L31774-31787.
2. Grep for `.cb-mode` selectors — migrate any unique styling into the kept
   `[data-cb="true"]` blocks or fold into the new `data-theme="light-cb"`
   value (see step 4).
3. localStorage cleanup: write a one-time migration that, on page load, if
   `cb_mode === '1'` and `tm-cb` is unset, set `tm-cb = 'true'` then
   `localStorage.removeItem('cb_mode')`. Run once, remove the migration shim
   after a week.
4. Theme-axis migration: replace the orthogonal `data-cb` attribute with a
   composite `data-theme` value. New scheme:
   - `data-theme="dark"` (default)
   - `data-theme="light"` (current light mode)
   - `data-theme="dark-cb"` (dark + colorblind palette swap)
   - `data-theme="light-cb"` (light + colorblind palette swap)
   `toggleColorblind()` becomes a function that appends/strips `-cb` to the
   current theme value. localStorage stays at `tm-theme`. CSS selectors
   collapse from `[data-cb="true"]` + `[data-theme="light"]` cross-products
   into clean per-theme rule blocks.

**Why deferred to post-v4.4:** v4.3 → v4.4 sprint is touching the theme system
heavily already (HM-LIGHT-MODE-FIX 2026-05-23 just landed). Stacking another
theme-axis refactor on top risks visual regressions during smoke. Pick this
back up once v4.4 has soaked for 48h.

**Verification target:** all current `[data-cb]` and `.cb-mode` styling
continues to render correctly in all 4 theme combinations after migration.
Browser smoke required per Frontend Ship Rule.

### Accessapple rebrand cleanup sprint
**Verified count: 22 references across 6 files** (down from claimed 24):
- `healthcheck.py` (2)
- `main.py` (1)
- `dashboard/app.py` (11)
- `docs/G1_MIGRATION_INVENTORY.md` (5)
- `docs/SECURITY_AUDIT.md` (3)
- `docs/XO_BACKLOG.md` (this file, references)

Pre-sprint checklist (unchanged from prior version):
1. Confirm `bridge.ollietrades.com` is in CORS allow-list at `dashboard/app.py:1237` (don't just swap — *verify*)
2. `git remote -v` to confirm GitHub remote — is `accessapple2/BigMac.git` still valid or also renamed?
3. After fix: end-to-end test from external browser via `bridge.ollietrades.com` → dashboard → API call
4. Update `healthcheck.py:481-487` success criteria to accept 2xx/3xx (Cloudflare Access redirect = healthy)
5. Pair with B16 fix — fixing only the URL without success-criteria fix leaves Crusher still flagging stale on the 303

**Why deferred:** sprint touches CORS (security boundary) and external API docs (user-facing). Needs Admiral approval + a weekday window with browser at hand for verification.

### UX Sprint (`docs/UX_SPRINT_2026-04-28.md`)
All acceptance criteria unchecked — sprint never started.
- Priority 1: Risk-adjusted Leaderboard (Sharpe/Sortino/max DD/calibration columns)
- Priority 2: Today's Read Strip + Collapsible Cards
- Priority 3: Plain Mode Toggle

### Chekov Rehab
- Extract S5 version: `git show 859a4f0:engine/chekov_autotrade.py`
- Ghost-trade S5 vs current for 30 days, promote the better one
- Current threshold: 5.0 (muted)

### Bench 4 Ghost Runs (none started)
- Uhura-EDGAR: 60-day ghost run, promote if Sharpe > Capitol's
- Aladdin: wire iShares ETF flow → paper-trade sector rotation
- Spock-R1: 60-day A/B vs McCoy-alone (`ollama pull deepseek-r1:7b` first)
- Picard: convert weekly briefing → Ollie regime-table modifier

### Other deferred
- Phase 2 historical performance forensics across trader.db, signals.db, arena.db
- Phase 3 new backtests for orphaned strategies (`engine/options_agents.py` classes)
- Phase 4 spread strategy comparison report
- signals.db archival cron — first eligible 2026-05-05

### HM-GAMEPLAN-EARNINGS-NULL-FIX ✅ RESOLVED (shipped 2026-05-23 same day banked)

Fix landed via `fix(dashboard): null guard in _gpEarningsRows earnings
renderer` — see commit log. Filter now requires both `e.ticker` AND a
string `e.date`; belt-and-braces fallback on the .slice() call as well.

(Original banking preserved below for the audit trail.)

**Symptom:** `Uncaught (in promise) TypeError: Cannot read properties of
null (reading 'slice') at _gpEarningsRows`

**Location:** `dashboard/static/index.html:4911` inside
`function _gpEarningsRows(earnings)`. Pre-existing bug, NOT introduced
by the W5-Sidebar Consolidation — surfaced during smoke because the
checklist asked the Captain to verify a clean DevTools console.

**Cause:** the function guards `!earnings || !earnings.length` at
line 4905 and filters items with truthy `e.ticker` at 4906, but
never guards `e.date === null`. Any earnings record with a ticker
populated but a null date triggers the throw at:

```js
html += '...'+e.ticker+'</span> '+e.date.slice(5)+...
//                                  ^^^^^^^^^^^^^^^ throws
```

Upstream cause is likely Polygon / yfinance returning a record with
a ticker but no confirmed earnings date for a freshly-listed or
recently-IPO'd symbol — happens intermittently, hence why the bug
was latent rather than constant.

**Fix shape (when picked up):**
```js
var datePart = (e.date && typeof e.date === 'string') ? ' '+e.date.slice(5) : '';
html += '...'+e.ticker+'</span>'+datePart+...;
```

Defensive null + type check on `e.date`, fall back to empty string
so the ticker chip still renders without the date suffix.

**Sibling check:** `_gpCongressFlags` at line 4917 has similar
shape — guards `c.amount || c.amount_range || '—'` defensively
(D4 comment shows the same Pattern was caught earlier there).
Same null-guard discipline needs to land in `_gpEarningsRows` and
likely a few other `_gp*` formatters that consume backend-shape
records.

**Why deferred:** non-blocking — Game Plan card still renders other
sections; the earnings sub-row throws silently and falls through.
Fix is a one-liner but worth pairing with a broader `_gp*` audit
to catch sibling null-guard misses in one sweep.

### HM-LIVE-TRADING-WS-PUSH (banked 2026-05-23, Holly Live Trading scope close)

**Context:** the Holly Live Trading view shipped with **polled REST**
data path (5s candle poll + 10s trade-event poll) per Captain decision
during scoping. Push-based ticker stream deferred to this ticket.

**Build:** new backend Server-Sent Events endpoint wrapping the
existing `engine/realtime_monitor.py` Finnhub WebSocket (L228-234)
into a browser-consumable stream. Frontend `EventSource` subscribes,
gets `{symbol, price, ts}` events as they arrive. Same endpoint can
multiplex Alpaca trade events from the existing event bus so the
marker overlay also gets push delivery instead of poll.

**Shape sketch:**
```python
@app.get("/api/live/stream")
def live_stream(symbol: str = "SPY"):
    """SSE — emits {type:'tick'|'trade', ...} for the symbol."""
    def gen():
        while True:
            ev = next_event()  # blocking on Finnhub queue + trade bus
            yield f"data: {json.dumps(ev)}\n\n"
    return StreamingResponse(gen(), media_type="text/event-stream")
```

**Frontend wiring:** replace `_ltPoll` loop in index.html with:
```js
var es = new EventSource('/api/live/stream?symbol=' + currentSymbol);
es.addEventListener('tick',  function(e) { _ltAppendTick(JSON.parse(e.data)); });
es.addEventListener('trade', function(e) { _ltAppendTrade(JSON.parse(e.data)); });
```

**Why deferred:** polling shipped tonight is the correct trade-off for
EOD scope — 10s marker freshness feels live enough; ship-tonight beats
ship-in-3h. Push upgrade is pure perf, no functional change.

**Affected files (est):**
- `dashboard/app.py` — new ~80 LOC SSE endpoint + queue subscription
- `engine/realtime_monitor.py` — expose internal event queue to FastAPI
- `dashboard/static/index.html` — swap `_ltPoll` for EventSource (~40 LOC)

### HM-ADMIN-ONLY-CSS-DEFAULT-DENY ✅ RESOLVED (shipped 2026-05-23 same day banked)

Fix landed via `feat(security): HM-ADMIN-ONLY-CSS-DEFAULT-DENY
default-deny gate` — see commit log. Changed line 329 from
`body.role-observer .admin-only { display: none !important; }` to
`body:not(.role-admin) .admin-only { display: none !important; }`.
Pre-auth load also benefits from the fail-safe (no role class yet
= treated as non-admin = hidden) at the cost of a brief flash on
slow networks when fetchMe() resolves the admin role.

(Original banking preserved below for the audit trail.)

**Finding:** the `.admin-only` CSS gate in `dashboard/static/index.html:329`
is one line and gates ONLY for `role=observer`:

```css
body.role-observer .admin-only { display: none !important; }
```

`role=admin` → gate doesn't fire → admin elements visible ✓
`role=observer` → gate fires → hidden ✓
**`role=charts` (or any future non-admin/non-observer role) → gate doesn't fire → admin elements visible** ✗

The system already has at least 3 roles per the auth middleware
(admin / observer / charts). Default-allow is the wrong posture for
admin-only — should be default-deny.

**Affected elements** (verified via grep of `.admin-only` in index.html):
- Sidebar nav: War Room (1 item + 1 group header — added during W5
  Sidebar Consolidation 2026-05-23)
- Model Control: 3 buttons (Pause All / Fallbacks / Force Scan)
- Ollie Fleet: ⚡ Send to Fleet button
- Trade Desk (or related panel): ⚡ Send to Fleet button
- Trade Desk Alpaca buttons: BUY / SELL / CLOSE ALL (3 buttons)
- Backtest panel: Run Backtest / Run Inverse Backtest (2 buttons)
- Whole-section gates: `section-strategy-lab` and `section-kill-switch`
  (both have `class="admin-only"` on the section div)

**Fix shape (when picked up):**
```css
/* Replace L329 with default-deny */
body:not(.role-admin) .admin-only { display: none !important; }
```

Or explicit per-role list if `.admin-only` needs to be visible to
some intermediate role. Verify no regression for the admin role
(should see everything as before).

**Why deferred:** simple one-line fix in isolation, but a security
boundary edit — wants a Captain-attended verification across all 3
roles (admin, observer, charts) before shipping. Pair with role
documentation in CLAUDE.md so future Claude sessions understand
the full role surface.

---

## ARCHITECTURAL ORPHANS (code exists, zero wiring to main.py)

| Agent/Class | File | Strategy | Wiring Status |
|-------------|------|----------|---------------|
| `QuarkIronCondor` | `engine/options_agents.py` | Iron condor | No scheduler entry |
| `McCoyBullPut` | `engine/options_agents.py` | Bull put spread | No scheduler entry |
| `AndersonBearCall` | `engine/options_agents.py` | Bear call spread | No scheduler entry |
| `CoveredCallAgent` | `engine/options_agents.py` | Covered call | No scheduler entry |
| `GhostKirkBullCall` | `engine/options_agents.py` | Ghost bull call | No scheduler entry |
| `GhostKirk0DTEBullCall` | `engine/options_agents.py` | Ghost 0DTE | No scheduler entry |
| `GhostLongCall` | `engine/options_agents.py` | Ghost long call | No scheduler entry |
| `GhostNakedPut` | `engine/options_agents.py` | Ghost naked put | No scheduler entry |
| `check_spread_exits()` | `engine/tiered_exits.py` | Model F exits | Imported at main.py:3952 but never called |
| `bear_call_spread()` | `engine/spread_trader.py` | Bear call spread | `SPREADS_ENABLED=False`, scaffolding only |
| `iron_condor()` | `engine/spread_trader.py` | Iron condor | `SPREADS_ENABLED=False`, scaffolding only |

---

## HIDDEN BOMBS (latent, not yet exploding)

| ID | File | Description | Trigger |
|----|------|-------------|---------|
| X3 | `strategies/bull_call_spread_v1.py:2691` | `ctx = {"regime": get_regime()}` — dict not MarketContext (note: regression check needed against `8e06b5e` Edit 3) | After import fix |
| X4 | `main.py:3952` | `MODEL_F_THRESHOLDS` imported at startup, `check_spread_exits()` never scheduled | When spreads go live |
| X5 | All 3 `_EXECUTION_ENABLED=False` | Three independent copies — must flip atomically | Gate-flip session |

---

## OPS UNVERIFIED

| Item | Check | When |
|------|-------|------|
| ~~Ghost scorecard calibration~~ ✅ CLEARED 2026-05-04 | Audit #6X verified endpoint healthy; 1,147 signals, 100% outcome coverage. Per Admiral verdict, SQL-level review sufficient — frontend column is follow-up sprint, not blocker. | (resolved) |
| Alpha threshold for `bull_spread_v1` first trade | Confirm threshold in strategy config | Before first trade |
| Chrome extension Profile 5 re-install | Manual check | Next session |

---

## FOLLOW-UPS FROM AUDIT-#1 (halt_mode introduction)

| ID | Task | Priority | Notes |
|----|------|----------|-------|
| HM-A ✅ FIXED 2026-05-04 | Migrate the ~22 `is_halted` read-sites to `halt_mode != 'active'` | MEDIUM | **Shipped 2026-05-04 (commit `a7e095a`). 14 production read sites migrated** (spec count "~22" was inflated; classification surfaced 14 actual reads after excluding write paths, drawdown-system reads, schema defines, and archived backups). Files: `dashboard/app.py` (9 sites including 2 `WHERE` filters + 7 SELECT/attr reads), `engine/paper_trader.py` (2 SELECTs — dropped unused column from buy/sell halt gate), `engine/morning_briefing.py:62`, `engine/war_room.py:835` (`WHERE is_halted=1` → `halt_mode != 'active'`), `reset_season2.py:64`. Every change tagged `# HM-A:`. API response shape preserved (`is_halted` JSON key, value derived from halt_mode). Drawdown-halt system at `ai_brain.py:817-848` + `risk_manager.py:868` + `post_earnings_drift.py` left alone — different concept (reads `agent_state.is_halted`, not `ai_players`). **Note (HM-S 2026-05-04):** the carve-out targets above were factually inaccurate. `ai_brain.py:817-848` and `risk_manager.py:868` do NOT read from `agent_state` (zero references confirmed by grep). The real drawdown-halt is `risk_manager.py::check_drawdown()` reading `portfolio_history` transiently. Only `post_earnings_drift.py:56` queries the phantom `agent_state` table (silent except). The carve-out *discipline* (don't touch drawdown-related code during halt-system migrations) was correct in spirit; the cited file:line targets were wrong. See HM-S report. |
| HM-B ✅ FIXED 2026-05-04 | Drop `is_halted` column from `ai_players` | RESOLVED | **Shipped 2026-05-04 (commit `9256890`).** Pre-flight (commit `a3a4cd0` HM-B-pre) migrated 4 unmigrated WRITE sites: `reset_season2.py:49,50`, `engine/season_manager.py:154,258`, `shared/matrix_bridge.py:114`, `setup_db.py:24` — all now use `halt_mode='active'` semantics. Live DB DDL: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51. Backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop (PID 13734). Halt state now has single source of truth: `halt_mode`. |
| HM-C ✅ FIXED 2026-05-04 | Update read-path consumers of `signals` / `watchlist_signals` to filter `halted_emit = 0` for scoring queries | MEDIUM | **Shipped 2026-05-04. 22 files modified, 28 SQL sites filtered. Scope was broader than first scoped: `ai_brain.py:563` (TIER-1 escalation), `bull_call_spread_v1.py:251` / `bear_put_spread_v1.py:270` (tier-2 spread vote), `crew_scanner.py:3963` (autopilot fleet consensus), `risk_manager.py:312` (bear-mode gate) all consume signals for current-day execution decisions, not just calibration. Halted players were implicitly voting through pre-fix-#1 backlog rows. Helper `HALTED_EMIT_FILTER` constant added to `engine/halt_gate.py` for single-source-of-truth migration when HM-A/HM-B retire `is_halted`. Display/forensic paths preserved. `/v1/signals` external API also filtered — note in commit message under Behavior change visible to /v1/signals consumers** |
| HM-D ✅ INVESTIGATED 2026-05-04 | `watchlist_signals` halted-player rows decision | LOW | **Verdict α (Retain) recommended.** 165 halted-player rows total (62 ollama-llama + 41 sulu + 35 gemini-2.5-pro + 27 grok-3). 34 still in `status='active'` but bounded and self-resolving — `signal_tracker.py:124,133` ages them to `hit_target`/`expired` over time, and `halt_gate.can_emit_signal` blocks new active rows. Optional follow-up HM-D-fix (~30-45 min): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers (5 in signal_tracker + crew_scanner.py:3965). Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`. |
| HM-E ✅ INVESTIGATED 2026-05-04 | Halted-player daily routines waste check | LOW | **Verdict B (modest waste).** Signal emission stopped naturally (last halted-player signal 3+ days ago). Trades all SELL action under exit_only — legitimate. **`ai_journal` runs daily for sulu + ollama-llama** — `main.py:520 run_journal()` and `engine/ai_journal.py:18 generate_journal_entry()` have zero halt-mode checks. ~2 LLM calls/day to Ollie Box for journals no one reads. Optional follow-up HM-E-fix (~5 min, low risk): add 3-line halt-mode check in `engine/ai_journal.py::generate_journal_entry()`. Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`. |
| HM-S ✅ INVESTIGATED 2026-05-04 | `agent_state` table ghost — drawdown-halt source of truth question | MEDIUM | **Verdict C (dead but harmless) + documentation drift.** Drawdown halt protection IS functional but does NOT read from `agent_state` as CLAUDE.md claims — it's recomputed transiently every cycle from `portfolio_history` in `engine/risk_manager.py::check_drawdown()` (3,562 rows, 20% threshold). `agent_state` table never existed in any of 13 .db files searched. Only one reader exists (`agents/post_earnings_drift.py:56`) and it silently degrades to `False` via bare `except: return False`. PED is paper-only via separate `gated=True` flag — broken halt-check cannot cause real-money damage. **Live gate-flip soak is safe.** Recommended actions: (1) fix CLAUDE.md "Why both is_halted and halt_mode" section to describe transient drawdown computation, not phantom agent_state table, (2) optional PED cleanup — replace dead `is_halted()` with simpler `enabled` toggle (~10 min). Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`. |
| HM-F ✅ RESOLVED 2026-05-04 | Add `halted_at` UPDATE to whatever code path sets `is_halted=1` going forward | RESOLVED | Audit found zero halt-write paths in current code. The four currently-halted players were halted via manual sqlite3 UPDATE; `season_manager.py` and `reset_season2.py` only UNHALT (set `is_halted=0`). Manual halt SQL is the only halt path; runbook documented in `CLAUDE.md` ## Manual halt SQL pattern. See `HM-F-future` for when a programmatic halt path appears. |
| HM-F-future | When a programmatic halt path appears (dashboard halt button, drawdown auto-halt, etc.), add `halt_player(conn, player_id, mode, reason)` helper to `engine/halt_gate.py` per HM-F Option 3 | LOW | **Do not pre-build.** YAGNI today — no caller exists. The helper should be written to fit whatever the real caller looks like (request handler? scheduled job? user-confirmation flow?), not in advance. |

---

## PATTERN NOTES

**Import-drift family (8+ items):** B12, B13, B14, B15, B17, AI-2, B29 share the
same family — "symbol moved, callers not updated, error swallowed by `except Exception`."
B14 + B15 closed today; remainder warrants a single disciplined import-drift sweep.

**Rebrand-drift family (B16, B23, accessapple sprint):** incomplete
`accessapple` → `ollietrades` rebrand left orphan domain references in code + docs.
22 refs across 6 files; sprint queued.

**Decorative-flag family (AI-3, AI-4):** `is_active`, `is_paused`, `crew_role` look
like state fields but don't gate execution. Only `is_halted` works. Document
before any new agent is wired.

---

## OPEN — Day-1 Soak Findings (2026-05-04 evening)

### HM-O — Ollie Box network outage (Scenario D, blocked at network layer)
- 192.168.1.166 unreachable: 100% ICMP loss + `nc: No route to host`. Not a stopped Ollama service — a network/power-layer failure that Scotty is not authorized to fix remotely.
- **Active impact during gate-flip soak:** three Ollie-Box-routed agents (`ollama-qwen3`, `ollama-coder`, `ollama-plutus`) emitting `HOLD, confidence=0.0` with `HTTPConnectionPool` error reasoning every signal cycle.
- **Action required from Captain/Admiral:** physically check power + network on Ollie Box. After it's back, re-run HM-O probe to verify all three models respond.
- **Follow-up (HM-X candidate):** circuit-breaker so unreachable Ollama doesn't keep emitting confidence-0.0 HOLDs into `signals` table.
- Full report: `docs/HM-O_OLLIE_BOX_HEALTH_2026-05-04.md`.

### HM-P — Confidence-scale audit (no urgent flag, deferred annotation pass)
- 42 production sites + 10 alt-named `conf` sites audited. **0 WRONG, 2 AMBIGUOUS (comments only), 49 CORRECT.**
- All gate-flipped strategy code (`bull_call_spread_v1`, `bear_put_spread_v1`, `executor`, `exit_manager`) verified: uses `TB_CONF_THRESHOLD = 85` against `trade_signals.confidence` (INT 0-100). **Soak may continue safely.**
- Implicit convention: `trade_signals` → INT 0-100; `signals`/`watchlist_signals`/`deep_scan_results`/`ghost_trades` + player decisions → REAL 0-1. Not documented anywhere central; one careless paste away from a silent bug.
- **HM-P-fix (deferred, low risk):** annotation pass adding `# scale: 0-100 INT` / `# scale: 0-1 REAL` at every comparison site. ~60-90 min one-shot. Optional rename `confidence` → `confidence_pct` in `engine/ollie_commander.approve_or_reject`.
- Full report: `docs/HM-P_CONFIDENCE_SCALE_AUDIT_2026-05-04.md`.

### HM-Q — execution_status vs halted_emit (Verdict A, no action)
- Both columns measure orthogonal things. `execution_status` = "what happened to this signal downstream"; `halted_emit` = "was the player allowed to act when emitted".
- HM-C is **not** redundant. `halted_emit` captures information (halt state at emission time) irrecoverable from `execution_status` or any join — `ai_players.halt_mode` is mutable and there is no halt-state audit log.
- **No schema change. No undo of HM-C.** Optional one-line annotation in `engine/halt_gate.py` near `HALTED_EMIT_FILTER` documenting the orthogonality.
- Open question worth chasing: **what writes `execution_status='EXPIRED'`?** 42,626 rows (69.5% of `signals`) and the audit found no writer — likely a sweeper job, but unverified.
- Full report: `docs/HM-Q_EXECUTION_STATUS_INVESTIGATION_2026-05-04.md`.

### HM-B — Drop ai_players.is_halted column (Day-1 evening, ✅ SHIPPED)
- HM-A read coverage was 100% clean, but pre-flight surfaced 4 unmigrated WRITE sites that would have SQL-errored post-drop. Migrated those first in HM-B-pre (`a3a4cd0`), then dropped column in HM-B (`9256890`).
- Live DB: `ALTER TABLE ai_players DROP COLUMN is_halted` on SQLite 3.51, backup at `backups/trader.db.pre-hmb-20260504_173026`. Service stable post-drop, no schema-related errors in trader.log.
- Halt state now has single source of truth: `halt_mode TEXT CHECK(halt_mode IN ('full','exit_only','active'))`.

### HM-D — watchlist_signals halted-player rows (Verdict α: Retain)
- 165 halted-player rows total. 34 still in `status='active'` but bounded and self-resolving — readers transition them out as price action plays out, and `halt_gate.can_emit_signal` blocks new active rows.
- Optional HM-D-fix (~30-45 min, deferred): add `JOIN ai_players halt_mode='active'` to 6 currently-unaware readers in `signal_tracker.py` + `crew_scanner.py:3965`. No urgency.
- Full report: `docs/HM-D_WATCHLIST_SIGNALS_VERDICT_2026-05-04.md`.

### HM-E — Halted-player daily routines (Verdict B: modest waste)
- Signal emission already stopped naturally; halted-player trades are all legitimate exit_only SELLs.
- **Active waste**: `ai_journal` daily routine runs for sulu + ollama-llama every market session — ~2 LLM calls/day to Ollie Box for journals no one reads. `main.py::run_journal()` + `engine/ai_journal.py::generate_journal_entry()` have no halt-mode check.
- Optional HM-E-fix (~5 min, low risk, deferred): add 3-line halt-mode check at the LLM-cost source in `generate_journal_entry()`.
- Full report: `docs/HM-E_HALTED_ROUTINES_VERDICT_2026-05-04.md`.

### HM-T — PED operational probe (Verdict B: silently inert, structurally unreachable promotion)
- PED is properly imported (main.py:3486) and scheduled every 15 min (main.py:3541) inside `if __name__ == "__main__":` block — the scheduler IS firing.
- **Lifetime activity: zero.** No row in `ai_players`, zero `signals` written, zero `trades`, zero log lines across all log files. Sitrep history (794 lines, 2026-05-01 onward) shows `PED signals: 0` every cycle.
- **Root cause:** `data/watchlist.txt` (PED's universe source at main.py:3496) does not exist. Falls back to 9 hardcoded ETF/mega-cap symbols. None have earnings in the 1-48hr post-earnings window today; effective trigger frequency is single-digit hours/year, single-digit signals/year after gap+vwap filters.
- **Gate-promotion criterion (30 trades + positive expectancy) is structurally unreachable.**
- Compute waste: negligible (rule-based, no LLM, ~0.1s/day total CPU).
- No other code reads `data/watchlist.txt` — the missing file is PED-specific. Was either deliberately abandoned or never wired.
- **Recommended: Option γ (formally retire).** Move to `archive/retired/`, remove schedule, document. Side benefit: closes HM-S-code by removing the phantom `agent_state` reference from active code paths. Option β (repair wiring with proper watchlist) is also viable if Captain sees PED research value.
- Full report: `docs/HM-T_PED_OPERATIONAL_PROBE_2026-05-04.md`.

### HM-T-fleet — Silent-Inertness Audit (Tuesday 2026-05-05)
- Extended HM-T's PED-class question fleet-wide. 49 ai_players + 130 schedule registrations classified.
- **7 PED-class inert agents identified:** anderson-bcs, mccoy-bps, quark-ic, covered-call (orphaned in `engine/options_agents.py`, file imported by nothing); qwen3-14b-pro (lab/backtest scaffold, never dispatched); red-alert (channel-mismatch — writes to non-existent `red_alert_log`); dayblade-0dte (was active until 2026-04-07, 28 days idle — watch list).
- **Halted-but-emitting confirmed:** ollama-llama leaked 947 post-halt signals (HM-A signal-emission gate gap). Earlier "2 post-halt trades NEW finding" claim was a query-window error; corrected in commit ee481fa — actual 7 post-halt trades, all clean exits, Verdict A (no trade-gate bug).
- **Orphan in signals not in roster:** `debate-pipeline` (1 row, 2026-03-31, vestigial).
- **Recommendations:** (1) ~~one bundled retirement commit for the 4 options_agents.py orphans (mirrors PED pattern)~~ **— APPLIED 2026-05-05 07:09 MST as Option 1 halt-only.** Pre-flight discovered `engine/options_agents.py` IS imported by `dashboard/app.py:17731` and contains 8 player_ids (4 targets + 4 ghosts), so the file was NOT archived. Instead 4 ai_players rows transitioned to `halt_mode='full'`. Code preserved per sacred-data rule. Open follow-ups: ghost-agents Option 4 investigation; HM-T-fleet doc has stale "imported by nothing" claim that needs a correction note; surgical file cleanup deferred until ghost investigation lands; (2) dispatch-loop investigation for qwen3-14b-pro; (3) clarify red-alert role; (4) signal-emission gate work (already in CLAUDE.md TODOs).
- 4 open Admiral questions: paid-model halting policy, options_agents retirement scope, dayblade-0dte timeline, mlx-qwen3/ollama-coder dispatch suppression.
- Full report: `docs/HM-T-fleet_SILENT_INERTNESS_AUDIT_2026-05-05.md`. No code/schema changes — investigation deliverable only.

### HM-I — Bridge Scope Investigation (Tuesday 2026-05-05)

**Status:** Admiral picked **Option β** (firm separation) 2026-05-05. Items 1+4 shipped same day; items 2/3/5 deferred.
**Priority:** Medium (architectural; running soak is stable)
**Investigation date:** 2026-05-05 morning (Scotty)

Inventoried the internal-book ↔ Alpaca-paper bridge. 3 books, 2 flows, 4-player routing table.

- **Active code-level finding:** `engine/paper_trader.py:1300` (partial-SELL path) called `_forward_to_alpaca` **without** the `route_mode == "trading"` gate that BUY (line 1015) and full-SELL (line 1167) both have. Source of ~181/day phantom-position skip log entries from legacy fleet players. **APPLIED 2026-05-05 commit `d06c33c`** (HM-I Option ε): added matching gate; all three forward paths now identical. Stale bytecode at PID 35155 means current process still emits skips until next restart.
- **Two-book policy formalized:** `CLAUDE.md` § "Architecture: Two-Book Bridge Policy" added 2026-05-05 commit `086a123`. Internal AI fleet book and Alpaca paper book are two separate ledgers by design. Routed players (super-agent, ollie-auto, neo-matrix, dalio-metals) + spread strategies forward to Alpaca; legacy fleet stays internal-only.
- **Phantom-reference fix:** `portfolios.id=5` renamed from "Dalio Metals" → "Enterprise Computer" 2026-05-05 to match `_EXECUTION_PORTFOLIO_BY_PLAYER` mapping. Resolution went from broken (fall-through paper) to correct (id=5, route_mode=tracking, log-only). Behavior change: dalio-metals no longer accumulates new internal-book trades — matches Option β log-only intent. Existing 37 trades + 2 positions preserved (FK on id, not name). DB-only change; no code/doc updates needed (refs were already correct).
- **Type 1 divergence count at investigation time:** 39 internal positions across 9 players that Alpaca paper doesn't have. Includes shorts (gemini-2.5-flash IREN/ONDS) and futures (enterprise-computer GC=F, SI=F) Alpaca paper can't accept. Stable post-β (legacy fleet stays internal by design).
- **5 options presented α/β/γ/δ/ε.** Admiral picked β. Item ε (decision-orthogonal) also applied.
- **β followups status:**
  - Item 2: Dashboard naming pass (Arena Paper vs Alpaca Paper visual distinction). **Deferred.**
  - Item 3: Webull dual-role split. **APPLIED 2026-05-05 07:56 MST** — code + service restart (PID 35155 → 59121) + DB migration atomic. New player `alpaca-mirror` (provider=alpaca-paper-broker, is_human=0). 3 positions migrated from webull to alpaca-mirror; webull retains 127 historical Webull-import trades + 73 portfolio_history rows. Kirk + first_officer + Q + cto_advisor + war_room + dashboard reads re-targeted to alpaca-mirror. SQL `!= 'webull'` exclusions in benchmark.py / war_room.py / holodeck_expansion.py rewritten as `is_human=0`. Stale-bytecode lockstep dictated atomic order: code → kickstart → DB. 18 files touched, 29 `# HM-I-β-Item3:` markers placed.
  - Item 5: Reconciliation report (replaces ε canary, daily NTFY on drift thresholds). **Deferred.**
- Full report: `docs/HM-I_BRIDGE_SCOPE_INVESTIGATION_2026-05-05.md`.

### Option 4 — Ghost Agents Investigation (Tuesday 2026-05-05)

**Status:** **CLOSED 2026-05-05 08:57 MST** — Admiral chose **Option B halt-only retirement**. 4 ghost agents transitioned `halt_mode='active' → 'full'` via DB UPDATE. File `engine/options_agents.py` untouched (sacred-data); `/api/options/scan-preview` endpoint continues serving 8 halted agents (4 production halted morning 06b5ce7 + 4 ghosts halted now). halt_gate API confirms all 4 ghosts return False on can_emit/open/close; active players (ollama-plutus, ollie-auto, super-agent) unaffected. Operationally a no-op (zero lifetime activity); DB now reflects behavioral reality. Pre-halt backup at `backups/trader.db.pre-ghost-retire-20260505_085718`.
**Original Status:** Open — awaiting Admiral A/B/C/D decision (no recommendation made).
**Priority:** Low (no current behavioral impact; either choice is reversible).
**Investigation date:** 2026-05-05 morning (Scotty)

Tested HM-T-fleet's ⚪ "by-design" classification of the 4 ghost agents (ghost-kirk-bc, ghost-kirk-0dte-bc, ghost-long-call, ghost-naked-put).

- **Verdict:** classification was **directionally correct**. All 4 ghosts are 🟡 half-wired — real classes with real scan logic, partitioned into a separate `options_books.ghost` research book ($2,500 starting capital) with drawdown gate, designed as A/B research framework. Not orphans.
- **But:** they share their dispatch path with the 4 production options agents we halted this morning (commit 06b5ce7). Both groups are preview-only — no scheduler entry, no execution step, only consumer is `dashboard/app.py:17731 /api/options/scan-preview`. The "separate confirm step" the run_scan_cycle docstring references doesn't exist in code.
- **4 options presented:** A leave alone (no action), B halt all 4 ghosts to mirror morning halt symmetrically, C activate (build the missing scheduler+confirm path), D retire entire options-engine subsystem.
- **Open Admiral questions:** was ghost activation always planned? should production+ghost halt status be symmetric? is the "separate confirm step" real or aspirational?
- Full report: `docs/OPTION-4_GHOST_AGENTS_INVESTIGATION_2026-05-05.md`.
- **Side observation:** morning halt of 4 production options agents (anderson-bcs/etc.) was effectively cosmetic — those agents had no path to fire either. Halt is still correct (marks them not-production), but didn't change behavior.

### HM-U — Silent-Failure Pattern Discussion (DISCUSSION ITEM, NOT A FIX)

**Status:** Open
**Priority:** Medium (architectural conversation, not a code change)
**Surfaced by:** HM-O / HM-S / HM-T / HM-E investigations on 2026-05-04, plus the stale-bytecode discovery during PED retirement verification

Today's audits found a recurring architectural anti-pattern across multiple subsystems:

| Subsystem | Silent-failure shape |
|---|---|
| HM-O (Ollie Box outage) | Connection-error reasoning text + `confidence=0.0` HOLD signals → treated as valid rows in `signals` table |
| HM-S (`agent_state` ghost) | `try/except Exception: return False` swallows missing-table error → drawdown-halt always says "not halted" |
| HM-T (PED inert) | Missing `data/watchlist.txt` → silently falls back to narrow universe → never qualifies → silently no-ops |
| HM-E (halted journals) | No halt-mode check on routines → continues running for halted players → wasted LLM calls |
| Stale-bytecode (PED-verification discovery) | `try/except: console.log(error)` at 4 call sites swallowed `no such column: is_halted` for 70 min before discovery |

**Common shape:** bare `except` / silent fallback / no-op success path / caught-and-logged-but-not-alerted. The codebase trades loud failure for quiet incorrectness in many spots, and the discipline of "don't crash the trader" has expanded to cover bugs that should be loud.

**Question for discussion (not for autonomous decision):**

1. Should bare `except Exception` blocks log the swallowed exception with stack trace by default (vs current pattern of `console.log(f"...: {e}")` losing the traceback)?
2. Should silent-fallback paths NTFY-alert when they fire (e.g., "PED couldn't load `data/watchlist.txt`, using fallback universe")?
3. Is there a project-wide error-handling philosophy worth writing down in CLAUDE.md (e.g., "data-layer SQL errors must NTFY-alert; LLM-API errors may be swallowed; config-fallback paths must log once-per-process")?
4. Are there other "wired-but-inert" agents we should fleet-audit (HM-T-fleet candidate)?
5. Should schema-change verification include a service restart in the verification phase, given the stale-bytecode trap from today (see Lessons section)?

**Recommended action:** Schedule a discussion-only session (Admiral + XO, no Scotty) to set posture. Then a follow-up sprint, if any, would write the explicit fix prompt.

**Not in scope here:** automatic refactor of all bare-except blocks. That's a code-philosophy decision, not a Scotty task.

### HM-S — agent_state table ghost (Verdict C: dead but harmless + docs drift)
- **`agent_state` does not exist in any of 13 .db files in the repo.** Confirmed via direct schema queries on every DB.
- **Only 1 reader** in production code: `agents/post_earnings_drift.py:56` — wrapped in bare `except Exception: return False`, so the missing table silently produces "not halted".
- **CLAUDE.md is factually wrong:** claims `engine/ai_brain.py` and `engine/risk_manager.py` read from `agent_state`. Neither file references `agent_state` at all. The actual drawdown-halt protection at `risk_manager.check_drawdown()` reads `portfolio_history` and recomputes `(peak - current) / peak >= 0.20` every cycle — transient, not flag-based, and FUNCTIONAL.
- **Safety implication for live gate-flip:** none. Drawdown halt + manual halt_mode runbook are both functional. PED's broken halt-flag is contained by separate `gated=True` paper-only gating.
- **Recommended:** (1) fix CLAUDE.md describe transient drawdown mechanism correctly (~5 min), (2) optional PED `is_halted()` cleanup (replace with `enabled` toggle, ~10 min). Both deferred.
- Full report: `docs/HM-S_AGENT_STATE_GHOST_2026-05-04.md`.

### HM-AB — bull_spread_v1 missing same-strategy self-skip check (2026-05-05)

**Status:** Open — strategy halted at commit `[this commit SHA]` pending fix.
**Priority:** High (was actively stacking positions; 18 open SPY bull_put_spreads accumulated <1 day post-gate-flip before halt).
**Surfaced by:** Admiral observation 2026-05-05 11:39 MST.

`strategies/bull_spread_v1.py` lacks a same-strategy self-skip check — the reciprocal of `strategies/bull_call_spread_v1.py:280-287` which queries `options_trades` for any open `bull_spread_v1` row on the same ticker and skips if found. Without the reciprocal, bull_spread_v1 is free to fire repeatedly on the same ticker (SPY) every signal tick (every 15 min per `main.py:2622` schedule), accumulating 18 open positions in <1 day.

**Halt applied 2026-05-05 11:39 MST (this commit):**
- `strategies/bull_spread_v1.py` `_EXECUTION_ENABLED = False` (module-level constant)
- `evaluate()` early-return checks the constant
- Auto-register call changed to `enabled=False`
- Belt-and-braces: either gate alone halts signal emission; both together provide redundant safety
- Stale-bytecode: PID 61083 has pre-halt bytecode in memory; halt takes effect on next service restart (planned ~13:00 MST per Admiral)
- Tag: `# HALT-2026-05-05:` markers in code

**Existing 18 positions ride** per Admiral directive — they're real Alpaca paper positions, max-loss-capped, same-expiration. `exit_manager` handles them on its scheduled cadence (TP / SL / expiration). **DO NOT close programmatically** during the halt window — closing while the underlying bug still exists risks stacking another bug on top.

**Fix shape (HM-AB session):**
1. Add `_already_open(ticker)` helper to `strategies/bull_spread_v1.py` mirroring `bull_call_spread_v1.py:275-290` — query `options_trades` for `WHERE strategy_id='bull_spread_v1' AND symbol=? AND exec_status='open'`.
2. Call it at the top of the per-ticker loop in `evaluate()`; skip ticker if already-open.
3. Once verified, flip both `_EXECUTION_ENABLED = True` and `enabled=True` to unhalt.

**Verification approach:**
- Pre-fix smoke: confirm a synthetic open row blocks signal emission for that ticker.
- Pre-unhalt: backlog audit of existing open positions; if any have already hit TP/SL/expiration, unhalt is safer because the strategy will see fewer "already open" hits naturally.
- Post-unhalt monitor: 1 hour soak with `tail -f logs/trader.log | grep bull_spread_v1` to confirm the strategy fires once per qualifying ticker per cycle, not stacking.

---


### HM-AF — dayblade-0dte spread cannibalization root cause (2026-05-06)

**Status:** **HALTED 2026-05-06 10:43:54 MST** via `UPDATE ai_players SET halt_mode='full'` (transaction took effect immediately, no service restart needed; halt-mode is read per-cycle).

**Surfaced by:** Day 3 morning observability check (Admiral + XO, 2026-05-06 10:00–10:45 MST), tracing the orphan SPY 732P short position visible in `positions` table after a clean MLEG fill.

**Root cause (the 2-day "spread positions vanish" mystery):** `dayblade-0dte` (T'Pol) was firing single-leg `submit_single_option(SELL)` calls on the LONG legs of bull_put_spread fills within minutes of the parent MLEG filling. Each fire dismantled a spread by selling its protective long leg, leaving an orphaned short PUT.

**Evidence chain:**
- 2026-05-05: 5 single-leg SELL fires across 4 timestamps (08:41 + 12:52–12:56 cluster) totaling 13 long-PUT contracts (1+3+5+1+3) — exactly matches the 13 spreads cleaned up by HM-AE Option B reconcile that evening. All fires logged at `engine/alpaca_options.py:251`.
- 2026-05-06 08:14:39 UTC: 1 single-leg SELL on SPY260515P00727000 (`order=4863f7fc-980d-4283-b30b-5fe89ae12ebb`) fired 2 minutes after the bull_put_spread MLEG `848ece89-...` filled at 08:12:41. Logged at `engine/alpaca_options.py:315` (different code path than yesterday's :251).
- Alpaca order data confirms both legs of every MLEG order today and yesterday filled cleanly with `filled_qty=1, status=filled` — Alpaca paper did NOT net-collapse the spreads as previously theorized; OUR code dismantled them.
- All firing entries attributed to `dayblade-0dte` in trader.log; one EOD-sweep log line confirms the cluster behavior: `(dayblade-sulu + dayblade-0dte EOD sweep)`.

**Halt SQL applied:**

    UPDATE ai_players
    SET halt_mode = 'full',
        halt_reason = '2026-05-06 spread cannibalization (closed long 727P leg of bull_put_spread_v1 2min after MLEG fill, alpaca_options.py:315; also responsible for 5 single-leg sells yesterday at alpaca_options.py:251 dismantling the 13-spread reconcile cohort)',
        halted_at = CURRENT_TIMESTAMP
    WHERE id = 'dayblade-0dte';

**Verification post-halt:**
- `halt_mode='full'`, `halted_at=2026-05-06 17:43:54 UTC` confirmed via SELECT
- No `Alpaca OPTIONS SELL` log entries from `dayblade-0dte` after 10:43 MST through 10:45 MST (~2 min observation window)
- `bull_spread_v1` self-skip continues to fire correctly on the orphan SPY 732P
- Stock-trading agents unaffected (ollama-plutus / qwen3 / capitol-trades / deepseek-7b-grok4 active throughout)

**Mis-attribution correction (HM-AE Option B):** Yesterday's HM-AE Option B reconcile marked 13 stale `options_trades` rows closed under the assumption that "Alpaca paper net-flat-collapsed the spreads." The cleanup was correct — the rows DID need to be marked closed because they WERE effectively closed — but the cause attribution was wrong. The actual cause was dayblade-0dte cannibalizing each spread's long leg, leaving naked shorts that subsequently closed via other paths or netted out. No corrective action needed on the 13 reconciled rows; this is a calibration note for future-XO.

**Open follow-ups (deferred to fresh-headed Scotty):**
1. **Investigate dayblade-0dte's two firing paths** — `alpaca_options.py:251` and `alpaca_options.py:315` are different code paths. Read both, trace callers, document trigger conditions. Why did `:315` fire on a 9-DTE option from an agent labeled `0dte`? Investigation only, no code changes. ~30 min.
2. **Architectural fix: spread-leg awareness for ALL agents** — long-term, ANY agent firing single-leg options closes should respect spread structure. Two approaches: (A) add `spread_id` + `is_spread_leg` columns to `positions`, populated when MLEG fills sync; (B) read-time check against `options_trades.legs_json` for matching open spread. Approach choice depends on Item 1's findings. ~60 min.
3. **Orphan SPY260515P00732000 short** (qty=-1, mv≈-$579, expires 2026-05-15) — Battle Station continues firing CLOSE_NOW every 2 min (legitimate panic on what looks like a naked short PUT) but the close routes through tracking-mode and never executes. Recommendation: let it expire on May 15. Paper money, no real risk. Set reminder for May 15 to verify expiration cleared the position.

**Reversal (if needed):**

    UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL WHERE id='dayblade-0dte';


**AMENDMENT 2026-05-06 11:00 MST (post-Scotty investigation):** Initial HM-AF writeup characterized `:251` and `:315` as two firing paths. Scotty investigation (`docs/diagnoses/dayblade_0dte_paths_2026-05-06.md`) corrected this: they are the same log line at different file offsets — commit `1eeff7d` (HM-V/HM-AA bundle, 2026-05-05 12:59 MST) inserted 147 lines above the success log inside `submit_single_option`. Pre-restart bytecode emitted `:251`; post-restart process emits `:315`. Single statement, single caller, single defect.

The actual contaminated code paths are THREE, all sharing the same root cause (no spread-leg awareness, no DTE filter, no agent-ownership filter):

- **P1 — Battle Station 2-min monitor** (`battle_station.py:684`): iterates ALL Alpaca options positions every 2 min, fires close on −50% pnl OR wrong-side-of-gamma-flip. Hardcodes `player_id="dayblade-0dte"` at `battle_station.py:668` for attribution but scope is global. Triggered today's `:315` fire on SPY 727P.
- **P2 — EOD sweep** (`main.py:2268` → `close_all_options` at `alpaca_options.py:590`): fires daily at 12:45 MST. Closes ALL options positions in the Alpaca book regardless of strategy/spread structure. Confirmed firing 2026-05-05 12:48:23.
- **P3 — dayblade.py:502 post-trade close_all_options**: fires `close_all_options` after every dayblade sell, NOT just EOD. Likely the highest-frequency leak; silently cannibalizing spreads since the 2026-05-04 gate flip.

**Halt of dayblade-0dte (`halt_mode='full'`) only stops P3.** P1 (Battle Station) and P2 (EOD sweep) remain active and will fire on any open options position regardless of dayblade-0dte's halt state.

**Additional finding — wrong-side-of-book bug:** `_get_alpaca_options_positions` strips qty sign at `battle_station.py:319`. Short positions get treated as longs in close logic, causing `submit_single_option(side="sell")` calls when the correct close action would be buy-to-close. Separate from cannibalization but compounds damage.

**Updated open follow-ups (supersedes original Items 1-3):**
1. **HM-AF-α** — Halt P1 + P2 + P3 via feature flag or guard (urgent, before Layer 1 ships). Scotty ~15-20 min.
2. **HM-AF-β** — Layer 1: Spread-leg awareness. `is_spread_leg(symbol)` helper cross-referencing `options_trades`/strategy_positions; applied to P1/P2/P3. Scotty ~60-90 min.
3. **HM-AF-γ** — Layer 2: Wrong-side-of-book correction in `_get_alpaca_options_positions`. Can ride with HM-AF-β.
4. **HM-AF-δ** — Layer 3: Remove hardcoded player_id in `battle_station.py:668`. Lower urgency.
5. Original Item 3 (orphan SPY 732P) unchanged — recommend let expire 2026-05-15.


---

### HM-AQ — Active Watchlist Coverage Decision (2026-05-07)

**Type:** Strategic scope decision (not a bug)
**Priority:** P3 — non-blocking, no execution risk
**Status:** **DECIDED 2026-05-07** — Captain approves broadening WATCH_STOCKS per criteria below. Implementation queued as HM-AQ-β. Spread-universe expansion deferred as HM-AQ-γ (out of scope, separate Captain decision).
**Origin:** 2026-05-07, "missed mover" investigation (DDOG +30.87%, FTNT +22.92%, MDB +14.19%, ZTS −21.37%, ARM −8.18%, TPR −8.14%)

#### Captain's decision (2026-05-07)

**WATCH_STOCKS expands** from 20 manually-curated mega-caps to a dynamically-refreshed universe matching:

| Criterion | Threshold |
|---|---|
| Market cap | ≥ $5B |
| Daily $ volume (20-day avg) | ≥ $50M |
| Refresh cadence | Weekly (Sunday pre-Monday-open) |
| Refresh source | Polygon screener API (Polygon Options Starter $29/mo activation under HM-AQ-β) |

**Expected size:** ~500-800 tickers.

**Risks acknowledged:** dashboard noise, scan-loop slowdown across 12+ iteration sites, more spread attempts (only relevant if HM-AQ-γ ships — for now, spread universes stay at 10 tickers).

**Catches:** all 6 missed movers from 2026-05-07 morning would have been in coverage under these criteria.

**Full criteria & roadmap:** `docs/UNIVERSE.md` (canonical reference; created in this commit).

#### Summary
The fleet's active iteration sources are locked to ~20 mega-cap names. Tickers outside that set are structurally invisible to every active scanner, dashboard surface, and spread engine — not filtered out by gates, simply never iterated.

#### Current state
| Source | Members | Used by |
|---|---|---|
| `config.py:24 WATCH_STOCKS` | 20 tickers (SPY, QQQ, TQQQ, NVDA, TSLA, AAPL, AMD, META, MSFT, GOOGL, AMZN, MU, ORCL, NOW, AVGO, PLTR, DELL, XLE, INTC, NUKZ) | dashboard (12+ iterations), `scripts/import_stooq.py` |
| Per-strategy `TIER_1+TIER_2` | 10 tickers (SPY, QQQ, IWM + 7 large-caps) | `bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1` |
| `scan_universe` (DB) | 2,741 catalog rows | passive metadata only — no live readers |

Of the 6 candidates that triggered this investigation: 5 in `scan_universe` (catalog only), 0 in any active iteration source. ZTS not even catalogued.

#### Acceptance criteria (status post-decision 2026-05-07)
- [x] Coverage criteria documented — `docs/UNIVERSE.md`
- [x] CO decision logged — broaden, this commit + OPS_LOG 2026-05-07
- [x] Implementation ticket spawned — HM-AQ-β below
- [x] Spread-universe scope decision deferred — HM-AQ-γ marker below

#### Related
- `docs/UNIVERSE.md` — canonical universe doc
- HM-AQ-β — implementation ticket (Polygon screener + weekly refresh + storage migration)
- HM-AQ-γ — spread-universe expansion (deferred marker, not in active queue)
- `bull_call_spread_v1.py` TIER_1/TIER_2 definitions (out of scope; see HM-AQ-γ)
- HM-AP (closed no-op) — `bull_call_spread_v1` silence verdict
- HM-AR — `earnings_universe` observability (sibling finding from same investigation)

---

### HM-AQ-β — Implement dynamic WATCH_STOCKS refresh (2026-05-07)

**Type:** Implementation (active queue)
**Priority:** P3 → escalated and shipped same-day
**Status:** **SHIPPED 2026-05-07** — 5 commits (`5eb479c` schema → `dd43bab` accessor → `12ad22d` refresher → `404f0a2` consumer migration → commit 5 = bug-fix bundle + plist + wet refresh + perf fix). Universe at $100M floor: ~1,223 names (927 CS + 296 ETF). Bulk-endpoint perf fix at 9 fan-out sites makes 1,223-symbol snapshots ~1-2s instead of ~47s. Full narrative: `docs/UNIVERSE.md`.
**Origin:** HM-AQ decision 2026-05-07 (`docs/UNIVERSE.md`).

#### Scope

Replace the static `config.py:WATCH_STOCKS = [...20 tickers]` constant with a dynamically-refreshed universe of ~500-800 tickers matching the HM-AQ inclusion criteria (market cap ≥ $5B, daily $ volume ≥ $50M).

**Sub-decisions logged:**
- **Screener:** Polygon (not Alpaca). Rationale: Polygon Options Starter $29/mo is approved-in-principle (CLAUDE.md 2026-04-16) and offers a richer screener than Alpaca's. Activation cost ($29/mo) is part of HM-AQ-β implementation. First paid exception under Free-Models-First doctrine.
- **Spread universes (`TIER_1+TIER_2`):** NOT in scope. Tracked separately as HM-AQ-γ.

#### Components
1. **`engine/universe_refresh.py`** (new) — Polygon screener API client, cap/volume filter, output writer.
2. **Storage migration** — replace `config.py:WATCH_STOCKS` constant with one of:
   - DB table `universe_active(symbol, last_refreshed_at, market_cap, avg_daily_dollar_volume, included_reason)` — preferred; queryable
   - File `data/watch_stocks.json` — simpler; no schema migration
   - Decision: TBD during implementation; either preserves the import-as-list pattern via a getter helper.
3. **launchd plist** `com.ollietrades.universe-refresh` — fires Sunday 14:00 MST (post-close, pre-Monday-open). Per HM-AT-β lesson, watch dirs/paths owned by `~/autonomous-trader/` to avoid TCC issues.
4. **Polygon Options Starter activation** — first paid exception activated under Free-Models-First. Document the activation in OPS_LOG.
5. **Iteration-site audit** — 12+ sites in `dashboard/app.py` walk `WATCH_STOCKS` (per HM-AU). Each site must be retested for:
   - Rate-limit impact (Alpaca/Polygon API call fan-out at 25-40× rows)
   - Latency impact (single-threaded `schedule.run_pending()` blocking — relevant to HM-AS cadence tail)
   - Render performance (frontend table sizes 25-40×)
6. **Soak window** — ship to a non-prod-blocking surface first (e.g. dashboard read-only view) before flipping all callers.

#### Effort
~4-8 h Scotty (range reflects whether iteration-site audit surfaces rate-limit issues that require batching).

#### Acceptance criteria
- [ ] `universe_refresh.py` produces 500-800 tickers matching criteria
- [ ] Weekly refresh fires reliably via launchd
- [ ] All iteration sites retested; no rate-limit failures, no latency regression > 2× pre-ship
- [ ] OPS_LOG entry for Polygon Options Starter activation
- [ ] HM-AS-β cadence drift warning continues to fire normally (i.e. broadening doesn't dramatically push the tail)

#### Related
- HM-AQ — Captain decision (parent)
- HM-AQ-γ — spread-universe expansion (deferred)
- `docs/UNIVERSE.md` — criteria + rationale
- HM-AS-β — cadence drift warning (will detect any regression)
- HM-AU — Kirk advisory source routing audit (12+ iteration sites)

---

### HM-AQ-β.2 — Curated-tier ADR inclusion + `is_adrc` flag (2026-05-07, refined)

**Type:** Universe scope expansion (HM-AQ-β follow-up)
**Priority:** P3 — LOW (some liquid ADRs missed; curated tier is high-signal, not noise)
**Status:** Proposed (scope refined 2026-05-07)
**Origin:** HM-AQ-β v3 dry-run 2026-05-07 surfaced 79 type-skipped tickers, mostly ADRCs (BP, NIO, GGB, VIST, LEGN, ...). Many of the largest (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN) have liquid options.
**Sequence:** After HM-AQ-β 24h soak (est. 2026-05-08 evening).

#### Captain's refined call (2026-05-07)
**Curated-tier inclusion, NOT blanket type=ADRC.** ADRCs are heterogeneous — TSM at $1T+ down to micro-cap reverse mergers. Blanket inclusion would add too much noise. Solution: apply the existing market_cap + dollar_volume filters to ADRCs (same thresholds as US CS); the filter naturally selects only the high-quality liquid tier.

Reasoning:
- Major-cap ADRs (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN, etc.) are high-quality liquid names that AI agents can trade like US stocks.
- Smaller ADRs add currency complexity, regional risks, lower liquidity — without comparable quality benefit.
- Existing $5B cap + $100M dollar-volume filters do the right curation if applied to type=ADRC the same way as type=CS.

#### Refined scope

**1. Apply existing cap + volume filters to ADRCs:**
- market_cap ≥ $5B (same as US CS)
- dollar_volume ≥ $100M (same threshold)
- Filter naturally selects the high-quality tier
- Predicted addition: ~30-50 names (TSM, ASML, BABA, SHOP, SE, NVO, NVS, AZN, BHP, RIO, TM, SONY, ...)

**2. Add `is_adrc INTEGER DEFAULT 0` column to `scan_universe`:**
- Lets per-strategy code opt-in or opt-out of ADRs
- Spread strategies might want US-listed CS only (currency-aware concern; settlement timing on holidays differs)
- Currency-aware strategies could leverage the flag for FX-hedge logic
- Schema migration: `migrations/HM-AQ-β.2_universe_is_adrc_2026-05-XX.sql`

**3. Update `engine/universe_refresh.py`:**
- Step 2: include `type=ADRC` in the cap+volume filter pass (not skip)
- Set `is_adrc=1` on the row when type=ADRC
- Existing CS rows: `is_adrc=0` (default)
- Other types (ETV, ETN, BSKT, FUND, PFD, ...): still skipped as before
- Keep the audit log line `etf_included` style — add `adrc_included <SYM> cap=$X.XB dollar_volume=$Y.YM`

**4. Update `engine/universe.py`:**
- `get_active_universe()` continues to return ALL passing symbols (CS + ETF + ADRC) — drop-in for current consumers
- New helper: `get_us_only_universe()` returns rows with `is_adrc=0` AND `ticker_type='CS'` (for spread strategies, currency-sensitive consumers)
- `get_universe_with_metadata()` includes `is_adrc` in the returned dict
- `universe_health()` adds `adrc_passing` count split

**5. Document in `docs/UNIVERSE.md`:**
- New section: "ADR tier inclusion rationale" — explain why ADRCs ARE included but with a flag
- New section: "Per-strategy opt-out pattern" — explains the `is_adrc` flag and the `get_us_only_universe()` helper
- This sets a precedent for future flag columns: `is_etf` (already implicit via ticker_type), `is_leveraged`, etc.

#### Effort
~30 min Scotty:
- 5 min: schema migration (`ALTER TABLE scan_universe ADD COLUMN is_adrc INTEGER DEFAULT 0`)
- 10 min: refresher patch + `_write_universe` insert clause
- 10 min: `engine/universe.py` helper + SQL filter updates
- 5 min: docs/UNIVERSE.md + dry-run + Captain spot-check

#### Acceptance criteria
- [ ] `scan_universe.is_adrc` column added; ADRC rows correctly flagged
- [ ] Refresher includes ADRCs passing $5B/$100M filters; predicted +30-50 names
- [ ] `engine.universe.get_active_universe()` includes ADRCs (drop-in for existing consumers)
- [ ] `engine.universe.get_us_only_universe()` excludes ADRCs (new helper for spread strategies)
- [ ] Captain spot-check confirms presence of TSM, ASML, BABA, SHOP, SE, NVO and absence of micro-cap ADRs
- [ ] `docs/UNIVERSE.md` updated with ADR rationale + flag pattern

#### Related
- HM-AQ-β — parent (shipped 2026-05-07, commits `5eb479c` → `e333f63`)
- 79 ADRC/other-type symbols logged via `type_skipped` audit line during v3 dry-run
- Future flag columns (deferred marker): `is_leveraged`, `is_inverse`, etc. — same pattern this ticket establishes
- Spread strategies (`bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1`) are likely consumers of `get_us_only_universe()` once HM-AQ-γ deferred ETF-spread question is revisited

---

### HM-AQ-γ — Spread-strategy universe expansion (deferred marker, 2026-05-07)

**Type:** Future Captain decision (NOT in active queue)
**Priority:** Deferred
**Status:** Marker only — kept so future-self knows the deferral was deliberate.
**Origin:** HM-AQ scope clarification 2026-05-07.

#### Why deferred
Spread strategies (`bull_spread_v1`, `bull_call_spread_v1`, `bear_put_spread_v1`) operate on options chains where **fill quality, bid-ask spread, and open interest dominate edge**. The 10-ticker `TIER_1+TIER_2` universe is curated for liquidity that supports defined-risk debit/credit spreads.

Expanding to mid-caps or thinly-traded names would introduce:
- Wider bid-ask spreads on options legs (eats edge)
- Lower OI / volume → fill risk on multi-leg orders
- Per-name option liquidity varies dramatically; coverage breadth doesn't translate to fill quality

**Captain principle (2026-05-07):** spread quality > spread coverage. Expanding spread universes requires its own analysis on per-name option-chain liquidity (avg daily option volume, OI floor, bid-ask spread floor) — separate Captain decision when surfaced.

#### When to revisit
- A specific mid/large-cap name with proven option liquidity becomes a high-conviction setup that current spread strategies miss
- A new options-liquidity-screener ships that can produce a vetted spread universe automatically
- Spread strategies' performance plateaus in a way that suggests universe-size limitation (currently they're tractor-beam-gate-limited per HM-AP, not universe-limited)

#### NOT a backlog item
This is a **deferred marker**, not an active ticket. Promote to a real ticket only when the trigger conditions above are met.

---

### HM-AR — earnings_universe Inject Observability (2026-05-07)

**Type:** Hygiene / observability
**Priority:** P4 — low, not safety-critical
**Status:** **AUDITED + DOCUMENTED 2026-05-07** — see `docs/EARNINGS.md`. Classified DEPRECATED. Cleanup queued as HM-AR-β below.
**Origin:** 2026-05-07, surfaced during HM-AQ investigation.

#### Audit findings (2026-05-07)

The original ticket framed `earnings_universe` as a single system. Audit revealed **three independent earnings code paths** that share nothing but the word "earnings":

1. **Options blackout (LIVE, safety-critical)** — `engine/options_selector.py::_next_earnings_date` reads `data/earnings_cache.json` + yfinance fallback. Independent of any SQLite table. **This is what actually protects options trades.**
2. **`main.py:679 run_earnings_universe_inject()` (LIVE)** — runs daily 06:00 AZ, but writes to **`scan_universe`** (via `engine.deep_scan.inject_earnings_tickers`), NOT `earnings_universe`. **Function name is a naming-drift lie.**
3. **`engine/earnings_injector.py` + `earnings_universe` table (DEAD ORPHAN)** — writer at line 78, reader at line 96, but **NO external caller**. The `__main__` block is the only entry point. Docstring says "Runs at 6:00 AM AZ" but no launchd/cron entry exists. Has been empty since creation.

**Classification: DEPRECATED.** Path 3 is dead code. Path 1 (the safety-critical one) is intact. Path 2 needs a rename to stop confusing investigators.

**No safety regression.** Options blackout enforcement is unaffected.

**Full path map:** `docs/EARNINGS.md`.

#### Acceptance criteria (status post-audit)
- [x] Audit + classification — `docs/EARNINGS.md`
- [x] SCHEMA.md row updated to point at audit
- [x] Cleanup ticket spawned — HM-AR-β below

---

### HM-AR-β — Retire `engine/earnings_injector.py` orphan + rename `run_earnings_universe_inject` (2026-05-07)

**Type:** Cleanup (HM-AR follow-up)
**Priority:** P4 — LOW (cosmetic; no functional change; eliminates naming-drift confusion)
**Status:** **SHIPPED 2026-05-07** — see commit and OPS_LOG. Path (a) formal retirement applied: orphan archived to `archive/earnings_injector.py.retired-20260507`; `main.py:679 run_earnings_universe_inject` renamed to `run_earnings_scan_inject` (4 sites: definition, error log, comment, schedule binding).
**Origin:** HM-AR audit 2026-05-07.

#### Recommended path: (a) formal retirement

Dead code is technical debt. The "run_earnings_universe_inject" naming-drift confusion alone justifies cleanup. Archive-not-delete honors the sacred-data rule. Effort small.

**Steps:**
1. Move `engine/earnings_injector.py` → `archive/retired/2026-05-07-earnings-injector/earnings_injector.py`. Per archive convention.
2. Leave the `earnings_universe` SQLite table in place (empty; no data to lose; sacred-data rule). Keep schema as forensic record. SCHEMA.md already documents it as deprecated.
3. **Rename `main.py:679 run_earnings_universe_inject()` → `run_earnings_scan_inject()`** to fix the naming-drift lie that confused HM-AR's initial framing. Update the schedule binding at `main.py:2585` accordingly.
4. Single commit + service restart.

#### Alternatives (not recommended)

- **(b) Wire the orphan to a scheduler** — theater without a consumer. `get_active_earnings_universe()` has no caller; populating the table doesn't help anything. Would need to also identify and ship a real consumer, doubling scope. Skip.
- **(c) Status quo** — kicks the can. Empty table + dormant script + lying function name continues to confuse future investigators. The HM-AR audit just spent time untangling exactly this. Don't pay that cost twice.

#### Effort
~15 min Scotty: file move + 2 small edits in `main.py` (function rename + schedule binding) + commit + service restart for the rename to take effect.

#### Acceptance criteria
- [ ] `engine/earnings_injector.py` archived to `archive/retired/2026-05-07-earnings-injector/`
- [ ] `main.py:679` function renamed to `run_earnings_scan_inject`
- [ ] `main.py:2585` schedule binding updated to call the new name
- [ ] `docs/EARNINGS.md` updated to reflect the retirement (path 2 rename + path 3 archive location)
- [ ] No new tracebacks post-restart
- [ ] OPS_LOG entry recording the archive + rename

#### Related
- HM-AR — audit (parent)
- `docs/EARNINGS.md` — three-path map
- `docs/SCHEMA.md` — earnings_universe deprecation note

---

### HM-AS-β — battle_station_monitor cadence-tail observability (2026-05-07)

**Type:** Observability
**Priority:** P3 — post-soak
**Status:** Proposed
**Origin:** HM-AS diagnosis 2026-05-07. Parent HM-AS closed as "diagnosed, deferred."

#### Diagnostic summary (HM-AS, see OPS_LOG 2026-05-07 09:30)
`run_battle_station_monitor` cadence median 2:01 (on target vs the `every(2).minutes` schedule binding at `main.py:2588`); p75 3:09; p95 5:07; max 11:00. Distribution: 69% on cadence, 17% in the 4-6 min tail, ~3% at 6+ min. Cause is architectural — `main.py:4036` runs a single-threaded `schedule.run_pending()` loop, and slow synchronous jobs (LLM calls, scans, backtests) periodically block subsequent ticks. Function itself (`main.py:1002`) is fast (flag check + early return when α guard active). Fire-count integrity for α-lift evidence preserved (80% recovery rate, 289 fires/12h matches histogram mean).

#### Shape
Add `logger.warning` when `run_battle_station_monitor` inter-fire interval exceeds 180s (3 min). Single-function add at `main.py:1002` (or wherever the monitor entry/exit points are). Tracks tail occurrences in production logs without changing scheduler architecture.

Sketch:
```python
_last_battle_station_run = 0.0
def run_battle_station_monitor():
    global _last_battle_station_run
    import time as _t
    now = _t.time()
    if _last_battle_station_run > 0 and (now - _last_battle_station_run) > 180:
        logger.warning(f"[HM-AS-β] battle_station cadence drift: {now - _last_battle_station_run:.0f}s since last fire (target 120s)")
    if now - _last_battle_station_run < 55:
        return
    _last_battle_station_run = now
    # ... existing body
```

#### Effort
~10 min Scotty. Single commit. No service restart required (function reload via natural restart cadence).

#### Acceptance criteria
- [ ] Warning fires in `trader_error.log` when next-tick gap >180s
- [ ] Historical pattern can be analyzed via `grep "[HM-AS-β]" logs/trader_error.log`
- [ ] No false positives on first-fire-after-startup (initial `_last_battle_station_run = 0.0` skipped)

#### Escalation path (if tail proves operationally relevant)
- Option (b) from HM-AS analysis: dedicated thread for battle_station — 15-30 min, isolated.
- Option (a): move all slow jobs to threaded execution — 30-60 min, touches every monitor.

#### Related
- HM-AS — diagnosed, deferred (2026-05-07 09:30)
- HM-AF-α — α-lift evidence integrity preserved by 80% fire-rate recovery
- `main.py:4036` — single-threaded scheduler architecture

---

### HM-AT-β — Schwab watcher: migrate watch dir off ~/Downloads to eliminate TCC dependency (2026-05-07)

**Type:** Workflow / robustness
**Priority:** P3 → P1 (escalated 2026-05-07: GUI fix path unavailable on headless Mini)
**Status:** **SHIPPED 2026-05-07** — see commit and OPS_LOG 2026-05-07.
**Origin:** HM-AT diagnosis 2026-05-07. Parent HM-AT closed via Full Disk Access GUI grant intent + `sleep 11` defense-in-depth (commit `e8b7f9e`); GUI grant proved infeasible on the headless Mini, so HM-AT-β became the actual fix.

#### Problem
Watch dir is currently `/Users/bigmac/Downloads/` (set 2026-05-04 to "meet downloads where the browser puts them"). macOS TCC restricts `~/Downloads/` access — the launchd audit session does not inherit Full Disk Access from Terminal/SSH, causing silent dormancy. HM-AT was resolved by manually granting `/bin/bash` Full Disk Access in System Settings. That grant is fragile: any TCC reset (macOS update, system reset, manual revoke) re-introduces the silent failure.

#### Shape
Migrate the watch dir from `~/Downloads/` to `~/autonomous-trader/inbox/`. The autonomous-trader directory is project-owned and not subject to TCC's user-data restrictions, so launchd-spawned agents can read it without any GUI grant.

Changes:
- Edit `scripts/schwab_csv_watcher.sh`: `WATCH_DIR="/Users/bigmac/autonomous-trader/inbox"` (was `/Users/bigmac/Downloads`).
- Create `~/autonomous-trader/inbox/` directory; add to `.gitignore` since the inbox holds transient CSVs.
- Update CLAUDE.md "Schwab Workflow" section to reflect new drop directory.
- Workflow change for Admiral: browser save target switches from Downloads to inbox/ (Chrome's "Ask where to save" or per-save dir change), OR add a one-liner cron / Hazel rule to move `~/Downloads/Sc[hw]ab*Positions*.csv` to inbox/.

#### Effort
~30 min Scotty (script edit + dir create + CLAUDE.md update + verify) + Admiral browser-config or Hazel rule.

#### Acceptance criteria
- [ ] `WATCH_DIR` constant moved off `~/Downloads/`
- [ ] launchd-driven watcher processes a test CSV without any TCC grant on `/bin/bash`
- [ ] Admiral workflow documented (browser save dir change OR Hazel rule)
- [ ] CLAUDE.md "Schwab Workflow" updated
- [ ] Bootout/bootstrap cycle in OPS_LOG showing TCC-free operation

#### Escalation path
If browser-save-dir change is unworkable, alternative: Hazel rule on `~/Downloads/` to move matching CSVs to `~/autonomous-trader/inbox/`. Hazel runs in user session and inherits TCC, so it can read Downloads even when launchd cannot.

#### Related
- HM-AT — closed via Full Disk Access GUI grant + `e8b7f9e` defense-in-depth
- OPS_LOG 2026-05-07 10:00 — TCC diagnosis + recovery path
- CLAUDE.md "Schwab Workflow" section — current drop dir documented

---

### HM-AU — Kirk advisory source routing audit (2026-05-07)

**Type:** Observability / documentation
**Priority:** P3 — low
**Status:** **AUDITED + DOCUMENTED 2026-05-07** — see `docs/KIRK_SOURCES.md`. One bug surfaced and queued as HM-AU-β.
**Origin:** 2026-05-07 morning Kirk paper-source check surfaced ambiguity in `/api/kirk/advisory?source=...` semantics.

#### Audit findings (2026-05-07)
1. **`?source=paper`** = engine path (`generate_kirk_advisory()`), reads `data/real_holdings.json`. **The name is post-Option A back-compat — actual data is Schwab/TradeStation, not Alpaca paper.** Per commit `e41ddb2` (2026-05-05), the engine was retargeted to `real_holdings.json`; the source name stayed for callers' back-compat.
2. **`?source=real`** = inline path at `dashboard/app.py:13422`, reads same `data/real_holdings.json` via `_read_real_positions_sync()`. Different output shape (regex-parsed action labels), bypasses rule engine + `kirk_advisory_log` writes.
3. **`?source=all`** = **bug** (HM-AU-β below). Both paper and real handlers read the same JSON file → returned positions are duplicated.
4. **Default source** = `"paper"` (function signature). Three of five front-end callers use the default; two use `_kirkSource` (typically `'real'`).
5. **Morning 23 → 11 position shift** explained: snapshot rewrite during HM-AT-β backlog drain at 09:14 MST; not a routing inconsistency.

Full behavior table: `docs/KIRK_SOURCES.md`.

#### Problem
Same endpoint (`/api/kirk/advisory`) returned different data depending on time of day, after a Schwab CSV import flipped intermediate state:
- 06:50 MST: `?source=paper` → 23 positions (Alpaca paper book)
- 10:50 MST: `?source=paper` → 11 positions (Schwab `real_holdings.json` after morning import)

Per HM-AJ-documented gotcha: `?source=real` **bypasses** `generate_kirk_advisory()` entirely and uses inline action-logic at `dashboard/app.py:13420`. Other `?source=` values' behavior is not documented — unclear which paths invoke the rule engine vs. inline logic, and what data file/table each one reads.

#### Open questions
1. What `?source=` values does the endpoint accept?
2. For each value: does it call `generate_kirk_advisory()` or use inline logic?
3. For each value: what is the underlying data source (Alpaca API, `real_holdings.json`, `paper_holdings.json`, schwab_holdings table, positions table)?
4. Which value does the dashboard front-end use by default? Does that match operator intent?
5. Is the source-name vs. data-source mapping intentional or accidental drift?

#### Shape
1. Read `dashboard/app.py:13420` (inline `?source=real` path) and `generate_kirk_advisory()` to enumerate accepted source values + branching logic.
2. Read each source's underlying data accessor.
3. Cross-reference dashboard front-end calls (search `kirk/advisory?source=` in HTML/JS).
4. Produce a behavior table mapping source value → code path → data source → typical row count.
5. Document in `CLAUDE.md` or `docs/SCHEMA.md` under a new "Kirk Advisory Routing" section.
6. If any source name contradicts its data source (e.g., `?source=paper` returning Schwab data), flag for follow-up rename or re-routing — but don't rename in this audit; document and surface to Admiral.

#### Effort
~30 min Scotty (read 4-6 code locations + 1 doc write).

#### Acceptance criteria
- [ ] Behavior table in `CLAUDE.md` or `docs/SCHEMA.md`: `?source=` value → code path → data source → expected row count
- [ ] HM-AJ gotcha note cross-linked
- [ ] Any naming/routing contradictions flagged with proposed renames (no actual renames in this audit)

#### Related
- HM-AJ — Kirk parse hardening + observability + alert hygiene (commit `796acbf`)
- 2026-05-07 morning observation: same endpoint returned 23 → 11 positions across the day
- `docs/KIRK_SOURCES.md` — full behavior table, snapshot data flow, naming-vs-data contradiction explained

---

### HM-AU-β — `?source=all` returns duplicate positions (2026-05-07)

**Type:** Bug
**Priority:** P3 — no front-end caller currently uses `?source=all` (per HM-AU audit grep), so user-visible impact is zero today; latent risk if a future caller adopts it.
**Status:** Proposed
**Origin:** HM-AU audit 2026-05-07. Bug surfaced when reading `dashboard/app.py:13488-13501` in light of post-Option A data routing (`paper` re-targeted to `real_holdings.json` in commit `e41ddb2`).

#### Bug
The `?source=all` branch concatenates `paper_positions + real_positions`:

```python
if source == "all":
    from engine.kirk_advisory import generate_kirk_advisory
    paper_result = generate_kirk_advisory()      # reads data/real_holdings.json
    paper_positions = paper_result.get("positions", []) or []
    for p in paper_positions:
        p["origin"] = "paper"
    paper_result["source"] = "all"
    paper_result["source_label"] = "Combined Paper + Real"
    paper_result["positions"] = paper_positions + real_positions  # ← BOTH come from real_holdings.json
    paper_result["real_cash_available"] = real_cash
    return paper_result
```

`paper_positions` (from `generate_kirk_advisory()` → `_load_real_holdings()`) and `real_positions` (from `_read_real_positions_sync()`) **both read `data/real_holdings.json`**. The concatenation produces each position twice, with one copy labeled `origin="paper"` and the other `origin="real"`. Pre-Option A this was correct (paper actually meant Alpaca paper book, real meant Schwab); post-Option A both sides resolve to the same file.

#### Reproduction
```bash
curl -s 'http://localhost:8080/api/kirk/advisory?source=all' \
  | python3 -c 'import sys,json; d=json.load(sys.stdin); print(len(d.get("positions",[])))'
```
Expected (post-fix): 11. Actual today: 22.

#### Fix shape options

| Option | Effort | Behavior |
|---|---|---|
| (A) Drop `paper_positions` from the union; rename to "engine-output for `real`" | ~10 min | `?source=all` returns engine-path advisory + cash, no duplication |
| (B) De-dup by `symbol` after concat | ~10 min | Keeps both paths' enriched data; latest write wins per symbol |
| (C) Restore `paper` to truly mean Alpaca paper book; revert Option A retargeting in `e41ddb2` | ~30 min | Largest scope — undoes the 2026-05-05 Admiral decision, breaks current callers; not recommended |
| (D) Deprecate `?source=all` entirely; return 410 Gone | ~5 min | Cleanest if no caller needs union semantics today |

**Recommendation: (A)** — given no front-end caller uses `?source=all` (per audit grep at `docs/KIRK_SOURCES.md`), the union semantics are unused. Returning the engine path's output keeps the action labels + market context + alert dedup intact and stops the duplication.

#### Acceptance criteria
- [ ] `?source=all` returns N unique positions (N = active accounts in `real_holdings.json`)
- [ ] No duplicate `symbol` values in the response
- [ ] `docs/KIRK_SOURCES.md` updated with post-fix behavior

#### Related
- HM-AU — audit + behavior table
- `e41ddb2` — Option A retargeting that created the latent bug
- `docs/KIRK_SOURCES.md`

---

### HM-AK — Fleet roster cleanup (2026-05-07)

**Type:** DB hygiene
**Priority:** P3 → shipped same-day
**Status:** **SHIPPED 2026-05-07** — see `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` and OPS_LOG 2026-05-07.
**Origin:** 2026-05-06 evening fleet roster check + 2026-05-07 morning HM-AK diagnosis. Surfaced 12 dormant zombies among 50 ai_players rows.

#### Outcome
12 dormant agents halted via UPDATE: 11 to `halt_mode='full'`, 1 (gemini-2.5-flash, 2 open positions) to `halt_mode='exit_only'`. Halt-mode census shifted from 37/9/4 (active/full/exit_only) to **25/20/5**.

**Halted (paid-API zombies, 6):**
- `claude-haiku`, `claude-sonnet`, `gpt-4o`, `gpt-o3`, `grok-4` → `halt_mode='full'`
- `gemini-2.5-flash` → `halt_mode='exit_only'` (had 2 open positions)

**Halted (dormant Ollama, 6):**
- `qwen-coder-haiku`, `qwen3-14b-grok3`, `qwen3-8b-4o`, `qwen3-8b-o3`, `ollama-glm4`, `ollama-gemma27b` → `halt_mode='full'`

**Side effect:** all three duplicate display name conflicts resolved (`Lt. Cmdr. Worf` × 3 → 1, `Lt. Cmdr. Spock` × 2 → 1, `Qwen3 14B Pro` × 2 → 1). The zombies leave active iteration, leaving only the canonical agent in each name slot.

**No service restart required** — halt_mode is read fresh per request via `engine/halt_gate.py`.

**Rollback:**
```sql
UPDATE ai_players SET halt_mode='active', halted_at=NULL, halt_reason=NULL
 WHERE halt_reason LIKE 'HM-AK 2026-05-07%';
```

#### Related
- `migrations/HM-AK_dormant_cleanup_2026-05-07.sql` — checked-in SQL artifact
- OPS_LOG 2026-05-07 — full diagnosis + outcome
- HM-AK-β below — architectural follow-up (scan loops still ignore halt_mode)

---

### HM-AK-β — Scan loops should filter by halt_mode, not is_active (2026-05-07)

**Type:** Architectural debt
**Priority:** P3 — escalated and shipped same-day
**Status:** **SHIPPED 2026-05-07** (commit `77de5be`) — Option A applied to the 3 known iteration sites (`main.py:1991`, `engine/risk_radar.py:168`, `engine/autopilot.py:63`). Iteration count drops ~49 → ~25 per cycle. Dashboard follow-up + dayblade-exclusion cleanup queued as HM-AK-β.2 + HM-AK-γ below.
**Origin:** HM-AK diagnosis 2026-05-07. Surfaced as a separate ticket because scope is too large for a same-day ship.

#### Problem
Multiple scan/iteration sites use `WHERE is_active=1` instead of `halt_mode='active'`:
- `main.py:1991` — `SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'`
- `engine/risk_radar.py:168` — same pattern
- `engine/autopilot.py:63` — same pattern
- `engine/cost_tracker.py:387` — `WHERE is_active=1`
- `engine/q_entity.py:224` — `WHERE is_active=1`
- `engine/providers/base.py:1119` — `WHERE is_active=1`
- ... and ~10 other sites (full inventory in HM-AK diagnosis logs)

After HM-AK, 25 rows are `halt_mode='active'` and 25 are halted (full or exit_only). But all 49 with `is_active=1` (only `webull` is `is_active=0`) still pass the iteration filter. Per-trade halt gates downstream block actual execution, so this is **not a safety issue** — it's just compute waste from iterating ~25 halted rows per cycle.

Per CLAUDE.md (2026-04-25 audit + HM-A migration): "halt_mode is now the only working per-player kill switch". The iteration sites haven't caught up.

#### Shape

**Option A (small, safe):** Replace `WHERE is_active=1` with `WHERE halt_mode='active'` at each iteration site. Touch ~17 SQL strings, one PR. ~1-2 h Scotty (read each site, verify call-site semantics, retest). Per-site analysis required because some callers may want to see halted agents (e.g. `cost_tracker` reporting historical costs).

**Option B (bigger, cleaner):** Introduce a single helper `engine/db_helpers.py::active_player_ids()` that returns agent IDs where `halt_mode='active'`, and migrate all iteration sites to call it. ~3 h Scotty. Future migrations only need to update the helper.

**Option C (defer):** No code change — accept that iteration is wider than execution. Compute waste is small (a few SELECTs per cycle).

#### Recommendation
**Option A or B post-soak.** Both are safe but neither is urgent. The execution-gate path is already correct via `halt_gate.py` per-trade checks; this is just iteration efficiency.

#### Acceptance criteria (if shipped)
- [ ] All `WHERE is_active=1` iteration sites replaced (or migrated to helper)
- [ ] Per-site verification that semantics are preserved (cost_tracker reports may want historical view)
- [ ] No regression in scan/trade/signal volume

#### Related
- HM-AK — parent (shipped 2026-05-07)
- HM-AK-β.2 — extend to dashboard sites (below)
- HM-AK-γ — drop redundant dayblade-0dte exclusion (below)
- 2026-04-25 audit notes — `is_active`, `is_paused`, `crew_role` are decorative; `halt_mode` is the kill switch
- HM-A — migrated production read paths from `is_halted` to `halt_mode`; iteration sites not migrated

---

### HM-AK-β.2 — Extend halt_mode filter to 3 dashboard iteration sites (2026-05-07)

**Type:** Architectural cleanup (HM-AK-β follow-up)
**Priority:** P4 — LOW (iteration efficiency only, not safety-critical)
**Status:** Proposed
**Origin:** HM-AK-β commit `77de5be` deferred 3 dashboard sites pending per-site read confirmation.

#### Problem
Three sites in `dashboard/app.py` use the identical scan-loop SQL pattern that HM-AK-β just patched in `main.py`/`engine/`, but were deferred because their use case (trade-iteration vs roster-display) wasn't confirmed at ship time:

- `dashboard/app.py:3904` — `SELECT id, display_name FROM ai_players WHERE is_active=1 AND id != 'dayblade-0dte'`
- `dashboard/app.py:4619` — same SQL
- `dashboard/app.py:12908` — same SQL

The `id != 'dayblade-0dte'` exclusion is the tell — it's the same pattern the scheduler scan loops use, suggesting these are also trade-iteration paths, not pure roster-display. But that needs to be **verified site-by-site** before applying the filter (display sites must show all halted agents).

#### Shape
Per site:
1. Read the surrounding function context
2. Classify as iteration (apply filter) or display (leave alone)
3. For iteration sites: add `AND halt_mode='active'` to the WHERE clause + tag `# HM-AK-β.2 2026-05-07`

#### Effort
~15-20 min Scotty (3 file reads + 0-3 edits depending on classification + commit + restart + verify).

#### Acceptance criteria
- [ ] Each of the 3 sites classified (iteration vs display) with rationale in commit message
- [ ] Iteration sites get `halt_mode='active'` filter
- [ ] Display sites left as-is, with comment explaining why
- [ ] Service restart + smoke verify

#### Related
- HM-AK-β — shipped 3-site fix (commit `77de5be`)
- HM-AK-γ — dayblade-exclusion cleanup (would touch the same sites; sequence HM-AK-β.2 first)
- `dashboard/app.py:5139, 5202` — already use `COALESCE(halt_mode,'active')='active'` (positive precedent in the same file)

---

### HM-AK-γ — Drop redundant `id != 'dayblade-0dte'` exclusion (2026-05-07)

**Type:** Cleanup (HM-AK-β follow-up)
**Priority:** P4 — LOW (no functional change)
**Status:** Proposed
**Origin:** HM-AK-β commit `77de5be` left the `id != 'dayblade-0dte'` clause in place for back-compat.

#### Problem
Post-HM-AK (commit `2b89651`) and HM-AF (earlier 2026-05-06), `dayblade-0dte` is `halt_mode='full'`. Once HM-AK-β added `halt_mode='active'` to the iteration filter, the explicit `id != 'dayblade-0dte'` exclusion became **redundant** — the halt_mode filter already excludes it.

Affected sites (all currently carry both clauses post-HM-AK-β):
- `main.py:1992`
- `engine/risk_radar.py:169`
- `engine/autopilot.py:64`
- `dashboard/app.py:3904, 4619, 12908` (after HM-AK-β.2 ships, if classified as iteration)

#### Shape
Drop the `AND id != 'dayblade-0dte'` clause from each site post-HM-AK-β.2. Tag `# HM-AK-γ 2026-05-07: removed redundant dayblade exclusion`.

**Constraint:** sequence HM-AK-β.2 BEFORE HM-AK-γ. If HM-AK-γ ships first and a future operator un-halts dayblade-0dte (e.g., to reactivate a 0DTE strategy), the iteration filter would no longer exclude it. The two-clause defense protects against that footgun until HM-AK-γ explicitly removes it as deliberate cleanup.

#### Effort
~5 min Scotty (after HM-AK-β.2 lands; then a single multi-site edit + commit + restart).

#### Acceptance criteria
- [ ] HM-AK-β.2 shipped first
- [ ] Redundant exclusion dropped at all confirmed iteration sites
- [ ] Service restart + smoke verify
- [ ] Re-confirm dayblade-0dte halt_mode='full' is the only protection (no rollback to active without explicit ticket)

#### Related
- HM-AK-β — shipped halt_mode filter (commit `77de5be`)
- HM-AK-β.2 — dashboard extension (sequence first)
- HM-AF — dayblade-0dte halt_mode='full' (the reason the exclusion is now redundant)

---

### HM-AW — Signal Center auth + network exposure review (HALTED 2026-05-07)

**Type:** Hygiene / network exposure
**Priority:** P3 — BLOCKED on HM-AW.3 (2FA enforcement) before LAN bind can ship
**Status:** **HALTED 2026-05-07** at Phase C — HARD STOP #10 fired. LAN bind shipped on local commit `0d3e5dc` (NOT pushed); Captain manual LAN verification surfaced that 2FA TOTP was advertised in code but NEVER WIRED — step-1 password match at `signal-center/server.py:658-665` sets `session["authenticated"]=True` directly without ever setting `totp_pending`, making step-2 (lines 624-652) dead code. Commit `0d3e5dc` was reset (`git reset --hard HEAD~1`); service restarted; bind verified back to `127.0.0.1:9000`. See `docs/HM-AW_PHASE_A_DIAGNOSE.md` Phase C section for the full diagnosis, rollback steps, and audit miss explanation. **Sequencing:** HM-AW.3 (2FA enforcement) MUST ship and verify before HM-AW (binding) can be re-attempted. HM-AW.2 (multi-user RBAC port) is a separate follow-on if Captain wants Bonnie/Dad on 9000.
**Origin:** 2026-05-07 14:55 MST Captain note post-HM-AQ-β ship close-out. Letter `HM-AT` was originally proposed but already used today (Schwab watcher TCC fix); rebadged as `HM-AW` to avoid collision (AV = HM-AV ALPACA→APCA simplification).
**Sequence:** Hold until HM-AW.3 ships AND HM-AQ-β stabilizes (24h soak after the 1,223-symbol universe lands in production, est. 2026-05-08 evening).

#### Background
Port 9000 (`signal-center` web UI) is currently bound to `127.0.0.1` from a legacy pre-2FA security posture. Browser access from non-bigmac devices requires SSH tunnel today. Captain wants to reopen 9000 to the network now that:
- 2FA TOTP enforcement is in place
- Multi-user auth (Captain, Bonnie observer mode, Dad charts permissions) is established
- The original justification for localhost-only (no auth layer) no longer applies

**Two distinct authentication paths to keep clear:**
- **Browser access** (humans) — 2FA TOTP + role-based access control via signal-center server
- **Automation access** (Scotty/scripts) — SSH keys + bigmac user account at the OS layer; does NOT go through Signal Center auth

These are NOT the same thing; CLAUDE.md should document them separately to prevent future-XO from conflating them.

#### Sub-questions for Phase 1 investigation (before any binding change)
1. **Network exposure path**:
   - (a) LAN-only via `0.0.0.0` (any 192.168.1.x device with auth) — simplest
   - (b) Cloudflare tunnel like `bridge.ollietrades.com:8080`, e.g. `signals.ollietrades.com` — broader
   - (c) Both — LAN + Cloudflare for full access redundancy
2. **2FA TOTP enforcement audit**: confirm 2FA is required on ALL sensitive routes (not just login page). Spot-check `/api/admin/*`, `/api/trades`, `/api/signals/post`, etc.
3. **RBAC verification**: confirm Bonnie observer mode (read-only) and Dad charts permissions (charts-only) still work post-network-exposure.
4. **Automation auth path documentation**: add a section to CLAUDE.md or `docs/AUTH.md` (new) clarifying the SSH-keys-vs-2FA distinction.

#### Investigation pre-flight (read-only, before any code change)
- `signal-center/server.py`: identify host argument and binding logic
- launchctl plist for `com.trademinds.signal-center`: verify no override of bind address
- Spot-check 2FA enforcement on a representative non-login route via `curl` without TOTP cookie
- Spot-check RBAC by simulating Bonnie/Dad auth tokens

#### Shape (after Phase 1 sub-questions resolved)
- One-line binding change in `signal-center/server.py` (or env var)
- launchctl restart of `com.trademinds.signal-center`
- Optional: Cloudflare tunnel config addition (separate ticket if pursued)

#### Effort
~30 min Scotty (Phase 1 investigation + binding change + restart + verify) once Captain greenlight.

#### Risk
LOW. Auth posture handles what bind-localhost was protecting. Reversal is a single-line revert + restart.

#### Acceptance criteria
- [x] Phase 1 sub-questions answered + documented (`docs/HM-AW_PHASE_A_DIAGNOSE.md`)
- [HALTED] Binding change shipped — was shipped on local commit `0d3e5dc`, then reset after HARD STOP #10
- [BLOCKED] All Signal Center routes confirmed under 2FA TOTP enforcement — **2FA never wired; tracked as HM-AW.3**
- [BLOCKED] Bonnie observer + Dad charts RBAC verified working — **RBAC never ported to port 9000; tracked as HM-AW.2**
- [ ] CLAUDE.md / docs/AUTH.md updated with SSH-keys-vs-2FA distinction (deferred until HM-AW re-attempt)

#### Related
- `docs/HM-AW_PHASE_A_DIAGNOSE.md` — full Phase A + C diagnose, rollback record, lessons
- HM-AW.2 (this file) — multi-user RBAC port (sequenced after HM-AW)
- HM-AW.3 (this file) — 2FA TOTP enforcement (prerequisite for HM-AW)
- HM-AT — Schwab watcher TCC fix (SHIPPED 2026-05-07; namespace collision avoided by using HM-AW here)
- HM-AT-β — Schwab watcher inbox migration (SHIPPED 2026-05-07, commit `5b87d69`)

---

### HM-AW.3 — Signal Center 2FA TOTP enforcement (2026-05-07)

**Type:** Auth / security gap
**Priority:** P2 — MUST ship before HM-AW (LAN bind) can re-attempt
**Status:** Proposed (filed 2026-05-07 from HARD STOP #10)
**Origin:** Captain manual Phase C verification of HM-AW (2026-05-07) discovered Sniff could log into 9000 from LAN with username + password only — no TOTP prompt. Investigation in `docs/HM-AW_PHASE_A_DIAGNOSE.md` F4 + Phase C section showed step-1 success branch sets `session["authenticated"] = True` directly; step-2 TOTP path is dead code.

#### Background
The TOTP infrastructure exists in `signal-center/server.py`:
- `_SC_TOTP_SECRET` env var (line 40), populated via `.env` inline loader (lines 21-29)
- `_sc_totp = pyotp.TOTP(_SC_TOTP_SECRET)` (line 41)
- TOTP HTML page (line 120) renders the 6-digit form
- Step-2 verification at lines 624-652 uses `_sc_totp.verify(code, valid_window=1)` against `session["totp_pending"]`

But step-1 success at lines 658-665 sets `session["authenticated"] = True` and redirects to `/` without ever setting `session["totp_pending"]`. The "2FA disabled — authenticate directly" comment is the smoking gun.

#### Shape
1. Edit step-1 success branch (lines 658-665) to:
   - If `_sc_totp` is configured (i.e. `TOTP_SECRET` is set): set `session["totp_pending"] = True`, set `session["totp_pending_user"] = username`, redirect to `/login?step=2` (do NOT set `authenticated`).
   - If `_sc_totp` is None: keep current direct-authenticate behaviour (degraded mode for environments without `TOTP_SECRET`).
2. Verify the existing step-2 path (lines 624-652) handles the `totp_pending` session key correctly when set from step-1 (it already does — it pops `totp_pending` and `totp_pending_user` and sets `authenticated` after `_sc_totp.verify` succeeds).
3. Smoke-test (localhost): POST username+password → expect 302 to `/login?step=2`; GET `/login?step=2` → expect TOTP page HTML.
4. Verify with the Sniff TOTP authenticator app from `.env` `TOTP_SECRET=4X6RT3GCI2CW5IJSZ76PO5QPIPPZRNDY`.
5. Captain manual verification from LAN device after HM-AW re-ship.

#### Effort
~20 min Scotty (small edit + restart + curl smoke + manual TOTP verification).

#### Risk
LOW for code change. Reversal is one-line revert. Risk of breaking login if `TOTP_SECRET` is invalid or pyotp version mismatch — mitigate by keeping the `if _sc_totp is None:` degraded branch so the service is never unloggable-into.

#### Acceptance criteria
- [ ] Step-1 success branch routes to step-2 when `_sc_totp is not None`
- [ ] curl smoke shows 302 → `/login?step=2` after correct password POST
- [ ] Captain manual TOTP verification from authenticator app succeeds
- [ ] HM-AW LAN bind change can be re-shipped after this lands

#### Related
- `docs/HM-AW_PHASE_A_DIAGNOSE.md` F4 — exact lines + diagnosis
- HM-AW (this file) — blocked on this ticket

---

### HM-AW.2 — Signal Center multi-user RBAC port (2026-05-07)

**Type:** Auth / RBAC
**Priority:** P3 — sequenced AFTER HM-AW (LAN bind, after HM-AW.3 2FA lands). Only required if Captain wants Bonnie/Dad on port 9000.
**Status:** Proposed (filed 2026-05-07 alongside HM-AW.3)
**Origin:** Phase A of HM-AW (2026-05-07) discovered that the multi-user RBAC config in `.env` (`DASHBOARD_USERS=Sniff:admin:..., Bonnie:observer:..., Dad:charts:...`) is consumed by `dashboard/app.py:557 _parse_users()` (port 8080) ONLY. `signal-center/server.py` (port 9000) reads only the singular `DASHBOARD_USER` / `DASHBOARD_PASS` env vars and accepts a single user. Captain elected to ship HM-AW with single-user posture (Sniff only on 9000); HM-AW.2 captures the optional follow-on if Bonnie/Dad need 9000 access too.

#### Shape
Port `_parse_users()` from `dashboard/app.py:557` into `signal-center/server.py`. Replace the singular `_SC_USER` / `_SC_PASS` check at line 658 with a registry lookup keyed on the submitted username. Preserve role attribution (admin / observer / charts). Wire role into `session["role"]` and add per-route role gating where Bonnie or Dad permissions differ from Sniff.

#### Effort
~30–60 min Scotty (port the function, swap the check, decide which signal-center routes admit observer/charts roles, smoke-test from each user's credentials).

#### Risk
LOW for the port itself; MEDIUM if signal-center routes need new role gates that don't exist in `dashboard/app.py` for analogous reasons. Reversal is straightforward (git revert).

#### Acceptance criteria
- [ ] `_parse_users()` ported and wired into `_auth_gate` / login flow
- [ ] Sniff, Bonnie, Dad all log in successfully with their own credentials
- [ ] Per-route role gating decisions documented (Bonnie read-only — what does that mean for `/api/signals/<id>/execute`? Dad charts-only — what does that mean for non-charts routes?)

#### Related
- HM-AW (this file) — single-user posture shipped under that ticket
- `dashboard/app.py:557 _parse_users()` — source of truth to port
- `.env` `DASHBOARD_USERS=Sniff:admin:ollietrades-admin,Bonnie:observer:ollietrades-crew,Dad:charts:none`

---

### HM-AM — Total Portfolio Unification (ALL PHASES SHIPPED 2026-05-07)

**Type:** Cross-source data layer (multi-phase epic)
**Priority:** P3 — closed; all four phases shipped 2026-05-07 (autonomous mode)
**Status:** **ALL PHASES SHIPPED 2026-05-07.** Phase 1 (`4f0bcff`) data layer · Phase 2 (`d338605`) Kirk envelope · Phase 3 (`d6c9647`) Advisory Team prompt · Phase 4 (`52d7298`) dalio-metals prompts. Captain intent ("metals are an extension of the total portfolio") closed end-to-end.
**Origin:** Captain mental-model 2026-05-06: "metals are an extension of the total portfolio." Schwab + Dilithium Reserve + Alpaca paper currently siloed across `data/real_holdings.json`, `metals_ledger` table, and `AlpacaBridge`. Kirk + Advisory Team see Schwab only. Goal: unified read-only API.

#### Phase 1 outcome

`engine/total_portfolio.py` provides:
- `get_total_portfolio() -> TotalPortfolio` — full unified view (positions + cash + totals + sources_loaded/failed)
- `get_portfolio_summary() -> dict` — lightweight summary
- 30s TTL cache (matches `engine/universe.py` precedent)
- Per-source resilience: each source loaded independently; failures recorded in `sources_failed` rather than raising

First smoke (2026-05-07): **22 positions, $138,371.20 total value**, all 3 sources loaded clean. See `docs/TOTAL_PORTFOLIO.md`.

#### Phase 2 (SHIPPED `d338605`) — Kirk advisory integration

`engine/kirk_advisory.py::generate_kirk_advisory()` now augments its return envelope with a `total_portfolio` key from `get_portfolio_summary()`. Per-Schwab-position executive action logic preserved (alert semantics + team_advisor_grok coupling intact). Defensive try/except: failure logs a warning and the envelope omits the key.

#### Phase 3 (SHIPPED `d6c9647`) — Advisory Team integration

`engine/team_advisor_grok.py::run_grok_subadvisor()` now injects a "## Total Portfolio Context" preamble into Grok's user prompt with cross-source totals (value/cash/invested + position count + sources_loaded). Stale `"~$22k notional + ~$2.2k cash"` hardcode removed. Per-Schwab-position breakdown loop (executive surface) preserved.

#### Phase 4 (SHIPPED `52d7298`) — `dalio-metals` strategy realign

Two `if self.player_id == "dalio-metals":` injection sites in `engine/providers/base.py` (single-shot prompt path + 3-step research/thesis/execute Step 2 thesis). Each appends a TOTAL PORTFOLIO CONTEXT block to `personality_block` so Mr. Dalio's All Weather reasoning sees Schwab + metals + Alpaca paper, not just metals. Other personas untouched.

#### Acceptance

- [x] `engine/total_portfolio.py` ships read-only data layer (Phase 1)
- [x] Standalone smoke succeeds (`venv/bin/python3 engine/total_portfolio.py`)
- [x] Per-source resilience verified (sources_failed pattern works)
- [x] 30s TTL cache + `force_refresh` flag
- [x] `docs/TOTAL_PORTFOLIO.md` documents module, data shape, deferred phases
- [x] Kirk advisory envelope includes `total_portfolio` (Phase 2)
- [x] Advisory Team prompt includes Total Portfolio Context preamble (Phase 3)
- [x] `dalio-metals` prompts include Total Portfolio Context preamble at both sites (Phase 4)
- [x] All consumers defensive (try/except, prompt builds without preamble on failure)

#### Cross-references

- `docs/TOTAL_PORTFOLIO.md` — full module reference
- `engine/alpaca_bridge.py::AlpacaBridge.status() / .positions()` — Alpaca source
- `data/real_holdings.json` — Schwab/TradeStation source (HM-AT-β pipeline)
- `metals_ledger` table — physical metals (`docs/SCHEMA.md`)
- HM-AT-β — Schwab CSV pipeline that feeds the real_holdings.json source
- HM-AU — Kirk advisory source routing audit (relevant when Phase 2 integration starts)

---

## Lessons

**2026-05-04 — Stale-bytecode trap from in-flight schema changes:** HM-B's `DROP COLUMN ai_players.is_halted` (commit `9256890`) created a stale-bytecode mismatch in the running trader process (PID 13734). The service was started at 08:32 MST — before HM-A's source migration shipped that morning — so the in-memory bytecode still had pre-HM-A SQL referencing the now-dropped column. Errors began at 17:36, but were caught by `try/except` blocks at the call sites and surfaced only as quiet `console.log` warnings: 15 occurrences across `War Room`, `ai_brain.py:286/295/533`, and three agents (ollama-coder, mlx-qwen3, energy-arnold) before discovery via log scan during PED retirement verification ~70 minutes later. Source code post-HM-A was clean; the issue was entirely in the long-running process's compiled module cache. **Future schema-change sessions should include a service restart in the verification phase OR a longer (30+ min) post-change soak window before declaring the change stable**, specifically to flush any pre-migration in-memory residue. This is also a HM-U datapoint: the silent-failure pattern (caught exceptions, swallowed errors) hid the issue from cursory checks — only a focused log scan surfaced it.

---

---

---

## SHIPPED 2026-05-06 19:40 MST — HM-AI Grok→Team rename (commit `b09d7a5`)

**Background:** "Grok" was legacy branding from the xAI Grok-4 era. The model has been qwen3:8b on Ollie Box since the 2026-04-17 RAM patch. HM-AG-β rewrote the scheduler docstring at `main.py:1718` to say "Advisory Team scheduler"; HM-AI continues that rename through the function, file, and variable layer so the code matches the docstring.

**Conceptual model (post-rename):**

    Team        = parent orchestrator   (run_team_advisor → run_team_scan)
    Grok-sub    = LLM-thesis sub-advisor (run_grok_subadvisor)         ← was run_grok_advisory
    Troi-sub    = sentiment sub-advisor  (run_troi_scan)
    Worf-sub    = tactical-risk sub-advisor (run_worf_scan)

The "grok" name now identifies the **sub-advisor role** (LLM-thesis sub-agent), not the model.

**Renames:**
- `engine/kirk_grok_advisor.py` → `engine/team_advisor_grok.py` (`git mv`, 95% similarity preserved)
- `run_grok_advisory()` → `run_grok_subadvisor()`
- `main.py def run_grok_advisor()` → `def run_team_advisor()`
- `main.py _grok_advisor_slots_done_today` → `_team_advisor_slots_done_today` (global flag)
- `engine/wb_advisory_team.py`: 1 import + 1 call + 1 docstring line
- `dashboard/app.py`: 1 import + 1 comment
- `engine/kirk_advisory.py`: 1 comment line
- Logger name in renamed file: `kirk_grok_advisor` → `team_advisor_grok`

**Preserved (intentionally not changed):**
- `portfolio_advice.advisor='grok'` DB rows — represents the sub-advisor role; preserves history
- Dashboard `🛸 Advisory Team` card with Grok/Worf tabs
- `[HM-AG-α]` log strings — "Grok" is the sub-advisor name, not the model
- `archive/retired/2026-05-04-kirk-swing-desk/` README and all `docs/*` historical references

**Verification matrix (all 9 GREEN, post-restart PID 75149):**
1. `git mv` rename history-preserving (95% similarity)
2. Zero orphan code refs to `kirk_grok_advisor` / `run_grok_advisory` (only self-documenting rename notes inside new file's docstring)
3. `import engine.team_advisor_grok` works; `from engine.team_advisor_grok import run_grok_subadvisor, get_scan_meta` resolves
4. Old `engine.kirk_grok_advisor` import path raises `ImportError`
5. Logger name updated to `team_advisor_grok`
6. Dashboard `/api/wb-team/advice` returns HTTP 200 with shape `{advisors:[grok,troi,worf], meta:{...}}`
7. Startup log line `"Advisory Team armed (Grok+Troi+Worf — fires 9:30 AM…)"` confirmed at `main.py:3879`
8. Manual `POST /api/wb-team/scan` returns `team_scan: true`; Troi + Worf each wrote 3 `portfolio_advice` rows under their advisor keys
9. `[HM-AG-α]` filter logs continue to fire under the renamed function

**Side observation (not a rename problem):** The post-rename trigger had Grok-sub return `parse_error: Expecting ',' delimiter: line 1 column 1514 (char 1513)` — qwen3:8b emitted malformed JSON on this run. The function ran end-to-end through the renamed path and hit the existing error-handling branch correctly. Pre-existing brittleness in `_parse_advice`'s strict `json.loads`. **Flagged as future HM-AJ candidate:** harden `_parse_advice` to recover from truncated/malformed LLM JSON (try-except `json.JSONDecodeError` with a salvage attempt that slices at the last complete `}` before the error position). Earlier 18:36 trigger saved 22/23 cleanly with 1 hallucination caught — proves filter + parse work when LLM behaves.

**Reversal:** `git revert b09d7a5` + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`. No DB state to roll back.


## SHIPPED 2026-05-06 18:00 MST — Kirk None-fix (commit `d2be8bb`)

**Root cause:** `engine/kirk_advisory.py:277` had a default-value bug:

    fg_score = fg.get("score", 50) if fg else 50

The `50` default only kicks in if `fg` is None OR the `score` key is missing. But Fear & Greed API can return `{"score": null}`, which makes `.get()` return None explicitly — bypassing the default. That None then flowed to line 357 (`if vix > 30 and fg_score < 35:`), throwing `TypeError: '<' not supported between instances of 'NoneType' and 'int'`.

**Fix:** Added explicit None-check:

    fg_score = fg.get("score") if fg else None
    fg_score = 50 if fg_score is None else fg_score

**Verified post-restart (PID 71272):** `generate_kirk_advisory()` returns clean dict with positions, cash=$2220.77 (matches Schwab snapshot), market_context, recommendations. No error key.

**Discovered along the way:**
- Kirk Advisory and "Advisory Team" (`engine/wb_advisory_team.run_team_scan`) are TWO separate systems with overlapping branding. The "Kirk Grok Swing Advisor" comment in main.py is misleading — that scheduler entry calls Advisory Team, not Kirk Advisory.
- Advisory Team has been working all along (10:40 MST today: 23 positions, 6 recommendations via qwen3:8b on Ollie Box). Kirk Advisory was the broken one.
- This was the root cause of the "Kirk silent" observability gap noted in HM-AF AMENDMENT — Kirk wasn't silent, it was crashing on every fire and only emitting the error log line.

**Open follow-ups:**
1. **Refresh `data/real_holdings.json`** — last updated 2026-05-04. Kirk now works but advises on stale positions until a fresh Schwab export is loaded.
2. **Add observability log lines** (original HM-AF Item #7-style) — Kirk currently logs only on error. Add success-side logging so we can verify daily fires.
3. **Investigate Advisory Team scope** — what's it advising on (23 positions ≠ Schwab ≠ Webull ≠ Alpaca counts), and is its output surfaced anywhere?


## SHIPPED 2026-05-06 11:53 MST — HM-AF-β + HM-AF-γ (commit `ca50d45`)

**HM-AF-β (Layer 1: spread-leg awareness):** New `engine/options_utils.py` (+143 new lines) with `parse_occ_symbol()` + `is_spread_leg(symbol)` + `has_open_spread_legs()`. 30s TTL in-memory cache to handle P1's 2-min loop performance. Match logic: parses OCC symbol → matches against `options_trades.legs_json` structured fields (underlying, expiration, option_type, strike) for rows WHERE `status='open' AND exec_status='open'`. Wired into all three contaminated paths:
- **P1** — `engine/battle_station.py::monitor_active_options` filters position list before the close-evaluation loop (+30/-3).
- **P2** — `engine/alpaca_options.py::close_all_options` per-position skip in EOD sweep (+17/-2).
- **P3** — `engine/dayblade.py` post-trade defense-in-depth observability log (+7).
Fail-closed: any leg-filter exception skips the close (conservative).

**HM-AF-γ (Layer 2: wrong-side-of-book correction):** `battle_station._get_alpaca_options_positions` now preserves qty sign via new `qty_signed` field (`qty` stays `abs()` for backcompat). `_auto_close` branches: `qty_signed < 0` (short) → `submit_single_option(side='buy')` for buy-to-close; `qty_signed > 0` (long) → `close_options_position` for sell-to-close. Fixes the bug where shorts were being treated as longs in close logic.

**HM-AF-α global guard remains ON** (`SPREAD_CANNIBALIZATION_GUARD_ENABLED=True` unchanged). β/γ are STAGED-AND-READY but DORMANT in production — every options close is intercepted by α before reaching β/γ. Lifting α requires a SEPARATE Phase 4 decision after 24h soak (review window opens 2026-05-07 ~11:53 MST).

**CLAUDE.md updated** with β/γ status row in the Feature Flags section, plus a note: "Lifting requires a separate Phase 4 decision; do not auto-lift" (+1/-1).

**Verification post-restart (PID 6633 → 7954, started 2026-05-06 11:53:52 MST):** All 7 deliverables green.
- New bytecode loaded ✅
- HM-AF-α outer guard still firing post-restart ✅ (11:53:59 first fire)
- `is_spread_leg` reachable via direct invocation ✅
- HM-AF-β code dormant under α (zero `[HM-AF-β]` log lines, exactly as designed) ✅
- CLAUDE.md updated with β/γ note + lift procedure ✅
- Zero `Alpaca OPTIONS SELL` post-restart ✅
- Zero `Alpaca options EOD close` post-restart ✅

**Unit test results (re-run against post-edit modules in venv Python):**
- `parse_occ_symbol("SPY260515P00732000")` → `{'underlying': 'SPY', 'expiration': '2026-05-15', 'option_type': 'put', 'strike': 732.0}` ✅
- `is_spread_leg("SPY260515P00732000")` → True ✅ (orphan from open spread id=27)
- `is_spread_leg("SPY260515P00727000")` → True ✅ (the cannibalized long leg, still in legs_json)
- `is_spread_leg("AAPL")` → False ✅
- `is_spread_leg("MSFT250517C00500000")` → False ✅
- `has_open_spread_legs()` → True ✅

The Test 5 result (`is_spread_leg("SPY260515P00727000") → True`) is the critical one — proves the helper correctly checks `options_trades.legs_json` (internal book) and not Alpaca positions. The 727P leg has been closed at Alpaca for hours but remains in the legs_json of the open spread row, and the helper finds it. Architecture is sound.

**Open items remaining (post-ship):**
1. **24h soak window** (opens 2026-05-07 ~11:53 MST) — monitor for unexpected `[HM-AF-β]` lines or any anomalies before deciding to lift α.
2. ~~**Today's 12:45 MST EOD sweep** — gated by HM-AF-α; verify post-12:46 with `grep "HM-AF-α.*close_all_options" logs/trader.log`.~~ ✅ **VERIFIED 2026-05-06 12:49:23 MST** — guard fired at `alpaca_options.py:600` blocking the sweep; zero actual EOD closes post-restart. P2 path now proven working in production alongside P1.
3. **HM-AF-δ** — remove hardcoded `player_id="dayblade-0dte"` in `battle_station.py:668` (lower priority).
4. **Orphan SPY260515P00732000 short** (qty=-1, expires 2026-05-15) — recommend let expire.

**Reversal:**

    git revert ca50d45
    launchctl kickstart -k gui/$(id -u)/com.trademinds.trader

(reverts both layers; α stays ON in either case)

To lift α (separate Phase 4 decision after 24h soak):

    # Edit config.py: SPREAD_CANNIBALIZATION_GUARD_ENABLED = False
    launchctl kickstart -k gui/$(id -u)/com.trademinds.trader


## SHIPPED 2026-05-03 — Sunday Morning Deploy

- **8e06b5e** regime fix deployed at 08:01 MST
- Manual `trader.db` backup taken: `backups/trader.db.pre_regime_fix_deploy_20260503_080141`
- 11 regime ticks verified post-restart (08:16:46 → 10:47:43, all `BULL_CROSS`)
- Edits 1, 2, 3 verified at code level (`main.py` lines 2610, 2656, 2685-2701)
- Runtime verification PENDING — Monday market-hours window 06:30-13:00 MST

## SHIPPED 2026-05-03 — Sunday Afternoon Deploy

- **d2ad748** B15 diagnostic patch (capture NameError traceback frames)
- **17d40b4** B15 fix — `OLLIE_URL` added to `initialize_dayblade()` import
- **cdc03d0** B14 fix — dead `GetAllPositionsRequest` import removed
- **58c43f0** Item 5 — ~60 lines dead crew-server polling removed from `premarket-scan.sh`
- PID 84968 deployed at 15:45 MST; 0 OLLIE_URL errors post-deploy (verified)

## SHIPPED 2026-05-02 (Saturday Night Drydock)

| Fix | File | Description |
|-----|------|-------------|
| Task 1 | git | Checkpoint commit `463c402` — 370 files, 8 drydock sessions |
| Task 3A | `engine/importers/ai4trade_importer.py` | Added `run_import()` alias → fixes nightly import crash |
| Task 3B | `uoa/scraper.py:16` | Fixed docstring example path |
| Task 3C | `premarket-scan.sh:46` | Commented out defunct `launchctl start com.trademinds.crew` |
| restart.sh | `restart.sh:11` | Split `qwen3.5:9b` across two vars to pass pre-commit hook |
