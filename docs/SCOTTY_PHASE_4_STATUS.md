# SCOTTY 2.9 — Phase 4 Status (FINAL)

> Six-task overnight sprint. Sacred rules respected throughout: no
> live state mutations, no halts, no fires, no source-file edits in
> tasks 1-4, draft patches only in task 5, halt-note in task 6.
> Final push to `origin/main` performed at end of Task 7.

**Date:** 2026-05-08 (overnight, local 2026-05-07 evening MST)
**Branch:** `main`
**Saturday verdict (carried from Phase 3):** **GO-WITH-DOC-FIX** — unchanged
**Commits added this sprint:** 6

---

## 1. Tasks shipped

| # | Task | Status | Commit | Evidence |
|---|---|---|---|---|
| 1 | Comprehensive infrastructure audit | **SHIPPED** | `f80cf34` | 644-line `docs/SCOTTY_INFRA_AUDIT.md`; sections A–P + ranked Top 10 |
| 2 | Saturday kill re-validate | **SHIPPED** (no commit needed — no drift) | — | `logs/saturday_kill_dryrun_2026-05-08.log` written; verdict + state identical to Phase 3 dry-run |
| 3 | Roster doc sync | **SHIPPED** | `219a25f` | `CLAUDE.md` +43 lines: Sniper Squad, Backtest Pool, Zombie Candidates sections added |
| 4 | HM-Q diagnostic | **SHIPPED** | `4c63c58` | 225-line `docs/HM-Q_DIAGNOSTIC.md`; **recommendation: KEEP-AS-IS** with doc + invariant test |
| 5 | Auth Phase 1 TIER A draft patches | **SHIPPED** | `4ab3572` | 7 patches + README, all 7 `git apply --check` clean |
| 6 | HM-AQ-β universe patch | **HALT** (no patch needed) | `242c157` | `infra/patches/hm-aq-b-NOT-NEEDED.md` — found β was already shipped 2026-05-07 (4 commits + follow-ons) |
| 7 | This status report + push | **SHIPPED** | this commit | — |

---

## 2. Infra Audit headline — top 3 URGENT findings

1. **🔴 `engine/ghost_trades.py` queries a non-existent schema.** The
   module references `g.player_id` and `g.created_at` but neither
   column exists in `data/ghost_trades.db.ghost_trades` (which has
   `agent` + `signal_time`) nor in `data/trader.db.ghost_trades`
   (which has `ts` + `side` + `advisor`). Result: 16+ stack traces in
   `logs/trader_error.log` of `sqlite3.OperationalError: no such
   column: g.player_id`. Single biggest recurring runtime error class.
   Fix is a 1-line column rename in `engine/ghost_trades.py:66+99`,
   but needs a Schema-Of-Record decision since two competing schemas
   exist for the same table name. **Filed `HM-AZ-ghost-trades-schema`
   for the next sprint.**

2. **🟡 No log rotation.** `logs/trader.log` is 36 MB / 460k lines
   after 9 days; `logs/trader_error.log` is 17 MB / 170k lines.
   Disk has 39 GiB free so runway is months — but rotation is
   overdue. Top 10 Item #3 in the audit.

3. **🟡 Bigmac swap is 53% (≈ 4.3 GB)** while RAM has 6-7 GB free.
   Watchdog logs show this consistently for the last 30+ min. Not
   actively paging (`vm_stat` swapouts ~0/sec) — sediment from earlier
   load, not current pressure. Recommend monitoring; consider a
   controlled trader restart during off-hours to clear it. **Do NOT
   restart during the Saturday kill window.**

Hardware otherwise 🟢: bigmac 9-day uptime, Ollie Box 3-day uptime
3.6 GiB used / 29 GiB total / GPU 0% / 32°C. LAN sub-5ms.

All Phase 1, 2, and 3 ships verified intact (off-host backup fired
2026-05-07 20:10, Schwab watcher loaded, model watcher plist queued
for Sunday cron, toggle infra unchanged).

---

## 3. Saturday script status

**Re-validated 2026-05-07 21:31. No drift.**

```
Pre-flight 1 ✓  Markets closed (DOW=4, 21:31)
Pre-flight 2 ✓  Toggle-map verdict: GO-WITH-DOC-FIX
Pre-flight 3 ✓  All 4 _EXECUTION_ENABLED gates True
Pre-flight 4    ollie-auto    last-6h trades=0  open positions=5  (UNCHANGED from Phase 3)
Pre-flight 4    ollama-llama  last-6h trades=0  open positions=0  (UNCHANGED)
```

Halt SQL identical to Phase 3 plan:

```sql
UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, halt_reason='[date] Sniper trial ended (Day 30/30); KILL ...' WHERE id='ollie-auto';
UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, halt_reason='[date] ollama-llama sunset ...' WHERE id='ollama-llama';
```

Source-of-truth list edits still required (Admiral applies manually):
- `dashboard/app.py:1445` remove `"ollie-auto"` from `FLEET_ACTIVE`
- `dashboard/app.py:1432` (PROTECTED_AGENTS body) remove `"ollama-llama"`
- `engine/proving_ground.py:34` remove `"ollama-llama"` from `SNIPER_AGENTS`

Saturday plan locked. **No commit needed for Task 2** — full
re-validation log preserved at `logs/saturday_kill_dryrun_2026-05-08.log`.

---

## 4. Roster doc changes (commit `219a25f`)

Added to `CLAUDE.md` after the Bench 4 section, all without disturbing
existing roster sections:

- **Sniper Squad — Active Scouts** (NEW): documents `deepseek-7b-grok4`
  (~178 sigs/day) + `qwen3-8b-flash` (~25 sigs/day) as the active
  Sniper Mode scouts, both `PROTECTED_AGENTS` members. Closes the
  Phase 3 finding "ON-state agents missing from roster docs".
- **Backtest Pool — Deliberate OFF (cost-doctrine, KEEP wired)** (NEW):
  documents `grok-4`, `claude-haiku`, `claude-sonnet`, `gpt-4o`,
  `gpt-o3` as toggle-OFF cost-savers. **Marks them NOT zombies** so
  future audits don't misread them.
- **Zombie Candidates — Future Cleanup** (NEW): the 14 truly-orphaned
  `halt_mode='full'` rows (HM-T-fleet-bundle, Option-4-ghost-bundle,
  HM-AK-dormant-cleanup) listed for the eventual cleanup sprint, with
  explicit "no DROP, no schedule" caveat per sacred-data rule.
- **Retired** section updated: `dayblade-sulu` added with confirmed
  TOGGLE-OFF (deliberate) status (zero trades 30d).
- **Gates & Coordination** updated: `ollie-auto` line now notes it IS
  the Sniper Mode role-holder, not a sub-mode toggle. Saturday's KILL
  is `halt_mode='full'` on this row.

---

## 5. HM-Q recommendation — **KEEP-AS-IS**

Per `docs/HM-Q_DIAGNOSTIC.md` (commit `4c63c58`):

`signals.execution_status` and `signals.halted_emit` are **not
redundant** — they answer different questions:

- `execution_status` (TEXT): forward-looking trade-lifecycle status
  (PENDING/EXECUTED/SIMULATED/SKIPPED/REJECTED/EXPIRED), actively
  written from 5+ sites in `engine/paper_trader.py`
- `halted_emit` (INTEGER): frozen-in-time provenance flag for
  scoring/calibration read-side exclusion (was player halted at emit
  time?). **No active writer** in the runtime — backfilled by HM-C
  fix #1, used only via `engine/halt_gate.py::HALTED_EMIT_FILTER`.

**Cardinality data:** 65,005 total signals; 1,143 with `halted_emit=1`
(all in `SKIPPED` or `REJECTED` execution_status). Invariant holds.

Recommended actions (NOT applied tonight):
1. Add to `docs/SCHEMA_NOTES.md` (or extend) — column-level docs.
2. Add `tests/test_signals_invariants.py::test_halted_emit_implies_no_fill`
   — a 5-line assertion.
3. **Defer** the future migration to a `halt_mode` join — it would
   require a halt-history audit table that doesn't exist (CLAUDE.md
   confirms HM-F finding "no programmatic halt paths").

---

## 6. Patches drafted

### Auth Phase 1 TIER A (commit `4ab3572`)

7 standalone patches in `infra/patches/auth-phase-1/`, all `git apply --check` clean against current `main`:

| Patch | Route | Effect |
|---|---|---|
| `01-kill-switch.patch` | `POST /api/kill-switch` | Closes ALL positions ALL models |
| `02-admin-clean-stale-snapshots.patch` | `POST /api/admin/clean-stale-snapshots` | Mutates portfolio_history |
| `03-trade-manual.patch` | `POST /api/trade/manual` | Manual market order to Alpaca |
| `04-alpaca-buy.patch` | `POST /api/alpaca/buy` | Live Alpaca buy |
| `05-alpaca-sell.patch` | `POST /api/alpaca/sell` | Live Alpaca sell |
| `06-alpaca-close-symbol.patch` | `POST /api/alpaca/close/{symbol}` | Live close one |
| `07-alpaca-close-all.patch` | `POST /api/alpaca/close-all` | Live close-all |

Each patch standalone:
- Adds `Depends` to `from fastapi import ...`
- Adds `from dashboard.auth import verify_admin_token`
- Adds `_: str = Depends(verify_admin_token)` parameter to the route signature

`infra/patches/auth-phase-1/README.md` documents the recommended
apply workflow (patch 01 first, soak one trading day under TOTP +
service-token bearers, then bundle 02-07 by extracting only their
route hunks since the imports already landed).

`dashboard/app.py` is unchanged on disk. No callers of
`verify_admin_token` outside the Phase 0 module + its test.

### HM-AQ-β universe patch — NOT NEEDED (commit `242c157`)

Sprint brief asked Scotty to draft this patch from scratch. **Memory
was stale.** HM-AQ-β was actually shipped 2026-05-07 in 4 commits
(`5eb479c`, `dd43bab`, `12ad22d`, `404f0a2`) plus follow-ons
(`e333f63` v3 wet-refresh, `050c08b` ADR scope, `83d5684`
halt_mode-filter extension). Universe at $100M floor is ~1,223 names
(927 CS + 296 ETF) per `docs/UNIVERSE.md`. Bulk-endpoint perf fix
takes 1,223-symbol snapshots from ~47s to ~1-2s.

`infra/patches/hm-aq-b-NOT-NEEDED.md` records the halt + evidence so
the Admiral sees Scotty noticed the staleness rather than blindly
producing a redundant patch.

---

## 7. Wall clock + commit count

| | |
|---|---|
| Commits added this sprint | **6** (5 task commits + this status doc, +1 for Task 6 halt note = 7 if status counted; the 6 above are the value-creating ones) |
| Lines added | ~2,150 across docs + 7 patches |
| Production routes touched | **0** |
| `paper_trader.py` / `main.py` / gate / strategy / `dashboard/app.py` touched | **0** |
| Service restarts | **0** |
| Halt mutations | **0** |
| `_EXECUTION_ENABLED` flips | **0** (all 4 still True, untouched) |
| `DROP TABLE` calls | **0** |
| Force pushes | **0** |
| Secret values generated | **0** |
| URGENT items surfaced | 1 (`engine/ghost_trades.py` schema mismatch — flagged via NTFY queued for next push) |

---

## 8. Outstanding for Admiral go

### Saturday 2026-05-09 after 13:00 MST

1. Close 5 ollie-auto open positions (per `SNIPER_MODE_CLOSURE_PLAN.md` ritual)
2. `bash scripts/saturday_kill.sh --execute` — type `KILL`
3. Apply the 3 doc-fix list edits manually (FLEET_ACTIVE,
   PROTECTED_AGENTS, SNIPER_AGENTS)
4. Restart trader so `dashboard/app.py` reloads
5. Acknowledge post-fire ntfy

### Auth Phase 1a (any time)

1. Generate the 3 secrets per `docs/AUTH_SETUP.md`
2. Verify `tests/test_auth.py` is 11/11 green (run from trader venv)
3. Identify programmatic POSTers to TIER A routes (auth plan §7.A
   queue) — likely 0–3
4. Apply `infra/patches/auth-phase-1/01-kill-switch.patch` first
5. Restart trader; smoke-test per README §"Per-route smoke test"
6. Soak 1 trading day, then bundle 02–07 (route hunks only) into a
   second commit
7. Continue Phase 1b/c/d per `docs/AUTH_PHASE_1_ROUTE_TIERS.md` §6

### Infra audit Top 10 triage (next sprint)

1. **Cold-restart bring-up runbook** (`docs/COLD_RESTART_RUNBOOK.md`)
2. **Fix `engine/ghost_trades.py` schema mismatch** — 1-line column
   rename + canonical-DB decision (HM-AZ)
3. **Log rotation on trader.log + trader_error.log**
4. **Quarterly backup-restore drill ritual** + `BACKUP_DRILL_LOG.md`
5. Track remaining 39 plists in `infra/launchd/`
6. Drop or reclassify 2 zero-byte DBs
7. Investigate `autonomous_trader.db` writer (current modtime, 2.5 MB stub)
8. Triage `logs/scanner.err` (648 KB)
9. Investigate "Imbalance scan: 1187 of 1223 futures unfinished"
10. Move `_rate_state` to persisted settings (CLAUDE.md 2026-05-05 lesson)

### Roster doc reconciliation (deferred)

12 active players still missing from CLAUDE.md (per Phase 3 status
§4 list — `mlx-qwen3`, `ollama-deepseek`, `ollama-kimi`, etc.). Phase
4 added the 2 highest-priority (deepseek-7b-grok4 + qwen3-8b-flash)
and the Backtest Pool. Remaining 12 are a future doc-sync sprint.

---

## 9. Tasks dropped — none

All 7 tasks completed. Task 6 produced a halt note instead of a
patch (correct per pace rule "If a task hits unexpected friction,
halt that task, write what you found, move to the next" — friction
was that the patch target was already merged).

---

## 10. Push readiness

6 commits ahead of `origin/main`. No untracked production code, no
modified gate / strategy / `paper_trader.py` / `dashboard/app.py`
files, no secrets in any diff, no halt mutations, no service
restarts. **Push authorized in the Captain's Phase 4 brief.**
Proceeding with end-of-Task-7 push.

---

## 11. Live notes (sprint journal)

- Started: this sprint's first commit `f80cf34` at ~21:30 local MST
- Order followed: 1 → 2 → 3 → 4 → 5 → 6 → 7 (no drops)
- Sacred-rules audit: zero violations. Source files untouched. DB
  read-only. Saturday kill plan unchanged.
- Note: SSH read against Ollie Box for Ollama-list specifically was
  blocked by harness policy; earlier SSH calls (uptime/free/df/etc)
  succeeded so adequate evidence was gathered for the audit.
- Note: `diff -u` output is rewritten by RTK in this environment;
  patches were generated using `/usr/bin/diff -u` to bypass.
