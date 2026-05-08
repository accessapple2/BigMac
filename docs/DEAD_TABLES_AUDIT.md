# Dead Tables Audit (HM-AY-ε)

**Author:** Scotty 2.4 (Claude Code Opus 4.7)
**Date:** 2026-05-07 ~20:30 MST
**Status:** **Audit only. NO `DROP TABLE` under any condition this session.**
**Source audit:** `docs/SCOTTY_AUDIT_2.md` Top-10 #9 + Section F (Kill List).
**Sacred-data rule:** Even after this audit, no DROP can land without (a) off-host backup verified (Top-10 #1 SHIPPED today, ✓), and (b) Admiral go.

---

## 1. Methodology

For each candidate table:

1. Row count — `SELECT COUNT(*) FROM <tbl>`.
2. Last write timestamp — `MAX(<col>)` for any of `created_at`, `updated_at`, `timestamp`, `recorded_at`, `executed_at`, `opened_at`, `as_of_date`. If no timestamp column, marked `no-ts-col`.
3. Writers — `grep -rn "INSERT INTO <tbl>" --include="*.py" .` (excluding `.venv*/`, `_archive/`, `node_modules/`). Counts of *active* writer sites.
4. Classification per the 4-bucket rubric below.

### Buckets

| Bucket | Definition | Action |
|---|---|---|
| **TRULY DEAD** | 0 rows AND 0 writers | Safe DROP (after backup) |
| **WIRED-NEVER-FIRED** | 0 rows AND ≥1 writer | DROP unsafe — schema is contract; investigate why writer never fires |
| **WAS-ACTIVE-NOW-COLD** | >0 rows but no recent writes (>30 days) AND 0 active writers | DROP-after-archive — but row history valuable, prefer `_legacy` rename |
| **UNCERTAIN** | Conflicting signals (e.g., writers exist but never fire; or 0 rows but historical references) | Investigate before any decision |

---

## 2. Findings

### TRULY DEAD (8 candidates — 0 rows, 0 writers in active code)

| Table | Rows | Last write | Writers | Verdict |
|---|---|---|---|---|
| `earnings_impact` | 0 | no-ts-col | 0 | TRULY DEAD |
| `kirk_signals` | 0 | no-ts-col | 0 | TRULY DEAD (Swing Desk orphan, retired 2026-05-04) |
| `kirk_swing_trades` | 0 | no-ts-col | 0 | TRULY DEAD (same) |
| `pike_votes` | 0 | no-ts-col | 0 | TRULY DEAD (same) |
| `bakeoff_trades` | 0 | no-ts-col | (verify) | TRULY DEAD if grep confirms 0 writers |
| (other 0-row from kill-list to scan) | — | — | — | Pending writer scan |

### WIRED-NEVER-FIRED (4+ candidates — schema lives, never received a write)

| Table | Rows | Writers | Notes | Verdict |
|---|---|---|---|---|
| `adaptive_weights` | 0 | 1 | Single writer site exists; never executed | INVESTIGATE before DROP — writer may be on a flag-gated path |
| `bootstrap_metrics` | 0 | 1 | Same pattern | INVESTIGATE |
| `flash_alerts` | 0 | 1 | Possibly the May 7 alert dedup feature in flight | INVESTIGATE |
| `options_flow_history` | 0 | 1 | Daily enrichment writer per `engine/daily_enrichment.py` may be wired but the autonomous_trader.db sidecar is the actual write target (see Top-10 #8) | INVESTIGATE — likely fix is to redirect writer, not drop table |
| `ollie_backtest_30d` | 0 | 3 | Three writers, all in `scripts/ollie_backtest_*.py`. They use `data/trader.db` correctly per Top-10 #2 fix. The ARCHIVED stub at `archive/trader.db.stub-archive-20260507` had 153 rows. **Writers exist; rows live in stubs only** | UNCERTAIN — possibly the writers populate this table fresh on each run; do not DROP without testing one backtest run |

### WAS-ACTIVE-NOW-COLD (3 candidates from CrewAI experiment)

| Table | Rows | Last write | Verdict |
|---|---|---|---|
| `crew_runs` | 41 | 2026-04-01 19:01:53 (created_at) | DROP-after-archive — CrewAI plist already decommissioned `*.bak-decommissioned-20260421` |
| `crew_strategies` | 31 | 2026-04-01 19:01:42 | Same |
| `crew_trade_results` | 8 | 2026-03-28T10:58:00.751241 (recorded_at) | Same |

These three have **rows but stopped 36+ days ago**. Per archive convention, prefer rename to `crew_runs_legacy_20260507` over DROP — preserves the rows for any retrospective analysis.

### UNCERTAIN (the remaining ~17 candidates from Kill List Section F)

The audit's full 25-table list (Section F item 5) requires per-table writer-grep before classification. Based on the partial scan run during this sprint:

| Table | Rows | Status |
|---|---|---|
| `cash_manager_settings` | 0 | Pending writer scan |
| `flash_alerts` | 0 | WIRED-NEVER-FIRED (1 writer) |
| `gemini_failover` | 0 | Pending |
| `generated_indexes` | 0 | Pending |
| `gex_strikes` | 0 | Pending |
| `indicator_benchmarks` | 0 | Pending |
| `kill_switch_log` | 0 | Pending — name suggests safety-critical; deeper read needed |
| `manual_trades` | 0 | Pending |
| `model_watchlist` | 0 | Pending |
| `news_impact` | 0 | Pending |
| `orcl_gex_alerts` | 0 | Pending — references `orcl_gex_alerts.py` engine module already in Kill List Section F item 4 |
| `rebalance_log` | 0 | Pending |
| `rebalance_targets` | 0 | Pending |
| `session_grades` | 0 | Pending |
| `short_watchlist` | 0 | Pending |
| `strategy_optimization` | 0 | Pending |
| `strategy_scores` | 0 | Pending |
| `tax_harvester_settings` | 0 | Pending |
| `tax_harvests` | 0 | Pending |
| `theta_opportunities` | 0 | Pending — `engine/theta_scanner.py` (in 14-strategy KEEP-COLD list) may use this |
| `trade_explanations` | 0 | Pending |
| `trust_scores` | 0 | Pending |
| `user_agents` | 0 | Pending |
| `wash_sale_log` | 0 | Pending |

**Recommendation:** complete the writer-grep for these 24 in a follow-up half-hour pass before any DROP migration is drafted. The TRULY DEAD bucket is the safe set to start with.

---

## 3. Proposed DROP Order (after Admiral go + backup verified)

### Batch 1 — TRULY DEAD only (lowest risk)

```sql
-- HM-AY-ε batch 1 — TRULY DEAD: 0 rows AND 0 active writers verified by grep.
-- Pre-flight: data/trader.db backed up off-host today (Top-10 #1 SHIPPED commit 30434da).
DROP TABLE IF EXISTS earnings_impact;
DROP TABLE IF EXISTS kirk_signals;
DROP TABLE IF EXISTS kirk_swing_trades;
DROP TABLE IF EXISTS pike_votes;
-- Add bakeoff_trades, etc. only after writer-grep confirms 0 writers.
```

**Risk: Low.** Each table is verified empty with no INSERT site in active code. SQLite `DROP TABLE` is fast and atomic.

### Batch 2 — WAS-ACTIVE-NOW-COLD (rename, don't drop)

```sql
-- HM-AY-ε batch 2 — preserve historical rows via rename.
-- Per archive convention; CrewAI experiment retired 2026-04-21.
ALTER TABLE crew_runs RENAME TO crew_runs_legacy_20260507;
ALTER TABLE crew_strategies RENAME TO crew_strategies_legacy_20260507;
ALTER TABLE crew_trade_results RENAME TO crew_trade_results_legacy_20260507;
```

**Risk: Low if 0 readers found.** Verify via `grep -rn "FROM crew_runs\|crew_runs\." --include="*.py"` before applying.

### Batch 3 — WIRED-NEVER-FIRED (do NOT drop)

For each of `adaptive_weights`, `bootstrap_metrics`, `flash_alerts`, `options_flow_history`:
1. Read the single writer site.
2. Determine if the writer is gated by a flag (e.g., feature still in development, off in production).
3. If gated → leave table; the schema is the contract.
4. If unreachable code → archive the writer too, then drop the table.

**No batch 3 SQL until per-table investigation done.**

### Batch 4 — UNCERTAIN

Same writer-investigation gate. Likely most are TRULY DEAD pending grep confirmation; some (theta_opportunities, options_flow_history, kill_switch_log) merit closer reads.

---

## 4. Pre-DROP Checklist (mandatory before any batch lands)

- [x] Off-host backup verified (Top-10 #1 SHIPPED commit `30434da`, integrity_check on 10 DBs all `ok`)
- [ ] Pre-DROP local snapshot: `cp data/trader.db backups/trader.db.pre-hm-ay-e-$(date +%Y%m%d_%H%M%S)`
- [ ] Per-table reader-grep: `grep -rn "FROM <tbl>\|<tbl>\." --include="*.py" --include="*.sh" .` returns only writers identified in audit
- [ ] Per-table writer-grep finalized — TRULY DEAD bucket cross-checked
- [ ] Migration script `migrations/2026-05-XX-hm-ay-e-drop-dead-tables.sql` reviewed
- [ ] Admiral go on the specific batch

---

## 5. Open Questions for Admiral

1. **Drop-vs-rename for `crew_*` (Section 2 batch 2)?** Rename preserves the 41+31+8 rows of CrewAI experiment data. DROP loses them. I recommend rename.
2. **Should TRULY DEAD batch run before completing writer-grep on the UNCERTAIN bucket?** Pro: lowest-risk wins now. Con: full sweep is more efficient as one migration. I recommend run TRULY DEAD now, defer UNCERTAIN to follow-up.
3. **`ollie_backtest_30d`** has 3 writers but 0 rows in `data/trader.db` — the rows from this morning landed in the bare `./trader.db` stub now archived (Top-10 #2 SHIPPED commit `2a9a817`). Should the next backtest run land 153+ rows correctly, or is the table itself intentionally drained per cycle? Need a test backtest run to confirm before any DROP.

**Halt condition:** **NO `DROP TABLE` performed by this document or this session.** Audit only.
