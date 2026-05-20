-- HM-HALTED-AT-BACKFILL — recover 3 halted_at values from halt_reason text
--
-- Audit #6A flagged that halt_reason text carries the halt date but
-- halted_at is the queryable column. CLAUDE.md "Manual halt SQL pattern"
-- doctrine: halted_at is mandatory. Three pre-doctrine rows currently
-- have halted_at IS NULL while halt_mode='full'; their halt_reason texts
-- name the date explicitly. This script backfills using those dates.
--
--   chekov       → 2026-05-11 (orphan-row hard-halt date)
--   super-agent  → 2026-05-11 (reconcile date)
--   webull       → 2026-05-13 (HM-WEBULL-LIQUIDATED date)
--
-- Captain verification ledger:
--   - chekov 2026-05-11   ✓ memory + prior-session conversation
--   - super-agent 2026-05-11 ✓ halt_reason text
--   - webull 2026-05-13   ✓ HM-WEBULL-LIQUIDATED memory
--
-- Backup (taken pre-execution):
--   backups/ai_players_pre_HM-HALTED-AT-BACKFILL_20260519_181715.sql
--
-- Re-running this script is safe (each UPDATE has AND halted_at IS NULL
-- guard) — a second run is a no-op against already-backfilled rows.
--
-- Run:
--   sqlite3 data/trader.db < scripts/hm_halted_at_backfill.sql
--
-- Post-execution verification: the final SELECT must report
--   remaining_null_count = 0

BEGIN TRANSACTION;

UPDATE ai_players
   SET halted_at   = '2026-05-11 00:00:00',
       halt_reason = halt_reason || ' (halted_at backfilled HM-HALTED-AT-BACKFILL 2026-05-19)'
 WHERE id = 'chekov'
   AND halted_at IS NULL;

UPDATE ai_players
   SET halted_at   = '2026-05-11 00:00:00',
       halt_reason = halt_reason || ' (halted_at backfilled HM-HALTED-AT-BACKFILL 2026-05-19)'
 WHERE id = 'super-agent'
   AND halted_at IS NULL;

UPDATE ai_players
   SET halted_at   = '2026-05-13 00:00:00',
       halt_reason = halt_reason || ' (halted_at backfilled HM-HALTED-AT-BACKFILL 2026-05-19)'
 WHERE id = 'webull'
   AND halted_at IS NULL;

-- In-transaction verify — these run against the post-UPDATE state.
-- If anything looks wrong, Captain can manually ROLLBACK before COMMIT.
.print
.print === In-transaction verify ===
SELECT id, halt_mode, halted_at, halt_reason
  FROM ai_players
 WHERE id IN ('chekov', 'super-agent', 'webull')
 ORDER BY id;

.print
.print === Remaining NULL halted_at rows with halt_mode != active (must be 0) ===
SELECT COUNT(*) AS remaining_null_count
  FROM ai_players
 WHERE halted_at IS NULL
   AND halt_mode != 'active';

COMMIT;

-- Post-commit sanity (outside tx) — duplicates the verify against
-- the persisted state so the script output is self-contained.
.print
.print === Post-commit verify (persisted state) ===
SELECT id, halt_mode, halted_at, halt_reason
  FROM ai_players
 WHERE id IN ('chekov', 'super-agent', 'webull')
 ORDER BY id;

.print
.print === Final NULL halted_at count (must be 0) ===
SELECT COUNT(*) AS final_null_count
  FROM ai_players
 WHERE halted_at IS NULL
   AND halt_mode != 'active';
