-- HM-HALTED-AT-ENFORCE — auto-fill halted_at when halt_mode flips non-active
-- Date: 2026-05-19
-- Source:
--   CLAUDE.md "Manual halt SQL pattern" + audit #6A
--   HM-HALTED-AT-BACKFILL (2026-05-19) — recovered 3 historical NULL rows
--
-- Background
-- ==========
-- The runbook says halted_at is mandatory whenever halt_mode != 'active'.
-- Audit #6A flagged 4 April halts where operators forgot the column. After
-- HM-HALTED-AT-BACKFILL recovered those NULLs, the next operator with a
-- copy-paste UPDATE that omits halted_at re-creates the same audit gap.
--
-- This migration installs two AFTER triggers that auto-fill halted_at to
-- CURRENT_TIMESTAMP whenever halt_mode lands non-active with halted_at
-- still NULL. The triggers are idempotent against explicit halted_at
-- values (they only fire WHEN NEW.halted_at IS NULL) and do not touch
-- halted_at on unhalt — CLAUDE.md preserves the historical timestamp.
--
-- Design choice — auto-fill (AFTER) over RAISE(ABORT) (BEFORE)
-- ===========================================================
-- Auto-fill is non-breaking: the existing canonical runbook UPDATE
-- pattern with an explicit halted_at = CURRENT_TIMESTAMP works
-- unchanged. RAISE(ABORT) would reject any historical-import UPDATE
-- that forgot the column and force operators to retry; auto-fill
-- captures the timestamp instead. The captured CURRENT_TIMESTAMP is
-- within sub-second of the operator's UPDATE.
--
-- Re-runnability
-- ==============
-- DROP TRIGGER IF EXISTS guards make this script safe to re-apply.
--
-- Backup
-- ======
-- backups/ai_players_pre_HM-HALTED-AT-BACKFILL_20260519_181715.sql
-- (covers the table state immediately before HM-HALTED-AT-BACKFILL +
-- HM-HALTED-AT-ENFORCE land in the same PR).
--
-- Apply
-- =====
--   sqlite3 data/trader.db < scripts/migrations/hm_halted_at_enforce.sql
--
-- Verify
-- ======
--   sqlite3 data/trader.db "SELECT name FROM sqlite_master WHERE type='trigger' AND name LIKE 'trg_ai_players_halted_at_%';"
--   -- Should list both trigger names.
--
-- Rollback
-- ========
--   DROP TRIGGER trg_ai_players_halted_at_on_update;
--   DROP TRIGGER trg_ai_players_halted_at_on_insert;

DROP TRIGGER IF EXISTS trg_ai_players_halted_at_on_update;
DROP TRIGGER IF EXISTS trg_ai_players_halted_at_on_insert;

CREATE TRIGGER trg_ai_players_halted_at_on_update
AFTER UPDATE OF halt_mode, halted_at ON ai_players
FOR EACH ROW
WHEN NEW.halt_mode != 'active' AND NEW.halted_at IS NULL
BEGIN
    UPDATE ai_players
       SET halted_at = CURRENT_TIMESTAMP
     WHERE id = NEW.id;
END;

CREATE TRIGGER trg_ai_players_halted_at_on_insert
AFTER INSERT ON ai_players
FOR EACH ROW
WHEN NEW.halt_mode != 'active' AND NEW.halted_at IS NULL
BEGIN
    UPDATE ai_players
       SET halted_at = CURRENT_TIMESTAMP
     WHERE id = NEW.id;
END;
