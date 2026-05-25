-- HM-CLEAN-STALE-ARCHIVE-NOT-DELETE Phase 1 migration (2026-05-25)
--
-- Creates portfolio_history_archived as the destination table for the
-- archive-then-delete pattern that replaces the pre-emergency-lock
-- behavior of dashboard/app.py::clean_stale_snapshots (which DELETEd
-- without archiving, violating the "Trade data is gold" sacred rule).
--
-- Schema mirrors portfolio_history (player_id is TEXT FK to ai_players,
-- season is INTEGER) plus 4 audit-trail columns and 2 indexes.
--
-- Parent ticket: HM-DATA-INTEGRITY-FORENSICS
-- Emergency lock at commit 45e57e1 (in main); this migration is the
-- precursor for the proper fix that re-enables the endpoint with
-- archive-then-delete semantics.
--
-- Pre-migration backup at:
--   data/trader.db.backup-2026-05-25-pre-HM-CLEAN-STALE-ARCHIVE
--
-- Rollback:
--   DROP TABLE portfolio_history_archived;
--   (rollback is safe; no other code references the new table until
--    the Phase 2 endpoint rewrite ships)

CREATE TABLE IF NOT EXISTS portfolio_history_archived (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    original_row_id     INTEGER NOT NULL,
    player_id           TEXT    NOT NULL,
    total_value         REAL,
    cash                REAL,
    positions_value     REAL,
    recorded_at         TIMESTAMP,
    season              INTEGER,
    -- Audit-trail columns ---------------------------------------------
    archived_at         TEXT    NOT NULL DEFAULT (datetime('now')),
    archived_by         TEXT    NOT NULL,  -- e.g. 'clean_stale_snapshots'
    archive_reason      TEXT    NOT NULL,  -- free-form reason string
    archive_session_id  TEXT    NOT NULL,  -- UUID grouping a single archive batch
    restored_at         TEXT             -- NULL until/unless the row is restored
);

CREATE INDEX IF NOT EXISTS idx_archived_player_season
    ON portfolio_history_archived(player_id, season);

CREATE INDEX IF NOT EXISTS idx_archived_at
    ON portfolio_history_archived(archived_at);

CREATE INDEX IF NOT EXISTS idx_archived_session
    ON portfolio_history_archived(archive_session_id);
