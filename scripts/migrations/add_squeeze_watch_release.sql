-- HM-SQUEEZE-RELEASE-DETECT migration (banked 2026-05-24)
-- Adds release-detection columns to squeeze_watch so the BB/KC scanner can
-- mark when a watched coil actually breaks out — the tradeable moment.
--
-- Idempotent on fresh DBs: the columns are also added via PRAGMA-guarded
-- conditionals in engine/bbkc_squeeze_scanner.py::_ensure_schema. This file
-- is the canonical migration record for tooling that replays from scratch.

ALTER TABLE squeeze_watch ADD COLUMN released_at TEXT;
ALTER TABLE squeeze_watch ADD COLUMN release_direction TEXT;       -- 'up' | 'down'
ALTER TABLE squeeze_watch ADD COLUMN release_volume_ratio REAL;
ALTER TABLE squeeze_watch ADD COLUMN release_close REAL;

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_release
    ON squeeze_watch(kind, released_at DESC) WHERE released_at IS NOT NULL;
