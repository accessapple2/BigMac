-- HM-SQUEEZE-BBKC-COMPRESSION migration (banked 2026-05-24)
-- Extends squeeze_watch with a `kind` column so BB/KC volatility-compression
-- scanner rows coexist with the original short-interest rows in one table.
--
-- Idempotent on fresh DBs: the kind/bbkc_duration_days columns are added with
-- IF NOT EXISTS via PRAGMA-guarded conditionals at the engine boot site
-- (engine/bbkc_squeeze_scanner.py::_ensure_schema). This file is the canonical
-- migration record for tooling that replays the schema from scratch.
--
-- Backfill rule: every existing row predates the kind column → 'short_interest'.

ALTER TABLE squeeze_watch ADD COLUMN kind TEXT NOT NULL DEFAULT 'short_interest';
ALTER TABLE squeeze_watch ADD COLUMN bbkc_duration_days INTEGER;

UPDATE squeeze_watch SET kind='short_interest' WHERE kind IS NULL OR kind='';

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_kind_ts
    ON squeeze_watch(kind, scan_ts DESC) WHERE dismissed = 0;
