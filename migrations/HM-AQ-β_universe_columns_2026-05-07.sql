-- HM-AQ-β 2026-05-07 — Add market_cap + options_eligible columns to scan_universe.
--
-- Captain decision logged 2026-05-07 (HM-AQ commit 773effe, docs/UNIVERSE.md):
-- WATCH_STOCKS expands from 20 mega-cap manual list to ~500-800 dynamic universe
-- matching market cap ≥ $5B + daily $vol ≥ $50M, refreshed weekly.
--
-- Substrate (Captain Option 4 hybrid): reuse the existing scan_universe table
-- (already 2,741 rows, refreshed Apr 5 → May 7 via engine/deep_scan.py).
-- This migration adds the two columns the new HM-AQ-β refresher needs.
--
-- Pre-state expected:
--   PRAGMA table_info(scan_universe) returns columns:
--   id, symbol, name, exchange, sector, avg_volume, avg_price, last_updated
--
-- Post-state expected:
--   ... plus market_cap REAL, options_eligible INTEGER DEFAULT 0
--
-- Sacred-data: ADD COLUMN only. Rollback would require schema-rebuild
-- (SQLite has no DROP COLUMN until 3.35); avoid by not shipping wrong fields.
--
-- Rollback (if absolutely needed):
--   sqlite3 data/trader.db <<'EOF'
--   BEGIN;
--   ALTER TABLE scan_universe RENAME TO scan_universe_old;
--   CREATE TABLE scan_universe (
--     id INTEGER PRIMARY KEY AUTOINCREMENT,
--     symbol TEXT NOT NULL UNIQUE,
--     name TEXT,
--     exchange TEXT,
--     sector TEXT,
--     avg_volume REAL,
--     avg_price REAL,
--     last_updated TEXT NOT NULL
--   );
--   INSERT INTO scan_universe (id, symbol, name, exchange, sector, avg_volume, avg_price, last_updated)
--     SELECT id, symbol, name, exchange, sector, avg_volume, avg_price, last_updated FROM scan_universe_old;
--   DROP TABLE scan_universe_old;
--   COMMIT;
--   EOF

BEGIN;

ALTER TABLE scan_universe ADD COLUMN market_cap REAL;
ALTER TABLE scan_universe ADD COLUMN options_eligible INTEGER DEFAULT 0;

COMMIT;

-- Verification:
-- PRAGMA table_info(scan_universe);  -- shows the two new columns
-- SELECT COUNT(*) FROM scan_universe; -- should still show prior row count
