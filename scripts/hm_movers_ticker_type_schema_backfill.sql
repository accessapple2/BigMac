-- HM-MOVERS-TICKER-TYPE-SCHEMA+BACKFILL
-- Captain pre-authorized ALTER TABLE 2026-05-20 Wave 3.
-- Closes Tuesday's HALT: mover_watchlist.ticker_type column did not exist.
-- Source: scan_universe.ticker_type (CS=2850 ETF=176 verified 2026-05-20 09:34 AZ).
--
-- This .sql file is the canonical statement record. The actual safe execution
-- path is the companion Python script `hm_movers_ticker_type_schema_backfill.py`
-- which adds idempotency guards (column-exists check before ALTER, etc.) per
-- the tests in `tests/test_hm_movers_ticker_type_schema_backfill.py`.

BEGIN TRANSACTION;

-- 1. Add ticker_type column to mover_watchlist (will fail on re-run; the
--    Python wrapper checks `PRAGMA table_info` first to skip if present).
ALTER TABLE mover_watchlist ADD COLUMN ticker_type TEXT;

-- 2. Backfill from scan_universe where match exists.
--    Mover symbols NOT in scan_universe (warrants, OTC, fringe IPOs)
--    keep ticker_type=NULL — that's the natural shape of the data.
UPDATE mover_watchlist
   SET ticker_type = (
     SELECT ticker_type FROM scan_universe
     WHERE scan_universe.symbol = mover_watchlist.symbol
   )
 WHERE EXISTS (
   SELECT 1 FROM scan_universe
   WHERE scan_universe.symbol = mover_watchlist.symbol
 );

-- 3. Verification SELECTs (results captured for Captain review).
SELECT 'total_rows' AS metric, COUNT(*) AS value FROM mover_watchlist
UNION ALL
SELECT 'still_null'  AS metric, COUNT(*) AS value FROM mover_watchlist WHERE ticker_type IS NULL
UNION ALL
SELECT 'not_null'    AS metric, COUNT(*) AS value FROM mover_watchlist WHERE ticker_type IS NOT NULL;

SELECT COALESCE(ticker_type, '(null)') AS ticker_type, COUNT(*) AS rows
  FROM mover_watchlist
 GROUP BY ticker_type
 ORDER BY rows DESC;

-- COMMIT after Captain reviews verification output.
-- ROLLBACK if distribution is anomalous (e.g., zero matches, or distribution
-- doesn't mirror scan_universe's CS-dominant ratio).
COMMIT;
