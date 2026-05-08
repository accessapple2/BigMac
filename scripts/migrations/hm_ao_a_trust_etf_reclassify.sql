-- HM-AO-α one-shot: trust-ETF reclassification
-- Date: 2026-05-08
-- Source: docs/SCOTTY_INFRA_AUDIT.md §K Top 10 Item, reports/grok_diff_2026-05-08.md §2.A,
--         docs/HM-AO-A_TRUST_ETF_FIX.md
--
-- The 5 physical-trust ETFs below are misclassified as ticker_type='CS'
-- in scan_universe because Polygon's /v3/reference/tickers API returns
-- 'CS' for trust structures. They have NULL market_cap (trusts report
-- AUM, not market cap), so they're rejected by the CS-branch filter
-- (`market_cap >= MIN_MARKET_CAP`) and never reach the ETF branch.
--
-- This migration corrects the existing rows. The loader fix
-- (engine/universe_refresh.py::TRUST_ETF_OVERRIDES) ensures the next
-- weekly Polygon refresh classifies them correctly without re-applying
-- this script.
--
-- Pre-snapshot: backups/trader.db.pre-hm-ao-a-20260508_065400 (257.8 MB)
-- Idempotent: WHERE clause guards against re-running on already-corrected rows.

UPDATE scan_universe
   SET ticker_type  = 'ETF',
       last_updated = datetime('now')
 WHERE symbol IN ('GLD', 'GLDM', 'IAU', 'SIVR', 'SLV')
   AND ticker_type = 'CS';

-- Verification (run after):
--   SELECT symbol, ticker_type, market_cap, options_eligible, last_updated
--     FROM scan_universe WHERE symbol IN ('GLD','GLDM','IAU','SIVR','SLV');
--
-- Expected: 5 rows, all ticker_type='ETF', market_cap NULL (unchanged),
-- last_updated bumped to now.
