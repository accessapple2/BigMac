-- HM-AQ-β v2 2026-05-07 — Add ticker_type column to scan_universe.
--
-- Captain refinement during dry-run (2026-05-07): ETFs were being excluded
-- from the universe because /v3/reference/tickers/{TICKER} returns no
-- market_cap for ETF tickers (they have AUM, not cap). Excluding them
-- would lose TQQQ, IWM, XLE, XLU, XLP, XLB, etc. — names the system
-- actively trades.
--
-- Resolution: store ticker_type alongside market_cap so the filter
-- query can branch:
--   - type='CS'  : require market_cap >= $5B AND dollar_volume >= $50M
--   - type='ETF' : require dollar_volume >= $50M only (no cap analog)
--   - type='ETN' : skipped at refresh time (debt notes; not in scope)
--
-- This is the v2 migration in the HM-AQ-β bundle. v1 (commit 5eb479c)
-- added market_cap + options_eligible columns. v2 adds ticker_type.
--
-- Default value 'CS' is a safe assumption for the 2,741 pre-existing rows
-- (deep_scan.py was originally a stocks-only refresher; rows are stock-shaped).
-- The new HM-AQ-β refresher will overwrite this on first run.

BEGIN;
ALTER TABLE scan_universe ADD COLUMN ticker_type TEXT DEFAULT 'CS';
COMMIT;

-- Verification:
-- PRAGMA table_info(scan_universe);  -- shows ticker_type column with default 'CS'
