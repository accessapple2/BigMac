-- Migration 002: IV history for long-term IV rank tracking
-- Sacred DB rule: CREATE IF NOT EXISTS only.
-- Purpose: alongside the existing engine/high_iv_scanner (realized vol),
-- begin accumulating true implied vol snapshots per ticker, per day.
-- When we have 252+ days of data, we graduate from realized-vol rank
-- to true implied-vol rank.

CREATE TABLE IF NOT EXISTS iv_history (
    ticker       TEXT NOT NULL,
    as_of_date   DATE NOT NULL,
    implied_vol  REAL NOT NULL,
    source       TEXT NOT NULL DEFAULT 'unknown',
    captured_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (ticker, as_of_date)
);

CREATE INDEX IF NOT EXISTS idx_iv_history_ticker_date
    ON iv_history(ticker, as_of_date DESC);
