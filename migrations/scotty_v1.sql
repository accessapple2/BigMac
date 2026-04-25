-- migrations/scotty_v1.sql
--
-- Scotty v1 — Short Squeeze Surveillance tables
-- Target DB: data/trader.db
--
-- Sacred DB rule respected: CREATE only, IF NOT EXISTS, no DROP/DELETE/TRUNCATE.
-- Safe to re-run. Leaves existing ~170 tables untouched.
--
-- Run with:   sqlite3 data/trader.db < migrations/scotty_v1.sql

CREATE TABLE IF NOT EXISTS scotty_watchlist (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    scan_date       TEXT    NOT NULL,   -- ISO date (UTC) of the scan
    ticker          TEXT    NOT NULL,
    score           INTEGER NOT NULL,   -- 0..4 in v1, will be 0..5 in v2
    short_pct       REAL,               -- % of float short
    float_shares_m  REAL,               -- float in millions
    days_to_cover   REAL,
    vol_ratio       REAL,               -- relative volume
    price           REAL,
    rsi             REAL,
    above_10d_high  INTEGER,            -- 0/1
    signals_json    TEXT,               -- full signals dict as JSON
    scotty_version  TEXT NOT NULL DEFAULT 'v1',
    created_at      TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_scotty_scan_date ON scotty_watchlist(scan_date);
CREATE INDEX IF NOT EXISTS idx_scotty_ticker    ON scotty_watchlist(ticker);
CREATE INDEX IF NOT EXISTS idx_scotty_score     ON scotty_watchlist(score DESC);

-- Per-ticker tracking: when we first saw it, peak score/date.
-- Useful for "how long has this name been cooking?" in the dashboard.
CREATE TABLE IF NOT EXISTS scotty_first_seen (
    ticker       TEXT    PRIMARY KEY,
    first_date   TEXT    NOT NULL,
    first_score  INTEGER NOT NULL,
    peak_score   INTEGER NOT NULL,
    peak_date    TEXT    NOT NULL,
    updated_at   TEXT    NOT NULL DEFAULT CURRENT_TIMESTAMP
);
