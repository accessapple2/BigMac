-- HM-DASH.4 migration: squeeze_candidates
-- Lower-tier (raw_score 3-4) companion to squeeze_watch (raw_score >= 5).
-- Scanner: engine/squeeze_scanner.py::_persist_results routes by score.
-- Captain constraint: squeeze_watch semantics UNCHANGED — see HM-DASH4.md.

CREATE TABLE IF NOT EXISTS squeeze_candidates (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    scan_ts         TEXT NOT NULL,
    short_pct       REAL,
    float_m         REAL,
    vol_ratio       REAL,
    rsi             REAL,
    breakout_score  REAL,
    composite_score REAL NOT NULL,
    threshold_tier  TEXT,                  -- mirrors squeeze_watch; usually 'WATCH' here (composite 30-40)
    price_at_scan   REAL,
    days_to_cover   REAL,                  -- HM-DASH.4: real column (squeeze_watch has it in notes only)
    notes           TEXT,
    dismissed       INTEGER DEFAULT 0,
    dismissed_at    TEXT,
    dismissed_reason TEXT,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_sc_symbol_ts
    ON squeeze_candidates(symbol, scan_ts DESC);

CREATE INDEX IF NOT EXISTS idx_sc_active
    ON squeeze_candidates(dismissed, composite_score DESC) WHERE dismissed = 0;
