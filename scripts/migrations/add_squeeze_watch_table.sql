-- HM-AO-β migration: squeeze_watch
-- Pattern mirrors ghost_options_watch (Ghost Watcher, NOT a voter).
-- Scanner: engine/squeeze_scanner.py
-- Writer:  engine/squeeze_scanner.py::_persist_results (added in Task 2)
-- Voter promotion: deferred (separate epic, requires 30d+ evidence).

CREATE TABLE IF NOT EXISTS squeeze_watch (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    symbol          TEXT NOT NULL,
    scan_ts         TEXT NOT NULL,                  -- ISO8601, when scanner picked it
    short_pct       REAL,                            -- short interest as % of float
    float_m         REAL,                            -- float in millions
    vol_ratio       REAL,                            -- current vol / 30d avg
    rsi             REAL,                            -- RSI(14)
    breakout_score  REAL,                            -- 0-1; 1.0 if price > 10d high
    composite_score REAL NOT NULL,                   -- 0-100 normalized (scanner score * 10)
    threshold_tier  TEXT,                            -- 'WATCH' (50-74), 'ALERT' (75-89), 'PRIORITY' (90+)
    price_at_scan   REAL,
    notes           TEXT,                            -- scanner-emitted context (days_to_cover, day_change_pct, raw score)
    ntfy_sent       INTEGER DEFAULT 0,               -- 1 if ntfy fired
    ntfy_deferred   INTEGER DEFAULT 0,               -- 1 if deferred by quiet hours; eligible on next post-06:00 ET scan
    dismissed       INTEGER DEFAULT 0,               -- Admiral can dismiss via dashboard
    dismissed_at    TEXT,
    dismissed_reason TEXT,
    created_at      TEXT DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_symbol_ts
    ON squeeze_watch(symbol, scan_ts DESC);

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_active
    ON squeeze_watch(dismissed, scan_ts DESC) WHERE dismissed = 0;

CREATE INDEX IF NOT EXISTS idx_squeeze_watch_tier
    ON squeeze_watch(threshold_tier, scan_ts DESC) WHERE dismissed = 0;
