-- HM-MINERVINI-TREND-FILTER migration (banked 2026-05-24)
-- Creates minervini_trend table — daily Stage-2 uptrend evaluation per
-- Mark Minervini's 8-condition Trend Template. Reads bars from the same
-- Alpaca bulk cache RS-rank uses; nightly job rebuilds at 20:45 AZ
-- (15 min after rs_rank so rs_pass LEFT JOIN sees fresh data).
--
-- Idempotent: engine/minervini_filter.py::_ensure_schema mirrors this
-- for fresh-DB and migration-drift handling.

CREATE TABLE IF NOT EXISTS minervini_trend (
    symbol          TEXT NOT NULL,
    computed_at     TEXT NOT NULL,
    template_score  INTEGER NOT NULL,    -- 0-8 (count of conds passed)
    template_pass   INTEGER NOT NULL,    -- 1 if all 8 trend-template conds, else 0
    rs_pass         INTEGER NOT NULL,    -- 1 if rs_rank >= 70, else 0
    conds_json      TEXT NOT NULL,       -- JSON map of {cond_name: bool}
    price_at_scan   REAL NOT NULL,
    high_52w        REAL,                -- nullable for short history
    low_52w         REAL,
    bars_used       INTEGER NOT NULL,
    PRIMARY KEY (symbol)
);

CREATE INDEX IF NOT EXISTS idx_minervini_score
    ON minervini_trend(template_pass DESC, template_score DESC);
