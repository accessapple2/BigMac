-- HM-RS-RANK-VS-SPY migration (banked 2026-05-24)
-- Creates rs_rank table — daily 1-99 relative-strength rank vs SPY across
-- scan_universe. Foundational for HM-MINERVINI-TREND-FILTER and any future
-- leader-composite scan.
--
-- v1: single 12wk window (~60 trading days). IBD-blended (3/6/9/12mo) v2
-- can ADD COLUMNs later if signal proves useful.
--
-- Idempotent: engine/rs_rank.py::_ensure_schema mirrors this for fresh-DB
-- and migration drift handling. Run pattern: full-table rewrite each
-- nightly cycle (DELETE FROM rs_rank → INSERT all) — no time-series, the
-- row is always "today's rank."

CREATE TABLE IF NOT EXISTS rs_rank (
    symbol        TEXT NOT NULL,
    computed_at   TEXT NOT NULL,
    rs_return_pct REAL NOT NULL,      -- 12wk return, percent
    rs_vs_spy_pct REAL NOT NULL,      -- excess vs SPY, percentage points
    rs_rank       INTEGER NOT NULL,   -- 1-99 universe-wide percentile
    bars_used     INTEGER NOT NULL,   -- usually 60; degrades for short history
    PRIMARY KEY (symbol)
);

CREATE INDEX IF NOT EXISTS idx_rs_rank_rank
    ON rs_rank(rs_rank DESC);
