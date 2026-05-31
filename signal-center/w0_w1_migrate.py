#!/usr/bin/env python3
"""W0+W1 schema migration — idempotent.

Adds three NEW tables to signals.db. NEVER mutates or drops existing rows
(sacred-data rule). Safe to run repeatedly.

  - source_registry    (W1 §2.1) — the source freshness spine
  - daily_bars         (W0 reusable infra) — Polygon daily OHLCV cache,
                        also read by Wave-3 fill model + future backtests
  - scored_predictions (W0 §3.8) — per-(signal_id, horizon) expectancy output

Run:  ../venv/bin/python3 w0_w1_migrate.py
"""
import os
import sqlite3

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "signals.db")

DDL = """
CREATE TABLE IF NOT EXISTS source_registry (
    source_id     TEXT PRIMARY KEY,
    display_name  TEXT,
    endpoint      TEXT,
    cadence_class TEXT,          -- realtime|intraday|daily|weekly|monthly|snapshot|archive
    criticality   TEXT,          -- live_decision|context|archive
    ts_field      TEXT,          -- JSON path / column holding the source's own last-update time
    ts_format     TEXT,          -- iso|epoch|epoch_ms|stardate|db_max:<table>.<col>
    enabled       INTEGER DEFAULT 1,
    notes         TEXT
);

CREATE TABLE IF NOT EXISTS daily_bars (
    symbol     TEXT NOT NULL,
    date       TEXT NOT NULL,    -- YYYY-MM-DD (US/Eastern trading day)
    open       REAL,
    high       REAL,
    low        REAL,
    close      REAL,
    volume     REAL,
    source     TEXT DEFAULT 'polygon',
    fetched_at TEXT DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (symbol, date)
);
CREATE INDEX IF NOT EXISTS idx_daily_bars_date ON daily_bars(date);

CREATE TABLE IF NOT EXISTS scored_predictions (
    id                 INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id          INTEGER NOT NULL,   -- == trade_signals.id == signal_outcomes.signal_id
    horizon_days       INTEGER NOT NULL,   -- 1 | 3 | 5 | 10
    symbol             TEXT,
    entry_date         TEXT,
    action             TEXT,               -- raw BUY/SELL/WATCH/BUY_CALL (coarse axis)
    direction          TEXT,               -- long | short | non_directional
    setup_tag          TEXT,               -- normalized setup tag (fine axis), agent fallback
    agent_name         TEXT,               -- per-agent axis
    entry              REAL,
    stop               REAL,
    target             REAL,
    risk               REAL,               -- abs(entry - stop)
    window_high        REAL,
    window_low         REAL,
    window_close       REAL,
    outcome_v2         TEXT,               -- STOP | TP | OPEN | UNSCOREABLE
    r_multiple         REAL,               -- horizon-sliced, stop-first
    realized_r         REAL,               -- cumulative cross-check (signal_outcomes)
    closed             INTEGER DEFAULT 0,  -- 1 if STOP or TP
    scoreable          INTEGER DEFAULT 0,  -- 1 if directional + has stop + has bars + not gated
    unscoreable_reason TEXT,               -- non_directional|no_stop|missing_levels|no_bars|stale_gated
    stale_gated        INTEGER DEFAULT 0,
    is_oos             INTEGER DEFAULT 0,  -- 1 if entry in last-30-trading-day holdout
    scored_at          TEXT DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(signal_id, horizon_days)
);
CREATE INDEX IF NOT EXISTS idx_scored_horizon ON scored_predictions(horizon_days);
CREATE INDEX IF NOT EXISTS idx_scored_axes ON scored_predictions(action, setup_tag, agent_name);
"""


def main():
    db = sqlite3.connect(DB_PATH)
    db.executescript(DDL)
    db.commit()
    tables = [r[0] for r in db.execute(
        "SELECT name FROM sqlite_master WHERE type='table' "
        "AND name IN ('source_registry','daily_bars','scored_predictions') ORDER BY name"
    ).fetchall()]
    print("OK tables present:", tables)
    db.close()


if __name__ == "__main__":
    main()
