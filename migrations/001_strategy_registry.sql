-- Strategy Registry migration (Task 3)
-- Sacred DB rule: CREATE IF NOT EXISTS only. No drops, no truncates.

CREATE TABLE IF NOT EXISTS strategies (
    strategy_id   TEXT PRIMARY KEY,
    display_name  TEXT NOT NULL,
    enabled       INTEGER NOT NULL DEFAULT 0,
    description   TEXT,
    created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_strategies_enabled
    ON strategies(enabled);

-- Add strategy_id column to trades (idempotent via schema check pattern)
-- SQLite doesn't have IF NOT EXISTS for ALTER TABLE, so we guard in Python.
-- See apply_migration.py.

-- Note: existing 'strategy_signals' table has a different schema (scan signals).
-- Registry-generated signals go in 'registry_signals' to avoid collision.
CREATE TABLE IF NOT EXISTS registry_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    strategy_id   TEXT NOT NULL,
    ticker        TEXT NOT NULL,
    action        TEXT NOT NULL,
    asset_type    TEXT NOT NULL,
    direction     TEXT NOT NULL,
    exit_tag      TEXT NOT NULL DEFAULT 'single',
    max_risk_usd  REAL NOT NULL,
    confidence    REAL,
    payload_json  TEXT,
    reasoning     TEXT,
    generated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    executed      INTEGER NOT NULL DEFAULT 0,
    executed_at   TIMESTAMP,
    FOREIGN KEY (strategy_id) REFERENCES strategies(strategy_id)
);

CREATE INDEX IF NOT EXISTS idx_registry_signals_strategy
    ON registry_signals(strategy_id, generated_at);

CREATE INDEX IF NOT EXISTS idx_registry_signals_ticker
    ON registry_signals(ticker, generated_at);
