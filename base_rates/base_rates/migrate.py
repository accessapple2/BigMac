"""Schema migration for base_rates. Idempotent."""
from __future__ import annotations

import argparse
import sqlite3
from pathlib import Path


SCHEMA = """
CREATE TABLE IF NOT EXISTS base_rate_features (
    symbol         TEXT NOT NULL,
    date           TEXT NOT NULL,
    close          REAL,
    pct_change     REAL,
    rsi14          REAL,
    rsi_slope      REAL,
    vix_close      REAL,
    vix_pct_change REAL,
    spy_above_200  INTEGER,
    fwd_5d_return  REAL,
    fwd_5d_maxdd   REAL,
    -- pre-computed bucket vector for fast match queries
    b_move         INTEGER,
    b_rsi          INTEGER,
    b_rsi_slope    INTEGER,
    b_vix          INTEGER,
    b_vix_move     INTEGER,
    b_trend        INTEGER,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_br_date
    ON base_rate_features(date);

CREATE INDEX IF NOT EXISTS idx_br_match
    ON base_rate_features(b_move, b_rsi, b_rsi_slope, b_vix, b_vix_move, b_trend);

CREATE TABLE IF NOT EXISTS base_rate_ingest_log (
    symbol    TEXT PRIMARY KEY,
    last_date TEXT,
    rows      INTEGER,
    updated   TEXT DEFAULT (datetime('now'))
);
"""


def migrate(db_path: str | Path) -> None:
    """Create tables and indexes if they don't exist. Safe to re-run."""
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(str(db_path)) as conn:
        conn.executescript(SCHEMA)
        conn.commit()
    print(f"[migrate] schema applied to {db_path}")


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument(
        "--db",
        default="signals.db",
        help="Path to signals.db (default: ./signals.db)",
    )
    args = p.parse_args()
    migrate(args.db)


if __name__ == "__main__":
    main()
