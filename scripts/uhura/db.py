"""Shared DB setup for UHURA research spike."""
import sqlite3
from pathlib import Path

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "uhura_research.db"


def get_conn(timeout: int = 10) -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=timeout)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_db() -> None:
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS uhura_raw_news (
            id          TEXT PRIMARY KEY,
            ticker      TEXT NOT NULL,
            headline    TEXT NOT NULL,
            source      TEXT,
            created_at  TEXT NOT NULL,
            updated_at  TEXT,
            summary     TEXT,
            content     TEXT,
            ingest_at   TEXT NOT NULL DEFAULT (datetime('now'))
        );
        CREATE INDEX IF NOT EXISTS idx_raw_ticker ON uhura_raw_news(ticker);
        CREATE INDEX IF NOT EXISTS idx_raw_created ON uhura_raw_news(created_at);

        CREATE TABLE IF NOT EXISTS uhura_signals (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            news_id         TEXT NOT NULL REFERENCES uhura_raw_news(id),
            ticker          TEXT NOT NULL,
            published_at    TEXT NOT NULL,
            headline_hash   TEXT NOT NULL,
            sentiment       TEXT CHECK(sentiment IN ('BULLISH','BEARISH','NEUTRAL')),
            confidence      REAL,
            event_type      TEXT CHECK(event_type IN ('earnings','merger','guidance','macro','other')),
            urgency         INTEGER CHECK(urgency BETWEEN 1 AND 5),
            parse_model     TEXT,
            gate_pass       INTEGER DEFAULT 0,
            gate_reason     TEXT,
            parse_at        TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(news_id, ticker)
        );
        CREATE INDEX IF NOT EXISTS idx_sig_ticker ON uhura_signals(ticker);
        CREATE INDEX IF NOT EXISTS idx_sig_published ON uhura_signals(published_at);
        CREATE INDEX IF NOT EXISTS idx_sig_gate ON uhura_signals(gate_pass);

        CREATE TABLE IF NOT EXISTS uhura_backtest_results (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            signal_id           INTEGER NOT NULL REFERENCES uhura_signals(id),
            ticker              TEXT NOT NULL,
            event_type          TEXT,
            sentiment           TEXT,
            urgency             INTEGER,
            confidence          REAL,
            bar_open            REAL,
            t5_ret              REAL,
            t15_ret             REAL,
            t30_ret             REAL,
            t60_ret             REAL,
            t240_ret            REAL,
            spy_t5              REAL,
            spy_t15             REAL,
            spy_t30             REAL,
            spy_t60             REAL,
            spy_t240            REAL,
            move_threshold_hit  INTEGER DEFAULT 0,
            regime              TEXT,
            backtest_at         TEXT NOT NULL DEFAULT (datetime('now')),
            UNIQUE(signal_id)
        );
        CREATE INDEX IF NOT EXISTS idx_bt_event ON uhura_backtest_results(event_type);
        CREATE INDEX IF NOT EXISTS idx_bt_sentiment ON uhura_backtest_results(sentiment);
    """)
    conn.commit()
    conn.close()
    print(f"DB ready: {DB_PATH}")
