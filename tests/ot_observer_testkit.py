"""Builders shared by the ot_observer tests. No test_ prefix, so pytest does not collect it.

Schemas are the live trader.db / signals.db CREATE statements (read 2026-09-14), trimmed to
the columns the observer tools read, so the fixtures fail the same way the real tables would.
"""
from __future__ import annotations

import os
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ot_observer.config import ObserverPaths  # noqa: E402

TRADER_SCHEMA = (
    """CREATE TABLE decision_audit (
        id INTEGER PRIMARY KEY, event_type TEXT NOT NULL, player_id TEXT, symbol TEXT,
        signal_id INTEGER, trade_id INTEGER, regime TEXT, spy_change REAL, vix REAL,
        confidence REAL, gate_verdict TEXT, reasoning_snippet TEXT,
        created_at TEXT DEFAULT (datetime('now')), raw_confidence REAL, meta_confidence REAL,
        confidence_modifier REAL, prompt_text TEXT, prompt_truncation_flag TEXT,
        prompt_truncation_detail TEXT)""",
    """CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT NOT NULL, symbol TEXT NOT NULL, action TEXT NOT NULL,
        qty REAL, price REAL, asset_type TEXT DEFAULT 'stock', option_type TEXT, strike_price REAL,
        expiry_date TEXT, reasoning TEXT, confidence REAL,
        executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, exit_price REAL, realized_pnl REAL,
        entry_price REAL, season INTEGER DEFAULT 1, alpaca_order_id TEXT, alpaca_status TEXT,
        execution_type TEXT DEFAULT 'simulated', signal_id INTEGER DEFAULT NULL,
        known_contaminated INTEGER DEFAULT 0, pnl_basis_invalid INTEGER DEFAULT 0,
        tz_bucket_suspect INTEGER DEFAULT 0)""",
    """CREATE TABLE positions (
        id INTEGER PRIMARY KEY, player_id TEXT NOT NULL, symbol TEXT NOT NULL, qty REAL,
        avg_price REAL, asset_type TEXT DEFAULT 'stock', option_type TEXT, strike_price REAL,
        expiry_date TEXT, opened_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        UNIQUE(player_id, symbol, asset_type, option_type, strike_price, expiry_date))""",
    """CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, display_name TEXT NOT NULL, provider TEXT NOT NULL,
        model_id TEXT NOT NULL, is_active INTEGER DEFAULT 1, halt_reason TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, is_paused INTEGER DEFAULT 0,
        season INTEGER DEFAULT 1, crew_role TEXT DEFAULT 'active', role TEXT DEFAULT 'production',
        halt_mode TEXT DEFAULT 'active', halted_at TIMESTAMP)""",
    """CREATE TABLE fleet_lifecycle_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_type TEXT NOT NULL, target_name TEXT NOT NULL,
        action TEXT NOT NULL, reason TEXT NOT NULL, order_doc TEXT, resume_by TEXT, review_by TEXT,
        backfilled INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        created_by TEXT NOT NULL DEFAULT 'fleet_lifecycle.py')""",
    """CREATE TABLE notifications (
        id INTEGER PRIMARY KEY AUTOINCREMENT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP, type TEXT,
        severity TEXT, title TEXT, body TEXT, icon TEXT, agent_id TEXT, acknowledged INTEGER DEFAULT 0)""",
)

SIGNALS_SCHEMA = (
    """CREATE TABLE trade_signals (
        id INTEGER PRIMARY KEY AUTOINCREMENT, type TEXT NOT NULL DEFAULT 'SWING', symbol TEXT NOT NULL,
        action TEXT NOT NULL, entry_price REAL, stop_loss REAL, take_profit REAL, confidence INTEGER,
        agent_name TEXT, model_used TEXT, reasoning TEXT, timeframe TEXT DEFAULT 'SWING',
        status TEXT NOT NULL DEFAULT 'NEW', created_at TEXT DEFAULT CURRENT_TIMESTAMP,
        executed_at TEXT, dismissed_at TEXT, w3_gex_regime TEXT)""",
)

# Real trader.log lines (2026-09-14), including a wrapped [SCHED-JOB] start and done,
# a same-second record, and a plain print() line with no rich timestamp column.
RICH_TRADER_LOG = [
    "[2026-09-14 05:01:45] [SCHED-JOB] start name=_run_riker_xo_synthesis main.py:110",
    "[2026-09-14 05:01:45] [ENDPOINT-DUR] GET /api/bridge/consensus       app.py:1640",
    "                      wall=0.01s status=200                                     ",
    "                      [SCHED-JOB] start                              main.py:110",
    "                      name=_run_ollietrades_signal_cycle                        ",
    "                      [SCHED-JOB] done                               main.py:116",
    "                      name=_run_ollietrades_signal_cycle wall=0.002s            ",
    "Database ready with 14 AI players",
    "[2026-09-14 05:01:46] HM-CB Polygon candles stale for DPZ:    market_data.py:960",
    "                      HM-CA Alpaca candles fallback to Yahoo market_data.py:1095",
]


def utc_text(minutes_ago: float = 0.0) -> str:
    """A UTC timestamp in SQLite's datetime('now') format."""
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)).strftime("%Y-%m-%d %H:%M:%S")


def _create_db(path: Path, schema: tuple[str, ...]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    for statement in schema:
        conn.execute(statement)
    conn.commit()
    conn.close()


def build_root(tmp_path: Path, *, dbs: bool = True, logs: bool = True) -> ObserverPaths:
    paths = ObserverPaths.under(tmp_path)
    if logs:
        paths.log_dir.mkdir(parents=True, exist_ok=True)
    if dbs:
        _create_db(paths.trader_db, TRADER_SCHEMA)
        _create_db(paths.signals_db, SIGNALS_SCHEMA)
    return paths


def execute_many(db_path: Path, sql: str, rows: list[tuple]) -> None:
    conn = sqlite3.connect(db_path)
    conn.executemany(sql, rows)
    conn.commit()
    conn.close()


def seed_all(paths: ObserverPaths, n: int) -> None:
    """n rows into every table a row tool reads; row i is i minutes old."""
    r = range(n)
    execute_many(
        paths.trader_db,
        "INSERT INTO decision_audit (event_type, player_id, symbol, confidence, gate_verdict, "
        "reasoning_snippet, prompt_text, created_at) VALUES (?,?,?,?,?,?,?,?)",
        [("arena_decision", "ollama-plutus", "SPY", 0.7, "pass", f"reason {i}", f"PROMPT {i}", utc_text(i)) for i in r],
    )
    execute_many(
        paths.trader_db,
        "INSERT INTO trades (player_id, symbol, action, qty, price, executed_at) VALUES (?,?,?,?,?,?)",
        [("ollama-plutus", "SPY", "BUY", 1, 500.0 + i, utc_text(i)) for i in r],
    )
    execute_many(
        paths.trader_db,
        "INSERT INTO positions (player_id, symbol, qty, avg_price, opened_at) VALUES (?,?,?,?,?)",
        [("ollama-plutus", f"S{i}", 1, 10.0, utc_text(i)) for i in r],
    )
    execute_many(
        paths.trader_db,
        "INSERT INTO ai_players (id, display_name, provider, model_id, halt_mode, halted_at) VALUES (?,?,?,?,?,?)",
        [(f"seat-{i}", f"Seat {i}", "ollama", "qwen3:8b", "active", None) for i in r],
    )
    execute_many(
        paths.trader_db,
        "INSERT INTO fleet_lifecycle_ledger (target_type, target_name, action, reason, created_at) VALUES (?,?,?,?,?)",
        [("agent", f"seat-{i}", "bench", "test", utc_text(i)) for i in r],
    )
    execute_many(
        paths.trader_db,
        "INSERT INTO notifications (timestamp, type, severity, title, body) VALUES (?,?,?,?,?)",
        [(utc_text(i), "alert", "info", f"title {i}", "body") for i in r],
    )
    execute_many(
        paths.signals_db,
        "INSERT INTO trade_signals (symbol, action, confidence, agent_name, created_at) VALUES (?,?,?,?,?)",
        [("SPY", "BUY", 70, "scanner", utc_text(i)) for i in r],
    )


def write_log(paths: ObserverPaths, filename: str, lines: list[str], *, age_seconds: float | None = None) -> Path:
    path = paths.log_dir / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n")
    if age_seconds is not None:
        t = time.time() - age_seconds
        os.utime(path, (t, t))
    return path
