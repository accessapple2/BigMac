"""tests/test_season_config_autowrite.py — HM-SEASON-CONFIG-AUTOWRITE-2026-09-13.

rotate_season()/start_season() never wrote season_config or settings.season_N_name, so
Season 8 had no config row and /api/season returned config:{} (Season panel: "Active: 1
agents"). Rotation now writes both as NEW rows only; ensure_season_config() backfills an
existing season from settings.season_N_start. Temp-file DB (season_manager uses WAL).
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.season_manager as sm  # noqa: E402

SCHEMA = """
CREATE TABLE ai_players (id TEXT PRIMARY KEY, display_name TEXT, cash REAL DEFAULT 7000.0,
    is_active INTEGER DEFAULT 1, halt_reason TEXT, halted_at TIMESTAMP,
    halt_mode TEXT DEFAULT 'active', season INTEGER DEFAULT 1);
CREATE TABLE settings (key TEXT PRIMARY KEY, value TEXT);
CREATE TABLE season_history (id INTEGER PRIMARY KEY AUTOINCREMENT, season INTEGER NOT NULL,
    player_id TEXT NOT NULL, display_name TEXT, final_value REAL, total_return_pct REAL,
    total_trades INTEGER DEFAULT 0, win_rate REAL DEFAULT 0, ended_at TEXT);
CREATE TABLE positions (player_id TEXT, symbol TEXT, qty REAL, avg_price REAL, asset_type TEXT);
CREATE TABLE trades (id INTEGER PRIMARY KEY AUTOINCREMENT, player_id TEXT, symbol TEXT,
    executed_at TEXT, realized_pnl REAL, season INTEGER);
CREATE TABLE season_config (season INTEGER PRIMARY KEY, name TEXT NOT NULL, start_date TEXT NOT NULL,
    end_date TEXT, active_agents TEXT, strategies TEXT, alpha_gate REAL DEFAULT 0.3,
    triple_filter TEXT, proving_ground INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')));
"""


@pytest.fixture
def db(tmp_path, monkeypatch):
    p = tmp_path / "trader.db"
    c = sqlite3.connect(p)
    c.executescript(SCHEMA)
    for stmt in sm.POSITIONS_ARCHIVE_DDL:
        c.execute(stmt)
    c.executemany("INSERT INTO ai_players (id, halt_mode, halt_reason) VALUES (?,?,?)", [
        ("qwen3-8b-flash", "active", None),
        ("ollama-plutus", "active", None),
        ("ollama-qwen3", "full", "[2026-08-31] HALT: parked"),
        ("guardian", "exit_only", "exit-only stop guardian"),
    ])
    c.execute("INSERT INTO settings VALUES ('current_season', '7')")
    c.commit()
    c.close()
    monkeypatch.setattr(sm, "DB", str(p))
    monkeypatch.setattr(sm, "_broker_open_symbols", lambda conn: (set(), "test-stub"))
    return p


def _q(p, sql, *args):
    c = sqlite3.connect(p)
    c.row_factory = sqlite3.Row
    try:
        return c.execute(sql, args).fetchall()
    finally:
        c.close()


def test_rotation_writes_config_row_and_name(db):
    with patch("engine.war_room.save_hot_take", return_value=True):
        assert sm.rotate_season(caller="test", apply=True) == 8
    row = _q(db, "SELECT * FROM season_config WHERE season=8")[0]
    start = _q(db, "SELECT value FROM settings WHERE key='season_8_start'")[0][0]
    assert row["name"] == "Season 8"
    assert row["start_date"] == start[:10]
    assert row["active_agents"].split(",") == ["ollama-plutus", "qwen3-8b-flash"]
    assert _q(db, "SELECT value FROM settings WHERE key='season_8_name'")[0][0] == "Season 8"


def test_start_season_writes_config_row(db):
    with patch("engine.war_room.save_hot_take", return_value=True):
        sm.start_season(9, apply=True)
    assert _q(db, "SELECT name FROM season_config WHERE season=9")[0][0] == "Season 9"


def test_existing_row_and_name_are_never_rewritten(db):
    c = sqlite3.connect(db)
    c.execute("INSERT INTO season_config (season, name, start_date, active_agents) "
              "VALUES (8, 'Hand Named', '2026-09-01', 'x')")
    c.execute("INSERT INTO settings VALUES ('season_8_name', 'Hand Named')")
    c.commit()
    c.close()
    with patch("engine.war_room.save_hot_take", return_value=True):
        sm.rotate_season(caller="test", apply=True)
    rows = _q(db, "SELECT name, start_date, active_agents FROM season_config WHERE season=8")
    assert [tuple(r) for r in rows] == [("Hand Named", "2026-09-01", "x")]
    assert _q(db, "SELECT value FROM settings WHERE key='season_8_name'")[0][0] == "Hand Named"


def test_ensure_season_config_backfills_idempotently(db):
    c = sqlite3.connect(db)
    c.executemany("INSERT OR REPLACE INTO settings VALUES (?, ?)",
                  [("current_season", "8"), ("season_8_start", "2026-09-11T06:13:15.686930")])
    c.commit()
    c.close()
    first = sm.ensure_season_config()
    second = sm.ensure_season_config()
    assert first["start_date"] == "2026-09-11"
    assert first["active_agents"] == "ollama-plutus,qwen3-8b-flash"
    assert first == second
    assert len(_q(db, "SELECT * FROM season_config")) == 1


def test_ensure_season_config_refuses_without_start_date(db):
    with pytest.raises(ValueError):
        sm.ensure_season_config(8)
    assert _q(db, "SELECT * FROM season_config") == []


def test_rotation_still_completes_if_season_config_table_is_missing(db):
    c = sqlite3.connect(db)
    c.execute("DROP TABLE season_config")
    c.commit()
    c.close()
    with patch("engine.war_room.save_hot_take", return_value=True):
        assert sm.rotate_season(caller="test", apply=True) == 8
    assert _q(db, "SELECT value FROM settings WHERE key='current_season'")[0][0] == "8"
