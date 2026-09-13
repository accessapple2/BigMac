"""tests/test_season_autorotate_gate.py — HM-SEASON-AUTOROTATE-GATE-2026-09-13.

The unattended Sunday season-rotation job must not be able to start a season on its own:
gated behind SEASON_AUTOROTATE_ENABLED (default off, absent = off). Fixture DB only; the
real rotate_season() is exercised only against a temp DB; alerts stay behind
alert_channels._under_pytest().
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import config  # noqa: E402
import engine.season_autorotate as sa  # noqa: E402
import engine.season_manager as sm  # noqa: E402

SUN_2358 = datetime(2026, 9, 13, 23, 58, 35)  # tonight's expected poll
SUN_1000 = datetime(2026, 9, 13, 10, 0, 0)

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


class _Log:
    def __init__(self):
        self.lines: list[str] = []

    def log(self, msg, *a, **k):
        self.lines.append(str(msg))


@pytest.fixture(autouse=True)
def _fresh(monkeypatch):
    monkeypatch.setattr(sa, "_skip_logged_keys", set())
    monkeypatch.setattr(sa, "_fired_sundays", set())
    log = _Log()
    monkeypatch.setattr(sa, "console", log)
    return log


@pytest.fixture
def fixture_db(tmp_path, monkeypatch):
    p = tmp_path / "trader.db"
    c = sqlite3.connect(p)
    c.executescript(SCHEMA)
    for stmt in sm.POSITIONS_ARCHIVE_DDL:
        c.execute(stmt)
    rows = [("clean-active", "active", None)] + [(f"zombie-{i}", "full", None) for i in range(30)]
    c.executemany("INSERT INTO ai_players (id, halt_mode, halt_reason) VALUES (?,?,?)", rows)
    c.executemany("INSERT INTO settings VALUES (?, ?)",
                  [("current_season", "8"), ("season_8_start", "2026-09-11T06:13:15.686930")])
    c.commit()
    c.close()
    monkeypatch.setattr(sm, "DB", str(p))
    monkeypatch.setattr(sm, "_broker_open_symbols", lambda conn: (set(), "test-stub"))
    return p


def _q(p, sql):
    c = sqlite3.connect(p)
    try:
        return c.execute(sql).fetchall()
    finally:
        c.close()


def test_flag_absent_resolves_false_fail_closed(monkeypatch):
    monkeypatch.delenv("SEASON_AUTOROTATE_ENABLED", raising=False)
    assert config._env_flag("SEASON_AUTOROTATE_ENABLED") is False
    for v in ("false", "0", "no", "off", "", "  ", "maybe"):
        monkeypatch.setenv("SEASON_AUTOROTATE_ENABLED", v)
        assert config._env_flag("SEASON_AUTOROTATE_ENABLED") is False, v
    for v in ("true", "1", "yes", "on", "TRUE"):
        monkeypatch.setenv("SEASON_AUTOROTATE_ENABLED", v)
        assert config._env_flag("SEASON_AUTOROTATE_ENABLED") is True, v


def test_flag_off_in_window_never_calls_rotate(monkeypatch, _fresh):
    monkeypatch.setattr(sa, "autorotate_enabled", lambda: False)
    rotate = MagicMock(return_value=9)
    monkeypatch.setattr(sm, "rotate_season", rotate)
    untouchable = MagicMock(side_effect=AssertionError("nothing past the flag may run when disabled"))
    monkeypatch.setattr(sa, "_already_rotated_today", untouchable)
    monkeypatch.setattr(sm, "_dry_run_unhalt_scope", untouchable)

    for now in (SUN_2358, SUN_2358, datetime(2026, 9, 13, 23, 55), datetime(2026, 9, 13, 23, 59, 59)):
        assert sa.run_scheduled_rotation(now) == "disabled"
        assert sa.run_scheduled_rotation(now, rotate=rotate) == "disabled"
    rotate.assert_not_called()
    untouchable.assert_not_called()
    assert _fresh.lines.count(f"[yellow]{sa.SKIP_LINE}") == 1  # once for tonight's window, not per poll


def test_flag_off_logs_one_line_at_first_poll(monkeypatch, _fresh):
    monkeypatch.setattr(sa, "autorotate_enabled", lambda: False)
    for _ in range(5):
        assert sa.run_scheduled_rotation(SUN_1000) == "disabled"
    assert _fresh.lines == [f"[yellow]{sa.SKIP_LINE}"]


def test_flag_on_in_window_still_honours_margin_guard(fixture_db, monkeypatch):
    from engine.alert_channels import _under_pytest
    assert _under_pytest(), "alerts must stay behind the pytest guard"
    monkeypatch.setattr(sa, "autorotate_enabled", lambda: True)
    before = _q(fixture_db, "SELECT id, halt_mode, cash, season FROM ai_players ORDER BY id")
    rotate = MagicMock(wraps=sm.rotate_season)

    with patch("engine.war_room.save_hot_take", return_value=True):
        assert sa.run_scheduled_rotation(SUN_2358, rotate=rotate) == "aborted"
        assert sa.run_scheduled_rotation(datetime(2026, 9, 13, 23, 59), rotate=rotate) == "already-fired"
    assert rotate.call_count == 1 and rotate.call_args.kwargs == {"caller": "cron-sunday", "apply": True}
    assert _q(fixture_db, "SELECT value FROM settings WHERE key='current_season'") == [("8",)]
    assert _q(fixture_db, "SELECT id, halt_mode, cash, season FROM ai_players ORDER BY id") == before
    assert _q(fixture_db, "SELECT COUNT(*) FROM season_config") == [(0,)]


def test_flag_on_outside_window_does_nothing(monkeypatch):
    monkeypatch.setattr(sa, "autorotate_enabled", lambda: True)
    rotate = MagicMock()
    for now in (SUN_1000, datetime(2026, 9, 13, 23, 49, 59), datetime(2026, 9, 12, 23, 58)):
        assert sa.run_scheduled_rotation(now, rotate=rotate) == "outside-window"
    rotate.assert_not_called()


def test_restart_inside_window_does_not_rotate_twice(fixture_db, monkeypatch):
    c = sqlite3.connect(fixture_db)
    c.executemany("INSERT OR REPLACE INTO settings VALUES (?, ?)",
                  [("current_season", "9"), ("season_9_start", "2026-09-13T23:51:00")])
    c.commit()
    c.close()
    monkeypatch.setattr(sa, "autorotate_enabled", lambda: True)
    rotate = MagicMock()
    assert sa.run_scheduled_rotation(SUN_2358, rotate=rotate) == "already-fired"
    rotate.assert_not_called()


def test_main_job_is_the_gated_5min_wrapper():
    src = (ROOT / "main.py").read_text()
    start = src.index("    def run_season_rotation():")
    block = src[start:src.index("schedule.every(", start) + 120]
    assert "run_scheduled_rotation(az_now())" in block
    assert "rotate_season" not in block.split("schedule.every(")[0]
    assert "schedule.every(5).minutes.do(run_season_rotation)" in src
    assert "schedule.every(30).minutes.do(run_season_rotation)" not in src
