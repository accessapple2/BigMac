"""HM-BRIDGE-SEASON-PNL 2026-08-29 — fleet-level season + lifetime P&L rollups.

dashboard.app._fleet_season_rollup() and _fleet_lifetime_pnl() back the
new season/lifetime figures in /api/fleet/pnl, consumed by both dashboard
headers. Investigating this ticket found season_overlay (the field the
ticket originally named) mixes an unreset portfolio_history baseline with
an anchored current value, producing a bogus fleet-wide artifact -- these
tests pin the corrected computations instead: _fleet_season_rollup sums
each row's already-correctly-anchored total_pnl/starting_capital, and
_fleet_lifetime_pnl computes a genuine cross-season figure from realized
P&L (all seasons, no filter) + current unrealized, against each player's
ORIGINAL (first-season) starting capital.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _row(player_id, total_pnl, starting_capital, unrealized_pnl=0.0, halt_mode="active"):
    return {
        "player_id": player_id,
        "halt_mode": halt_mode,
        "total_pnl": total_pnl,
        "starting_capital": starting_capital,
        "unrealized_pnl": unrealized_pnl,
    }


# ─── _fleet_season_rollup ───────────────────────────────────────────────────

def test_season_rollup_basic_sum_and_return_pct():
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0),
        _row("dax", -200.0, 10000.0),
        _row("troi", 150.0, 10000.0),
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 450.0
    assert result["season_baseline"] == 30000.0
    assert result["season_return_pct"] == 1.5  # 450 / 30000 * 100


def test_season_rollup_agent_with_no_season_trades_contributes_zero():
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0),
        _row("fresh-agent", 0.0, 10000.0),  # no season trades yet -- flat
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 500.0
    assert result["season_baseline"] == 20000.0


def test_season_rollup_halted_agent_still_counts_per_existing_convention():
    """No halt_mode filtering in the rollup itself -- rows are whatever
    leaderboard() already selected (FLEET_ACTIVE roster, halted or not)."""
    import dashboard.app as app_module

    rows = [
        _row("mccoy", 500.0, 10000.0, halt_mode="active"),
        _row("halted-agent", -300.0, 10000.0, halt_mode="exit_only"),
    ]
    result = app_module._fleet_season_rollup(rows)
    assert result["season_pnl"] == 200.0
    assert result["season_baseline"] == 20000.0


def test_season_rollup_empty_rows_no_division_by_zero():
    import dashboard.app as app_module

    result = app_module._fleet_season_rollup([])
    assert result == {"season_pnl": 0.0, "season_baseline": 0.0, "season_return_pct": 0.0}


def test_season_rollup_rounding_to_two_decimals():
    import dashboard.app as app_module

    result = app_module._fleet_season_rollup([_row("mccoy", 100.005, 10000.0)])
    assert result["season_pnl"] == round(100.005, 2)


# ─── _fleet_lifetime_pnl ────────────────────────────────────────────────────

def _make_lifetime_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE trades (id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, "
        "realized_pnl REAL, season INTEGER, executed_at TEXT)"
    )
    conn.execute(
        "CREATE TABLE portfolio_history (id INTEGER PRIMARY KEY, player_id TEXT, "
        "total_value REAL, season INTEGER, recorded_at TEXT)"
    )
    conn.commit()
    conn.close()


def _insert_trade(path, player_id, realized_pnl, season, executed_at="2026-06-01 12:00:00"):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO trades (player_id, action, realized_pnl, season, executed_at) "
        "VALUES (?, 'SELL', ?, ?, ?)",
        (player_id, realized_pnl, season, executed_at),
    )
    conn.commit()
    conn.close()


def _insert_snapshot(path, player_id, season, recorded_at="2026-06-01 12:00:00"):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO portfolio_history (player_id, total_value, season, recorded_at) "
        "VALUES (?, 10000, ?, ?)",
        (player_id, season, recorded_at),
    )
    conn.commit()
    conn.close()


def _conn(path):
    c = sqlite3.connect(path)
    c.row_factory = sqlite3.Row
    return c


def test_lifetime_sums_realized_pnl_across_all_seasons_plus_unrealized():
    import dashboard.app as app_module

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_lifetime_db(path)
        _insert_snapshot(path, "mccoy", season=1)
        _insert_trade(path, "mccoy", 500.0, season=1)
        _insert_trade(path, "mccoy", -100.0, season=2)
        _insert_trade(path, "mccoy", 300.0, season=3)

        rows = [_row("mccoy", total_pnl=999, starting_capital=10000, unrealized_pnl=50.0)]
        conn = _conn(path)
        try:
            result = app_module._fleet_lifetime_pnl(conn, rows)
        finally:
            conn.close()
        # 500 - 100 + 300 (all three seasons) + 50 unrealized = 750
        assert result["lifetime_pnl"] == 750.0
        assert result["lifetime_starting_capital"] == 10000.0  # first_season=1 -> $10k baseline
        assert result["lifetime_return_pct"] == 7.5


def test_lifetime_uses_players_first_season_baseline_not_season_one():
    """An agent that joined later (first portfolio_history row in season 4)
    must use season 4's starting capital ($7,000), not season 1's ($10,000)
    -- they never actually had a season-1 stake."""
    import dashboard.app as app_module

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_lifetime_db(path)
        _insert_snapshot(path, "late-joiner", season=4)
        _insert_trade(path, "late-joiner", 200.0, season=4)

        rows = [_row("late-joiner", total_pnl=200, starting_capital=7000, unrealized_pnl=0.0)]
        conn = _conn(path)
        try:
            result = app_module._fleet_lifetime_pnl(conn, rows)
        finally:
            conn.close()
        assert result["lifetime_starting_capital"] == 7000.0
        assert result["lifetime_pnl"] == 200.0


def test_lifetime_agent_with_no_trades_or_snapshots_falls_back_cleanly():
    import dashboard.app as app_module

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_lifetime_db(path)

        rows = [_row("brand-new", total_pnl=0, starting_capital=10000, unrealized_pnl=0.0)]
        conn = _conn(path)
        try:
            result = app_module._fleet_lifetime_pnl(conn, rows)
        finally:
            conn.close()
        assert result["lifetime_pnl"] == 0.0
        assert result["lifetime_starting_capital"] == 10000.0  # default first_season=1


def test_lifetime_empty_rows_no_division_by_zero():
    import dashboard.app as app_module

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_lifetime_db(path)
        conn = _conn(path)
        try:
            result = app_module._fleet_lifetime_pnl(conn, [])
        finally:
            conn.close()
        assert result == {
            "lifetime_pnl": 0.0,
            "lifetime_starting_capital": 0.0,
            "lifetime_return_pct": 0.0,
        }


def test_lifetime_multi_agent_sums_independently():
    import dashboard.app as app_module

    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_lifetime_db(path)
        _insert_snapshot(path, "mccoy", season=1)
        _insert_trade(path, "mccoy", 500.0, season=1)
        _insert_snapshot(path, "dax", season=1)
        _insert_trade(path, "dax", -250.0, season=1)

        rows = [
            _row("mccoy", total_pnl=500, starting_capital=10000, unrealized_pnl=0.0),
            _row("dax", total_pnl=-250, starting_capital=10000, unrealized_pnl=0.0),
        ]
        conn = _conn(path)
        try:
            result = app_module._fleet_lifetime_pnl(conn, rows)
        finally:
            conn.close()
        assert result["lifetime_pnl"] == 250.0
        assert result["lifetime_starting_capital"] == 20000.0
