"""tests/test_agent_ratings_options_inclusion.py — HM-RATING-OPTIONS-BLIND-2026-08-29.

engine.agent_ratings.calculate_rating() used to exclude options trades
entirely (a deliberate pre-Season-5 mispricing guard) -- for a
predominantly options-trading fleet this made fleet_report_card() show
0 trades / N/A rating for nearly the whole active roster, which in turn
starved engine.ollietrades_signal.get_winning_models() (reuses this exact
function) of any winning models to gate on. Fixed (Admiral-directed) by
merging in CLOSED options_trades rows, but only from the real-quotes era
(exit_date >= TROI_REAL_QUOTES_ERA_START, 2026-07-07) -- the same
boundary the options-fill-integrity fix already trusts elsewhere, so the
original guard's rationale (don't grade agents on known-mispriced data)
still holds for anything older.
"""
from __future__ import annotations

import sqlite3
import sys
import tempfile
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import engine.agent_ratings as ratings  # noqa: E402


def _make_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""
        CREATE TABLE trades (
            id INTEGER PRIMARY KEY, player_id TEXT, action TEXT,
            realized_pnl REAL, confidence REAL, entry_price REAL,
            exit_price REAL, executed_at TEXT, symbol TEXT,
            asset_type TEXT, season INTEGER
        )
    """)
    conn.execute("""
        CREATE TABLE options_trades (
            id INTEGER PRIMARY KEY, agent_id TEXT, structure TEXT,
            symbol TEXT, status TEXT, pnl REAL, exit_date TEXT
        )
    """)
    conn.commit()
    conn.close()


def _insert_stock_trade(path, player_id, pnl, season, executed_at="2026-08-01 12:00:00"):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO trades (player_id, action, realized_pnl, confidence, "
        "executed_at, symbol, asset_type, season) VALUES (?, 'SELL', ?, 0.8, ?, 'AAPL', 'stock', ?)",
        (player_id, pnl, executed_at, season),
    )
    conn.commit()
    conn.close()


def _insert_option_trade(path, agent_id, pnl, exit_date, status="closed"):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, symbol, status, pnl, exit_date) "
        "VALUES (?, 'csp', 'SPY_260101C500', ?, ?, ?)",
        (agent_id, status, pnl, exit_date),
    )
    conn.commit()
    conn.close()


def test_real_quotes_era_options_are_included():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert_stock_trade(path, "troi", 50.0, season=ratings._CURRENT_SEASON)
        _insert_option_trade(path, "troi", 100.0, "2026-07-08 10:00:00")  # after era start
        _insert_option_trade(path, "troi", -30.0, "2026-08-01 10:00:00")

        with patch.object(ratings, "DB_PATH", path):
            result = ratings.calculate_rating("troi", "alltime")

        assert result["total_trades"] == 3  # 1 stock + 2 real-quotes options
        assert result["total_pnl"] == 50.0 + 100.0 - 30.0


def test_pre_era_options_are_excluded():
    """Options priced before TROI_REAL_QUOTES_ERA_START (2026-07-07) must
    stay excluded -- the original mispricing guard's rationale still holds
    for old data, only the boundary moved from 'never' to 'era-gated'."""
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert_stock_trade(path, "troi", 50.0, season=ratings._CURRENT_SEASON)
        _insert_stock_trade(path, "troi", 25.0, season=ratings._CURRENT_SEASON)
        _insert_option_trade(path, "troi", 9999.0, "2026-06-01 10:00:00")  # before era start

        with patch.object(ratings, "DB_PATH", path):
            result = ratings.calculate_rating("troi", "alltime")

        assert result["total_trades"] == 2  # only the two stock trades
        assert result["total_pnl"] == 75.0


def test_open_options_positions_are_excluded():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert_stock_trade(path, "troi", 50.0, season=ratings._CURRENT_SEASON)
        _insert_stock_trade(path, "troi", 25.0, season=ratings._CURRENT_SEASON)
        _insert_option_trade(path, "troi", 500.0, "2026-08-01 10:00:00", status="open")

        with patch.object(ratings, "DB_PATH", path):
            result = ratings.calculate_rating("troi", "alltime")

        assert result["total_trades"] == 2


def test_sanity_cap_still_applies_to_options():
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert_stock_trade(path, "troi", 50.0, season=ratings._CURRENT_SEASON)
        _insert_stock_trade(path, "troi", 25.0, season=ratings._CURRENT_SEASON)
        _insert_option_trade(path, "troi", 10000.0, "2026-08-01 10:00:00")  # > $3,500 cap

        with patch.object(ratings, "DB_PATH", path):
            result = ratings.calculate_rating("troi", "alltime")

        assert result["total_trades"] == 2  # the outlier option trade is excluded


def test_agent_with_no_options_activity_unaffected():
    """Pure stock-trading agent's rating must be identical to before this fix."""
    with tempfile.TemporaryDirectory() as d:
        path = str(Path(d) / "t.db")
        _make_db(path)
        _insert_stock_trade(path, "mccoy", 50.0, season=ratings._CURRENT_SEASON)
        _insert_stock_trade(path, "mccoy", -20.0, season=ratings._CURRENT_SEASON)

        with patch.object(ratings, "DB_PATH", path):
            result = ratings.calculate_rating("mccoy", "alltime")

        assert result["total_trades"] == 2
        assert result["total_pnl"] == 30.0
