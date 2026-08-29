"""tests/test_holodeck_drawdown_sign.py — HM-BACKTEST-ARENA-DD-SIGN-2026-08-29.

engine.holodeck (RSI / MA-cross strategies, vectorbt-powered) used to persist
max_drawdown as vectorbt's own POSITIVE magnitude convention, while
dashboard.app's buy_hold/momentum strategies compute it as negative
(peak-to-trough %). The shared Backtest Arena leaderboard showed both
signs side by side for the same metric. _dd_stat() normalizes vectorbt's
value to negative so every strategy path agrees.

IMPORTANT: engine/holodeck.py imports vectorbt at module level, which is
only installed in .venv-backtest (never the main .venv -- see CLAUDE.md
doctrine: never import vectorbt in serving code). Run this file with:

    .venv-backtest/bin/python3 -m pytest tests/test_holodeck_drawdown_sign.py -v

It will fail to collect under the main .venv/bin/python3 (ModuleNotFoundError:
vectorbt) -- that's expected, not a regression; it's excluded from the
standard `.venv/bin/python3 -m pytest tests/` run for the same reason
engine/holodeck.py itself is never imported in-process by dashboard/app.py.
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from engine.holodeck import _dd_stat, _stat  # noqa: E402


def test_positive_vectorbt_drawdown_normalized_to_negative():
    """vectorbt reports 'Max Drawdown [%]' as a positive magnitude."""
    stats = {"Max Drawdown [%]": 23.5}
    assert _dd_stat(stats) == -23.5


def test_zero_drawdown_stays_zero():
    stats = {"Max Drawdown [%]": 0.0}
    assert _dd_stat(stats) == 0.0


def test_already_negative_drawdown_stays_negative():
    """Defensive: if a future vectorbt version ever reports it negative,
    don't double-negate into a positive number."""
    stats = {"Max Drawdown [%]": -23.5}
    assert _dd_stat(stats) == -23.5


def test_missing_key_returns_zero():
    stats = {}
    assert _dd_stat(stats) == 0.0


def test_rounding_precision():
    stats = {"Max Drawdown [%]": 12.34567}
    assert _dd_stat(stats, decimals=2) == -12.35


def test_dd_stat_does_not_affect_other_stat_extraction():
    """_stat() itself (used for total_return, win_rate, sharpe, etc.) must
    be untouched -- the sign fix is scoped to drawdown only."""
    stats = {"Total Return [%]": 15.2, "Max Drawdown [%]": 8.0}
    assert _stat(stats, "Total Return [%]") == 15.2
    assert _dd_stat(stats) == -8.0
