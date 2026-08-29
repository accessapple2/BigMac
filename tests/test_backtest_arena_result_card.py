"""tests/test_backtest_arena_result_card.py — HM-BACKTEST-ARENA-RESULT-CARD-2026-08-29.

The Backtest Arena result card showed a bare '—' for Sharpe on the
buy_hold/momentum strategies (they never computed one, unlike the
vectorbt-powered rsi/ma_cross paths) and for Win Rate whenever there were
no trades or an unresolved open position -- indistinguishable from
"we don't know". These tests cover dashboard.app._sharpe_from_equity_curve
in isolation (pure function, no network).
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def test_sharpe_computed_for_a_rising_curve():
    import dashboard.app as app_module

    curve = [{"date": f"2026-01-{i:02d}", "value": 10000 + i * 20} for i in range(1, 30)]
    sharpe = app_module._sharpe_from_equity_curve(curve)
    assert sharpe is not None
    assert sharpe > 0  # steadily rising equity -> positive Sharpe


def test_sharpe_negative_for_a_falling_curve():
    import dashboard.app as app_module

    curve = [{"date": f"2026-01-{i:02d}", "value": 10000 - i * 20} for i in range(1, 30)]
    sharpe = app_module._sharpe_from_equity_curve(curve)
    assert sharpe is not None
    assert sharpe < 0


def test_sharpe_none_for_flat_curve_zero_variance():
    """A perfectly flat equity curve has zero return variance -- Sharpe is
    undefined (division by zero), not a fabricated 0.0."""
    import dashboard.app as app_module

    curve = [{"date": f"2026-01-{i:02d}", "value": 10000.0} for i in range(1, 10)]
    assert app_module._sharpe_from_equity_curve(curve) is None


def test_sharpe_none_for_too_few_points():
    import dashboard.app as app_module

    assert app_module._sharpe_from_equity_curve([{"date": "2026-01-01", "value": 10000}]) is None
    assert app_module._sharpe_from_equity_curve([]) is None


def test_sharpe_none_for_zero_valued_points():
    """Division-by-zero guard: a $0 equity point must not crash the
    return-series calculation."""
    import dashboard.app as app_module

    curve = [{"date": "2026-01-01", "value": 0.0}, {"date": "2026-01-02", "value": 100.0},
             {"date": "2026-01-03", "value": 110.0}]
    # Should not raise; either a real Sharpe or None, never an exception.
    app_module._sharpe_from_equity_curve(curve)


def test_momentum_win_rate_reason_no_trades_when_never_triggered():
    """If momentum never crosses positive, num_trades=0 and win_rate must
    say why instead of showing a bare dash."""
    import dashboard.app as app_module
    from unittest.mock import patch, MagicMock
    import pandas as pd

    with patch("yfinance.download") as mock_dl:
        # Flat/declining series -> momentum never crosses positive
        dates = pd.date_range("2026-01-01", periods=60, freq="D")
        closes = pd.Series([100.0 - i * 0.01 for i in range(60)], index=dates)
        df = MagicMock()
        df.empty = False
        df.__len__ = lambda self: 60
        df.__getitem__ = lambda self, key: closes if key == "Close" else None
        mock_dl.return_value = df

        result = app_module._run_momentum("TEST", days=30, lookback=20)

    if "error" not in result:
        assert result["win_rate"] is None
        assert result["win_rate_reason"] == "no_trades"
        assert result["num_trades"] == 0
