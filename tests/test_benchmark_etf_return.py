"""HM-S6-PERF-FINDINGS-2026-07-10 finding 3.1 — engine.benchmark._get_etf_return
previously used yfinance directly with a bare `except: return None`. When
yfinance failed, every benchmark nulled out silently AND the >5%-
underperformance alert (gated on spy_ret is not None) could never fire --
the alarm shared a failure mode with what it watches. Fixed to use the
Polygon-backed get_intraday_candles cascade and fire a real ops alert
(not swallow) on failure.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.benchmark import _get_etf_return  # noqa: E402


def _candle(when: datetime, close: float):
    return {"time": when.strftime("%Y-%m-%dT%H:%M:%S") + "Z",
            "open": close, "high": close, "low": close, "close": close, "volume": 1000}


def test_computes_total_return_from_first_to_last_close():
    now = datetime.now(timezone.utc)
    candles = [
        _candle(now - timedelta(days=29), 400.0),
        _candle(now - timedelta(days=15), 410.0),
        _candle(now, 440.0),
    ]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        result = _get_etf_return("SPY", 30)
    assert result == pytest.approx(10.0)  # (440-400)/400 * 100


def test_picks_start_candle_closest_to_requested_window_not_earliest_available():
    """range_ mapping pads wider than the requested window (e.g. "1mo" for a
    30-day ask actually spans ~60 days) -- the start price must be the first
    bar AT/AFTER the requested window start, not whatever the widest
    available candle happens to be."""
    now = datetime.now(timezone.utc)
    candles = [
        _candle(now - timedelta(days=58), 300.0),   # outside the 30d window -- must be ignored
        _candle(now - timedelta(days=29), 400.0),    # first bar inside the window -- this is start
        _candle(now, 440.0),
    ]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        result = _get_etf_return("SPY", 30)
    assert result == pytest.approx(10.0)  # uses 400 as start, not 300


def test_returns_none_and_fires_ops_alert_on_fetch_failure():
    """The literal bug: a fetch failure must not be silently swallowed --
    it must log loudly AND fire a real (ops-classified) alert so the
    >5%-underperformance alarm's blindness is itself observable."""
    with patch("engine.market_data.get_intraday_candles", side_effect=RuntimeError("Polygon HTTP 429")), \
         patch("engine.alert_channels.send_alert") as mock_alert:
        result = _get_etf_return("SPY", 30)
    assert result is None
    mock_alert.assert_called_once()
    call_kwargs = mock_alert.call_args.kwargs
    assert call_kwargs["alert_type"] == "sentinel_benchmark_etf_fetch"  # "sentinel_" prefix -> classifies as ops, not signal


def test_returns_none_when_no_candles_and_still_alerts():
    with patch("engine.market_data.get_intraday_candles", return_value=[]), \
         patch("engine.alert_channels.send_alert") as mock_alert:
        result = _get_etf_return("SPY", 30)
    assert result is None
    mock_alert.assert_called_once()


def test_ops_alert_failure_itself_does_not_crash_the_caller():
    """send_alert raising (e.g. notifications table unavailable) must not
    propagate -- a broken alert channel shouldn't also break the benchmark
    computation it's trying to report on."""
    with patch("engine.market_data.get_intraday_candles", side_effect=RuntimeError("boom")), \
         patch("engine.alert_channels.send_alert", side_effect=RuntimeError("ntfy down")):
        result = _get_etf_return("SPY", 30)  # must not raise
    assert result is None


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
