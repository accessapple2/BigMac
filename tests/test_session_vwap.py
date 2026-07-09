"""Tests for engine.market_data.get_session_vwap — Gamma Map VWAP gauge.

Bridge-v2's Gamma Map · SPY panel had a VWAP gauge permanently stuck at "—"
because /api/market/gex never computed it. get_session_vwap fills that gap
by reusing get_intraday_candles' existing Alpaca/Polygon/Yahoo bar cascade.
"""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from unittest.mock import patch

import pytest

from engine.market_data import _MARKET_TZ, get_session_vwap


def _iso_utc(dt: datetime) -> str:
    return dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S") + "Z"


def _candle(when: datetime, high: float, low: float, close: float, volume: float) -> dict:
    return {
        "time": _iso_utc(when),
        "open": close, "high": high, "low": low, "close": close, "volume": volume,
    }


def test_computes_volume_weighted_typical_price():
    now = datetime.now(_MARKET_TZ)
    candles = [
        _candle(now - timedelta(minutes=2), high=101, low=99, close=100, volume=1000),
        _candle(now - timedelta(minutes=1), high=103, low=101, close=102, volume=3000),
    ]
    # typical prices: 100, 102 — volume-weighted mean = (100*1000 + 102*3000) / 4000 = 101.5
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        assert get_session_vwap("SPY") == pytest.approx(101.5)


def test_no_candles_returns_none():
    with patch("engine.market_data.get_intraday_candles", return_value=[]):
        assert get_session_vwap("SPY") is None


def test_prior_day_bars_excluded_from_todays_vwap():
    now = datetime.now(_MARKET_TZ)
    stale = [_candle(now - timedelta(days=1, minutes=5), high=90, low=88, close=89, volume=5000)]
    with patch("engine.market_data.get_intraday_candles", return_value=stale):
        assert get_session_vwap("SPY") is None


def test_zero_volume_bars_excluded():
    now = datetime.now(_MARKET_TZ)
    candles = [_candle(now - timedelta(minutes=1), high=101, low=99, close=100, volume=0)]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        assert get_session_vwap("SPY") is None


def test_upstream_exception_returns_none_not_raise():
    with patch("engine.market_data.get_intraday_candles", side_effect=RuntimeError("boom")):
        assert get_session_vwap("SPY") is None


def test_malformed_timestamp_skipped_not_fatal():
    now = datetime.now(_MARKET_TZ)
    candles = [
        {"time": "not-a-timestamp", "open": 1, "high": 1, "low": 1, "close": 1, "volume": 100},
        _candle(now - timedelta(minutes=1), high=101, low=99, close=100, volume=500),
    ]
    with patch("engine.market_data.get_intraday_candles", return_value=candles):
        assert get_session_vwap("SPY") == pytest.approx(100.0)


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
