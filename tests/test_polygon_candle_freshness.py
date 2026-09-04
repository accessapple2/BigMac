"""HM-BUG-BATCH-2026-07-10 — get_intraday_candles Polygon path silently
returned stale data.

Found live: get_session_vwap('SPY') returned None during live market hours
even with a valid POLYGON_API_KEY and an HTTP 200 response. Root cause:
the Polygon aggs request had no `sort` param, and that endpoint's default
sort is ASCENDING (oldest first). A 2-day-padded 1-minute-bar window for a
liquid symbol like SPY produces well over 500 bars, so `&limit=500` with no
sort silently truncated to the OLDEST 500 -- the response looked healthy
(HTTP 200, real OHLCV data) but its last bar was over 38 hours stale,
never reaching "today." This is a serialization-looks-fine, data-is-wrong
bug -- the kind that doesn't show up as an error anywhere.

Fixed by adding `sort=desc` (get the newest 500) and reversing the result
before returning (every caller -- VWAP's forward walk, chart rendering,
the outcome-resolution walk-forward engine -- assumes ascending order).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from engine.market_data import get_intraday_candles  # noqa: E402


def _polygon_bar(dt: datetime, price: float):
    return {"t": int(dt.timestamp() * 1000), "o": price, "h": price + 1,
            "l": price - 1, "c": price, "v": 1000}


def test_polygon_request_specifies_sort_desc(monkeypatch):
    """The literal fix: without sort=desc, Polygon's default ascending order
    + limit=500 silently truncates to the oldest bars in the window."""
    captured_url = {}

    def _fake_get(url, timeout=None):
        captured_url["url"] = url
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": [_polygon_bar(datetime.now(timezone.utc), 100.0)]}
        return resp

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")

    assert "sort=desc" in captured_url["url"]


def test_returns_newest_bars_in_ascending_order_when_over_limit(monkeypatch):
    """Simulates the exact failure mode: Polygon has far more than 500 bars
    in the requested window. With sort=desc, the API would give us the
    newest 500 (here simplified to a handful) -- the function must return
    them in chronological (ascending) order, ending with the most recent."""
    now = datetime.now(timezone.utc)
    # Polygon (mocked) returns newest-first, as sort=desc would actually give.
    newest_first = [
        _polygon_bar(now, 103.0),
        _polygon_bar(now - timedelta(minutes=1), 102.0),
        _polygon_bar(now - timedelta(minutes=2), 101.0),
    ]

    def _fake_get(url, timeout=None):
        assert "sort=desc" in url
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": newest_first}
        return resp

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    with patch("requests.get", side_effect=_fake_get):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")

    assert len(candles) == 3
    # Ascending: oldest first, newest (most recent, closest to "now") last.
    assert candles[0]["close"] == 101.0
    assert candles[-1]["close"] == 103.0
    times = [c["time"] for c in candles]
    assert times == sorted(times)


# ── HM-POLYGON-FRESHNESS-2026-09-04 ─────────────────────────────────────────
# A different bug from the sort=desc one above: even with newest-first
# sorting fixed, Massive/Polygon's free "Stocks Basic" tier is End-of-Day
# data (confirmed against their own pricing page and by live testing) --
# a call during market hours can return HTTP 200 with a real, well-formed,
# properly-sorted payload that is simply yesterday's (or older) session.
# Live-demonstrated: engine/volatility_breakout.py's detect_breakout()
# consumed a 22.3h-stale Polygon response for MSFT and emitted a "live"
# BEARISH breakout signal built entirely from an already-closed session,
# which build_breakout_prompt_section() injects verbatim into every AI
# trading agent's prompt. Fix: reject any Polygon response whose newest
# bar exceeds _POLYGON_STALENESS_THRESHOLD_HOURS and fall through to
# Alpaca instead of trusting it.

import engine.market_data as market_data


@pytest.fixture(autouse=True)
def _reset_polygon_cooldown_state():
    """Staleness rejection must NOT set the 429 cooldown (see the
    dedicated test below) -- reset between tests regardless so no test
    leaks cooldown state into the next one."""
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False
    yield
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False


def _no_alpaca_creds(monkeypatch):
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)


def test_stale_polygon_result_is_rejected(monkeypatch):
    """A well-formed, properly-sorted Polygon response whose newest bar is
    older than the threshold must not be returned as-is."""
    stale_time = datetime.now(timezone.utc) - timedelta(hours=22.3)
    stale_bars = [_polygon_bar(stale_time, 100.0)]

    def _fake_get(url, timeout=None):
        assert "sort=desc" in url
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": stale_bars}
        return resp

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    with patch("requests.get", side_effect=_fake_get), \
         patch.object(market_data, "_yahoo_chart", return_value=None):
        candles = get_intraday_candles("MSFT", interval="5m", range_="1d")

    # Alpaca creds removed and Yahoo blocked -- if the stale Polygon data
    # had been accepted, it would be the only possible non-empty result.
    # An empty result proves it was rejected, not silently returned.
    assert candles == []


def test_fresh_polygon_result_is_used_normally(monkeypatch):
    """Regression: a response within the freshness window must be
    completely unaffected by this check."""
    fresh_time = datetime.now(timezone.utc) - timedelta(minutes=5)
    fresh_bars = [_polygon_bar(fresh_time, 100.0)]

    def _fake_get(url, timeout=None):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": fresh_bars}
        return resp

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    with patch("requests.get", side_effect=_fake_get):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")

    assert len(candles) == 1
    assert candles[0]["close"] == 100.0


def test_staleness_boundary(monkeypatch):
    """Just under the threshold passes; just over it is rejected -- proves
    the check compares against _POLYGON_STALENESS_THRESHOLD_HOURS itself,
    not some other hardcoded value."""
    threshold = market_data._POLYGON_STALENESS_THRESHOLD_HOURS

    def _bars_aged(hours_old):
        t = datetime.now(timezone.utc) - timedelta(hours=hours_old)
        return [_polygon_bar(t, 100.0)]

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)

    with patch("requests.get", side_effect=lambda url, timeout=None: Mock(
            status_code=200, json=lambda: {"results": _bars_aged(threshold - 0.05)})):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")
    assert len(candles) == 1  # just under threshold -- accepted

    with patch("requests.get", side_effect=lambda url, timeout=None: Mock(
            status_code=200, json=lambda: {"results": _bars_aged(threshold + 0.05)})), \
         patch.object(market_data, "_yahoo_chart", return_value=None):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")
    assert candles == []  # just over threshold -- rejected


def test_staleness_rejection_does_not_set_polygon_cooldown(monkeypatch):
    """Only a real 429 means 'slow down' -- stale-but-valid data is a
    different problem and must not trip the shared rate-limit cooldown
    that every OTHER caller/symbol would then also skip Polygon for."""
    stale_bars = [_polygon_bar(datetime.now(timezone.utc) - timedelta(hours=5), 100.0)]

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    with patch("requests.get", side_effect=lambda url, timeout=None: Mock(
            status_code=200, json=lambda: {"results": stale_bars})), \
         patch.object(market_data, "_yahoo_chart", return_value=None):
        get_intraday_candles("SPY", interval="1m", range_="1d")

    assert market_data._is_polygon_limited() is False


def test_stale_rejection_is_logged(monkeypatch):
    """The Admiral asked to be able to see how often Polygon is serving
    stale data -- verify the rejection actually logs, not just silently
    falls through."""
    stale_bars = [_polygon_bar(datetime.now(timezone.utc) - timedelta(hours=5), 100.0)]

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    with patch("requests.get", side_effect=lambda url, timeout=None: Mock(
            status_code=200, json=lambda: {"results": stale_bars})), \
         patch.object(market_data, "_yahoo_chart", return_value=None), \
         patch.object(market_data.console, "log") as mock_log:
        get_intraday_candles("SPY", interval="1m", range_="1d")

    logged = " ".join(str(c.args[0]) for c in mock_log.call_args_list)
    assert "stale" in logged.lower()


def test_daily_interval_routinely_trips_the_check_by_design(monkeypatch):
    """interval='1d' callers (e.g. gap_scanner's _get_daily_candles) will
    almost always trip this check, since a completed daily bar is
    timestamped at that day's start -- accepted side effect, not a bug:
    it just means daily requests fall through to Alpaca too."""
    # A daily bar for "today," timestamped at market open -- several
    # hours old by the time this runs, same as it would be in production.
    daily_bar_time = datetime.now(timezone.utc).replace(hour=13, minute=30, second=0, microsecond=0)
    if daily_bar_time > datetime.now(timezone.utc):
        daily_bar_time -= timedelta(days=1)
    bars = [_polygon_bar(daily_bar_time, 100.0)]

    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    with patch("requests.get", side_effect=lambda url, timeout=None: Mock(
            status_code=200, json=lambda: {"results": bars})), \
         patch.object(market_data, "_yahoo_chart", return_value=None):
        candles = get_intraday_candles("SPY", interval="1d", range_="2mo")

    assert candles == []


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
