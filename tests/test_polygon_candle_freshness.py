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


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
