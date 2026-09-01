"""tests/test_polygon_429_backoff.py — HM-429-BACKOFF-2026-09-01.

get_intraday_candles (engine/market_data.py, the HM-CB/HM-CA/Yahoo
cascade) has 15+ uncoordinated callers, each independently walking its
own ticker universe. Before this fix, every single call retried Polygon
fresh even while it was already 429'ing, then fell through to Alpaca one
symbol at a time -- 37,174 Polygon 429s + 777 Alpaca 429s in one day
(2026-09-01), traced in relay_2026-09-01_four-item-followup.md section 9.

The fix: a 429 specifically (not a generic failure -- timeout, malformed
JSON, symbol not found) sets a module-level cooldown. While the cooldown
is active, ANY caller for ANY symbol skips the doomed Polygon attempt
entirely and falls straight to Alpaca -- Alpaca still gets called exactly
as before, only the wasted Polygon call is removed.

These tests never make a real network call (Polygon or Alpaca) -- Alpaca
credentials are explicitly removed from the environment so the HM-CA
block deterministically falls through past it rather than risking a real
call if this box's real .env happens to have live keys loaded.
"""
from __future__ import annotations

import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.market_data as market_data  # noqa: E402
from engine.market_data import (  # noqa: E402
    _is_polygon_limited,
    _set_polygon_limited,
    get_intraday_candles,
)


@pytest.fixture(autouse=True)
def _reset_polygon_cooldown_state():
    """Module-level globals persist across tests -- reset before and after
    every test so no test leaks a cooldown into the next one."""
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False
    yield
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False


@pytest.fixture(autouse=True)
def _no_real_yahoo_call():
    """When both Polygon and Alpaca fail/are skipped, the cascade falls
    through to a real Yahoo HTTP call by default -- block that in every
    test so this file never depends on network access or is slowed down
    by a real request. Tests that specifically care about reaching this
    point override it themselves (see the explicit patch in the
    falls-through-to-alpaca test)."""
    with patch.object(market_data, "_yahoo_chart", return_value=None):
        yield


def _polygon_bar(dt: datetime, price: float):
    return {"t": int(dt.timestamp() * 1000), "o": price, "h": price + 1,
            "l": price - 1, "c": price, "v": 1000}


def _no_alpaca_creds(monkeypatch):
    """Force the HM-CA block to fail deterministically (credentials
    unavailable) regardless of what this box's real .env has loaded --
    never risk a real Alpaca call from a test."""
    monkeypatch.delenv("APCA_API_KEY_ID", raising=False)
    monkeypatch.delenv("APCA_API_SECRET_KEY", raising=False)


# ── the cooldown helpers themselves ─────────────────────────────────────────

def test_not_limited_by_default():
    assert _is_polygon_limited() is False


def test_set_polygon_limited_activates_cooldown():
    _set_polygon_limited()
    assert _is_polygon_limited() is True


def test_cooldown_expires_after_window():
    _set_polygon_limited()
    assert _is_polygon_limited() is True
    market_data._polygon_limited_until = time.time() - 1  # force-expire
    assert _is_polygon_limited() is False


# ── get_intraday_candles behavior ───────────────────────────────────────────

def test_429_sets_cooldown(monkeypatch):
    """A 429 from Polygon must set the module-level cooldown."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)

    def _fake_get(url, timeout=None):
        resp = Mock()
        resp.status_code = 429
        return resp

    assert _is_polygon_limited() is False
    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")
    assert _is_polygon_limited() is True


def test_non_429_failure_does_not_set_cooldown(monkeypatch):
    """A generic failure (500, malformed response, etc.) is a one-off --
    it must NOT trip the shared cooldown that penalizes every other caller."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)

    def _fake_get(url, timeout=None):
        resp = Mock()
        resp.status_code = 500
        return resp

    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")
    assert _is_polygon_limited() is False


def test_second_call_during_cooldown_skips_polygon_entirely(monkeypatch):
    """The actual fix: once ANY call sees a 429, the NEXT call (even for a
    different symbol) must not attempt Polygon at all while the cooldown
    is active -- this is what collapses the 37,174-call storm."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    call_count = {"n": 0}

    def _fake_get(url, timeout=None):
        call_count["n"] += 1
        resp = Mock()
        resp.status_code = 429
        return resp

    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")
        assert call_count["n"] == 1, "first call must attempt Polygon"

        get_intraday_candles("AAPL", interval="1m", range_="1d")
        assert call_count["n"] == 1, (
            "second call (different symbol, still in cooldown) must NOT "
            "attempt Polygon -- this is the wasted-call elimination"
        )


def test_polygon_tried_again_after_cooldown_expires(monkeypatch):
    """Once the cooldown window passes, Polygon must be attempted again --
    this is a temporary backoff, not a permanent kill switch."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)
    call_count = {"n": 0}

    def _fake_get(url, timeout=None):
        call_count["n"] += 1
        resp = Mock()
        resp.status_code = 429
        return resp

    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("SPY", interval="1m", range_="1d")
        assert call_count["n"] == 1

    market_data._polygon_limited_until = time.time() - 1  # force-expire

    with patch("requests.get", side_effect=_fake_get):
        get_intraday_candles("AAPL", interval="1m", range_="1d")
        assert call_count["n"] == 2, "Polygon must be retried once the cooldown expires"


def test_success_still_returns_candles_when_not_limited(monkeypatch):
    """Sanity: the happy path (no 429 at all) is completely unchanged --
    this fix must not alter normal successful behavior."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    now = datetime.now(timezone.utc)

    def _fake_get(url, timeout=None):
        resp = Mock()
        resp.status_code = 200
        resp.json.return_value = {"results": [_polygon_bar(now, 100.0)]}
        return resp

    with patch("requests.get", side_effect=_fake_get):
        candles = get_intraday_candles("SPY", interval="1m", range_="1d")
    assert len(candles) == 1
    assert candles[0]["close"] == 100.0
    assert _is_polygon_limited() is False


def test_skip_during_cooldown_still_falls_through_to_alpaca_not_silently_empty(monkeypatch):
    """HM-CA must still be attempted when Polygon is skipped -- the caller
    gets a real second-provider attempt, not an immediate empty return
    just because Polygon was in cooldown."""
    monkeypatch.setenv("POLYGON_API_KEY", "test-key")
    _no_alpaca_creds(monkeypatch)  # forces HM-CA to raise "credentials unavailable"
    _set_polygon_limited()  # already in cooldown before this call

    alpaca_reached = {"hit": False}

    def _fake_yahoo_chart(symbol, interval="1d", range_="1mo"):
        alpaca_reached["hit"] = True  # proves we got PAST the Polygon skip
        return None

    with patch("requests.get") as _mock_get, \
         patch.object(market_data, "_yahoo_chart", side_effect=_fake_yahoo_chart):
        result = get_intraday_candles("SPY", interval="1m", range_="1d")
        _mock_get.assert_not_called()  # Polygon skipped entirely
    assert alpaca_reached["hit"] is True  # cascade continued past HM-CA to Yahoo
    assert result == []  # nothing available anywhere -- honest empty, not fabricated


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
