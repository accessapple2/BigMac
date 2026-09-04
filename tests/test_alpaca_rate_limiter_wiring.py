"""tests/test_alpaca_rate_limiter_wiring.py — HM-ALPACA-LIMITER-WIRE-2026-09-04.

engine/rate_limiter.py::AlpacaRateLimiter (150/min conservative token
bucket, real Alpaca ceiling 200/min) already existed and was already wired
into deep_scan.py/strategy_rotator.py -- but not into engine/market_data.py,
which is where the actual 777 Alpaca 429s on 2026-09-01 came from:
"get_alpaca_bars HTTP 429" (525), "_alpaca_bulk_bars_chunk HTTP 429" (236),
"get_alpaca_bars batch HTTP 429" (16) -- all three in market_data.py, none
of them going through the shared limiter every other caller already does.

These tests verify the shared singleton's acquire() is actually invoked
before each of the three real HTTP call sites this fix touched:
  1. get_intraday_candles's HM-CA block (engine/market_data.py)
  2. get_alpaca_bars (both the batch request and the per-symbol fallback loop)
  3. _alpaca_bulk_bars_chunk

None of these tests make a real network call -- requests.get and the
Alpaca SDK client are both mocked.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import engine.market_data as market_data  # noqa: E402


@pytest.fixture(autouse=True)
def _reset_polygon_cooldown_state():
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False
    yield
    market_data._polygon_limited_until = 0
    market_data._polygon_cooldown_logged = False


@pytest.fixture(autouse=True)
def _reset_alpaca_headers_cache():
    """_get_alpaca_headers() memoizes into a module global on first call --
    reset it so each test's monkeypatched env vars actually take effect."""
    market_data._alpaca_headers = None
    yield
    market_data._alpaca_headers = None


def test_get_intraday_candles_alpaca_fallback_acquires_token(monkeypatch):
    """HM-CA block: Polygon unavailable (no key) forces the fall-through
    to Alpaca -- the SDK call must be preceded by a limiter.acquire()."""
    monkeypatch.delenv("POLYGON_API_KEY", raising=False)
    monkeypatch.setenv("APCA_API_KEY_ID", "fake-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "fake-secret")

    mock_client = Mock()
    mock_resp = Mock()
    mock_resp.data = {}  # no bars -- HM-CA raises "no bars returned", falls to Yahoo
    mock_client.get_stock_bars.return_value = mock_resp

    with patch("alpaca.data.StockHistoricalDataClient", return_value=mock_client), \
         patch.object(market_data, "_yahoo_chart", return_value=None), \
         patch("engine.rate_limiter.limiter") as mock_limiter:
        market_data.get_intraday_candles("SPY", interval="1m", range_="1d")

    mock_limiter.acquire.assert_called_once()
    mock_client.get_stock_bars.assert_called_once()


def test_get_alpaca_bars_single_symbol_acquires_token(monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "fake-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "fake-secret")

    fake_resp = Mock()
    fake_resp.ok = True
    fake_resp.json.return_value = {"bars": {"AAPL": []}}

    with patch("requests.get", return_value=fake_resp) as mock_get, \
         patch("engine.rate_limiter.limiter") as mock_limiter:
        market_data.get_alpaca_bars("AAPL", timeframe="1Day", days=5)

    mock_limiter.acquire.assert_called_once()
    mock_get.assert_called_once()


def test_get_alpaca_bars_batch_fallback_acquires_token_per_symbol(monkeypatch):
    """On a batch failure, get_alpaca_bars retries per-symbol -- each of
    those N real HTTP calls must consume its own token, not just the one
    initial (failed) batch call."""
    monkeypatch.setenv("APCA_API_KEY_ID", "fake-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "fake-secret")

    batch_fail = Mock(ok=False, status_code=400)
    per_symbol_ok = Mock(ok=True)
    per_symbol_ok.json.return_value = {"bars": []}

    call_log = []

    def _fake_get(url, headers=None, params=None, timeout=None):
        call_log.append(url)
        return batch_fail if url.endswith("/bars") else per_symbol_ok

    with patch("requests.get", side_effect=_fake_get), \
         patch("engine.rate_limiter.limiter") as mock_limiter:
        market_data.get_alpaca_bars(["AAPL", "MSFT"], timeframe="1Day", days=5)

    # 1 batch call + 2 per-symbol fallback calls = 3 total HTTP calls,
    # each must have acquired its own token.
    assert len(call_log) == 3
    assert mock_limiter.acquire.call_count == 3


def test_alpaca_bulk_bars_chunk_acquires_token(monkeypatch):
    monkeypatch.setenv("APCA_API_KEY_ID", "fake-key")
    monkeypatch.setenv("APCA_API_SECRET_KEY", "fake-secret")

    fake_resp = Mock()
    fake_resp.ok = True
    fake_resp.json.return_value = {"bars": {}}

    with patch("requests.get", return_value=fake_resp) as mock_get, \
         patch("engine.rate_limiter.limiter") as mock_limiter:
        market_data._alpaca_bulk_bars_chunk(["AAPL", "MSFT"], "2026-08-01", "2026-09-01")

    mock_limiter.acquire.assert_called_once()
    mock_get.assert_called_once()


def test_no_credentials_never_touches_the_limiter():
    """A request that fails before reaching Alpaca (missing creds) must
    never spend a rate-limit token -- there's no call to throttle.

    Patches _get_alpaca_headers() directly rather than deleting env vars:
    it calls load_dotenv() internally, which would silently repopulate
    real credentials from this box's actual .env the moment the env vars
    are removed -- this is the reliable way to force the no-creds path
    regardless of what's really configured on the machine running the test.
    """
    with patch.object(market_data, "_get_alpaca_headers", return_value=None), \
         patch("engine.rate_limiter.limiter") as mock_limiter:
        result = market_data.get_alpaca_bars("AAPL", timeframe="1Day", days=5)

    mock_limiter.acquire.assert_not_called()
    assert result.empty


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
