"""tests/test_fundamentals_etf_skip.py — HM-SLOW-FUNDAMENTALS regression tests.

The dashboard's 7 timeout endpoints (/api/screener, /api/patterns,
/api/pattern-alerts, /api/channels, /api/risk-radar, /api/market/correlation,
/api/trendlines) all fan out to ``engine.stock_fundamentals.fetch_fundamentals``,
which hits ``yahoo_quote_summary`` for every symbol — including the ~25 ETFs
in the active universe. ETF responses are uniformly empty (Yahoo returns
None for every fundamentals module on ETF symbols by design — see
feedback_etf_market_cap_lookup memory note), so the slow call is pure
wasted I/O.

Post-patch contract:
  - ETF symbols (per scan_universe.ticker_type IN ('ETF','ETN')) return an
    all-None sentinel dict without any yahoo_quote_summary call.
  - The returned sentinel matches the shape expected by
    engine/quality_gate.py:50 ETF fast-path detection so existing QG
    behavior is unaffected.
  - Stock symbols still hit yahoo_quote_summary on a cache miss.
  - The in-memory cache (existing behavior) still serves repeat calls
    within ``_CACHE_TTL`` (1h) for stocks; ETF sentinel is cheaper than
    cache lookup but caching it preserves consistent return semantics.

Run from project root:
    venv/bin/python3 -m pytest tests/test_fundamentals_etf_skip.py -v
"""
from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest import mock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _reset_module_state():
    """Clear caches between tests so prior cached data doesn't leak."""
    import engine.stock_fundamentals as sf
    sf._cache.clear()
    # Reset the post-patch ETF set if present so the mocked _load_etf_symbols
    # is exercised. Pre-patch this attribute won't exist; that's fine.
    if hasattr(sf, "_etf_symbols"):
        sf._etf_symbols = None


def _bypass_db_cache():
    """Return a patch context that makes ``_get_cached`` always miss.

    Production has real fundamentals cached for live symbols in the SQLite
    stock_fundamentals table (24h TTL). Without this bypass, tests that try
    to drive symbols through the Yahoo path would silently short-circuit on
    a cache hit, producing false-green or false-red results unrelated to
    the ETF-skip behavior under test.
    """
    return mock.patch(
        "engine.stock_fundamentals._get_cached",
        return_value=None,
    )


# Realistic Yahoo response for a stock (just enough fields so fetch_fundamentals
# parses without raising). Most fields are None — the function tolerates that.
_YAHOO_STOCK_RESPONSE = {
    "financialData": {
        "revenueGrowth": 0.08,
        "earningsGrowth": 0.12,
        "recommendationKey": "buy",
    },
    "defaultKeyStatistics": {},
    "summaryDetail": {"marketCap": 1_000_000_000_000},
    "calendarEvents": {},
    "recommendationTrend": {},
    "majorHoldersBreakdown": {},
    "summaryProfile": {"sector": "Technology"},
    "earningsTrend": {},
}


class TestEtfSkipPath(unittest.TestCase):
    """ETF symbols must skip the slow Yahoo call entirely."""

    def setUp(self):
        _reset_module_state()

    @mock.patch("engine.stock_fundamentals._save_to_db")
    @mock.patch("engine.market_data.yahoo_quote_summary")
    def test_etf_symbol_returns_without_yahoo_call(self, mock_yahoo, _mock_save):
        """Post-patch: ETF symbol returns sentinel; yahoo_quote_summary NOT called.

        Pre-patch (TDD red): fetch_fundamentals will reach yahoo_quote_summary
        because there is no ETF check; this test fails on the call_count
        assertion.
        """
        import engine.stock_fundamentals as sf

        mock_yahoo.return_value = _YAHOO_STOCK_RESPONSE  # safety net

        # Stub the post-patch ETF detection. The patch will read this.
        with mock.patch.object(
            sf, "_load_etf_symbols", create=True, return_value={"TQQQ", "IBIT", "NUKZ"}
        ), _bypass_db_cache():
            sf._etf_symbols = None  # force re-load via the stub
            result = sf.fetch_fundamentals("TQQQ")

        self.assertEqual(
            mock_yahoo.call_count, 0,
            msg=f"yahoo_quote_summary must NOT be called for ETF symbol; "
                f"call_count={mock_yahoo.call_count}",
        )
        self.assertIsNotNone(result, msg="ETF must return a sentinel dict, not None")
        self.assertIsInstance(result, dict)
        # QG ETF fast-path detection at engine/quality_gate.py:50 reads these
        # three keys; all must be None for the fast-path to fire.
        self.assertIsNone(result.get("earnings_growth"),
                          msg=f"sentinel.earnings_growth must be None, got {result.get('earnings_growth')}")
        self.assertIsNone(result.get("revenue_growth"),
                          msg=f"sentinel.revenue_growth must be None")
        self.assertIsNone(result.get("recommendation"),
                          msg=f"sentinel.recommendation must be None")

    @mock.patch("engine.stock_fundamentals._save_to_db")
    @mock.patch("engine.market_data.yahoo_quote_summary")
    def test_etf_skip_no_db_write(self, mock_yahoo, mock_save):
        """ETF sentinel must NOT be persisted to the stock_fundamentals table."""
        import engine.stock_fundamentals as sf
        with mock.patch.object(
            sf, "_load_etf_symbols", create=True, return_value={"TQQQ"}
        ), _bypass_db_cache():
            sf._etf_symbols = None
            sf.fetch_fundamentals("TQQQ")
        self.assertEqual(
            mock_save.call_count, 0,
            msg=f"_save_to_db must NOT be called for ETF; call_count={mock_save.call_count}",
        )


class TestStockPathPreserved(unittest.TestCase):
    """Non-ETF symbols must continue to hit Yahoo on a cache miss."""

    def setUp(self):
        _reset_module_state()

    @mock.patch("engine.stock_fundamentals._save_to_db")
    @mock.patch("engine.market_data.yahoo_quote_summary")
    def test_stock_symbol_calls_yahoo_once(self, mock_yahoo, _mock_save):
        """Stock symbol with no cache → yahoo_quote_summary called exactly once."""
        import engine.stock_fundamentals as sf
        mock_yahoo.return_value = _YAHOO_STOCK_RESPONSE

        with mock.patch.object(
            sf, "_load_etf_symbols", create=True, return_value={"TQQQ"}
        ), _bypass_db_cache():
            sf._etf_symbols = None
            sf.fetch_fundamentals("AAPL")  # AAPL is NOT in the ETF set

        self.assertEqual(
            mock_yahoo.call_count, 1,
            msg=f"yahoo_quote_summary should be called exactly once for stock; "
                f"got {mock_yahoo.call_count}",
        )

    @mock.patch("engine.stock_fundamentals._save_to_db")
    @mock.patch("engine.market_data.yahoo_quote_summary")
    def test_stock_repeat_call_hits_cache(self, mock_yahoo, _mock_save):
        """Two consecutive fetch_fundamentals calls for a stock → only ONE Yahoo call."""
        import engine.stock_fundamentals as sf
        mock_yahoo.return_value = _YAHOO_STOCK_RESPONSE

        # First call: cache miss → Yahoo invoked.
        # Second call: in-memory _cache populated by first call's success path,
        # so _get_cached returns the cached entry without hitting Yahoo.
        # We mock _get_cached with a side_effect that returns None first time
        # and the cached entry second time, simulating real cache behavior.
        cache_hits = [None]  # first call: miss; subsequent: derived below
        def _get_cached_stub(symbol):
            if cache_hits[0] is None:
                return None  # cache miss on first call
            return cache_hits[0]  # cache hit on subsequent calls

        with mock.patch.object(
            sf, "_load_etf_symbols", create=True, return_value=set()
        ), mock.patch("engine.stock_fundamentals._get_cached", side_effect=_get_cached_stub):
            sf._etf_symbols = None
            first = sf.fetch_fundamentals("AAPL")
            cache_hits[0] = first  # populate the stubbed cache
            sf.fetch_fundamentals("AAPL")  # should be cached

        self.assertEqual(
            mock_yahoo.call_count, 1,
            msg=f"second call should hit cache; got {mock_yahoo.call_count} Yahoo calls",
        )


class TestEtfRepeatCallCached(unittest.TestCase):
    """ETF symbol on repeat → still no Yahoo call (idempotent skip)."""

    def setUp(self):
        _reset_module_state()

    @mock.patch("engine.stock_fundamentals._save_to_db")
    @mock.patch("engine.market_data.yahoo_quote_summary")
    def test_etf_repeat_calls_zero_yahoo(self, mock_yahoo, _mock_save):
        import engine.stock_fundamentals as sf
        with mock.patch.object(
            sf, "_load_etf_symbols", create=True, return_value={"TQQQ"}
        ), _bypass_db_cache():
            sf._etf_symbols = None
            for _ in range(5):
                sf.fetch_fundamentals("TQQQ")
        self.assertEqual(
            mock_yahoo.call_count, 0,
            msg=f"5 ETF fetches → 0 Yahoo calls expected; got {mock_yahoo.call_count}",
        )


if __name__ == "__main__":
    unittest.main()
