"""tests/test_trades_endpoint_price_batching.py — HM-TRADES-BARE-SLOW-2026-07-09.

Covers a live-incident finding (2026-07-09): bare GET /api/trades (no query
params) fetches a live price PER OPEN ROW serially via
engine.market_data.get_stock_price -- at the live incident's 407 open trades,
this made the bare call effectively hang (each provider in the 4-provider
fallback chain has its own 5-15s timeout on a cache miss). ?limit=N always
worked because a small N sidestepped most of the loop.

Fix: (1) lower the default `limit` from 500 to 100, directly bounding the
worst case for callers that don't pass it explicitly; (2) dedup by unique
symbol and fetch via engine.market_data.get_all_prices() -- the same
ThreadPoolExecutor-based parallel batch fetcher /api/market/prices already
uses -- instead of one get_stock_price() call per row.

Live-verified against trader.db (408 real open trades): bare call (new
default limit=100) went from 4.35s to <0.01s on a warm cache; limit=500
(162 open) went from 18.02s to 2.51s; limit=1000 (289 open) completed in
5.82s where it previously would not complete in reasonable time at all.

This test verifies the two structural properties directly (default limit
value, and that price-fetch calls are deduped to unique symbols) without
needing network I/O or the full FastAPI/DB stack.
"""
from __future__ import annotations

import inspect
import unittest

import dashboard.app as app_module


class DefaultLimitTests(unittest.TestCase):
    def test_default_limit_is_bounded_not_500(self) -> None:
        sig = inspect.signature(app_module.get_all_trades)
        default_limit = sig.parameters["limit"].default
        self.assertLessEqual(
            default_limit, 200,
            f"default limit={default_limit} is too high -- bare /api/trades "
            "must stay bounded for callers that don't pass ?limit explicitly",
        )


class PriceFetchDedupTests(unittest.TestCase):
    """Verifies get_all_trades' source uses the batch/parallel fetcher and
    computes a deduped symbol set, not a per-row serial call."""

    def test_source_calls_get_all_prices_not_get_stock_price_per_row(self) -> None:
        src = inspect.getsource(app_module.get_all_trades)
        self.assertIn(
            "get_all_prices", src,
            "must use the batch/parallel price fetcher (same one "
            "/api/market/prices uses), not a per-row get_stock_price() call",
        )

    def test_source_dedupes_symbols_via_a_set(self) -> None:
        src = inspect.getsource(app_module.get_all_trades)
        # The dedup construct: a set comprehension over open-trade symbols.
        self.assertIn("_open_symbols", src)
        self.assertRegex(
            src, r"\{r\[.symbol.\]\s+for\s+r\s+in\s+rows",
            "expected a set comprehension deduping symbols across rows",
        )


if __name__ == "__main__":
    unittest.main()
