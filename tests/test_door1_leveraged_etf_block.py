"""HM-DOOR1-CENTRALIZE 2026-07-03 — regression test for the leveraged-ETF
CSP write ban (door1, 2026-06-19: "no new 3x-leveraged CSP writes, tail
risk outweighs premium income").

Root cause this closes: wheel_strategy.py enforced its own copy of the
ticker list; shadow_csp.py had the identical WHEEL_TICKERS universe but
never checked it at all -- a 2026-06-28 ghost-book UPRO write proved this
wasn't hypothetical. Fix moved enforcement into open_options_trade() itself
(engine/options_exec.py), the one function every CSP writer already calls,
so no future caller can bypass it just by omitting its own copy of the check.

Safety note: the door1 check in open_options_trade() returns BEFORE any
sqlite3.connect() call, so calling it directly here with a leveraged
symbol makes zero real database writes -- confirmed via before/after
row-count checks in the tests below, not just assumed from reading the code.

Environment note: engine/wheel_strategy.py uses `str | None` union syntax
that only evaluates under Python 3.10+ (the live trader's .venv runtime,
3.14) -- it cannot be imported under this repo's test-runner venv (3.9,
pre-existing and unrelated to this fix; confirmed by reproducing the same
ImportError with a bare `import engine.wheel_strategy` before writing this
test). The wheel_strategy aliasing check below reads its source text
instead of importing it live, to still catch a regression (someone
reverting the alias back to an independent hardcoded list) without
requiring an import this test environment can't perform.
"""
from __future__ import annotations

import os
import sqlite3
import unittest

from engine.options_exec import open_options_trade, LEVERAGED_ETF_TICKERS
from engine.shadow_csp import _build_candidates, WHEEL_TICKERS as SHADOW_WHEEL_TICKERS

_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

DB_PATH = "data/trader.db"


def _max_options_trade_id() -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        row = conn.execute("SELECT MAX(id) FROM options_trades").fetchone()
        return row[0] or 0
    finally:
        conn.close()


class Door1CentralEnforcementTests(unittest.TestCase):
    """Every writer path funnels through open_options_trade() -- test it
    directly rather than mocking each caller, since that's the actual
    choke point the fix relies on."""

    def _attempt_leveraged_csp(self, symbol: str):
        before = _max_options_trade_id()
        result = open_options_trade(
            book_tag="ghost",
            agent_id="test-door1-regression",
            structure="csp",
            symbol=symbol,
            expiration="2026-08-01",
            legs=[{"side": "short", "type": "put", "strike": 100.0,
                   "qty": 1, "entry_price": 5.0}],
        )
        after = _max_options_trade_id()
        return result, before, after

    def test_soxl_csp_blocked_no_db_write(self) -> None:
        result, before, after = self._attempt_leveraged_csp("SOXL")
        self.assertIsNone(result)
        self.assertEqual(before, after, "blocked attempt must not write a row")

    def test_upro_csp_blocked_no_db_write(self) -> None:
        result, before, after = self._attempt_leveraged_csp("UPRO")
        self.assertIsNone(result)
        self.assertEqual(before, after, "blocked attempt must not write a row")

    def test_every_leveraged_ticker_blocked(self) -> None:
        for ticker in LEVERAGED_ETF_TICKERS:
            with self.subTest(ticker=ticker):
                result, before, after = self._attempt_leveraged_csp(ticker)
                self.assertIsNone(result, f"{ticker} should be blocked")
                self.assertEqual(before, after, f"{ticker} must not write a row")

    def test_blocked_attempt_is_logged(self) -> None:
        with self.assertLogs("options_exec", level="WARNING") as cm:
            self._attempt_leveraged_csp("SOXL")
        self.assertTrue(
            any("HM-DOOR1" in line and "SOXL" in line for line in cm.output),
            f"expected an HM-DOOR1 warning mentioning SOXL, got: {cm.output}",
        )

    def test_non_leveraged_symbol_not_blocked_by_door1_check(self) -> None:
        # A long_call on a leveraged ticker must NOT trip door1 (CSP-specific,
        # matching wheel_strategy.py's original scope) -- verified by
        # confirming it gets PAST the early-return. Patches sqlite3.connect
        # to an in-memory DB (missing the real schema) so the write attempt
        # that follows fails harmlessly instead of touching the real
        # database -- an earlier version of this test used the real DB_PATH
        # and accidentally wrote a live row (caught, reverted, and fixed
        # here -- see HANDOFF for the incident note).
        import logging
        import sqlite3 as sqlite3_module
        from unittest.mock import patch

        records = []
        handler = logging.Handler()
        handler.emit = lambda record: records.append(record)
        logger = logging.getLogger("options_exec")
        logger.addHandler(handler)
        try:
            with patch(
                "engine.options_exec.sqlite3.connect",
                return_value=sqlite3_module.connect(":memory:"),
            ):
                result = open_options_trade(
                    book_tag="ghost", agent_id="test-door1-regression",
                    structure="long_call", symbol="SOXL", expiration="2026-08-01",
                    legs=[{"side": "long", "type": "call", "strike": 100.0,
                           "qty": 1, "entry_price": 5.0}],
                )
        finally:
            logger.removeHandler(handler)
        # In-memory DB has no options_trades table -> the write fails and
        # returns None, but for a DIFFERENT reason than door1 -- confirm via
        # the log that door1 itself never fired, not just that it returned None.
        self.assertIsNone(result)
        door1_fired = any("HM-DOOR1" in r.getMessage() for r in records)
        self.assertFalse(
            door1_fired,
            "door1 is CSP-specific -- a long_call structure on a leveraged "
            "ticker must not trip it (matches wheel_strategy.py's original scope)",
        )


class Door1SingleSourceOfTruthTests(unittest.TestCase):
    """The whole point of centralizing: no per-file copies of the ticker
    list that can silently drift apart."""

    def test_wheel_strategy_blocklist_aliases_the_shared_constant(self) -> None:
        # Can't import wheel_strategy.py under this test runner (see module
        # docstring) -- source-text check instead: catches a regression back
        # to an independent hardcoded list without requiring the live import.
        path = os.path.join(_REPO_ROOT, "engine", "wheel_strategy.py")
        with open(path) as f:
            src = f.read()
        self.assertIn(
            "from engine.options_exec import open_options_trade, close_options_trade, LEVERAGED_ETF_TICKERS",
            src,
            "wheel_strategy.py must import the shared ticker list from options_exec",
        )
        self.assertIn(
            "LEVERAGED_ETF_BLOCKLIST = LEVERAGED_ETF_TICKERS",
            src,
            "wheel_strategy.py's blocklist must alias the shared constant, not redefine it",
        )

    def test_shadow_csp_candidates_never_include_leveraged_tickers(self) -> None:
        # _build_candidates fetches live prices -- rather than mock the whole
        # network path, confirm the leveraged tickers were filtered out of
        # its own WHEEL_TICKERS-derived scan loop by checking none of them
        # can appear in the emitted candidate list regardless of price data.
        leveraged_in_universe = [t for t in SHADOW_WHEEL_TICKERS if t in LEVERAGED_ETF_TICKERS]
        self.assertTrue(
            leveraged_in_universe,
            "sanity check: shadow_csp's WHEEL_TICKERS should still list leveraged "
            "tickers (the filter is in _build_candidates, not the universe itself)",
        )
        candidates = _build_candidates(vix=20.0, held=set())
        candidate_tickers = {c["ticker"] for c in candidates}
        blocked_that_leaked = candidate_tickers & LEVERAGED_ETF_TICKERS
        self.assertFalse(
            blocked_that_leaked,
            f"leveraged tickers leaked into shadow_csp candidates: {blocked_that_leaked}",
        )


if __name__ == "__main__":
    unittest.main()
