"""HM-TROI-GUARDRAILS-TRIM 2026-07-04 tests.

Two layers:
  1. get_csp_exposure() / csp_options_cap_breached() — pure computation
     against an isolated temp SQLite DB (never the live trader.db), so
     these tests stay correct regardless of what Troi's live book looks
     like on any given day.
  2. wheel_strategy.run_wheel_scan() gate behavior for both
     config.TROI_CSP_CAP_GATE states, fully mocked (no live DB, no network,
     no real options_exec writes).
"""
from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.risk_manager as rm


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE options_books (book_tag TEXT PRIMARY KEY, current_cash REAL)"
    )
    conn.execute(
        "CREATE TABLE options_trades (id INTEGER PRIMARY KEY, book_tag TEXT, "
        "agent_id TEXT, structure TEXT, symbol TEXT, status TEXT, legs_json TEXT)"
    )
    conn.commit()
    conn.close()


def _insert_csp(path: str, book_tag: str, symbol: str, strike: float,
                qty: int = 1, status: str = "open") -> None:
    legs = json.dumps([{"side": "short", "type": "put", "strike": strike,
                        "qty": qty, "entry_price": 1.0}])
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO options_trades (book_tag, agent_id, structure, symbol, status, legs_json) "
        "VALUES (?, 'options-sosnoff', 'csp', ?, ?, ?)",
        (book_tag, symbol, status, legs),
    )
    conn.commit()
    conn.close()


class CspExposureComputationTests(unittest.TestCase):
    """Pure computation against an isolated temp DB — deterministic."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self._db_patch = patch.object(rm, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def _set_book_cash(self, cash: float, book_tag: str = "fleet"):
        conn = sqlite3.connect(self.db_path)
        conn.execute(
            "INSERT OR REPLACE INTO options_books (book_tag, current_cash) VALUES (?, ?)",
            (book_tag, cash),
        )
        conn.commit()
        conn.close()

    def test_empty_book_zero_exposure(self):
        self._set_book_cash(10_000.0)
        exp = rm.get_csp_exposure()
        self.assertEqual(exp["total_notional"], 0.0)
        self.assertEqual(exp["options_cap_utilization_pct"], 0.0)
        self.assertEqual(exp["per_underlying_notional"], {})

    def test_single_symbol_notional_and_utilization(self):
        self._set_book_cash(10_000.0)
        _insert_csp(self.db_path, "fleet", "QQQ", strike=500.0, qty=1)
        exp = rm.get_csp_exposure()
        # notional = strike * 100 * qty = 500 * 100 * 1 = 50,000
        self.assertEqual(exp["total_notional"], 50_000.0)
        self.assertEqual(exp["per_underlying_notional"]["QQQ"], 50_000.0)
        self.assertAlmostEqual(exp["options_cap_utilization_pct"], 500.0)  # 50k / 10k * 100

    def test_multi_symbol_sums_correctly(self):
        self._set_book_cash(100_000.0)
        _insert_csp(self.db_path, "fleet", "SOXL", strike=100.0, qty=2)   # 20,000
        _insert_csp(self.db_path, "fleet", "SOXL", strike=110.0, qty=1)   # 11,000
        _insert_csp(self.db_path, "fleet", "UPRO", strike=120.0, qty=1)   # 12,000
        exp = rm.get_csp_exposure()
        self.assertEqual(exp["per_underlying_notional"]["SOXL"], 31_000.0)
        self.assertEqual(exp["per_underlying_notional"]["UPRO"], 12_000.0)
        self.assertEqual(exp["total_notional"], 43_000.0)

    def test_closed_trades_excluded(self):
        self._set_book_cash(10_000.0)
        _insert_csp(self.db_path, "fleet", "QQQ", strike=500.0, qty=1, status="closed")
        exp = rm.get_csp_exposure()
        self.assertEqual(exp["total_notional"], 0.0)

    def test_other_book_tag_excluded(self):
        self._set_book_cash(10_000.0)
        self._set_book_cash(5_000.0, book_tag="ghost")
        _insert_csp(self.db_path, "ghost", "QQQ", strike=500.0, qty=1)
        exp = rm.get_csp_exposure(book_tag="fleet")
        self.assertEqual(exp["total_notional"], 0.0)

    def test_breach_detection_under_cap(self):
        self._set_book_cash(1_000_000.0)  # huge book, tiny position -> under 10%
        _insert_csp(self.db_path, "fleet", "QQQ", strike=500.0, qty=1)  # 50,000 = 5%
        breached, exp = rm.csp_options_cap_breached()
        self.assertFalse(breached)

    def test_breach_detection_over_cap(self):
        self._set_book_cash(10_000.0)
        _insert_csp(self.db_path, "fleet", "QQQ", strike=500.0, qty=1)  # 50,000 = 500%
        breached, exp = rm.csp_options_cap_breached()
        self.assertTrue(breached)

    def test_zero_book_value_no_divide_by_zero(self):
        self._set_book_cash(0.0)
        _insert_csp(self.db_path, "fleet", "QQQ", strike=500.0, qty=1)
        exp = rm.get_csp_exposure()  # must not raise
        self.assertEqual(exp["options_cap_utilization_pct"], 0.0)


class WheelScanGateTests(unittest.TestCase):
    """run_wheel_scan() gate behavior for both TROI_CSP_CAP_GATE states.
    Everything else mocked -- no live DB, no network, no real opens."""

    def setUp(self):
        import engine.wheel_strategy as ws
        self.ws = ws
        ws._done_today = False
        ws._last_date = None

    def test_gate_on_blocks_open_when_breached(self):
        with patch("config.TROI_CSP_CAP_GATE", True), \
             patch("engine.risk_manager.csp_options_cap_breached",
                   return_value=(True, {"total_notional": 1_000_000.0,
                                         "options_cap_utilization_pct": 999.0})), \
             patch("engine.risk_manager.log_csp_exposure", return_value={}), \
             patch.object(self.ws, "_is_market_hours", return_value=True), \
             patch.object(self.ws, "get_portfolio", return_value={"cash": 10_000, "positions": []}), \
             patch.object(self.ws, "open_options_trade") as mock_open, \
             patch("engine.alert_channels.send_alert") as mock_alert:
            self.ws.run_wheel_scan()
            mock_open.assert_not_called()
            mock_alert.assert_called_once()
            self.assertTrue(self.ws._done_today)

    def test_gate_off_does_not_block_on_cap_alone(self):
        """With the flag OFF, a cap breach must not by itself stop the scan
        from proceeding past the gate point (it may still no-op later for
        unrelated reasons like VIX/premium thresholds -- we only assert the
        gate's own early-return path was not taken)."""
        with patch("config.TROI_CSP_CAP_GATE", False), \
             patch("engine.risk_manager.csp_options_cap_breached",
                   return_value=(True, {"total_notional": 1_000_000.0,
                                         "options_cap_utilization_pct": 999.0})) as mock_breach, \
             patch.object(self.ws, "_is_market_hours", return_value=True), \
             patch.object(self.ws, "get_portfolio", return_value={"cash": 10_000, "positions": []}), \
             patch.object(self.ws, "get_fear_greed_index", return_value={"signals": {"vix": {"value": 10.0}}}):
            self.ws.run_wheel_scan()
            # Gate check itself must be skipped entirely when the flag is off.
            mock_breach.assert_not_called()
            # VIX=10 < MIN_VIX=18 makes it no-op for an unrelated reason,
            # which is fine -- confirms we got PAST the gate, not stuck on it.
            self.assertTrue(self.ws._done_today)


if __name__ == "__main__":
    unittest.main()
