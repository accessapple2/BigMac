"""HM-CSP-WHEEL-SCAN-LOG-2026-07-05 tests.

Verifies _log_scan_outcome() writes one row per scan attempt, and that
run_wheel_scan()'s four exit paths (max_positions, cap_blocked, vix_skip,
scan_completed) each produce the correct outcome row. Isolated temp DB only
-- never touches data/trader.db.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.wheel_strategy as ws


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute('''CREATE TABLE csp_wheel_scan_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        book_tag TEXT NOT NULL DEFAULT 'fleet',
        outcome TEXT NOT NULL,
        tickers_evaluated INTEGER DEFAULT 0,
        positions_opened INTEGER DEFAULT 0,
        total_notional REAL,
        options_cap_utilization_pct REAL,
        detail TEXT
    )''')
    conn.commit()
    conn.close()


class LogScanOutcomeTests(unittest.TestCase):
    """Pure write-path tests against an isolated temp DB."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self._db_patch = patch.object(ws, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def _rows(self):
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute("SELECT * FROM csp_wheel_scan_log ORDER BY id").fetchall()
        conn.close()
        return rows

    def test_basic_write(self):
        ws._log_scan_outcome("vix_skip", detail="VIX 15.0 < 18")
        rows = self._rows()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["outcome"], "vix_skip")
        self.assertEqual(rows[0]["book_tag"], "fleet")
        self.assertEqual(rows[0]["detail"], "VIX 15.0 < 18")

    def test_exposure_fields_persisted(self):
        ws._log_scan_outcome(
            "cap_blocked",
            exposure={"total_notional": 12_000.0, "options_cap_utilization_pct": 55.5},
        )
        row = self._rows()[0]
        self.assertEqual(row["total_notional"], 12_000.0)
        self.assertEqual(row["options_cap_utilization_pct"], 55.5)

    def test_none_exposure_does_not_raise(self):
        ws._log_scan_outcome("max_positions_reached")  # no exposure passed
        row = self._rows()[0]
        self.assertIsNone(row["total_notional"])
        self.assertIsNone(row["options_cap_utilization_pct"])

    def test_scan_completed_counts(self):
        ws._log_scan_outcome("scan_completed", tickers_evaluated=6, positions_opened=2)
        row = self._rows()[0]
        self.assertEqual(row["tickers_evaluated"], 6)
        self.assertEqual(row["positions_opened"], 2)

    def test_write_failure_does_not_raise(self):
        """A broken DB path must not propagate -- logging failure can't break the scan."""
        with patch.object(ws, "DB", "/nonexistent/path/does/not/exist.db"):
            ws._log_scan_outcome("vix_skip")  # must not raise


class WheelScanLogIntegrationTests(unittest.TestCase):
    """Extends the existing WheelScanGateTests pattern (tests/test_troi_csp_cap_gate.py)
    to assert each of run_wheel_scan()'s four exit paths writes the right outcome row."""

    def setUp(self):
        ws._done_today = False
        ws._last_date = None
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self._db_patch = patch.object(ws, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def _outcomes(self):
        conn = sqlite3.connect(self.db_path)
        rows = [r[0] for r in conn.execute("SELECT outcome FROM csp_wheel_scan_log").fetchall()]
        conn.close()
        return rows

    def test_max_positions_logs_max_positions_reached(self):
        held = [{"symbol": t, "asset_type": "option"} for t in ws.WHEEL_TICKERS[:ws.MAX_POSITIONS]]
        with patch.object(ws, "_is_market_hours", return_value=True), \
             patch.object(ws, "get_portfolio", return_value={"cash": 10_000, "positions": held}):
            ws.run_wheel_scan()
        self.assertEqual(self._outcomes(), ["max_positions_reached"])

    def test_cap_breach_logs_cap_blocked(self):
        with patch("config.TROI_CSP_CAP_GATE", True), \
             patch("engine.risk_manager.csp_options_cap_breached",
                   return_value=(True, {"total_notional": 1_000_000.0,
                                         "options_cap_utilization_pct": 999.0})), \
             patch("engine.risk_manager.log_csp_exposure", return_value={}), \
             patch.object(ws, "_is_market_hours", return_value=True), \
             patch.object(ws, "get_portfolio", return_value={"cash": 10_000, "positions": []}), \
             patch("engine.alert_channels.send_alert"):
            ws.run_wheel_scan()
        self.assertEqual(self._outcomes(), ["cap_blocked"])

    def test_vix_too_low_logs_vix_skip(self):
        with patch("config.TROI_CSP_CAP_GATE", False), \
             patch.object(ws, "_is_market_hours", return_value=True), \
             patch.object(ws, "get_portfolio", return_value={"cash": 10_000, "positions": []}), \
             patch.object(ws, "get_fear_greed_index", return_value={"signals": {"vix": {"value": 10.0}}}):
            ws.run_wheel_scan()
        self.assertEqual(self._outcomes(), ["vix_skip"])

    def test_full_scan_logs_scan_completed(self):
        with patch("config.TROI_CSP_CAP_GATE", False), \
             patch.object(ws, "_is_market_hours", return_value=True), \
             patch.object(ws, "get_portfolio", return_value={"cash": 10_000, "positions": []}), \
             patch.object(ws, "get_fear_greed_index", return_value={"signals": {"vix": {"value": 30.0}}}), \
             patch.object(ws, "get_stock_price", return_value={"price": 0}):
            # price=0 makes every ticker skip via `if price <= 0: continue`,
            # so the loop runs to completion with 0 opens -- exercises the
            # scan_completed path without needing a real options_exec write.
            ws.run_wheel_scan()
        outcomes = self._outcomes()
        self.assertEqual(outcomes, ["scan_completed"])


if __name__ == "__main__":
    unittest.main()
