"""HM-CLEANUP-TRIO-2026-07-04 Item 1 tests.

engine.paper_trader._csp_realized_pnl_v1 -- isolated temp DB, never live
trader.db, so these stay correct regardless of Troi's live book state.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.paper_trader as pt


def _make_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "CREATE TABLE options_trades (id INTEGER PRIMARY KEY, agent_id TEXT, "
        "structure TEXT, status TEXT, pnl REAL, exit_date TEXT)"
    )
    conn.commit()
    conn.close()


def _insert(path: str, agent_id: str, pnl: float, exit_date: str,
           structure: str = "csp", status: str = "closed") -> None:
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO options_trades (agent_id, structure, status, pnl, exit_date) VALUES (?,?,?,?,?)",
        (agent_id, structure, status, pnl, exit_date),
    )
    conn.commit()
    conn.close()


class CspRealizedPnlV1Tests(unittest.TestCase):

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_db(self.db_path)
        self._db_patch = patch.object(pt, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def test_no_trades_returns_zero(self):
        self.assertEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 0.0)

    def test_sums_only_matching_agent(self):
        _insert(self.db_path, "options-sosnoff", 100.0, "2026-06-01")
        _insert(self.db_path, "options-sosnoff", 200.0, "2026-06-02")
        _insert(self.db_path, "strategy:bull_spread_v1", 999.0, "2026-06-01")
        self.assertAlmostEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 300.0)

    def test_other_agent_gets_zero_even_when_troi_has_trades(self):
        _insert(self.db_path, "options-sosnoff", 100.0, "2026-06-01")
        self.assertEqual(pt._csp_realized_pnl_v1("ollama-plutus"), 0.0)

    def test_excludes_open_status(self):
        _insert(self.db_path, "options-sosnoff", 100.0, "2026-06-01", status="closed")
        _insert(self.db_path, "options-sosnoff", 9999.0, "2026-06-01", status="open")
        self.assertAlmostEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 100.0)

    def test_excludes_non_csp_structure(self):
        _insert(self.db_path, "options-sosnoff", 100.0, "2026-06-01", structure="csp")
        _insert(self.db_path, "options-sosnoff", 500.0, "2026-06-01", structure="bull_put_spread")
        self.assertAlmostEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 100.0)

    def test_v2_era_boundary_excludes_trades_on_or_after_start(self):
        _insert(self.db_path, "options-sosnoff", 100.0, "2026-07-05")  # before boundary
        _insert(self.db_path, "options-sosnoff", 500.0, pt.TROI_V2_ERA_START)  # on boundary -> excluded
        _insert(self.db_path, "options-sosnoff", 777.0, "2026-07-10")  # after boundary -> excluded
        self.assertAlmostEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 100.0)

    def test_handles_negative_pnl_correctly(self):
        _insert(self.db_path, "options-sosnoff", 500.0, "2026-06-01")
        _insert(self.db_path, "options-sosnoff", -200.0, "2026-06-02")
        self.assertAlmostEqual(pt._csp_realized_pnl_v1("options-sosnoff"), 300.0)


class GetPortfolioWithPnlCspFoldInTests(unittest.TestCase):
    """get_portfolio_with_pnl folds in CSP P&L without affecting agents that
    have none. Mocks get_portfolio to avoid touching the live positions table."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_db(self.db_path)
        self._db_patch = patch.object(pt, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def test_zero_positions_and_zero_csp_matches_cash_only(self):
        with patch.object(pt, "get_portfolio", return_value={"cash": 7000.0, "positions": []}):
            res = pt.get_portfolio_with_pnl("some-agent", {})
        self.assertEqual(res["csp_realized_pnl_v1"], 0.0)
        self.assertEqual(res["total_value"], 7000.0)

    def test_csp_pnl_folds_into_total_value(self):
        _insert(self.db_path, "options-sosnoff", 29868.74, "2026-06-01")
        with patch.object(pt, "get_portfolio", return_value={"cash": 12880.20, "positions": []}):
            res = pt.get_portfolio_with_pnl("options-sosnoff", {})
        self.assertAlmostEqual(res["csp_realized_pnl_v1"], 29868.74)
        self.assertAlmostEqual(res["total_value"], 12880.20 + 29868.74, places=2)


if __name__ == "__main__":
    unittest.main()
