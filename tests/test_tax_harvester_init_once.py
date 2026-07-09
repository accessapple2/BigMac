"""tests/test_tax_harvester_init_once.py — HM-TAX-INIT-ONCE-2026-07-09.

Covers a live-incident finding (2026-07-09): engine/tax_harvester.py's
get_ytd_summary(), get_harvest_history(), scan_opportunities(), and
get_active_wash_sales() all called _init_tables() unconditionally on every
invocation -- a real write transaction (CREATE TABLE + several INSERT OR
IGNORE + commit) on every single read. Under concurrent write load from the
live trader process, this incidental write blocked behind SQLite's
busy_timeout and surfaced as a 500 on /api/tax/history (uncaught) and
apparent hangs on sibling endpoints.

Fixed with a process-lifetime guard so the DB work only actually runs once.
These tests verify the guard, not the schema-creation SQL itself (unchanged).
"""
from __future__ import annotations

import unittest
from unittest.mock import patch

import engine.tax_harvester as tax_harvester


class InitTablesOnceGuardTests(unittest.TestCase):
    def setUp(self) -> None:
        self._orig_flag = tax_harvester._tables_initialized
        tax_harvester._tables_initialized = False

    def tearDown(self) -> None:
        tax_harvester._tables_initialized = self._orig_flag

    def test_first_call_runs_the_real_init(self) -> None:
        with patch.object(tax_harvester, "_init_tables_uncached") as mock_init:
            tax_harvester._init_tables()
        mock_init.assert_called_once()
        self.assertTrue(tax_harvester._tables_initialized)

    def test_subsequent_calls_skip_the_real_init(self) -> None:
        with patch.object(tax_harvester, "_init_tables_uncached") as mock_init:
            tax_harvester._init_tables()
            tax_harvester._init_tables()
            tax_harvester._init_tables()
            tax_harvester._init_tables()
        mock_init.assert_called_once()

    def test_flag_survives_across_read_path_entry_points(self) -> None:
        """The read-path functions (get_active_wash_sales, scan_opportunities,
        etc.) all call _init_tables() at their top -- once the flag is set by
        any one of them, none of the others should re-trigger the real init."""
        with patch.object(tax_harvester, "_init_tables_uncached") as mock_init:
            tax_harvester._init_tables()  # simulates the first read-path call
            self.assertTrue(tax_harvester._tables_initialized)
            for _ in range(4):  # simulates 4 more read-path functions calling it
                tax_harvester._init_tables()
        mock_init.assert_called_once()


if __name__ == "__main__":
    unittest.main()
