"""HM-EOD-REPORT-2026-07-05 tests. Isolated temp DB only -- never touches
data/trader.db (except the final real-DB dry-run smoke test, which passes
--dry-run and therefore performs zero writes and zero ntfy pushes)."""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.eod_report as er


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, is_human INTEGER DEFAULT 0, crew_role TEXT DEFAULT 'active',
        halt_mode TEXT DEFAULT 'active'
    )""")
    conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, player_id TEXT, created_at TEXT)")
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, executed_at TEXT, realized_pnl REAL
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, book_tag TEXT DEFAULT 'fleet', agent_id TEXT,
        structure TEXT, status TEXT, entry_date TEXT, pnl REAL
    )""")
    conn.execute("""CREATE TABLE csp_wheel_scan_log (
        id INTEGER PRIMARY KEY, scanned_at TEXT, outcome TEXT
    )""")
    conn.execute("""CREATE TABLE eod_report_log (
        report_date TEXT PRIMARY KEY, signals_count INTEGER, trades_count INTEGER,
        conversion_pct REAL, guarded_pnl REAL, error_count INTEGER,
        sent_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


class SignalTradeConversionTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_no_signals_reports_none_not_zero(self):
        result = er.signal_trade_conversion(self.conn, "2026-07-06")
        self.assertEqual(result["signals"], 0)
        self.assertIsNone(result["conversion_pct"])

    def test_conversion_computed(self):
        for i in range(10):
            self.conn.execute("INSERT INTO signals (player_id, created_at) VALUES ('x', '2026-07-06 10:00:00')")
        for i in range(3):
            self.conn.execute("INSERT INTO trades (player_id, executed_at) VALUES ('x', '2026-07-06 11:00:00')")
        self.conn.commit()
        result = er.signal_trade_conversion(self.conn, "2026-07-06")
        self.assertEqual(result["signals"], 10)
        self.assertEqual(result["trades"], 3)
        self.assertEqual(result["conversion_pct"], 30.0)

    def test_excludes_other_days(self):
        self.conn.execute("INSERT INTO signals (player_id, created_at) VALUES ('x', '2026-07-05 10:00:00')")
        self.conn.execute("INSERT INTO signals (player_id, created_at) VALUES ('x', '2026-07-07 10:00:00')")
        self.conn.commit()
        result = er.signal_trade_conversion(self.conn, "2026-07-06")
        self.assertEqual(result["signals"], 0)


class GuardedPnlForDateTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def _insert(self, player_id, executed_at, pnl, action="SELL"):
        self.conn.execute(
            "INSERT INTO trades (player_id, action, executed_at, realized_pnl) VALUES (?, ?, ?, ?)",
            (player_id, action, executed_at, pnl),
        )
        self.conn.commit()

    def test_sums_realized_pnl_for_date(self):
        self._insert("qwen3-8b-flash", "2026-07-06 10:00:00", 50.0)
        self._insert("capitol-trades", "2026-07-06 11:00:00", -20.0)
        result = er.guarded_pnl_for_date(self.conn, "2026-07-06")
        self.assertEqual(result["fleet_total"], 30.0)
        self.assertEqual(len(result["per_agent"]), 2)

    def test_excludes_tracking_players(self):
        self._insert("dalio-metals", "2026-07-06 10:00:00", 500.0)
        result = er.guarded_pnl_for_date(self.conn, "2026-07-06")
        self.assertEqual(result["fleet_total"], 0.0)

    def test_excludes_pre_garbage_floor(self):
        self._insert("qwen3-8b-flash", "2026-04-01 10:00:00", 500.0)  # pre-2026-05-14
        result = er.guarded_pnl_for_date(self.conn, "2026-04-01")
        self.assertEqual(result["fleet_total"], 0.0)

    def test_excludes_other_days(self):
        self._insert("qwen3-8b-flash", "2026-07-05 10:00:00", 50.0)
        result = er.guarded_pnl_for_date(self.conn, "2026-07-06")
        self.assertEqual(result["fleet_total"], 0.0)

    def test_excludes_non_sell_action(self):
        self._insert("qwen3-8b-flash", "2026-07-06 10:00:00", 50.0, action="BUY")
        result = er.guarded_pnl_for_date(self.conn, "2026-07-06")
        self.assertEqual(result["fleet_total"], 0.0)


class GenuineErrorCountTests(unittest.TestCase):
    def setUp(self):
        fd, self.log_path = tempfile.mkstemp(suffix=".log")
        os.close(fd)

    def tearDown(self):
        os.remove(self.log_path)

    def _write(self, lines):
        with open(self.log_path, "w") as f:
            f.write("\n".join(lines) + "\n")

    def test_counts_real_errors(self):
        self._write([
            "[2026-07-06 10:00:00] ERROR something broke",
            "[2026-07-06 10:00:01] normal log line",
        ])
        self.assertEqual(er.genuine_error_count([self.log_path], "2026-07-06"), 1)

    def test_excludes_known_false_positive(self):
        self._write([
            "[2026-07-06 10:00:00] [Kirk] advisory complete: 0 positions, 0 critical, 0 high, 0 medium, 0 low; cash=$100",
        ])
        self.assertEqual(er.genuine_error_count([self.log_path], "2026-07-06"), 0)

    def test_excludes_other_dates(self):
        self._write(["[2026-07-05 10:00:00] ERROR wrong day"])
        self.assertEqual(er.genuine_error_count([self.log_path], "2026-07-06"), 0)

    def test_missing_file_does_not_raise(self):
        self.assertEqual(er.genuine_error_count(["/nonexistent/file.log"], "2026-07-06"), 0)

    def test_catches_critical_and_fatal_too(self):
        self._write([
            "[2026-07-06 10:00:00] CRITICAL disk full",
            "[2026-07-06 10:00:01] FATAL crash",
        ])
        self.assertEqual(er.genuine_error_count([self.log_path], "2026-07-06"), 2)


class PriorDayDeltaTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def _persist(self, report_date, guarded_pnl):
        self.conn.execute(
            "INSERT INTO eod_report_log (report_date, guarded_pnl) VALUES (?, ?)",
            (report_date, guarded_pnl),
        )
        self.conn.commit()

    def test_no_prior_row_returns_none(self):
        self.assertIsNone(er.prior_day_delta(self.conn, "2026-07-06"))

    def test_matching_figure_returns_none(self):
        self._persist("2026-07-05", 100.0)
        self.conn.execute(
            "INSERT INTO trades (player_id, action, executed_at, realized_pnl) VALUES (?, 'SELL', ?, ?)",
            ("qwen3-8b-flash", "2026-07-05 10:00:00", 100.0),
        )
        self.conn.commit()
        self.assertIsNone(er.prior_day_delta(self.conn, "2026-07-06"))

    def test_revised_figure_produces_delta_string(self):
        self._persist("2026-07-05", 100.0)
        # Fresh recompute finds an extra trade that wasn't there at 2 PM.
        self.conn.execute(
            "INSERT INTO trades (player_id, action, executed_at, realized_pnl) VALUES (?, 'SELL', ?, ?)",
            ("qwen3-8b-flash", "2026-07-05 20:00:00", 150.0),
        )
        self.conn.commit()
        delta = er.prior_day_delta(self.conn, "2026-07-06")
        self.assertIsNotNone(delta)
        self.assertIn("150.00", delta)
        self.assertIn("100.00", delta)

    def test_skips_weekend_to_find_most_recent_row(self):
        self._persist("2026-07-03", 50.0)  # Friday
        self.conn.execute(
            "INSERT INTO trades (player_id, action, executed_at, realized_pnl) VALUES (?, 'SELL', ?, ?)",
            ("qwen3-8b-flash", "2026-07-03 10:00:00", 50.0),
        )
        self.conn.commit()
        # Monday's report (2026-07-06) should compare against Friday (matches,
        # no delta), not assume a nonexistent Saturday/Sunday row (which would
        # recompute to $0 and falsely report a discrepancy).
        self.assertIsNone(er.prior_day_delta(self.conn, "2026-07-06"))


class DryRunSmokeTest(unittest.TestCase):
    """Runs the actual script (--dry-run) against the REAL trader.db to
    confirm it executes end-to-end with no import errors and touches
    neither ntfy nor eod_report_log. This is the only test in this file
    that reads the real DB -- --dry-run guarantees zero writes."""

    def test_dry_run_against_real_db(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "eod_report.py"), "--dry-run"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("DRY RUN", result.stdout)


if __name__ == "__main__":
    unittest.main()
