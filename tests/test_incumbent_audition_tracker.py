"""HM-INCUMBENT-AUDITION-TRACKER-2026-07-05 + HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT
fix + HM-EDGE-PROVENANCE (2026-07-05) suspension update, against
engine.crew.audition_tracking (new module, imported by nothing yet -- safe to
build/test independently of weekly_tuning_crew.py's lazy-import timing hold).

HM-EDGE-PROVENANCE ruling: both incumbent auditions are SUSPENDED -- counting
formula-priced/unrouted internal-sim trades measured a heuristic, not a
trader. Only broker-executed trades (real alpaca_order_id/broker_order_id)
count now, fleet-wide, not just for the two incumbents.

Isolated temp DB only -- never touches data/trader.db.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest

import engine.crew.audition_tracking as at


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, is_human INTEGER DEFAULT 0, crew_role TEXT DEFAULT 'active',
        halt_mode TEXT DEFAULT 'active'
    )""")
    conn.execute("""CREATE TABLE signals (
        id INTEGER PRIMARY KEY, player_id TEXT, signal TEXT, created_at TEXT
    )""")
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, executed_at TEXT,
        realized_pnl REAL, season INTEGER, execution_type TEXT DEFAULT 'simulated',
        alpaca_order_id TEXT
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, book_tag TEXT DEFAULT 'fleet', agent_id TEXT,
        structure TEXT, status TEXT, entry_date TEXT, pnl REAL, broker_order_id TEXT
    )""")
    conn.execute("""CREATE TABLE csp_wheel_scan_log (
        id INTEGER PRIMARY KEY, scanned_at TEXT, book_tag TEXT DEFAULT 'fleet',
        outcome TEXT, tickers_evaluated INTEGER DEFAULT 0, positions_opened INTEGER DEFAULT 0,
        total_notional REAL, options_cap_utilization_pct REAL, detail TEXT
    )""")
    conn.commit()
    conn.close()


def _insert_trade(path, player_id, executed_at, realized_pnl,
                   execution_type="simulated", alpaca_order_id=None):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO trades (player_id, action, executed_at, realized_pnl, season, "
        "execution_type, alpaca_order_id) VALUES (?, 'SELL', ?, ?, 1, ?, ?)",
        (player_id, executed_at, realized_pnl, execution_type, alpaca_order_id),
    )
    conn.commit()
    conn.close()


def _insert_options_trade(path, agent_id, entry_date, pnl, status="closed",
                           book_tag="fleet", broker_order_id=None):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO options_trades (book_tag, agent_id, structure, status, entry_date, pnl, "
        "broker_order_id) VALUES (?, ?, 'csp', ?, ?, ?, ?)",
        (book_tag, agent_id, status, entry_date, pnl, broker_order_id),
    )
    conn.commit()
    conn.close()


def _insert_scan_log(path, outcome, scanned_at):
    conn = sqlite3.connect(path)
    conn.execute(
        "INSERT INTO csp_wheel_scan_log (scanned_at, outcome) VALUES (?, ?)",
        (scanned_at, outcome),
    )
    conn.commit()
    conn.close()


class CleanOptionsTradeCountTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_counts_closed_since_floor_default_no_broker_requirement(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 100.0)
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-01", 50.0)  # before floor
        stats = at.clean_options_trade_count(self.conn, "options-sosnoff", "2026-07-04 09:34:26")
        self.assertEqual(stats["trade_count"], 1)
        self.assertEqual(stats["total_pnl"], 100.0)

    def test_require_broker_execution_excludes_internal_sim(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 100.0, broker_order_id=None)
        stats = at.clean_options_trade_count(
            self.conn, "options-sosnoff", "2026-07-04 09:34:26", require_broker_execution=True
        )
        self.assertEqual(stats["trade_count"], 0)

    def test_require_broker_execution_includes_real_fill(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 100.0, broker_order_id="abc123")
        stats = at.clean_options_trade_count(
            self.conn, "options-sosnoff", "2026-07-04 09:34:26", require_broker_execution=True
        )
        self.assertEqual(stats["trade_count"], 1)

    def test_excludes_open_positions(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 100.0, status="open")
        stats = at.clean_options_trade_count(self.conn, "options-sosnoff", "2026-07-04 09:34:26")
        self.assertEqual(stats["trade_count"], 0)

    def test_zero_trades_no_divide_by_zero(self):
        stats = at.clean_options_trade_count(self.conn, "options-sosnoff", "2026-07-04 09:34:26")
        self.assertEqual(stats["trade_count"], 0)
        self.assertIsNone(stats["win_rate"])

    def test_other_book_tag_excluded(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 100.0, book_tag="ghost")
        stats = at.clean_options_trade_count(self.conn, "options-sosnoff", "2026-07-04 09:34:26")
        self.assertEqual(stats["trade_count"], 0)


class BrokerExecutedTradeCountTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_excludes_simulated_trades(self):
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-06-01", 10.0,
                       execution_type="simulated", alpaca_order_id=None)
        stats = at.broker_executed_trade_count(self.conn, "qwen3-8b-flash", "2026-05-14")
        self.assertEqual(stats["trade_count"], 0)

    def test_includes_alpaca_paper_with_order_id(self):
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-06-01", 10.0,
                       execution_type="alpaca_paper", alpaca_order_id="order-1")
        stats = at.broker_executed_trade_count(self.conn, "qwen3-8b-flash", "2026-05-14")
        self.assertEqual(stats["trade_count"], 1)
        self.assertEqual(stats["total_pnl"], 10.0)

    def test_alpaca_paper_without_order_id_still_excluded(self):
        # execution_type says alpaca_paper but no order id -- require both.
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-06-01", 10.0,
                       execution_type="alpaca_paper", alpaca_order_id=None)
        stats = at.broker_executed_trade_count(self.conn, "qwen3-8b-flash", "2026-05-14")
        self.assertEqual(stats["trade_count"], 0)

    def test_before_floor_excluded(self):
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-05-01", 10.0,
                       execution_type="alpaca_paper", alpaca_order_id="order-1")
        stats = at.broker_executed_trade_count(self.conn, "qwen3-8b-flash", "2026-05-14")
        self.assertEqual(stats["trade_count"], 0)


class WheelScanDiagnosisTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_no_scans_reports_no_data(self):
        diag = at.wheel_scan_diagnosis(self.conn, "2026-07-04 09:34:26")
        self.assertTrue(diag["captured"])
        self.assertEqual(diag["total_scans"], 0)

    def test_cap_blocked_is_structural(self):
        _insert_scan_log(self.db_path, "cap_blocked", "2026-07-05 10:00:00")
        _insert_scan_log(self.db_path, "cap_blocked", "2026-07-06 10:00:00")
        diag = at.wheel_scan_diagnosis(self.conn, "2026-07-04 09:34:26")
        self.assertEqual(diag["cap_blocked"], 2)
        self.assertIn("cap gate", diag["diagnosis"])

    def test_only_mechanical_skips_is_structural_not_cap(self):
        _insert_scan_log(self.db_path, "vix_skip", "2026-07-05 10:00:00")
        _insert_scan_log(self.db_path, "max_positions_reached", "2026-07-06 10:00:00")
        diag = at.wheel_scan_diagnosis(self.conn, "2026-07-04 09:34:26")
        self.assertEqual(diag["cap_blocked"], 0)
        self.assertEqual(diag["mechanical_skips"], 2)
        self.assertIn("no scan reached evaluation", diag["diagnosis"])

    def test_evaluated_scans_are_not_labeled_structural(self):
        _insert_scan_log(self.db_path, "scan_completed", "2026-07-05 10:00:00")
        diag = at.wheel_scan_diagnosis(self.conn, "2026-07-04 09:34:26")
        self.assertEqual(diag["scan_completed"], 1)
        self.assertNotIn("structural_zero", diag["diagnosis"])

    def test_scans_before_floor_excluded(self):
        _insert_scan_log(self.db_path, "cap_blocked", "2026-07-01 10:00:00")  # before floor
        diag = at.wheel_scan_diagnosis(self.conn, "2026-07-04 09:34:26")
        self.assertEqual(diag["total_scans"], 0)

    def test_missing_table_reports_not_captured(self):
        conn2 = sqlite3.connect(":memory:")
        conn2.row_factory = sqlite3.Row
        conn2.execute("CREATE TABLE ai_players (id TEXT)")  # no csp_wheel_scan_log
        diag = at.wheel_scan_diagnosis(conn2, "2026-07-04 09:34:26")
        self.assertFalse(diag["captured"])


class TrackIncumbentAuditionsTests(unittest.TestCase):
    """HM-EDGE-PROVENANCE (2026-07-05): both incumbents are suspended --
    clean_guarded_trades now only counts broker-executed trades."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_reports_both_incumbents(self):
        results = at.track_incumbent_auditions(self.conn)
        ids = {r["player_id"] for r in results}
        self.assertEqual(ids, {"options-sosnoff", "qwen3-8b-flash"})

    def test_both_report_suspended(self):
        results = at.track_incumbent_auditions(self.conn)
        for r in results:
            self.assertTrue(r["suspended"])
            self.assertEqual(r["status"], "suspended_pending_broker_routing")
            self.assertIn("suspension_reason", r)

    def test_troi_internal_sim_csp_does_not_count(self):
        # Her real historical shape: internal-sim, no broker_order_id.
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 500.0, broker_order_id=None)
        results = at.track_incumbent_auditions(self.conn)
        troi = next(r for r in results if r["player_id"] == "options-sosnoff")
        self.assertEqual(troi["clean_guarded_trades"], 0)

    def test_troi_broker_executed_csp_would_count(self):
        _insert_options_trade(self.db_path, "options-sosnoff", "2026-07-10", 500.0, broker_order_id="real-order-1")
        results = at.track_incumbent_auditions(self.conn)
        troi = next(r for r in results if r["player_id"] == "options-sosnoff")
        self.assertEqual(troi["clean_guarded_trades"], 1)
        self.assertEqual(troi["total_pnl"], 500.0)

    def test_qwen_simulated_trade_does_not_count(self):
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-06-01", 10.0,
                       execution_type="simulated", alpaca_order_id=None)
        results = at.track_incumbent_auditions(self.conn)
        worf = next(r for r in results if r["player_id"] == "qwen3-8b-flash")
        self.assertEqual(worf["clean_guarded_trades"], 0)

    def test_qwen_broker_executed_trade_would_count(self):
        _insert_trade(self.db_path, "qwen3-8b-flash", "2026-06-01", 10.0,
                       execution_type="alpaca_paper", alpaca_order_id="real-order-2")
        results = at.track_incumbent_auditions(self.conn)
        worf = next(r for r in results if r["player_id"] == "qwen3-8b-flash")
        self.assertEqual(worf["clean_guarded_trades"], 1)

    def test_troi_includes_structural_diagnosis(self):
        results = at.track_incumbent_auditions(self.conn)
        troi = next(r for r in results if r["player_id"] == "options-sosnoff")
        self.assertIn("structural_diagnosis", troi)

    def test_qwen_has_no_structural_diagnosis(self):
        results = at.track_incumbent_auditions(self.conn)
        worf = next(r for r in results if r["player_id"] == "qwen3-8b-flash")
        self.assertNotIn("structural_diagnosis", worf)

    def test_no_floor_or_deadline_in_output_while_suspended(self):
        results = at.track_incumbent_auditions(self.conn)
        for r in results:
            self.assertNotIn("floor", r)
            self.assertNotIn("deadline", r)
            self.assertNotIn("days_remaining", r)


class ScoreBenchCandidateFromRealTradesTests(unittest.TestCase):
    """HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT fix, updated for HM-EDGE-PROVENANCE:
    now requires broker-execution evidence, not just any clean trade."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_no_real_activity_returns_none(self):
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertIsNone(result)

    def test_simulated_trades_alone_return_none(self):
        # HM-EDGE-PROVENANCE: internal-sim trades no longer count as "measured".
        for i in range(5):
            _insert_trade(self.db_path, "cto-grok42", f"2026-06-{10+i:02d}", 10.0,
                           execution_type="simulated", alpaca_order_id=None)
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertIsNone(result)

    def test_broker_executed_trades_get_measured(self):
        for i in range(3):
            _insert_trade(self.db_path, "cto-grok42", f"2026-06-{10+i:02d}", 10.0,
                           execution_type="alpaca_paper", alpaca_order_id=f"order-{i}")
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertIsNotNone(result)
        self.assertEqual(result["detail"]["measured_via"], "broker_executed_trades_and_or_options_trades")
        self.assertEqual(result["detail"]["real_trades_count"], 3)
        self.assertEqual(result["detail"]["real_total_pnl"], 30.0)

    def test_broker_executed_options_trades_get_measured(self):
        for i in range(3):
            _insert_options_trade(self.db_path, "some-csp-agent", f"2026-06-{10+i:02d}", 20.0,
                                   broker_order_id=f"order-{i}")
        result = at.score_bench_candidate_from_real_trades(self.conn, "some-csp-agent", "2026-05-14")
        self.assertIsNotNone(result)
        self.assertEqual(result["detail"]["real_options_trades_count"], 3)
        self.assertEqual(result["detail"]["real_total_pnl"], 60.0)

    def test_below_threshold_real_trades_still_insufficient_data(self):
        _insert_trade(self.db_path, "cto-grok42", "2026-06-10", 10.0,
                       execution_type="alpaca_paper", alpaca_order_id="order-1")  # only 1, needs 20
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertEqual(result["verdict"], "insufficient_data")
        self.assertEqual(result["detail"]["real_trades_count"], 1)

    def test_at_or_above_threshold_positive_pnl_passes(self):
        for i in range(20):
            _insert_trade(self.db_path, "cto-grok42", f"2026-06-{(i%28)+1:02d}", 5.0,
                           execution_type="alpaca_paper", alpaca_order_id=f"order-{i}")
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertEqual(result["verdict"], "pass")

    def test_at_or_above_threshold_negative_pnl_fails(self):
        for i in range(20):
            _insert_trade(self.db_path, "cto-grok42", f"2026-06-{(i%28)+1:02d}", -5.0,
                           execution_type="alpaca_paper", alpaca_order_id=f"order-{i}")
        result = at.score_bench_candidate_from_real_trades(self.conn, "cto-grok42", "2026-05-14")
        self.assertEqual(result["verdict"], "fail")


if __name__ == "__main__":
    unittest.main()
