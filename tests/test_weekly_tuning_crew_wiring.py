"""HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT + HM-INCUMBENT-AUDITION-TRACKER wiring
tests, against the actual weekly_tuning_crew.py integration (not the
standalone engine.crew.audition_tracking module, which has its own full
test coverage in tests/test_incumbent_audition_tracker.py).

Wired in at the 2026-07-05 21:30 MST boundary (docs/XO_BACKLOG.md
HM-GATE-RESTART-HOLD) -- the scheduler's first call to this module already
fired at 21:30:00 with the pre-wiring code (confirmed via trader.log), so
this file's own first real import happens next Sunday, not tonight.
"""
from __future__ import annotations

import os
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import engine.crew.weekly_tuning_crew as wtc


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, is_human INTEGER DEFAULT 0, crew_role TEXT DEFAULT 'active',
        halt_mode TEXT DEFAULT 'active'
    )""")
    conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY, player_id TEXT, signal TEXT, created_at TEXT)")
    conn.execute("""CREATE TABLE trades (
        id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, executed_at TEXT,
        realized_pnl REAL, execution_type TEXT DEFAULT 'simulated', alpaca_order_id TEXT
    )""")
    conn.execute("""CREATE TABLE options_trades (
        id INTEGER PRIMARY KEY, book_tag TEXT DEFAULT 'fleet', agent_id TEXT,
        structure TEXT, status TEXT, entry_date TEXT, pnl REAL, broker_order_id TEXT
    )""")
    conn.execute("""CREATE TABLE csp_wheel_scan_log (
        id INTEGER PRIMARY KEY, scanned_at TEXT, outcome TEXT
    )""")
    conn.execute("""CREATE TABLE model_adjustments (
        id INTEGER PRIMARY KEY, player_id TEXT, adjustment_type TEXT, old_value TEXT,
        new_value TEXT, reason TEXT, source TEXT, effective_date TEXT
    )""")
    conn.commit()
    conn.close()


class RunAuditionsWiringTests(unittest.TestCase):
    """Confirms _run_auditions() actually calls into
    score_bench_candidate_from_real_trades() when signals=0, end to end."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def _add_candidate(self, pid, halt_mode="full", crew_role="active"):
        self.conn.execute(
            "INSERT INTO ai_players (id, is_human, crew_role, halt_mode) VALUES (?, 0, ?, ?)",
            (pid, crew_role, halt_mode),
        )
        self.conn.commit()

    def test_zero_signals_zero_real_activity_stays_insufficient_data(self):
        self._add_candidate("cto-grok42")
        result = wtc._run_auditions(self.conn)
        r = next(x for x in result["results"] if x["player_id"] == "cto-grok42")
        self.assertEqual(r["verdict"], "insufficient_data")
        self.assertNotIn("measured_via", r)

    def test_zero_signals_simulated_trades_stay_insufficient_data(self):
        # HM-EDGE-PROVENANCE: internal-sim trades don't count even though
        # they're real rows in the trades table.
        self._add_candidate("cto-grok42")
        for i in range(5):
            self.conn.execute(
                "INSERT INTO trades (player_id, action, executed_at, realized_pnl, "
                "execution_type, alpaca_order_id) VALUES (?, 'SELL', ?, ?, 'simulated', NULL)",
                ("cto-grok42", f"2026-06-{10+i:02d}", 10.0),
            )
        self.conn.commit()
        result = wtc._run_auditions(self.conn)
        r = next(x for x in result["results"] if x["player_id"] == "cto-grok42")
        self.assertEqual(r["verdict"], "insufficient_data")
        self.assertNotIn("measured_via", r)

    def test_zero_signals_broker_executed_trades_get_measured(self):
        self._add_candidate("cto-grok42")
        for i in range(3):
            self.conn.execute(
                "INSERT INTO trades (player_id, action, executed_at, realized_pnl, "
                "execution_type, alpaca_order_id) VALUES (?, 'SELL', ?, ?, 'alpaca_paper', ?)",
                ("cto-grok42", f"2026-06-{10+i:02d}", 10.0, f"order-{i}"),
            )
        self.conn.commit()
        result = wtc._run_auditions(self.conn)
        r = next(x for x in result["results"] if x["player_id"] == "cto-grok42")
        self.assertEqual(r["measured_via"], "broker_executed_trades_and_or_options_trades")
        self.assertEqual(r["real_trades_count"], 3)

    def test_signals_present_path_unaffected(self):
        """When signals > 0, the original backtest-replay path still runs --
        this wiring must not interfere with the pre-existing behavior."""
        self._add_candidate("some-candidate")
        for i in range(5):
            self.conn.execute(
                "INSERT INTO signals (player_id, signal, created_at) VALUES (?, 'BUY', ?)",
                ("some-candidate", f"2026-06-{10+i:02d}"),
            )
        self.conn.commit()
        with patch("engine.backtester.backtest_player", return_value={"stats": {}, "signals_tested": 0}):
            result = wtc._run_auditions(self.conn)
        r = next(x for x in result["results"] if x["player_id"] == "some-candidate")
        self.assertEqual(r["clean_signals_in_db"], 5)
        self.assertNotIn("measured_via", r)  # took the signals path, not the real-trades fallback


class RunWeeklyTuningIncumbentWiringTests(unittest.TestCase):
    """Confirms run_weekly_tuning()'s output includes incumbent_auditions,
    without invoking the full LLM-calling pipeline (agents 1-3)."""

    def test_incumbent_auditions_key_present_in_output_shape(self):
        # Full run_weekly_tuning() makes real Ollama/Gemini calls -- out of
        # scope for a unit test. Verify the wiring source directly instead:
        # the function must reference track_incumbent_auditions and return
        # it under "incumbent_auditions".
        import inspect
        src = inspect.getsource(wtc.run_weekly_tuning)
        self.assertIn("track_incumbent_auditions", src)
        self.assertIn('"incumbent_auditions"', src)


class OllamaLoudFailureTests(unittest.TestCase):
    """HM-TUNING-CREW-REPAIR-2026-07-06: _ollama() must log loudly instead
    of silently returning "" on a bad or empty response -- the exact,
    reproduced root cause of tonight's 2026-07-05 21:30 zero-score run
    (verified live: the real Agent-1 prompt against real data parses fine
    most of the time, so this is an intermittent HTTP/empty-body failure,
    not a fundamentally broken prompt)."""

    @patch("engine.crew.weekly_tuning_crew.requests.post")
    def test_non_ok_response_logs_alert(self, mock_post):
        mock_post.return_value.ok = False
        mock_post.return_value.status_code = 503
        mock_post.return_value.text = "Service Unavailable"
        with patch.object(wtc.console, "log") as mock_log:
            result = wtc._ollama("test prompt")
        self.assertEqual(result, "")
        logged = " ".join(str(c) for c in mock_log.call_args_list)
        self.assertIn("ALERT", logged)
        self.assertIn("503", logged)

    @patch("engine.crew.weekly_tuning_crew.requests.post")
    def test_empty_response_body_logs_alert(self, mock_post):
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"response": ""}
        with patch.object(wtc.console, "log") as mock_log:
            result = wtc._ollama("test prompt")
        self.assertEqual(result, "")
        logged = " ".join(str(c) for c in mock_log.call_args_list)
        self.assertIn("ALERT", logged)
        self.assertIn("empty response", logged)

    @patch("engine.crew.weekly_tuning_crew.requests.post")
    def test_normal_response_does_not_alert(self, mock_post):
        mock_post.return_value.ok = True
        mock_post.return_value.status_code = 200
        mock_post.return_value.json.return_value = {"response": "real content"}
        with patch.object(wtc.console, "log") as mock_log:
            result = wtc._ollama("test prompt")
        self.assertEqual(result, "real content")
        logged = " ".join(str(c) for c in mock_log.call_args_list)
        self.assertNotIn("ALERT", logged)


class ZeroScoreZeroAdjustmentGuardTests(unittest.TestCase):
    """Integration test of the actual guard logic inside run_weekly_tuning():
    when Ollama/Gemini return unusable output despite real trade activity to
    score, a loud alert must fire -- not just the routine "Scored 0 models"
    info line that gave zero trace of why on the real 2026-07-05 21:30 run."""

    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        conn = sqlite3.connect(self.db_path)
        conn.execute("""CREATE TABLE ai_players (
            id TEXT PRIMARY KEY, display_name TEXT, is_active INTEGER DEFAULT 1,
            is_paused INTEGER DEFAULT 0, provider TEXT, model_id TEXT
        )""")
        conn.execute("""CREATE TABLE daily_lessons (
            id INTEGER PRIMARY KEY, player_id TEXT, grade TEXT, symbol TEXT,
            pnl REAL, lesson TEXT, date TEXT
        )""")
        conn.execute("""CREATE TABLE trades (
            id INTEGER PRIMARY KEY, player_id TEXT, action TEXT, executed_at TEXT,
            realized_pnl REAL, execution_type TEXT DEFAULT 'simulated', alpaca_order_id TEXT
        )""")
        conn.execute("""CREATE TABLE model_scores (
            id INTEGER PRIMARY KEY, player_id TEXT, period TEXT, date TEXT,
            win_rate REAL, regime_alignment REAL, confidence_calibration REAL,
            overall_score REAL, spam_rate_pct REAL, data_window TEXT
        )""")
        conn.execute("""CREATE TABLE model_adjustments (
            id INTEGER PRIMARY KEY, player_id TEXT, adjustment_type TEXT, old_value TEXT,
            new_value TEXT, reason TEXT, source TEXT, effective_date TEXT
        )""")
        conn.execute("""CREATE TABLE signals (
            id INTEGER PRIMARY KEY, player_id TEXT, signal TEXT, created_at TEXT
        )""")
        conn.execute("""CREATE TABLE options_trades (
            id INTEGER PRIMARY KEY, book_tag TEXT DEFAULT 'fleet', agent_id TEXT,
            structure TEXT, status TEXT, entry_date TEXT, pnl REAL, broker_order_id TEXT
        )""")
        conn.execute("""CREATE TABLE csp_wheel_scan_log (
            id INTEGER PRIMARY KEY, scanned_at TEXT, outcome TEXT
        )""")
        conn.execute(
            "INSERT INTO ai_players (id, display_name, is_active) VALUES ('qwen3-8b-flash', 'Worf', 1)"
        )
        conn.execute(
            "INSERT INTO trades (player_id, action, executed_at, realized_pnl) "
            "VALUES ('qwen3-8b-flash', 'SELL', datetime('now', '-1 day'), 10.0)"
        )
        conn.commit()
        conn.close()
        self._db_patch = patch.object(wtc, "DB", self.db_path)
        self._db_patch.start()

    def tearDown(self):
        self._db_patch.stop()
        os.remove(self.db_path)

    def test_agent1_zero_score_with_real_trades_alerts(self):
        with patch.object(wtc, "_ollama", return_value=""), \
             patch.object(wtc, "_gemini", return_value=""), \
             patch("engine.crew.weekly_tuning_crew._run_auditions",
                   return_value={"candidates_scored": 0, "pass": 0, "fail": 0, "insufficient_data": 0, "results": []}), \
             patch("engine.crew.audition_tracking.track_incumbent_auditions", return_value=[]), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            wtc.run_weekly_tuning()
        alert_types = [c.kwargs.get("alert_type") for c in mock_alert.call_args_list]
        self.assertIn("tuning_crew_zero_scored", alert_types)

    def test_no_trade_activity_does_not_false_alarm(self):
        # Empty trades table -- 0 scored is CORRECT here, not a failure.
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM trades")
        conn.commit()
        conn.close()
        with patch.object(wtc, "_ollama", return_value=""), \
             patch.object(wtc, "_gemini", return_value=""), \
             patch("engine.crew.weekly_tuning_crew._run_auditions",
                   return_value={"candidates_scored": 0, "pass": 0, "fail": 0, "insufficient_data": 0, "results": []}), \
             patch("engine.crew.audition_tracking.track_incumbent_auditions", return_value=[]), \
             patch("engine.alert_channels.send_alert") as mock_alert:
            wtc.run_weekly_tuning()
        alert_types = [c.kwargs.get("alert_type") for c in mock_alert.call_args_list]
        self.assertNotIn("tuning_crew_zero_scored", alert_types)


if __name__ == "__main__":
    unittest.main()
