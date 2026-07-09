"""tests/test_riker_synthesis_lock_retry.py — HM-RIKER-LOCK-RETRY-2026-07-09.

Covers a recurring live incident: engine/riker_synthesis.py::_save_synthesis()
threw an uncaught `sqlite3.OperationalError: database is locked` during the
market-open write burst, killing the whole 10-minute cron cycle after the read
phase had already succeeded -- identical signature on 2026-07-07 and
2026-07-09 (06:40/06:50 cycles silently skipped both days). The module also
used raw sqlite3.connect() throughout instead of the shared hardened
engine.db_conn.get_conn() helper (synchronous=NORMAL) used elsewhere in the
codebase for this exact contention class.

These tests prove: (1) a transient lock error is retried and eventually
succeeds without raising, (2) persistent lock failure still raises after
exhausting retries (not silently swallowed), and (3) run_synthesis() itself
survives persistent failure and still runs the alert-check step -- the
higher-value side effect of a synthesis cycle -- rather than crashing the
whole cron invocation as it did on both incident dates.
"""
from __future__ import annotations

import sqlite3
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import engine.riker_synthesis as riker_synthesis


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "riker_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE rikers_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_type TEXT, source TEXT, title TEXT,
            content TEXT, conviction REAL
        )
    """)
    conn.commit()
    conn.close()
    return db_path


_SYNTHESIS = {
    "summary": {
        "total_signals": 5, "high_conf_signals": 2,
        "trades_executed": 1, "open_positions": 3, "fleet_agents": 2,
    },
    "convergence": [],
}


class SaveSynthesisRetryTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self._orig_backoff = riker_synthesis._SAVE_RETRY_BACKOFF_SECS
        riker_synthesis._SAVE_RETRY_BACKOFF_SECS = 0.0  # don't slow the test suite down

    def tearDown(self) -> None:
        self._tmpdir.cleanup()
        riker_synthesis._SAVE_RETRY_BACKOFF_SECS = self._orig_backoff

    def test_succeeds_on_first_try_with_no_contention(self) -> None:
        with patch.object(riker_synthesis, "DB_PATH", self.db_path):
            riker_synthesis._save_synthesis(_SYNTHESIS)

        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute("SELECT COUNT(*) FROM rikers_log").fetchone()
        conn.close()
        self.assertEqual(rows[0], 1)

    def test_retries_and_recovers_from_transient_lock_error(self) -> None:
        real_get_conn = riker_synthesis.get_conn
        call_count = {"n": 0}

        def flaky_get_conn(*args, **kwargs):
            call_count["n"] += 1
            if call_count["n"] < 3:
                conn = MagicMock()
                conn.execute.side_effect = sqlite3.OperationalError("database is locked")
                return conn
            return real_get_conn(*args, **kwargs)

        with patch.object(riker_synthesis, "DB_PATH", self.db_path), \
             patch.object(riker_synthesis, "get_conn", side_effect=flaky_get_conn):
            riker_synthesis._save_synthesis(_SYNTHESIS)  # must not raise

        self.assertEqual(call_count["n"], 3)
        conn = sqlite3.connect(str(self.db_path))
        rows = conn.execute("SELECT COUNT(*) FROM rikers_log").fetchone()
        conn.close()
        self.assertEqual(rows[0], 1)

    def test_raises_after_exhausting_retries_on_persistent_lock(self) -> None:
        def always_locked(*args, **kwargs):
            conn = MagicMock()
            conn.execute.side_effect = sqlite3.OperationalError("database is locked")
            return conn

        with patch.object(riker_synthesis, "DB_PATH", self.db_path), \
             patch.object(riker_synthesis, "get_conn", side_effect=always_locked):
            with self.assertRaises(sqlite3.OperationalError):
                riker_synthesis._save_synthesis(_SYNTHESIS)


class RunSynthesisSurvivesPersistFailureTests(unittest.TestCase):
    """Reproduces the actual incident shape: persistence fails, but the
    cron cycle must not crash outright -- alert-checking still runs."""

    def test_run_synthesis_does_not_crash_when_save_fails(self) -> None:
        fake_synthesis = dict(_SYNTHESIS, timestamp="2026-07-09 06:40:00", period_minutes=10)
        fake_synthesis["high_confidence"] = []
        fake_synthesis["agent_activity"] = {}
        fake_synthesis["recent_trades"] = []

        with patch.object(riker_synthesis, "generate_synthesis", return_value=fake_synthesis), \
             patch.object(riker_synthesis, "_save_synthesis",
                           side_effect=sqlite3.OperationalError("database is locked")), \
             patch.object(riker_synthesis, "_check_alerts", return_value=[]) as mock_alerts:
            result = riker_synthesis.run_synthesis()  # must not raise

        mock_alerts.assert_called_once()
        self.assertEqual(result, fake_synthesis)


if __name__ == "__main__":
    unittest.main()
