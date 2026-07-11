"""HM-WEEKLY-DIGEST-2026-07-11 tests. Isolated temp DB only -- never touches
data/trader.db (except the final real-DB dry-run smoke test, which passes
--dry-run and therefore performs zero writes and zero ntfy pushes)."""
from __future__ import annotations

import json
import os
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import scripts.weekly_digest as wd


def _make_test_db(path: str) -> None:
    conn = sqlite3.connect(path)
    conn.execute("CREATE TABLE ai_players (id TEXT PRIMARY KEY)")
    conn.execute("""CREATE TABLE model_scores (
        id INTEGER PRIMARY KEY, player_id TEXT, overall_score REAL,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.execute("""CREATE TABLE model_adjustments (
        id INTEGER PRIMARY KEY, player_id TEXT,
        created_at TEXT DEFAULT (datetime('now'))
    )""")
    conn.execute("""CREATE TABLE api_costs (
        id INTEGER PRIMARY KEY, player_id TEXT, cost_usd REAL,
        timestamp TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


class TuningResultsTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_no_recent_activity_reports_zero_not_error(self):
        result = wd.tuning_results(self.conn)
        self.assertEqual(result["models_scored"], 0)
        self.assertEqual(result["adjustments_saved"], 0)
        self.assertIsNone(result["avg_score"])

    def test_counts_recent_scores_and_adjustments(self):
        self.conn.execute(
            "INSERT INTO model_scores (player_id, overall_score, created_at) "
            "VALUES ('x', 50.0, datetime('now', '-1 day'))"
        )
        self.conn.execute(
            "INSERT INTO model_scores (player_id, overall_score, created_at) "
            "VALUES ('y', 40.0, datetime('now', '-2 days'))"
        )
        self.conn.execute(
            "INSERT INTO model_adjustments (player_id, created_at) "
            "VALUES ('x', datetime('now', '-1 day'))"
        )
        self.conn.commit()
        result = wd.tuning_results(self.conn)
        self.assertEqual(result["models_scored"], 2)
        self.assertEqual(result["avg_score"], 45.0)
        self.assertEqual(result["adjustments_saved"], 1)

    def test_excludes_stale_activity_outside_window(self):
        self.conn.execute(
            "INSERT INTO model_scores (player_id, overall_score, created_at) "
            "VALUES ('x', 50.0, datetime('now', '-30 days'))"
        )
        self.conn.commit()
        result = wd.tuning_results(self.conn)
        self.assertEqual(result["models_scored"], 0)


class SpendTests(unittest.TestCase):
    def setUp(self):
        fd, self.db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        _make_test_db(self.db_path)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.row_factory = sqlite3.Row

    def tearDown(self):
        self.conn.close()
        os.remove(self.db_path)

    def test_no_spend_reports_zero(self):
        result = wd.spend_30d(self.conn)
        self.assertEqual(result["total_30d"], 0)
        self.assertEqual(result["top"], [])
        self.assertEqual(result["off_roster"], [])

    def test_flags_off_roster_spend_even_when_small(self):
        """HM-SHADOW-PIPELINE-COST-AUDIT's whole finding was real spend
        hiding outside ai_players -- this is the regression guard for that
        exact blind spot."""
        self.conn.execute("INSERT INTO ai_players (id) VALUES ('roster-agent')")
        self.conn.execute(
            "INSERT INTO api_costs (player_id, cost_usd, timestamp) "
            "VALUES ('roster-agent', 1.50, datetime('now', '-1 day'))"
        )
        self.conn.execute(
            "INSERT INTO api_costs (player_id, cost_usd, timestamp) "
            "VALUES ('wr-shadow-ghost', 22.10, datetime('now', '-1 day'))"
        )
        self.conn.commit()
        result = wd.spend_30d(self.conn)
        self.assertEqual(result["total_30d"], 23.6)
        off_ids = [r["player_id"] for r in result["off_roster"]]
        self.assertEqual(off_ids, ["wr-shadow-ghost"])
        self.assertNotIn("roster-agent", off_ids)

    def test_excludes_spend_older_than_30_days(self):
        self.conn.execute(
            "INSERT INTO api_costs (player_id, cost_usd, timestamp) "
            "VALUES ('old-spender', 99.0, datetime('now', '-45 days'))"
        )
        self.conn.commit()
        result = wd.spend_30d(self.conn)
        self.assertEqual(result["total_30d"], 0)

    def test_zero_cost_rows_excluded_from_top(self):
        self.conn.execute(
            "INSERT INTO api_costs (player_id, cost_usd, timestamp) "
            "VALUES ('free-caller', 0.0, datetime('now', '-1 day'))"
        )
        self.conn.commit()
        result = wd.spend_30d(self.conn)
        self.assertEqual(result["top"], [])


class SweepSummaryTests(unittest.TestCase):
    def setUp(self):
        self.tmpdir = tempfile.mkdtemp()
        (Path(self.tmpdir) / "reports").mkdir()

    def _write_report(self, name: str, agents: list) -> None:
        path = Path(self.tmpdir) / "reports" / name
        path.write_text(json.dumps({"generated": "2026-07-11T00:00:00", "agents": agents}))

    def test_no_reports_returns_none(self):
        with patch("scripts.weekly_digest.ROOT", Path(self.tmpdir)):
            self.assertIsNone(wd.sweep_summary())

    def test_picks_latest_and_excludes_incomplete(self):
        import time
        self._write_report(
            "fleet_realism_sweep_clean_20260701_000000.json",
            [{"player_id": "old", "guarded": {"total_pnl": 1.0, "trades": 1}}],
        )
        time.sleep(0.01)
        self._write_report(
            "fleet_realism_sweep_clean_20260705_093932.INCOMPLETE_cron_syntax_test.json",
            [{"player_id": "bad", "guarded": {"total_pnl": 999.0, "trades": 1}}],
        )
        time.sleep(0.01)
        self._write_report(
            "fleet_realism_sweep_clean_20260704_213532.json",
            [
                {"player_id": "winner", "guarded": {"total_pnl": 100.0, "trades": 5}},
                {"player_id": "loser", "guarded": {"total_pnl": -20.0, "trades": 2}},
                {"player_id": "no-data-agent"},
            ],
        )
        with patch("scripts.weekly_digest.ROOT", Path(self.tmpdir)):
            result = wd.sweep_summary()
        self.assertEqual(result["agents_scored"], 2)
        self.assertEqual(result["agents_no_data"], 1)
        self.assertEqual(result["total_guarded_pnl"], 80.0)
        self.assertEqual(result["total_guarded_trades"], 7)
        self.assertEqual(result["top"], "winner")
        self.assertEqual(result["bottom"], "loser")


class FormatDigestTests(unittest.TestCase):
    def test_handles_missing_sweep_gracefully(self):
        digest = {
            "week_of": "2026-07-11",
            "sweep": None,
            "tuning": {"models_scored": 0, "avg_score": None, "adjustments_saved": 0},
            "auditions": [],
            "spend": {"total_30d": 0, "top": [], "off_roster": []},
        }
        body = wd.format_digest(digest)
        self.assertIn("no report found", body)

    def test_marks_suspended_auditions_distinctly(self):
        digest = {
            "week_of": "2026-07-11",
            "sweep": None,
            "tuning": {"models_scored": 0, "avg_score": None, "adjustments_saved": 0},
            "auditions": [
                {"player_id": "opts-agent", "suspended": True,
                 "clean_guarded_trades": 0, "target": 20},
                {"player_id": "active-agent", "suspended": False,
                 "clean_guarded_trades": 5, "target": 20, "days_remaining": 10},
            ],
            "spend": {"total_30d": 0, "top": [], "off_roster": []},
        }
        body = wd.format_digest(digest)
        self.assertIn("opts-agent: SUSPENDED", body)
        self.assertIn("active-agent: 5/20 (10d left)", body)


class DryRunSmokeTest(unittest.TestCase):
    """Runs the actual script (--dry-run) against the REAL trader.db to
    confirm it executes end-to-end with no import errors and fires no
    ntfy push. This is the only test in this file that reads the real
    DB -- --dry-run guarantees zero writes/pushes."""

    def test_dry_run_against_real_db(self):
        result = subprocess.run(
            [sys.executable, str(ROOT / "scripts" / "weekly_digest.py"), "--dry-run"],
            capture_output=True, text=True, cwd=str(ROOT), timeout=30,
        )
        self.assertEqual(result.returncode, 0, msg=result.stderr)
        self.assertIn("DRY RUN", result.stdout)
        self.assertIn("Weekly Digest", result.stdout)


if __name__ == "__main__":
    unittest.main()
