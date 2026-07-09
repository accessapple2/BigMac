"""tests/test_bench_entry_gate.py — HM-BENCH-ENTRY-GATE-2026-07-09.

Covers a product decision from the 2026-07-09 session: BENCH (rating D/E,
per engine.agent_ratings.lineup_advisor's threshold) was advisory-only --
displayed on the Fleet Report Card but never enforced, so a benched agent
could still open new positions freely. Live incident: Dax (Lt. Jadzia Dax,
crew_role='advisory', halt_mode='active') opened an XLE position the same
day her rating showed BENCH.

Directive: BENCH blocks NEW entries only. Exits, stops, and risk-reducing
actions must always be allowed. halt_mode stays fully independent (this
reads agent_ratings, never writes ai_players.halt_mode). Every blocked
entry is logged with the grade that blocked it.

These tests cover _bench_block_reason() directly (the pure gate-decision
logic) against an isolated temp DB, and verify by source inspection that
sell()/sell_partial() -- the exit paths -- never call it.
"""
from __future__ import annotations

import inspect
import sqlite3
import unittest
from pathlib import Path
from unittest.mock import patch

import engine.paper_trader as paper_trader


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "bench_gate_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE agent_ratings (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            player_id TEXT, timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
            period TEXT, rating TEXT, rating_score REAL
        )
    """)
    conn.commit()
    conn.close()
    return db_path


class BenchBlockReasonTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self.conn = sqlite3.connect(str(self.db_path))

    def tearDown(self) -> None:
        self.conn.close()
        self._tmpdir.cleanup()

    def _insert_rating(self, player_id, rating, score, period="alltime"):
        self.conn.execute(
            "INSERT INTO agent_ratings (player_id, period, rating, rating_score) "
            "VALUES (?, ?, ?, ?)",
            (player_id, period, rating, score),
        )
        self.conn.commit()

    def _call(self, player_id):
        with patch.object(paper_trader, "_conn", return_value=sqlite3.connect(str(self.db_path))):
            return paper_trader._bench_block_reason(player_id)

    def test_rating_d_blocks(self) -> None:
        self._insert_rating("dax", "D", 45.0)
        reason = self._call("dax")
        self.assertIsNotNone(reason)
        self.assertIn("D", reason)

    def test_rating_e_blocks(self) -> None:
        self._insert_rating("mccoy", "E", 20.0)
        reason = self._call("mccoy")
        self.assertIsNotNone(reason)

    def test_rating_c_does_not_block(self) -> None:
        self._insert_rating("worf", "C", 60.0)
        self.assertIsNone(self._call("worf"))

    def test_rating_b_does_not_block(self) -> None:
        self._insert_rating("spock", "B", 75.0)
        self.assertIsNone(self._call("spock"))

    def test_rating_a_does_not_block(self) -> None:
        self._insert_rating("data", "A", 92.0)
        self.assertIsNone(self._call("data"))

    def test_never_rated_player_fails_open(self) -> None:
        """No agent_ratings row at all -- must not block (fail open, same
        posture as every other gate in this module)."""
        self.assertIsNone(self._call("brand-new-agent"))

    def test_most_recent_rating_wins_over_stale_older_one(self) -> None:
        """An agent that WAS BENCH but has since recovered must not stay
        blocked forever -- the most recent snapshot governs."""
        self.conn.execute(
            "INSERT INTO agent_ratings (player_id, period, rating, rating_score, timestamp) "
            "VALUES ('dax', 'alltime', 'D', 45.0, '2026-07-01 00:00:00')"
        )
        self.conn.execute(
            "INSERT INTO agent_ratings (player_id, period, rating, rating_score, timestamp) "
            "VALUES ('dax', 'alltime', 'B', 75.0, '2026-07-09 00:00:00')"
        )
        self.conn.commit()
        self.assertIsNone(self._call("dax"))

    def test_weekly_period_row_does_not_affect_alltime_gate(self) -> None:
        """The gate is explicitly scoped to period='alltime' -- a bad
        weekly-period rating must not block entries."""
        self._insert_rating("chekov", "E", 15.0, period="weekly")
        self.assertIsNone(self._call("chekov"))


class ExitPathsNeverGatedTests(unittest.TestCase):
    """Source-inspection guard: the BENCH gate must exist only in the
    entry-opening functions (buy, short_sell), never in the exit paths
    (sell, sell_partial) -- this is the asymmetry the directive requires."""

    def test_buy_calls_bench_gate(self) -> None:
        src = inspect.getsource(paper_trader.buy)
        self.assertIn("_bench_block_reason", src)

    def test_short_sell_calls_bench_gate(self) -> None:
        src = inspect.getsource(paper_trader.short_sell)
        self.assertIn("_bench_block_reason", src)

    def test_sell_never_calls_bench_gate(self) -> None:
        src = inspect.getsource(paper_trader.sell)
        self.assertNotIn("_bench_block_reason", src)

    def test_sell_partial_never_calls_bench_gate(self) -> None:
        src = inspect.getsource(paper_trader.sell_partial)
        self.assertNotIn("_bench_block_reason", src)


if __name__ == "__main__":
    unittest.main()
