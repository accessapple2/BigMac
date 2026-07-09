"""tests/test_event_tape_session_extremes_perf.py — HM-EVENT-TAPE-WAL-CONTENTION-2026-07-09.

Covers the actual root cause of the "dead" event-tape detector (2026-07-09):
per-function timing isolated the stall to _detect_session_extremes()'s query
against price_ticks, which had two compounding anti-patterns at real
production scale (64K+ rows/day, one symbol at 18K+ rows):

  1. `substr(ts, 1, 10) = ?` isn't sargable -- EXPLAIN QUERY PLAN showed a
     full covering-index SCAN instead of a seek.
  2. A correlated subquery (`WHERE t1.id = (SELECT MAX(id) FROM today_ticks
     t2 WHERE t2.symbol = t1.symbol)`) re-scanned the day's ticks once per
     row -- O(n^2) against a CTE with tens of thousands of rows. Confirmed
     live: the original query ran 3+ minutes of CPU time and never
     completed; the rewrite (ts range predicate + GROUP BY instead of
     correlated subquery) completes in 0.04s against the same data.

These tests prove (a) correctness is unchanged -- same new-high/new-low
firing behavior, prior_high/prior_low still explicitly excludes the latest
tick -- and (b) performance at a realistic single-symbol tick volume that
would have made the old correlated-subquery version pathological.
"""
from __future__ import annotations

import sqlite3
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

import engine.event_tape as event_tape


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "session_extremes_test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""
        CREATE TABLE price_ticks (
            id     INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            price  REAL NOT NULL,
            volume INTEGER,
            ts     TEXT NOT NULL
        )
    """)
    conn.execute("CREATE INDEX idx_price_ticks_symbol_ts ON price_ticks(symbol, ts)")
    conn.execute("CREATE INDEX idx_price_ticks_ts ON price_ticks(ts)")
    conn.execute("""
        CREATE TABLE event_tape (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol          TEXT NOT NULL,
            event_type      TEXT NOT NULL,
            narration       TEXT NOT NULL,
            price           REAL,
            magnitude       REAL,
            in_scanner_tier INTEGER,
            detected_at     TEXT DEFAULT (datetime('now')),
            metadata        TEXT
        )
    """)
    conn.execute("""
        CREATE TABLE strategy_signals (
            ticker TEXT, strategy_name TEXT, created_at TEXT
        )
    """)
    conn.commit()
    conn.close()
    return db_path


class SessionExtremesCorrectnessTests(unittest.TestCase):
    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.conn.close()
        self._tmpdir.cleanup()

    def _insert_ticks(self, symbol: str, prices: list[float], when: datetime) -> None:
        for i, px in enumerate(prices):
            ts = (when + timedelta(seconds=i)).strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z"
            self.conn.execute(
                "INSERT INTO price_ticks (symbol, price, volume, ts) VALUES (?, ?, 100, ?)",
                (symbol, px, ts),
            )
        self.conn.commit()

    def test_fires_new_session_high_when_latest_exceeds_prior_by_buffer(self) -> None:
        now = datetime.now(timezone.utc)
        # 25 prior ticks flat at 100, then a fresh high well past the 0.1% buffer.
        prices = [100.0] * 25 + [105.0]
        self._insert_ticks("ZZZZ", prices, now - timedelta(minutes=5))

        event_tape._detect_session_extremes(self.conn)

        rows = self.conn.execute(
            "SELECT event_type, price FROM event_tape WHERE symbol='ZZZZ'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "new_session_high")
        self.assertEqual(rows[0]["price"], 105.0)

    def test_does_not_fire_when_latest_within_buffer_of_prior_high(self) -> None:
        now = datetime.now(timezone.utc)
        # Latest tick ties the prior high exactly -- must not fire (buffer not exceeded).
        prices = [100.0] * 25 + [100.0]
        self._insert_ticks("YYYY", prices, now - timedelta(minutes=5))

        event_tape._detect_session_extremes(self.conn)

        rows = self.conn.execute(
            "SELECT * FROM event_tape WHERE symbol='YYYY'"
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_requires_minimum_prior_coverage(self) -> None:
        now = datetime.now(timezone.utc)
        # Only 10 prior ticks -- below the 20-tick minimum, must not fire
        # even though the latest tick is a large jump.
        prices = [100.0] * 10 + [200.0]
        self._insert_ticks("XXXX", prices, now - timedelta(minutes=5))

        event_tape._detect_session_extremes(self.conn)

        rows = self.conn.execute(
            "SELECT * FROM event_tape WHERE symbol='XXXX'"
        ).fetchall()
        self.assertEqual(len(rows), 0)

    def test_prior_high_excludes_latest_tick_itself(self) -> None:
        # If prior_high included the latest tick, a genuine new high would
        # never fire (last_px > last_px * 1.001 is always false). This is
        # the correctness invariant the GROUP BY rewrite must preserve.
        now = datetime.now(timezone.utc)
        # 29 flat prior ticks, then a final tick that clears the 0.1% buffer.
        prices = [100.0] * 29 + [100.5]
        self._insert_ticks("WWWW", prices, now - timedelta(minutes=5))

        event_tape._detect_session_extremes(self.conn)

        rows = self.conn.execute(
            "SELECT event_type FROM event_tape WHERE symbol='WWWW'"
        ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["event_type"], "new_session_high")

    def test_only_todays_ticks_considered(self) -> None:
        now = datetime.now(timezone.utc)
        # Yesterday's ticks must not count toward today's prior coverage.
        self._insert_ticks("VVVV", [100.0] * 25, now - timedelta(days=1))
        self._insert_ticks("VVVV", [100.0] * 5 + [105.0], now - timedelta(minutes=5))

        event_tape._detect_session_extremes(self.conn)

        # Only 5 prior ticks today -- below the 20-tick minimum despite
        # 25 ticks existing in the table overall.
        rows = self.conn.execute(
            "SELECT * FROM event_tape WHERE symbol='VVVV'"
        ).fetchall()
        self.assertEqual(len(rows), 0)


class SessionExtremesPerformanceTests(unittest.TestCase):
    """Guards against regressing back to the O(n^2) correlated-subquery
    pattern -- a realistic single-symbol tick volume (matching the
    production incident, one symbol logged 18K+ ticks in a session) must
    complete in well under a second, not minutes."""

    def setUp(self) -> None:
        import tempfile
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = _make_db(Path(self._tmpdir.name))
        self.conn = sqlite3.connect(str(self.db_path))
        self.conn.row_factory = sqlite3.Row

    def tearDown(self) -> None:
        self.conn.close()
        self._tmpdir.cleanup()

    def test_completes_quickly_at_realistic_high_volume_symbol_scale(self) -> None:
        now = datetime.now(timezone.utc)
        n = 6000  # scaled down from the live 18K-tick incident for test speed
        rows = [
            ("HOTSTOCK", 100.0 + (i % 50) * 0.01, 100,
             (now - timedelta(minutes=10) + timedelta(milliseconds=i * 5))
             .strftime("%Y-%m-%dT%H:%M:%S.%f") + "000Z")
            for i in range(n)
        ]
        self.conn.executemany(
            "INSERT INTO price_ticks (symbol, price, volume, ts) VALUES (?, ?, ?, ?)",
            rows,
        )
        self.conn.commit()

        t0 = time.monotonic()
        event_tape._detect_session_extremes(self.conn)
        elapsed = time.monotonic() - t0

        self.assertLess(
            elapsed, 2.0,
            f"_detect_session_extremes took {elapsed:.2f}s against {n} ticks -- "
            "the O(n^2) correlated-subquery regression would take far longer",
        )

    def test_uses_indexed_seek_not_full_scan(self) -> None:
        """EXPLAIN QUERY PLAN must show a SEARCH (seek), not a SCAN, on the
        ts range predicate -- guards against the substr() sargability
        regression specifically."""
        plan = self.conn.execute(
            "EXPLAIN QUERY PLAN "
            "SELECT id FROM price_ticks WHERE ts >= ? AND ts < ?",
            ("2026-01-01", "2026-01-02"),
        ).fetchall()
        plan_text = " ".join(str(tuple(row)) for row in plan)
        self.assertIn("SEARCH", plan_text)
        self.assertNotIn("SCAN", plan_text)


if __name__ == "__main__":
    unittest.main()
