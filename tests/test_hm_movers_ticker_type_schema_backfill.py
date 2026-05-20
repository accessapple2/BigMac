"""tests/test_hm_movers_ticker_type_schema_backfill.py — TDD coverage for
HM-MOVERS-TICKER-TYPE-SCHEMA+BACKFILL.

Builds a fresh fixture DB with the minimum schemas needed (mover_watchlist
without ticker_type + scan_universe with ticker_type), exercises the migrate()
function in scripts/hm_movers_ticker_type_schema_backfill.py, and asserts:

  1. ALTER TABLE adds ticker_type to an empty fixture DB
  2. Backfill populates rows where scan_universe match exists
  3. Backfill leaves rows NULL where no scan_universe match
  4. Idempotent re-run causes no destructive changes (column-exists guard)
  5. Existing mover_watchlist row data (symbol, last_price, etc.) preserved

Run from project root:
    venv/bin/python3 -m pytest tests/test_hm_movers_ticker_type_schema_backfill.py -v
"""
from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

# Import target module — sys.path is set above so scripts/ is reachable
from scripts.hm_movers_ticker_type_schema_backfill import (  # noqa: E402
    _column_exists,
    migrate,
)


def _build_fixture_db(db_path: Path) -> None:
    """Create the minimum schema + seed rows mirroring live shape."""
    conn = sqlite3.connect(str(db_path))
    try:
        # mover_watchlist — matches live shape EXCEPT ticker_type (the column being added).
        conn.execute(
            """
            CREATE TABLE mover_watchlist (
                symbol TEXT PRIMARY KEY,
                last_price REAL,
                pct_change REAL,
                volume INTEGER
            )
            """
        )
        # scan_universe — has ticker_type, source of truth.
        conn.execute(
            """
            CREATE TABLE scan_universe (
                id INTEGER PRIMARY KEY,
                symbol TEXT NOT NULL,
                ticker_type TEXT DEFAULT 'CS'
            )
            """
        )
        # Seed scan_universe with mixed CS + ETF entries.
        conn.executemany(
            "INSERT INTO scan_universe (symbol, ticker_type) VALUES (?, ?)",
            [
                ("AAPL", "CS"),
                ("MSFT", "CS"),
                ("SPY", "ETF"),
                ("QQQ", "ETF"),
                ("NVDA", "CS"),
            ],
        )
        # Seed mover_watchlist — 3 matching + 2 unmatched (warrants/fringe).
        conn.executemany(
            "INSERT INTO mover_watchlist (symbol, last_price, pct_change, volume) VALUES (?, ?, ?, ?)",
            [
                ("AAPL", 180.50, 1.5, 50_000_000),
                ("SPY",  450.10, 0.8, 80_000_000),
                ("NVDA", 950.00, 3.2, 30_000_000),
                ("ACHR.WS", 1.20, 5.0, 100_000),   # warrant, NOT in scan_universe
                ("AAOG",    0.50, 12.0, 50_000),   # fringe, NOT in scan_universe
            ],
        )
        conn.commit()
    finally:
        conn.close()


class HmMoversTickerTypeBackfillTests(unittest.TestCase):
    """5 cases per Captain Wave 3 spec."""

    def setUp(self) -> None:
        # Per-test fresh fixture DB in tmpfile.
        import tempfile

        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmpdir.name) / "trader_fixture.db"
        _build_fixture_db(self.db_path)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    # ── Test 1 ───────────────────────────────────────────────────────────
    def test_alter_adds_ticker_type_column(self) -> None:
        # Pre-state: column absent.
        with sqlite3.connect(str(self.db_path)) as conn:
            self.assertFalse(_column_exists(conn, "mover_watchlist", "ticker_type"))

        migrate(self.db_path, apply=True)

        # Post-state: column present.
        with sqlite3.connect(str(self.db_path)) as conn:
            self.assertTrue(_column_exists(conn, "mover_watchlist", "ticker_type"))

    # ── Test 2 ───────────────────────────────────────────────────────────
    def test_backfill_populates_matching_symbols(self) -> None:
        migrate(self.db_path, apply=True)

        with sqlite3.connect(str(self.db_path)) as conn:
            rows = dict(
                conn.execute(
                    "SELECT symbol, ticker_type FROM mover_watchlist WHERE symbol IN ('AAPL','SPY','NVDA')"
                ).fetchall()
            )
        self.assertEqual(rows.get("AAPL"), "CS")
        self.assertEqual(rows.get("SPY"), "ETF")
        self.assertEqual(rows.get("NVDA"), "CS")

    # ── Test 3 ───────────────────────────────────────────────────────────
    def test_backfill_leaves_unmatched_rows_null(self) -> None:
        migrate(self.db_path, apply=True)

        with sqlite3.connect(str(self.db_path)) as conn:
            rows = dict(
                conn.execute(
                    "SELECT symbol, ticker_type FROM mover_watchlist WHERE symbol IN ('ACHR.WS','AAOG')"
                ).fetchall()
            )
        self.assertIsNone(rows.get("ACHR.WS"))
        self.assertIsNone(rows.get("AAOG"))

    # ── Test 4 ───────────────────────────────────────────────────────────
    def test_idempotent_rerun(self) -> None:
        # First run.
        stats_first = migrate(self.db_path, apply=True)

        # Second run — must NOT raise (would raise OperationalError without
        # the column-exists guard) and must not alter the data.
        stats_second = migrate(self.db_path, apply=True)

        self.assertEqual(stats_first["total_rows"], stats_second["total_rows"])
        self.assertEqual(stats_first["still_null"], stats_second["still_null"])
        self.assertEqual(stats_first["not_null"], stats_second["not_null"])

    # ── Test 5 ───────────────────────────────────────────────────────────
    def test_existing_row_data_preserved(self) -> None:
        # Capture pre-state of all mover_watchlist columns.
        with sqlite3.connect(str(self.db_path)) as conn:
            pre = {r[0]: r for r in conn.execute(
                "SELECT symbol, last_price, pct_change, volume FROM mover_watchlist"
            ).fetchall()}

        migrate(self.db_path, apply=True)

        # Post-state: same rows, same non-ticker_type columns.
        with sqlite3.connect(str(self.db_path)) as conn:
            post = {r[0]: r for r in conn.execute(
                "SELECT symbol, last_price, pct_change, volume FROM mover_watchlist"
            ).fetchall()}

        self.assertEqual(pre.keys(), post.keys())
        for sym in pre:
            self.assertEqual(pre[sym], post[sym], msg=f"row data drifted for {sym}")


if __name__ == "__main__":
    unittest.main()
