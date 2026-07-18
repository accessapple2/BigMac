"""tests/test_season_rotation_reactivation_scope.py — HM-SEASON-ROTATION-
BLANKET-REACTIVATE regression tests.

Covers engine/season_manager.py's rotate_season()/start_season() unhalt
scope fix (2026-07-18): the season-reset UPDATE must only touch agents
with halt_reason IS NULL, and must abort (no writes) rather than
reactivate the fleet if the dry-run affected count exceeds the current
active count by more than ROTATION_REACTIVATION_MARGIN.

Root cause this guards against: the pre-fix UPDATE had no halt_reason
filter at all and blanket-reset halt_mode='active' for every ai_players
row except webull/alpaca-mirror/neo-matrix, silently reactivating 65
retired/halted/zombie agents when the automatic Sunday-night rotation
fired on 2026-07-12.

Uses a real temp-file SQLite DB (not :memory:) because season_manager's
_conn() sets PRAGMA journal_mode=WAL, which requires a file-backed DB,
and because the fix opens multiple separate connections (dry-run check,
then the write connection) that must share state.

Run from project root:

    .venv/bin/python3 -m pytest tests/test_season_rotation_reactivation_scope.py -v
"""

from __future__ import annotations

import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import engine.season_manager as season_manager  # noqa: E402

_SCHEMA_SQL = """
CREATE TABLE ai_players (
    id            TEXT PRIMARY KEY,
    display_name  TEXT,
    cash          REAL DEFAULT 7000.0,
    is_active     INTEGER DEFAULT 0,
    halt_reason   TEXT,
    halted_at     TIMESTAMP,
    halt_mode     TEXT DEFAULT 'active',
    season        INTEGER DEFAULT 1
);
CREATE TABLE settings (
    key   TEXT PRIMARY KEY,
    value TEXT
);
CREATE TABLE season_history (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    season            INTEGER NOT NULL,
    player_id         TEXT NOT NULL,
    display_name      TEXT,
    final_value       REAL,
    total_return_pct  REAL,
    total_trades      INTEGER DEFAULT 0,
    win_rate          REAL DEFAULT 0,
    ended_at          TEXT
);
CREATE TABLE positions (
    player_id TEXT,
    symbol    TEXT,
    qty       REAL,
    avg_price REAL,
    asset_type TEXT
);
CREATE TABLE trades (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    player_id    TEXT,
    symbol       TEXT,
    executed_at  TEXT,
    realized_pnl REAL,
    season       INTEGER
);
"""


def _seed(conn: sqlite3.Connection, rows: list[tuple]) -> None:
    conn.executemany(
        "INSERT INTO ai_players (id, halt_mode, halt_reason, halted_at) VALUES (?, ?, ?, ?)",
        rows,
    )
    conn.execute(
        "INSERT OR REPLACE INTO settings (key, value) VALUES ('current_season', '1')"
    )
    conn.commit()


class SeasonRotationReactivationScopeTests(unittest.TestCase):
    """Surface: engine/season_manager.py rotate_season() / start_season()."""

    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self._db_path = str(Path(self._tmpdir.name) / "test_trader.db")
        conn = sqlite3.connect(self._db_path)
        conn.executescript(_SCHEMA_SQL)
        conn.commit()
        conn.close()

        # Point season_manager at the temp DB for the duration of the test.
        self._orig_db = season_manager.DB
        season_manager.DB = self._db_path

    def tearDown(self) -> None:
        season_manager.DB = self._orig_db
        self._tmpdir.cleanup()

    def _conn(self) -> sqlite3.Connection:
        c = sqlite3.connect(self._db_path)
        c.row_factory = sqlite3.Row
        return c

    # ---- _dry_run_unhalt_scope() ------------------------------------------

    def test_scope_check_never_matches_a_row_with_halt_reason_set(self) -> None:
        """The eligible-rows WHERE clause must exclude every row with a
        non-NULL halt_reason, regardless of its halt_mode."""
        conn = self._conn()
        _seed(conn, [
            ("neo-matrix", "full", "excluded by id, irrelevant", None),
            ("webull", "full", "excluded by id, irrelevant", None),
            ("clean-active", "active", None, None),
            ("retired-agent", "full", "2026-05-05 retired via HM-T-fleet bundle", "2026-05-05"),
            ("roster-cap-agent", "full", "[2026-07-05] HM-ROSTER-CAP: MAX_ACTIVE_AGENTS=8", "2026-07-05"),
            ("bakeoff-clone", "full", "HM-BM bakeoff clone — audit trail only", "2026-05-16"),
            ("exit-only-agent", "exit_only", "HM-GUARDIAN-ADOPTION: exit-only stop guardian", "2026-06-12"),
        ])
        conn.commit()

        rows = conn.execute(
            f"SELECT id FROM ai_players WHERE {season_manager._UNHALT_ELIGIBLE_WHERE}",
            (season_manager.NEO_PLAYER_ID,),
        ).fetchall()
        touched_ids = {r["id"] for r in rows}
        self.assertEqual(touched_ids, {"clean-active"})
        conn.close()

    def test_scope_check_reports_safe_when_would_affect_matches_active_count(self) -> None:
        conn = self._conn()
        _seed(conn, [
            ("clean-active-1", "active", None, None),
            ("clean-active-2", "active", None, None),
            ("retired-agent", "full", "retired for cause", "2026-05-05"),
        ])
        conn.commit()

        scope = season_manager._dry_run_unhalt_scope(conn)
        self.assertEqual(scope["active_before"], 2)
        self.assertEqual(scope["would_affect"], 2)
        self.assertTrue(scope["safe"])
        conn.close()

    def test_scope_check_reports_unsafe_when_would_affect_blows_past_margin(self) -> None:
        """Reproduces the exact 2026-07-12 incident shape: 2 active, but
        many halt_reason-NULL halted rows exist (simulating a future data-
        quality regression where halt_reason gets cleared without setting
        the reason) — must be flagged unsafe, not silently reactivated."""
        conn = self._conn()
        rows = [("clean-active-1", "active", None, None), ("clean-active-2", "active", None, None)]
        # 20 halted agents with NULL halt_reason (the dangerous shape) —
        # comfortably past active_before(2) + margin(10) = 12.
        for i in range(20):
            rows.append((f"zombie-{i}", "full", None, None))
        _seed(conn, rows)
        conn.commit()

        scope = season_manager._dry_run_unhalt_scope(conn)
        self.assertEqual(scope["active_before"], 2)
        self.assertEqual(scope["would_affect"], 22)
        self.assertFalse(scope["safe"])
        conn.close()

    # ---- rotate_season() end-to-end ----------------------------------------

    def test_rotate_season_aborts_and_writes_nothing_when_unsafe(self) -> None:
        conn = self._conn()
        rows = [("clean-active-1", "active", None, None)]
        for i in range(30):
            rows.append((f"zombie-{i}", "full", None, None))
        _seed(conn, rows)
        conn.commit()
        conn.close()

        before = self._conn().execute(
            "SELECT id, halt_mode, cash, season FROM ai_players ORDER BY id"
        ).fetchall()

        result = season_manager.rotate_season()
        self.assertIsNone(result, "rotate_season() must return None on abort")

        after_conn = self._conn()
        after = after_conn.execute(
            "SELECT id, halt_mode, cash, season FROM ai_players ORDER BY id"
        ).fetchall()
        self.assertEqual(
            [tuple(r) for r in before], [tuple(r) for r in after],
            "aborted rotation must leave ai_players completely unchanged",
        )
        current_season = after_conn.execute(
            "SELECT value FROM settings WHERE key='current_season'"
        ).fetchone()["value"]
        self.assertEqual(current_season, "1", "season must not advance on abort")
        after_conn.close()

    def test_rotate_season_never_touches_halt_reason_rows_when_safe(self) -> None:
        conn = self._conn()
        _seed(conn, [
            ("clean-active", "active", None, None),
            ("retired-agent", "full", "2026-05-05 retired via HM-T-fleet bundle", "2026-05-05"),
            ("roster-cap-agent", "full", "[2026-07-05] HM-ROSTER-CAP: MAX_ACTIVE_AGENTS=8", "2026-07-05"),
            ("bakeoff-clone", "full", "HM-BM bakeoff clone — audit trail only", "2026-05-16"),
            ("exit-only-agent", "exit_only", "HM-GUARDIAN-ADOPTION: exit-only stop guardian", "2026-06-12"),
        ])
        conn.commit()
        conn.close()

        # engine.war_room.save_hot_take has its OWN hardcoded DB path
        # (data/trader.db, not season_manager.DB) — mock it so a "safe"
        # rotation in this test never touches the real production DB.
        with patch("engine.war_room.save_hot_take", return_value=True):
            result = season_manager.rotate_season()
        self.assertEqual(result, 2, "safe rotation must proceed and return the new season number")

        after_conn = self._conn()
        halted = after_conn.execute(
            "SELECT id, halt_mode, halt_reason FROM ai_players WHERE id != 'clean-active' ORDER BY id"
        ).fetchall()
        for r in halted:
            self.assertNotEqual(
                r["halt_mode"], "active",
                f"{r['id']} must remain halted — it has an explicit halt_reason on file",
            )
            self.assertIsNotNone(
                r["halt_reason"],
                f"{r['id']}'s halt_reason must be preserved, not cleared",
            )
        clean = after_conn.execute(
            "SELECT halt_mode FROM ai_players WHERE id='clean-active'"
        ).fetchone()
        self.assertEqual(clean["halt_mode"], "active")
        after_conn.close()

    # ---- start_season() mirrors rotate_season() ----------------------------

    def test_start_season_also_aborts_when_unsafe(self) -> None:
        conn = self._conn()
        rows = [("clean-active-1", "active", None, None)]
        for i in range(30):
            rows.append((f"zombie-{i}", "full", None, None))
        _seed(conn, rows)
        conn.commit()
        conn.close()

        result = season_manager.start_season(5)
        self.assertIn("error", result)
        self.assertIn("scope", result)

        after_conn = self._conn()
        current_season = after_conn.execute(
            "SELECT value FROM settings WHERE key='current_season'"
        ).fetchone()["value"]
        self.assertEqual(current_season, "1", "season must not advance on abort")
        after_conn.close()


if __name__ == "__main__":
    unittest.main()
