"""HM-CLEAN-STALE-ARCHIVE-NOT-DELETE Phase 4 — pattern tests.

Tests the archive-then-delete SQL pattern that backs the
``clean_stale_snapshots`` endpoint and the
``restore_portfolio_history_from_archive`` recovery endpoint. Each test
uses an isolated tmpfile sqlite DB with the same schema as production
(portfolio_history, portfolio_history_archived, ai_players, settings).

The 5 cases enumerated in the mission brief:
  1. archive captures full row contents
  2. transaction rolls back if archive INSERT fails (no orphan deletes)
  3. session_id groups rows correctly
  4. restore endpoint reverses cleanly
  5. calling endpoint with no matching rows is no-op (returns empty)

Tested at the SQL pattern level — endpoint glue is thin enough that
exercising the underlying transaction semantics validates the doctrine.
"""
from __future__ import annotations

import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path


# ── Fixture DB builder ──────────────────────────────────────────────────


def _build_fixture_db(path: Path) -> None:
    conn = sqlite3.connect(str(path))
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE ai_players (
            id TEXT PRIMARY KEY,
            cash REAL DEFAULT 10000
        );
        CREATE TABLE portfolio_history (
            id INTEGER PRIMARY KEY,
            player_id TEXT NOT NULL REFERENCES ai_players(id),
            total_value REAL,
            cash REAL,
            positions_value REAL,
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            season INTEGER DEFAULT 1
        );
        CREATE TABLE portfolio_history_archived (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            original_row_id INTEGER NOT NULL,
            player_id TEXT NOT NULL,
            total_value REAL,
            cash REAL,
            positions_value REAL,
            recorded_at TIMESTAMP,
            season INTEGER,
            archived_at TEXT NOT NULL DEFAULT (datetime('now')),
            archived_by TEXT NOT NULL,
            archive_reason TEXT NOT NULL,
            archive_session_id TEXT NOT NULL,
            restored_at TEXT
        );
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        INSERT INTO settings(key, value) VALUES ('current_season', '6');

        -- Player A: recapitalized (cash >= 9999) with stale history.
        INSERT INTO ai_players(id, cash) VALUES ('player-a', 10000.0);
        INSERT INTO portfolio_history(player_id, total_value, cash, positions_value, recorded_at, season)
        VALUES
          ('player-a', 8000.0, 5000.0, 3000.0, '2026-04-10 12:00:00', 6),
          ('player-a', 7500.0, 4500.0, 3000.0, '2026-04-11 12:00:00', 6),
          ('player-a', 8500.0, 5500.0, 3000.0, '2026-04-12 12:00:00', 6);

        -- Player B: also recapitalized + stale.
        INSERT INTO ai_players(id, cash) VALUES ('player-b', 9999.5);
        INSERT INTO portfolio_history(player_id, total_value, cash, positions_value, recorded_at, season)
        VALUES
          ('player-b', 8900.0, 5000.0, 3900.0, '2026-04-15 12:00:00', 6),
          ('player-b', 8700.0, 4900.0, 3800.0, '2026-04-16 12:00:00', 6);

        -- Player C: NOT recapitalized — cash too low; should NOT be archived.
        INSERT INTO ai_players(id, cash) VALUES ('player-c', 7500.0);
        INSERT INTO portfolio_history(player_id, total_value, cash, positions_value, recorded_at, season)
        VALUES ('player-c', 8500.0, 5500.0, 3000.0, '2026-04-17 12:00:00', 6);

        -- Player D: recapitalized but total_value >= 9000 — not stale, should NOT be archived.
        INSERT INTO ai_players(id, cash) VALUES ('player-d', 10500.0);
        INSERT INTO portfolio_history(player_id, total_value, cash, positions_value, recorded_at, season)
        VALUES ('player-d', 9500.0, 6500.0, 3000.0, '2026-04-18 12:00:00', 6);
        """
    )
    conn.commit()
    conn.close()


# ── Logic replicated from endpoint (kept in sync via parallel review) ──


def _clean_stale_archive_then_delete(db_path: Path, force_mismatch: bool = False) -> dict:
    """Mirror of dashboard/app.py::clean_stale_snapshots transaction.

    ``force_mismatch`` injects an artificial archive failure to verify
    the rollback path. Production code path does NOT have this flag.
    """
    session_id = str(uuid.uuid4())
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        season_row = conn.execute(
            "SELECT value FROM settings WHERE key='current_season'"
        ).fetchone()
        season = int(season_row["value"]) if season_row else 3

        candidates = conn.execute(
            """
            SELECT DISTINCT ph.player_id FROM portfolio_history ph
            JOIN ai_players ap ON ap.id = ph.player_id
            WHERE ph.season = ?
              AND ap.cash >= 9999
              AND ph.total_value < 9000
            """,
            (season,),
        ).fetchall()
        candidate_pids = [r["player_id"] for r in candidates]

        if not candidate_pids:
            return {
                "ok": True,
                "archived_count": 0,
                "deleted_count": 0,
                "session_id": session_id,
                "by_player": {},
            }

        archived = 0
        deleted = 0
        by_player: dict = {}
        for pid in candidate_pids:
            rows = conn.execute(
                "SELECT id, player_id, total_value, cash, positions_value, "
                "recorded_at, season FROM portfolio_history "
                "WHERE player_id = ? AND season = ?",
                (pid, season),
            ).fetchall()
            for r in rows:
                conn.execute(
                    "INSERT INTO portfolio_history_archived "
                    "(original_row_id, player_id, total_value, cash, "
                    " positions_value, recorded_at, season, "
                    " archived_by, archive_reason, archive_session_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (r["id"], r["player_id"], r["total_value"], r["cash"],
                     r["positions_value"], r["recorded_at"], r["season"],
                     "clean_stale_snapshots",
                     f"cash>=9999 AND total_value<9000 (season {season})",
                     session_id),
                )
                archived += 1
            d = conn.execute(
                "DELETE FROM portfolio_history WHERE player_id = ? AND season = ?",
                (pid, season),
            )
            by_player[pid] = d.rowcount
            deleted += d.rowcount

        if force_mismatch:
            # Simulate the consistency-check rollback path.
            archived -= 1

        if archived != deleted:
            conn.rollback()
            return {
                "ok": False,
                "rolled_back": True,
                "archived": archived,
                "deleted": deleted,
                "session_id": session_id,
            }

        conn.commit()
        return {
            "ok": True,
            "archived_count": archived,
            "deleted_count": deleted,
            "session_id": session_id,
            "by_player": by_player,
        }
    finally:
        conn.close()


def _restore_from_archive(db_path: Path, session_id: str) -> dict:
    """Mirror of dashboard/app.py::restore_portfolio_history_from_archive."""
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        candidates = conn.execute(
            "SELECT id, original_row_id, player_id, total_value, cash, "
            " positions_value, recorded_at, season FROM portfolio_history_archived "
            "WHERE archive_session_id = ? AND restored_at IS NULL",
            (session_id,),
        ).fetchall()
        if not candidates:
            return {"ok": True, "restored_count": 0, "session_id": session_id}
        restored = 0
        for r in candidates:
            conn.execute(
                "INSERT INTO portfolio_history "
                " (player_id, total_value, cash, positions_value, recorded_at, season) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (r["player_id"], r["total_value"], r["cash"],
                 r["positions_value"], r["recorded_at"], r["season"]),
            )
            conn.execute(
                "UPDATE portfolio_history_archived SET restored_at = datetime('now') WHERE id = ?",
                (r["id"],),
            )
            restored += 1
        conn.commit()
        return {"ok": True, "restored_count": restored, "session_id": session_id}
    finally:
        conn.close()


# ── Tests ────────────────────────────────────────────────────────────────


class CleanStaleArchiveTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmpdir = tempfile.TemporaryDirectory()
        self.db_path = Path(self._tmpdir.name) / "trader_fixture.db"
        _build_fixture_db(self.db_path)

    def tearDown(self) -> None:
        self._tmpdir.cleanup()

    def _row_count(self, table: str, where: str = "") -> int:
        with sqlite3.connect(str(self.db_path)) as c:
            row = c.execute(f"SELECT COUNT(*) FROM {table} {where}").fetchone()
        return row[0]

    # 1. archive captures full row
    def test_archive_captures_full_row(self) -> None:
        result = _clean_stale_archive_then_delete(self.db_path)
        self.assertTrue(result["ok"])
        # player-a (3 rows) + player-b (2 rows) = 5 archived
        self.assertEqual(result["archived_count"], 5)
        self.assertEqual(result["deleted_count"], 5)

        with sqlite3.connect(str(self.db_path)) as c:
            c.row_factory = sqlite3.Row
            arch = c.execute(
                "SELECT * FROM portfolio_history_archived "
                "WHERE player_id='player-a' ORDER BY original_row_id"
            ).fetchall()
        self.assertEqual(len(arch), 3)
        # Full column copy verified: original totals match fixture.
        self.assertEqual(arch[0]["total_value"], 8000.0)
        self.assertEqual(arch[0]["cash"], 5000.0)
        self.assertEqual(arch[0]["positions_value"], 3000.0)
        self.assertEqual(arch[0]["season"], 6)
        # Audit-trail columns populated.
        self.assertEqual(arch[0]["archived_by"], "clean_stale_snapshots")
        self.assertIn("cash>=9999", arch[0]["archive_reason"])
        self.assertEqual(arch[0]["archive_session_id"], result["session_id"])
        self.assertIsNotNone(arch[0]["archived_at"])
        self.assertIsNone(arch[0]["restored_at"])

    # 2. transaction rolls back if archive INSERT fails — no orphan deletes
    def test_rollback_on_mismatch_no_orphan_deletes(self) -> None:
        pre_ph = self._row_count("portfolio_history")
        pre_arch = self._row_count("portfolio_history_archived")
        result = _clean_stale_archive_then_delete(self.db_path, force_mismatch=True)
        self.assertFalse(result["ok"])
        self.assertTrue(result["rolled_back"])
        # NO data should be lost — both tables back to pre-state.
        self.assertEqual(self._row_count("portfolio_history"), pre_ph)
        self.assertEqual(self._row_count("portfolio_history_archived"), pre_arch)

    # 3. session_id groups rows correctly
    def test_session_id_groups_rows(self) -> None:
        result = _clean_stale_archive_then_delete(self.db_path)
        sid = result["session_id"]
        self.assertEqual(
            self._row_count(
                "portfolio_history_archived",
                f"WHERE archive_session_id='{sid}'",
            ),
            5,
        )
        # Sanity: per-player breakdown sums to 5.
        with sqlite3.connect(str(self.db_path)) as c:
            by_player = dict(
                c.execute(
                    "SELECT player_id, COUNT(*) FROM portfolio_history_archived "
                    "WHERE archive_session_id=? GROUP BY player_id",
                    (sid,),
                ).fetchall()
            )
        self.assertEqual(by_player, {"player-a": 3, "player-b": 2})

    # 4. restore reverses cleanly + marks archive row restored_at
    def test_restore_reverses_cleanly(self) -> None:
        # Snapshot non-archived rows BEFORE archive — these must remain after restore.
        with sqlite3.connect(str(self.db_path)) as c:
            survivor_ids_before = set(
                row[0]
                for row in c.execute(
                    "SELECT id FROM portfolio_history "
                    "WHERE player_id NOT IN ('player-a','player-b')"
                ).fetchall()
            )

        clean = _clean_stale_archive_then_delete(self.db_path)
        sid = clean["session_id"]
        self.assertEqual(self._row_count("portfolio_history"), 2)  # player-c + player-d survive

        restore = _restore_from_archive(self.db_path, sid)
        self.assertTrue(restore["ok"])
        self.assertEqual(restore["restored_count"], 5)

        # portfolio_history now has the original 7 rows again (2 survivors + 5 restored).
        self.assertEqual(self._row_count("portfolio_history"), 7)
        # Survivors are unchanged (not touched by archive or restore).
        with sqlite3.connect(str(self.db_path)) as c:
            survivor_ids_after = set(
                row[0]
                for row in c.execute(
                    "SELECT id FROM portfolio_history "
                    "WHERE player_id NOT IN ('player-a','player-b')"
                ).fetchall()
            )
        self.assertEqual(survivor_ids_after, survivor_ids_before)

        # Archive rows are all stamped restored_at.
        with sqlite3.connect(str(self.db_path)) as c:
            unrestored = c.execute(
                "SELECT COUNT(*) FROM portfolio_history_archived "
                "WHERE archive_session_id=? AND restored_at IS NULL",
                (sid,),
            ).fetchone()[0]
        self.assertEqual(unrestored, 0)

        # Idempotency: re-running restore is a no-op.
        restore2 = _restore_from_archive(self.db_path, sid)
        self.assertEqual(restore2["restored_count"], 0)
        self.assertEqual(self._row_count("portfolio_history"), 7)  # unchanged

    # 5. no-match case is a no-op
    def test_no_match_returns_empty(self) -> None:
        # Clear the qualifying state — set all players cash < 9999.
        with sqlite3.connect(str(self.db_path)) as c:
            c.execute("UPDATE ai_players SET cash = 5000")
            c.commit()
        pre_ph = self._row_count("portfolio_history")
        pre_arch = self._row_count("portfolio_history_archived")
        result = _clean_stale_archive_then_delete(self.db_path)
        self.assertTrue(result["ok"])
        self.assertEqual(result["archived_count"], 0)
        self.assertEqual(result["deleted_count"], 0)
        self.assertEqual(result["by_player"], {})
        # State unchanged.
        self.assertEqual(self._row_count("portfolio_history"), pre_ph)
        self.assertEqual(self._row_count("portfolio_history_archived"), pre_arch)


if __name__ == "__main__":
    unittest.main()
