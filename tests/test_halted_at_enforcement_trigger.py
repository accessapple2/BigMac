"""tests/test_halted_at_enforcement_trigger.py — HM-HALTED-AT-ENFORCE regression tests.

Covers the SQL trigger enforcement at
``scripts/migrations/hm_halted_at_enforce.sql`` that auto-fills
``ai_players.halted_at`` to ``CURRENT_TIMESTAMP`` whenever ``halt_mode``
transitions away from ``'active'`` while ``halted_at`` is still NULL.

The trigger is the DB-side counterpart to the CLAUDE.md "Manual halt SQL
pattern" runbook. Per audit #6A, the runbook alone produced 3 NULL
``halted_at`` rows (chekov / super-agent / webull, backfilled by
HM-HALTED-AT-BACKFILL). The trigger closes the operator-forgetfulness gap
so future halts cannot land with a NULL timestamp.

Design choice — auto-fill (AFTER) over RAISE(ABORT) (BEFORE):

  - Auto-fill is non-breaking: the existing runbook UPDATE pattern with
    an explicit ``halted_at = CURRENT_TIMESTAMP`` continues to work; the
    trigger only kicks in when the column was left NULL.
  - The captured ``CURRENT_TIMESTAMP`` fires within the same tx as the
    UPDATE — wall-clock matches the operator's halt decision within
    sub-second resolution.
  - ABORT would force every ad-hoc SQL fragment to be re-pasted with the
    halted_at column on first attempt, which trades operator-friction for
    strictness without preventing the underlying mistake.

Trigger scope:

  - ``trg_ai_players_halted_at_on_update``: AFTER UPDATE OF halt_mode,
    halted_at on ai_players, WHEN NEW.halt_mode != 'active' AND
    NEW.halted_at IS NULL.
  - ``trg_ai_players_halted_at_on_insert``: AFTER INSERT on ai_players,
    WHEN NEW.halt_mode != 'active' AND NEW.halted_at IS NULL.

Both triggers update the row in place to set ``halted_at =
CURRENT_TIMESTAMP``. Unhalting (``halt_mode → 'active'``) does NOT touch
``halted_at`` — CLAUDE.md preserves the historical halt timestamp.

Run from project root:

    venv/bin/python3 -m pytest tests/test_halted_at_enforcement_trigger.py -v
"""

from __future__ import annotations

import sqlite3
import sys
import unittest
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

_MIGRATION_PATH = _PROJECT_ROOT / "scripts" / "migrations" / "hm_halted_at_enforce.sql"

# Minimal schema mirroring ai_players for the columns the trigger touches.
# Keeping it tight isolates the trigger behavior from unrelated columns.
_SCHEMA_SQL = """
CREATE TABLE ai_players (
    id          TEXT PRIMARY KEY,
    halt_mode   TEXT NOT NULL DEFAULT 'active'
                  CHECK (halt_mode IN ('active', 'exit_only', 'full')),
    halted_at   TIMESTAMP,
    halt_reason TEXT
);
"""


def _make_db_with_trigger() -> sqlite3.Connection:
    """In-memory DB seeded with the ai_players subset and the trigger DDL."""
    conn = sqlite3.connect(":memory:")
    conn.executescript(_SCHEMA_SQL)
    conn.executescript(_MIGRATION_PATH.read_text(encoding="utf-8"))
    conn.commit()
    return conn


class HaltedAtEnforcementTriggerTests(unittest.TestCase):
    """Surface: scripts/migrations/hm_halted_at_enforce.sql triggers."""

    def setUp(self) -> None:
        self.conn = _make_db_with_trigger()
        self.conn.execute(
            "INSERT INTO ai_players (id, halt_mode, halted_at, halt_reason) "
            "VALUES (?, 'active', NULL, NULL)",
            ("test-player",),
        )
        self.conn.commit()

    def tearDown(self) -> None:
        self.conn.close()

    # ---- Migration script self-checks --------------------------------------

    def test_0_migration_file_exists(self) -> None:
        self.assertTrue(
            _MIGRATION_PATH.exists(),
            f"Expected migration at {_MIGRATION_PATH}",
        )

    # ---- UPDATE-path enforcement -------------------------------------------

    def test_1_update_to_full_with_null_halted_at_autofills(self) -> None:
        """UPDATE halt_mode='full' with halted_at NULL → trigger fills halted_at."""
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'full', "
            "halt_reason = 'test full halt' WHERE id = 'test-player'"
        )
        self.conn.commit()

        row = self.conn.execute(
            "SELECT halt_mode, halted_at, halt_reason FROM ai_players WHERE id = 'test-player'"
        ).fetchone()
        self.assertEqual(row[0], "full")
        self.assertIsNotNone(
            row[1],
            f"Trigger should have auto-filled halted_at; got NULL. Row: {row!r}",
        )

    def test_2_update_to_exit_only_with_null_halted_at_autofills(self) -> None:
        """UPDATE halt_mode='exit_only' also triggers (any non-active mode)."""
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'exit_only' WHERE id = 'test-player'"
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'test-player'"
        ).fetchone()[0]
        self.assertIsNotNone(
            halted_at,
            "exit_only is a non-active mode and must auto-fill halted_at",
        )

    def test_3_update_with_explicit_halted_at_preserved(self) -> None:
        """Caller-supplied halted_at is NOT overwritten by the trigger."""
        explicit_ts = "2026-03-15 10:30:00"
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'full', halted_at = ?, "
            "halt_reason = 'historical' WHERE id = 'test-player'",
            (explicit_ts,),
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'test-player'"
        ).fetchone()[0]
        self.assertEqual(
            halted_at,
            explicit_ts,
            f"Explicit halted_at must be preserved; got {halted_at!r}",
        )

    def test_4_unhalt_does_not_touch_halted_at(self) -> None:
        """Re-activating (halt_mode='active') must NOT clear halted_at.

        CLAUDE.md doctrine: 'leave halted_at and halt_reason as historical
        record (do not clear)' when unhalting.
        """
        # First, halt the player to set halted_at.
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'full' WHERE id = 'test-player'"
        )
        self.conn.commit()
        halted_ts = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'test-player'"
        ).fetchone()[0]
        self.assertIsNotNone(halted_ts, "Pre-condition: halt step should fill halted_at")

        # Then unhalt — trigger must NOT touch halted_at.
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'active' WHERE id = 'test-player'"
        )
        self.conn.commit()
        after_unhalt = self.conn.execute(
            "SELECT halt_mode, halted_at FROM ai_players WHERE id = 'test-player'"
        ).fetchone()
        self.assertEqual(after_unhalt[0], "active")
        self.assertEqual(
            after_unhalt[1],
            halted_ts,
            f"Unhalt must not modify historical halted_at; was {halted_ts!r}, now {after_unhalt[1]!r}",
        )

    # ---- INSERT-path enforcement -------------------------------------------

    def test_5_insert_non_active_with_null_halted_at_autofills(self) -> None:
        """INSERT with halt_mode='full' + NULL halted_at → trigger fills it."""
        self.conn.execute(
            "INSERT INTO ai_players (id, halt_mode, halted_at, halt_reason) "
            "VALUES (?, 'full', NULL, 'born halted')",
            ("born-halted-player",),
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'born-halted-player'"
        ).fetchone()[0]
        self.assertIsNotNone(
            halted_at,
            "INSERT with non-active halt_mode and NULL halted_at must auto-fill",
        )

    def test_6_insert_active_with_null_halted_at_noop(self) -> None:
        """INSERT with halt_mode='active' must NOT auto-fill halted_at."""
        self.conn.execute(
            "INSERT INTO ai_players (id, halt_mode, halted_at, halt_reason) "
            "VALUES (?, 'active', NULL, NULL)",
            ("fresh-active-player",),
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'fresh-active-player'"
        ).fetchone()[0]
        self.assertIsNone(
            halted_at,
            f"Active players must keep halted_at NULL; got {halted_at!r}",
        )

    def test_7_insert_non_active_with_explicit_halted_at_preserved(self) -> None:
        """INSERT with explicit halted_at must be preserved verbatim."""
        explicit_ts = "2025-12-01 09:00:00"
        self.conn.execute(
            "INSERT INTO ai_players (id, halt_mode, halted_at, halt_reason) "
            "VALUES (?, 'full', ?, 'imported historical')",
            ("imported-player", explicit_ts),
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'imported-player'"
        ).fetchone()[0]
        self.assertEqual(halted_at, explicit_ts)

    # ---- Idempotency / no-op when already filled ---------------------------

    def test_8_re_update_does_not_overwrite_halted_at(self) -> None:
        """Editing halt_reason on an already-halted row preserves halted_at."""
        explicit_ts = "2026-01-01 00:00:00"
        self.conn.execute(
            "UPDATE ai_players SET halt_mode = 'full', halted_at = ? "
            "WHERE id = 'test-player'",
            (explicit_ts,),
        )
        self.conn.commit()

        # Now edit only halt_reason; halted_at and halt_mode unchanged.
        self.conn.execute(
            "UPDATE ai_players SET halt_reason = 'updated note' WHERE id = 'test-player'"
        )
        self.conn.commit()

        halted_at = self.conn.execute(
            "SELECT halted_at FROM ai_players WHERE id = 'test-player'"
        ).fetchone()[0]
        self.assertEqual(
            halted_at,
            explicit_ts,
            "A no-op-on-halt UPDATE must not disturb halted_at",
        )


if __name__ == "__main__":
    unittest.main()
