"""tests/test_fleet_lifecycle_backfill_agents.py —
HM-FLEET-LIFECYCLE-BACKFILL-TARGET-2026-09-01.

scripts/fleet_lifecycle_backfill_agents.py was a one-time bulk seed
script (skips any target that already has a ledger row) with no ongoing
reconciliation use. Live incident: ollama-qwen3/qwen3-4b-audition were
halted via a raw SQLite UPDATE on 2026-08-31, bypassing
scripts/fleet_lifecycle.py entirely -- the only tool available to record
it afterward was fleet_lifecycle.py's own normal `halt` action, which
correctly wrote a real order doc and ledger row but with backfilled=0
(nothing could write backfilled=1 for a single already-existing target
on demand). --target closes that gap.

Never touches the real trader.db -- everything runs against a temp DB.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts import fleet_lifecycle_backfill_agents as bf  # noqa: E402


def _make_db(tmp_path: Path) -> Path:
    db_path = tmp_path / "trader.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE ai_players (
        id TEXT PRIMARY KEY, display_name TEXT, halt_mode TEXT, halt_reason TEXT)""")
    conn.execute("""CREATE TABLE fleet_lifecycle_ledger (
        id INTEGER PRIMARY KEY AUTOINCREMENT, target_type TEXT, target_name TEXT,
        action TEXT, reason TEXT, order_doc TEXT, resume_by TEXT, review_by TEXT,
        backfilled INTEGER DEFAULT 0, created_at TEXT DEFAULT (datetime('now')),
        created_by TEXT DEFAULT 'test')""")
    conn.commit()
    conn.close()
    return db_path


class _Args:
    def __init__(self, **kw):
        self.dry_run = False
        self.target = None
        self.__dict__.update(kw)


def test_target_mode_backfills_one_agent(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO ai_players (id, display_name, halt_mode, halt_reason) "
        "VALUES ('ollama-qwen3', 'Dax', 'full', "
        "'[2026-08-31] HALT: HM-SEAT-CONSOLIDATION -- direct SQLite UPDATE bypass')"
    )
    conn.commit()
    conn.close()

    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    rc = bf._backfill_one(conn, "ollama-qwen3", dry_run=False)
    assert rc == 0

    row = conn.execute(
        "SELECT * FROM fleet_lifecycle_ledger WHERE target_name='ollama-qwen3'"
    ).fetchone()
    assert row is not None
    assert row["action"] == "halt"
    assert row["backfilled"] == 1
    assert row["order_doc"] is None
    assert "HM-SEAT-CONSOLIDATION" in row["reason"]


def test_target_mode_works_even_when_a_ledger_row_already_exists(tmp_path):
    """The exact gap this mode closes: bulk mode's `already` skip-set would
    refuse to touch a target with existing history. --target must not."""
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO ai_players (id, display_name, halt_mode, halt_reason) "
        "VALUES ('quark-ic', 'Quark', 'active', NULL)"
    )
    conn.execute(
        "INSERT INTO fleet_lifecycle_ledger "
        "(target_type, target_name, action, reason, backfilled) "
        "VALUES ('agent', 'quark-ic', 'active', 'original seed', 1)"
    )
    conn.commit()

    rc = bf._backfill_one(conn, "quark-ic", dry_run=False)
    assert rc == 0

    rows = conn.execute(
        "SELECT action, backfilled FROM fleet_lifecycle_ledger "
        "WHERE target_name='quark-ic' ORDER BY id"
    ).fetchall()
    assert len(rows) == 2  # original seed preserved, new row appended (INSERT-only ledger)
    assert rows[-1][0] == "active"
    assert rows[-1][1] == 1


def test_target_mode_dry_run_writes_nothing(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    conn.execute(
        "INSERT INTO ai_players (id, display_name, halt_mode, halt_reason) "
        "VALUES ('quark-ic', 'Quark', 'full', 'test halt')"
    )
    conn.commit()

    rc = bf._backfill_one(conn, "quark-ic", dry_run=True)
    assert rc == 0
    n = conn.execute("SELECT COUNT(*) FROM fleet_lifecycle_ledger").fetchone()[0]
    assert n == 0


def test_target_mode_unknown_agent_errors(tmp_path):
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    rc = bf._backfill_one(conn, "does-not-exist", dry_run=False)
    assert rc == 1
    n = conn.execute("SELECT COUNT(*) FROM fleet_lifecycle_ledger").fetchone()[0]
    assert n == 0


def test_bulk_mode_still_skips_existing_targets(tmp_path):
    """Regression guard: adding --target must not change bulk mode's
    original idempotent skip-if-present behavior."""
    db_path = _make_db(tmp_path)
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        "INSERT INTO ai_players (id, display_name, halt_mode, halt_reason) "
        "VALUES ('quark-ic', 'Quark', 'active', NULL)"
    )
    conn.execute(
        "INSERT INTO fleet_lifecycle_ledger "
        "(target_type, target_name, action, reason, backfilled) "
        "VALUES ('agent', 'quark-ic', 'active', 'already seeded', 1)"
    )
    conn.commit()
    conn.close()

    with patch.object(bf, "DB_PATH", db_path), \
         patch("sys.argv", ["fleet_lifecycle_backfill_agents.py"]):
        rc = bf.main()
    assert rc == 0

    conn = sqlite3.connect(str(db_path))
    n = conn.execute(
        "SELECT COUNT(*) FROM fleet_lifecycle_ledger WHERE target_name='quark-ic'"
    ).fetchone()[0]
    assert n == 1  # untouched -- bulk mode's original skip-if-present behavior


if __name__ == "__main__":
    import pytest
    raise SystemExit(pytest.main([__file__, "-q"]))
