"""HM-RULE1-DELETE-LAYERS 2026-09-09 — proves both layers actually block DELETE.

Layer 1: BEFORE DELETE triggers (setup_db.py, connection-agnostic).
Layer 2: engine.db_safety's authorizer (defense-in-depth, also covers
DROP TABLE which no trigger can intercept).

Wired into .githooks/pre-commit's explicit file list so this runs on
every real commit, not just when the full suite happens to be run
manually.
"""
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pytest

import engine.db_safety as db_safety
import setup_db

RULE1_TABLES = db_safety.RULE1_TABLES


def _fresh_db(tmp_path):
    path = str(tmp_path / "test_trader.db")
    old_db = setup_db.DB if hasattr(setup_db, "DB") else None
    conn = sqlite3.connect(path)
    conn.close()
    return path


@pytest.fixture()
def rule1_conn(tmp_path):
    path = str(tmp_path / "rule1_test.db")
    conn = sqlite3.connect(path)
    for t in RULE1_TABLES:
        conn.execute(f"CREATE TABLE {t} (id INTEGER PRIMARY KEY, x TEXT)")
        conn.execute(
            f"CREATE TRIGGER trg_rule1_no_delete_{t} BEFORE DELETE ON {t} "
            f"BEGIN SELECT RAISE(ABORT, 'RULE #1: {t} rows are never deleted -- archive or mark instead'); END"
        )
        conn.execute(f"INSERT INTO {t} (x) VALUES ('probe')")
    conn.commit()
    yield conn
    conn.close()


@pytest.mark.parametrize("table", sorted(RULE1_TABLES))
def test_trigger_blocks_delete(rule1_conn, table):
    with pytest.raises(sqlite3.IntegrityError, match="RULE #1"):
        rule1_conn.execute(f"DELETE FROM {table} WHERE id=1")


@pytest.mark.parametrize("table", sorted(RULE1_TABLES))
def test_trigger_blocks_bare_delete_all(rule1_conn, table):
    """A DELETE with no WHERE clause (whole-table) must also be blocked --
    presence of a trigger disables SQLite's truncate-optimization fast path."""
    with pytest.raises(sqlite3.IntegrityError, match="RULE #1"):
        rule1_conn.execute(f"DELETE FROM {table}")


def test_authorizer_denies_delete(tmp_path):
    path = str(tmp_path / "authz_test.db")
    conn = db_safety.guarded_connect(path)
    conn.execute("CREATE TABLE trades (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO trades (id) VALUES (1)")
    conn.commit()
    with pytest.raises(sqlite3.DatabaseError):
        conn.execute("DELETE FROM trades WHERE id=1")
    conn.close()


def test_authorizer_denies_drop_table(tmp_path):
    path = str(tmp_path / "authz_drop_test.db")
    conn = db_safety.guarded_connect(path)
    conn.execute("CREATE TABLE signals (id INTEGER PRIMARY KEY)")
    conn.commit()
    with pytest.raises(sqlite3.DatabaseError):
        conn.execute("DROP TABLE signals")
    conn.close()


def test_authorizer_allows_untracked_table_delete(tmp_path):
    """The guard must not be overbroad -- a table NOT in RULE1_TABLES is
    unaffected."""
    path = str(tmp_path / "authz_scope_test.db")
    conn = db_safety.guarded_connect(path)
    conn.execute("CREATE TABLE scratch_table (id INTEGER PRIMARY KEY)")
    conn.execute("INSERT INTO scratch_table (id) VALUES (1)")
    conn.commit()
    conn.execute("DELETE FROM scratch_table WHERE id=1")  # must not raise
    conn.commit()
    assert conn.execute("SELECT COUNT(*) FROM scratch_table").fetchone()[0] == 0
    conn.close()


def test_live_db_has_all_eight_triggers_installed():
    """Confirms tonight's live migration actually took, not just the test DB."""
    conn = sqlite3.connect("data/trader.db")
    installed = {
        row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='trigger' "
            "AND name LIKE 'trg_rule1_no_delete_%'"
        )
    }
    conn.close()
    expected = {f"trg_rule1_no_delete_{t}" for t in RULE1_TABLES}
    assert expected <= installed, f"missing on live DB: {expected - installed}"
