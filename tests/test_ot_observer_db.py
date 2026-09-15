"""ot_observer read-only DB layer.

Doctrine under test (2026-09-14): the observer must never be the thing holding a lock
(Monday's red_alert_check self-lock took out 168 jobs), and it must never turn "cannot
read" into an empty result.
"""
from __future__ import annotations

import os
import shutil
import sqlite3
import sys
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))
import ot_observer_testkit  # noqa: E402,F401  (puts the repo root on sys.path)

from ot_observer import db  # noqa: E402


def _wal_db(path: Path, rows: int = 1) -> sqlite3.Connection:
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE t(x)")
    conn.executemany("INSERT INTO t VALUES (?)", [(i,) for i in range(rows)])
    conn.commit()
    return conn


@pytest.mark.parametrize("statement", [
    "INSERT INTO t VALUES (99)",
    "UPDATE t SET x = 5",
    "DELETE FROM t",
    "CREATE TABLE u(y)",
])
def test_read_only_handle_rejects_every_write(tmp_path, statement):
    path = tmp_path / "a.db"
    _wal_db(path).close()
    conn = db.connect_ro(path)
    try:
        with pytest.raises(sqlite3.OperationalError, match="readonly|read-only|query_only"):
            conn.execute(statement)
    finally:
        conn.close()
    check = sqlite3.connect(path)
    assert check.execute("SELECT count(*) FROM t").fetchone()[0] == 1
    check.close()


def test_fetch_returns_rows_as_dicts(tmp_path):
    path = tmp_path / "a.db"
    _wal_db(path, rows=2).close()
    assert db.fetch(path, "SELECT x FROM t ORDER BY x") == [{"x": 0}, {"x": 1}]


def test_missing_db_is_unavailable_and_is_not_created(tmp_path):
    missing = tmp_path / "nope.db"
    with pytest.raises(db.Unavailable) as exc:
        db.fetch(missing, "SELECT 1")
    assert exc.value.reason
    assert not missing.exists()


def test_unopenable_wal_index_is_unavailable_not_empty(tmp_path):
    """-wal present, -shm missing, directory not writable: the read-only open cannot
    build the WAL index. This is the condition that genuinely fails (a missing -shm in a
    writable directory reads fine -- verified 2026-09-14 on SQLite 3.53)."""
    live = tmp_path / "live.db"
    writer = _wal_db(live, rows=1)
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("INSERT INTO t VALUES (5)")
    writer.commit()
    snap = tmp_path / "snap"
    snap.mkdir()
    shutil.copy(live, snap / "live.db")
    shutil.copy(str(live) + "-wal", snap / "live.db-wal")
    writer.close()
    assert not (snap / "live.db-shm").exists()
    os.chmod(snap, 0o555)
    try:
        with pytest.raises(db.Unavailable) as exc:
            db.fetch(snap / "live.db", "SELECT count(*) AS n FROM t")
        assert exc.value.reason
    finally:
        os.chmod(snap, 0o755)


def test_locked_db_fails_fast_as_unavailable(tmp_path):
    path = tmp_path / "locked.db"
    writer = sqlite3.connect(path)
    writer.execute("PRAGMA journal_mode=WAL")
    writer.execute("CREATE TABLE t(x)")
    writer.commit()
    writer.execute("PRAGMA locking_mode=EXCLUSIVE")
    writer.execute("INSERT INTO t VALUES (1)")
    writer.commit()
    try:
        started = time.monotonic()
        with pytest.raises(db.Unavailable) as exc:
            db.fetch(path, "SELECT count(*) AS n FROM t")
        assert time.monotonic() - started < 1.0
        assert "locked" in exc.value.reason
    finally:
        writer.close()


def test_runaway_query_is_aborted_within_its_budget(tmp_path):
    path = tmp_path / "a.db"
    _wal_db(path).close()
    runaway = "WITH RECURSIVE c(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM c) SELECT count(*) FROM c"
    started = time.monotonic()
    with pytest.raises(db.Unavailable) as exc:
        db.fetch(path, runaway, budget_ms=100)
    assert time.monotonic() - started < 1.5
    assert "budget" in exc.value.reason


def test_no_handle_survives_a_request(tmp_path):
    path = tmp_path / "a.db"
    writer = _wal_db(path, rows=3)
    writer.execute("PRAGMA wal_autocheckpoint=0")
    writer.execute("INSERT INTO t VALUES (7)")
    writer.commit()

    db.fetch(path, "SELECT count(*) AS n FROM t")
    with pytest.raises(db.Unavailable):
        db.fetch(tmp_path / "nope.db", "SELECT 1")

    assert db.open_handle_count() == 0
    busy, _log, _checkpointed = writer.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
    assert busy == 0
    writer.close()
