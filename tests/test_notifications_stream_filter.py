"""HM-BUG-BATCH-2026-07-10 item 7 (ALERT STREAM SEPARATION, task 26).

get_notifications()'s `stream` filter is computed in Python
(classify_alert_stream), not a SQL column -- so a naive `LIMIT ?` applied
before that filter can starve the result down to far fewer than `limit`
rows when ops and signal rows are interleaved (e.g. asking for the 5 most
recent "ops" notifications out of a DB where the most recent 100 rows are
mostly "signal" would return 0-1 rows instead of 5). This test pins the
fix: widen the SQL fetch when a stream filter is active, then truncate to
`limit` after filtering.
"""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _seed_notifications(db_path, n_signal=30, n_ops=3):
    """Interleave many signal rows with a few ops rows, ops oldest-first so
    a naive small SQL LIMIT (applied before Python-side stream filtering)
    would miss them entirely."""
    conn = sqlite3.connect(str(db_path))
    conn.execute("""CREATE TABLE notifications (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp   TEXT DEFAULT CURRENT_TIMESTAMP,
        type        TEXT,
        severity    TEXT,
        title       TEXT,
        body        TEXT,
        icon        TEXT,
        agent_id    TEXT,
        acknowledged INTEGER DEFAULT 0
    )""")
    # ops rows inserted first (lowest ids = oldest)
    for i in range(n_ops):
        conn.execute(
            "INSERT INTO notifications (type, severity, title, body, icon, acknowledged) "
            "VALUES (?, 'info', ?, 'ops body', '🚨', 0)",
            (f"sentinel_lock_errors_{i}", f"Ops {i}"),
        )
    # then a wall of signal rows (highest ids = newest)
    for i in range(n_signal):
        conn.execute(
            "INSERT INTO notifications (type, severity, title, body, icon, acknowledged) "
            "VALUES (?, 'info', ?, 'signal body', '📈', 0)",
            (f"dyn_rsi_oversold_TICK{i}", f"Signal {i}"),
        )
    conn.commit()
    conn.close()


def _fake_conn(db_path):
    def _conn():
        c = sqlite3.connect(str(db_path))
        c.row_factory = sqlite3.Row
        return c
    return _conn


def test_stream_filter_does_not_starve_results_behind_a_small_limit(tmp_path):
    """The literal failure mode: request limit=5 ops notifications while 30
    newer signal rows sit on top of them in id order. Without widening the
    SQL fetch, LIMIT 5 pulls only the 5 newest (all signal) rows and the
    Python-side ops filter then returns zero."""
    import dashboard.app as app_module

    db_path = tmp_path / "notif_test.db"
    _seed_notifications(db_path, n_signal=30, n_ops=3)

    with patch.object(app_module, "_conn", _fake_conn(db_path)), \
         patch.object(app_module, "_init_notifications_table", lambda: None):
        result = app_module.get_notifications(since=0, limit=5, stream="ops", acknowledged="all")

    assert len(result) == 3
    assert all(r["stream"] == "ops" for r in result)


def test_limit_is_respected_after_stream_filtering(tmp_path):
    """Truncation to `limit` must happen AFTER the Python-side stream
    filter, not before -- else a wide SQL fetch would silently ignore the
    caller's requested page size."""
    import dashboard.app as app_module

    db_path = tmp_path / "notif_test2.db"
    _seed_notifications(db_path, n_signal=30, n_ops=3)

    with patch.object(app_module, "_conn", _fake_conn(db_path)), \
         patch.object(app_module, "_init_notifications_table", lambda: None):
        result = app_module.get_notifications(since=0, limit=2, stream="signal", acknowledged="all")

    assert len(result) == 2
    assert all(r["stream"] == "signal" for r in result)


def test_unfiltered_request_unchanged(tmp_path):
    """No `stream` filter -> behavior must stay exactly what it was before
    this fix (plain SQL LIMIT, no widening)."""
    import dashboard.app as app_module

    db_path = tmp_path / "notif_test3.db"
    _seed_notifications(db_path, n_signal=30, n_ops=3)

    with patch.object(app_module, "_conn", _fake_conn(db_path)), \
         patch.object(app_module, "_init_notifications_table", lambda: None):
        result = app_module.get_notifications(since=0, limit=5, stream="", acknowledged="all")

    assert len(result) == 5


if __name__ == "__main__":
    raise SystemExit(pytest.main([__file__, "-q"]))
