"""engine/db_safety.py — HM-RULE1-DELETE-LAYERS 2026-09-09.

Connection-layer guard, defense-in-depth alongside the BEFORE DELETE
triggers in setup_db.py. The triggers are the primary, connection-agnostic
protection for DELETE (they fire regardless of which code opened the
connection); this module additionally covers DROP TABLE, which no SQLite
trigger can intercept.

Deliberately NOT wired into every existing `_conn()` helper across the
codebase tonight -- doing that touches a large number of live decision/exit
paths in one pass, which is out of scope for tonight's "code changes only
where nothing reaches a decision or exit path" constraint. This is a new,
opt-in utility: use `guarded_connect()` in place of `sqlite3.connect()`
where a connection doesn't need to touch the hot trading path (tooling,
tests, admin scripts). Wiring it into the shared trading-path connections
is a separate, larger, higher-risk session.
"""
from __future__ import annotations

import sqlite3

RULE1_TABLES = frozenset({
    "trades", "decision_audit", "signals", "signals_v2", "agent_ratings",
    "desk_execution_trace", "crew_decisions", "notifications",
})


def _authorizer(action, arg1, arg2, dbname, source):
    if action == sqlite3.SQLITE_DELETE and arg1 in RULE1_TABLES:
        return sqlite3.SQLITE_DENY
    if action == sqlite3.SQLITE_DROP_TABLE and arg1 in RULE1_TABLES:
        return sqlite3.SQLITE_DENY
    return sqlite3.SQLITE_OK


def install_guard(conn: sqlite3.Connection) -> None:
    """Attach the RULE #1 authorizer to an existing connection."""
    conn.set_authorizer(_authorizer)


def guarded_connect(path: str, **kwargs) -> sqlite3.Connection:
    """sqlite3.connect() wrapper with the RULE #1 authorizer pre-installed."""
    conn = sqlite3.connect(path, **kwargs)
    install_guard(conn)
    return conn
