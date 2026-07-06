"""engine/db_conn.py — shared sqlite3 connection helper.

HM-WAL-BUSY-TIMEOUT-HYGIENE (2026-07-06), wave 1. Centralizes the two
per-connection settings that do NOT persist on the database file itself
(unlike journal_mode=WAL, which is durable in the DB header once set):

  - busy_timeout=30000 (30s). NOTE: main.py already monkey-patches
    sqlite3.connect process-wide with this exact setting (main.py:69-74),
    so anything running inside the main.py process already gets it for
    free. This helper sets it explicitly anyway so callers outside that
    process (signal-center's separate Flask/py3.9 process, standalone
    scripts) aren't silently relying on a patch that only exists in one
    of the two processes that touch these databases.
  - synchronous=NORMAL. NOT patched anywhere today — repo-wide grep found
    it set explicitly in only 5 files. Safe under WAL (WAL's own commit
    protocol already makes the extra fsync FULL performs optional); FULL
    is stricter than WAL needs and was adding fsync cost to every writer
    during exactly the contention windows that caused the 2026-07-06
    lock storm. journal_mode itself is NOT re-set here — already durable
    on the DB file, re-issuing it per connection is a no-op that only
    adds a statement to every hot path.

Not a connection pool — callers still own the connection's lifecycle
(the existing `with contextlib.closing(get_conn()) as c:` pattern), this
only centralizes what a fresh connection gets configured with.

Full proposal / rollback notes: docs/XO_BACKLOG.md, HM-WAL-BUSY-TIMEOUT-HYGIENE.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional

DEFAULT_DB_PATH = Path(__file__).resolve().parent.parent / "data" / "trader.db"


def get_conn(
    db_path: Optional[str] = None,
    *,
    timeout: int = 30,
    check_same_thread: bool = True,
) -> sqlite3.Connection:
    """Open a sqlite3 connection with the hardened defaults.

    Caller still owns lifecycle — use with contextlib.closing(...) or an
    explicit .close(), same as any other sqlite3.connect() call.
    """
    conn = sqlite3.connect(
        str(db_path) if db_path else str(DEFAULT_DB_PATH),
        timeout=timeout,
        check_same_thread=check_same_thread,
    )
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn
