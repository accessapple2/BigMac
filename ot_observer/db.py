"""Read-only SQLite access for the observer.

- One `file:...?mode=ro` URI handle per request, closed in `finally`, plus PRAGMA query_only.
- 200 ms busy timeout and a hard query time budget (progress handler), so the observer
  fails fast instead of waiting on, or holding, a lock.
- Any failure to read raises Unavailable with the real reason. Callers must report it;
  it is never turned into an empty result.

Uses sqlite3.dbapi2.connect, which main.py's global `sqlite3.connect = _patched_connect`
does not rebind -- the observer still runs as its own process regardless.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from sqlite3 import dbapi2 as _dbapi2
from typing import Any, Iterable

from . import config


class Unavailable(Exception):
    """A source could not be read. `reason` says why."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


_handles_lock = threading.Lock()
_open_handles = 0


def _adjust_handles(delta: int) -> None:
    global _open_handles
    with _handles_lock:
        _open_handles += delta


def open_handle_count() -> int:
    """Handles opened by this module and not yet closed. Nonzero means a leak."""
    return _open_handles


class _TrackedConnection(sqlite3.Connection):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._ot_tracked = True
        _adjust_handles(+1)

    def close(self) -> None:
        if getattr(self, "_ot_tracked", False):
            self._ot_tracked = False
            _adjust_handles(-1)
        super().close()


def connect_ro(path: Path, busy_timeout_ms: int = config.BUSY_TIMEOUT_MS) -> sqlite3.Connection:
    """Open a read-only handle. Caller must close it. Raises Unavailable."""
    path = Path(path)
    uri = f"{path.resolve().as_uri()}?mode=ro"
    try:
        conn = _dbapi2.connect(uri, uri=True, timeout=busy_timeout_ms / 1000,
                               check_same_thread=False, factory=_TrackedConnection)
    except sqlite3.Error as exc:
        raise Unavailable(f"cannot open {path}: {exc}") from exc
    try:
        conn.execute(f"PRAGMA busy_timeout = {int(busy_timeout_ms)}")
        conn.execute("PRAGMA query_only = 1")
    except sqlite3.Error as exc:
        conn.close()
        raise Unavailable(f"cannot open {path}: {exc}") from exc
    return conn


def fetch(path: Path, sql: str, params: Iterable[Any] = (), *,
          busy_timeout_ms: int = config.BUSY_TIMEOUT_MS,
          budget_ms: int = config.QUERY_BUDGET_MS) -> list[dict]:
    """Run one fixed query on a fresh read-only handle and close it. Raises Unavailable."""
    path = Path(path)
    conn = connect_ro(path, busy_timeout_ms)
    deadline = time.monotonic() + budget_ms / 1000
    conn.set_progress_handler(lambda: 1 if time.monotonic() > deadline else 0, 1000)
    try:
        cursor = conn.execute(sql, tuple(params))
        columns = [c[0] for c in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    except sqlite3.OperationalError as exc:
        if "interrupt" in str(exc).lower():
            raise Unavailable(
                f"query budget exceeded ({budget_ms} ms) on {path.name}; aborted so the observer holds no lock"
            ) from exc
        raise Unavailable(f"read failed on {path.name}: {exc}") from exc
    except sqlite3.Error as exc:
        raise Unavailable(f"read failed on {path.name}: {exc}") from exc
    finally:
        conn.close()
