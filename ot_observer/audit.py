"""Audit log: every observer call appended to logs/ot_observer.log as one JSON line
(timestamp in UTC and MST, tool, args, status, row count, duration) -- including disabled,
rejected and unavailable calls -- so what the XO asked for, and when, is visible without
asking it."""
from __future__ import annotations

import json
import threading
from typing import Any

from . import timefmt
from .config import ObserverPaths

_write_lock = threading.Lock()


def append(paths: ObserverPaths, *, tool: str, args: dict[str, Any], status: str | None,
           row_count: int | None, duration_ms: float) -> str | None:
    """Append one entry. Returns None on success, or the failure reason (never raises)."""
    entry = {
        "ts": timefmt.now_label(),
        "tool": tool,
        "args": args,
        "status": status,
        "row_count": row_count,
        "duration_ms": duration_ms,
    }
    try:
        line = json.dumps(entry, default=str) + "\n"
        with _write_lock:
            paths.audit_log.parent.mkdir(parents=True, exist_ok=True)
            with open(paths.audit_log, "a", encoding="utf-8") as handle:
                handle.write(line)
        return None
    except (OSError, TypeError, ValueError) as exc:
        return f"{type(exc).__name__}: {exc}"
