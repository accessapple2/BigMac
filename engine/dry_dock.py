"""Dry-dock mode — file-based flag so monitors know downtime is intentional.

Same shape as engine.fleet_halt's KILL_SWITCH: a file's presence is the
single source of truth, cheap to check, and impossible to silently drift
the way an ignore-list with an expired revisit date can (see CLAUDE.md's
doc-revisit-date lesson) — there's no date to go stale, only a flag that's
either there or isn't.

Usage in a monitor::

    from engine import dry_dock
    if dry_dock.is_docked():
        info = dry_dock.dock_info()
        # downgrade to an hourly informational heartbeat instead of paging
    else:
        # normal alerting

To activate:   dry_dock.enter("reason") — or `touch data/DRY_DOCK` by hand
               (dock_info() degrades gracefully on a hand-touched empty file)
To deactivate: dry_dock.exit_dock() — or `rm data/DRY_DOCK`

Removing the file restores every dock-aware monitor's normal paging
automatically — no per-monitor code needs to change back at undock.

Created 2026-09-11, dry-dock breach follow-up: origin_healthcheck.sh's
blind "port down -> restart it" remedy, and hm_ops_sentinel.py's blind
"main.py not running -> red_alert" check, are the same failure class —
a monitor with no concept of intentional downtime. This flag is the fix
hm_ops_sentinel.py adopts first; other monitors migrate opportunistically.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

_FLAG_FILE = Path(__file__).resolve().parent.parent / "data" / "DRY_DOCK"


def is_docked() -> bool:
    """Return True if the dry-dock flag is set."""
    return _FLAG_FILE.exists()


def dock_info() -> dict:
    """Return {'since': iso-str|None, 'reason': str|None}. Never raises."""
    try:
        return json.loads(_FLAG_FILE.read_text())
    except Exception:
        return {"since": None, "reason": None}


def enter(reason: str) -> None:
    """Set the dry-dock flag with a reason + UTC timestamp."""
    _FLAG_FILE.parent.mkdir(parents=True, exist_ok=True)
    _FLAG_FILE.write_text(json.dumps({
        "since": datetime.now(timezone.utc).isoformat(),
        "reason": reason,
    }, indent=2))


def exit_dock() -> None:
    """Clear the dry-dock flag. No-op if already clear."""
    _FLAG_FILE.unlink(missing_ok=True)
