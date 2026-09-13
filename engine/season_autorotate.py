"""engine/season_autorotate.py — HM-SEASON-AUTOROTATE-GATE-2026-09-13.

The scheduled (unattended) season-rotation path. Seasons are started deliberately now
(S8 was a manual `rotate_season(caller="s8-manual")` run 2026-09-11), so this path is
gated behind config.SEASON_AUTOROTATE_ENABLED — default off, absent = off — and returns
before any sentinel read, scope/margin check, or call into rotate_season() when disabled.

Hardening only (enables nothing while the flag is off): main.py polls every 5 min, the
window is 10 min wide (23:50–23:59 Sunday AZ, > poll interval, the Kirk/CTO rule), and a
once-per-Sunday sentinel (in-memory + durable season_N_start check) makes it at-most-once,
so the trader's restart phase no longer decides whether it fires.

rotate_season()/start_season() are untouched — manual rotation, margin guard included,
works exactly as before.

HOLD (2026-09-13): no rotation, auto or manual, should run until rotate_season()'s
position-row deletion blocks on broker-backed positions instead of orphaning them
(KMI/TQQQ came from exactly this). Enabling this flag before that lands re-opens it.
"""
from __future__ import annotations

from datetime import datetime

from rich.console import Console

console = Console()

SKIP_LINE = "season auto-rotation disabled by SEASON_AUTOROTATE_ENABLED=false; skipping"
CALLER = "cron-sunday"
WINDOW_START_MINUTE = 50  # Sunday 23:50–23:59 AZ

_skip_logged_keys: set[str] = set()
_fired_sundays: set[str] = set()


def autorotate_enabled() -> bool:
    import config
    return bool(getattr(config, "SEASON_AUTOROTATE_ENABLED", False))


def in_window(now: datetime) -> bool:
    return now.weekday() == 6 and now.hour == 23 and now.minute >= WINDOW_START_MINUTE


def _already_rotated_today(now: datetime) -> bool:
    """Durable half of the sentinel (survives a restart inside the window): the current
    season's start stamp is today's date."""
    from engine import season_manager as sm
    conn = sm._conn()
    try:
        cur = conn.execute("SELECT value FROM settings WHERE key='current_season'").fetchone()
        if not cur:
            return False
        start = conn.execute("SELECT value FROM settings WHERE key=?",
                             (f"season_{cur[0]}_start",)).fetchone()
        return bool(start and str(start[0])[:10] == now.date().isoformat())
    finally:
        conn.close()


def run_scheduled_rotation(now: datetime, rotate=None) -> str:
    """One scheduler poll. Returns the outcome:
    'disabled' | 'outside-window' | 'already-fired' | 'rotated:<n>' | 'aborted' | 'error'."""
    sunday = now.date().isoformat()
    if not autorotate_enabled():
        # One line at the process's first poll and once per Sunday window — not every poll.
        key = f"window:{sunday}" if in_window(now) else "startup"
        if key not in _skip_logged_keys:
            _skip_logged_keys.add(key)
            console.log(f"[yellow]{SKIP_LINE}")
        return "disabled"
    if not in_window(now):
        return "outside-window"
    if sunday in _fired_sundays:
        return "already-fired"
    try:
        if _already_rotated_today(now):
            _fired_sundays.add(sunday)
            console.log(f"[yellow]season auto-rotation: a season already started today ({sunday}); skipping")
            return "already-fired"
    except Exception as e:
        console.log(f"[red]season auto-rotation: sentinel check failed ({e}); skipping (fail-closed)")
        return "error"
    _fired_sundays.add(sunday)  # at-most-once per Sunday, marked before the call
    if rotate is None:
        from engine.season_manager import rotate_season as rotate
    try:
        new = rotate(caller=CALLER)
    except Exception as e:
        console.log(f"[red]Season rotation error: {e}")
        return "error"
    if new is None:
        console.log("[bold red]⭐ Season auto-rotation ABORTED by reactivation-scope safety check — "
                    "no DB writes made, season NOT advanced.")
        return "aborted"
    console.log(f"[bold green]⭐ Season auto-rotation complete → Season {new}")
    return f"rotated:{new}"
