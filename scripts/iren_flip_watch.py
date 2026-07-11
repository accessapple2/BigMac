#!/usr/bin/env python3
"""scripts/iren_flip_watch.py — HM-FLEET-REBASELINE-2026-07-04 follow-up.

Watches for the gemini-2.5-flash IREN short to close, then flips the agent's
halt_mode exit_only -> full (docs/XO_BACKLOG.md:1752-1756) and self-removes
its own cron entry. Run every 15 min via cron; see install instructions in
the commit/PR that added this file.
"""
from __future__ import annotations

import os
import socket
import subprocess
import sys
import sqlite3
import threading
import urllib.error
import urllib.request
import json
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DB_PATH = ROOT / "data" / "trader.db"
RESTART_LOCKDIR = Path("/tmp/uss_trader_restart.lock")  # scripts/trader_restart.sh mutex

PLAYER_ID = "gemini-2.5-flash"
SYMBOL = "IREN"
NTFY_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "ollietrades-admin")
CRON_MARKER = "iren_flip_watch.py"  # unique substring used to self-remove the cron line

HALT_REASON = (
    "{date} HM-FLEET-REBASELINE-2026-07-04 retirement complete: guarded return "
    "8.93%<9%, spam 54.5%>48%; IREN position closed, exit_only->full flip "
    "executed by scripts/iren_flip_watch.py"
)


def _restart_in_progress() -> bool:
    """Mirror trader_restart.sh's mkdir-atomic lock: if the lockdir exists and
    its recorded pid is alive, a restart is actively running (single-writer
    gate) -- skip this tick rather than racing it."""
    if not RESTART_LOCKDIR.exists():
        return False
    pid_file = RESTART_LOCKDIR / "pid"
    if not pid_file.exists():
        return True  # lock just created, pid not written yet -- treat as in-progress
    try:
        pid = int(pid_file.read_text().strip())
    except (ValueError, OSError):
        return True
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False  # stale lock, holder is dead
    except PermissionError:
        return True
    else:
        return True


_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def _ntfy(title: str, body: str) -> None:
    """HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: this box has no working IPv6
    route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07) — forces IPv4 via a
    local getaddrinfo monkeypatch."""
    try:
        data = body.encode("utf-8")
        req = urllib.request.Request(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=data,
            headers={"Title": title},
            method="POST",
        )
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                urllib.request.urlopen(req, timeout=10)
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
    except (urllib.error.URLError, OSError) as exc:
        print(f"ntfy error: {exc}", file=sys.stderr)


def _get_state() -> tuple[str | None, float]:
    """Returns (halt_mode, iren_qty). iren_qty is 0.0 if no open position row."""
    conn = sqlite3.connect(DB_PATH, timeout=20)
    try:
        row = conn.execute(
            "SELECT halt_mode FROM ai_players WHERE id=?", (PLAYER_ID,)
        ).fetchone()
        halt_mode = row[0] if row else None
        row = conn.execute(
            "SELECT qty FROM positions WHERE player_id=? AND symbol=?",
            (PLAYER_ID, SYMBOL),
        ).fetchone()
        qty = float(row[0]) if row else 0.0
        return halt_mode, qty
    finally:
        conn.close()


def _flip_halt_mode() -> bool:
    """Guarded UPDATE: only flips rows still exit_only. Returns True if this
    call actually changed the row (rowcount == 1)."""
    conn = sqlite3.connect(DB_PATH, timeout=20)
    try:
        cur = conn.execute(
            "UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, "
            "halt_reason=? WHERE id=? AND halt_mode='exit_only'",
            (HALT_REASON.format(date=datetime.now(timezone.utc).date()), PLAYER_ID),
        )
        conn.commit()
        return cur.rowcount == 1
    finally:
        conn.close()


def _remove_own_cron() -> None:
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        print("crontab -l failed, cannot self-remove cron entry", file=sys.stderr)
        return
    kept = [ln for ln in result.stdout.splitlines() if CRON_MARKER not in ln]
    new_crontab = "\n".join(kept) + "\n"
    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)


def main() -> int:
    if _restart_in_progress():
        print("trader restart in progress -- skipping this check")
        return 0

    halt_mode, qty = _get_state()

    if halt_mode != "exit_only":
        print(f"halt_mode is '{halt_mode}', not exit_only -- nothing to do, "
              f"removing stale cron entry")
        _remove_own_cron()
        return 0

    if qty != 0:
        print(f"IREN still open (qty={qty}) -- nothing to do")
        return 0

    if not _flip_halt_mode():
        print("qty is 0 but guarded UPDATE affected 0 rows (race?) -- leaving "
              "cron in place for next tick", file=sys.stderr)
        return 0

    _ntfy(
        "IREN closed, gemini-2.5-flash flipped to full",
        "IREN position closed; gemini-2.5-flash halt_mode flipped exit_only->full "
        "per docs/XO_BACKLOG.md:1752-1756. Removing watch cron.",
    )
    _remove_own_cron()
    print("IREN closed -- flip executed, ntfy sent, cron self-removed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
