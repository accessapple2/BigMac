#!/usr/bin/env python3
"""HM-Q-MARKET-OPEN-PING — ONE-SHOT market-open reminder for Q's FIRST live
session (2026-06-09). NTFYs the Captain that Q (the Grok War Room voice) is live
now that the session is open, with a quick status (Q's recent debate takes +
dissent count). Complements q_dissent_watch.py (the recurring per-dissent alert).

ONE-SHOT: a date guard (TARGET_DATE) means this fires on exactly one day and is a
silent no-op every other time it's invoked — so the cron entry can linger
harmlessly. Local cron (the remote /schedule can't read this local trader.db).
Read-only DB, stdlib only.
"""
import socket
import sqlite3
import sys
import threading
import urllib.request
from datetime import datetime
from pathlib import Path

ROOT = Path("/Users/bigmac/autonomous-trader")
DB = ROOT / "data" / "trader.db"
NTFY = "https://ntfy.sh/ollietrades-admin"
TARGET_DATE = "2026-06-09"  # Q's first live session — fire once, then no-op forever

_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def _ntfy(title: str, body: str) -> None:
    """HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: this box has no working IPv6
    route to ntfy.sh (HM-NTFY-IPV6-NOROUTE, 2026-07-07) — forces IPv4 via a
    local getaddrinfo monkeypatch."""
    try:
        req = urllib.request.Request(
            NTFY, data=body.encode("utf-8"),
            headers={
                "Title": title.encode("ascii", "replace").decode("ascii"),
                "Priority": "default",
                "Tags": "bell,q",
            },
            method="POST",
        )
        with _ntfy_ipv4_lock:
            socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                urllib.request.urlopen(req, timeout=8)
            finally:
                socket.getaddrinfo = _orig_getaddrinfo
    except Exception as e:
        print(f"[q-open-ping] ntfy failed: {e}", file=sys.stderr)


def main() -> int:
    # ONE-SHOT guard: the box runs in Arizona local time (no DST), so naive
    # datetime.now() is the AZ date. Fire only on Q's first live session.
    if datetime.now().strftime("%Y-%m-%d") != TARGET_DATE:
        return 0
    takes = q_dissents = 0
    try:
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=15)
        try:
            takes = conn.execute(
                "SELECT COUNT(*) FROM war_room WHERE player_id='q-witness' "
                "AND created_at >= datetime('now','-1 day')"
            ).fetchone()[0]
            if conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='crew_dissent_log'"
            ).fetchone():
                q_dissents = conn.execute(
                    "SELECT COUNT(*) FROM crew_dissent_log WHERE dissenter='Q'"
                ).fetchone()[0]
        finally:
            conn.close()
    except Exception as e:
        print(f"[q-open-ping] db read failed: {e}", file=sys.stderr)

    _ntfy(
        "📣 Market open — Q is live",
        f"Q (the Grok War Room voice) is debating this session. "
        f"Takes in last 24h: {takes}. Q dissents logged so far: {q_dissents}. "
        f"Each new Q dissent will NTFY automatically — or ask me to check anytime.",
    )
    print(f"[q-open-ping] pinged: takes={takes} q_dissents={q_dissents}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
