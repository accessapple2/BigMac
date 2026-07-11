#!/usr/bin/env python3
"""HM-Q-DISSENT-WATCH — local cron watcher for Q's (the Grok War Room voice)
first live dissents. Queries crew_dissent_log WHERE dissenter='Q' and NTFYs any
NEW rows (tracked by last-seen id in a state file, so each fires exactly once).

Local mechanism (NOT a remote /schedule routine — those can't read this local
trader.db). Same posture as scripts/schwab_csv_watcher.sh — read-only on the DB,
stdlib-only (sqlite3 + urllib), no venv dependency. Runs during market hours via
crontab; a no-op (silent exit) when there are no new Q dissents.
"""
import socket
import sqlite3
import sys
import threading
import urllib.request
from pathlib import Path

ROOT = Path("/Users/bigmac/autonomous-trader")
DB = ROOT / "data" / "trader.db"
STATE = ROOT / "data" / "q_dissent_watch.state"
NTFY = "https://ntfy.sh/ollietrades-admin"

_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, socket.AF_INET, type, proto, flags)


def _last_id() -> int:
    try:
        return int(STATE.read_text().strip())
    except Exception:
        return 0


def _save(last_id: int) -> None:
    try:
        STATE.write_text(str(last_id))
    except Exception as e:
        print(f"[q-dissent-watch] state save failed: {e}", file=sys.stderr)


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
                "Tags": "cyclone,q",
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
        print(f"[q-dissent-watch] ntfy failed: {e}", file=sys.stderr)


def main() -> int:
    last = _last_id()
    try:
        conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True, timeout=15)
    except Exception as e:
        print(f"[q-dissent-watch] db open failed: {e}", file=sys.stderr)
        return 1
    try:
        # Table may not exist on a fresh deploy — treat as no dissents.
        if not conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='crew_dissent_log'"
        ).fetchone():
            return 0
        rows = conn.execute(
            "SELECT id, symbol, dissenter_call, consensus_call, dissent_magnitude, "
            "consensus_size, total_voters, created_at "
            "FROM crew_dissent_log WHERE dissenter='Q' AND id > ? ORDER BY id ASC",
            (last,),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        return 0

    max_id = last
    for rid, sym, call, cons, mag, csize, total, ts in rows:
        _ntfy(
            f"🌀 Q dissent — {sym}",
            f"Q is dissenting {call} vs the crew's {cons} on {sym} "
            f"[{mag}, {csize}/{total} agree] at {ts}. "
            f"Q's first live disagreements are accruing — watch the dissent-accuracy stats.",
        )
        max_id = max(max_id, rid)
    _save(max_id)
    print(f"[q-dissent-watch] NTFYed {len(rows)} new Q dissent(s); last_id={max_id}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
