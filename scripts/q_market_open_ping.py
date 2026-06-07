#!/usr/bin/env python3
"""HM-Q-MARKET-OPEN-PING — Monday market-open reminder. NTFYs the Captain that Q
(the Grok War Room voice) is live now that the session is open, with a quick
status (Q's recent debate takes + dissent count). Complements q_dissent_watch.py
(which fires on each actual Q dissent); this is the "market's open, go look" ping.

Local cron (the remote /schedule can't read this local trader.db). Read-only DB,
stdlib only. Fires Monday at market open via crontab.
"""
import sqlite3
import sys
import urllib.request
from pathlib import Path

ROOT = Path("/Users/bigmac/autonomous-trader")
DB = ROOT / "data" / "trader.db"
NTFY = "https://ntfy.sh/ollietrades-admin"


def _ntfy(title: str, body: str) -> None:
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
        urllib.request.urlopen(req, timeout=8)
    except Exception as e:
        print(f"[q-open-ping] ntfy failed: {e}", file=sys.stderr)


def main() -> int:
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
