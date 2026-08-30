#!/usr/bin/env python3
"""PROPOSED -- HM-DEPARTURE-HARDENING Phase 1 item 4b (ollie-machine gate).
Not yet in crontab; standalone until approved.

Pre-committed gate (docs/XO_BACKLOG.md HM-OLLIE-MACHINE-KILLGATE, Admiral
decision 2026-07-05): if ollie-machine has recorded zero trades (checking
BOTH `trades` and `options_trades` -- see HM-SWEEP-SIGNALS-TABLE-BLIND-SPOT,
options/CSP agents are invisible to a trades-only sweep) by 2026-07-24, a
halt proposal goes to the Admiral. If it has traded, re-assess on the
merits like any other candidate -- this script does not make that call,
only reports which branch applies.

COMPUTE AND PUSH ONLY -- never halts, pauses, or modifies ollie-machine.
Not to be confused with the separate, fleet-wide Door-1 G1-G4 gate
(door1_kill_gate_check.py) -- same date, different, single-agent trigger.

Proposed schedule: cron daily, e.g. `0 14 * * 1-5` (same slot as
eod_report.py) or piggyback directly onto the Door-1 script's cron line
since they share a gate date -- Admiral's call.
"""
from __future__ import annotations

import sqlite3
import subprocess
import sys
from datetime import date
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
DB_PATH = REPO / "data" / "trader.db"
NTFY_TOPIC = "ollietrades-admin"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

PLAYER_ID = "ollie-machine"
GATE_DATE = date(2026, 7, 24)


def trade_counts() -> dict:
    conn = sqlite3.connect(str(DB_PATH))
    try:
        equity_n = conn.execute(
            "SELECT COUNT(*) FROM trades WHERE player_id=?", (PLAYER_ID,)
        ).fetchone()[0]
        options_n = conn.execute(
            "SELECT COUNT(*) FROM options_trades WHERE agent_id=?", (PLAYER_ID,)
        ).fetchone()[0]
    finally:
        conn.close()
    return {"trades": equity_n, "options_trades": options_n, "total": equity_n + options_n}


def ntfy_post(message: str, priority: str = "default", title: str = "") -> None:
    try:
        subprocess.run(
            ["curl", "-s", "-H", f"Priority: {priority}"] +
            (["-H", f"Title: {title}"] if title else []) +
            ["-d", message, NTFY_URL],
            capture_output=True, timeout=15,
        )
    except Exception:
        pass


def main() -> int:
    today = date.today()
    counts = trade_counts()
    has_traded = counts["total"] > 0
    is_gate_day_or_later = today >= GATE_DATE
    days_remaining = (GATE_DATE - today).days

    if has_traded:
        message = (
            f"ollie-machine gate check ({today.isoformat()}): HAS TRADED "
            f"({counts['trades']} trades, {counts['options_trades']} options_trades). "
            f"Re-assess on the merits like any other candidate -- no halt trigger."
        )
        title = "ollie-machine: active, re-assess on merits"
        priority = "default"
    elif is_gate_day_or_later:
        message = (
            f"ollie-machine gate check ({today.isoformat()}): ZERO trades in "
            f"trades or options_trades since creation (2026-06-01), gate date "
            f"{GATE_DATE.isoformat()} has passed. HALT PROPOSAL per Admiral's "
            f"2026-07-05 decision (docs/XO_BACKLOG.md HM-OLLIE-MACHINE-KILLGATE)."
        )
        title = "ollie-machine: HALT PROPOSAL (kill gate triggered)"
        priority = "urgent"
    else:
        message = (
            f"ollie-machine gate check ({today.isoformat()}): zero trades so "
            f"far, {days_remaining} day(s) until the {GATE_DATE.isoformat()} "
            f"gate date. No action yet."
        )
        title = "ollie-machine: still silent, gate pending"
        priority = "default"

    print(message)
    ntfy_post(message, priority=priority, title=title)
    return 0


if __name__ == "__main__":
    sys.exit(main())
