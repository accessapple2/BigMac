#!/usr/bin/env python3
"""HM-SIGNALS-V2-STARVATION-RECURRENCE Monday verification (2026-07-12, one-shot).

Answers the queued watch-item question from docs/XO_BACKLOG.md
(HM-SIGNALS-V2-STARVATION-RECURRENCE, filed 2026-07-12): after Monday's
market open, are the 140 rows that sat idle all weekend
(data/backups/hm_signals_v2_monday_check_baseline_20260712.json --
ids 67350-67489, all active-source, dated 2026-07-10 20:01:52 through
2026-07-11 02:59:23) actually draining, or getting permanently
newest-first-outranked by Monday's fresh same-day signal volume the same
way two prior backlogs required one-time archive cleanups (2026-07-06
commit aa55f1d, 2026-07-09 commit b3e9ade)?

Read-only against trader.db (three plain SELECTs). Writes only this
script's own relay report + an XO_BACKLOG.md append -- same convention as
every other one-shot HM-* script in this repo. Meant to be fired once by
a self-removing cron entry Monday morning, not run repeatedly, but is
safe to re-run (the XO_BACKLOG append is guarded against duplication).

Deliberately does NOT git add/commit/push -- "no auto-push by design" is a
standing repo doctrine (there is no automatic git push anywhere in this
system; HM-PUSH-HEALTH-MONITOR is the independent safety net for lag, not
a green light to add push automation). The files are left as plain
uncommitted writes on disk for the next live session to review and commit.

Verdict logic (reported, not auto-decided -- ticket closure needs
Admiral sign-off per its own text):
  - 0 baseline rows still pending -> drained cleanly, recommend closing
    the ticket as "didn't recur this time."
  - baseline rows still pending AND newer (higher-id) rows from the same
    sources have already reached a terminal status -> confirmed
    outranked, recommend the ticket becomes active work.
  - anything in between -> report the exact numbers, no verdict forced.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

DB_PATH = ROOT / "data" / "trader.db"
BASELINE_PATH = ROOT / "data" / "backups" / "hm_signals_v2_monday_check_baseline_20260712.json"
RELAY_PATH = ROOT / "data" / "reports" / "relay" / "relay_2026-07-13_signals-v2-monday-check.md"
BACKLOG_PATH = ROOT / "docs" / "XO_BACKLOG.md"
BACKLOG_MARKER = "**Monday check result (2026-07-13"
CRON_MARKER = "hm_signals_v2_monday_check.py"  # unique substring used to self-remove the cron line


def _remove_own_cron() -> None:
    """Same self-removal pattern as scripts/iren_flip_watch.py: filter this
    script's own line out of crontab by unique marker substring, rewrite."""
    result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
    if result.returncode != 0:
        print("crontab -l failed, cannot self-remove cron entry", file=sys.stderr)
        return
    kept = [ln for ln in result.stdout.splitlines() if CRON_MARKER not in ln]
    new_crontab = "\n".join(kept) + "\n"
    subprocess.run(["crontab", "-"], input=new_crontab, text=True, check=True)


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    import sqlite3

    from engine.market_calendar import market_hours_elapsed

    baseline = json.loads(BASELINE_PATH.read_text())
    baseline_ids = [row["id"] for row in baseline]
    baseline_max_id = max(baseline_ids)

    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row

    placeholders = ",".join("?" * len(baseline_ids))
    baseline_now = conn.execute(
        f"SELECT id, status FROM signals_v2 WHERE id IN ({placeholders})",
        baseline_ids,
    ).fetchall()
    status_counts: dict[str, int] = {}
    for row in baseline_now:
        status_counts[row["status"]] = status_counts.get(row["status"], 0) + 1
    still_pending = status_counts.get("pending", 0)
    drained = len(baseline_ids) - still_pending

    pending_total = conn.execute(
        "SELECT COUNT(*) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]
    oldest = conn.execute(
        "SELECT MIN(created_at) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]

    # Direct outranking evidence: newer rows (higher id than the whole
    # baseline batch) that already reached a terminal status while
    # baseline rows are still sitting pending.
    newer_terminal = conn.execute(
        "SELECT COUNT(*) FROM signals_v2 "
        "WHERE id > ? AND status IN ('executed','expired','stale','cancelled','failed')",
        (baseline_max_id,),
    ).fetchone()[0]
    conn.close()

    oldest_age_hours = None
    oldest_age_market_hours = None
    if oldest:
        oldest_dt = datetime.strptime(oldest, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        oldest_age_hours = (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 3600.0
        oldest_age_market_hours = market_hours_elapsed(oldest_dt)

    if still_pending == 0:
        verdict = (
            "DRAINED CLEANLY. All 140 baseline rows transitioned out of pending. "
            "Recommend closing HM-SIGNALS-V2-STARVATION-RECURRENCE as "
            "\"didn't recur this time\" -- no code change needed."
        )
    elif still_pending > 0 and newer_terminal > 0:
        verdict = (
            f"CONFIRMED OUTRANKED. {still_pending}/{len(baseline_ids)} baseline rows are "
            f"still pending while {newer_terminal} newer row(s) (id > {baseline_max_id}) "
            "already reached a terminal status ahead of them -- the newest-first + "
            "drain-cap mechanism is recurring exactly as the ticket predicted. "
            "Recommend HM-SIGNALS-V2-STARVATION-RECURRENCE becomes active work "
            "(needs Admiral sign-off on candidate fix (a) TTL vs (b) hybrid ordering)."
        )
    else:
        verdict = (
            f"INCONCLUSIVE. {still_pending}/{len(baseline_ids)} baseline rows still pending, "
            f"{drained} drained, no direct newer-row-outranking evidence yet "
            f"({newer_terminal} newer terminal rows). Worth another look later in the "
            "session rather than a same-morning verdict."
        )

    report = f"""# Relay: HM-SIGNALS-V2-STARVATION-RECURRENCE Monday check

**Date:** 2026-07-13 (automated one-shot cron, HM-SIGNALS-V2-STARVATION-RECURRENCE)
**Baseline:** {BASELINE_PATH.name} -- 140 rows, ids {min(baseline_ids)}-{baseline_max_id},
83 ollama-plutus + 57 ollama-qwen3, dated 2026-07-10 20:01:52 - 2026-07-11 02:59:23.

## What was asked

Queued from `docs/XO_BACKLOG.md` (`HM-SIGNALS-V2-STARVATION-RECURRENCE`, filed
2026-07-12): after Monday's open, are the 140 weekend-idle rows draining, or
being permanently outranked by newest-first ordering the same way two prior
backlogs required one-time archive cleanups?

## Result

```
baseline rows still pending:     {still_pending} / {len(baseline_ids)}
baseline rows transitioned:      {drained} / {len(baseline_ids)}
by transitioned status:          {status_counts}
current total pending (all):     {pending_total}
current oldest-pending age:      {f'{oldest_age_hours:.1f}h wall-clock, {oldest_age_market_hours:.1f} market-hours' if oldest_age_hours is not None else 'n/a (queue empty)'}
newer (id>{baseline_max_id}) rows already terminal: {newer_terminal}
```

**Verdict:** {verdict}

## Open items

Ticket status left as-is in `docs/XO_BACKLOG.md` (still 🔵, not auto-closed
or auto-escalated) -- this report records the verified numbers; closing or
escalating the ticket needs Admiral sign-off per its own text.
"""

    print(report)

    if dry_run:
        print("[monday-check] --dry-run: no files written, no git operations.")
        return 0

    RELAY_PATH.parent.mkdir(parents=True, exist_ok=True)
    RELAY_PATH.write_text(report)

    if BACKLOG_MARKER not in BACKLOG_PATH.read_text():
        backlog_note = (
            f"\n{BACKLOG_MARKER} 07:00 MST, automated one-shot):** "
            f"{still_pending}/{len(baseline_ids)} baseline rows still pending, "
            f"{newer_terminal} newer rows already terminal. {verdict} "
            f"Full numbers in `data/reports/relay/{RELAY_PATH.name}`.\n"
        )
        text = BACKLOG_PATH.read_text()
        marker_anchor = "Needs Admiral sign-off before either candidate fix is built.\n"
        if marker_anchor in text:
            text = text.replace(marker_anchor, marker_anchor + backlog_note, 1)
            BACKLOG_PATH.write_text(text)
        else:
            print("[monday-check] WARNING: couldn't find anchor in XO_BACKLOG.md, "
                  "skipping backlog append (relay report still written).", file=sys.stderr)
    else:
        print("[monday-check] backlog already has a Monday-check note, not duplicating.")

    print(f"[monday-check] wrote {RELAY_PATH} and appended docs/XO_BACKLOG.md -- "
          f"NOT committed or pushed (no-auto-push doctrine). Review + commit in "
          f"the next live session.")

    _remove_own_cron()
    print("[monday-check] cron entry self-removed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
