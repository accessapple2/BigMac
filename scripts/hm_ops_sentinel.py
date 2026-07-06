#!/usr/bin/env python3
"""HM-FD-LOCK-SENTINEL (2026-07-06, post-incident prevention).

Independent watchdog for the failure mode behind today's morning lock storm
(HM-SQLITE-CONN-FD-LEAK / HM-RIKER-SYNTHESIS-LOCK-CONTENTION in
docs/XO_BACKLOG.md): leaked sqlite connections on main.py -> FD growth ->
lock contention -> "database is locked" errors feeding back into more leaks.

Runs as its own cron entry (5-10 min cadence), independent of main.py's own
scheduler, so a main.py hang or restart doesn't also kill the thing watching
it (see CLAUDE.md "Alarms must not share a failure mode with what they watch").

Four checks, each alerts independently (own alert_type, own rate limit):
  1. FD count on data/trader.db (+ -wal/-shm) for the main.py PID.
       WARNING at >150, RED_ALERT at >250 (Admiral-revised 2026-07-06 --
       the original 60/120 false-fired against an observed healthy
       140-170 plateau on day one of live sentinel data; revisit after a
       week of real readings). Trend context (delta over time) is included
       in the alert text either way so a firing is self-diagnosing.
  2. rikers_log heartbeat age (engine/riker_synthesis.py, cron */10).
       RED_ALERT if >25 min stale AND currently market hours.
  3. "database is locked" occurrences in trader_error.log in the last
       LOCK_WINDOW_MIN minutes. Any occurrence -> WARNING (this is the
       symptom the FD leak produces; catch it directly too).
  4. signals_v2 pending-queue depth + oldest-pending age
       (HM-SIGNALS-V2-FIFO-STARVATION). WARNING if pending>3000 or
       oldest-pending age >48h.

Exit codes:
  0 - all checks healthy
  2 - at least one alert fired
  1 - error running the checks themselves
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
ERROR_LOG = ROOT / "logs" / "trader_error.log"
STATE_PATH = ROOT / "data" / ".hm_ops_sentinel_state.json"

FD_WARN_THRESHOLD = 150   # Admiral-revised 2026-07-06: observed healthy plateau
FD_RED_THRESHOLD = 250    # 140-170 during a normal session; 60/120 false-fired daily.
# Revisit after a week of sentinel data once a real baseline is established.
HEARTBEAT_STALE_MIN = 25
LOCK_WINDOW_MIN = 10
QUEUE_PENDING_WARN = 3000
QUEUE_OLDEST_WARN_HOURS = 48


def _load_state() -> dict:
    try:
        return json.loads(STATE_PATH.read_text())
    except Exception:
        return {}


def _save_state(state: dict) -> None:
    try:
        STATE_PATH.write_text(json.dumps(state))
    except Exception as e:
        print(f"[sentinel] failed to persist state: {e}", file=sys.stderr)


def _main_pid() -> int | None:
    r = subprocess.run(
        ["pgrep", "-f", "[m]ain.py"], capture_output=True, text=True
    )
    pids = [p for p in r.stdout.split() if p.isdigit()]
    return int(pids[0]) if pids else None


def check_fd_count(alerts: list[tuple[str, str, str]]) -> dict:
    pid = _main_pid()
    if pid is None:
        alerts.append((
            "red_alert", "sentinel_main_py_down",
            "HM-OPS-SENTINEL: main.py is not running (pgrep found no match). "
            "trader_keepalive_cron should catch this within 5 min; flagging directly too.",
        ))
        return {"pid": None, "fd_count": None}

    r = subprocess.run(["lsof", "-p", str(pid)], capture_output=True, text=True)
    fd_count = sum(1 for line in r.stdout.splitlines() if "trader.db" in line)

    # Growth-rate context, keyed to this PID so a restart resets the baseline
    # instead of comparing across processes. A genuine leak climbs
    # monotonically; healthy load plateaus -- this makes a RED firing
    # self-diagnosing (still climbing = real; flat = re-tune the threshold)
    # without silently overriding the Admiral-specified static thresholds.
    state = _load_state()
    prev = state.get("fd_samples", {}).get(str(pid))
    now_iso = datetime.now(timezone.utc).isoformat()
    trend = ""
    if prev:
        delta = fd_count - prev["fd_count"]
        mins = max(1.0, (datetime.now(timezone.utc) - datetime.fromisoformat(prev["ts"])).total_seconds() / 60.0)
        trend = f" ({delta:+d} over {mins:.0f} min, {delta / mins:+.1f}/min)"
    state.setdefault("fd_samples", {})
    state["fd_samples"] = {str(pid): {"fd_count": fd_count, "ts": now_iso}}  # single-entry, this PID only
    _save_state(state)

    if fd_count > FD_RED_THRESHOLD:
        alerts.append((
            "red_alert", "sentinel_fd_red",
            f"HM-OPS-SENTINEL: main.py (PID {pid}) holds {fd_count} FDs on "
            f"trader.db/-wal/-shm (> {FD_RED_THRESHOLD} red threshold){trend}. "
            f"Same precursor pattern as this morning's HM-SQLITE-CONN-FD-LEAK "
            f"lock storm -- check for a new leak site before this compounds. "
            f"NOTE: if the trend is flat/negative, this is likely a healthy "
            f"market-hours plateau, not an active leak -- see HM-OPS-SENTINEL "
            f"filing in XO_BACKLOG for today's observed baseline.",
        ))
    elif fd_count > FD_WARN_THRESHOLD:
        alerts.append((
            "warning", "sentinel_fd_warn",
            f"HM-OPS-SENTINEL: main.py (PID {pid}) holds {fd_count} FDs on "
            f"trader.db/-wal/-shm (> {FD_WARN_THRESHOLD} warn threshold){trend}.",
        ))
    return {"pid": pid, "fd_count": fd_count}


def check_riker_heartbeat(alerts: list[tuple[str, str, str]]) -> dict:
    import sqlite3

    from engine.risk_manager import RiskManager

    conn = sqlite3.connect(DB_PATH, timeout=10)
    row = conn.execute("SELECT MAX(created_at) FROM rikers_log").fetchone()
    conn.close()
    last = row[0] if row else None
    if last is None:
        return {"last": None, "age_min": None}

    last_dt = datetime.strptime(last, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    age_min = (datetime.now(timezone.utc) - last_dt).total_seconds() / 60.0

    market_now = RiskManager.is_market_hours() == "market"
    if age_min > HEARTBEAT_STALE_MIN and market_now:
        alerts.append((
            "red_alert", "sentinel_riker_heartbeat",
            f"HM-OPS-SENTINEL: rikers_log heartbeat is {age_min:.0f} min stale "
            f"(last write {last} UTC) during market hours -- riker_synthesis "
            f"cron (*/10) may be failing its final persist (database is "
            f"locked was the root cause this morning).",
        ))
    return {"last": last, "age_min": round(age_min, 1)}


def check_lock_errors(alerts: list[tuple[str, str, str]]) -> dict:
    """Count "database is locked" occurrences appended since the last run.

    trader_error.log lines are "HH:MM:SS [LRS] message" with NO date, and the
    file spans multiple days between weekly rotations -- a line like
    "21:31:58 ..." from last night reads as numerically LATER than "12:11"
    right now, which would falsely look like "10 minutes ago" under a
    wall-clock time-window comparison. Sidestep the ambiguity entirely: track
    a persisted byte offset and only scan bytes appended since the previous
    sentinel run (this cron's own cadence, 5-10 min, IS the window). Resets
    to 0 if the file is smaller than the last offset (rotated).
    """
    if not ERROR_LOG.exists():
        return {"lock_errors": 0}

    state = _load_state()
    last_offset = state.get("error_log_offset", None)
    size = ERROR_LOG.stat().st_size

    if last_offset is None or last_offset > size:
        # First run, or file rotated since last run -- nothing to compare
        # against yet; start the checkpoint at EOF rather than scanning the
        # whole multi-day file (which would double-count the 07:xx storm).
        new_text = ""
    else:
        with ERROR_LOG.open("rb") as f:
            f.seek(last_offset)
            new_text = f.read().decode("utf-8", errors="replace")

    state["error_log_offset"] = size
    _save_state(state)

    count = sum(1 for line in new_text.splitlines() if "database is locked" in line)

    if count > 0:
        alerts.append((
            "warning", "sentinel_lock_errors",
            f"HM-OPS-SENTINEL: {count} \"database is locked\" occurrence(s) "
            f"in trader_error.log in the last {LOCK_WINDOW_MIN} min.",
        ))
    return {"lock_errors": count}


def check_signals_v2_queue(alerts: list[tuple[str, str, str]]) -> dict:
    import sqlite3

    conn = sqlite3.connect(DB_PATH, timeout=10)
    pending = conn.execute(
        "SELECT COUNT(*) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]
    oldest = conn.execute(
        "SELECT MIN(created_at) FROM signals_v2 WHERE status='pending'"
    ).fetchone()[0]
    conn.close()

    oldest_age_hours = None
    if oldest:
        oldest_dt = datetime.strptime(oldest, "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        oldest_age_hours = (datetime.now(timezone.utc) - oldest_dt).total_seconds() / 3600.0

    reasons = []
    if pending > QUEUE_PENDING_WARN:
        reasons.append(f"pending={pending} (> {QUEUE_PENDING_WARN})")
    if oldest_age_hours is not None and oldest_age_hours > QUEUE_OLDEST_WARN_HOURS:
        reasons.append(f"oldest-pending age={oldest_age_hours:.0f}h (> {QUEUE_OLDEST_WARN_HOURS}h)")

    if reasons:
        alerts.append((
            "warning", "sentinel_signals_v2_queue",
            f"HM-OPS-SENTINEL: signals_v2 pending queue -- {', '.join(reasons)}. "
            f"See HM-SIGNALS-V2-FIFO-STARVATION in docs/XO_BACKLOG.md "
            f"(known residual backlog, propose-first fix not yet applied).",
        ))
    return {
        "pending": pending,
        "oldest": oldest,
        "oldest_age_hours": round(oldest_age_hours, 1) if oldest_age_hours is not None else None,
    }


def _dispatch(alerts: list[tuple[str, str, str]], dry_run: bool = False) -> None:
    if dry_run:
        for level_kw, alert_type, message in alerts:
            print(f"[sentinel] [DRY RUN] would dispatch [{level_kw}/{alert_type}]: {message}")
        return
    try:
        from engine.alert_channels import AlertLevel, send_alert
        level_map = {"info": AlertLevel.INFO, "warning": AlertLevel.WARNING, "red_alert": AlertLevel.RED_ALERT}
        for level_kw, alert_type, message in alerts:
            send_alert(
                message=message,
                level=level_map[level_kw],
                alert_type=alert_type,
                rate_limit_secs=1800,  # 30 min -- cron runs every 5-10 min, don't spam every tick
            )
            print(f"[sentinel] ALERT dispatched [{level_kw}/{alert_type}]: {message[:100]}")
    except Exception as e:
        print(f"[sentinel] alert dispatch failed: {type(e).__name__}: {e}", file=sys.stderr)


def main() -> int:
    dry_run = "--dry-run" in sys.argv
    alerts: list[tuple[str, str, str]] = []
    try:
        fd_status = check_fd_count(alerts)
        heartbeat_status = check_riker_heartbeat(alerts)
        lock_status = check_lock_errors(alerts)
        queue_status = check_signals_v2_queue(alerts)
    except Exception as e:
        print(f"[sentinel] error running checks: {type(e).__name__}: {e}", file=sys.stderr)
        return 1

    print(f"[sentinel] fd={fd_status} heartbeat={heartbeat_status} "
          f"lock={lock_status} queue={queue_status}")

    if alerts:
        _dispatch(alerts, dry_run=dry_run)
        return 2

    print("[sentinel] OK -- all checks within threshold")
    return 0


if __name__ == "__main__":
    sys.exit(main())
