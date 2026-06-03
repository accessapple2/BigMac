"""HM-AT-γ Schwab cadence drift alert (2026-05-18).

Reads MAX(imported_at) from schwab_holdings; NTFYs ollietrades-admin
WARNING if the most recent snapshot is older than 48h. Prevents
recurrence of the 11-day silent-gap incident.

Fires daily via launchd com.ollietrades.schwab-cadence.plist (06:00 AZ).

Exit codes:
  0 - within cadence window OR alert fired successfully
  2 - alert fired (drift detected, NTFY sent)
  1 - error / DB unavailable / table missing
"""
from __future__ import annotations

import sqlite3
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

# HM-SCHWAB-CADENCE-DBPATH 2026-06-02: absolute path. Was relative "data/trader.db",
# which under cron's cwd ($HOME, no `cd` in the cron line) opened ~/data/trader.db
# (table-less) → every run errored "no such table: schwab_holdings" → the 48h staleness
# alarm silently no-op'd. Derive from __file__ so cwd can't break it.
DB_PATH = str(Path(__file__).resolve().parent.parent / "data" / "trader.db")
CADENCE_HOURS_WARN = 48


def _parse_ts(ts_str: str) -> datetime | None:
    """imported_at is `datetime('now')` UTC. Parse with timezone-naive UTC."""
    if not ts_str:
        return None
    fmts = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f")
    for fmt in fmts:
        try:
            return datetime.strptime(ts_str[:26], fmt).replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            continue
    return None


def main() -> int:
    try:
        conn = sqlite3.connect(DB_PATH)
        row = conn.execute(
            "SELECT MAX(imported_at) FROM schwab_holdings"
        ).fetchone()
        conn.close()
    except sqlite3.OperationalError as e:
        print(f"[schwab-cadence] DB error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"[schwab-cadence] unexpected error: {type(e).__name__}: {e!r}",
              file=sys.stderr)
        return 1

    latest_str = row[0] if row else None
    if not latest_str:
        # No rows at all — distinct from drift; treat as critical
        print("[schwab-cadence] schwab_holdings is empty — no snapshots ever imported")
        _alert(
            "Schwab cadence CRITICAL: schwab_holdings table is empty. "
            "No snapshots ever imported. Cron drift or schema mismatch.",
            "critical",
        )
        return 2

    latest = _parse_ts(latest_str)
    if latest is None:
        print(f"[schwab-cadence] unparseable imported_at: {latest_str!r}")
        return 1

    now = datetime.now(timezone.utc)
    age_hours = (now - latest).total_seconds() / 3600.0
    age_str = f"{age_hours:.1f}h"

    if age_hours <= CADENCE_HOURS_WARN:
        print(f"[schwab-cadence] OK — last import {age_str} ago (latest={latest_str})")
        return 0

    days = age_hours / 24.0
    msg = (
        f"Schwab cadence WARNING: last schwab_holdings import was {age_str} ago "
        f"({days:.1f} days, latest snapshot @ {latest_str}). "
        f"Cron drift suspected — investigate scripts/schwab_csv_watcher.sh + "
        f"com.ollietrades.schwab-watcher plist."
    )
    print(f"[schwab-cadence] DRIFT — {msg}")
    _alert(msg, "warning")
    return 2


def _alert(message: str, level_kw: str = "warning") -> None:
    """Send NTFY to ollietrades-admin. Uses engine.alert_channels when
    available; falls back to plain HTTP POST if not in venv import path."""
    try:
        from engine.alert_channels import send_alert, AlertLevel
        level_map = {
            "warning": AlertLevel.WARNING,
            "critical": AlertLevel.WARNING,  # CRITICAL not defined; reuse WARNING
            "info": AlertLevel.INFO,
        }
        level = level_map.get(level_kw, AlertLevel.WARNING)
        send_alert(
            message=message,
            level=level,
            alert_type=f"hm-at-gamma-schwab-cadence-{level_kw}",
            rate_limit_secs=86400,  # 24h per process; fine for daily cron
        )
        print("[schwab-cadence] NTFY dispatched via engine.alert_channels")
        return
    except Exception as e:
        print(f"[schwab-cadence] engine.alert_channels unavailable: {e}",
              file=sys.stderr)

    # Fallback — plain HTTP POST to ntfy.sh (matches alert_channels topic conventions)
    try:
        import urllib.request
        topic = "ollietrades-admin"
        req = urllib.request.Request(
            f"https://ntfy.sh/{topic}",
            data=message.encode("utf-8"),
            headers={"Title": "TradeMinds — Schwab cadence drift",
                     "Priority": "high",
                     "Tags": "warning,ollietrades"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as r:
            print(f"[schwab-cadence] fallback ntfy POST HTTP {r.status}")
    except Exception as e:
        print(f"[schwab-cadence] fallback ntfy failed: {e}", file=sys.stderr)


if __name__ == "__main__":
    sys.exit(main())
