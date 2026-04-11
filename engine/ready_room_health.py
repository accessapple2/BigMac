"""
Dr. Crusher's Extended Scan — Ready Room & Red Alert Health Checks
-------------------------------------------------------------------
Checks that the Ready Room pipeline is populated and Red Alert is
polling. Call check_ready_room_health() from the existing healthcheck.

Returns a dict with pass/fail for each sub-check.
"""
from __future__ import annotations

import os
import sqlite3
import time
from datetime import date, datetime
from typing import Any
from urllib.request import urlopen, Request
from urllib.error import URLError

_TRADEMINDS_DB = os.path.expanduser("~/autonomous-trader/data/trader.db")
_ALERT_DB_PATH  = os.path.join(os.path.expanduser("~/autonomous-trader"), "autonomous_trader.db")
_DASHBOARD_URL  = "http://127.0.0.1:8080"


def _db_check(db_path: str, query: str, params: tuple = ()) -> Any:
    try:
        conn = sqlite3.connect(db_path, timeout=5)
        result = conn.execute(query, params).fetchone()
        conn.close()
        return result
    except Exception:
        return None


def check_ready_room_health() -> dict[str, Any]:
    """
    Extended scan for Ready Room + Red Alert systems.

    Returns:
      {
        "briefing_today":      bool,    # ready_room_briefings has row today
        "red_alert_polling":   bool,    # intraday_snapshots has row in last 10 min
        "condition_endpoint":  bool,    # /api/ready-room/condition responds
        "oi_snapshot_today":   bool,    # oi_changes has morning row today
        "vix_data_fresh":      bool,    # vix_term_structure has row in last hour
        "overall":             bool,    # all critical checks pass
        "details":             dict,    # per-check diagnostic info
      }
    """
    today        = date.today().isoformat()
    now_epoch    = time.time()
    details: dict[str, Any] = {}

    # ── 1. Ready Room briefing today ─────────────────────────────────────────
    row = _db_check(
        _TRADEMINDS_DB,
        "SELECT COUNT(*), MAX(created_at) FROM ready_room_briefings WHERE date(created_at) = ?",
        (today,),
    )
    briefing_count = row[0] if row else 0
    briefing_ts    = row[1] if row else None
    briefing_today = briefing_count >= 1
    details["briefing_today"] = {
        "pass": briefing_today,
        "count": briefing_count,
        "latest": briefing_ts,
        "message": f"{briefing_count} briefing(s) today" if briefing_today else "No briefing yet today",
    }

    # ── 2. Red Alert polling (row within last 10 min) ────────────────────────
    row2 = _db_check(
        _ALERT_DB_PATH,
        "SELECT MAX(created_at) FROM intraday_snapshots WHERE snap_date = ?",
        (today,),
    )
    last_snap_ts = row2[0] if row2 else None
    red_alert_ok = False
    snap_age_min = None
    if last_snap_ts:
        try:
            from datetime import datetime as _dt
            ts = _dt.fromisoformat(last_snap_ts.replace("Z", "+00:00"))
            snap_age_min = (now_epoch - ts.timestamp()) / 60
            red_alert_ok = snap_age_min < 10
        except Exception:
            pass
    details["red_alert_polling"] = {
        "pass": red_alert_ok,
        "last_snap": last_snap_ts,
        "age_min": round(snap_age_min, 1) if snap_age_min else None,
        "message": (f"Last poll {snap_age_min:.1f}m ago" if snap_age_min else "No snapshot today"),
    }

    # ── 3. Condition endpoint ─────────────────────────────────────────────────
    condition_ok = False
    condition_msg = "Not checked"
    try:
        req = Request(
            _DASHBOARD_URL + "/api/ready-room/condition",
            headers={"User-Agent": "DrCrusher-HealthCheck/1.0"},
        )
        with urlopen(req, timeout=5) as r:
            condition_ok = r.status == 200
            condition_msg = f"HTTP {r.status}"
    except Exception as exc:
        condition_msg = str(exc)[:60]
    details["condition_endpoint"] = {"pass": condition_ok, "message": condition_msg}

    # ── 4. OI snapshot today ──────────────────────────────────────────────────
    row3 = _db_check(
        _TRADEMINDS_DB,
        "SELECT COUNT(*) FROM oi_changes WHERE snap_date = ? AND snap_type = 'morning'",
        (today,),
    )
    oi_count = row3[0] if row3 else 0
    oi_today = oi_count > 0
    details["oi_snapshot_today"] = {
        "pass": oi_today,
        "strikes_saved": oi_count,
        "message": f"{oi_count} OI strikes saved" if oi_today else "Morning OI snapshot not yet taken",
    }

    # ── 5. VIX data freshness (row within last hour) ──────────────────────────
    row4 = _db_check(
        _TRADEMINDS_DB,
        "SELECT MAX(created_at) FROM vix_term_structure",
    )
    last_vix_ts = row4[0] if row4 else None
    vix_fresh = False
    vix_age_min = None
    if last_vix_ts:
        try:
            from datetime import datetime as _dt
            ts = _dt.fromisoformat(last_vix_ts.replace("Z", "+00:00"))
            vix_age_min = (now_epoch - ts.timestamp()) / 60
            vix_fresh = vix_age_min < 60
        except Exception:
            pass
    details["vix_data_fresh"] = {
        "pass": vix_fresh,
        "last_fetch": last_vix_ts,
        "age_min": round(vix_age_min, 1) if vix_age_min else None,
        "message": (f"VIX {vix_age_min:.0f}m old" if vix_age_min else "No VIX data"),
    }

    # ── Critical: briefing + condition endpoint ───────────────────────────────
    overall = briefing_today and condition_ok

    return {
        "briefing_today":    briefing_today,
        "red_alert_polling": red_alert_ok,
        "condition_endpoint": condition_ok,
        "oi_snapshot_today": oi_today,
        "vix_data_fresh":    vix_fresh,
        "overall":           overall,
        "details":           details,
    }


def auto_restart_red_alert() -> bool:
    """Attempt to restart the Red Alert polling thread if it has died."""
    try:
        from engine.red_alert import start_red_alert, _running
        if not _running:
            start_red_alert()
            return True
        return False
    except Exception:
        return False
