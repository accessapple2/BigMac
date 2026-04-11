"""
Deflector Shield Status — Pre-trade guard for scheduled market-moving events.
Fetches from FRED API and Finnhub earnings calendar to block trading near high-impact events.
"""
from __future__ import annotations

import json
import os
import sqlite3
import time
import requests
from datetime import datetime, timezone, timedelta
from typing import Optional

from config import FRED_API_KEY
from engine.finnhub_data import _fh_get

DB = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))

# Arizona = UTC-7, no DST
AZ_OFFSET = timedelta(hours=-7)
ET_OFFSET = timedelta(hours=-4)  # EDT; use -5 for EST — close enough for pre-market

# Cache: 1 hour TTL (events don't change during the day)
_cache: dict = {}
TTL = 3600

SPY_TOP50 = set(
    "AAPL,MSFT,NVDA,AMZN,GOOGL,META,BRK.B,LLY,AVGO,TSLA,WMT,JPM,V,UNH,XOM,"
    "ORCL,MA,HD,PG,COST,JNJ,ABBV,BAC,NFLX,KO,CVX,MRK,AMD,CRM,PEP,TMO,ADBE,"
    "LIN,ACN,MCD,ABT,PM,GE,IBM,TXN,GS,RTX,CAT,ISRG,SPGI,BKNG,AXP,DHR,AMGN,NOW".split(",")
)

CRITICAL_KEYWORDS = ["federal open market committee", "fomc", "consumer price index",
                     "nonfarm payroll", "employment situation"]
HIGH_KEYWORDS = ["gross domestic product", "personal consumption", "pce",
                 "producer price", "retail sales", "initial claims"]
MEDIUM_KEYWORDS = ["housing", "durable goods", "ism", "pmi", "consumer confidence"]

# Static fallback events (approximate times, ET)
STATIC_EVENTS = [
    {"name": "FOMC Meeting", "month_day": (1, 28), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (3, 18), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (5, 6),  "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (6, 17), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (7, 29), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (9, 16), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (10, 28), "time": "14:00", "impact": "CRITICAL"},
    {"name": "FOMC Meeting", "month_day": (12, 9), "time": "14:00", "impact": "CRITICAL"},
]


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.row_factory = sqlite3.Row
    return conn


def _ensure_table():
    try:
        conn = _conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS market_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_date TEXT NOT NULL,
                event_name TEXT NOT NULL,
                event_time TEXT,
                impact TEXT NOT NULL,
                source TEXT NOT NULL,
                description TEXT,
                created_at TEXT NOT NULL DEFAULT (datetime('now')),
                UNIQUE(event_date, event_name)
            )
        """)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[EventShield] DB init error: {e}")


def _classify_fred_impact(name: str, is_thursday: bool) -> str:
    lower = name.lower()
    if any(k in lower for k in CRITICAL_KEYWORDS):
        return "CRITICAL"
    if any(k in lower for k in HIGH_KEYWORDS):
        if "initial claims" in lower and not is_thursday:
            return "MEDIUM"
        return "HIGH"
    if any(k in lower for k in MEDIUM_KEYWORDS):
        return "MEDIUM"
    return "LOW"


def _fetch_fred_events(today_str: str, is_thursday: bool) -> list[dict]:
    events = []
    if not FRED_API_KEY:
        return events
    try:
        url = "https://api.stlouisfed.org/fred/releases/dates"
        params = {
            "realtime_start": today_str,
            "realtime_end": today_str,
            "api_key": FRED_API_KEY,
            "file_type": "json",
            "include_release_dates_with_no_data": "false",
        }
        resp = requests.get(url, params=params, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        for item in data.get("release_dates", []):
            name = item.get("release_name", "Unknown Release")
            impact = _classify_fred_impact(name, is_thursday)
            events.append({
                "event_date": today_str,
                "event_name": name,
                "event_time": None,  # FRED doesn't provide exact times
                "impact": impact,
                "source": "FRED",
                "description": f"Release ID {item.get('release_id', '?')}",
            })
    except Exception as e:
        print(f"[EventShield] FRED fetch error: {e}")
    return events


def _fetch_finnhub_earnings(today_str: str) -> list[dict]:
    events = []
    try:
        data = _fh_get("/calendar/earnings", {"from": today_str, "to": today_str})
        if not data:
            return events
        for item in data.get("earningsCalendar", []):
            symbol = item.get("symbol", "")
            if symbol in SPY_TOP50:
                hour = item.get("hour", "")
                # BMO = before market open ~8:30 ET, AMC = after close
                event_time = "08:30" if hour == "BMO" else ("16:00" if hour == "AMC" else None)
                events.append({
                    "event_date": today_str,
                    "event_name": f"{symbol} Earnings",
                    "event_time": event_time,
                    "impact": "HIGH",
                    "source": "FINNHUB",
                    "description": f"EPS est: {item.get('epsEstimate', 'N/A')} | {hour}",
                })
    except Exception as e:
        print(f"[EventShield] Finnhub earnings error: {e}")
    return events


def _fetch_static_events(today_str: str) -> list[dict]:
    """Return any static known events matching today."""
    try:
        today = datetime.strptime(today_str, "%Y-%m-%d")
        events = []
        for ev in STATIC_EVENTS:
            month, day = ev["month_day"]
            if today.month == month and today.day == day:
                events.append({
                    "event_date": today_str,
                    "event_name": ev["name"],
                    "event_time": ev["time"],
                    "impact": ev["impact"],
                    "source": "STATIC",
                    "description": "Known US market calendar event",
                })
        return events
    except Exception:
        return []


def _store_events(events: list[dict]):
    if not events:
        return
    try:
        conn = _conn()
        for ev in events:
            conn.execute(
                """INSERT OR IGNORE INTO market_events
                   (event_date, event_name, event_time, impact, source, description)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (ev["event_date"], ev["event_name"], ev.get("event_time"),
                 ev["impact"], ev["source"], ev.get("description")),
            )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[EventShield] DB store error: {e}")


def fetch_todays_events(force: bool = False) -> list[dict]:
    """Fetch and cache today's market events. Returns list of event dicts."""
    now = time.time()
    if not force and _cache.get("ts") and (now - _cache["ts"]) < TTL:
        return list(_cache["data"])

    _ensure_table()

    # Use Arizona time to determine today's date
    now_az = datetime.now(timezone.utc).astimezone(timezone(AZ_OFFSET))
    today_str = now_az.strftime("%Y-%m-%d")
    is_thursday = now_az.weekday() == 3

    all_events: list[dict] = []
    all_events.extend(_fetch_fred_events(today_str, is_thursday))
    all_events.extend(_fetch_finnhub_earnings(today_str))
    all_events.extend(_fetch_static_events(today_str))

    # Deduplicate by name
    seen = set()
    unique_events = []
    for ev in all_events:
        key = ev["event_name"]
        if key not in seen:
            seen.add(key)
            unique_events.append(ev)

    _store_events(unique_events)

    _cache["ts"] = now
    _cache["data"] = unique_events
    return list(unique_events)


def _event_datetime_et(event: dict, today_str: str) -> Optional[datetime]:
    """Return event as a tz-aware ET datetime, or None if no time available."""
    raw_time = event.get("event_time")
    if not raw_time:
        raw_time = "06:00"  # pre-market default
    try:
        dt_naive = datetime.strptime(f"{today_str} {raw_time}", "%Y-%m-%d %H:%M")
        return dt_naive.replace(tzinfo=timezone(ET_OFFSET))
    except Exception:
        return None


def get_advisor_cap(now_et: Optional[datetime] = None) -> tuple[float, str]:
    """
    Called by Counselor Troi. Returns (multiplier_cap, reason_str).
    Uses Arizona time internally; displays times in ET.
    """
    if now_et is None:
        now_et = datetime.now(timezone.utc).astimezone(timezone(ET_OFFSET))

    now_az = datetime.now(timezone.utc).astimezone(timezone(AZ_OFFSET))
    today_str = now_az.strftime("%Y-%m-%d")

    events = fetch_todays_events()

    # Impact priority order
    impact_order = {"CRITICAL": 4, "HIGH": 3, "MEDIUM": 2, "LOW": 1}

    best_critical = None
    best_high = None
    best_medium = None

    for ev in events:
        ev_dt = _event_datetime_et(ev, today_str)
        if ev_dt is None:
            continue
        diff = (ev_dt - now_et).total_seconds() / 60  # minutes; negative = past
        within_30 = -30 <= diff <= 30
        impact = ev.get("impact", "LOW")

        if impact == "CRITICAL":
            if best_critical is None or abs(diff) < abs(
                (_event_datetime_et(best_critical, today_str) - now_et).total_seconds() / 60
            ):
                best_critical = ev
            if within_30:
                label = "imminent" if diff >= 0 else "just passed"
                return (0.0, f"STAND DOWN: {ev['event_name']} {label}")

        elif impact == "HIGH":
            if best_high is None:
                best_high = ev
            if within_30:
                return (0.0, f"STAND DOWN: {ev['event_name']} imminent")

        elif impact == "MEDIUM":
            if best_medium is None:
                best_medium = ev

    if best_critical:
        ev_dt = _event_datetime_et(best_critical, today_str)
        time_str = ev_dt.strftime("%H:%M ET") if ev_dt else "today"
        return (0.25, f"WARNING CRITICAL EVENT: {best_critical['event_name']} at {time_str}")

    if best_high:
        ev_dt = _event_datetime_et(best_high, today_str)
        time_str = ev_dt.strftime("%H:%M ET") if ev_dt else "today"
        return (0.5, f"WARNING HIGH IMPACT: {best_high['event_name']} at {time_str}")

    if best_medium:
        return (0.75, f"INFO MEDIUM EVENT: {best_medium['event_name']}")

    return (1.0, "")


def get_event_shield_status() -> dict:
    """Returns current shield status for API endpoint."""
    now_et = datetime.now(timezone.utc).astimezone(timezone(ET_OFFSET))
    now_az = datetime.now(timezone.utc).astimezone(timezone(AZ_OFFSET))
    today_str = now_az.strftime("%Y-%m-%d")

    events = fetch_todays_events()
    multiplier_cap, reason = get_advisor_cap(now_et)

    # Find next upcoming event
    next_event = None
    min_minutes = float("inf")
    for ev in events:
        ev_dt = _event_datetime_et(ev, today_str)
        if ev_dt is None:
            continue
        diff_min = (ev_dt - now_et).total_seconds() / 60
        if 0 <= diff_min < min_minutes:
            min_minutes = diff_min
            next_event = {
                "name": ev["event_name"],
                "time": ev.get("event_time") or "06:00",
                "impact": ev["impact"],
                "minutes_until": int(diff_min),
            }

    shield_active = multiplier_cap < 1.0 and any(
        ev["impact"] in ("CRITICAL", "HIGH") for ev in events
    )
    # Shield active if any CRITICAL/HIGH within 2 hours
    for ev in events:
        ev_dt = _event_datetime_et(ev, today_str)
        if ev_dt and ev["impact"] in ("CRITICAL", "HIGH"):
            diff_min = (ev_dt - now_et).total_seconds() / 60
            if 0 <= diff_min <= 120:
                shield_active = True
                break

    return {
        "events_today": events,
        "next_event": next_event,
        "shield_active": shield_active,
        "multiplier_cap": multiplier_cap,
        "reason": reason,
        "all_clear": not shield_active and not events,
        "fetched_at": now_et.isoformat(),
    }


def run_event_shield_daily():
    """Called at 7:00 AM ET. Refreshes today's events."""
    fetch_todays_events(force=True)


# Fetch on module import (startup)
try:
    fetch_todays_events()
except Exception as e:
    print(f"[EventShield] Startup fetch error: {e}")
