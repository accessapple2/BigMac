"""engine/bridge_staleness.py — HM-BRIDGE-STALENESS-CONTRACT-2026-09-13.

One stamp for every Bridge panel payload: as_of / source / age_hours / stale.
The front-end has one matching render rule (otStaleApply/otStaleText in
dashboard/static/index.html and bridge-v2.html): stale -> grey the data and say
"STALE · as of <as_of>", never hide it. Display-only; nothing here writes.
"""
from __future__ import annotations

from datetime import datetime, timezone


def age_hours(as_of, naive_tz: str = "utc", now: datetime | None = None) -> float | None:
    """Hours since `as_of`, or None if missing/unparseable (a missing timestamp must
    never read as fresh). Accepts ISO strings (' ' or 'T', optional 'Z'/offset), epoch
    seconds, or datetimes. `naive_tz` says how to read a tz-less value: "utc" (SQLite
    datetime('now')) or "local" (Python datetime.now().isoformat()) — this repo's
    writers use both."""
    if as_of is None or as_of == "":
        return None
    now = now or datetime.now(timezone.utc)
    try:
        if isinstance(as_of, datetime):
            dt = as_of
        elif isinstance(as_of, (int, float)):
            dt = datetime.fromtimestamp(float(as_of), tz=timezone.utc)
        else:
            s = str(as_of).strip().replace(" ", "T", 1)
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc) if naive_tz == "utc" else dt.astimezone()
        return (now - dt).total_seconds() / 3600.0
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def stamp(payload: dict, as_of, source: str, max_age_hours: float, naive_tz: str = "utc") -> dict:
    """Return a copy of `payload` plus the staleness contract fields. Never mutates
    `payload` (callers pass cached dicts)."""
    age = age_hours(as_of, naive_tz=naive_tz)
    out = dict(payload)
    if isinstance(as_of, datetime):
        out["as_of"] = as_of.isoformat()
    elif isinstance(as_of, (int, float)):
        out["as_of"] = datetime.fromtimestamp(float(as_of), tz=timezone.utc).isoformat()
    else:
        out["as_of"] = as_of or None
    out["source"] = source
    out["age_hours"] = round(age, 2) if age is not None else None
    out["stale"] = age is None or age >= max_age_hours
    return out


def gex_max_age_hours() -> float:
    """GEX panels share canonical_gex's one staleness bar, so display can't drift from
    what the gates reason over."""
    from engine.canonical_gex import CANONICAL_GEX_MAX_AGE_DAYS
    return CANONICAL_GEX_MAX_AGE_DAYS * 24.0
