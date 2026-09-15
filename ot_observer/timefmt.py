"""Timestamp labelling. Every timestamp leaves the observer as UTC and MST, both labelled,
with how it was stored. Arizona (America/Phoenix) is UTC-7 all year -- no DST.

Stored bases, verified against the writers on 2026-09-14:
- "utc": SQLite datetime('now') / CURRENT_TIMESTAMP defaults, or explicit UTC writers.
- "mst_local": naive datetime.now() writers and the rich / logging log formats.
- "halted_at": ai_players.halted_at has two writers -- fleet_lifecycle.py stores
  datetime.now().isoformat() (local, 'T' separator + fraction); proving_ground and manual
  halts store CURRENT_TIMESTAMP (UTC, space separator). Read per value.
- "unverified": basis not yet checked against its writer; no zone is guessed.
"""
from __future__ import annotations

from datetime import datetime, timezone
from zoneinfo import ZoneInfo

MST = ZoneInfo("America/Phoenix")


def label(dt: datetime) -> dict:
    """An aware datetime as {"utc": ...Z, "mst": ... MST}, whole seconds."""
    utc = dt.astimezone(timezone.utc)
    return {
        "utc": utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "mst": utc.astimezone(MST).strftime("%Y-%m-%d %H:%M:%S MST"),
    }


def now_label() -> dict:
    return label(datetime.now(timezone.utc))


def _stored_as(raw: str, basis: str) -> str:
    if basis == "halted_at":
        return "mst_local" if "T" in raw else "utc"
    if basis in ("utc", "mst_local"):
        return basis
    raise ValueError(f"unknown timestamp basis: {basis}")


def from_db(value: object, basis: str) -> dict | None:
    """A stored timestamp value labelled in both zones. None stays None."""
    if value is None:
        return None
    raw = value if isinstance(value, str) else str(value)
    if basis == "unverified":
        return {"utc": None, "mst": None, "stored_as": "unverified_basis", "raw": raw}
    try:
        parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
    except ValueError:
        return {"utc": None, "mst": None, "stored_as": "unparseable", "raw": raw}
    if parsed.tzinfo is not None:
        return {**label(parsed), "stored_as": "explicit_offset", "raw": raw}
    stored_as = _stored_as(raw, basis)
    zone = timezone.utc if stored_as == "utc" else MST
    return {**label(parsed.replace(tzinfo=zone)), "stored_as": stored_as, "raw": raw}


def from_log_mst(naive_mst: datetime) -> dict:
    """A naive local (MST) log timestamp labelled in both zones."""
    return {**label(naive_mst.replace(tzinfo=MST)), "stored_as": "mst_local"}


def from_aware(dt: datetime) -> dict:
    return {**label(dt), "stored_as": "explicit_offset"}


def mst_naive(dt_utc: datetime) -> datetime:
    return dt_utc.astimezone(MST).replace(tzinfo=None)
