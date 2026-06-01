"""engine/source_gate.py — Source Integrity Gate (Wave 1).

The single freshness registry + gate that every live-decision path consults.
Reused by BOTH services, so it must import cleanly under:
  - signal-center  Python 3.9  (server.py, Flask)
  - trader         Python 3.14 (engine/consensus.py)

Therefore: `Optional[...]` only (NO PEP 604 `X | None`), no heavy engine
imports. Reuses engine.market_calendar (also 3.9-safe) for market-aware age.

Public API:
  source_freshness(source_id)         -> dict (age, state, as_of, ...)
  is_usable(source_id)                -> bool   (LIVE gate: enabled + not RED live_decision)
  is_quarantined(source_id)           -> bool   (PROVENANCE gate: disabled or archive)
  all_health()                        -> dict   (grid for /api/sources/health)
  set_enabled(source_id, enabled)     -> bool   (manual quarantine flip)

Two distinct applications of one registry:
  * LIVE gating (consensus / router / alerts): is_usable() — freshness-based.
  * BACKLOG provenance (W0 expectancy stale_gated): is_quarantined() — was the
    producing source ever a legit live feed? Old-but-from-a-good-source signals
    are still scored; only quarantined/archive provenance is excluded.
"""
from __future__ import annotations

import fnmatch
import glob as _glob
import json
import os
import sqlite3
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

try:
    from engine import market_calendar as mc
except Exception:  # pragma: no cover - allow standalone use
    import market_calendar as mc  # type: ignore

# ── Paths ────────────────────────────────────────────────────────────────
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SIGNALS_DB = os.path.join(_ROOT, "signal-center", "signals.db")
TRADER_DB = os.path.join(_ROOT, "data", "trader.db")

# db_max sources default to signals.db; a `db_max:<db>:<table>.<col>` spec can
# point at another DB. Whitelisted keys only (no arbitrary paths).
_DB_PATHS = {"signals": SIGNALS_DB, "trader": TRADER_DB}

# Base cadence (days) for `snapshot`-class sources (§2.2).
SNAPSHOT_CADENCE_DAYS = {"schwab_snapshot": 3, "metals": 7}

# State constants
GREEN, AMBER, RED, UNKNOWN = "GREEN", "AMBER", "RED", "UNKNOWN"
RETIRED = "RETIRED"  # intentionally-inactive (superseded/retired) — NOT a RED fault

# ── Bridge (for bridge_iso ts resolution). No auth: read-only, localhost. ──
_BRIDGE = "http://127.0.0.1:8080"


# ── DB helper ──────────────────────────────────────────────────────────────
def _db(path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(path or SIGNALS_DB, timeout=10)
    conn.row_factory = sqlite3.Row
    return conn


def get_registry_row(source_id: str) -> Optional[Dict[str, Any]]:
    conn = _db()
    try:
        r = conn.execute(
            "SELECT * FROM source_registry WHERE source_id=?", (source_id,)
        ).fetchone()
        return dict(r) if r else None
    finally:
        conn.close()


# ── Timestamp parsing ────────────────────────────────────────────────────
def _parse_ts(raw: Any) -> Optional[datetime]:
    """Best-effort parse of a source timestamp into a naive UTC-ish datetime.

    Handles ISO ('2026-05-29 13:06:11' / '...T...'), date-only, and epoch.
    Returns None on failure (caller treats as UNKNOWN -> RED for gating)."""
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        # epoch seconds or ms
        val = float(raw)
        if val > 1e12:
            val /= 1000.0
        try:
            return datetime.utcfromtimestamp(val)
        except Exception:
            return None
    s = str(raw).strip()
    if not s:
        return None
    s = s.replace("Z", "").replace("T", " ")
    # trim fractional seconds beyond parsing
    for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d %H:%M", "%Y-%m-%d", "%Y.%m.%d"):
        try:
            return datetime.strptime(s[:26] if "." in s else s, fmt)
        except ValueError:
            continue
    return None


def _walk(obj: Any, dotpath: str) -> Any:
    cur = obj
    for part in dotpath.split("."):
        if isinstance(cur, dict):
            cur = cur.get(part)
        else:
            return None
    return cur


_TS_CANDIDATE_KEYS = (
    "created_at", "fetched_at", "generated_at", "timestamp",
    "updated_at", "as_of", "last_updated", "date", "session_time",
)


def _resolve_ts(ts_format: str, ts_field: str) -> Tuple[Optional[datetime], Optional[str]]:
    """Return (parsed_dt, raw_str). raw_str is the source's own as_of string."""
    if not ts_format or ts_format in ("manual", "none"):
        return None, None

    if ts_format.startswith("db_max:"):
        spec = ts_format[len("db_max:"):]
        # optional db selector: db_max:<db>:<table>.<col> (default signals)
        db_path = None
        if ":" in spec:
            db_key, _, spec = spec.partition(":")
            db_path = _DB_PATHS.get(db_key)
            if db_path is None:
                return None, None
        table, _, col = spec.partition(".")
        # whitelist identifiers (no injection)
        if not table.isidentifier() or not col.isidentifier():
            return None, None
        conn = _db(db_path)
        try:
            row = conn.execute(
                "SELECT max(%s) AS m FROM %s" % (col, table)  # nosec - identifiers validated
            ).fetchone()
            raw = row["m"] if row else None
            return _parse_ts(raw), (str(raw) if raw is not None else None)
        except Exception:
            return None, None
        finally:
            conn.close()

    if ts_format.startswith("bridge_iso:"):
        rest = ts_format[len("bridge_iso:"):]
        endpoint, _, dotpath = rest.partition(":")
        try:
            import requests  # local import; available in both envs
            resp = requests.get(_BRIDGE + endpoint, timeout=8)
            if resp.status_code != 200:
                return None, None
            data = resp.json()
        except Exception:
            return None, None
        raw = _walk(data, dotpath) if dotpath else None
        if raw is None and isinstance(data, dict):
            # fallback: scan common timestamp keys at top level
            for k in _TS_CANDIDATE_KEYS:
                if k in data and data[k]:
                    raw = data[k]
                    break
        return _parse_ts(raw), (str(raw) if raw is not None else None)

    if ts_format.startswith("file_mtime:"):
        pattern = ts_format[len("file_mtime:"):]
        try:
            matches = _glob.glob(pattern)
            if not matches:
                return None, None
            newest = max(matches, key=os.path.getmtime)
            mt = os.path.getmtime(newest)
            return datetime.utcfromtimestamp(mt), datetime.utcfromtimestamp(mt).isoformat()
        except Exception:
            return None, None

    return None, None


# ── Market-aware age ───────────────────────────────────────────────────────
def _last_session_close(now_utc: Optional[datetime] = None) -> datetime:
    """Most recent NYSE session close, as an ET-localized datetime."""
    now_et = mc._to_et(now_utc)
    d = now_et.date()
    close_t = mc.EARLY_CLOSE_TIME if mc.is_early_close_day(d) else mc.MARKET_CLOSE_TIME
    # If today is a trading day and we're at/after close, last close is today.
    if d.weekday() < 5 and not mc.is_us_market_holiday(d):
        today_close = mc.ET.localize(datetime.combine(d, close_t))
        if now_et >= today_close:
            return today_close
    # else step back to the previous trading day's close
    for _ in range(14):
        d = d - timedelta(days=1)
        if d.weekday() < 5 and not mc.is_us_market_holiday(d):
            ct = mc.EARLY_CLOSE_TIME if mc.is_early_close_day(d) else mc.MARKET_CLOSE_TIME
            return mc.ET.localize(datetime.combine(d, ct))
    return now_et  # pathological fallback


def _market_aware_age_seconds(as_of_dt: datetime, now_utc: Optional[datetime] = None) -> float:
    """Age in seconds, but for closed-market windows measure against the last
    session close (a source updated at/after last close is 'current')."""
    now_et = mc._to_et(now_utc)
    # localize as_of to ET (assume the source stamps in ET local time)
    if as_of_dt.tzinfo is None:
        as_of_et = mc.ET.localize(as_of_dt)
    else:
        as_of_et = as_of_dt.astimezone(mc.ET)
    if mc.is_us_market_open(now_utc):
        return max(0.0, (now_et - as_of_et).total_seconds())
    last_close = _last_session_close(now_utc)
    if as_of_et >= last_close:
        return 0.0  # updated since the market last closed -> current
    return max(0.0, (last_close - as_of_et).total_seconds())


# ── State mapping per cadence class (§2.2) ───────────────────────────────────
_DAY = 86400.0


def _classify(cadence_class: str, source_id: str, age_s: float) -> str:
    c = cadence_class
    if c == "realtime":
        return GREEN if age_s <= 60 else AMBER if age_s <= 300 else RED
    if c == "intraday":
        return GREEN if age_s <= 900 else AMBER if age_s <= 3600 else RED
    if c == "hourly":   # hourly producers (e.g. signal_outcomes scorecard) — the
        # intraday 15m-GREEN band is impossible for them; this band fits an hourly tick.
        return GREEN if age_s <= 5400 else AMBER if age_s <= 4 * 3600 else RED
    if c == "daily":
        return GREEN if age_s <= _DAY else AMBER if age_s <= 2 * _DAY else RED
    if c == "daily_batch":  # once-per-trading-day morning batch (e.g. shadow signal
        # bridge). GREEN within ~1 trading day of the last batch; small grace; RED once
        # >1 day with no new batch so a DEAD bridge still trips (market-aware age, so
        # weekends/holidays don't false-trip).
        return GREEN if age_s <= _DAY else AMBER if age_s <= _DAY + 10800 else RED
    if c == "weekly":
        return GREEN if age_s <= 7 * _DAY else AMBER if age_s <= 14 * _DAY else RED
    if c == "monthly":
        return GREEN if age_s <= 35 * _DAY else AMBER if age_s <= 45 * _DAY else RED
    if c == "snapshot":
        base = SNAPSHOT_CADENCE_DAYS.get(source_id, 3) * _DAY
        return GREEN if age_s <= base else AMBER if age_s <= 2 * base else RED
    if c == "archive":
        return RED  # archive is never live
    return UNKNOWN


def _human_age(age_s: float) -> str:
    if age_s < 90:
        return "%ds" % int(age_s)
    if age_s < 5400:
        return "%dm" % int(age_s // 60)
    if age_s < 36 * 3600:
        return "%dh" % int(age_s // 3600)
    return "%dd" % int(age_s // 86400)


# ── Public: freshness ────────────────────────────────────────────────────
def source_freshness(source_id: str, now_utc: Optional[datetime] = None) -> Dict[str, Any]:
    row = get_registry_row(source_id)
    if row is None:
        return {"source_id": source_id, "state": UNKNOWN, "error": "not_registered"}

    # RETIRED = intentionally inactive (superseded). Distinct from a RED fault:
    # not freshness-checked, never a gate input, surfaced as RETIRED in the grid.
    if row["criticality"] == "retired" or row["cadence_class"] == "retired":
        return {
            "source_id": source_id,
            "display_name": row["display_name"],
            "cadence_class": row["cadence_class"],
            "criticality": row["criticality"],
            "enabled": row["enabled"],
            "state": RETIRED,
            "as_of": None,
            "age_human": "retired",
            "quarantined": False,
            "retired": True,
            "notes": row["notes"],
        }

    if not row["enabled"] or row["cadence_class"] == "archive":
        return {
            "source_id": source_id,
            "display_name": row["display_name"],
            "cadence_class": row["cadence_class"],
            "criticality": row["criticality"],
            "enabled": row["enabled"],
            "state": RED if row["cadence_class"] == "archive" else UNKNOWN,
            "as_of": None,
            "age_human": "n/a",
            "quarantined": (not row["enabled"]),
        }

    as_of_dt, as_of_raw = _resolve_ts(row["ts_format"], row["ts_field"])
    if as_of_dt is None:
        state = UNKNOWN
        age_s = None
        age_human = "unknown"
    else:
        age_s = _market_aware_age_seconds(as_of_dt, now_utc)
        state = _classify(row["cadence_class"], source_id, age_s)
        age_human = _human_age(age_s)

    return {
        "source_id": source_id,
        "display_name": row["display_name"],
        "cadence_class": row["cadence_class"],
        "criticality": row["criticality"],
        "enabled": row["enabled"],
        "as_of": as_of_raw,
        "age_seconds": age_s,
        "age_human": age_human,
        "market_aware": not mc.is_us_market_open(now_utc),
        "state": state,
        "quarantined": False,
    }


# ── Public: gates ──────────────────────────────────────────────────────────
def is_usable(source_id: str, now_utc: Optional[datetime] = None) -> bool:
    """LIVE gate. A live_decision source that is RED/UNKNOWN is NOT usable.
    Context/archive criticality never blocks a live decision here (they're
    not decision inputs), but disabled sources are never usable."""
    row = get_registry_row(source_id)
    if row is None or not row["enabled"]:
        return False
    if row["cadence_class"] == "archive" or row["criticality"] == "archive":
        return False
    if row["criticality"] == "live_decision":
        st = source_freshness(source_id, now_utc)["state"]
        return st in (GREEN, AMBER)
    return True


def is_quarantined(source_id: str) -> bool:
    """PROVENANCE gate for backlog scoring: a source is quarantined if it is
    disabled or archive-class. Old-but-legit signals are NOT quarantined."""
    row = get_registry_row(source_id)
    if row is None:
        return False  # unknown provenance -> don't silently drop; counted elsewhere
    return (not row["enabled"]) or row["cadence_class"] == "archive" or row["criticality"] == "archive"


def all_health(now_utc: Optional[datetime] = None) -> Dict[str, Any]:
    conn = _db()
    try:
        ids = [r["source_id"] for r in conn.execute(
            "SELECT source_id FROM source_registry"
        ).fetchall()]
    finally:
        conn.close()
    sources = [source_freshness(sid, now_utc) for sid in ids]
    order = {RED: 0, UNKNOWN: 1, AMBER: 2, GREEN: 3, RETIRED: 4}
    sources.sort(key=lambda s: order.get(s.get("state"), 9))
    summary = {"green": 0, "amber": 0, "red": 0, "unknown": 0, "quarantined": 0,
               "retired": 0, "total": len(sources)}
    for s in sources:
        if s.get("quarantined"):
            summary["quarantined"] += 1
        st = s.get("state")
        if st == GREEN:
            summary["green"] += 1
        elif st == AMBER:
            summary["amber"] += 1
        elif st == RETIRED:
            summary["retired"] += 1
        elif st == UNKNOWN:
            # UNKNOWN-by-design (manual/idle snapshot sources: metals, schwab w/ no CSV)
            # is NOT a fault — own bucket, render grey, do NOT inflate the RED alarm count.
            summary["unknown"] += 1
        elif st == RED:
            summary["red"] += 1
    return {"summary": summary, "sources": sources}


def set_enabled(source_id: str, enabled: bool) -> bool:
    conn = _db()
    try:
        cur = conn.execute(
            "UPDATE source_registry SET enabled=? WHERE source_id=?",
            (1 if enabled else 0, source_id),
        )
        conn.commit()
        return cur.rowcount > 0
    finally:
        conn.close()


# ── W1 NTFY auto-quarantine tracker (2026-06-01) ────────────────────────────
# Report-only first (project pattern: ship gates default-off). The tracker fires
# throttled NTFY when a live_decision source flips RED or stays RED past a
# threshold; it RECOMMENDS quarantine but only auto-disables when the flag below
# is flipped on by the Admiral. Consecutive-RED is counted per tracker TICK
# (min-interval gated) not per poll, so frontend poll frequency can't inflate it.
AUTO_QUARANTINE_ENABLED = False          # flip True to let the tracker set_enabled(0)
RED_TICKS_TO_QUARANTINE = 3              # consecutive RED ticks before recommend/auto
_TRACKER_MIN_INTERVAL_S = 900           # advance at most every 15 min
_HEALTH_STATE_PATH = os.path.join(_ROOT, "data", "source_health_state.json")


def _load_health_state() -> Dict[str, Any]:
    try:
        import json as _json
        with open(_HEALTH_STATE_PATH) as f:
            return _json.load(f)
    except Exception:
        return {}


def _save_health_state(state: Dict[str, Any]) -> None:
    import json as _json
    import tempfile
    try:
        d = os.path.dirname(_HEALTH_STATE_PATH)
        fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
        with os.fdopen(fd, "w") as f:
            f.write(_json.dumps(state))
        os.replace(tmp, _HEALTH_STATE_PATH)
    except Exception:
        pass


def _quarantine_ntfy(title: str, message: str, priority: str = "high",
                     tags: str = "rotating_light") -> None:
    try:
        from engine.alert_channels import _send_ntfy
        _send_ntfy(title, message, priority=priority, tags=tags, topic="ollietrades-admin")
    except Exception:
        pass


def check_source_health_alerts(now_utc: Optional[datetime] = None,
                               force: bool = False) -> Dict[str, Any]:
    """W1 alerting/auto-quarantine. Call on each /api/sources/health poll; advances
    at most once per _TRACKER_MIN_INTERVAL_S (force=True bypasses, for tests).

    Fires throttled NTFY (ollietrades-admin) on: a live_decision source flipping RED
    (one per state-change), and a live_decision source RED >= RED_TICKS_TO_QUARANTINE
    consecutive ticks (recommend; auto-disable only if AUTO_QUARANTINE_ENABLED).
    Never raises — returns a small action summary for observability."""
    try:
        now = now_utc or datetime.now(timezone.utc)
        state = _load_health_state()
        last_run = state.get("_last_run", 0)
        now_epoch = now.timestamp()
        if not force and (now_epoch - float(last_run or 0)) < _TRACKER_MIN_INTERVAL_S:
            return {"skipped": "min-interval"}

        health = all_health(now)
        actions = {"alerts": [], "recommended": [], "quarantined": [], "ts": now.isoformat()}
        for s in health.get("sources", []):
            sid = s.get("source_id")
            if not sid:
                continue
            st = s.get("state")
            crit = s.get("criticality")
            prev = state.get(sid, {}) if isinstance(state.get(sid), dict) else {}
            prev_state = prev.get("state")
            consec = int(prev.get("consec_red", 0))
            fired = bool(prev.get("quarantine_fired", False))
            consec = consec + 1 if st == RED else 0

            if st == RED and crit == "live_decision" and prev_state != RED:
                _quarantine_ntfy(
                    f"Source RED: {s.get('display_name', sid)}",
                    f"{sid} ({crit}) flipped RED — stale {s.get('age_human')}, as_of {s.get('as_of')}",
                    priority="high", tags="rotating_light")
                actions["alerts"].append(sid)

            if (st == RED and crit == "live_decision"
                    and consec >= RED_TICKS_TO_QUARANTINE and not fired):
                if AUTO_QUARANTINE_ENABLED:
                    set_enabled(sid, False)
                    _quarantine_ntfy(
                        f"AUTO-QUARANTINE: {sid}",
                        f"{sid} RED {consec} ticks (live_decision) -> enabled=0. Re-enable is manual.",
                        priority="urgent", tags="no_entry_sign")
                    actions["quarantined"].append(sid)
                else:
                    _quarantine_ntfy(
                        f"RECOMMEND QUARANTINE: {sid}",
                        f"{sid} RED {consec} consecutive ticks (live_decision). Report-only — "
                        f"flip source_gate.AUTO_QUARANTINE_ENABLED to auto-disable.",
                        priority="high", tags="warning")
                    actions["recommended"].append(sid)
                fired = True
            elif st != RED:
                fired = False

            state[sid] = {"state": st, "consec_red": consec, "quarantine_fired": fired}

        state["_last_run"] = now_epoch
        _save_health_state(state)
        return actions
    except Exception as e:
        return {"error": "%s: %s" % (type(e).__name__, e)}
