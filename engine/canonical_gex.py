"""Canonical GEX accessor (HM-GEX-CANONICAL).

Single importable source of truth for a symbol's gamma-exposure profile so that
EVERY consumer (Bridge endpoints, Ready Room / Troi, etc.) reads the SAME numbers.

Priority (HM-GEX-ALPACA-REPOINT-2026-09-12 -- see relay doc of the same date):
  0) Alpaca live/cached snapshot (gex_calculator.py, data/trader.db's own
     gex_snapshots table) -- real-time, already funded, already the source the
     one real decision-path gate (risk_manager.py) and the fleet-wide LLM
     prompt injection (providers/base.py) have used unconditionally the whole
     time. Accepted only if fresher than ALPACA_GEX_MAX_AGE_DAYS -- being
     real-time doesn't mean it can't stall (main.run_alpaca_gex_refresh keeps
     it warm every 15min RTH; this gate is what happens if that ever stops).
  1) Polygon intraday in-process cache (engine.options_flow_gex, refreshed
     ~15m RTH by main.run_gex_snapshot_refresh) -- kept as an optional
     secondary tier in case the Polygon subscription is ever restored; dead
     since the 2026-07-22 entitlement 403, tier 0 above supersedes it in
     practice today.
  2) latest daily row in data/flow_gex.db (Polygon)
  3) Polygon live compute

Returns a dict. On total failure returns {"underlying": sym, "error": "..."} —
callers must check for "error" before trusting the values.

NOTE on shape: the daily flow_gex.db row persists NET GEX per strike only (not
per-strike call/put OI). Consumers that need OI (P/C ratio, max-pain) must keep
their own OI source; this helper is authoritative for the headline levels
(spot, total_gex, gamma_flip, call_wall, put_wall, king_node, regime).

Kept Python 3.9-safe (no PEP 604 unions) — imported by engine code that may run
under either interpreter.
"""
from typing import Optional

# HM-GEX-ALPACA-REPOINT-2026-09-12: tier-0 freshness bar. Deliberately much
# tighter than CANONICAL_GEX_MAX_AGE_DAYS below (1 day, calibrated for a
# once-daily Polygon collector) -- this tier is supposed to be real-time,
# refreshed every 15min RTH (main.run_alpaca_gex_refresh), so 2x that
# interval (30min, same "2x tier cadence" margin as HM-SCAN-LIVENESS-WATCHDOG)
# is the right bar for "did the refresh stall," not 24 hours.
ALPACA_GEX_MAX_AGE_DAYS = 30.0 / (24 * 60)  # 30 minutes, expressed in days

# HM-GEX-FRESHNESS-GATE-2026-09-01: gamma exposure is intraday regime data —
# walls/flip move with the day's flow. A day-old snapshot is already stale
# for decisioning, even though it's a perfectly valid "last known" value for
# an audit trail. This is the ONE place that number lives; every freshness
# check in this pipeline (dashboard staleness markers, the ready_room/
# dynamic_advisor overlay gate below) reads it from here rather than
# hardcoding it locally, precisely so the definition of "stale" can't drift
# between consumers again the way it silently did before this fix.
CANONICAL_GEX_MAX_AGE_DAYS = 1.0


def snapshot_age_days(as_of) -> Optional[float]:
    """Age in days of a canonical-GEX `_asof`/`as_of` timestamp, or None if
    missing/unparseable (never silently 0 — a missing timestamp must not
    read as "fresh"). Handles both ' ' and 'T' separators (both are
    observed in the wild across this pipeline's different writers)."""
    if not as_of:
        return None
    try:
        from datetime import datetime, timezone
        s = str(as_of).replace(" ", "T")
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 86400.0
    except Exception:
        return None


def canonical_gex_if_fresh(symbol: str) -> Optional[dict]:
    """canonical_gex(symbol) IF its `_asof` is within CANONICAL_GEX_MAX_AGE_DAYS,
    else None.

    HM-GEX-FRESHNESS-GATE-2026-09-01: canonical_gex()'s own priority list
    (intraday cache -> flow_gex.db -> live compute) already fails safe when
    Polygon is unreachable (returns an "error" key). The gap this closes is
    different: when the *daily collector itself* has been dead for weeks
    (HM-GEX-RETIRED, the /v3/snapshot/options 403), tier 2 of that priority
    list still returns a structurally VALID row — no error, just old. Two
    call sites (engine/ready_room.py, engine/dynamic_advisor.py) were each
    independently treating "no error" as "use this," unconditionally
    overwriting live Alpaca-derived values with a frozen Polygon snapshot.
    This is the single freshness gate both now share instead of duplicating
    the same age check twice — callers that get None here fall through to
    their own live/legacy source exactly as they already do on a real error.
    """
    c = canonical_gex(symbol)
    if not c or c.get("error"):
        return None
    age = snapshot_age_days(c.get("_asof"))
    if age is None or age >= CANONICAL_GEX_MAX_AGE_DAYS:
        return None
    return c


def latest_snapshot(symbol: str) -> Optional[dict]:
    """Latest durable data/flow_gex.db row for `symbol`, or None if none exists.

    Pure local SQLite read — no upstream/Polygon calls, no live compute. This is
    tier 2 of canonical_gex()'s priority list, factored out so callers that want
    a last-known-good value without ever touching a (possibly dead) live upstream
    can call it directly.
    """
    sym = (symbol or "").upper()
    try:
        import sqlite3 as _sq
        import json as _json
        from pathlib import Path as _P
        dbp = _P(__file__).parent.parent / "data" / "flow_gex.db"
        conn = _sq.connect(str(dbp))
        conn.row_factory = _sq.Row
        r = conn.execute(
            "SELECT * FROM gex_snapshots WHERE underlying=? ORDER BY id DESC LIMIT 1",
            (sym,),
        ).fetchone()
        conn.close()
        if not r:
            return None
        ps = _json.loads(r["per_strike_json"] or "{}")
        strikes = [
            {"strike": float(k), "net_gex": v}
            for k, v in sorted(ps.items(), key=lambda kv: float(kv[0]))
        ]
        magnets = sorted(strikes, key=lambda x: -abs(x["net_gex"]))[:5]
        king = max(ps, key=lambda k: abs(ps[k])) if ps else None
        return {
            "underlying": sym, "spot": r["spot"], "total_gex": r["total_gex"],
            "regime": r["regime"], "gamma_flip": r["gamma_flip"],
            "call_wall": r["call_wall"], "put_wall": r["put_wall"],
            "king_node": (float(king) if king is not None else None),
            "magnets": magnets, "strikes": strikes,
            "_asof": r["asof"], "_src": "daily-flow_gex.db",
        }
    except Exception:
        return None


def _alpaca_snapshot_fresh(symbol: str) -> Optional[dict]:
    """Tier 0: latest Alpaca-sourced gex_snapshots row (data/trader.db), reshaped
    to this module's canonical dict shape, IF fresher than ALPACA_GEX_MAX_AGE_DAYS.

    Uses `created_at` (SQLite `datetime('now')`, genuinely UTC) as the age basis,
    NOT gex_calculator.GEXProfile's own `timestamp` field -- that one is written
    via naive `datetime.now().isoformat()` (local/Arizona, per this repo's server
    TZ), and snapshot_age_days() assumes a naive input is UTC. Using `timestamp`
    here would silently misjudge every row's age by the local UTC offset (7h) --
    exactly the class of bug HM-TRADES-TZ-GATE-2026-09-12 fixed everywhere else
    in this repo today, so it must not be reintroduced here.
    """
    try:
        from gex_calculator import get_latest_snapshot
        snap = get_latest_snapshot(symbol)
        if not snap:
            return None
        age = snapshot_age_days(snap.get("created_at"))
        if age is None or age >= ALPACA_GEX_MAX_AGE_DAYS:
            return None
        levels = snap.get("levels") or []
        strikes = [
            {"strike": lv["strike"], "net_gex": lv.get("net_gex"),
             "call_gex": lv.get("call_gex"), "put_gex": lv.get("put_gex")}
            for lv in levels
        ]
        magnets = sorted(strikes, key=lambda x: -abs(x.get("net_gex") or 0))[:5]
        spot = snap.get("spot_price")
        flip = snap.get("gamma_flip")
        total_gex = snap.get("total_gex")
        if spot is not None and flip is not None:
            regime = ("LONG GAMMA · stable (spot above flip)" if spot >= flip
                       else "SHORT GAMMA · volatile (spot below flip)")
        else:
            regime = ("LONG GAMMA · stable" if (total_gex or 0) > 0
                       else "SHORT GAMMA · volatile")
        return {
            "underlying": symbol.upper(), "spot": spot, "total_gex": total_gex,
            "regime": regime, "gamma_flip": flip,
            "call_wall": snap.get("call_wall"), "put_wall": snap.get("put_wall"),
            "king_node": snap.get("max_gamma_strike"),
            "magnets": magnets, "strikes": strikes,
            "_asof": snap.get("created_at"), "_src": "alpaca",
        }
    except Exception:
        return None


def canonical_gex(symbol: str) -> dict:
    sym = (symbol or "").upper()
    # 0) Alpaca live snapshot — HM-GEX-ALPACA-REPOINT-2026-09-12, see module docstring
    a = _alpaca_snapshot_fresh(sym)
    if a:
        return a
    # 1) Polygon intraday in-process cache
    try:
        from engine import options_flow_gex as _ofg
        latest = _ofg.get_latest()
        d = (latest.get("data") or {}).get(sym)
        if d and d.get("gex") and not d["gex"].get("error"):
            g = dict(d["gex"])
            g["_asof"] = latest.get("ts")
            g["_src"] = "intraday-cache"
            return g
    except Exception:
        pass
    # 2) latest daily row in flow_gex.db
    snap = latest_snapshot(sym)
    if snap:
        # HM-DRYDOCK 2026-06-09 EPIC4: if the stored daily row has COLLAPSED walls
        # (call==put==king on one strike — a stale/pre-put_wall-fix artifact), do NOT serve it;
        # fall through to the live compute (3) which yields distinct walls. Prevents the
        # post-restart re-collapse (empty intraday cache → was serving this stale row).
        _cw, _pw, _kn = snap["call_wall"], snap["put_wall"], snap["king_node"]
        if not (_cw is not None and _cw == _pw == _kn):
            return snap
        # collapsed → fall through to live compute below
    # 3) live compute
    try:
        from engine import options_flow_gex as _ofg
        g = _ofg.compute_gex(sym)
        g["_src"] = "live-compute"
        return g
    except Exception as e:
        return {"underlying": sym, "error": "%s: %s" % (type(e).__name__, e)}
