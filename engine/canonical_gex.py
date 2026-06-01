"""Canonical GEX accessor (HM-GEX-CANONICAL).

Single importable source of truth for a symbol's gamma-exposure profile so that
EVERY consumer (Bridge endpoints, Ready Room / Troi, etc.) reads the SAME numbers.
Polygon-native, gamma×OI, BS-re-gamma flip, ±20% / ≤60DTE band — observation-only.

Priority:
  1) intraday in-process cache (engine.options_flow_gex, refreshed ~15m RTH by
     main.run_gex_snapshot_refresh)
  2) latest daily row in data/flow_gex.db
  3) live compute

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


def canonical_gex(symbol: str) -> dict:
    sym = (symbol or "").upper()
    # 1) intraday in-process cache
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
        if r:
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
        pass
    # 3) live compute
    try:
        from engine import options_flow_gex as _ofg
        g = _ofg.compute_gex(sym)
        g["_src"] = "live-compute"
        return g
    except Exception as e:
        return {"underlying": sym, "error": "%s: %s" % (type(e).__name__, e)}
