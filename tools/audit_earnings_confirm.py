"""
Measure how often the len(dates)==1 Yahoo proxy is wrong vs the multi-source resolver.

Run from the autonomous-trader root:
  .venv/bin/python tools/audit_earnings_confirm.py

Outputs:
  false_confirm  — proxy said "confirmed" but resolver says ESTIMATED
  false_estimate — proxy said "estimated" but resolver says CONFIRMED
  full per-symbol table
"""
from __future__ import annotations
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from engine.earnings_calendar import fetch_earnings, _cache as _ec_cache
from engine.earnings_confirm import confirm_earnings, Confidence, get_confirmed_flag
from engine.universe import get_active_universe


def held_symbols() -> list[str]:
    """Return symbols currently in any open position."""
    try:
        import sqlite3
        db = os.environ.get("TRADEMINDS_DB", os.path.expanduser("~/autonomous-trader/data/trader.db"))
        conn = sqlite3.connect(db, check_same_thread=False)
        rows = conn.execute("SELECT DISTINCT symbol FROM positions").fetchall()
        conn.close()
        return [r[0] for r in rows]
    except Exception as e:
        print(f"[warn] could not read positions: {e}")
        return []


def get_confirmed_proxy(sym: str) -> bool | None:
    """Return the len(dates)==1 proxy value from the in-process cache."""
    fetch_earnings([sym])          # populate cache
    cached = _ec_cache.get(sym) or _ec_cache.get(sym.upper())
    if not cached:
        return None
    return cached.get("confirmed")


# Extend with externally known truth labels (manually from IR pages / press releases)
KNOWN_TRUTH: dict[str, str] = {
    # "MU": "confirmed",   # add verified cases here
}


def main() -> None:
    universe = set(s.upper() for s in get_active_universe())
    held = set(s.upper() for s in held_symbols())
    candidates = universe | held | set(KNOWN_TRUTH.keys())

    print(f"Scanning {len(candidates)} symbols …\n")
    fmt = "{:<8} {:<10} {:<12} {:<32} {}"
    print(fmt.format("SYMBOL", "PROXY", "RESOLVER", "SOURCES", "TRUTH"))
    print("-" * 80)

    rows = []
    for sym in sorted(candidates):
        proxy = get_confirmed_proxy(sym)
        res   = confirm_earnings(sym)
        conf  = res["confidence"] if res else None
        srcs  = res["sources"]    if res else []
        truth = KNOWN_TRUTH.get(sym, "—")
        rows.append((sym, proxy, conf, srcs, truth))
        print(fmt.format(sym, str(proxy), str(conf), str(srcs)[:32], truth))

    false_confirm  = sum(1 for _,p,c,_,_ in rows if p is True  and c != Confidence.CONFIRMED)
    false_estimate = sum(1 for _,p,c,_,_ in rows if p is False and c == Confidence.CONFIRMED)
    total_confirm  = sum(1 for _,p,_,_,_ in rows if p is True)
    total_estimate = sum(1 for _,p,_,_,_ in rows if p is False)

    print("\n" + "=" * 80)
    print(f"proxy confirmed  : {total_confirm}")
    print(f"proxy estimated  : {total_estimate}")
    print(f"false-confirm  (proxy=T, resolver=E): {false_confirm}  ← stops silently removed")
    print(f"false-estimate (proxy=F, resolver=C): {false_estimate}  ← MU-miss risk")
    if false_confirm > 0:
        print("\n⚠  false_confirm > 0: positions may have had stops silently removed.")
        print("   This is the justification for the multi-source resolver.")


if __name__ == "__main__":
    main()
