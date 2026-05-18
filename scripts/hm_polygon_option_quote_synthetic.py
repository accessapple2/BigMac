"""HM-POLYGON-OPTIONS-CHAIN-QUOTE-HELPER synthetic verification (2026-05-18).

Hits a real wheel CSP option contract via the new
PolygonData.get_option_quote helper. Verifies plumbing + shape; does
not write to any DB. No production callers exist at the time of this
script — HM-CHECK-OPTION-EXITS-SHORT-PREMIUM-RULES is the first
consumer (scheduled for clear-the-plate Item 6).
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from engine.providers.polygon_provider import PolygonData


def main() -> int:
    pd = PolygonData()
    if not pd.is_active():
        print("FAIL: PolygonData not active (POLYGON_API_KEY missing?)")
        return 1

    # Known-real, currently-listed contracts (verified via
    # /v3/reference/options/contracts). Wheel CSP strikes ($66.30,
    # $144.48, $122.96) are paper-only and don't exist on the real
    # options chain — banked as HM-WHEEL-STRIKE-SNAP-TO-REAL for the
    # SHORT-PREMIUM-RULES consumer epic to resolve.
    test_cases = [
        ("O:SPY281215P01350000", "SPY"),   # SPY 2028-12-15 put strike 1350
        ("O:SPY281215P01340000", "SPY"),   # SPY 2028-12-15 put strike 1340
        ("O:SPY281215P01330000", "SPY"),   # SPY 2028-12-15 put strike 1330
    ]

    passes = 0
    fails = 0
    for occ, ticker in test_cases:
        print(f"\n--- {ticker} {occ} ---")
        q = pd.get_option_quote(occ)
        if q is None:
            print(f"  WARN: get_option_quote returned None (possibly empty payload for {occ})")
            fails += 1
            continue
        ok_shape = all(k in q for k in [
            "bid", "ask", "mid", "last_trade", "iv",
            "delta", "gamma", "theta", "vega",
            "source", "occ_symbol", "expiration", "strike", "type",
        ])
        if not ok_shape:
            print(f"  FAIL: shape — missing keys: {q}")
            fails += 1
            continue
        if q.get("source") != "polygon":
            print(f"  FAIL: source != polygon, got {q.get('source')}")
            fails += 1
            continue
        print(f"  OK: bid={q['bid']} ask={q['ask']} mid={q['mid']:.2f} "
              f"iv={q['iv']:.4f} delta={q['delta']:.3f} "
              f"strike={q['strike']} type={q['type']} exp={q['expiration']}")
        passes += 1

    # Negative cases
    print("\n--- Negative tests ---")
    cases_neg = [
        (None, "None occ → None"),
        ("", "Empty occ → None"),
        ("AAPL", "No O: prefix → None"),
        ("O:", "Only prefix → None"),
    ]
    for occ, label in cases_neg:
        q = pd.get_option_quote(occ)
        if q is None:
            print(f"  OK: {label}")
            passes += 1
        else:
            print(f"  FAIL: {label} returned {q!r}")
            fails += 1

    # Bulk wrapper
    print("\n--- Bulk wrapper ---")
    bulk = pd.get_option_quotes_bulk([t[0] for t in test_cases])
    print(f"  bulk keys: {list(bulk.keys())}")
    if len(bulk) == 3:
        print("  OK: 3 entries returned")
        passes += 1
    else:
        print(f"  FAIL: expected 3 entries, got {len(bulk)}")
        fails += 1

    print(f"\n[SUMMARY] passes={passes} fails={fails}")
    return 0 if fails == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
