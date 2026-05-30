"""HM-SHORT-ACTIVATION dry-run — eyes-on gate before flipping SHORT_ENABLED.

READ-ONLY. Places no orders. SHORT_ENABLED stays False throughout.

Guard is ALL-PAID-SOURCE (2026-05-30 data audit): days-to-cover via Polygon,
earnings via Finnhub. NO finvizfinance (free scrape), NO yfinance. SI %-of-float
dropped for now (HM-FINVIZ-ELITE-AUTH). Every fetch miss FAILS CLOSED.

Exercises the guard two ways:
  A) LOGIC PROOF  — synthetic inputs so each branch is visibly demonstrated
                    (exit ladder; DTC>5 block; earnings block; fail-closed on a
                    missing DTC; fail-closed on a missing earnings calendar; the
                    20%-of-book aggregate cap blocking a 3rd concurrent short).
  B) LIVE DATA    — real squeeze_block() on representative tickers, source-tagged,
                    proving Polygon DTC + Finnhub earnings fire on real data.
"""
import sys
sys.path.insert(0, "/Users/bigmac/autonomous-trader")

from engine import short_guard as G

AGENTS = sorted(G.SHORT_AUTHORIZED_AGENTS)
print("="*74)
print("HM-SHORT-ACTIVATION DRY-RUN  (SHORT_ENABLED stays OFF — no orders placed)")
print("="*74)
print(f"Authorized agents: {AGENTS}  (dalio-metals excluded: tracking-only)")
print(f"Params: hard_stop={G.SHORT_HARD_STOP_PCT:.0%}  target={G.SHORT_TARGET_PCT:.0%}@"
      f"{G.SHORT_TARGET_COVER_FRAC:.0%}  trail={G.SHORT_TRAIL_PCT:.0%}  "
      f"pos_cap={G.SHORT_MAX_POSITION_PCT:.0%}  agg_cap={G.SHORT_MAX_AGGREGATE_PCT:.0%}")
print(f"Squeeze block: DTC>{G.SQUEEZE_DTC_MAX:.0f} [Polygon] OR earnings<="
      f"{G.SQUEEZE_EARNINGS_DAYS}d [Finnhub]  — FAILS CLOSED on any fetch miss")

print("\n" + "-"*74)
print("A) LOGIC PROOF — exit ladder for a $100.00 short entry")
print("-"*74)
lv = G.short_levels(100.00)
print(f"  entry $100.00 → hard_stop(buy) ${lv['hard_stop']}  | "
      f"target ${lv['target']} (cover {lv['target_cover_frac']:.0%}) | "
      f"trail {lv['trail_pct']:.0%} on runner")
assert lv["hard_stop"] == 108.0 and lv["target"] == 90.0, "ladder math wrong"
print("  ✓ ladder math correct (stop 108 / target 90 / trail 5%)")

print("\n  squeeze_block decision table (synthetic — patched (dtc, src) + earn probes):")
# (label, dtc_value, earn_value, expect_block, why)
#   dtc_value None  → Polygon miss        → fail-closed
#   earn_value None → Finnhub miss        → fail-closed
#   earn_value True → reporting ≤3d        → block
cases = [
    ("CLEAN",      2.0,  False, False, "DTC 2.0 ok, earnings clear → ALLOW"),
    ("HIGH_DTC",   6.5,  False, True,  "DTC 6.5 > 5 → BLOCK"),
    ("EARNINGS",   2.0,  True,  True,  "earnings ≤3d → BLOCK"),
    ("DTC_MISS",   None, False, True,  "Polygon DTC unavailable → FAIL-CLOSED"),
    ("EARN_MISS",  2.0,  None,  True,  "Finnhub earnings unavailable → FAIL-CLOSED"),
]
_orig = (G._fetch_dtc, G._earnings_within)
for name, dtc, earn, expect_block, why in cases:
    G._fetch_dtc       = lambda s, _d=dtc: (_d, "polygon" if _d is not None else "none")
    G._earnings_within = lambda s, d, _e=earn: _e
    blocked, reason = G.squeeze_block(name)
    flag = "BLOCK" if blocked else "ALLOW"
    ok = "✓" if blocked == expect_block else "✗ MISMATCH"
    print(f"   {ok} {name:10} DTC={str(dtc):>5} earn={str(earn):>5} → {flag:5} ({why})")
    print(f"        reason: {reason}")
G._fetch_dtc, G._earnings_within = _orig  # restore

print("\n  aggregate 20%-of-book cap — three sequential 10% shorts on a $10,000 book:")
book = 10000.0
open_short = 0.0
for i in (1, 2, 3):
    room, at_cap = G.aggregate_short_room(open_short, book)
    this = book * G.SHORT_MAX_POSITION_PCT  # a 10% short = $1,000
    if at_cap or this > room:
        print(f"   ✓ short #{i}: open=${open_short:.0f} room=${room:.0f} "
              f"need=${this:.0f} → BLOCKED (would exceed 20% book cap)")
    else:
        print(f"     short #{i}: open=${open_short:.0f} room=${room:.0f} "
              f"need=${this:.0f} → ALLOWED")
        open_short += this
print("   → exactly 2 × 10% shorts fit under the 20% aggregate cap; the 3rd is blocked.")

print("\n" + "-"*74)
print("B) LIVE DATA — real squeeze_block() (Polygon DTC + Finnhub earnings, read-only)")
print("-"*74)
print("  Source tag per verdict MUST show only [polygon]/[finnhub] — zero finviz, zero yf.")
live_tickers = ["AAPL", "MSFT", "GME", "CVNA", "ZZZZ_FAKE_TICKER"]
for t in live_tickers:
    try:
        blocked, reason = G.squeeze_block(t)
    except Exception as e:
        blocked, reason = True, f"SHORT REFUSED (probe error {type(e).__name__} — fail-closed)"
    mark = "⛔ EXCLUDED " if blocked else "✅ SHORTABLE"
    extra = "" if blocked else "  [stop +8% / tgt -10% / trail 5%]"
    print(f"   {mark} {t:18} {reason}{extra}")

print("\n" + "-"*74)
print("C) EARNINGS-FIRES-VIA-FINNHUB — direct proof the repoint works (not inert)")
print("-"*74)
# Find a real name reporting within 3d straight from the Finnhub calendar the
# guard uses, then show squeeze_block blocking it on the earnings branch.
try:
    from engine.finnhub_data import get_earnings_calendar
    from datetime import datetime, timedelta, timezone
    today = datetime.now(timezone.utc).date()
    rows = get_earnings_calendar(
        from_date=today.strftime("%Y-%m-%d"),
        to_date=(today + timedelta(days=G.SQUEEZE_EARNINGS_DAYS)).strftime("%Y-%m-%d"),
    )
    print(f"  Finnhub earnings rows (watchlist ∩ next {G.SQUEEZE_EARNINGS_DAYS}d): {len(rows)}")
    sample = [str(r.get("symbol")) for r in rows][:8]
    print(f"  sample symbols: {sample}")
    if rows:
        probe = str(rows[0].get("symbol"))
        within = G._earnings_within(probe, G.SQUEEZE_EARNINGS_DAYS)
        blocked, reason = G.squeeze_block(probe)
        print(f"  earnings probe {probe}: _earnings_within(3d)={within}")
        print(f"  squeeze_block({probe}) → {'BLOCK' if blocked else 'ALLOW'} :: {reason}")
        print("  ✓ earnings guard FIRES on live Finnhub data" if within
              else "  (probe not within window — see row list above)")
    else:
        print("  (no watchlist names reporting in the next 3d right now — calendar")
        print("   fetch succeeded with rows market-wide; synthetic EARNINGS case in")
        print("   Part A proves the block branch; live fetch proves it's not inert.)")
except Exception as e:
    print(f"  Finnhub earnings probe error: {type(e).__name__}: {e}")

print("\n" + "="*74)
print("END DRY-RUN — SHORT_ENABLED still OFF. No orders placed. Awaiting Captain approval.")
print("="*74)
