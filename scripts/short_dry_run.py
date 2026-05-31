"""HM-SHORT-ACTIVATION dry-run — eyes-on gate. READ-ONLY, places no orders.

Three-gate squeeze guard (HM-FINVIZ-ELITE-AUTH Option B):
  1. DTC > 5        [Polygon]       — ALWAYS enforced, fail-CLOSED
  2. earnings <= 3d [Finnhub]       — ALWAYS enforced, fail-CLOSED
  3. SI% > 20       [Finviz Elite]  — layered on top; DEGRADES-TO-SKIP when Elite
                                      down (DTC+earnings still enforced), never to
                                      a free source, never 0 gates.
"""
import sys
sys.path.insert(0, "/Users/bigmac/autonomous-trader")
from engine import short_guard as G

print("="*78)
print("HM-SHORT-ACTIVATION DRY-RUN — 3-gate squeeze guard (Elite SI% + Polygon DTC + Finnhub earn)")
print("="*78)
print(f"Gates: DTC>{G.SQUEEZE_DTC_MAX:.0f}[polygon] | earnings<={G.SQUEEZE_EARNINGS_DAYS}d[finnhub] "
      f"| SI%>{G.SQUEEZE_SI_PCT_MAX:.0f}[finviz-elite, degrade-to-skip]")

# ── A) LOGIC PROOF (synthetic — every branch deterministic) ──
print("\n" + "-"*78)
print("A) LOGIC PROOF (synthetic patches — each gate + the Option-B degrade)")
print("-"*78)
lv = G.short_levels(100.0)
assert lv["hard_stop"] == 108.0 and lv["target"] == 90.0
print(f"  exit ladder: stop ${lv['hard_stop']} / target ${lv['target']} / trail {lv['trail_pct']:.0%}  ok")

cases = [
    ("CLEAN_3GATE",     2.0,  False, (8.0,  "finviz-elite"),        False, "DTC ok + earn ok + SI 8%<20 -> ALLOW (3 gates)"),
    ("SI_BLOCK",        2.0,  False, (24.0, "finviz-elite"),        True,  "SI 24%>20 -> BLOCK [finviz-elite]"),
    ("DTC_BLOCK",       6.5,  False, (8.0,  "finviz-elite"),        True,  "DTC 6.5>5 -> BLOCK (SI irrelevant)"),
    ("EARN_BLOCK",      2.0,  True,  (8.0,  "finviz-elite"),        True,  "earnings<=3d -> BLOCK"),
    ("DTC_FAILCLOSED",  None, False, (8.0,  "finviz-elite"),        True,  "Polygon DTC None -> FAIL-CLOSED (never skips)"),
    ("EARN_FAILCLOSED", 2.0,  None,  (8.0,  "finviz-elite"),        True,  "Finnhub earn None -> FAIL-CLOSED (never skips)"),
    ("ELITE_DOWN_OK",   2.0,  False, (None, "elite-unavailable"),   False, "Elite down + DTC ok + earn ok -> ALLOW (2 gates, degraded)"),
    ("ELITE_DOWN_HIDTC",6.5,  False, (None, "elite-unavailable"),   True,  "Elite down BUT DTC 6.5>5 -> STILL BLOCK (degrade != open DTC)"),
]
_o = (G._fetch_dtc, G._earnings_within, G._fetch_si_pct_elite)
allok = True
for name, dtc, earn, si_t, expect, why in cases:
    G._fetch_dtc          = lambda s, _d=dtc: (_d, "polygon" if _d is not None else "none")
    G._earnings_within    = lambda s, d, _e=earn: _e
    G._fetch_si_pct_elite = lambda s, _t=si_t: _t
    b, r = G.squeeze_block(name)
    ok = (b == expect); allok = allok and ok
    print(f"   {'OK ' if ok else 'XX '}{name:18} -> {'BLOCK' if b else 'ALLOW'}  ({why})")
    print(f"        reason: {r}")
G._fetch_dtc, G._earnings_within, G._fetch_si_pct_elite = _o
print(f"  LOGIC PROOF: {'ALL PASS' if allok else 'MISMATCH — REVIEW'}")

print("\n  aggregate 20%-of-book cap (3x 10% shorts on $10k book):")
book, opn = 10000.0, 0.0
for i in (1, 2, 3):
    room, at_cap = G.aggregate_short_room(opn, book); need = book*G.SHORT_MAX_POSITION_PCT
    if at_cap or need > room:
        print(f"   OK #{i}: open=${opn:.0f} room=${room:.0f} need=${need:.0f} -> BLOCKED")
    else:
        print(f"      #{i}: open=${opn:.0f} room=${room:.0f} need=${need:.0f} -> ALLOWED"); opn += need

# ── B) LIVE DATA — Elite UP (real authed Elite SI% + Polygon DTC + Finnhub earn) ──
print("\n" + "-"*78)
print("B) LIVE DATA — Elite UP (real authed Elite SI% + Polygon DTC + Finnhub earn)")
print("-"*78)
print("  Scenario 1 (SI gate fires on REAL data): BYND/KSS/UPST SI%>20 -> BLOCK [finviz-elite]")
print("  Scenario 2 (clean 3-gate ALLOW): AAPL low SI/DTC -> ALLOW")
print("  Threshold note: GME real SI%=14.2% is BELOW the 20% floor, so SI gate does NOT")
print("  fire on GME; its DTC 4.2<5 also clears -> GME ALLOWED at current thresholds.")
print("  (Blocking GME on SI would need SQUEEZE_SI_PCT_MAX lowered — an Admiral call.)")
for t in ["BYND", "KSS", "UPST", "AAPL", "GME", "CVNA", "ZZZZ_FAKE"]:
    try:
        b, r = G.squeeze_block(t)
    except Exception as e:
        b, r = True, f"SHORT REFUSED (probe error {type(e).__name__} — fail-closed)"
    print(f"   {'EXCLUDED ' if b else 'SHORTABLE'} {t:11} {r}")

# ── C) ELITE DOWN (simulated) — degrade-to-skip; DTC+earnings STILL enforced ──
print("\n" + "-"*78)
print("C) ELITE DOWN (simulated: SI fetch -> (None,'elite-unavailable'))")
print("-"*78)
print("  Scenario 3 (degrade != open DTC): KSS high-DTC -> STILL BLOCKED by DTC")
print("  Scenario 4 (bounded accepted risk, VISIBLE): BYND (SI was sole blocker, DTC 2.2)")
print("             -> now ALLOWED, logged '… Elite unavailable, SI% gate skipped (2 gates)'")
_orig_elite = G._fetch_si_pct_elite
G._fetch_si_pct_elite = lambda s: (None, "elite-unavailable")
for t in ["BYND", "KSS", "AAPL"]:
    try:
        b, r = G.squeeze_block(t)
    except Exception as e:
        b, r = True, f"SHORT REFUSED (probe error {type(e).__name__})"
    print(f"   {'EXCLUDED ' if b else 'SHORTABLE'} {t:11} {r}")
G._fetch_si_pct_elite = _orig_elite

# ── D) FAIL-CLOSED regression — only SI% degrades; DTC + earnings never do ──
print("\n" + "-"*78)
print("D) FAIL-CLOSED regression — only SI% degrades; DTC + earnings never do")
print("-"*78)
_o2 = (G._fetch_dtc, G._earnings_within)
G._fetch_dtc = lambda s: (None, "none"); G._earnings_within = lambda s, d: False
b, r = G.squeeze_block("POLY_DOWN")
print(f"   {'OK ' if b else 'XX '}Polygon down -> {'BLOCK' if b else 'ALLOW(!)'} :: {r}")
G._fetch_dtc = lambda s: (2.0, "polygon"); G._earnings_within = lambda s, d: None
b, r = G.squeeze_block("FINN_DOWN")
print(f"   {'OK ' if b else 'XX '}Finnhub down -> {'BLOCK' if b else 'ALLOW(!)'} :: {r}")
G._fetch_dtc, G._earnings_within = _o2

print("\n" + "="*78)
print("END DRY-RUN — read-only, no orders. Awaiting Captain approval.")
print("="*78)
