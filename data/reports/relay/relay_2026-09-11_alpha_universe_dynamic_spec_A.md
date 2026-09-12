# Spec A — dynamic ALPHA_UNIVERSE + real gate check in risk_manager, before sizing

Not built. Research only, per the Admiral's explicit instruction. Covers:
what the daily alpha calc costs across Phase 1.2's screened output (~46-100
names), what it needs from Polygon, and where the gate check slots into
`risk_manager.py`.

## Headline correction to the premise: this needs NOTHING from Polygon

Checked every one of `composite_alpha`'s 12 signal components directly in
`engine/alpha_signals.py` — grepped the whole file for "polygon" first
(zero hits), then read every `run_*` function to confirm its real data
source. **None of the 12 touch Polygon, today or in this design.** Sources
in use: FINRA (free), SEC EDGAR (free), Reddit (free), Yahoo/yfinance
(free), FRED (free), Alpaca (free with the existing paper account). If the
expectation going in was "this needs a Polygon upgrade," that expectation
doesn't match what the calculator actually does — flagging this the same
way the trade_id claim got corrected last round, rather than forcing an
answer to match the premise.

## Per-component cost of widening the universe (24 -> ~46-100 symbols)

| Signal | Weight | Source | Current filtering | Marginal cost at 100 symbols |
|---|---|---|---|---|
| dark_pool | 0.20 | FINRA REGSHO daily bulk file (free) | Downloads the FULL market file already, then does `if sym not in ALPHA_UNIVERSE: continue` | **$0, zero added calls** — already processes every symbol in the feed; only the post-filter list needs to change |
| ftd | 0.15 | SEC FOIADOCS bulk ZIP (free) | Same pattern — full-market file, filtered post-download (`if sym not in ALPHA_UNIVERSE: continue`) | **$0, zero added calls** |
| insider | 0.20 | SEC EDGAR, per-symbol (`data.sec.gov/submissions/CIK{cik}.json` + up to 8 Form-4 XML fetches for symbols with recent activity) | `for sym in ALPHA_UNIVERSE:` — genuinely scales per symbol | **$0 (EDGAR is free), but time scales ~4x** (24 -> 100 symbols): 1 base call/symbol always, up to 8 more only for symbols with a hit. Needs pacing against SEC's fair-access guidance (already sends a real `User-Agent`, see `_EDGAR_HEADERS`) |
| put_call | 0.10 | yfinance/Alpaca, CBOE index tickers (^PCCE/^PCCR) | Market-wide, one shared score for all symbols | **$0, zero added calls** |
| vix_structure | 0.10 | Yahoo Finance HTTP API (^VIX/^VIX3M) | Market-wide | **$0, zero added calls** |
| sentiment (reddit) | 0.10 | Reddit JSON endpoints, 3 subreddits x 2 endpoints = 6 fixed calls, ticker-mention regex over the results | Fixed call count regardless of tracked-symbol count | **$0, zero added calls** |
| yield_curve | 0.03 | FRED CSV (DGS2/DGS10) | Market-wide | **$0** |
| opex | 0.05 | Pure calendar math (3rd-Friday rule) | Market-wide | **$0** |
| earnings | 0.03 | yfinance, per-symbol (`tk.calendar` + `tk.earnings_history`, wrapped in the existing `yf_call_safe`/`YFSweepAbort` rate-limit guard) | `for sym in ALPHA_UNIVERSE:` — 2 calls/symbol | **$0 (yfinance is free), but this is the real reliability risk** — the sweep-abort mechanism already exists because Yahoo throttles at 24 symbols' worth of calls (48 calls/day); at 100 symbols (200 calls/day) expect more frequent early aborts, not a hard failure but a "some symbols don't get an earnings score today" degradation |
| rebalancing | 0.00 | Pure calendar math (quarter-end rule) | Market-wide | **$0** (also currently weighted zero, unrelated to this spec) |
| rallies_consensus / rallies_debate | 0.05 + 0.05 | Internal (this repo's own Arena/debate output, not an external API) | Already keyed per-symbol from internal data | **$0** — no external call at all |

**Total dollar cost: $0.** The two components with any real marginal cost
are insider (EDGAR, free, more wall-clock time) and earnings (yfinance,
free, more throttling risk). Neither needs a paid API tier anywhere in this
design.

## What actually needs to change (not built, described for scope)

1. `ALPHA_UNIVERSE` (currently a hardcoded 24-symbol list,
   `engine/alpha_signals.py:39`) becomes a function of Phase 1.2's daily
   screen (`engine.mccoy_screen.get_mccoy_screened_symbols()`, DEFAULT_LIMIT
   100, live count today 46) instead of a literal. `compute_composite()`'s
   `for sym in ALPHA_UNIVERSE:` loop and the two bulk-file filters
   (dark_pool, ftd) read the same dynamic list.
2. `dark_pool`/`ftd`'s post-filter is a one-line change each (swap the
   membership check's source list) — no restructuring, since both already
   process the full market file.
3. `insider`/`earnings`'s `for sym in ALPHA_UNIVERSE:` loops need to run
   against the wider dynamic list — no code-structure change, just more
   iterations. Earnings should get the same kind of early-abort tolerance
   it already has, since partial completion at 100 symbols is expected
   behavior, not a bug to fix.
4. Cadence question, not resolved here: today's daily batch runs once,
   after close, against a fixed list that barely changes day to day.
   McCoy's screen changes daily and is itself computed pre-market — running
   the alpha calc against a *dynamic* list means either (a) running it
   AFTER the day's screen is known (shifts the alpha calc's own schedule
   later, likely into premarket/open, which changes its own data
   freshness assumptions — dark_pool/ftd have inherent multi-day lag
   already, unrelated to this), or (b) running the alpha calc for the
   UNION of today's and yesterday's screens (wider, avoids a same-day gap
   for a name that just entered the screen, at the cost of computing scores
   for some symbols McCoy isn't even looking at today). Not decided here —
   a real scheduling design question, not a Polygon or cost question.

## Where the gate check slots into risk_manager.py — exact location

`engine/risk_manager.py::check_buy()` (line 717) already has the pattern
this needs, immediately adjacent to where it should go:

```python
# UNIVERSAL minimum conviction (0.65 for all models)          <- line 764
effective_min_conv = self.UNIVERSAL_MIN_CONVICTION
model_min_conv = self.get_model_guardrail(player_id, "min_conviction")
if model_min_conv:
    effective_min_conv = max(effective_min_conv, model_min_conv)
if confidence < effective_min_conv:
    return False, f"LOW_CONVICTION: {confidence:.0%} below {effective_min_conv:.0%} minimum"

# <-- a composite_alpha >= 0.3 check belongs here, same shape:
#     return False, f"LOW_ALPHA: composite_alpha={alpha} below 0.3 minimum"
#     (or "no composite_alpha data for {symbol}" -- also a reject, matching
#     Phase 1.3's fail-closed posture: missing evidence never passes either)

cost = qty * price                                              # <- line 772
total_value = portfolio["cash"] + sum(...)
...
# position-sizing math starts here
```

This is the correct insertion point per the Admiral's own instruction
("before sizing") — everything from line 772 onward is sizing math
(`cost`, `effective_cap`, GEX/VIX/Fear-Greed multipliers). A gate check
placed between the conviction check (770) and the sizing block (772)
rejects before any qty/allocation math runs, exactly mirroring how
`UNIVERSAL_MIN_CONVICTION` itself already works — same function, same
`(bool, reason)` return shape, same fail-closed posture (missing data
rejects, same as low confidence rejects).

**Scope note, not resolved here:** `check_buy()` is called for every
player, not just McCoy. Gating it universally would affect every model
this session hasn't reviewed. The real insertion would need to be scoped
(a per-player guardrail flag, similar to how `get_model_guardrail(player_id,
"min_conviction")` already works two lines above) rather than a blanket
addition — this spec identifies the location, not the scoping mechanism,
per "don't build."

## Summary for the choice

- **Dollar cost of Option A: $0.** No Polygon, no new paid dependency.
- **Real cost: engineering time** (the dynamic-universe wiring + the
  scheduling question above) **and a small reliability tax** on the
  earnings signal at higher symbol counts (already-tolerated, not new).
- **The gate location is a clean five-line addition** to an existing,
  well-understood function, immediately next to the check it should sit
  beside.
- What this spec does NOT resolve: the scheduling question (#4 above),
  the per-player scoping question for the new gate, and whether widening
  `ALPHA_UNIVERSE` changes anything about `dark_pool`/`ftd`'s existing
  multi-day data lag becoming more or less relevant against a faster-moving
  small/mid-cap screen than the mega-cap list it was built for.
