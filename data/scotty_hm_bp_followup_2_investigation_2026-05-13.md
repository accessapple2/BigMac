# HM-BP-FU-2 Phase 2 Investigation $(date +%Y-%m-%d)

## Status: DEFERRED — bug is dormant, real fix requires careful refactor

## Corruption profile

- 2026-04-08: 1 corrupt row (options-sosnoff)
- 2026-03-12: 27 corrupt rows (gemini-2.5-pro batch)
- **Total: 28 rows in 2 months, only 1 in last 5 weeks**

gemini-2.5-pro is halted, so the agent that triggered the bug is largely out of the loop.

## Real culprit: paper_trader.py:2105 — not the 3 original candidates

```python
2105: current_price = estimate_option_price(ot, strike, stock_price, avg_price, expiry)
2123: result = sell(pid, sym, current_price, asset_type="option", ...)
```

`estimate_option_price` takes `stock_price` as input. If any of its return
paths falls back to returning `stock_price` unfiltered (e.g., expired/OTM/
missing-data path), the underlying spot becomes the option's exit_price.

Also relevant: paper_trader.py:1788 — same function called from expire_options.

## What DayBlade does right (and what's missing)

dayblade.py:437 has `if price < 0.01: BLOCKED`. Defense against $0 exits.
But it doesn't catch the spot-as-premium case (13×–18× ratios observed).

## Recommended next session approach

1. Read `estimate_option_price` end-to-end — identify all return paths,
   find any that return stock_price unfiltered
2. Fix at source (return option premium always, never spot)
3. Add high-ratio guard at sell() site only AFTER understanding when
   legitimate 5x+ moves occur (0DTE gaps, OTM→ITM transitions) — guard
   may need to be ratio-aware vs time-of-day-aware

## Backfill decision (Captain still owes A/B/C/D)

With 28 rows total, Option B (SQL UPDATE corrected via realized_pnl math)
is achievable. Option A (leave, filter handles it) still defensible.
Option C (soft-delete flag) overkill at this volume.

## NOT shipping tonight

Defense-in-depth guard rejected because legitimate 5x+ options moves exist.
Need careful audit of when those occur before adding the check.
