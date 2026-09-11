# Relay — B6: Options premium restatement, all seasons. 2026-09-11

Builds on `relay_2026-09-09_plutus-v1-archaeology-and-season-dsr.md`, which
already root-caused and partially remediated Season 1's corruption. This
pass: (1) extends restatement to `options_trades` (a separate table,
different bug, different era), (2) answers the literal directive question
— does Season 1/2/4 become PSR/DSR-scoreable — with a live re-run of the
already-built `archive_harness.py`, (3) definitively closes out whether
Season 1's known-corrupted rows are reconstructable at all.

RULE #1 honored throughout: every action below is additive (new columns,
new views, new report files) or a live-code fix to how *future* rows get
written. No existing row value in `trades` or `options_trades` was ever
changed. Fresh backup taken before any write:
`data/backups/trader_20260911_074244_pre_b6_restatement.db`.

## Two separate bugs, two separate tables, two separate eras

1. **`trades` (asset_type='option'), Season 1, March 2026** — the
   underlying-stock-price-as-exit-premium recording bug, already found and
   flagged 2026-09-09 (27 rows, `pnl_basis_invalid=1`). Root-caused in
   `engine/risk_manager.py`'s shared stop-loss/take-profit path, already
   fixed at the source that same night.
2. **`options_trades`, pre-2026-07-07** — a *different* bug: `engine/
   wheel_strategy.py` and `engine/shadow_csp.py` computed CSP entry premium
   from a pure VIX-scaled formula (`price * min(0.08, vix/500)`), never a
   real quote. Root-caused and fixed at the source 2026-07-07 (P0-A/P0-B,
   commit `1311da3` — see `docs/XO_BACKLOG.md`). **Never restated
   retroactively until this pass.**

## Part 1 — Season 1/2/4 PSR/DSR-scoreability (the literal directive question)

Ran `scripts/archive_harness.py` (already built 2026-09-09, already
excludes implausible option closes via a >15x entry/exit ratio filter)
fresh against all three seasons:

| Season | trials_tested | Scoreable? | Detail |
|---|---|---|---|
| 1 | 9 | **NO** | Every player `INSUFFICIENT_N` (min_n=5) — too few clean rows even with options included; the 27 known-corrupted rows are excluded by the ratio filter and contribute nothing either way. |
| 2 | 10 | **YES** | 6/10 players clear n≥5 (11-26 trades each) — genuinely usable data, **zero ratio-filter exclusions** (no rows matched the Season-1-style corruption signature — independently reconfirms the 2026-09-09 finding "seasons 2-7 show zero matches"). All 6 scoreable players show negative/zero raw Sharpe and DSR=0.0000 — nothing graduates, but the read is legitimate, not a data-quality artifact. |
| 4 | 3 | **NO** | Trivially small (n=1 for the only qualifying player) — season 4 was a ~1-day season with almost no trade volume of any kind. |

**Verdict: Season 2 becomes PSR/DSR-scoreable; Seasons 1 and 4 do not (data
volume, not data quality, is the blocker for both).**

## Part 2 — Season 1's 27 corrupted rows: reconstruction is impossible, confirmed empirically

Tested the actual mechanism, not just re-asserted the 2026-09-09
conclusion: pulled **real Alpaca historical option bars** for a comparable
March-2026 contract (AAPL 255C exp 2026-04-10) — **data exists and is
accurate** (2026-03-11 daily close = $11.90, exact match to a live row's
recorded entry_price). Alpaca's historical coverage is NOT the blocker.

**The actual blocker: zero of the 27 flagged rows have `strike_price` or
`expiry_date` recorded at all.** Checked directly, not assumed — every one
of the 27 shows `strike_price=NULL, expiry_date=NULL`. There is no
contract to look up. This is a stronger, more precise finding than the
2026-09-09 doc's "true value unrecoverable" — it's not that the data
wasn't durable, it's that the contract identity itself was never captured.
**No further action possible; the existing tier-label lower-bound in
`trades_restated` (built 2026-09-09) remains the best available estimate.**

Also checked contract-data completeness across ALL Season 1/2/4 option
rows (not just the 27 flagged ones), in case any unflagged rows might be
reconstructable: Season 1 has strike+expiry on only 14/105 rows, Season 2
on 52/185, Season 4 on 1/2 — and every row with complete data is a BUY
(opening) entry, not a SELL with a P&L claim to check. **Nothing in the
legacy `trades`-table options data is both (a) claiming a P&L and (b) has
enough contract data to verify that claim, beyond the 27 already handled.**

## Part 3 — `options_trades`' 120 pre-fix rows: reconstructed where possible

New script: `scripts/options_trades_restatement.py`. For each pre-2026-07-07
row, rebuilds the OCC contract symbol per leg from `legs_json` (which — unlike
the old `trades` rows — reliably carries strike/expiration) and fetches a
real Alpaca historical option bar (hourly first, for same-day open/close
trades where daily bars would collapse entry and exit to an identical price
and silently produce a fake $0 — caught and fixed mid-build, see the
script's own docstring for the specific bug), falling back to daily bars
for less liquid/older contracts.

**Formula, verified by hand against real rows before writing any code:**
`pnl = entry_credit_debit + exit_credit_debit`, `entry_credit_debit =
Σ(sign × entry_price × qty × mult)`, sign = +1 short / −1 long. **Found a
real, live units inconsistency along the way**: CSP/wheel rows use
`mult=100` (dollar-scale, ×contracts×shares-per-contract), while the older
equity-spread rows (`strategy:bull_spread_v1`) use `mult=1` (raw per-share,
not even multiplied by `qty`) — two different code paths writing the same
table in two different units. Detected per-row (whichever reproduces the
row's own recorded `entry_credit_debit`), not silently normalized — flagged
here as its own finding, separate from the pricing bug.

**Results (120 rows):**

| Basis | n | What it means |
|---|---|---|
| `real_alpaca_bar` | 15 | Reconstructed from real market data. All same-day SPY `bull_put_spread` scalps (`swingdesk-manual`) — small, plausible restated P&Ls ($-0.02 to $1.79); one row (id 28) had both a recorded and restated figure to compare: $1.82 booked vs $1.71 restated, an 11-cent gap consistent with hourly-bar timing noise, not a red flag. |
| `unrecoverable_no_alpaca_bar` | 87 | Overwhelmingly leveraged-ETF CSP legs — TQQQ (25), SOXL (24), UPRO (24), plus 14 SPY/QQQ. **Verified this is a real Alpaca coverage gap, not a script bug**: direct test against TQQQ260616P00066300 (a real leg from one of these rows) returns zero bars in both the entry and exit windows — the contract genuinely has no recorded trades in Alpaca's data for those dates (illiquid deep-leveraged-ETF strikes, plausible on its own terms). |
| `not_closed_no_restatement_needed` | 12 | Still open/canceled/failed — no settled P&L claim exists to check. |
| `unrecoverable_unknown_unit_convention` | 6 | The very first `options_trades` rows ever written (ids 2-9, 2026-04-22, one literally `symbol='TEST'`) — used a third, qty-blind unit convention (`entry_price` difference with no multiplier and no `qty` scaling at all) that doesn't reproduce under either detected convention. All 6 have `pnl=NULL` in the DB — no claim was ever finalized for them either. |

**Shipped, additive, RULE #1 compliant:**
- `options_trades` gained 4 new columns (`entry_credit_debit_restated`,
  `exit_credit_debit_restated`, `pnl_restated`, `restatement_basis`) —
  `ALTER TABLE ADD COLUMN` only, no existing column touched.
- `options_trades_restated` VIEW (mirrors the existing `trades_restated`
  pattern) — `pnl_best_estimate` is non-NULL only where `restatement_basis
  = 'real_alpaca_bar'`; everything else stays NULL rather than guess.
- Both added to `setup_db.py` (idempotent `IF NOT EXISTS`/existence-check
  guards) so a fresh DB reproduces the same schema.
- Full per-row detail: `data/reports/options_restatement/
  options_trades_restatement.json`.

## Net answer to the directive's three asks

1. **"Every affected row across all seasons, not just Season 1"** — done:
   Season 1's 27 rows (already flagged 2026-09-09) plus `options_trades`'
   120 pre-fix rows (this pass) are the complete universe of affected rows;
   Season 2/4 legacy option rows were checked and are NOT affected by
   either known bug.
2. **"Reconstruct from Alpaca options bars where possible; mark
   unrecoverable where not"** — done: 15/120 `options_trades` rows
   reconstructed from real bars; 93 marked unrecoverable with a specific,
   verified reason each (not a blanket "couldn't do it"); Season 1's 27
   rows reconfirmed unrecoverable for a *stronger* reason than previously
   documented (no contract identity was ever recorded, not just no durable
   price).
3. **"Report whether Seasons 1/2/4 become PSR/DSR-scoreable"** — done:
   Season 2 yes (6 players, all non-graduating), Seasons 1 and 4 no
   (insufficient volume, not corrupted data).

## Not done / explicitly out of scope this pass

- The units-inconsistency finding (mult=100 vs mult=1 across two code
  paths writing `options_trades`) is flagged but not fixed — fixing it
  would mean touching the *live* write path for a table currently in
  active use by `options-sosnoff`/Troi, out of scope for a restatement
  pass. Filed to `docs/XO_BACKLOG.md`.
- No dashboard/consumer was rewired to read `options_trades_restated` —
  same posture as `trades_restated`'s original ship (view built, wiring a
  future decision).
