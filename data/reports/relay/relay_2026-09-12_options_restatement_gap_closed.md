# Relay — 2026-09-12. Options premium restatement — gap closed, all seasons.

RULE #1 honored throughout: every write is additive (an already-existing
`ALTER TABLE ADD COLUMN` schema, filled in only where previously blank).
No `options_trades` row's original recorded value (`entry_credit_debit`,
`exit_credit_debit`, `pnl`) was ever touched.

## Correcting the premise first

**The restatement was not queued-and-never-run — it ran and shipped on
2026-09-11**, commit `d6eaf3c` ("feat: B6 -- options premium restatement,
all seasons"), `docs/XO_BACKLOG.md` marks it `DONE 2026-09-11`, and the
live DB already carried 120 processed rows with the four restatement
columns populated before I touched anything tonight. Full original
results: `relay_2026-09-11_B6_options_premium_restatement.md`.

**What was actually true, and worth the "run it" instruction regardless:**
that pass had a real, verifiable scoping gap. `scripts/options_trades_
restatement.py`'s row-selection filtered on **`exit_date < '2026-07-07'`**,
but the bug it exists to restate lives in **entry** pricing (`engine/
wheel_strategy.py`/`shadow_csp.py` computed `entry_credit_debit` from a
synthetic vix/500 formula at OPEN time, fixed at the source 2026-07-07).
A row opened inside the bug window that happened to close a day or more
after 07-07 was silently never checked. Found 5 such rows sitting with a
blank `restatement_basis` in the live DB before touching anything — 3
`shadow-qwen35-csp` CSP closes (entered 06-26/06-28, exited 07-08/07-20),
1 `test-door1-regression` fixture row (entered 07-03), and 1 genuinely
out-of-scope row (`strategy:bull_spread_v1` id 140, entered 07-13 — after
the fix, correctly never in scope).

**Fixed and reran, idempotently.** Filter is now `(entry_date <
'2026-07-07' OR exit_date < '2026-07-07' OR exit_date IS NULL) AND
(restatement_basis IS NULL OR restatement_basis = '')` — the second clause
means rerunning can never re-fetch or overwrite any of the original 120
rows' already-committed values, only fill genuine gaps. Backup taken first
(`trader_prerestatement_gap_2026-09-12T184451.db`, `integrity_check=ok`).

**A mistake caught and fixed in the same pass, not swept under anything:**
the script's JSON report writer overwrote the file wholesale rather than
merging, so my first `--write` run briefly reduced `data/reports/
options_restatement/options_trades_restatement.json` from the original
120-row detail down to just the 4 newly-covered rows. The database itself
was never at risk (the script only `UPDATE`s the 4 rows it selected; the
other 120 were untouched in the DB the whole time) — but the standalone
JSON export briefly misrepresented itself as complete. Recovered the
original 120 from git history (`d6eaf3c`), merged by id, now 124 rows,
verified complete. Fixed the script itself to merge-by-id on every future
run instead of overwriting, so this can't recur.

**Regression test**: `tests/test_options_trades_restatement.py` (7 tests)
— `detect_mult()`/`occ_symbol()` pure-function coverage, plus three tests
against a throwaway sqlite fixture proving the corrected SQL (a) catches
the exact entry-side gap the old filter missed, (b) never re-selects an
already-restated row, (c) correctly excludes a row entered after the fix.
Full suite (`.venv/bin/python3`, the correct interpreter — bare `python3`
under-reports today, several files import `fastapi`/`alpaca`): 1207
passed, 13 pre-existing unrelated failures (confirmed via `git stash`
diff), zero new regressions.

## Final tally — all 125 `options_trades` rows, gap closed

| `restatement_basis` | n | What it means |
|---|---|---|
| `real_alpaca_bar` | 15 | Reconstructed from a real Alpaca historical option bar. |
| `unrecoverable_no_alpaca_bar` | 91 (was 87 — +4 from tonight's gap-fill) | Alpaca has no recorded bar for that contract in that window. |
| `not_closed_no_restatement_needed` | 12 | Never settled (open/canceled/failed) — no P&L claim to check. |
| `unrecoverable_unknown_unit_convention` | 6 | The very first rows ever written (2026-04-22), a third unit convention that reproduces under neither known scale. |
| *(blank — correctly out of scope)* | 1 | Row 140, entered 2026-07-13 — after the fix, no bug exposure, restatement doesn't apply. |

**Corrected P&L: $4.68 total**, across exactly 15 rows (all
`strategy:bull_spread_v1`, all SPY `bull_put_spread`) — the only rows in
the entire 125-row table with an independently-verified real number.
14 of those 15 never had a *recorded* `pnl` at all (blank — they were
bulk-closed by a 2026-05-05 reconciliation script, not a real trading
decision); the restatement backfilled a defensible mark from real bars,
but the event itself still isn't an organic exit. Only **one row in the
whole table** (id 28, $1.82 booked → $1.71 restated) is both a real market
outcome and a natural exit (`expired_otm`).

## Season 1/2/4 PSR/DSR-scoreability — unchanged, and correctly so

**This restatement pass has zero effect on that question.**
`scripts/archive_harness.py` (the tool that actually computes PSR/DSR
scoreability) explicitly excludes `options_trades` by design — its own
comment: *"Multi-leg spread strategies (`options_trades` table, different
schema...) are out of scope for this pass."* It reads only the `trades`
table (`asset_type='option'`), a separate 27-row corruption already
reconfirmed unrecoverable in the 09-11 pass (no `strike_price`/
`expiry_date` was ever recorded for those rows — not a durability gap, a
contract-identity gap). Nothing about `trades` changed today. The 09-11
verdict stands, unchanged, because nothing that could change it moved:

| Season | Scoreable? | Why |
|---|---|---|
| 1 | **NO** | Every player `INSUFFICIENT_N` even with options included — too few clean rows, not a data-quality exclusion. |
| 2 | **YES** | 6/10 players clear n≥5, zero ratio-filter exclusions (no Season-1-style corruption signature) — legitimate read, all show negative/zero Sharpe and DSR=0.0000, nothing graduates. |
| 4 | **NO** | n=1 for the only qualifying player — a ~1-day season, trivial volume. |

## Which options strategies have a real premium-priced record, and which are artifact

| Strategy / agent | Rows | Real, verified record? |
|---|---|---|
| **`options-sosnoff`** (Troi, CSP/wheel) | 84, all pre-07-07 | **100% artifact.** Every single row — the strategy's entire $29,868.74 booked "profit" — is `unrecoverable_no_alpaca_bar`. Not one trade can be confirmed against real market pricing; the entry premiums were computed by the known-buggy synthetic formula and Alpaca has no bar to check any of them against (72 leveraged-ETF legs — TQQQ/SOXL/UPRO — plus 12 SPY/QQQ that also failed to reconstruct). |
| **`shadow-qwen35-csp`** (bakeoff arm) | 6, all pre-07-07 | **100% artifact.** Same disease, same verdict — all 6 rows unrecoverable, its entire $6,076.23 booked record unverified. This is the same n=3 (now known to be n=6 total, 3 already in scope + 3 recovered tonight) that Bridge Phase 2 earlier today already withheld a DSR number for on `n<MIN_N` grounds — now confirmed the underlying P&L itself was never verifiable either, a second, independent reason not to trust this arm's numbers. |
| **`strategy:bull_spread_v1`** — `bull_call_spread` | 5 (+1 `bull_put_spread` sharing the same zombie batch) | **100% artifact.** Covered in full in this evening's earlier audit — zombie rows, one literally `symbol='TEST'`, bulk-closed by a cleanup script, `pnl=NULL` on all of them, restatement basis `unrecoverable_unknown_unit_convention`. Zero real `bull_call_spread` market exits exist, ever. |
| **`strategy:bull_spread_v1`** — `bull_put_spread` | 21 | **Overwhelmingly artifact, with one real exception.** 4 failed to execute (broker never got the order); 14 were bulk-reconciled (recoverable pricing, but the *event* is a cleanup script, not a trading decision) — together summing to the $4.68 restated total above; 1 (id 140) is out of the bug's scope but has its own unrelated data-integrity problem (exit date a month past its own stated expiration, flagged in the earlier audit); exactly **1 row (id 28)** is a real, verified, organically-decided outcome: +$1.71. |
| **`swingdesk-manual`** | 8, none closed | **No record yet either way.** All `not_closed_no_restatement_needed` — nothing has settled to judge. |
| `test-door1-regression` | 1 | Not a strategy — a regression-test fixture (`pnl=0.0`), included in the restatement sweep for consistency, not evidence of anything. |

**One-sentence version for the decision this feeds**: across every options
strategy this repo has ever run, there is exactly **one** genuinely
real, independently-verified, organically-decided premium-priced
outcome in the data — a single $1.71 SPY bull put spread from
2026-05-14. Everything else — Troi's full CSP/wheel history, the shadow
bakeoff arm, every bull call spread ever, and all but one bull put spread
— is either synthetic-formula pricing that can't be checked, a bulk
cleanup artifact, or a trade that never executed at all.
