# Relay: options_book_summary trade counts + Relay Doctrine (S6, work block 3)

**Date:** 2026-07-10
**Commit:** `79baae4` (pushed to `exec-pipeline`)
**Prior work blocks:** Finding 4 (`8837da6`), CSP-era sweep (`5b0395b` +
relay `e33db80`). This block follows up on that sweep's own "open items"
list — item 2, `options_book_summary()`'s `total_trades`/`wins`/`losses`.

## Ask

Captain: "go check the options_book_summary total_trades/wins/losses
columns." Follow-up to the flag left in the prior sweep's relay report.

## What was found

Confirmed the same disease as `realized_pnl`/`today_pnl` (fixed in
`5b0395b`), plus one separate, unrelated bug found along the way.

**Bug 1 — same CSP-era issue.** `total_trades`/`wins`/`losses` on
`options_books` are stored counters, updated in `engine/options_exec.py`
with zero era-awareness — never filtered by structure or by
`TROI_REAL_QUOTES_ERA_START`. Live numbers before the fix:

```
fleet: total_trades=84, wins=81, losses=4   (96% win rate)
ghost: total_trades=7,  wins=5,  losses=0   (100% win rate)
```

Actual breakdown by structure (`options_trades`, `status='closed'`):

```
fleet: bull_call_spread=5, bull_put_spread=16, csp=84 (105 total closed)
ghost: csp=5
```

All 84 of fleet's CSP closes have `exit_date` between 2026-05-26 and
2026-07-04 — every single one is pre-boundary (synthetic VIX-formula
pricing, `TROI_REAL_QUOTES_ERA_START` = 2026-07-07). Restated to
real-quotes-eligible only (non-CSP, or CSP with `exit_date >=
2026-07-07`):

```
fleet: 21 eligible closes, 1 win, 0 losses (20 have NULL pnl, unclassifiable)
ghost: 2 eligible closes, 2 wins, 0 losses
```

So the "96% win rate" being reported was ~96% synthetic data; the honest
picture for fleet right now is 1 confirmed real win out of 21 eligible
closes, with most of those 21 not even having a recorded P&L yet.

**Bug 2 — separate, NOT fixed.** `total_trades` increments on trade
**open** (`options_exec.py:145`); `wins`/`losses` increment on trade
**close** (`options_exec.py:208-217`). These are two different counters
measuring two different events that don't sum to anything meaningful
together. Worse: fleet's 21 spread trades (`bull_call_spread`,
`bull_put_spread`) never incremented `total_trades` at all — they were
evidently opened through a path that bypassed `open_options_trade()`
(backfill/migration, most likely), so the stored `84` undercounted even
before considering the era issue (105 trades actually closed). Fixing
this would mean deciding whether to trust/backfill the stored counters or
switch to computing everything live from `options_trades` — a bigger,
separate call. Flagged for the Captain, not actioned.

**No live display consumer.** Grepped both `dashboard/static/index.html`
and `dashboard/static/bridge-v2.html`: the only UI wired to
`/api/options/book-summary` (`pollOptionsEngine()` in `index.html`) reads
`current_cash`, `today_pnl`, `realized_pnl`, and open positions —
`total_trades`/`wins`/`losses` are not rendered anywhere from this
endpoint today. (The `wins`/`losses` visible elsewhere in `bridge-v2.html`
come from an unrelated endpoint, `/api/fleet-report-card`.) Shipped the
fix anyway for correctness and consistency with every other
`get_portfolio_with_pnl`-adjacent surface touched this season, since the
data mutation risk is zero (read-time override only, stored counters
untouched).

## Fix

`dashboard/app.py::options_book_summary()` — added a live query using the
identical `(structure != 'csp' OR exit_date >= ?)` filter already applied
to `realized_pnl`/`today_pnl` in the same function, overriding
`info["total_trades"]`/`info["wins"]`/`info["losses"]` before they're
returned. Stored `options_books` counters are not written to.

## Testing

- Extended the existing `test_options_book_summary_...` test in
  `tests/test_pnl_restated_sweep.py`: fixture's `options_books` table
  deliberately has no `total_trades`/`wins`/`losses` columns at all,
  proving the endpoint computes these live rather than passing through
  stored values. Added a loss-side fixture row (previous fixture only had
  wins) to cover both branches of the win/loss CASE.
- Full suite: 964 passed, same 14 pre-existing unrelated failures as
  every prior run this session (bbkc squeeze scanner, conviction-stop
  shadow, fleet-trail conviction scale, ollama cancel-on-timeout,
  quality-gate hold x4, universe-filter x3, war-room instrumentation x3).
- `py_compile` clean.
- Trader restarted, single-PID bind confirmed, zero orphans.

## Live verification (post-restart, production data)

```
GET /api/options/book-summary  (200, 0.01s)
fleet: total_trades=21, wins=1, losses=0   (was 84/81/4)
ghost: total_trades=2,  wins=2, losses=0   (was 7/5/0)
```

`logs/trader_error.log` / `logs/trader.log` checked — no errors tied to
`options_book_summary`, `book-summary`, or `app.py`. (Unrelated ambient
noise present: Ollie Max Ollama host briefly unreachable at restart —
pre-existing infra flakiness, nothing to do with this change.)

## Also this block: Relay Doctrine added to CLAUDE.md

Captain made the informal relay workflow (introduced mid-session in work
block 1) a standing, permanent rule — added a "Relay Doctrine" section to
`CLAUDE.md` (after "Verify before claiming"): every completed work block
or directive writes a relay report to `data/reports/relay/` before being
declared done, committed and pushed with the work, in every session going
forward — not just P&L-reconciliation-style investigations.

## Open items (carried forward)

1. **`total_trades` open-vs-close counting bug** (Bug 2 above) — needs a
   Captain decision on approach before touching it.
2. From the prior sweep's open items, still open: `engine/benchmark.py`'s
   `autonomous_trader.db` usage was separately investigated this session
   and confirmed to be an intentional, correctly-applied architecture
   (14 files share the same split; not a bug) — that item is now closed,
   no fix needed.
