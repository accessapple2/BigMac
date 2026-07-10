# Relay Report — S6 Finding 4: CSP P&L Reconciliation

**Date:** 2026-07-10 · **Branch:** exec-pipeline · **Status:** Investigated, fixed, shipped
**Commits:** `efd0b8b` (fix), preceded by `3ac69ce` + `b5145e5` (M-5 allocator / deployment-floor, unrelated but same session)

## The question

Original findings report (S6 performance dig): strategy table showed +$35.3k realized CSP P&L, but the account headline showed only +$1.5k since 2026-04-24. Two hypotheses offered: (a) strategy P&L is lifetime/cross-season while headline is S6-only, or (b) CSP `realized_pnl` books premium without netting assignment/close costs.

## What I found

Neither hypothesis was quite right. The real answer: **two structurally different books.**

- **Headline P&L** (`/api/account/equity-curve`, `/api/performance/summary`'s `headline` field) reads the **real Alpaca paper-brokerage account** via `GET /v2/account/portfolio/history` (`dashboard/app.py:10423-10468`, `_get_alpaca_equity_and_spy_raw`). This is actual broker equity, not a sum of any local trade table.
- **CSP/Wheel strategy cards** (`/api/strategy/pnl`, `/api/performance/summary`'s "CSP / Wheel" card) summed `options_trades.pnl` for `status='closed'`, agent_id in `{options-sosnoff, shadow-qwen35-csp}`, **no date/season filter at all**. This table is a fully internal, decoupled paper ledger (`options_books.current_cash`) — confirmed live that **zero** of the 89 closed CSP rows have a `broker_order_id` or `exec_status='filled'`. None of it ever touched the real Alpaca account the headline reads from.

**Deeper issue underneath that:** most of the $35.3k was priced via a synthetic VIX-formula ("fantasy CSP premium" — the code's own comment) before real quotes shipped. `engine/paper_trader.py` already has this era boundary formalized:

```python
TROI_REAL_QUOTES_ERA_START = "2026-07-07"  # P0-A HM-OPTIONS-FILL-INTEGRITY
```

The season leaderboard already grades CSP agents on `_csp_realized_pnl_real_quotes()` only (`exit_date >= TROI_REAL_QUOTES_ERA_START`) — that number was **$0.00 at investigation time**, correctly, since no real-quote CSP had closed yet. The two dashboard cards above were the only places still showing the unfiltered lifetime total, with no label indicating most of it was never a real fill.

Hypothesis (b) — un-netted assignment cost — is **not supported**. `wheel_assignment_ledger.assign_csp()` calls `close_options_trade()` on the *same* row (not a second row), which computes `pnl = entry_credit_debit - close_cost` in place. Assignment is netted correctly.

## The fix

Both queries now filter `options_trades` to `(structure != 'csp' OR exit_date >= TROI_REAL_QUOTES_ERA_START)` — scoped to CSP rows specifically so a future non-CSP options strategy (covered calls, spreads) sharing the same code path isn't silently affected by a pricing-era boundary that has nothing to do with it.

**Live before/after** (both endpoints):
| | Trades | P&L |
|---|---|---|
| Before | 89 | $35,301.64 |
| After | 2 | $2,511.39 |

The 2 remaining trades are both `shadow-qwen35-csp`, exit_date `2026-07-08`, confirmed directly against the DB. (Not $0.00 — 2 real-quote CSPs closed in the few hours between the initial investigation and shipping the fix; that's the fix correctly tracking live state, not a bug.)

3 new tests in `tests/test_csp_wheel_real_quotes_filter.py`: excludes pre-real-quotes CSP while including a non-CSP structure regardless of date (proves scoping doesn't leak), same assertion on the `/api/performance/summary` card, all-synthetic-era-yields-zero scenario. Full suite: 961 passed, same 14 pre-existing unrelated failures as the rest of tonight's session.

## Open items / not touched

- `engine/benchmark.py`'s module-level `DB` constant still points to `autonomous_trader.db` (a separate 6MB file, no `trades` table) while the fleet-P&L functions in the same file correctly hardcode `data/trader.db` — flagged during the earlier P0 benchmark-pipeline fix, still unexamined.
- No other `options_trades`-reading surfaces were audited for the same unfiltered-CSP pattern beyond these two endpoints — worth a quick grep if there's appetite (`grep -rn "options_trades" dashboard/app.py`) to confirm nothing else is still showing the inflated figure.

## Session cross-reference (same evening, exec-pipeline)

- `dd9fc3e` — P0: benchmark pipeline silent yfinance failure fixed
- `3ac69ce` — M-5 Multitronic allocator + deployment-floor advisory (S6 findings 5/1)
- `b5145e5` — deployment-floor calibration (long_equity_max_pct target, floor tightened to 1/2) — **live-fired correctly** on first post-fix check (7.3% actual vs 32.5% floor, BULL_CROSS)
- `efd0b8b` — this fix
