# Relay: CSP-era P&L sweep (HM-P&L-RECONCILIATION S6, work block 2)

**Date:** 2026-07-10
**Commit:** `5b0395b` (pushed to `exec-pipeline`)
**Prior work block:** Finding 4 root-cause + first two dashboard fixes — see
`relay_2026-07-10_finding4-pnl-reconciliation.md` (commit `8837da6`), which
this sweep directly follows up on.

## Ask

Captain's request after Finding 4 shipped: "sweep options_trades for other
unfiltered CSP surfaces." Finding 4 fixed two dashboard cards
(`strategy_pnl()`, `performance_summary()`'s CSP/Wheel card) that queried
`options_trades` without excluding pre-`TROI_REAL_QUOTES_ERA_START`
(2026-07-07) synthetic CSP premium. The open question left in that report:
"no other options_trades surfaces audited beyond these two."

## Method

Delegated to a background Explore agent: find every caller of
`get_portfolio_with_pnl()` and every other direct `options_trades` query
involving `structure='csp'`, and classify each by whether it reads the raw
(`total_value`/`return_pct`) or restated (`total_value_restated`/
`return_pct_restated`) fields. It returned 10 numbered findings. I read the
actual call sites myself (not just the agent's summary) before fixing
anything, then fixed 8, left 2 deliberately alone.

## What shipped (8 fixes)

**Decision-affecting (not just display):**

1. `engine/providers/base.py::build_prompt()` — the "Competitive
   Intelligence" block injected into **every active fleet agent's LLM
   system prompt, every scan cycle**. Was feeding `options-sosnoff` in at
   #2 fleet-wide with $42,748.94/+510.7% — a number the real account never
   had — driving "the leader has $X, you're $Y behind" framing into every
   other agent's prompt (and arguably reinforcing her own behavior off a
   fantasy number, in her own prompt too).
2. `engine/leader_signal.py::_get_standings()` — feeds
   `_get_leader_recent_buys()` (copy-the-leader signal injection) and
   `run_weekly_elimination()` (pauses agents at ≤ -15% return). Same
   inflated figure was distorting both.

**Display/reporting:**

3. `dashboard/app.py::player_detail()` (`/api/arena/player/{id}`) — new
   override block, same pattern as the existing Enterprise-Computer metals
   override just below it.
4. `dashboard/app.py::get_capital()`'s `_player_capital()` (`/api/capital`).
5. `dashboard/app.py::leaderboard()`'s CSP win_rate/trade_counts block —
   this one was a *different* bug flavor: it filtered on the wrong era
   constant entirely (`exit_date < TROI_V2_ERA_START`, i.e. counted the
   all-synthetic pre-boundary book) instead of
   `exit_date >= TROI_REAL_QUOTES_ERA_START`. Fixed the constant, fixed the
   comparison direction, added an explicit `else` branch to zero the
   counts when no real-quotes-era CSP has closed (previously would have
   silently left stale/undefined values).
6. `dashboard/app.py::options_book_summary()` (`/api/options/book-summary`)
   — both `realized_pnl` and `today_pnl` queries, scoped so the era filter
   only excludes `structure='csp'` rows (non-CSP structures still always
   count).
7. `main.py::run_daily_summary()` — Telegram daily summary message.
8. `engine/ai_brain.py` — scan-cycle console log line (cosmetic only).

## Deliberately NOT touched

- **`dashboard/app.py::player_pnl()`** (`/api/arena/player/{id}/pnl`) —
  already returns both raw and restated fields with clear labels. Grepped
  both `index.html` and `bridge-v2.html`: neither currently calls this
  endpoint. No live consumer to mislead; left as-is rather than guessing at
  a fix for a dead endpoint.
- **`engine/paper_trader.py::get_portfolio_with_pnl()` itself** — not a bug.
  This is the intentional dual-field root function. Its own comment
  explains the raw fields are left untouched because
  `save_equity_snapshot()` and other historical-equity-curve writers depend
  on that exact shape for continuity — restating those retroactively would
  break the recorded equity curve. Every *caller* that reads the raw fields
  for current-state display or decisions is the actual bug; the function
  itself is correct by design.

## Testing

- New file `tests/test_pnl_restated_sweep.py`, 3 tests:
  - `_get_standings()` uses restated value, ranks correctly off it.
  - `leaderboard()`'s CSP stats use the real-quotes-era boundary, not the
    unrelated v2 (position-sizing) era boundary.
  - `options_book_summary()` excludes pre-boundary CSP only, still
    includes non-CSP structures regardless of date (proves the scoping
    doesn't leak into other strategies).
- Full suite: `.venv/bin/python3 -m pytest tests/ -q` → **964 passed**,
  same 14 pre-existing unrelated failures as the rest of this session
  (bbkc squeeze scanner, conviction-stop shadow, fleet-trail conviction
  scale, ollama cancel-on-timeout, quality-gate hold x4, universe-filter
  x3, war-room instrumentation x3) — confirmed no regressions introduced.
- `py_compile` clean on all 5 touched source files.
- Trader restarted (`zsh scripts/trader_restart.sh`), single-PID bind
  confirmed, zero orphans.

## Live verification (post-restart, production data)

```
leaderboard: options-sosnoff        trades: 0, win_rate: 0.0
  (correct -- zero real-quotes-era CSP closes)
player_detail: options-sosnoff      total_value: 12880.2, return_pct: 84.0
  (was $42,748.94 / +510.7%)
capital: Troi                       total_value: 12880.2
  (was inflated to match the above)
options book-summary: fleet         realized_pnl: 1.82
  (was including $29,868.74 fantasy premium)
```

`logs/trader.log` / `logs/trader_error.log` checked for errors touching
`leader_signal`, `providers/base`, `build_prompt` since restart — clean.

## Open items (not touched this pass)

1. **`engine/benchmark.py`'s `DB = "autonomous_trader.db"`** — a second,
   much smaller (6MB, no `trades` table) DB, distinct from the canonical
   827MB `data/trader.db` used everywhere else. Flagged in the Finding 4
   report too; still not investigated or fixed. Unrelated to the CSP-era
   bug specifically, but the same "check what a module actually reads
   before trusting its numbers" discipline applies.
2. **`options_book_summary()`'s `total_trades`/`wins`/`losses` fields**
   (distinct from `realized_pnl`/`today_pnl`, which this pass fixed) come
   from a summary column stored directly in the `options_books` table row,
   not computed from `options_trades` at request time. Noticed during live
   verification but not confirmed to have the same disease and not
   touched — worth a follow-up look if those numbers get relied on
   anywhere.

## Bottom line

Two of these eight were the ones that mattered most: a fantasy P&L figure
was live-feeding into fleet LLM prompts and into the elimination gate,
not just a dashboard card. Both are fixed and live-verified. The
remaining six are display/reporting correctness fixes, same root cause,
lower individual stakes.
