# HM-F4 — Backtest-vs-Live-Paper Reconciliation (spec note)
# Pinned by HM-AUDIT-T0 (2026-05-28) BEFORE the Tier-1 run. XO 4.8 audit finding F-4.

## Why
The headline numbers — Super Trader Sharpe 4.78, IC Squadron Sharpe 3.93 / 97.8% WR —
are almost certainly optimistic (overfit / favorable window / unmodeled slippage).
Convert them from backtest to live-validated reality before any real-money gate.

## SOURCE-OF-TRUTH — LOCKED (do NOT deviate)
**The reconciliation MUST read `trades_clean`, NOT raw `trades`.**

Raw `trades` carries the ~153% price-writeback inflation (HM-TRADES-PRICE-WRITEBACK).
**235 rows** are flagged `known_contaminated=1`. Running the reconciliation off raw
`trades` inherits the inflation and the whole exercise is worthless.

`trades_clean` view DDL (confirmed live 2026-05-28):
```sql
CREATE VIEW trades_clean AS
  SELECT * FROM trades
  WHERE execution_type='alpaca_paper'
    AND executed_at >= '2026-05-21'
    AND COALESCE(known_contaminated,0)=0;
```
i.e. real Alpaca-paper fills only, post-writeback-fix date (2026-05-21), contamination excluded.
**Caveat:** the `executed_at >= '2026-05-21'` floor means the clean live window is short
(~1 week) — realized Sharpe from it will be noisy. Report N (trade count) alongside, and
do not over-interpret a short-window Sharpe.

## Comparison rung
Compare the live-paper realized number against the existing **OOS baseline:
Sharpe 2.692 / 65.8% WR / 456 trades** (NOT the IS 4.845, which the docs already
label OVERFIT). Sequence of trust: IS (4.845, overfit) → OOS (2.692, known) →
live-paper-clean (the real edge estimate, TBD from this run).

## What to compute (when the run is built — NOT part of T0)
From `trades_clean` over the live window: realized WR, realized Sharpe, and
slippage vs modeled entry/exit. The gap between 97.8% modeled and live-clean
realized is the real edge estimate. Everything strategic flows from that number.

STATUS: spec/source-of-truth pinned only. No reconciliation script written yet (Tier-1 work).
