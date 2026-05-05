# HM-AE Option B Reconcile Log — 2026-05-05

## Action

Reconciled 13 stale `options_trades` rows (`agent_id='strategy:bull_spread_v1'`,
`status='open'`, `exec_status='open'`) to match Alpaca paper's actual state
(positions netted to zero earlier today via the legacy single-leg close path).

## Backup

Pre-UPDATE backup: `backups/trader.db.pre-hm-ae-reconcile-20260505_154909`
- Size: 232 MB
- Integrity: `PRAGMA integrity_check` → `ok`
- options_trades row count: 23 (matches live)

## SQL applied

```sql
UPDATE options_trades
   SET status = 'closed',
       exec_status = 'closed',
       exit_date = datetime('now'),
       exit_reason = 'HM-AE-Option-B-reconcile-2026-05-05'
 WHERE agent_id = 'strategy:bull_spread_v1'
   AND status = 'open'
   AND exec_status = 'open';
```

Executed 2026-05-05 15:49:31 MST. Rows affected: 13.

## Affected rows

13 rows, IDs **14–26 inclusive**:

| id | structure | symbol | entry_credit_debit | entry_local |
|---|---|---|---|---|
| 14 | bull_put_spread | SPY | 1.79  | 2026-05-05 08:34:44 |
| 15 | bull_put_spread | SPY | 1.55  | 2026-05-05 08:50:19 |
| 16 | bull_put_spread | SPY | 1.615 | 2026-05-05 09:06:06 |
| 17 | bull_put_spread | SPY | 1.695 | 2026-05-05 09:21:26 |
| 18 | bull_put_spread | SPY | 1.67  | 2026-05-05 09:36:42 |
| 19 | bull_put_spread | SPY | 1.76  | 2026-05-05 09:51:58 |
| 20 | bull_put_spread | SPY | 1.77  | 2026-05-05 10:09:36 |
| 21 | bull_put_spread | SPY | 1.91  | 2026-05-05 10:25:51 |
| 22 | bull_put_spread | SPY | 1.69  | 2026-05-05 10:41:10 |
| 23 | bull_put_spread | SPY | 1.745 | 2026-05-05 10:56:32 |
| 24 | bull_put_spread | SPY | 1.605 | 2026-05-05 11:11:48 |
| 25 | bull_put_spread | SPY | 1.675 | 2026-05-05 11:27:04 |
| 26 | bull_put_spread | SPY | 1.755 | 2026-05-05 11:42:20 |

Row 14 is the original HM-Z first-live-spread fill (broker_order_id `88d58691`).

## Untouched (deliberately)

- 4 rows with `exec_status='failed_pre_fix'` — HM-Z ghost rows from this
  morning's pre-fix bytecode window
- 6 rows with `exec_status='test_cleanup'` — pre-existing wiring tests
  from 2026-04-22 (per HM-AE Thread 1; documented in
  `docs/SCHEMA.md:290` and `docs/BACKTEST_2026-04-25_6MONTH.md`)

## Fields deliberately left NULL

- `exit_credit_debit`: ground-truth not available from the legacy
  single-leg close path that fired today (8 separate single-leg orders
  closed the 13 spreads asymmetrically — see HM-AE investigation
  commit `34ffb11` for the full close-order trail)
- `pnl`: same reason; would require constructing per-spread P&L from
  the leg-level close fills which is a second-pass exercise

The +$518 net cash impact for the day is already reconciled within $2
in the HM-AE investigation. This UPDATE does not affect cash; it only
reconciles the position-state tracking.

## Why this was needed

PID 70953 has been running pre-HM-AC-Option-B bytecode all day.
HM-AC Option B (commit `19c6746`) ships the atomic MLEG-close path
that updates `options_trades.status='open' → 'closed'` correctly.
That fix has been on disk for ~3.5 hours but unloaded.

When `exit_manager` fired close intents on the 13 spreads today, the
legacy single-leg close path:

- Successfully netted positions to zero on Alpaca paper (via 8 separate
  single-leg fills: 4 SELL of long puts, 4 BUY-to-close of short puts)
- Did **NOT** update `options_trades.status` locally

Without this reconcile, every future Item 5 / Item 6 reconciliation
run would surface 6 OCC symbols as `options_routed_drift.in_internal_not_alpaca`
indefinitely — false-drift findings that would erode confidence in
the canary.

## Post-reconcile verification

- `SELECT COUNT(*) FROM options_trades WHERE agent_id='strategy:bull_spread_v1'
  AND status='open' AND exec_status='open'` → **0**
- `engine.reconciliation.get_internal_options_positions()` → **0 legs across 0 agents**
- Item 6 canary on next 13:30 MST run will report **no options drift**
- Ghosts and test_cleanup row counts unchanged (4 and 6)

## Restart pattern

The atomic service restart that follows this reconcile loads, in addition
to HM-AC Option B:

- HM-V success-side NTFY (5 broker-fill sites)
- HM-AA-broad enrichment (4 pristine error sites)
- HM-AC Option A pre-flight buying-power check
- Day-2 lessons + Thread A canary route_mode filter
- Kirk realign to Schwab `data/real_holdings.json`
- Item 6 options reconciliation pass

After restart, future spread closes update `options_trades.status`
correctly through the new MLEG path.

## What this does NOT fix

- The legacy single-leg close path in `engine/alpaca_options.py` still
  exists for genuine single-leg closes (e.g., dalio-metals options).
  It still doesn't update `options_trades.status`. Future single-leg
  close drift can recur until that path is patched. Separate session,
  low urgency (no current single-leg-only options agents are firing
  closes — bull_spread_v1 is the only options producer today).
- Cash-side P&L attribution for the 13 spreads remains unattributed
  in the database. Per HM-AE investigation, the day's net cash impact
  is +$518; constructing per-spread P&L requires matching each MLEG
  open against the corresponding asymmetric single-leg closes.
- Row 14's HM-Z significance (the original first-live-spread fill)
  is preserved in commit history but no longer surfaces from a SELECT
  on `options_trades` filtered to open rows.

## Rollback (if needed)

```bash
sqlite3 data/trader.db ".restore 'backups/trader.db.pre-hm-ae-reconcile-20260505_154909'"
```

Note: rollback would restore the stale-row state and re-introduce the
false-drift findings on the next reconciliation. Not recommended unless
investigation reveals the reconcile was applied to wrong rows.

## Closes

- HM-AE remediation (Admiral pick: Option B per HM-AE deliverable
  commit `34ffb11`)
- Internal-vs-Alpaca drift for today's 13 bull_put_spread fills

## Cross-references

- HM-AE investigation: commit `34ffb11`
- HM-AC Option B fix: commit `19c6746`
- HM-Z first live spread fill: row 14 (broker_order_id `88d58691`)
- Item 6 options reconciliation: commit `c7e0573`
