# OllieTrades Kill Gate — Season 6 / Door 1

Pre-committed 2026-06-19. Do not edit after DAY 0 (~2026-06-24). Gates are pass/fail, not
negotiated. If a gate is ambiguous on the day, it fails.

## Window

| Milestone | Approx Date | Description |
|-----------|-------------|-------------|
| DAY 0 | ~2026-06-24 | Gate clock starts. Screenshot baseline equity-vs-SPY at 0%. |
| DAY 30 verdict | ~2026-07-24 | Pull headline numbers. Run against gates below. |
| MSI return wall | ~2026-08-18 | Costco 90-day deadline. Confirm against receipt. |

Buffer = ~25 days between verdict and return wall. Don't drift DAY 0.

## Gates (all must pass for KEEP)

### G1 — Money

> **CSP net realized P&L + open-position MTM ≥ +0.5% of starting equity (~+$500)**

- Source: `/api/strategy/pnl` bucket `csp_wheel` → pnl field
- MTM intrinsic: query `options_trades` where `status='open'` → `SUM(mtm_intrinsic)`
- Starting equity: screenshot taken at DAY 0

### G2 — Risk

> **Account max drawdown over the window < SPY max drawdown (or < 3% if SPY flat)**

- Source: `/api/account/equity-curve` → daily `account` series
- Max DD = `max(peak - trough) / peak` over the DAY 0 → DAY 30 window
- SPY flat defined as: SPY return < ±1% over the window

### G3 — Tail

> **No single closed trade or assignment loss > 20% of total premium collected over the window**

- Source: `options_trades` where `status='closed'` AND `entry_date >= DAY_0`
- Premium collected = `SUM(entry_credit_debit)` (positive = credit received)
- Worst loss = `MIN(pnl)` (most negative single row)
- Tripwire: `abs(worst_loss) / premium_collected > 0.20` → G3 FAIL
- If no closed trades yet: G3 passes (no realized loss)

### G4 — vs Paid

> **OllieTrades (return / max_DD ratio) ≥ the parallel benchmark's same ratio**

Parallel benchmark started same day, same window:
- Income path: Option Alpha paper log and/or JEPI/JEPQ return
- Congress path: NANC/KRUZ return

If benchmark is untested (no data): G4 is *inconclusive*, not a fail — note it.

## KEEP branch

G1 + G2 + G3 all pass:
- Scale CSP book, halt all other strategies permanently
- Re-run this gate every 30 days (rolling)
- Hardware stays; return window is irrelevant

## G3-only fail (tail event)

G1 + G2 pass but G3 fails:
- One repair cycle: lower-delta puts, liquid underlyings, smaller size
- Re-run one more 30-day gate
- Fails G3 again → FAIL branch. No third try.

## FAIL branch

G1 flat/negative, or G3 fails twice, or G4 dominated by benchmark:
1. **Return MSI to Costco before ~Aug 18 (day 90).** Bring packaging.
2. Stand up the substitute that matches what worked:
   - Hands-off income → **JEPI / JEPQ** (covered-call ETFs, ~8–10% yield, 0.35% ER)
   - Self-directed wheel → **Option Alpha** ($39/mo, your own broker)
   - Congress edge → **NANC / KRUZ** ETFs or **Autopilot** app
3. Park core capital in SPY or one of the above.
4. Archive OllieTrades read-only as research sandbox.

## Day-30 query cheat sheet

```bash
# G1 — realized P&L
curl -s http://localhost:8000/api/strategy/pnl | python3 -m json.tool | grep csp_wheel -A4

# G1 — open MTM intrinsic
cd ~/autonomous-trader
python3 -c "
import sqlite3; db=sqlite3.connect('data/trader.db')
r=db.execute(\"SELECT SUM(mtm_intrinsic), COUNT(*) FROM options_trades WHERE status='open'\").fetchone()
print('open MTM intrinsic:', r)
"

# G2/G3 context
curl -s http://localhost:8000/api/account/equity-curve | python3 -m json.tool | grep -E 'account_pct|spy_pct|edge'
```
