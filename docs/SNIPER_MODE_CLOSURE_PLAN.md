# Sniper Mode Closure Plan

**Author:** Scotty 2.4 (Claude Code Opus 4.7)
**Date:** 2026-05-07 ~20:30 MST
**Status:** **Plan only — awaiting Admiral go.** No agent halts, no scheduler edits, no DB mutations performed by this document.
**Trial endpoint:** Day 26/30 ends Saturday 2026-05-09 EOD MST.
**Source audit:** `docs/SCOTTY_AUDIT_2.md` Section C (Sniper Mode verdict).

---

## 1. Final Scorecard — Bug-Affected Metrics vs Corrected Metrics

### Bug-affected (`data/proving_ground.db::running_scorecard` row id=28)

| Metric | Value | Why suspect |
|---|---|---|
| total_trades | 244 | **Wrong universe.** Rolls up `daily_trades` from legacy ghost agents (deepseek-7b-grok4, ollama-plutus, qwen3-8b-flash, gemini-2.5-flash, ollama-llama, grok-4) — NOT `ollie-auto`. |
| cumulative_return | **+1259.99** | Summed `pnl_pct` across rollup, not portfolio %. Read as "1260% return" — wildly wrong. |
| rolling_win_rate | 75.0% | Aggregated over the rollup, not the routed Sniper player. |
| rolling_sharpe | 4.892 | Same — aggregate, not Sniper. |
| max_drawdown | **−87.557** | Synthetic curve from summed pnl_pct, not equity. Documented bug. `dd_bench_status=fail` — known. |
| exec_gap_pp | 4.0 | Apples-to-oranges (intent vs realized notional sizing). |
| Bench gates | 4 of 6 pass (wr/sharpe/cl/trades pass; dd/gap fail per known bug) | Misleading "almost-promote" signal. |

### Corrected — recomputed from `data/trader.db` (Sniper = `ollie-auto`)

| Metric | Value | Compare to OOS-A |
|---|---|---|
| Trades (30d) | **74** | — |
| Win rate (30d) | **90.5%** (38/42 closed) | OOS-A: 65.8% |
| Total realized P&L (30d) | **+$75.45** on $10k notional book | — |
| Total return (30d) | **+0.75%** | — |
| Avg trade notional | **$72.75** | Plutus: $204; Capitol: $265 |
| Daily Sharpe (annualized) | 14.6 | OOS-A: 2.69 |
| Max drawdown | 0.00% | — |

**Equity curve note:** `portfolio_history` has **zero rows for `ollie-auto`** in the last 30 days. The Sniper player is *not* writing to portfolio_history, which is why the Sharpe-from-curve cannot be cleanly computed and why the proving_ground rollup substituted ghost data. **This is the v2 prerequisite.**

### Rest-of-fleet ex-Sniper (`data/trader.db::trades`, 30d window)

| Metric | Value |
|---|---|
| Trades | 435 |
| Wins | 315 |
| WR | 72.4% |
| Total realized P&L | **+$6,688.40** |
| Plutus alone | +$4,108 (61% of fleet P&L) |

**Sniper is 89× smaller in absolute return than the rest of the fleet.** Its 14.6 Sharpe is a sizing artifact, not alpha — when avg notional drops to $73, daily-return variance collapses regardless of edge.

---

## 2. Ranked Decision

### KILL (recommended)

- Halt `ollie-auto` to `halt_mode='full'` on Saturday 2026-05-09 EOD MST.
- Remove from `dashboard/app.py:FLEET_ACTIVE` (currently includes "ollie-auto" at line 1445).
- Stop the proving_ground.daily_trades rollup OR fix it to track only `ollie-auto` trades (not the legacy fleet).
- Document lesson learned (sizing artifact pattern, gate-metric bug, $73 vs $200 notional).
- **Do NOT delete** any rows from `trades`, `portfolio_history`, or `proving_ground.*` — sacred data rule.

### HARD-EXTEND (only if Admiral wants to preserve "Sniper Mode" as a concept)

- Reset trial to a **fresh 30-day window** with notional-parity sizing (~$200/trade target).
- Require `portfolio_history` writer for `ollie-auto` BEFORE the trial starts.
- Use the new Proving Ground v2 acceptance criteria below (not the bugged scorecard).
- Trial ends 2026-06-08.

### Why not EXTEND (the marginal "more data" option)

- The data already shows the *architecture* is wrong (sizing throttle), not the data is thin.
- 4 more days at $73/trade adds zero information.

---

## 3. KILL Ritual (precise steps for Admiral execute, after go)

### Pre-flight (Sat 2026-05-09 morning)

```bash
# Confirm zero-or-low open positions for Sniper
sqlite3 data/trader.db "SELECT symbol, qty, avg_price FROM positions WHERE player_id='ollie-auto' AND qty != 0"
```

### Halt SQL (per CLAUDE.md manual halt pattern)

```sql
UPDATE ai_players
   SET halt_mode  = 'full',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '[2026-05-09] Sniper Mode trial KILL — sizing-artifact Sharpe, +0.75%/30d vs fleet +66.9%, KILL per docs/SNIPER_MODE_CLOSURE_PLAN.md'
 WHERE id = 'ollie-auto';
```

### FLEET_ACTIVE removal

`dashboard/app.py:1445` — remove the line `"ollie-auto",       # Fleet Commander (gate)`. One-line edit. Restart `com.trademinds.trader`.

### Lesson-doc commit

```
docs(retro): Sniper Mode trial KILL 2026-05-09 — sizing artifact lesson learned
```

Body: ~200 words documenting the three traps (rollup tracking the wrong universe, summed-pnl_pct as cumulative_return, throttle-to-win sizing).

### Sacred-data preservation

Per archive convention: code stays in repo. `engine/` modules tagged with the Sniper logic remain — muted via halt, not deleted. Rehab path: ghost-trade with corrected sizing for 30 days at any future point.

---

## 4. Lessons Learned (for retro doc)

### Lesson 1 — Rollup must match the universe of interest

`proving_ground.running_scorecard` aggregates `daily_trades` rows across legacy ghost agents (deepseek-7b-grok4, ollama-plutus, qwen3-8b-flash, etc.). The Sniper player (`ollie-auto`) never appeared in `daily_trades`. Result: scorecard described the legacy fleet, the Admiral read it as "Sniper Mode." **Fix: explicit `WHERE player_id IN (...)` scope on every scorecard read.**

### Lesson 2 — Sharpe needs daily equity, not summed pnl_pct

`cumulative_return` was implemented as `SUM(pnl_pct)` across closed trades. That's a sum-of-percent-changes, not a real cumulative return. A cumulative return needs `(equity_t / equity_0 - 1)`. The synthetic max_drawdown computed off this wrong curve gave −87.5% — physically impossible at the actual sizing. **Fix: portfolio_history writer for every promoted player; compute Sharpe and DD from that curve.**

### Lesson 3 — Throttled sizing inflates Sharpe artificially

When avg notional drops from fleet-norm ~$200 to Sniper's $73, daily-return variance collapses while expected return stays small. Sharpe = mean / std → mean shrinks proportionally but std shrinks faster → Sharpe inflates. **Fix: Proving Ground v2 acceptance criterion 5 (avg notional within ±25% of fleet median).**

---

## 5. Proving Ground v2 — Acceptance Criteria (Draft)

All metrics computed from `data/trader.db::portfolio_history` + `trades`. No `proving_ground.*` rollup tables (they re-introduce the bug).

| # | Gate | SQL/Formula | Threshold |
|---|---|---|---|
| 1 | Equity-curve Sharpe (annualized, daily) | `daily_ret = total_value/LAG(total_value,1) - 1; Sharpe = AVG(daily_ret)/STDDEV(daily_ret)*SQRT(252)` over 30d | **≥ 2.0** |
| 2 | Max drawdown (peak-to-trough) | Python pass on equity curve: `peak=max-so-far; dd=(peak-eq)/peak` | **≤ 10%** |
| 3 | 30d total return | `(last_total_value / first_total_value - 1) * 100` | **≥ +3%** |
| 4 | Trade count | `COUNT(*) FROM trades WHERE player_id=:p AND executed_at > datetime('now','-30 days')` | **≥ 100** |
| 5 | Avg notional within ±25% of fleet median | `AVG(qty*price) FROM trades WHERE player_id=:p ... -30d` vs `median(AVG by all active players)` | **0.75× ≤ x ≤ 1.25× fleet median** |
| 6 | Beats SPY 30d return AND ≥ 50% of rest-of-fleet return | SPY total return from yfinance/Polygon; rest-of-fleet from same trade query ex Sniper | **Both must pass** |

### Optional Gate 7 — Regime-tag check

When 30d-window VIX > 25 (high-vol regime), require **Sharpe ≥ 4.0** to validate the OOS-A regime-robust ≥4 baseline.

### Pre-flight checklist before any v2 trial

- [ ] Player has at least 30 days of `portfolio_history` writes.
- [ ] Avg notional ≥ fleet median × 0.75 from cycle 1 onward.
- [ ] Scorecard query explicitly scopes to the player_id under test.

---

## 6. Open Questions for Admiral

1. **KILL or HARD-EXTEND?** I recommend KILL. The architecture (rollup-wrong-universe + sizing-throttle) makes the current trial uninformative.
2. **Should `ollie-auto` be removed from `FLEET_ACTIVE` (dashboard list) and `_EXECUTION_PORTFOLIO_BY_PLAYER` (paper_trader.py routing) atomically, or staged?** I recommend atomic — leaving stale entries causes phantom rows in dashboards.
3. **Proving Ground v2 — own-launchd-job or rolled into existing scorecard runner?** If own-job, suggest 30-min cadence, writes to a new `proving_ground_v2` table to avoid breaking existing reads.

**Halt condition:** await Admiral go on KILL vs HARD-EXTEND before any execution.
