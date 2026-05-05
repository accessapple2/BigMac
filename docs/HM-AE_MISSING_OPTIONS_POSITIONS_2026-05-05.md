# HM-AE — Missing Alpaca Options Positions Investigation

*2026-05-05. Read-only investigation. Three threads. Admiral picks remediation.*

## Question

Item 6's first reconciliation run flagged 13 internal-book `bull_put_spread` rows (status=open, exec_status=open, with broker_order_ids) whose corresponding 13 MLEG fills exist on Alpaca, but Alpaca's positions API returns **0 options positions**. Plus 6 mystery `test_cleanup` rows. Plus a cash-math gap that didn't fit any clean hypothesis.

## Headline finding

**The 13 spreads opened AND closed today, leaving zero Alpaca options positions and a fully-coherent +$518 cash trail. The internal `options_trades` rows weren't updated to reflect the closes — that is the *real* divergence Item 6 caught.** PID 70953 is running pre-HM-AC bytecode, so HM-AC Option B's atomic MLEG-close path (commit 19c6746, on disk only) didn't fire. The pre-HM-AC single-leg close path did, and Alpaca correctly netted the legs to flat.

## Thread 1 — `test_cleanup` origin

**Resolved: not from today.** All 6 rows are dated **2026-04-22**, predate today's session by ~2 weeks, and have **NO `broker_order_id`** (never went to a broker).

| id | structure | symbol | entry_date | entry_credit |
|---|---|---|---|---|
| 2 | bull_call_spread | SPY | 2026-04-22T10:00 | -3.0 |
| 3 | bull_put_spread | QQQ | 2026-04-22T10:00 | +5.0 |
| 4 | bull_call_spread | NVDA | 2026-04-22T10:00 | -1.5 |
| 5 | bull_call_spread | AAPL | 2026-04-22T10:00 | -2.5 |
| 8 | bull_call_spread | SPY | 2026-04-22T20:27 (UTC) | -3.0 |
| 9 | bull_call_spread | **TEST** | 2026-04-22T13:00 | -0.5 |

Documentation already covered these:
- `docs/SCHEMA.md:290` — "Existing rows have `exec_status = 'test_cleanup'` (pre-gate-flip seeding)."
- `docs/BACKTEST_2026-04-25_6MONTH.md:131-140` — explicit listing.

No code path currently writes `'test_cleanup'`; rows were inserted manually as setup/wiring tests. Item 6's filter `status='open' AND exec_status='open'` already excludes them — a non-issue. The earlier prompt's "origin unknown today" framing was a misread of `MAX(entry_date)` filtering.

**Verdict: closed. Not anomalous. Not new. Already documented.**

## Thread 2 — Where are the Alpaca options positions?

**Resolved: closed today via 8 single-leg orders.**

### Alpaca account state (current)

```
account_number:           PA3JOBQ4UQHU
status:                   ACTIVE
options_trading_level:    3
cash:                     $94,836.14
buying_power:             $195,335.11
options_buying_power:     $97,667.55
portfolio_value:          $100,498.97
equity:                   $100,494.13
last_equity (yesterday):  $99,996.49
long_market_value:        $5,657.99   (KMI + NVDA + WMB only)
initial_margin:           $2,831.41   (50% Reg T on stocks; no options margin)
maintenance_margin:       $1,698.85
```

### Today's order trail (21 orders, all filled)

**13 MLEG opens** (15:34–18:42 UTC), strikes mixed:

| count | structure | strikes | avg credit (per share) |
|---|---|---|---|
| 8 | 719/724 | bull_put_spread | $1.66–$1.74 |
| 1 | 720/725 | bull_put_spread | $1.74 |
| 4 | 718/723 | bull_put_spread | $1.64–$1.79 |

Sum of `-filled_avg_price` (per-share credit) = **$21.97 → $2,197 cash inflow** (per-contract × 100).

**8 single-leg closes** later in the day, all `status=filled`:

| order id | created_at (UTC) | side | symbol | qty | filled @ |
|---|---|---|---|---|---|
| 2804cd68 | 15:41:45 | sell | SPY P718 (long) | 1 | $4.55 |
| 861c775a | 19:48:30 | buy  | SPY P723 (short) | 4 | $5.42 |
| de6422de | 19:48:30 | buy  | SPY P724 (short) | 8 | $5.79 |
| 7df1d3a9 | 19:48:30 | buy  | SPY P725 (short) | 1 | $6.19 |
| da636106 | 19:53:00 | sell | SPY P718 (long) | 3 | $4.14 |
| f1882374 | 19:53:02 | sell | SPY P719 (long) | 5 | $4.42 |
| 3138e7f7 | 19:53:03 | sell | SPY P720 (long) | 1 | $4.73 |
| adb9ae14 | 19:56:40 | sell | SPY P719 (long) | 3 | $4.54 |

Aggregating these closes by leg type:

| leg | qty closed | matches MLEG opens? |
|---|---|---|
| Long P718 sold | 4 | ✅ matches 4 spreads at 718/723 |
| Long P719 sold | 8 | ✅ matches 8 spreads at 719/724 |
| Long P720 sold | 1 | ✅ matches 1 spread at 720/725 |
| Short P723 bought back | 4 | ✅ matches 4 spreads at 718/723 |
| Short P724 bought back | 8 | ✅ matches 8 spreads at 719/724 |
| Short P725 bought back | 1 | ✅ matches 1 spread at 720/725 |

**Every single leg from every single MLEG open was closed today.** Alpaca correctly nets the round-trips to zero options positions, which is exactly what `/v2/positions?asset_class=us_option` returns.

### Why single-leg closes (not MLEG)

`strategies/executor.py:217-287` has a leg-count dispatch:
- 2-leg vertical → `close_vertical_spread` (MLEG, **HM-AC Option B**, commit 19c6746)
- 1-leg → `close_options_position` (single-leg)

The HM-AC Option B fix landed at commit `19c6746` today, but **PID 70953 started before that commit and is running pre-HM-AC bytecode**. Therefore today's exit_manager firings used the legacy single-leg path. Alpaca correctly accepted SELL-to-close (long puts) and BUY-to-close (short puts) — these are valid `PositionIntent` close instructions and don't require atomic MLEG. Net effect: every spread closed cleanly, just not atomically.

**Verdict: Alpaca's position state is correct. The internal book is stale. Stale-bytecode lesson applies — HM-AC Option B is on disk but unloaded.**

## Thread 3 — Cash math

**Resolved: math closes within $2.**

```
Pre-spreads cash baseline (portfolio_history at 03:42 UTC):  $94,318.18
Current Alpaca cash:                                         $94,836.14
                                                            ─────────────
Cash delta:                                                     +$517.96

Single-leg SELL fills (closing long puts), per-contract:
  3 × $4.54 × 100 = $1,362.00
  1 × $4.73 × 100 = $   473.00
  5 × $4.42 × 100 = $ 2,210.00
  3 × $4.14 × 100 = $ 1,242.00
  1 × $4.55 × 100 = $   455.00
                                                            ─────────────
Total cash IN from sells:                                    +$5,742.00

Single-leg BUY fills (closing short puts), per-contract:
  1 × $6.19 × 100 = $   619.00
  8 × $5.79 × 100 = $ 4,632.00
  4 × $5.42 × 100 = $ 2,168.00
                                                            ─────────────
Total cash OUT for buys:                                     -$7,419.00

Net single-leg close impact:                                 -$1,677.00

MLEG opening credits collected:                              +$2,197.00

                                                            ─────────────
Reconstructed cash delta:    +$2,197 − $1,677 =                 +$520.00
Actual cash delta:                                              +$517.96
                                                            ─────────────
GAP:                                                            -$2.04 ✓
```

**Verdict: coherent.** The previous prompt's "$94,318 → $93,474, delta -$844" was incorrect baseline data; current state checks out.

## Cross-thread observations

1. **All three "anomalies" had benign explanations.** Item 6's first run did not surface a *bug*; it surfaced an *internal-book staleness*: rows that should have been marked closed weren't.

2. **The internal-book staleness has a named root cause:** the close path that fires from `exit_manager`/`executor` doesn't update `options_trades.status` after Alpaca confirms close-fills. Either the row-update happens elsewhere and didn't fire, or the legacy single-leg close path lacks the row-update entirely while HM-AC Option B's `close_vertical_spread` includes it.

3. **Item 6 worked correctly on its first run.** The drift it reported is real (in/out of internal book vs Alpaca). Whether to re-classify that as a *bug-in-the-close-path* or a *correct-canary-finding* depends on whether the row update is the executor's responsibility or some downstream sync's.

4. **Stale bytecode is now load-bearing.** PID 70953 is missing 6+ commits worth of fixes (HM-V, HM-AA-broad, HM-AC-Option-A, HM-AC Option B, Day-2 lessons, Thread A, Kirk realign) and Item 6 is pending. Today's drift is partly a consequence — HM-AC Option B's MLEG close path would have made closes atomic and *might* have triggered row updates as a side effect of having the new code path. We don't know that until the service restarts and we observe a natural close cycle.

## Options for the Admiral

### Option A — Service restart, then resync internal book

- Effort: ~5 min restart + ~1 min wait for next exit-manager cycle.
- Effect: HM-AC Option B's MLEG-close path becomes active. Future closes are atomic. **Existing 13 internal-book stale rows are still stale** — they need a separate update.
- Risk: stale rows remain stale unless an explicit cleanup runs.

### Option B — Mark the 13 internal rows closed (DB UPDATE), then restart

- Effort: ~5 min targeted SQL `UPDATE options_trades SET status='closed', exit_credit_debit=..., exit_date=..., exit_reason='HM-AE: pre-restart auto-close detected' WHERE id IN (14..32 active subset)`. Compute `exit_credit_debit` from the actual close fills (per Thread 2 table).
- Effect: internal book matches Alpaca. Future opens/closes go through HM-AC Option B post-restart.
- Risk: writing 13 rows; sacred-data-rule allows updates if reasoned (this is reasoned per Thread 2 evidence). Per-row exit credit must be computed from the right Alpaca close fill — small correctness exposure if the qty assignments don't match cleanly.

### Option C — Investigate why row update didn't fire, then ship a fix

- Effort: ~30 min Scotty session — trace `executor._close_legs_individually` (legacy path) vs `close_vertical_spread` to find the row-update site.
- Effect: code-level fix that ensures internal rows get marked closed regardless of which close path fires.
- Risk: scope creep; another commit waiting for restart.

### Option D — Defer; let Item 6 keep flagging the drift

- Effort: zero.
- Effect: Item 6 NTFYs `options: 6 OCC in internal not on Alpaca` daily until the rows naturally expire (5/15/2026 expiration). Then they auto-close.
- Risk: the drift NTFYs become noise; loss of confidence in the canary; stale-row rot accumulates.

## What I (Scotty) deliberately did NOT do

- No DB UPDATEs (Option B remediation deferred to Admiral pick)
- No service restart (Option A pending Admiral approval)
- No code changes (Option C deferred)
- No row-update path investigation in `executor.py` beyond surface scan
- Did not push the pending Item 6 build commit (still on disk only; this doc is the only commit)
- Did not modify any other module

## Remaining items snapshot (post-HM-AE investigation)

### Pending Admiral conversations (you+me, no Scotty)
- **HM-AE remediation decision (A/B/C/D from this investigation)**
- **Item 6 push decision** (still uncommitted in working tree — `git diff engine/reconciliation.py` shows +321/-19)

### Pending fresh-headed Scotty (~30 min each)
- HM-AD ai_brain ~10/min loop investigation
- dalio-metals stale-CALL auto-TP fix
- Thread B alpaca-mirror sync staleness (separate from this investigation's threads)
- Thread C trades.alpaca_order_id population + canary reframe
- alpaca_portfolio_sync asset_type='stock' bug for options
- wb_advisory_team.py:121 orchestrator alpaca-mirror leak
- HM-AE remediation execution (depends on Admiral pick A/B/C)

### Pending bigger Scotty (~60 min)
- HM-I-β Item 2 (dashboard naming, awake-watching)
- Iron-condor 4-leg atomic close

### Awaiting natural verification
- HM-AC Option B MLEG close on first natural exit (becomes meaningful only post-restart)
- HM-V first NTFY fire (post-restart)
- HM-AC Option A first reject (post-restart)
- Item 6 first options reconciliation run (post-push + restart)
- Tomorrow's 13:30 reconciliation with Thread A filter applied
- Kirk first real output post-restart

### Cleanup items (low priority)
- Stale-bytecode pending restart (now 7 commits worth + Item 6 still uncommitted)
- Persist `_rate_state` to settings table
- HM-W phase 2 working-tree stragglers
- HM-T-fleet doc reference of bull_spread_v1 unhalt
- ALPACA_* vs APCA_* env-var consolidation
- `cash_source: "Manual update needed"` misleading label in Kirk
