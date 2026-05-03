# Monday April 27, 2026 — Morning Checklist

Generated Saturday April 25, 2026 EOD after 20-fix drydock.

## Pre-market (06:00–06:30 MST)

- [ ] Open dashboard, verify USS OLLIETRADES title shows (was TRADEMINDS)
- [ ] Models page: confirm 5 HALTED badges visible (chekov, dayblade-sulu,
      grok-3, navigator, ollama-llama)
- [ ] Hover halted players, confirm halt_reason text shows in tooltip
- [ ] System RAM check: should be 6+ GB free on bigmac
- [ ] Gate sanity: `python3 -c "from strategies.executor import _EXECUTION_ENABLED; print(_EXECUTION_ENABLED)"`
      MUST print False

## Market open (06:30 MST)

- [ ] First bull_spread_v1 cycle should fire 06:30–06:45 MST
- [ ] `tail -50 logs/main.log | grep bull_spread`
- [ ] Should see: "run_bull_spread_signals" entry per 15-min cadence
- [ ] Should see: "run_bull_spread_exits" entry per 5-min cadence
- [ ] Any errors → STOP and investigate before next cycle

## Position management (manual)

- [ ] **CLOSE Spock MU 500C 11.51 contracts** at market open
  - Cost basis $4,889
  - BSM fair value $12.58 vs entry $42.48 (3.4x overpay)
  - Recover residual ~$92-161
- [ ] **HOLD Spock MU stock** 0.89 sh @ $471.40 (tiny, no action)
- [ ] **HOLD Capitol Trades FMAO** 21.14 sh @ $26.52 with stop at $23.34
  - Verify stop order is set in DB
  - Congressional signal valid (Robert E. Latta, score 72)

## 9:45 MST — iv_history Day 4

- [ ] Confirm iv-backfill plist fired
- [ ] `sqlite3 data/trader.db "SELECT COUNT(DISTINCT symbol) FROM iv_history WHERE date=date('now')"`
- [ ] Should be 10/10 symbols
- [ ] If <10, investigate before Tuesday

## Investigations (no time pressure, slot in when possible)

- [ ] Captain's Portfolio popup data source
  - `/api/captain/portfolio` returns 404
  - But popup still fires KMI/WMB SELL NOW
  - Trace where popup data actually comes from
- [ ] 13-second `/api/portfolio` fetch — profile and cache
- [ ] `/api/wheel/status` 500 — known issue, low priority
- [ ] 4 dashboard 404s: sectors, news, congress, captain/portfolio

## Tuesday gate-flip readiness check (end of Monday)

- [ ] iv_history Day 4 complete (10/10)
- [ ] bull_spread_v1 fired >=4 cycles cleanly Monday
- [ ] No new errors in logs/main.log
- [ ] Spock MU 500C closed (real residual recovered)
- [ ] FMAO stop order verified
- [ ] Ghost scorecard reviewed at /api/signals/scorecard
- [ ] Captain's Portfolio popup investigated (data source confirmed)

If all checked, Tuesday gate-flip stays on schedule.
If any fail, push to Wednesday/Thursday — better to flip a validated
system than rush a broken date (memory #5 says push work, not dates).

## Tuesday morning (06:00 MST) — gate-flip per agent

Per Saturday's gate-flip plan:
- FLIP: Ollie, Counselor Troi (options), Cmdr. Trip Tucker, Spock stocks,
        Data, Chekov scanner (if scheduler issue resolved), Dax, Worf, Capitol
- WATCH: bull_spread_v1 (until 30-cycle gate clears, expected Wed-Thu)
- HOLD: Spock options (need 10+ closed options trades)
- HOLD: Navigator (signal->trade linkage), Chekov scanner (if zero output continues)
- EXCLUDE: McCoy (per quality audit), Dax options (-$521 single trade)

## Capitol Trades baseline reset (added Saturday 2026-04-25 EOD)

Saturday's bug-impact audit found the autopilot $0 P&L bug was contained
to ONE agent: Capitol Trades. Bug window: April 13 - April 24, 2026.
31 of 34 sell records (91%) had exit_price = entry_price.

The mechanism: Capitol Trades buys off-watchlist Congress stocks
(FSV, RPM, AJG, BLK, MA, IBP, LGIH, TSM, URI). Autopilot's prices dict
was built only from WATCH_STOCKS, so off-watchlist symbols returned
current_price=0, triggering the avg_price fallback and recording the
exit at entry price.

The Saturday Drydock fix (autopilot.py:126 Layer 1 + main.py:1095 Layer 2)
addresses this directly. Layer 2 widens the prices dict to include all
open positions, not just watchlist.

### What this means for Capitol Trades

- Pre-April-25 trade history: data corrupted, actual P&L unknowable
  without OHLCV reconstruction per symbol per exit date
- $36.27 recorded realized P&L is meaningless (3 of 34 sells had real data)
- Leaderboard position pre-April-25 should be treated as "pre-correction"
- Real baseline starts Monday April 27, 2026 with the fix live

### What this does NOT mean

- All other agents (Ollie, Spock, Troi, Trip Tucker, Data, Chekov, Worf,
  Dax, Navigator) have CLEAN trade history. The bug never touched them
  because they trade watchlist symbols where price lookup always worked.
- Tuesday gate-flip GO/NO-GO decisions stand for all other agents
- Fleet leaderboard is accurate for everyone except Capitol Trades

### Monday actions for Capitol Trades

- [ ] Verify Layer 2 fix is active: if Capitol Trades autopilot-exits any
      off-watchlist symbol Monday, confirm exit price reflects market
      not entry
- [ ] Optional: tag the 31 corrupted trades in DB for historical clarity:
      sqlite3 data/trader.db "
        SELECT COUNT(*) FROM trades
        WHERE player_id = 'capitol-trades'
          AND exit_price IS NOT NULL
          AND ABS(exit_price - price) < 0.001
          AND executed_at >= '2026-04-13'
          AND executed_at <= '2026-04-25';
      "
      (Verify count = 31, then run UPDATE with execution_type = 'autopilot_bug_zero_pnl')
- [ ] Document Capitol Trades on dashboard as "BASELINE RESET 2026-04-25"
      so the leaderboard does not penalize it for the corrupted window
- [ ] Re-evaluate Capitol Trades gate-flip readiness after 30+ post-fix
      closed trades accumulate (estimated mid-May 2026)

### What still cannot be determined

The actual market price at the moment of each autopilot exit is not stored
anywhere -- only the incorrectly recorded entry price. To reconstruct true
P&L for the 31 affected trades, OHLCV cross-reference would be needed:
- MA: exits Apr 17 (bought Apr 15 at $502.89, 2-day hold)
- AJG, BLK, IBP: exits Apr 17 (same batch as MA)
- LGIH: exits Apr 20
- TSM, URI: exits Apr 24 (next-day exits from Apr 23 entries)

These were short-hold positions (1-4 days) on low-volatility value/dividend
names. The actual suppressed P&L per position was probably $10-$80 per
exit, total range +/- $200-$500. Immaterial to fleet rankings, but
important for Capitol Trades' own scorecard if reconstruction is desired.

### Out of scope for Monday

- OHLCV reconstruction of the 31 affected trades -- defer to S7 sprint or
  drop entirely, depending on whether Capitol Trades stays in active fleet
- S1 era synthetic price artifacts (50 trades, 6 retired/halted agents,
  March 13-27 -- different root cause, documentation artifact only,
  no fix needed)

## Fleet Reality Check -- Two Data Quality Variances (added Saturday 2026-04-25 EOD)

Saturday evening's comprehensive lifetime fleet audit surfaced two
data quality issues running in opposite directions. One was already
known. One was new.

### Variance A -- S1 Synthetic Price Inflation (KNOWN, deliberately left alone)

**Status:** Pre-existing variance, previously identified and discussed.
The decision was made not to correct it. Documenting here for the record
so future-self / future-Claude has the context.

**What it is:** S1-era positions had test/synthetic entry prices set
at unrealistic values (TSLA at $21, AMD at $12, AVGO at $18, AMZN at $8).
When those positions closed at real S3 market prices, the recorded
realized P&L was massively inflated. Some related to options agents
using stock values rather than option premium values, others were
straight test data carryover.

**Scale:** 27 sell records, $283,484 in fabricated P&L

**Affected agents:**
- gemini-2.5-pro: reported +$225,453 -> real ~-$11,030
- claude-sonnet: reported +$43,372 -> real ~-$3,630
- Smaller artifacts on retired agents

**Why not corrected:** A previous decision (specifics not retained in
current memory) determined that correction was not preferred. Possible
reasons: (a) historical record preservation, (b) agents are retired so
correction provides no operational value, (c) reconstruction would
require OHLCV historical pulls that weren't worth the effort,
(d) some other reason.

**Action Monday:** Decision tree:

  Option 1: Correct it -- add a data_quality flag column, mark these
  27 sells as 'S1_SYNTHETIC_PRICE'. Filter from leaderboard display.
  Estimated effort: 30-60 min. Restart not required.

  Option 2: Document it permanently -- add a banner/footnote on the
  Leaderboard component noting the S1 artifact. Don't touch the data,
  just contextualize what users see. Estimated effort: 10 min.

  Option 3: Leave as-is -- accept the variance, never anchor decisions
  on the inflated numbers. Use post-S4 (Mar 20+) data only for evaluation.

**Recommendation:** Option 2 (display footnote) at minimum. Option 1
preferred if Admiral wants to ship a clean S7 with accurate baselines.

### Variance B -- Autopilot $0 P&L (NEW, fixed Saturday Drydock)

**Status:** Fixed at autopilot.py:126 + main.py:1095 in this morning's
drydock. Forward data is clean.

**What it is:** Off-watchlist symbols (Capitol Trades' Congressional stocks
like FSV, RPM, AJG, BLK, MA, IBP, LGIH, TSM, URI) were exited by autopilot
at their buy price because the prices dict only covered WATCH_STOCKS.
Recorded realized P&L = $0.

**Scale:** Capitol Trades only, 31 of 34 sell records (91%) zeroed.
$5,224 notional capital affected. Window: April 13 - April 24, 2026.

**Action Monday:** See "Capitol Trades baseline reset" section above.

### True Fleet Performance -- The Anchor Number

**Stripped of S1 inflation, the fleet's realized P&L since S4 (Mar 20+):**
**+$2,694 across 26 trading days.**

This is the honest baseline. Small profit. Not the $232K headline.

**Per-agent clean P&L (S4 onward only):**

| Agent | Real P&L | Status |
|-------|----------|--------|
| Counselor Troi (options-sosnoff) | +$2,060 | Clean, 4/4 wins |
| Lt. Cmdr. Worf (gemini-2.5-flash) | +$181 | Clean, 21/21 |
| Cmdr. Trip Tucker (energy-arnold) | +$168 | Clean, 92.9% WR |
| Ollie | +$44 | Clean |
| Capitol Trades | +$36 (recorded, true unknown) | Bug-affected |
| dalio-metals | -$164 | Clean loser |
| dayblade-0dte | -$3,781 | Clean, 5.3% WR (halted) |

### Implications for Tuesday Gate-Flip

The Tuesday gate-flip decisions are NOT affected by these variances:
- Ollie GO: confirmed clean
- Counselor Troi GO: confirmed clean
- Trip Tucker GO: confirmed clean
- Spock options NO-GO: stands
- McCoy EXCLUDE: stands
- Bull Spread V1: now wired, watching

The variances affect HISTORICAL display, not FORWARD operational decisions.

### Implications for Season 7 Thesis

S7 is "the season the agents actually learn." This audit suggests a
corollary: S7 starts with clean, honest baselines. Whether that means
correcting the S1 artifact (Option 1) or just documenting it (Option 2)
is the Admiral's call. But starting S7 with the leaderboard showing
+$225K from gemini-2.5-pro would undermine the "we made the agents
actually learn" narrative -- that number is artifact, not learning.

### Reading order for Monday morning

1. Pre-market checklist (top of this file)
2. Capitol Trades baseline reset section
3. THIS section (fleet reality check)
4. Make the Option 1 / Option 2 / Option 3 call on S1 artifact
5. Proceed with day's trading plan
