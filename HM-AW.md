# HM-AW: Credibility Sweep

## Goal
Fix the ten visible-on-first-glance bugs across bridge.ollietrades.com (:8080)
and signal.ollietrades.com (:9000) that a fresh visitor pattern-matches as
"broken." Single PR, single pass. No new features.

Source: Claude 4.7 audit, May 13 2026. Findings cross-checked against live
sites via Claude for Chrome.

## The Ten

1. **Convergence card renders `[object Object]` ~329 times** (Signal -> Signals
   tab -> Convergence). JSON serialization failure on strategy objects. Add a
   `.toJSON()` or stringify pass before render.

2. **Score 110 on a 100-point scale** (Signal -> Top 25 Stocks, SMCI row; also
   propagates to Spreads + Premium Hunter). Add `Math.min(score, 100)` clamp
   at the scorer, not the renderer.

3. **F&G value mismatch.** Bridge shows 81 (Extreme Greed), Signal shows 50
   (Neutral) at the same timestamp. Pick canonical source, repoint both UIs.
   Do NOT paper over with a display-side fallback.

4. **GEX value mismatch.** Bridge shows 2.8B, Signal shows 0.00B. Same fix as
   #3 -- canonical source, repoint.

5. **VIX value mismatch.** One panel shows 17.9, another shows N/A. Same
   pattern. Likely #3 #4 #5 share a root cause -- one feeder is stale or one
   UI is reading from a stub. Investigate before patching.

6. **Bull/Bear Consensus: 0 bull / 0 bear -> "Bear majority"** (Signal ->
   Signals tab -> Bull/Bear Consensus card, grade E). Default-case logic bug.
   When both counts are zero, return "No signal" not "Bear majority."

7. **Gamma Environment copy contradicts itself.** Header reads "Dealers long
   gamma -- moves dampened, mean-reverting." Impact field reads "Trend
   amplification, bigger moves." One string is swapped from the negative-gamma
   branch.

8. **Signal Weights sum to 92%, not 100%.** Missing 8% somewhere in the YAML
   or scorer config. Audit the 16 components, find the gap, document the math.

9. **Backtest leaderboard shows garbage test values.** All-Time Backtest
   History row reads "BEST EVER: Return +12366.00%, Sharpe 20.88, WR 10000%,
   Trades 2329." Either purge test rows or add a sanity filter (WR <= 100,
   Sharpe <= 10). No impossible values displayed.

10. **RSI Overbought tagged as positive/bullish signal** (Signal -> Top 25
    Stocks). Currently sits alongside "Volume Surge · Uptrend" as if all three
    are bullish. RSI > 70 is cautionary. Flip the polarity or move it to a
    separate caution column.

## Bonus (only if trivial)

- Earnings tab on Signal shows 20 upcoming events; History tab logs "No
  upcoming events" same day. Stale snapshot vs live query. One-line fix ->
  take it. Otherwise note and defer to HM-AY.

## Constraints

- Single PR, single epic. No scope creep into HM-AY (crew P&L math) or HM-AX
  (dead routes).
- For #3, #4, #5 -- pick canonical source, repoint both UIs. No display-side
  fallbacks. If the feeder is the problem, fix the feeder.
- FRONTEND SHIP RULE: items #1, #2, #6, #7, #10 are JS-visible. After each,
  hard-reload the affected site and smoke-test in the browser. Static checks
  alone do not count as shipped (HM-BJ.E2 lesson).
- After backend changes: `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`

## Self-verify

1. Hard reload bridge.ollietrades.com and signal.ollietrades.com.
2. Walk each of the ten items, confirm fixed in the browser.
3. Confirm Signal weights now sum to 100%.
4. Confirm no `[object Object]` strings anywhere on Signal.
5. Confirm Top 25 Stocks max Score is <= 100.

## Closure format

One line per fix:
- `1. Convergence: serializer fix at <file:line> -- green`
- `2. Score clamp: added at <file:line> -- green`
- ... etc.

Anomalies or root-cause surprises: full paragraph.
Routine green: one line.

## Out of scope (do not touch)

- HM-AY Crew P&L vs WR math (Spock/Dax/McCoy 80%+ WR, negative P&L -- separate
  diagnostic epic)
- HM-AX Dead Route Cleanup (8 "More" items rendering Inst Intel -- separate
  one-liner epic, can run alongside this one if Scotty has bandwidth)
- HM-AN Morpheus reframe (already queued)
- HM-AM Total Portfolio Unification (already queued)
