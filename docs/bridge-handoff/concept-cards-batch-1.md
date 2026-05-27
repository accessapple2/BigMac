# OllieTrades Bridge — Concept Cards (Batch 1)

Reference content for the three-depth Explain layer. Each card matches the five-section shape from section 4 of the design handoff:

1. **One-liner** — what it is in a sentence
2. **Computed** — mechanics, briefly
3. **Numbers** — thresholds and zones
4. **How the crew uses it** — named to characters who rely on it
5. **Gotchas** — when it lies

Cards in this batch (10): MACD, Bear Call Spread, Bull Put Spread, IV Rank, Time Stop, Gamma Flip, Convergence, Stop Loss, Take Profit, ATR.

These pair 1:1 with the `concept_id` values referenced in trade-signal payloads (section 6 of the handoff) and the glyph registry in `ConceptGlyphs.tsx`. The dashboard Concept Drawer should render one card per concept, opening beside the trade card so the concrete and abstract sit side by side.

The emoji in each heading is a placeholder — the production component replaces it with the matching `<ConceptGlyph />` SVG.

---

## 📊 MACD — Moving Average Convergence Divergence
*A trend-plus-momentum indicator showing when short-term price action is accelerating away from, or converging back into, the longer-term trend.*

**One-liner:** Plots the gap between two moving averages of price and asks: is that gap widening (trend strengthening) or narrowing (trend weakening)?

**Computed:** Default settings are 12, 26, 9. Take the 12-period EMA, subtract the 26-period EMA — that's the MACD line. Take the 9-period EMA of the MACD line — that's the signal line. Plot the difference between them as a histogram. Three things on the chart: the MACD line, its smoothed signal line, and the histogram bars representing the gap between them.

**Numbers:**
- MACD line crosses **above** signal → bullish momentum building
- MACD line crosses **below** signal → bearish momentum building
- MACD line **above zero** = price above its 26-period average (uptrend)
- MACD line **below zero** = price below its 26-period average (downtrend)
- Histogram bars **growing** in either direction = momentum accelerating
- Histogram bars **shrinking** = momentum fading (early warning before a line cross)
- Divergence (price makes new high, MACD doesn't) = reversal risk

**How the crew uses MACD:**
- **Worf (Bear Spreads):** waits for MACD cross-down combined with RSI > 70 before entering bear call spreads. Cross alone isn't enough.
- **McCoy (Crisis Doctor):** bullish cross-up with RSI < 30 = oversold-rescue confirmation.
- **Spock (Pure Quant):** ~10% weight in composite trend score; never standalone.
- **Chekov (Convergence):** MACD cross counts as one vote toward the 4-of-N threshold.
- **Riker (Daily Briefing):** reports MACD state for SPY/QQQ/IWM in the morning briefing.

**Gotchas:** MACD lags — it's an average of averages, so the signal often comes *after* the easy move. In choppy markets it crosses back and forth with no follow-through; wait for histogram confirmation. The default 12/26/9 is calibrated to daily bars — using the same settings on 5-min charts means different things. Divergence signals are unreliable in strong trends; the indicator can stay divergent for weeks while price grinds higher.

---

## 📉 Bear Call Spread
*A defined-risk credit structure that profits if a stock stays below a chosen ceiling until expiration.*

**One-liner:** Sell one call (closer to the money), buy one call at a higher strike for protection. Collect a credit. Win if the stock closes below the lower strike at expiry.

**Computed:** Sell 1 call at strike A. Buy 1 call at strike B, where B > A. Net premium received = price of A minus price of B = the credit. Max profit = the credit, kept if stock ≤ A at expiry. Max loss = (B − A) × 100 minus credit, suffered if stock ≥ B at expiry. Breakeven = A + (credit / 100).

**Numbers:**
- Width (B − A) defines the dollar risk: $3-wide spread = $300 maximum risk per contract before credit
- DTE typically 7–21 days for fast theta capture
- Probability of profit ≈ 1 minus delta of short call (selling a 30-delta call ≈ 70% POP)
- Target exit: 60–80% of max credit captured
- Hard stop: underlying reclaims short strike A on volume ≥ 1.5× average
- Time stop: close at 7 DTE if not yet at 50% capture

**How the crew uses it:**
- **Worf:** primary structure. Fires when RSI > 70, price at multi-touch resistance, IV rank 30–60.
- **Spock:** allowed when composite bearish score ≤ −20 and IVR > 30.
- **Chekov:** offers it as the suggested structure when 4+ bearish strategies converge.

**Gotchas:** Earnings inside the DTE blows up the model — IV crush can hand you a winner or a gap above strike B can max-loss you in one print. Risk-reward is structurally asymmetric (a typical 1:3 setup means you risk $300 to make $100), so win rate has to be high to net positive. Theta decays in your favor only if price doesn't trend through the short strike. Strong rallies pin you against the long strike with widening losses faster than the calendar can rescue.

---

## 📈 Bull Put Spread
*A defined-risk credit structure that profits if a stock stays above a chosen floor until expiration.*

**One-liner:** Sell one put (closer to the money), buy one put at a lower strike for protection. Collect a credit. Win if the stock closes above the higher strike at expiry.

**Computed:** Sell 1 put at strike A. Buy 1 put at strike B, where B < A. Net premium received = price of A minus price of B = the credit. Max profit = the credit, kept if stock ≥ A at expiry. Max loss = (A − B) × 100 minus credit, suffered if stock ≤ B at expiry. Breakeven = A − (credit / 100).

**Numbers:**
- Width (A − B) defines the dollar risk
- DTE typically 7–21 days for theta capture
- POP ≈ 1 minus delta of short put (selling a 30-delta put ≈ 70% POP)
- Target: 60–80% of max credit captured
- Hard stop: underlying breaks short strike A on rising volume
- Skew note: put premium is structurally inflated, which helps the credit math

**How the crew uses it:**
- **McCoy:** primary structure for oversold-rescue setups on quality names with RSI < 30 and capitulation volume.
- **Spock:** allowed when composite bullish score ≥ +20 and IVR > 30.
- **Dax (Dividend Value):** uses on dividend-payers near multi-touch support when IVR > 40.
- **Chekov:** offers when 4+ bullish strategies converge.

**Gotchas:** Earnings inside the DTE is the same time bomb as bear call spreads, just inverted. Selling premium near "obvious" support feels safe but support breaks; capitulation looks like opportunity right up until it's a hole through the floor. Skew that helps the credit math also signals what other market participants fear — read that signal, don't just collect the premium. Don't sell into a falling-knife setup; "the trend is your friend" cuts both ways and a falling stock can keep falling for weeks.

---

## 🌡️ IV Rank
*A 0-to-100 number that ranks today's implied volatility against the past year's range — high means options are expensive, low means they're cheap.*

**One-liner:** Compares the current IV reading to its 52-week high and low to tell you whether option premium is rich or cheap relative to recent history.

**Computed:** `IVR = 100 × (IV_today − IV_52w_low) / (IV_52w_high − IV_52w_low)`. A reading of 70 means current IV sits 70% of the way between the year's lowest IV print and its highest. Unlike IV percentile (which counts the share of days IV was lower), IVR is anchored to the year's extremes only — one big spike can stretch the scale for the whole following year.

**Numbers:**
- IVR < 20 — cheap options. Buy premium (long calls, debit spreads, long vol).
- IVR 20–40 — neutral. Most structures work.
- IVR 40–60 — mid. Sweet spot for defined-risk credit spreads.
- IVR > 60 — rich. Strongly prefer selling premium.
- IVR > 90 — extreme. Usually around earnings, FOMC, news. Crush is coming.

**How the crew uses IVR:**
- **Worf:** prefers 30–60 band for bear call spreads — rich enough to sell, not extreme enough to whip.
- **McCoy:** requires IVR > 40 before opening a cash-secured put; below that, capital tie-up doesn't justify the premium.
- **Spock:** structure-selection input. Never enters short-premium trades when IVR < 25.
- **Troi (Risk):** flags caution when IVR > 80 sector-wide (crowded fear or crowded greed).

**Gotchas:** IVR can stay elevated for weeks during macro stress, then crater the second uncertainty resolves — don't bet on fast mean reversion. Single-name IVR near binary events (earnings) reads "rich" but is rational, not an opportunity. IVR tells you premium richness, never direction. Different data providers compute the 52-week window slightly differently (rolling vs. calendar); use a consistent source.

---

## ⏰ Time Stop
*A rule that says "if this trade hasn't worked by day X, close it" — exit by the clock, not by price.*

**One-liner:** A pre-committed expiration-relative or bar-count-relative exit, separate from price stops and profit targets, that fires on a calendar condition.

**Computed:** Set at trade entry. For defined-risk credit spreads, the canonical rule is: close at DTE = 7 if the trade is at < 50% of max profit and the underlying hasn't moved meaningfully toward the profit zone. For directional swing trades: close after N bars of no progress (typical N = 5–10). For 0DTE: close 30 minutes before market close regardless of P&L, or at 50% capture by 1pm ET.

**Numbers:**
- Bear/bull spreads: 7 DTE if < 50% credit captured
- 0DTE: 50% by 1pm ET, hard close at 3:30pm ET
- Swing trades: 5–10 bar no-progress = exit
- Longer-dated debit spreads: time stop at 30 DTE remaining (theta accelerates fast inside that window)

**How the crew uses it:**
- **Worf:** mandatory 7 DTE rule on every bear call spread.
- **McCoy:** 14 DTE time stop on cash-secured puts — if the bounce hasn't started, the diagnosis was wrong.
- **Spock:** systematic 7 DTE close on all defined-risk credit spreads regardless of P&L.
- **Scotty (Engineering):** enforces time stop at the executor level — fires the close order automatically when the threshold hits.

**Gotchas:** Closing a small-loss trade feels worse than holding for the comeback — but holding is exactly how 7 DTE turns into 1 DTE and gamma rips you. Time stops matter most when the trade is structurally working but slow; they matter least when the trade is structurally broken (your hard price stop should already have fired). Don't confuse "time stop" with "give up" — it's risk management, not pessimism. Calendar exits also protect you from event-driven gaps the next session.

---

## 🔄 Gamma Flip
*The price level where aggregate market-maker gamma exposure switches sign — above it, dealers dampen volatility; below it, they amplify it.*

**One-liner:** A regime line. On one side of it, dealer hedging pushes against price moves and creates mean reversion. On the other side, dealer hedging pushes with price moves and creates trend extension.

**Computed:** Aggregate the options open interest weighted by each contract's gamma, summed across all strikes for the underlying, mapped to a single underlying price. Above that price, dealers are net long gamma — they must sell into rallies and buy into dips to stay delta-neutral, which dampens realized volatility. Below it, dealers are net short gamma — they must buy into rallies and sell into dips, which amplifies realized volatility. The exact computation requires options chain data and dealer-positioning assumptions; published numbers come from providers like SqueezeMetrics and SpotGamma.

**Numbers:**
- Price > gamma flip → stable regime, mean-reverting intraday, low realized vol
- Price < gamma flip → unstable regime, trends extend, realized vol expands
- Distance matters: 1% above flip = mildly stabilizing, 5% above = strongly stabilizing
- The flip level itself moves as new open interest builds
- Major OPEX expirations frequently reset the flip dramatically

**How the crew uses gamma flip:**
- **Worf:** prefers entries when underlying is above the flip — better drift-against-trade dynamics for credit spreads.
- **Troi:** flags a regime change when price crosses the flip — sentiment context shifts.
- **Spock:** factors gamma state as a regime variable in his composite score; `gamma_state="UNSTABLE"` cuts the size factor.
- **Riker:** includes the SPY/QQQ flip level in the daily briefing.

**Gotchas:** Gamma flip is a *model output*, not an observed price level — different providers give different numbers. Around major options expirations, the level can move ±2% in a single session and invalidate stale flip readings. The flip is most predictive in index ETFs (SPY/QQQ/IWM) where dealer flows dominate; it's much less reliable on small-caps where retail flow dominates pricing. Don't trade gamma flip mechanically — read it as context for other setups.

---

## 🎯 Convergence
*A rule that says "no single signal — wait until 4 or more independent strategies all agree on direction before sizing in."*

**One-liner:** A multi-signal gatekeeper. Each strategy votes BULLISH, BEARISH, or NEUTRAL; the trade only fires when the votes line up.

**Computed:** Maintain a set of independent strategy evaluations — RSI extreme, MACD cross, relative volume confirmation, breadth, options skew, gamma state, sector trend, et al. For each scan cycle, ask each strategy: which direction? Convergence threshold (4 of 7, 5 of 9, etc.) is a calibration choice; OllieTrades default is 4 of N. Each vote can be weighted, and disagreement counts (4 bullish + 3 bearish nets out very differently from 4 bullish + 0 bearish).

**Numbers:**
- 1 vote → noise
- 2 votes → weak signal (chip toward but don't enter)
- 3 votes → moderate (size at 50% of standard)
- 4+ votes → convergence (full standard allocation)
- 6+ votes → strong convergence (size factor 1.25–1.5×)
- Net signal = bullish_votes − bearish_votes. Threshold applies to the net.

**How the crew uses it:**
- **Chekov:** primary methodology. Never fires without convergence — acts as the gatekeeper layer for the whole fleet.
- **Spock:** uses convergence count as one input to his composite score, not the only filter.
- **Kirk (Captain):** convergence is required to upgrade a name from WATCH to BUY in the Captain's plan.
- **Riker:** flags convergence events in the daily briefing — "Three names crossed 4-of-N overnight."

**Gotchas:** Strategies that share underlying inputs aren't independent. RSI, MACD, and stochastic all measure momentum from the same price series — 4 votes from 4 momentum indicators is one vote dressed up as four. Real convergence requires *different families*: momentum + volume + breadth + options + macro. Convergence also lags — by the time 4 signals line up, the easy part of the move has often already happened, and you may be buying the second half at the top of the first leg.

---

## 🛑 Stop Loss
*A pre-committed price level where the trade is closed at a loss, no questions asked.*

**One-liner:** The cap on how much any single trade can hurt the account. Set at entry, never moved against you mid-trade.

**Computed:** Set at trade entry. For long stock: stop = entry − N × ATR, or stop = entry × (1 − %risk). For defined-risk options spreads: hard stop usually triggered by underlying reclaiming the short strike on rising volume, not by a P&L percentage. Position size is *derived from* the stop, not the other way around: (max risk dollars) / (entry − stop) = share count or contract count.

**Numbers:**
- Per-trade risk: 0.5–1.0% of account standard; 0.25% for low-conviction setups
- ATR multipliers: 1.5× for swing trades, 2.5× for position trades
- Credit spreads: hard stop typically at 1.5–2× credit collected
- 0DTE: stop at 30–50% of debit paid (time decay is brutal)

**How the crew uses it:**
- **Worf:** hard stop = underlying reclaims short strike on volume ≥ 1.5× average. Not a P&L stop.
- **McCoy:** stop at 1.5× credit collected on the cash-secured put.
- **Spock:** ATR-based stops; multiplier varies by holding period.
- **Scotty:** executor enforces every stop programmatically. No mid-session overrides allowed.

**Gotchas:** "I'll exit if it breaks support" without a number written down becomes "I'll wait one more bar" forever. Stops that are too tight get hit on noise; stops that are too loose ruin position sizing. *Moving a stop further away mid-trade is the cardinal sin* — it converts a managed loss into an unmanaged disaster, and it's the single most common way disciplined traders blow up. Stops don't guarantee execution at the level either — gap risk and slippage are real, especially overnight and around news. Plan for slippage in the worst case, not the typical case.

---

## 🏁 Take Profit
*A pre-committed price or P&L level where the trade is closed at a gain, ideally in tiered partials.*

**One-liner:** The opposite of a stop — a written exit on the winning side, designed to bank progress before the trade reverses.

**Computed:** Set at entry. For directional trades: 1:1 R, 2:1 R, 3:1 R tiers, with stop moving to breakeven after the first target. For credit spreads: 50% or 75% of max credit captured. For 0DTE: 50% by midday or earlier. Risk-multiple R = (target − entry) / (entry − stop). Tiered scaling: 1/3 off at 1R, 1/3 at 2R, hold 1/3 with stop trailed at BE — classic and durable.

**Numbers:**
- Three-tier: 1R / 2R / runner with stop at breakeven on the runner
- Credit spreads: 50% credit-captured = "manageable winner"; 75% = "milk only if confident"
- 0DTE: 50% by 1pm ET or get out
- Closing at 100% of max profit only happens at expiration — and you eat assignment / pin risk getting there

**How the crew uses it:**
- **Worf:** closes bear call spreads at 75% credit captured (debit-to-close = 25% of credit collected).
- **McCoy:** closes CSPs at 50% credit captured, redeploys capital.
- **Spock:** tiered closing — 1/3 at 1R, 1/3 at 2R, trailing stop on the runner.
- **Dax:** dividend-value trades use price-based targets, not R-multiples (target = previous swing high).

**Gotchas:** "Let winners run" sounds wise and breaks portfolios. Most options trades don't run; they revert. Holding for the last 25% of credit on a spread usually returns it to the market when gamma turns on you near expiry. Conversely, scaling out too early on directional debit trades caps the asymmetric payoff that justified the trade in the first place. The right answer depends on the structure: defined-risk credit spreads → exit early; directional debit positions → scale out, let the runner run with disciplined trailing.

---

## 📏 ATR — Average True Range
*A volatility measure that tells you the typical daily price range — used to size stops and gauge "normal" movement.*

**One-liner:** Reports the average distance a stock typically travels in a session, in dollar terms. SPY ATR(14) of $4 means the typical daily envelope is about $4 wide.

**Computed:** True Range = the largest of (today's high − today's low), |today's high − yesterday's close|, |today's low − yesterday's close|. ATR(N) = N-period moving average (typically 14) of True Range. Reported in the underlying's units. Some platforms use exponential smoothing instead of simple averaging; behavior is similar.

**Numbers:**
- ATR rising = volatility expanding; ATR falling = volatility contracting
- Stop distance: 1.5× ATR for swing trades, 2.5× ATR for position trades
- Position sizing: (max-loss-dollars) / (1.5 × ATR) = share count for an ATR-based stop
- ATR ÷ price = normalized volatility — use it to compare across tickers
- TSLA ATR/price ≈ 4% vs MSFT ≈ 1.2% = different risk regimes despite identical dollar position size

**How the crew uses ATR:**
- **Worf:** requires underlying within 1 ATR of resistance before entering a bear call spread — too far and the rejection isn't structurally meaningful.
- **Spock:** uses ATR for position sizing on every trade. Never sizes a position without an ATR reading on file.
- **Scotty:** executor uses ATR for slippage estimates on order entry and stop placement.
- **Kirk:** ATR informs which support and resistance levels make it onto the Captain's plan — levels closer than 0.5 ATR get merged.

**Gotchas:** ATR is backward-looking — it tells you what volatility *was*, not what it will be. Around earnings or scheduled news catalysts, the expected move (extracted from options pricing) is more useful than ATR. ATR in raw dollar terms doesn't transfer across tickers — always normalize by price for comparison. Very low ATR readings often precede regime changes; compression resolves with expansion more often than it resolves with continued calm.

---

_End of batch 1._
