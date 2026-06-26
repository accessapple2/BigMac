# OllieTrades Concept Cards — Batch 2

Twenty-six concept cards in the five-section shape established in batch 1, covering the remaining items from handoff section 4. Indicators → Options Greeks → Options strategies → Regime → Risk → Metrics.

---

## 🌊 Relative Volume

*A simple ratio that asks: is today's tape louder than usual for this stock?*

**One-liner:** Compares current volume to the same time-of-day average over the recent past — if today's print is heavier, somebody's making decisions and the move has a tailwind.

**Computed:** `RelVol = current_volume / avg_volume_at_same_time_of_day(lookback)`. Typical lookback is 20 trading days. Some scanners use full-session relative volume (cumulative since open ÷ historical average at this minute); others use a rolling 5-minute window. The 5-min version is faster to react; the cumulative version is harder to fake.

**Numbers:**
- < 1.0 = quieter than usual (often a stalking session, not a tradable one)
- 1.0–1.5 = normal volume, nothing special
- 1.5–3.0 = elevated — something is happening, watch the structure
- > 3.0 = heavy — institutional flow or news; structure breaks have follow-through
- > 10× on a small-cap = momentum trade window open; expect volatility both ways

**How the crew uses Relative Volume:**
- **Holly (Scanner Alpha):** primary filter — anything < 2× gets dropped before structure analysis.
- **Chekov (Convergence):** counts a > 1.5× print as one vote toward the convergence threshold.
- **Navigator:** requires > 2× on the trigger bar to take a breakout entry; protects against fake-outs.
- **Sulu (DayBlade):** intraday volume bursts > 5× often precede directional moves into the next 0DTE strike level.

**Gotchas:** Pre-market and post-market volume distorts ratios — a 10× number at 9:31 ET may be a $50K block that means nothing. Low-float small-caps can show absurd ratios on tiny absolute volume. Always pair with absolute volume floor (e.g., > 100K shares) to avoid trading noise.

---

## 🎀 EMA Ribbon

*A stack of exponential moving averages that visualizes whether the trend is organized or confused.*

**One-liner:** Plotting several EMAs (typically 8, 13, 21, 34, 55, 89) on the same chart creates a ribbon — when it fans out cleanly in one direction, trend is healthy; when it tangles, the market is consolidating or about to reverse.

**Computed:** Each EMA is a price average that weights recent bars more heavily than older ones. The ribbon is just multiple EMAs of increasing lookback length stacked together. Default Fibonacci series (8/13/21/34/55/89) gives proportional spacing.

**Numbers:**
- All EMAs stacked, shortest on top, fanning up = strong uptrend
- All stacked, shortest on bottom, fanning down = strong downtrend
- EMAs crossing each other / tangling = trend weakness or transition
- Price above the entire ribbon + ribbon expanding = continuation setup
- Price slicing back through multiple ribbon EMAs = breakdown of the trend structure

**How the crew uses EMA Ribbon:**
- **Spock (Pure Quant):** ribbon order is a ~12% input in the composite trend score.
- **Chekov (Convergence):** "ribbon aligned" counts as one of the convergence votes.
- **Sulu (DayBlade):** intraday 5/13/34 mini-ribbon used as 0DTE bias filter.
- **Navigator:** ribbon expansion required before taking trend-resumption entries.

**Gotchas:** Lagging by design — by the time the ribbon is fully fanned, the easy move is often done. The ribbon "tangles" right at the most interesting moments (reversals, transitions), which is when you most want clarity and least get it. Different ribbon settings give different stories on the same chart; pick one set and stop flipping.

---

## 🎈 Bollinger Bands

*A volatility envelope that draws statistical "normal range" lines around a moving average.*

**One-liner:** Two bands sit a fixed number of standard deviations above and below a moving average — about 95% of recent price action lives inside them, so prints outside the bands are statistically unusual.

**Computed:** Middle line = 20-period SMA. Upper band = SMA + (2 × 20-period standard deviation). Lower band = SMA − (2 × 20-period standard deviation). The bands expand when volatility rises and contract when it falls — a "Bollinger squeeze" is when band width hits a multi-week low, often preceding a volatility expansion.

**Numbers:**
- Price tagging upper band = near top of recent range (not automatically "sell")
- Price tagging lower band = near bottom of recent range (not automatically "buy")
- Squeeze (bandwidth < 20-day percentile 20) = volatility expansion likely soon
- Price closing outside the band = strong directional move, often continues 1–3 bars
- Walking the band (consecutive closes outside) = trend, do not fade

**How the crew uses Bollinger Bands:**
- **McCoy (Crisis Doctor):** lower-band tag + RSI < 30 = oversold-rescue CSP candidate.
- **Worf (Bear Spreads):** upper-band tag + RSI > 70 on a known resistance = bear call spread entry.
- **Spock (Pure Quant):** uses band-width compression as a volatility-regime indicator.
- **Dax (Dividend Value):** lower-band tag on a quality dividend name = patient accumulation trigger.

**Gotchas:** Bollinger Bands don't predict direction — they describe range. Tagging the upper band in a strong uptrend means "the move is real" not "fade it." The 2-sigma default catches ~95% of moves only if the underlying is normally distributed, which markets aren't. Fat tails happen.

---

## 🚀 Gap & Go

*A momentum setup that asks: does a stock that gapped open hard hold its level, or fade?*

**One-liner:** A stock opens significantly above (or below) its prior close on news, earnings, or sympathy flow — Gap & Go is the technique of buying the open if the gap holds, trading the continuation of the overnight conviction.

**Computed:** Gap = `(open − prior_close) / prior_close × 100`. Useful gap threshold varies by liquidity: > 4% for large-caps, > 8% for mid-caps, > 15% for small-caps. The "go" condition is usually a hold of the opening range high (or low for short-side gaps) within the first 5–15 minutes.

**Numbers:**
- Gap < 4% on a large-cap = not really a gap, normal noise
- Gap 4–10% = tradeable if catalyst + volume confirm
- Gap > 10% on heavy volume = momentum window; expect wide swings
- Open above pre-market high + holds first 5-min low = "go" trigger
- Filling the gap intraday = failed setup; expect mean reversion to continue

**How the crew uses Gap & Go:**
- **Sulu (DayBlade):** prime 0DTE setup — gap up + opening range break = call buy into next strike.
- **Holly (Scanner Alpha):** flags pre-market gappers > 5% with > 100K pre-market volume.
- **Chekov (Convergence):** treats opening-range break as one convergence vote when intraday-armed.
- **Navigator:** waits for failed gap fill before taking a gap continuation; reduces fakeouts.

**Gotchas:** Most gaps fill — the trade is identifying which ones don't. Earnings gaps are uniquely treacherous; the market often "decides" 30+ minutes after open. Gap & Go on light pre-market volume (< 50K shares) is shooting at shadows. Never enter at the open print; let the first range establish.

---

## ↩️ Pullback to SMA

*A continuation setup that buys a stock retracing into a moving average within an established trend.*

**One-liner:** In an uptrend, price oscillates above and below shorter-term moving averages but tends to find footing at deeper ones — a controlled pullback to the 20-, 50-, or 200-day SMA is often where the trend resumes.

**Computed:** Need three things: (1) an established trend (e.g., price > 50 SMA, 50 SMA > 200 SMA), (2) a retracement bringing price into contact with a key SMA, (3) a reaction off that SMA (rejection wick, volume spike, bullish engulfing).

**Numbers:**
- Pullback to 20 SMA in a strong trend = first-touch dip, often shallow
- Pullback to 50 SMA = standard "buy the dip" zone for swing traders
- Pullback to 200 SMA = deeper retracement, often the line institutions defend
- Break of 200 SMA on volume = trend potentially over; stop and reassess
- Reclaim of broken SMA within 1–3 bars = false breakdown, strongest re-entry

**How the crew uses Pullback to SMA:**
- **Spock (Pure Quant):** detects SMA proximity (< 1% from SMA) as a setup precondition.
- **Chekov (Convergence):** "near 20/50 SMA in uptrend" counts as one convergence vote.
- **Dax (Dividend Value):** prefers 50 SMA pullbacks on dividend aristocrats for cost-basis improvement.
- **Navigator:** combines SMA pullback + volume dry-up + EMA ribbon for trend-resumption setups.

**Gotchas:** Pullbacks fail in transitioning markets — what looked like a "buy the 50" becomes a slow grind below it. Confirm the trend before assuming the dip is bought; don't catch falling knives by calling them pullbacks. The 200 SMA is heavily watched, which means it can become a self-fulfilling pivot OR an obvious trap; both happen.

---

## 🏜️ Volume Dry-Up

*The quiet moment before a move — when sellers exhaust themselves and the tape goes silent.*

**One-liner:** A stock pulls back on visibly declining volume, suggesting sellers have stopped pressing — the next directional move (usually up, in an uptrend) often comes on a volume re-expansion.

**Computed:** Look at the last 3–5 pullback bars: each successive bar has lower volume than the prior. Quantitatively, volume on the most recent pullback bar < 50% of the 20-bar average. Bonus: the candle bodies are also shrinking (narrowing range).

**Numbers:**
- 3 consecutive declining-volume pullback bars = mild dry-up
- 5+ consecutive declining-volume bars + narrowing range = strong dry-up
- Volume on the lowest bar < 50% of 20-bar average = signal-quality dry-up
- Re-expansion to > 1.5× average on a green bar = the trigger; entry here
- Dry-up that breaks DOWN on volume = invalidation; not the setup you thought

**How the crew uses Volume Dry-Up:**
- **Holly (Scanner Alpha):** flags dry-up patterns after 3+ qualifying bars; alpha is in the spring-loaded condition.
- **Chekov (Convergence):** "volume dry-up" is one of the named convergence votes.
- **Navigator:** prefers pullback-to-SMA + volume dry-up + EMA ribbon = three-strategy continuation entry.
- **Spock (Pure Quant):** uses volume entropy as a volatility-compression input.

**Gotchas:** Dry-up is a setup, not a signal — you still need the re-expansion to enter. Holidays and pre-market sessions naturally show dry volume that means nothing structurally. On low-float small-caps, "dry-up" might just be a single market-maker stepping back; not a signal. The re-expansion must be in the trend direction; volume bursts down through the SMA invalidate the whole structure.

---

## ▶️ Trend Resumption

*The "buy the dip in an uptrend" thesis, with rules that try to keep you from buying the wrong dip.*

**One-liner:** Trend resumption setups identify a healthy retracement inside an established trend (lower volume, shallow drawdown, hold of key support) and trigger entry when the trend visibly reasserts itself.

**Computed:** Composite signal — typically requires (a) price > 50 SMA, (b) 50 SMA > 200 SMA, (c) recent pullback bottomed at a defined support (20 SMA, prior swing low, gamma flip level), (d) volume dry-up during pullback, (e) volume re-expansion on the trigger bar in the trend direction.

**Numbers:**
- Pullback depth < 38.2% Fib of prior swing = shallow, high-quality
- Pullback depth 38.2–61.8% Fib = standard retracement, still tradeable
- Pullback depth > 61.8% Fib = deep, trend integrity at risk
- Trigger bar volume > 1.5× average + closing in top third of range = entry
- Reclaim of broken structure within 1–3 bars after a stop-out = strongest re-entry signal

**How the crew uses Trend Resumption:**
- **Chekov (Convergence):** named as one of the convergence vote types; usually the deciding one in continuation plays.
- **Navigator:** primary engine for the "Navigator buys" you see in the comms (gap_fill + volume_dry_up + ema_ribbon).
- **Spock (Pure Quant):** assigns trend-resumption setups a higher base-rate prior than mean-reversion plays.
- **Holly (Scanner Alpha):** scans for the trigger bar in real-time; flags into the convergence queue.

**Gotchas:** Trend resumption fails the moment the trend itself fails — and you don't know the trend has ended until well after it has. Use a hard structural stop (below the pullback low, below the 50 SMA, etc.), not just a percentage. In choppy or distribution markets, "resumption" setups print constantly and resolve into the same range — only take them when broader breadth confirms.

---

## 💥 Breakout Volume

*A breakout is only real if the tape says it is — and the tape speaks in volume.*

**One-liner:** Price breaking above a defined resistance level (range high, prior pivot, descending trendline) without a meaningful volume expansion is more often a fake-out than a breakout; volume confirmation separates the two.

**Computed:** Identify the resistance level (recent swing high, multi-touch trendline, consolidation range top). On the breakout bar, require: (a) close above the level, (b) volume ≥ 1.5× the 20-bar average, (c) ideally close in the top third of the bar's range. Bonus signal: tightness in the bars immediately before (low-volatility coil → expansion).

**Numbers:**
- Breakout volume < 1× average = likely fake; expect re-entry into range within 1–3 bars
- Volume 1–1.5× = ambiguous; wait for a higher-low pullback to confirm
- Volume 1.5–3× = quality breakout; standard entry
- Volume > 3× + close in top quartile = strong breakout, expect continuation
- Volume > 10× on a low-float = momentum window; manage like a trade, not an investment

**How the crew uses Breakout Volume:**
- **Holly (Scanner Alpha):** flags volume-confirmed breakouts in real time; feeds the convergence queue.
- **Chekov (Convergence):** "breakout volume" is one of the named convergence vote types.
- **Navigator:** requires volume-confirmed break before taking the entry; rejects volume-light breakouts even if everything else aligns.
- **Sulu (DayBlade):** intraday breakouts on volume drive 0DTE strike selection for the next leg.

**Gotchas:** Pre-market gaps can register as "breakouts" with no real volume support — wait for the cash-session print. Quarter/month-end rebalancing creates fake volume that has nothing to do with the chart. Breakouts of obvious levels (round numbers, headline resistance) attract algorithmic stop-running; the cleanest breakouts often come from less-obvious technical levels.

---

## 💨 IV Crush

*The post-earnings (or post-event) collapse in implied volatility — premium-sellers feast, premium-buyers bleed.*

**One-liner:** Before a known event (earnings, FDA decision, Fed meeting), implied volatility on options inflates as market participants price in uncertainty — once the event passes, IV deflates rapidly even if the underlying barely moves, which crushes the value of long options.

**Computed:** Compare implied volatility on the same-strike, similar-DTE option from the day before the event to the day after. A typical post-earnings IV drop of 30–60% in a single session is "IV crush." Quantitatively, IV Crush % = `(IV_pre − IV_post) / IV_pre`.

**Numbers:**
- Pre-earnings IV percentile > 70 = significant crush expected
- Post-event IV drop of 20–40% = standard crush
- Post-event IV drop > 50% = severe crush — long calls/puts often lose value even if directionally correct
- Stocks that "missed but rallied" / "beat but dropped" = the crush is doing the work; the directional bet alone wasn't enough
- IV percentile rebounds within 5–10 sessions to baseline; the trade window is small

**How the crew uses IV Crush:**
- **Worf (Bear Spreads):** sells bear call spreads into elevated pre-event IV; collects the crush as the spread decays.
- **McCoy (Crisis Doctor):** sells cash-secured puts on quality names with high pre-event IV; pockets the crush.
- **Sulu (DayBlade):** avoids buying naked options into earnings unless directional conviction is extreme.
- **Holly (Scanner Alpha):** flags upcoming events on high-IV-percentile names for premium-selling candidates.

**Gotchas:** Premium-selling into IV crush is profitable in expectation but tail-risky — the move can exceed the spread width, blowing through both legs. Earnings reactions are bimodal; size accordingly. IV crush isn't unique to earnings — any scheduled event (FDA decision, Fed meeting, OpEx) creates pricing-in/pricing-out cycles.

---

## Δ Delta

*The first Greek — how much an option's price changes per $1 move in the underlying.*

**One-liner:** Delta measures directional sensitivity. A 0.30 delta call gains ~$0.30 when the stock goes up $1; a 0.70 delta put loses ~$0.70 when the stock goes up $1.

**Computed:** Theoretical delta comes from Black-Scholes; in practice, brokers display it directly on the option chain. Delta is also a rough proxy for the probability the option expires in-the-money: a 0.30 call has roughly a 30% chance of finishing ITM at expiry. Delta varies from 0 to 1 for calls and 0 to −1 for puts.

**Numbers:**
- 0.50 delta = at-the-money (ATM); 50/50 odds, 50% directional capture
- 0.30 delta = ~30% prob ITM; commonly sold as the short leg of credit spreads
- 0.16 delta = ~1-standard-deviation OTM; "safe" short strike for high-prob spreads
- 0.70+ delta = deep ITM; behaves more like stock than an option
- Delta of 1.00 = stock equivalent; option moves dollar-for-dollar with shares

**How the crew uses Delta:**
- **Worf (Bear Spreads):** sells the 0.30 delta call as the short leg; 0.16 delta if seeking higher probability.
- **McCoy (Crisis Doctor):** sells 0.30 delta cash-secured puts on quality names; collects premium.
- **T'Pol (0DTE Defense):** monitors portfolio delta in aggregate; targets delta-neutral for iron condor setups.
- **Sulu (DayBlade):** picks 0.40–0.50 delta strikes for short-term directional 0DTE bets — high capture, fast theta.

**Gotchas:** Delta is dynamic — it changes as the stock moves (that's gamma's job). A 0.30 delta short put can become 0.70 delta if the stock drops 5%; your "30% probability" trade is now a "70% pain" trade. Delta-as-probability is an approximation, not a guarantee; works best on liquid, near-the-money strikes.

---

## Γ Gamma

*The second Greek — how fast delta itself changes as the stock moves.*

**One-liner:** Gamma is the rate of change of delta. High gamma means the option's directional sensitivity ramps quickly with the underlying; low gamma means it's stable. Gamma is highest near at-the-money and on near-expiry options — which is why 0DTE is so dangerous.

**Computed:** Mathematically: γ = ∂Δ/∂S, the partial derivative of delta with respect to the underlying price. In practice: take two scenarios 1% apart on the underlying, compare deltas, and the difference per dollar of move is gamma. Brokers display gamma directly on the option chain.

**Numbers:**
- ATM 30-DTE call: gamma ≈ 0.02–0.05 (delta moves ~0.02–0.05 per $1 stock move)
- ATM 1-DTE call: gamma ≈ 0.20+ (delta can swing wildly intraday)
- OTM 30-DTE: gamma low (~0.005); option behaves mostly as a binary
- Aggregated dealer gamma (GEX) > 0 = dealers buy dips, sell rips (stabilizing)
- Aggregated dealer gamma (GEX) < 0 = dealers sell dips, buy rips (amplifying)

**How the crew uses Gamma:**
- **T'Pol (0DTE Defense):** core risk metric — gamma cannibalization on 0DTE means a small underlying move can wipe credit-spread P&L.
- **Troi (Counselor):** reads aggregate dealer gamma (positive = stable market, negative = volatile market).
- **Worf (Bear Spreads):** prefers low-gamma environments (30+ DTE) where short-leg moves are predictable.
- **Sulu (DayBlade):** rides gamma — buys 0DTE near gamma flips for explosive delta expansion.

**Gotchas:** Gamma cuts both ways — it accelerates wins AND losses. The "0DTE roulette" reputation comes from gamma being maximum on expiry day. Position sizing matters MORE on high-gamma trades, not less. Aggregated market gamma (GEX) regimes can change intraday; what looks stable at 10 AM can flip to volatile by 2 PM around large OpEx events.

---

## 🧱 Put Wall / Call Wall

*The largest concentrations of dealer-hedged options open interest — magnets and barriers that shape intraday price action.*

**One-liner:** Strikes with massive open interest force dealers (who are typically short those options) to hedge in volume — Put Walls act as support (dealers buy the dip to hedge), Call Walls act as resistance (dealers sell the rip to hedge).

**Computed:** Aggregate option open interest at each strike, weighted by dealer-hedging implications. The Put Wall is the strike with the largest concentration of put-side gamma exposure below current price; Call Wall is the largest concentration above. Different vendors compute slightly differently (open interest vs. gamma-weighted vs. volume-weighted); for OllieTrades, gamma-weighted is the canonical source.

**Numbers:**
- Distance to Put Wall < 0.5% = strong support; expect bounce attempts
- Distance to Call Wall < 0.5% = strong resistance; expect rejection attempts
- Price between walls = "pinned" range; mean-reversion strategies favored
- Price breaks through Call Wall on volume = momentum acceleration; walls become magnet → resistance flip
- Wall rolls (large OI moves between strikes) = regime change for the day

**How the crew uses Put Wall / Call Wall:**
- **T'Pol (0DTE Defense):** sets strike selection — never sells short options inside the walls; targets outside the walls.
- **Troi (Counselor):** quotes wall levels in the daily read ("Key levels: call wall $758, put wall $750").
- **Sulu (DayBlade):** trades the bounce/rejection at wall touches; tightest stops in the playbook.
- **Riker (Daily Briefing):** includes wall levels in the morning gameplan as key intraday pivots.

**Gotchas:** Walls aren't immovable — heavy directional flow can roll them mid-session. The "pin" effect is strongest on OpEx Fridays (third Friday of the month) and weakest on light-volume days. Walls on individual stocks are less reliable than walls on indices (SPY/QQQ) due to thinner option markets. Always check that the wall hasn't already been broken before assuming it's still a level.

---

## 🧲 GEX — Gamma Exposure

*The aggregate dealer hedging requirement across all options — turns the entire market into one giant feedback loop.*

**One-liner:** GEX tells you whether dealers need to BUY when price drops (positive GEX = stabilizing) or SELL when price drops (negative GEX = destabilizing). It's the market's hidden volatility regime.

**Computed:** Sum across all strikes: `GEX = Σ (gamma × open_interest × 100 × spot²)` with sign convention applied — dealer net long gamma positive, net short negative. Common simplification: positive GEX = put-heavy environment (dealers hedged long), negative GEX = call-heavy (dealers hedged short).

**Numbers:**
- GEX > $5B (positive) = stable regime; expect range-bound, mean-reverting action
- GEX 0 to $5B = mildly stabilizing; normal volatility
- GEX < 0 = volatile regime; trend-following and momentum strategies favored
- GEX < −$3B = "vol expansion" warning; large directional moves likely
- Gamma Flip = the spot price at which net GEX crosses zero; pivotal intraday level

**How the crew uses GEX:**
- **Troi (Counselor):** primary regime input — sets Options Structure intermarket signal.
- **T'Pol (0DTE Defense):** halts new credit-spread entries when GEX flips negative mid-session.
- **Sulu (DayBlade):** trades momentum when GEX is negative, mean-reversion when positive.
- **Riker (Daily Briefing):** reports daily GEX and Gamma Flip in morning briefing; sets day's expected volatility profile.

**Gotchas:** GEX is a regime indicator, not a directional one — high positive GEX doesn't say "up," it says "calm." Single-stock GEX is noisy; index-level GEX (SPX/SPY) is the reliable read. GEX shifts intraday as new options trade — refresh hourly during volatile sessions, not just at open. Vendors differ in methodology by 10–30% on the same day; pick one source and stay consistent.

---

## 🦅 Iron Condor

*A defined-risk, four-legged short-volatility strategy that profits if the underlying stays in a range.*

**One-liner:** Sell a call spread above the market AND sell a put spread below the market — collect both premiums. If the underlying stays between the two short strikes through expiry, both spreads expire worthless and you keep the credit.

**Computed:** Four legs: (1) sell OTM call, (2) buy further OTM call (defines upside risk), (3) sell OTM put, (4) buy further OTM put (defines downside risk). Net credit = sum of premiums received minus premiums paid. Max profit = net credit. Max loss = wing width − net credit. Breakevens = short call strike + credit (upside), short put strike − credit (downside).

**Numbers:**
- Best entry IV percentile = > 50 (premium-rich environment)
- Typical short-strike delta = 0.16–0.30 (high-prob, lower-credit setups)
- Typical wing width = $5–$10 on SPY, scaled for stock price
- Profit target = 50% of max credit (close early, don't hold through expiry)
- Stop / management = 200% of credit on either side, or breach of short strike

**How the crew uses Iron Condor:**
- **Worf (Bear Spreads) + McCoy (Crisis Doctor):** jointly run condor candidates; one watches the call side, the other the put side.
- **T'Pol (0DTE Defense):** approves intraday/short-DTE condors in low-volatility regimes only (GEX positive, VIX < 20).
- **Troi (Counselor):** flags "no clear direction + premium-rich" as condor-favorable environment.
- **Spock (Pure Quant):** estimates expected value of condors using realized vs. implied vol spread.

**Gotchas:** Condors lose money fast when the underlying moves through a short strike — defined risk is real but uncomfortable. Earnings, FOMC, and other event days can blow through wings; avoid holding condors through scheduled binary catalysts. Liquidity matters — four-legged trades on illiquid options bleed money on slippage. Iron condors are NOT "free money"; they trade frequent small wins for occasional ugly losses (negatively skewed P&L).

---

## 💰 Covered Call

*An income strategy that monetizes existing stock positions by selling upside potential.*

**One-liner:** Own 100 shares of a stock, sell one OTM call against it — you collect the premium immediately, and if the stock stays below the strike at expiry, you keep both the shares and the premium.

**Computed:** Long 100 shares + short 1 OTM call (typically 30–45 DTE). Max upside = strike − cost basis + premium received. Max downside = unlimited (you still own the stock if it crashes). Breakeven = cost basis − premium received. Effective yield = (premium / cost basis) × (12 / months_to_expiry).

**Numbers:**
- Typical short strike delta = 0.20–0.30 (modest upside cap, decent premium)
- Typical DTE = 30–45 days (peak theta decay zone)
- Effective annualized yield from premium = 8–25% on quality dividend names
- Roll trigger = short strike within 1× ATR of spot, OR delta > 0.50
- Maximum reasonable basis-improvement run = 6–10 cycles before re-evaluating thesis

**How the crew uses Covered Call:**
- **Dax (Dividend Value):** core income overlay on long-held dividend positions; pairs with the dividend itself for compounded yield.
- **McCoy (Crisis Doctor):** uses covered calls on positions that ran into overbought territory rather than selling outright.
- **Spock (Pure Quant):** computes expected value of covered-call income vs. straight buy-and-hold; chooses higher EV.
- **Troi (Counselor):** signals "elevated VIX + bullish bias" as ideal covered-call environment.

**Gotchas:** Covered calls cap your upside — if the stock rallies 50% on news, you exit at the strike and miss the rest. Buying back the call after a rally to "save the shares" often costs more than the original premium. Selling calls too tight (high delta, short DTE) on volatile names produces frequent assignment headaches. Not a hedge — your downside is still 100% of the stock's drop minus the small premium.

---

## 💵 Cash-Secured Put

*The mirror of the covered call — get paid to wait for a price you'd happily buy at anyway.*

**One-liner:** Hold cash equal to (strike × 100), sell one OTM put — you collect premium immediately, and if the stock stays above the strike at expiry, you keep the premium without ever owning shares.

**Computed:** Short 1 OTM put + reserve `strike × 100` in cash to cover assignment. Max profit = premium collected. Max loss = (strike × 100) − premium collected (if stock goes to zero). Breakeven = strike − premium. If assigned, effective cost basis = strike − premium received.

**Numbers:**
- Typical short strike delta = 0.20–0.30 (modest assignment probability, decent premium)
- Typical DTE = 30–45 days
- Effective annualized yield = 10–25% on quality names with patient capital
- Best entry IV percentile = > 50 (premium-rich)
- Wheel mode = sell CSP → if assigned, sell covered calls until called away → repeat

**How the crew uses Cash-Secured Put:**
- **McCoy (Crisis Doctor):** primary tool — sells CSPs on quality names that just dropped on no fundamental change (sentiment dislocation).
- **Dax (Dividend Value):** uses CSPs as entry mechanism for dividend positions; gets paid to set a limit order.
- **Wheel CSPs (system role):** the "Troi/McCoy" wheel rotates between CSPs and covered calls on SOXL/TQQQ in production book.
- **Worf (Bear Spreads):** sells CSPs at deeply oversold reversal candidates; pairs with bear call spreads on adjacent names.

**Gotchas:** "Cash-secured" is real — the cash IS at risk. A CSP on a single name that gaps down 40% on bad news loses 40% of the strike value minus the small premium. Selling CSPs on names you DON'T want to own is a category mistake; if you wouldn't buy the stock at the strike, don't sell the put. Tax treatment differs (short-term capital gains on premium) — relevant for taxable accounts.

---

## ⚡ 0DTE

*Zero days to expiry — the fastest, riskiest, most addictive corner of the options market.*

**One-liner:** Options that expire today. Gamma is maximum, theta decay is brutal, and every minute of price action translates directly into option-price action with no time-value buffer.

**Computed:** 0DTE refers to any option contract whose expiry is the same trading day. SPY/SPX, QQQ, and now IWM offer daily expiries (M/W/F + Tue/Thu added for SPX). Pricing dynamics are pure gamma and intrinsic value — extrinsic value collapses fast as the close approaches.

**Numbers:**
- Theta decay rate = ~50–80% of remaining value in the final 90 minutes
- ATM 0DTE gamma = 0.20–0.40 (delta can swing 30+ points on a 1% underlying move)
- Win rate for buying ATM 0DTE = 35–45% historically (high variance, occasional huge wins)
- Win rate for selling 0DTE credit spreads = 70–85% (small wins, occasional big losses)
- "Power hour" (3–4 PM ET) = highest gamma + thinnest liquidity = worst time to enter, best time to exit

**How the crew uses 0DTE:**
- **Sulu (DayBlade):** primary specialist — directional 0DTE bets at key gamma levels (walls, flip, king node).
- **T'Pol (0DTE Defense):** unhalted 2026-05-27 with β/γ cannibalization defense layers; sells 0DTE credit spreads in stable regimes.
- **Troi (Counselor):** approves 0DTE only when GEX is positive and walls are well-defined.
- **Riker (Daily Briefing):** flags 0DTE windows when intermarket signals are aligned.

**Gotchas:** 0DTE is the most efficient way to lose money fast in retail trading. Liquidity dries up in the final 15 minutes; bid-ask spreads can be 50%+ of the option price. Stops don't always trigger (price gaps right through them). Size positions assuming you might lose 100% of the entry premium — because you might. Selling 0DTE looks profitable until the one day it doesn't.

---

## 😰 VIX

*The "fear gauge" — the market's pricing of expected near-term S&P 500 volatility.*

**One-liner:** VIX is calculated from SPX option prices and expresses, in annualized percentage terms, the implied volatility the market expects over the next 30 days. Higher = more fear / uncertainty priced in.

**Computed:** Weighted average of out-of-the-money SPX put and call IVs across two near-term expirations, normalized to a 30-day window. CBOE formula is in their white paper; brokers and data feeds display VIX directly. VIX of 16 ≈ 1% expected daily SPX move (16/√252).

**Numbers:**
- < 15 = CALM regime; full position sizing
- 15–20 = NORMAL; standard allocation
- 20–30 = ELEVATED; reduce position size 25%, tighten stops
- 30–40 = FEAR; reduce size 50%, defensive posture
- > 40 = PANIC; cash or inverse only — recovery setups warm up here

**How the crew uses VIX:**
- **Troi (Counselor):** primary position-sizing modifier — sets the Size 1.0×/0.75×/0.5×/0×.
- **Dalio (Macro):** treats VIX > 28 as one of two conditions for Midterm Recovery Protocol activation.
- **Riker (Daily Briefing):** reports VIX in the morning gameplan with size guidance attached.
- **Worf / McCoy (Premium Sellers):** prefer VIX 18–28 — rich premium without panic-level event risk.

**Gotchas:** VIX is forward-looking implied vol, not realized vol — it can be wrong about what actually happens. VIX backwardation (front-month VIX > VIX futures) is a stress signal that lasts longer than the spike itself. "Crush" trades selling VIX after a panic spike are statistically profitable but tail-risky — the spike can keep going. VIX at extreme lows (< 12) historically precedes volatility expansions — calm doesn't mean safe.

---

## 🎢 Fear & Greed

*A composite sentiment index — seven signals collapsed into one 0-to-100 number.*

**One-liner:** Mixes market momentum, breadth, junk-bond demand, volatility, put/call ratio, safe-haven demand, and stock-price strength into a single sentiment score. Extreme readings on either end are mean-reversion candidates.

**Computed:** Seven equally-weighted sub-components, each normalized to a 0-to-100 scale, then averaged. OllieTrades uses a derived version with weighted inputs: VIX, RSI (SPY), market breadth (advance/decline), safe-haven demand (TLT vs SPY relative), and momentum (50-day vs 125-day).

**Numbers:**
- 0–24 = EXTREME FEAR — historically a bullish reversal zone for swing positions
- 25–44 = FEAR — sentiment headwind for longs
- 45–55 = NEUTRAL — no edge from sentiment
- 56–75 = GREED — sentiment tailwind, but watch for froth
- 76–100 = EXTREME GREED — historically a top-of-range zone, take profits / tighten stops

**How the crew uses Fear & Greed:**
- **Troi (Counselor):** displays the score with explicit action implications ("take profits, tighten stops" at 76+).
- **Dalio (Macro):** treats F&G < 35 as one of two conditions for the Midterm Recovery Protocol.
- **Riker (Daily Briefing):** reports the daily F&G score in the morning gameplan.
- **Capitol Trades (Congress Copycat):** down-weights bullish congressional buys when F&G > 75 (frothy distribution risk).

**Gotchas:** F&G is a contrary indicator at extremes only — at moderate readings (40–60) it carries no signal. The index can stay in EXTREME GREED for weeks during melt-up trends; "extreme" isn't a stop sign. Different sources publish different methodologies (CNN vs. Crypto Fear & Greed vs. custom indices); know which one you're reading. The score is descriptive of past/current state, not predictive of next move.

---

## 🌬️ Market Breadth

*The "behind the headline" measure — how many stocks are actually participating in the move?*

**One-liner:** Indices can rise on a handful of mega-caps while the average stock is rolling over. Breadth measures the underlying participation: how many issues are advancing vs. declining, above their moving averages, making new highs vs. new lows.

**Computed:** Standard breadth metrics include: (1) advance/decline ratio (NYSE/NASDAQ), (2) % of S&P 500 stocks above their 20-/50-/200-day SMAs, (3) new 52-week highs minus new 52-week lows, (4) cumulative advance-decline line vs. price (divergence signal). OllieTrades canonicalizes (2) — % of sectors above their 20MA.

**Numbers:**
- > 75% of stocks above 20MA = strong, broad uptrend
- 50–75% above 20MA = healthy trend
- 25–50% above 20MA = narrowing leadership — caution warranted
- < 25% above 20MA = broken trend or panic; defensive posture
- New highs > new lows on a down day = potential bullish divergence

**How the crew uses Market Breadth:**
- **Troi (Counselor):** sets the "5/11 sectors above 20MA" warning when breadth narrows.
- **Riker (Daily Briefing):** reports breadth in the morning gameplan; cautions on narrow leadership.
- **Spock (Pure Quant):** uses breadth divergence as input to regime classification (BULL_TREND vs. CHOP vs. BEAR_TREND).
- **Dalio (Macro):** weights breadth deterioration heavily when sizing macro positions.

**Gotchas:** Breadth can lead or lag depending on the regime — sometimes narrowing breadth precedes a top by months, sometimes by days. Don't size based on breadth alone; pair it with price action and a stop. Sector-level breadth (11 sectors) is noisier than stock-level breadth (500 names); both have uses, neither is definitive. Breadth thrust signals (a sudden surge from oversold to broad participation) historically mark major bottoms — rare but powerful.

---

## 👑 King Node

*The single strike with the largest open interest — where the market's option positioning is most concentrated.*

**One-liner:** The "king" of a given expiration's option chain is the strike with the largest open interest — dealers hedging that strike create the strongest gravitational pull on intraday price.

**Computed:** Scan the option chain for the expiration of interest (often nearest weekly or monthly), find the strike with the highest open interest across calls and puts combined. King Node strength = OI at that strike / median OI across the chain. A King Node 5× the median is dominant; 20× is a magnet.

**Numbers:**
- Distance from spot to King Node < 0.5% = strong pin pressure
- King Node OI 5–10× chain median = noticeable influence
- King Node OI > 20× chain median = dominant magnet
- Multiple King Nodes within 1% = "node cluster" — choppy, range-bound action expected
- King Node above current price + heavy call volume = "magnet up" setup

**How the crew uses King Node:**
- **T'Pol (0DTE Defense):** treats King Node as the daily pin target; positions strikes outside it.
- **Sulu (DayBlade):** trades the magnet — buys 0DTE in the direction of the King Node when underlying is offset.
- **Troi (Counselor):** notes King Node in the daily Options Structure read.
- **Riker (Daily Briefing):** includes King Node level when daily flow concentrates at a single strike.

**Gotchas:** King Nodes don't matter on illiquid option chains — large OI on single-name stocks can come from one institutional hedge, not market consensus. The "pin" effect is strongest on OpEx Fridays and weakest on light-volume days. Multiple King Nodes can offset each other; check whether the surrounding open interest is balanced or skewed. King Node levels can flip intraday as new positioning trades through.

---

## 🎒 Trail Stop

*A dynamic stop that follows price up — locks in gains as a trade works, but never moves against you.*

**One-liner:** Instead of a fixed stop price, a trail stop adjusts upward (for longs) as price advances — staying a defined distance below the high-water mark. If price drops back to that distance, the stop fires.

**Computed:** Three common methods: (1) fixed-percentage trail (e.g., 8% below highest close), (2) ATR-based trail (e.g., 2× ATR below the highest high), (3) structural trail (below the most recent higher-low / swing low). The choice depends on whether you want time-based, volatility-based, or structure-based exits.

**Numbers:**
- 5% trail = tight; gets stopped out on normal volatility
- 8% trail = moderate; common for swing trades
- 12–15% trail = loose; appropriate for long-term trend trades
- 2× ATR trail = volatility-adjusted; widens in chop, tightens in stability
- Structural trail = price-action-driven; trails the most recent swing low

**How the crew uses Trail Stop:**
- **Dr. McCoy (Crisis Doctor):** uses RSI-based autopilot trims — sells 50% of the position when RSI exceeds 70.
- **Lt. Jadzia Dax (Dividend Value):** scaled exits on momentum names — 50% at +5%, 25% at +7.8%, etc.
- **Spock (Pure Quant):** structural trail (below most recent higher low) on quant-detected trends.
- **Navigator (Convergence):** ATR-based trail wide enough to survive normal pullback within the trend.

**Gotchas:** Trail stops fail in gappy markets — overnight gaps can blow through the trail and exit at horrible prints. Tight trails compound trading costs and tax friction; loose trails give back too much of unrealized gain. Trail-stop psychology is brutal — watching profit retrace from +25% to +12% before the stop fires feels worse than the original entry stop. Use TIME-stops alongside trail stops to exit positions that go nowhere instead of slowly bleeding the trail.

---

## ⚖️ Position Sizing

*The least-glamorous, most-important risk decision — how much to risk on any single trade.*

**One-liner:** Position sizing determines what % of capital is exposed per trade. Get it right and survive bad streaks; get it wrong and one bad trade ends the account.

**Computed:** Standard formulas: (1) fixed fractional: `position_size = account_value × risk_pct / stop_distance`, (2) Kelly fraction: `f* = (W × R − L) / R` where W=win prob, R=avg win/avg loss ratio, L=loss prob, (3) volatility-targeted: `size = target_vol / (stop_distance / spot)`. Most retail traders use a fixed 1–2% risk-per-trade rule.

**Numbers:**
- Risk per trade = 0.5% of account → conservative, can survive 20+ consecutive losses
- Risk per trade = 1% of account → standard; can survive 10 consecutive losses
- Risk per trade = 2% of account → aggressive; one bad streak can be devastating
- Full Kelly = mathematically optimal long-run growth, but volatile; usually fractioned to 0.25× or 0.5× Kelly
- Maximum portfolio at any single name = 5–10% of account (concentration limit)

**How the crew uses Position Sizing:**
- **Troi (Counselor):** sets the daily Size 1.0× / 0.75× / 0.5× modifier based on regime signals.
- **Spock (Pure Quant):** computes per-trade size using fractional-Kelly applied to estimated edge.
- **All agents:** respect the FLEET CASH ceiling/reserve/floor structure — max $20K per category bucket.
- **Dalio (Macro):** allocates by volatility-target across asset classes (metals, equities, cash).

**Gotchas:** Sizing on win-rate alone misses the point — a 70% win rate with 1:2 risk-reward LOSES money. The Kelly formula assumes you know your edge precisely; most retail estimates are wildly optimistic. Position sizing should DECREASE on a losing streak, not stay constant ("anti-martingale"). The single best risk-management lever is size, not stop placement.

---

## ✅ Win Rate

*The simplest performance metric — what fraction of trades are winners?*

**One-liner:** Win rate = number of profitable trades / total trades. Useful for understanding strategy character, but worthless without payoff context (a 95% win-rate strategy losing $20 for every $1 it wins is unprofitable).

**Computed:** `WR = winning_trades / total_trades × 100`. Window matters — last 30 trades, last 90 days, since inception. OllieTrades fleet leaderboard reports rolling 30-day win rate per agent.

**Numbers:**
- > 70% = high win rate; usually trend-following with tight stops, or premium-selling
- 50–70% = balanced; achievable for most disciplined retail strategies
- 40–50% = trend-trader range — needs > 2:1 reward/risk to be profitable
- < 40% = low win rate; only profitable with high R-multiples (3:1, 5:1, 10:1)
- 100% win rate over many trades = a yellow flag; you're not taking enough loss to learn

**How the crew uses Win Rate:**
- **Fleet Leaderboard:** displays win rate per agent (Neo 94.9%, Dax 100%, Worf 100%, Ollie 63.5%).
- **Riker (Daily Briefing):** "FULL THROTTLE / BENCH" status partially driven by recent win-rate trend.
- **Spock (Pure Quant):** combines win rate with expectancy and Sharpe to rank composite signals.
- **Captain (Steve):** uses win rate as a sanity check on agents that look profitable on P&L but have suspiciously low trade counts.

**Gotchas:** A 100% win rate on 5 trades is statistically meaningless. Survivorship bias inflates retail win rate estimates — losing trades get hidden, taken off the books, or "haven't stopped out yet." Win rate without payoff context misleads — premium sellers can show 90%+ win rates while losing money during a tail event. Watch trade count alongside win rate; a 100% W/L of 4/0 says little.

---

## 🔢 Profit Factor

*The truer profitability metric — total gross profit divided by total gross loss.*

**One-liner:** Profit Factor answers: for every $1 lost on losing trades, how many dollars do winning trades make? A profit factor above 1.0 means the strategy is net profitable; below 1.0 means it's bleeding.

**Computed:** `PF = sum_of_gross_winning_trades / sum_of_gross_losing_trades` (absolute values, no signs). Computed over a defined window — last 30 days, last 100 trades, since inception. Distinct from win rate: a 30%-WR strategy with 5:1 reward/risk has PF of ~2.0 and is profitable; a 70%-WR strategy with 0.3:1 has PF of 0.7 and is losing.

**Numbers:**
- < 1.0 = losing strategy; do not deploy real capital
- 1.0–1.3 = marginally profitable; barely covers slippage and fees
- 1.3–2.0 = solid; most realistic professional strategies live here
- 2.0–3.0 = strong; usually small sample or genuine edge
- > 3.0 = exceptional; verify the sample size isn't lying to you

**How the crew uses Profit Factor:**
- **Spock (Pure Quant):** reports PF alongside Sharpe in composite agent ranking.
- **Fleet Leaderboard:** profit factor visible on every agent row (Neo PF 2.10, Worf PF 6.68 — small sample, high quality).
- **Spock / Strategy Lab:** uses PF as a gate for shadow-to-production strategy promotion (PF > 1.3 minimum on OOS sample).
- **Captain (Steve):** sanity check — an agent with PF > 5.0 on < 20 trades is either a genuine outlier or a sample-size mirage.

**Gotchas:** PF is sensitive to outliers — one huge winner can push PF artificially high (and vice versa for one disaster). Use PF alongside median trade and worst-trade-percent-of-account to get a fuller picture. PF over a single regime (just a bull run) doesn't generalize — demand PF across multiple regimes before sizing up. Slippage and commissions reduce PF in live trading vs. backtest; pad your minimum threshold.

---

## 🎲 Expectancy

*The mathematical edge per trade — the metric the W0 SUPER_MAX program built itself around.*

**One-liner:** Expectancy combines win rate and average payoff into a single per-trade dollar (or R-multiple) value: `E = (Win% × AvgWin) − (Loss% × AvgLoss)`. Positive expectancy = the math says trade; negative = the math says stop.

**Computed:** Two common forms: (1) dollar expectancy: `E$ = (WR × avg_$_win) − ((1 − WR) × avg_$_loss)`, (2) R-multiple expectancy: `ER = (WR × avg_R_win) − ((1 − WR) × avg_R_loss)`, where R = initial risk on the trade (e.g., distance to stop). R-multiple is preferred because it's position-size-invariant.

**Numbers:**
- E$ < 0 = losing on average — every trade is a coin flip with negative skew
- E$ = 0 = breakeven; trading covers your time
- ER between 0 and 0.2 = positive but marginal; needs volume to matter
- ER 0.2–0.5 = solid edge — the W0 expectancy gate (BUY +0.43R @ 5d) sits here
- ER > 0.5 = exceptional; verify against multiple regimes before sizing up

**How the crew uses Expectancy:**
- **Spock (Pure Quant) + Expectancy Engine (W0):** confirmed +0.43R @ 5d BUY edge on 1,095 trades, 57% WR — the load-bearing W0 win.
- **Chekov (Convergence):** ranks setups by estimated expectancy, not just count of agreeing strategies.
- **Spock (Strategy Promotion):** strategies must clear expectancy gate before promotion from shadow to live execution.
- **Captain (Steve):** treats expectancy as the truest measure of edge across the fleet.

**Gotchas:** Expectancy on small samples is meaningless — need 100+ trades minimum to trust the number, 500+ for confidence intervals. Strategies can have positive expectancy in backtest and negative in live trading due to slippage, fees, and regime change. Expectancy in one regime (bull-quiet) doesn't generalize to others (bear-vol). Pad with a multi-regime requirement before sizing aggressively. The W0 expectancy engine exists precisely because intuition lies about edge and math doesn't.

---

## Closing notes

**Total cards now shipped:** 10 (batch 1) + 26 (batch 2) = **36 of 37**.

**Reference card:** RSI ships inline in handoff section 4 — already-rendered as the model example, no separate card needed.

**Five-section shape maintained:** every card carries (1) tagline, (2) one-liner, (3) computed, (4) numbers, (5) crew usage, (6) gotchas — written for traders who want to know not just what the metric is, but how the fleet's named agents actually act on it.

**Crew anchoring:** characters reference the live OllieTrades fleet roster — Spock/Data/Uhura/Worf/McCoy/Dax/Sulu/T'Pol/Troi/Dalio/Holly/Navigator/Chekov/Riker/Capitol Trades. Each card names 3–4 specific agents whose strategy depends on or reads that concept. Future-proofs the cards against agent additions by anchoring to behaviors, not just labels.

**Implementation hook:** each card's `## emoji Name` heading is suitable as an `id` slug (e.g., `gex` → `#concept-gex`); ticker-card and walk-back links should target these anchors via the `concept_id` field already specified in handoff section 6 (signal payload schema).
