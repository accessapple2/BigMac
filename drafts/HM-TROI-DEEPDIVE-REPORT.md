# HM-TROI-DEEPDIVE-2026-07-03 — Troi (options-sosnoff) Options Book Deep Dive

Read-only analysis. No config changes, no trades, no DB writes outside this file.
Source: `data/trader.db` (`options_trades`, `trades`, `ai_players`, `options_books`, `regime_history`),
`engine.market_data.get_alpaca_bars`, `AGENT-RULES-REVIEW-2026-07-03.md` / `docs/XO_BACKLOG.md`
(HM-TROI-MAXPOS-CAP-DEAD, filed 2026-07-03 — this deep-dive corroborates and extends that finding).

## Data-provenance note (read this before the numbers below)

The directive states Troi's realized P&L as **+$9,442.88**. The cleanly-sourced, verifiable number
from `options_trades` (agent_id='options-sosnoff', the CSP/Wheel ledger) is **+$7,972.21** across 36
closed trades — this exactly matches yesterday's HM-TROI-MAXPOS-CAP-DEAD finding (100% WR, 36/36).
I could not reconstruct $9,442.88 exactly from any single query. The closest bridge: $7,972.21 (closed
CSPs) + $1,546.51 (one early TQQQ put sold 2026-03-30, closed 2026-04-08, *before* the `options_trades`
table existed — a pre-system wheel-style trade) = $9,518.72, about $76 over the stated figure. A
separate legacy `trades`-table entry for 3 GOOGL covered-call sells (+$513.84) predates the wheel
system entirely and is unrelated to SOXL/TQQQ — excluded. **All P&L analysis below uses the verified
$7,972.21 CSP ledger** as the primary basis; where the pre-system TQQQ trade is relevant to the regime
question it's called out separately.

---

## 1. Ledger Summary

**84 total options_trades rows for options-sosnoff, book_tag='fleet', structure='csp' throughout**
(no `covered_call` structure rows exist anywhere in the table — see Regime Verdict, this matters):
- **36 closed** (2026-05-17 → 2026-06-12 opens), **100% win rate**, **zero assignments**
- **48 still open** (2026-06-05 → 2026-06-29 opens)

Every position: sell a ~30 DTE, ~12% OTM cash-secured put. Every closed trade exited via one of three
mechanisms — **never** via assignment:

| exit_reason | count | sum P&L | avg P&L | what it means |
|---|---|---|---|---|
| `time_stop_21dte` | 24 | $3,554.11 | $148.09 | Closed at 9 days held regardless of price — the dominant exit (67% of trades), captures only the front ~30% of theta decay |
| `tp_premium_decay_50pct` | 6 | $1,054.10 | $175.68 | Closed early once premium decayed 50% |
| `expired_otm` | 6 | $3,364.00 | $560.67 | Ran to expiry worthless — full premium kept, largest avg P&L/trade |

Full per-trade ledger (open date, strike, DTE@open, premium, outcome, days held, realized P&L) —
36 closed + 48 open rows — is in the appendix at the bottom of this file.

**Days held (closed trades):** min 9, max 30, avg 15.5. The *majority* of trades never got close to
expiry — they were pulled at the first opportunity (21 DTE), which is a conservative choice that
also means the strategy has collected far less than max theoretical premium per trade in exchange for
less time-in-market risk.

---

## 2. P&L Decomposition

**+$7,972.21 total, entirely premium capture — zero assignment-and-recovery gains, because zero
assignments have occurred.** This is the single most important fact about this book: there is no
"skill in managing the wheel through assignment" to evaluate yet, because the wheel has never
actually turned past step 1 (sell the put).

**Per-symbol (closed trades only):**

| Symbol | Trades | Sum P&L | Avg P&L | Win rate |
|---|---|---|---|---|
| SOXL | 6 | $3,364.00 | $560.67 | 100% |
| TQQQ | 24 | $3,351.94 | $139.66 | 100% |
| UPRO | 6 | $1,256.27 | $209.38 | 100% |

(Directive named "SOXL vs TQQQ" — UPRO is a real third of this book and included above for
completeness; it's also the single most exposed symbol in the open book, see §4.)

**Win rate / avg win / avg loss / largest loss:** 36/36 = 100%. Avg win $221.45. **Zero losses,
zero losing trades of any size.** Largest single win $604.00 (id 31, SOXL, expired worthless).
Smallest win $117.67 (id 76, TQQQ, time-stop exit).

**Max concurrent capital-at-risk (open CSPs, strike × 100 × contracts, summed):**

| Symbol | Open contracts | Total notional | Avg notional/contract |
|---|---|---|---|
| SPY | 6 | $387,696 | $64,616 |
| QQQ | 6 | $376,646 | $62,774 |
| SOXL | 18 | $334,210 | $18,567 |
| UPRO | 18 | $216,847 | $12,047 |
| **TOTAL** | **48** | **$1,315,399** | — |

This exactly matches yesterday's HM-TROI-MAXPOS-CAP-DEAD finding. Notable: **TQQQ currently has
zero open positions** — all 24 TQQQ CSPs sold are already closed. The live book today is
SOXL + UPRO + QQQ + SPY, not SOXL + TQQQ as the directive's framing (written before this deep-dive)
assumed. See §4 for why this matters a lot for the worst-case number.

---

## 3. Regime Dependence Check — Verdict: **This is beta, not demonstrated alpha**

### Evidence

**A. The market regime flipped to bear on 2026-06-11/12 and has stayed there for 3+ weeks —
straight through today.** From `regime_history` (SPY/QQQ 8/21 MA cross classifier):

| Date range | Regime |
|---|---|
| 2026-05-15 → 06-10 | BULL_CROSS / brief CAUTIOUS_BULL dips |
| **2026-06-11** | **BEAR_CROSS** |
| **2026-06-12 → 07-04 (today)** | **CAUTIOUS_BEAR, uninterrupted** |

**B. Troi did not pause or de-risk when the regime flipped — exposure *increased*, both in count and
in notional-per-contract, entirely inside the confirmed bear window:**
- 20 of the 48 open SOXL/UPRO positions were opened on **2026-06-11 and 06-12** — the exact
  regime-flip days.
- **All 12 QQQ/SPY positions (the two largest, newest legs, $764K combined notional) were opened
  2026-06-23 → 06-29** — 2+ weeks *into* the confirmed bear regime, not before it.
- The pivot from leveraged ETFs (SOXL/UPRO/TQQQ, ~$12-19K notional/contract) to QQQ/SPY
  (~$63-65K notional/contract) happened *because of* the Door-1 leveraged-ETF ban — but the
  practical effect was a ~4x jump in per-contract notional, executed during the worst regime window
  of the whole sample, not the best one.

**C. The underlying instruments did move — some of it violently, some of it close enough to test
strikes intraday — but nothing has yet crossed the line into actual assignment:**
- Using Troi's own self-reported entry-day spot prices (from `options_trades.notes`, more reliable
  than the raw Alpaca daily bars for SOXL — see data-quality note below): **SOXL actually *rallied*
  ~+70% net** over the trading window (from a 2026-05-19 dip of $141.45 to $241.34 by 06-12) — the
  100% win rate and best avg-P&L/trade on SOXL is substantially explained by semiconductors
  diverging *bullishly* from the broader "bear regime" label, not by Troi correctly timing anything.
- TQQQ and UPRO both show real chop with meaningful intraday range around the 06-09 to 06-11 window
  (the regime-flip days). Notably: **TQQQ's raw intraday low on 2026-06-09 was $66.79** — below the
  strikes of at least 3 CSPs opened just one day earlier (06-08, strikes $67.69/$67.96/$68.35).
  Those puts were very likely intraday-ITM on 06-09 and survived only because the underlying
  recovered by the close ($73.69) — a near-miss, not a demonstration that the strategy handles a
  real test well.
- **Data-quality caveat:** raw Alpaca daily bars for SOXL show implausible ~15-20%+ single-day
  swings in June (e.g., $226→$301→$230 over 4 trading days) inconsistent with the smooth strike
  progression in Troi's own trade notes. This looks like corrupted/split-unadjusted data, not real
  price action — I used the self-reported notes-based spot series instead, which is internally
  consistent with the strike ladder. Flagged as a separate scanner/data-quality issue, not something
  resolved in this report.

### Honest answer

**Alpha would require evidence the strategy correctly reads and adapts to changing risk conditions.
The evidence points the other way: exposure and per-contract notional both increased during the
confirmed bear window, and the strategy has a perfect record specifically because the underlying
instruments never fell far enough, fast enough, to breach a strike — not because Troi/the wheel
logic did anything to protect against the case where they do.** A -12%-OTM, 30-day cash-secured-put
seller on 3x-leveraged ETFs has a payoff shape that is *short volatility and short tail risk by
construction*: it wins steadily in any range-bound-to-mildly-bearish market and takes a large loss
exactly when a real drawdown finally arrives. 100% win rate over 36 trades and zero assignments over
84 trades (including 3+ weeks of a "bear regime" that only produced high-single-digit-to-teens %
moves) is the *expected* signature of that payoff shape during a period with no tail event yet — not
evidence the tail event has been handled well. This is "leveraged-ETF wheel in an up-to-choppy
market," i.e., regime-dependent beta, dressed in wheel-strategy language.

---

## 4. Scale & Risk Report

**Capital base ambiguity (same one flagged in HM-TROI-MAXPOS-CAP-DEAD, unresolved):** Troi's own
`ai_players.cash` = **$12,880.20**, decoupled from CSP accounting since HM-W1F4 (2026-05-17). The
CSP notional is drawn against a *shared* `options_books.fleet` pool (current_cash $73,380.21, split
across options-sosnoff + strategy:bull_spread_v1 + swingdesk-manual — no clean Troi-only slice).

**Current vs doubled, against the stated caps — it's already blown through every cap, before any
doubling:**

| Basis | Current notional | vs cap | Doubled notional | vs cap |
|---|---|---|---|---|
| own `ai_players.cash` ($12,880.20) | $1,315,399 | **102x** | $2,630,798 | **204x** |
| shared `options_books.fleet` cash ($73,380.21) | $1,315,399 | **17.9x** | $2,630,798 | **35.9x** |
| `config.OPTIONS_TOTAL_MAX_PCT` = 10% of the shared-book cash → cap should be **$7,338** | $1,315,399 | **179x the cap** | $2,630,798 | **358x the cap** |

There is no meaningful "where does doubling collide with the 30% position cap / cash floors"
question to answer, because **the collision already happened, silently, at current size** — this is
the same root cause HM-TROI-MAXPOS-CAP-DEAD identified: `wheel_strategy.py`'s position cap and dedup
check read from the stock `positions` table, which CSP legs never populate (they only write to
`options_trades`), so the cap has been structurally blind to this exposure since the wheel started.
"What happens if Troi's allocation doubles" is really "what happens if this keeps compounding with
zero effective governor" — and the QQQ/SPY pivot (4x the notional-per-contract of the leveraged-ETF
legs) shows that's already actively happening, not a hypothetical.

**Worst case: simultaneous assignment in a -20% semi/tech drawdown.**

Two readings, because "a -20% drawdown" is ambiguous for 3x-leveraged instruments:

*(a) -20% applied directly to each instrument's own price* (i.e., SOXL/UPRO themselves fall 20%,
not the sector they track 3x):

| Symbol | Assigned | Net paper loss |
|---|---|---|
| UPRO | 18 of 18 | $6,184.40 |
| SOXL | 4 of 18 | $1,678.20 |
| QQQ | 6 of 6 | $19,590.00 |
| SPY | 6 of 6 | $18,039.20 |
| **TOTAL** | | **$45,491.80** |

*(b) -20% applied at the sector/index level* — the economically correct reading of "a -20%
semi/tech drawdown," since SOXL/UPRO are 3x leveraged and would fall roughly 3x that (~60%), while
QQQ/SPY (unleveraged) fall the full -20%:

| Symbol | Assigned | Net paper loss | ETF-level shock applied |
|---|---|---|---|
| SOXL | 18 of 18 | $145,370.20 | -60% |
| UPRO | 18 of 18 | $106,617.20 | -60% |
| QQQ | 6 of 6 | $19,590.00 | -20% |
| SPY | 6 of 6 | $18,039.20 | -20% |
| **TOTAL** | | **$289,616.60** | |

**The directive's named scenario ("SOXL and TQQQ assigned simultaneously") is now moot as literally
stated — TQQQ has zero open positions today.** Reading (b) — **$289,616.60** — is the real,
economically-grounded worst case for the book as it actually stands, and it is **22.5x** Troi's own
stated cash ($12,880.20) and **3.9x** the entire shared-book cash pool ($73,380.21). A single
plausible, garden-variety sector correction (a -20% semiconductor/tech drawdown is well within
historical norms, not a black-swan assumption) would not just erase this book's $7,972.21 of
realized gains — it would wipe out several multiples of whatever capital base you use to measure it
against.

---

## 5. Recommendation: **Add guardrails immediately — do not scale, do not leave as-is**

This is not a call to shut the strategy down; premium-selling on liquid ETFs is a legitimate,
well-understood source of edge over long samples, and 36/36 winners is a real, if short, track
record. But three things are true at once and none of them support "leave as-is":

1. **The risk-management control that's supposed to bound this is provably non-functional**
   (HM-TROI-MAXPOS-CAP-DEAD's root cause, corroborated here) — `wheel_strategy.py` cannot see its
   own open CSP count because it reads the wrong table. This is a **fix-first, not optional** item.
2. **The strategy has never been tested by the event it's structurally short** (a real drawdown big
   enough to test a -12% OTM strike on a 3x ETF), and the sample window that produced the 100% win
   rate included a persistent "bear regime" that the strategy sailed through by exposure staying
   the same or growing, not by adapting.
3. **Worst-case exposure today ($289,616.60 on the economically-correct reading) already dwarfs
   every stated capital base by 4-22x.** Scaling up before fixing #1 would scale the bug, not the
   edge.

**Concrete next steps, in order:**
- Fix `wheel_strategy.py`'s position-count/dedup logic to read from `options_trades` (open CSPs by
  symbol), not the stock `positions` table — this alone re-arms the existing `MAX_POSITIONS=3` cap
  that's currently dead code for options.
- Resolve the capital-base ambiguity (own `ai_players.cash` vs shared `options_books.fleet` pool) so
  "10% options cap" means something concrete in dollars, then enforce it at CSP-open time, not just
  at position-review time.
- Until both of the above ship: **do not open new QQQ/SPY-sized (or larger) CSP legs** — the
  per-contract notional jump from the leveraged-ETF era is the single biggest lever on the
  worst-case number, and it happened without any explicit sizing decision, just a symbol swap.
- Once the caps are real: consider a smaller, explicitly-capped continuation to build a track record
  that actually includes an assignment event before trusting this book's win rate as representative.

---

## Appendix — full trade-level ledger

### Closed CSPs (36) — full lifecycle

| id | symbol | open date | strike | DTE@open | premium | outcome | days held | realized P&L |
|---|---|---|---|---|---|---|---|---|
| 30 | TQQQ | 2026-05-17 | $66.30 | 30 | $277.00 | tp_premium_decay_50pct | 25 | $190.29 |
| 31 | SOXL | 2026-05-17 | $144.48 | 30 | $604.00 | expired_otm | 30 | $604.00 |
| 32 | UPRO | 2026-05-17 | $122.96 | 30 | $514.00 | time_stop_21dte | 9 | $212.97 |
| 47 | TQQQ | 2026-05-18 | $64.96 | 30 | $275.00 | tp_premium_decay_50pct | 24 | $182.01 |
| 48 | SOXL | 2026-05-18 | $134.51 | 30 | $569.00 | expired_otm | 30 | $569.00 |
| 49 | UPRO | 2026-05-18 | $121.77 | 30 | $515.00 | time_stop_21dte | 9 | $213.38 |
| 50 | TQQQ | 2026-05-18 | $64.78 | 30 | $274.00 | tp_premium_decay_50pct | 24 | $181.35 |
| 51 | SOXL | 2026-05-18 | $133.62 | 30 | $565.00 | expired_otm | 30 | $565.00 |
| 52 | UPRO | 2026-05-18 | $121.58 | 30 | $514.00 | time_stop_21dte | 9 | $212.97 |
| 53 | TQQQ | 2026-05-19 | $63.91 | 30 | $261.00 | tp_premium_decay_50pct | 23 | $166.65 |
| 54 | SOXL | 2026-05-19 | $125.26 | 30 | $512.00 | expired_otm | 30 | $512.00 |
| 55 | UPRO | 2026-05-19 | $120.55 | 30 | $493.00 | time_stop_21dte | 9 | $204.27 |
| 56 | TQQQ | 2026-05-19 | $63.27 | 30 | $263.00 | tp_premium_decay_50pct | 23 | $167.93 |
| 57 | SOXL | 2026-05-19 | $124.48 | 30 | $518.00 | expired_otm | 30 | $518.00 |
| 58 | UPRO | 2026-05-19 | $119.89 | 30 | $499.00 | time_stop_21dte | 9 | $206.75 |
| 59 | TQQQ | 2026-05-20 | $65.68 | 30 | $269.00 | tp_premium_decay_50pct | 22 | $165.86 |
| 60 | SOXL | 2026-05-20 | $145.75 | 30 | $596.00 | expired_otm | 30 | $596.00 |
| 61 | UPRO | 2026-05-20 | $121.47 | 30 | $497.00 | time_stop_21dte | 9 | $205.93 |
| 62 | TQQQ | 2026-06-05 | $64.87 | 30 | $301.00 | time_stop_21dte | 10 | $128.96 |
| 67 | TQQQ | 2026-06-08 | $67.69 | 30 | $292.00 | time_stop_21dte | 9 | $120.99 |
| 70 | TQQQ | 2026-06-08 | $67.96 | 30 | $292.00 | time_stop_21dte | 9 | $120.99 |
| 73 | TQQQ | 2026-06-08 | $68.35 | 30 | $287.00 | time_stop_21dte | 9 | $118.92 |
| 76 | TQQQ | 2026-06-09 | $67.10 | 30 | $284.00 | time_stop_21dte | 9 | $117.67 |
| 79 | TQQQ | 2026-06-09 | $65.44 | 30 | $287.00 | time_stop_21dte | 9 | $118.92 |
| 82 | TQQQ | 2026-06-09 | $64.10 | 30 | $291.00 | time_stop_21dte | 9 | $120.57 |
| 86 | TQQQ | 2026-06-10 | $64.31 | 30 | $307.00 | time_stop_21dte | 12 | $140.54 |
| 90 | TQQQ | 2026-06-11 | $63.74 | 30 | $311.00 | time_stop_21dte | 11 | $137.75 |
| 96 | TQQQ | 2026-06-11 | $63.12 | 30 | $308.00 | time_stop_21dte | 11 | $136.42 |
| 99 | TQQQ | 2026-06-11 | $61.61 | 30 | $297.00 | time_stop_21dte | 11 | $131.55 |
| 102 | TQQQ | 2026-06-11 | $63.47 | 30 | $309.00 | time_stop_21dte | 11 | $136.86 |
| 105 | TQQQ | 2026-06-11 | $65.02 | 30 | $321.00 | time_stop_21dte | 11 | $142.18 |
| 108 | TQQQ | 2026-06-11 | $64.59 | 30 | $295.00 | time_stop_21dte | 11 | $130.66 |
| 111 | TQQQ | 2026-06-12 | $65.81 | 30 | $289.00 | time_stop_21dte | 10 | $123.82 |
| 114 | TQQQ | 2026-06-12 | $67.55 | 30 | $289.00 | time_stop_21dte | 10 | $123.82 |
| 117 | TQQQ | 2026-06-12 | $68.74 | 30 | $292.00 | time_stop_21dte | 10 | $125.11 |
| 120 | TQQQ | 2026-06-12 | $68.54 | 30 | $285.00 | time_stop_21dte | 10 | $122.11 |

### Open CSPs (48) — still live, no realized P&L

| id | symbol | open date | strike | DTE@open | premium | expiration |
|---|---|---|---|---|---|---|
| 63 | SOXL | 2026-06-05 | $166.87 | 30 | $774.00 | 2026-07-05 |
| 64 | UPRO | 2026-06-05 | $121.41 | 30 | $563.00 | 2026-07-05 |
| 68 | SOXL | 2026-06-08 | $185.86 | 30 | $803.00 | 2026-07-08 |
| 69 | UPRO | 2026-06-08 | $124.02 | 30 | $536.00 | 2026-07-08 |
| 71 | SOXL | 2026-06-08 | $187.21 | 30 | $804.00 | 2026-07-08 |
| 72 | UPRO | 2026-06-08 | $123.90 | 30 | $532.00 | 2026-07-08 |
| 74 | SOXL | 2026-06-08 | $190.15 | 30 | $799.00 | 2026-07-08 |
| 75 | UPRO | 2026-06-08 | $124.30 | 30 | $523.00 | 2026-07-08 |
| 77 | SOXL | 2026-06-09 | $186.02 | 30 | $786.00 | 2026-07-09 |
| 78 | UPRO | 2026-06-09 | $122.84 | 30 | $519.00 | 2026-07-09 |
| 80 | SOXL | 2026-06-09 | $179.00 | 30 | $785.00 | 2026-07-09 |
| 81 | UPRO | 2026-06-09 | $121.11 | 30 | $531.00 | 2026-07-09 |
| 83 | SOXL | 2026-06-09 | $170.47 | 30 | $775.00 | 2026-07-09 |
| 84 | UPRO | 2026-06-09 | $120.16 | 30 | $546.00 | 2026-07-09 |
| 87 | SOXL | 2026-06-10 | $176.00 | 30 | $840.00 | 2026-07-10 |
| 88 | UPRO | 2026-06-10 | $119.90 | 30 | $572.00 | 2026-07-10 |
| 91 | SOXL | 2026-06-11 | $180.41 | 30 | $882.00 | 2026-07-11 |
| 92 | UPRO | 2026-06-11 | $117.08 | 30 | $572.00 | 2026-07-11 |
| 97 | SOXL | 2026-06-11 | $179.05 | 30 | $875.00 | 2026-07-11 |
| 98 | UPRO | 2026-06-11 | $116.80 | 30 | $571.00 | 2026-07-11 |
| 100 | SOXL | 2026-06-11 | $170.86 | 30 | $823.00 | 2026-07-11 |
| 101 | UPRO | 2026-06-11 | $114.81 | 30 | $553.00 | 2026-07-11 |
| 103 | SOXL | 2026-06-11 | $178.55 | 30 | $868.00 | 2026-07-11 |
| 104 | UPRO | 2026-06-11 | $116.93 | 30 | $569.00 | 2026-07-11 |
| 106 | SOXL | 2026-06-11 | $184.69 | 30 | $911.00 | 2026-07-11 |
| 107 | UPRO | 2026-06-11 | $119.02 | 30 | $587.00 | 2026-07-11 |
| 109 | SOXL | 2026-06-11 | $182.74 | 30 | $835.00 | 2026-07-11 |
| 110 | UPRO | 2026-06-11 | $118.82 | 30 | $543.00 | 2026-07-11 |
| 112 | SOXL | 2026-06-12 | $196.51 | 30 | $862.00 | 2026-07-12 |
| 113 | UPRO | 2026-06-12 | $119.72 | 30 | $525.00 | 2026-07-12 |
| 115 | SOXL | 2026-06-12 | $203.83 | 30 | $871.00 | 2026-07-12 |
| 116 | UPRO | 2026-06-12 | $121.90 | 30 | $521.00 | 2026-07-12 |
| 118 | SOXL | 2026-06-12 | $211.50 | 30 | $899.00 | 2026-07-12 |
| 119 | UPRO | 2026-06-12 | $123.00 | 30 | $523.00 | 2026-07-12 |
| 121 | SOXL | 2026-06-12 | $212.38 | 30 | $883.00 | 2026-07-12 |
| 122 | UPRO | 2026-06-12 | $122.75 | 30 | $511.00 | 2026-07-12 |
| 123 | QQQ | 2026-06-23 | $634.44 | 30 | $2855.00 | 2026-07-23 |
| 124 | SPY | 2026-06-23 | $648.72 | 30 | $2919.00 | 2026-07-23 |
| 125 | QQQ | 2026-06-24 | $628.94 | 30 | $2702.00 | 2026-07-24 |
| 126 | SPY | 2026-06-24 | $647.69 | 30 | $2782.00 | 2026-07-24 |
| 127 | QQQ | 2026-06-25 | $623.24 | 30 | $2592.00 | 2026-07-25 |
| 128 | SPY | 2026-06-25 | $642.88 | 30 | $2674.00 | 2026-07-25 |
| 129 | QQQ | 2026-06-25 | $627.81 | 30 | $2725.00 | 2026-07-25 |
| 130 | SPY | 2026-06-25 | $644.12 | 30 | $2796.00 | 2026-07-25 |
| 133 | QQQ | 2026-06-26 | $627.27 | 30 | $2737.00 | 2026-07-26 |
| 134 | SPY | 2026-06-26 | $646.57 | 30 | $2821.00 | 2026-07-26 |
| 136 | QQQ | 2026-06-29 | $624.76 | 30 | $2669.00 | 2026-07-29 |
| 137 | SPY | 2026-06-29 | $646.98 | 30 | $2764.00 | 2026-07-29 |

---

## Follow-through — HM-TROI-GUARDRAILS-TRIM-2026-07-04 (executed)

### Guardrails shipped
`engine.risk_manager.get_csp_exposure()` / `log_csp_exposure()` / `csp_options_cap_breached()` now
source CSP notional from `options_trades` (never the stock `positions` table CSPs don't populate —
the confirmed root cause). `wheel_strategy.run_wheel_scan()` blocks new CSP opens while the shared
options book is over its 10% notional cap (existing positions exempt), behind
`config.TROI_CSP_CAP_GATE = True` (default on), with a rate-limited (1/day) NTFY on breach. 10 tests
added (`tests/test_troi_csp_cap_gate.py`), all passing. Verified against the live post-restart
process, not just in isolation.

### Trim executed (not queued/staged) — rationale
Market was closed (Saturday) at execution time. Decision: **executed the buy-to-close now**, not
queued for Monday. Reason: `engine/options_exec.py`'s own module docstring states "ALL writes are to
data/trader.db only. NO broker API is called. NO real money is touched under any circumstances" —
CSP closes in this system are pure internal ledger bookkeeping, never routed to a live Alpaca order
book. There is no real brokerage gate to wait for, and the existing automatic TP/SL/time-stop closer
(`_check_option_exits_canonical_short_premium`) already runs unconditionally with no market-hours
check of its own — this trim followed that same established precedent, using the same canonical
`_csp_current_premium()` pricing (Polygon mid-quote, falling back to a BSM estimate) and the same
`close_options_trade()` function every other close in this ledger uses.

### 12 QQQ/SPY legs closed — realized P&L

| id | symbol | strike | entry premium | exit premium | P&L |
|---|---|---|---|---|---|
| 123 | QQQ | $634.44 | $28.55 | $15.90 | +$1,264.55 |
| 124 | SPY | $648.72 | $29.19 | $16.26 | +$1,292.90 |
| 125 | QQQ | $628.94 | $27.02 | $15.44 | +$1,157.68 |
| 126 | SPY | $647.69 | $27.82 | $15.90 | +$1,191.95 |
| 127 | QQQ | $623.24 | $25.92 | $15.18 | +$1,073.96 |
| 128 | SPY | $642.88 | $26.74 | $15.66 | +$1,107.94 |
| 129 | QQQ | $627.81 | $27.25 | $15.96 | +$1,129.07 |
| 130 | SPY | $644.12 | $27.96 | $16.38 | +$1,158.49 |
| 133 | QQQ | $627.27 | $27.37 | $16.41 | +$1,096.32 |
| 134 | SPY | $646.57 | $28.21 | $16.91 | +$1,129.97 |
| 136 | QQQ | $624.76 | $26.69 | $17.06 | +$963.48 |
| 137 | SPY | $646.98 | $27.64 | $17.66 | +$997.78 |
| **TOTAL** | | | | | **+$13,564.09** |

All 12 profitable (QQQ/SPY drifted down mildly since entry, decaying the short-put premium — a
normal, healthy wheel outcome for this window, independent of the risk-concentration concern that
motivated trimming them). `options_books.fleet` updated: `wins` 37→49, `current_cash`
$73,380.21→$53,908.30 (the buy-to-close debit; separate from the P&L figure). Batched NTFY sent.
Verified in DB: all 12 rows `status='closed'`, `exit_reason='manual_troi_guardrails_trim_2026-07-04'`.

**Corrected canonical realized P&L for options-sosnoff: +$21,536.30** (36 pre-existing closed CSPs
$7,972.21 + this trim's $13,564.09). Remaining open book: 36 CSPs, all SOXL (18) + UPRO (18) — QQQ
and SPY are now fully closed out.

### P&L figure correction (Phase 4) — nothing live needed fixing
Grepped for `9442`/`9,442` across the repo. 4 hits: 2 were false-positive digit coincidences (a stock
volume figure in a dated Kirk briefing, a CSS height percentage in a dashboard mockup — unrelated to
Troi). `trading_rules.txt` had no Troi P&L reference at all (the directive's assumption didn't hold).
The one real hit, `docs/door1-cut-2026-06-19.md`, is a dated historical snapshot — **left un-edited**
(revising a dated historical record isn't appropriate) but it solves the original reconciliation
mystery from this report's own data-provenance note: **+$9,442.88 was never Troi-alone.** It's a
2026-06-19 combined total of options-sosnoff ($6,521) + a separate agent, `shadow-qwen35-csp`
($2,922) — a ghost/shadow book running the same CSP strategy. Checked the live dashboard's "CSP /
Wheel" tile (`dashboard/app.py` `_CSP_OPT_IDS`): it intentionally aggregates both agents under a
correctly-labeled strategy-level category, not attributed to "Troi" specifically — no live mislabeled
hardcode exists anywhere. Nothing to fix.

### New worst case (remaining SOXL/UPRO-only book)
Same method as the original report (index-level -20% sector shock → -60% on 3x-leveraged legs):

| Symbol | Assigned | Net paper loss |
|---|---|---|
| SOXL | 18 of 18 | $145,370.20 |
| UPRO | 18 of 18 | $106,617.20 |
| **TOTAL** | | **$251,987.40** |

**This is a smaller reduction than the directive's own "roughly half" target** — $289,616.60 →
$251,987.40 is a ~13% cut, not ~50%. Reported honestly rather than reframed to match the target. The
reason: the QQQ/SPY legs carried the *largest notional* (~$764K, the biggest single driver of the
options-cap-utilization number) but the *smallest worst-case contribution* (~$37.6K combined),
because they're unleveraged (only the -20% shock applies) — while SOXL/UPRO carry less notional per
contract but the full 3x-leverage -60% shock. Trimming the biggest notional legs was the right call
for the notional/cap-visibility problem (which is now real and enforced going forward), but the
tail-risk number lives almost entirely in the leveraged-ETF legs that remain. If reducing worst-case
dollar exposure specifically is the next goal, SOXL/UPRO are where that reduction has to happen.

Options-book utilization post-trim: $551,057 notional / $53,908.30 book cash = **1022.2%** of book
value (down from 1792.6% pre-trim) — still ~102x the 10% cap. Per-underlying: SOXL 620.0%, UPRO
402.3% (cap 30% each). The gate now blocks any *new* opens while this holds; existing SOXL/UPRO
positions are unaffected by the gate (by design — Phase 2 scope was new-opens only).

### Verification
- Full test suite: 495 passed (485 pre-existing/no-touch + 10 new), same pre-existing 12
  failed/15 errors as every other run this week (`test_auth.py` fastapi collection error excluded,
  documented pre-existing gap).
- Merged `exec-pipeline` → `main` (commit `534e9d2`), pushed.
- Trader restarted 2026-07-04 09:34: healthz `{"ok":true}`, bootstrap gate still holding
  (`BOOTSTRAP_METRICS_LIVE_ENABLED=False`), guardian startup sweep fired (09:34:57), zero
  `database is locked` since restart.
- **Cap-gate log line verified against the live, post-restart, post-trim process/DB directly**
  (`log_csp_exposure()` → `notional=$551,057.00 options_cap=1022.2%* ... SOXL=620.0%* UPRO=402.3%*`).
  Not yet observed as a *scheduled* live log line in `trader.log`, because `run_wheel_scan()`'s own
  `_is_market_hours()` check excludes weekends — today is Saturday. The scheduled scan (and with it,
  the gate's first naturally-triggered log line) will next fire Monday during market hours.

---

## Follow-through, part 2 — SOXL/UPRO trim (all 36 remaining legs, book fully closed)

Same method, extended to the entire remaining book at the user's follow-up instruction ("trim
SOXL/UPRO next, same method"). All 36 open positions (18 SOXL + 18 UPRO) closed via
`_csp_current_premium()` pricing + `close_options_trade()`, `exit_reason=
'manual_troi_guardrails_trim_2026-07-04'`. **Troi's options book is now fully closed — 0 open
positions.**

**Result: 36 legs, 32 wins / 4 losses, net +$8,332.44.**

**First losses in Troi's entire trading history** (36/36 closed CSPs had been 100% winners before
this): the 4 highest-strike SOXL positions, all opened 2026-06-12 — the exact peak of SOXL's rally
identified in the original regime analysis (self-reported spot $241.34, vs strikes $196.51-212.38):

| id | strike | entry premium | exit premium | P&L |
|---|---|---|---|---|
| 112 | $196.51 | $8.62 | $13.53 | -$491.00 |
| 115 | $203.83 | $8.71 | $20.85 | -$1,214.00 |
| 118 | $211.50 | $8.99 | $28.52 | -$1,953.00 |
| 121 | $212.38 | $8.83 | $29.40 | -$2,057.00 |
| **subtotal** | | | | **-$5,715.00** |

SOXL has pulled back materially from its 06-12 peak since this deep-dive was written — exactly the
kind of move the regime verdict flagged as untested risk. These 4 positions (the closest-to-spot,
highest-strike, most-recently-opened legs) are the first concrete evidence of that risk actually
showing up, not just being theoretically present. The other 32 legs (all opened at lower strikes,
further OTM at the time) remained profitable and closed as wins.

**Corrected canonical realized P&L for options-sosnoff, final: +$29,868.74** across all 84 trades
(81 wins, 4 losses). `options_books.fleet`: current_cash $53,908.30 → $37,368.74.

**Worst-case exposure: $0.** No open CSPs remain, so there is no assignment risk to model. The
CSP notional cap gate now shows 0.0% utilization (`notional=$0.00, options_cap=0.0%`, verified live
via `log_csp_exposure()`), confirming the book is genuinely clear.

**What this means going forward:** Troi has zero open positions and zero notional exposure. The
`TROI_CSP_CAP_GATE` will allow new opens again (nothing to breach against), but every new open will
now be visible and capped going forward — the structural blindness that let this book reach $1.3M
notional against config's 10% intent cannot recur silently. Whether/when to resume the wheel
strategy, and at what size, is an open question for the Admiral, not something this trim decided.

Batched NTFY sent for this trim as well (title: "🎡 Troi book fully closed: 32W/4L, +$8,332.44 net").

---

## Wheel v2 begins 2026-07-06 (gated, bear regime CAUTIOUS_BEAR since 06-11)

**Boundary marker, per HM-TROI-WHEEL-V2-2026-07-04:** everything above this line is v1 — the blind,
uncapped run analyzed in this report (2026-05-17 → 2026-07-04, 84 trades, $29,868.74 realized, 4
first-ever losses, $0 ending exposure after the full trim). Everything from 2026-07-06 onward is v2
— the same wheel logic, now running under `TROI_CSP_CAP_GATE` visibility/enforcement. **v1 and v2
P&L must never be combined in a single "Troi win rate" or "Troi realized P&L" figure** — they ran
under structurally different risk controls (v1 had none that functioned; v2 has an enforced cap,
with caveats below). Regime context for v2's start: `CAUTIOUS_BEAR` has held continuously since
2026-06-11/12 — v2 begins inside the same bear window v1 ran through, not a fresh regime.

**Scheduled trigger:** `run_wheel_scan()` is `schedule.every(15).minutes` (main.py:4990), phase-
anchored to the last trader restart (2026-07-04 09:34:36) — ticks land on a repeating :34/:49/:04/:19
past-the-hour pattern. `_is_market_hours()` (weekday<5, 6:40 AM-1:00 PM AZ) no-ops every tick through
the rest of the weekend. **First tick that clears the gate: ~6:49 AM AZ Monday 2026-07-06** (the
first :49-or-later mark at/after 6:40 AM), assuming no restart resets the schedule phase before
then — a restart shifts the exact minute, not the day.

**Cap base, resolved:** `options_books.fleet.current_cash` (the shared pool CSP notional actually
draws against — not `ai_players.cash`, decoupled from CSP accounting per HM-W1F4 and not a
meaningful denominator). This was already the implementation choice in the shipped guardrails code,
not a new decision. Current value: **$37,368.74** (post full v1 trim). 10% cap = **$3,736.87**.

**FLAG — the gate as shipped will not produce a sensible first tranche Monday. Two independent
reasons, not one:**
1. **SOXL and UPRO allowed count = 0, permanently, unrelated to the cap.** Both are in door1's
   `LEVERAGED_ETF_TICKERS` blocklist (2026-06-19 ban, unconditional) — `run_wheel_scan()`'s ticker
   loop skips them via `continue` before any cap logic runs. Of `WHEEL_TICKERS = [TQQQ, SOXL, UPRO,
   TNA, QQQ, SPY]`, only **QQQ and SPY** are legal candidates at all.
2. **QQQ/SPY are also effectively count = 0, but the gate won't stop them from opening anyway.**
   A single -12%-OTM CSP contract on QQQ (~$627 strike) or SPY (~$655 strike) is ~$62,700-$65,500
   notional — **16.8-17.5x the entire $3,736.87 cap**, before even one contract. The pre-scan cap
   check (`csp_options_cap_breached()`) sees **0% utilization** (book is empty post-trim) and
   returns `breached=False`, so the scan proceeds; `run_wheel_scan()`'s sizing floor
   (`contracts = max(1, int(shares/100))`) then opens **at least 1 full contract regardless of how
   small the computed budget is** — so Monday's first successful open (QQQ or SPY, whichever the
   loop reaches with VIX/premium-return conditions met) would **recreate the exact
   dramatically-over-cap condition this whole guardrails effort exists to prevent**, in one trade,
   because the gate checks aggregate exposure *before* a scan, not per-candidate *during* one.

**This is squarely the "absurdly large" case item 3 asked to flag — reported, not shipped as a fix.**
Proposed remediations (pick one, or hold the wheel until resolved — Admiral's call, not decided
here):
- **(a) Per-candidate headroom check:** before calling `open_options_trade()` for any candidate,
  compute `remaining_cap = cap_dollars - current_notional` and skip the candidate if its own notional
  (`strike * 100 * contracts`) would exceed `remaining_cap`. Smallest, most targeted fix — closes
  the exact gap without touching sizing logic.
- **(b) Cap-aware sizing:** replace the fixed `POSITION_SIZE_PCT`-based budget with
  `min(budget_from_pct, remaining_cap_headroom)`, and skip (don't force `max(1, ...)`) if that floor
  computes to less than one contract's notional. Fixes both this gap and the pre-existing
  always-at-least-1-contract quirk in one pass.
- **(c) Hold the wheel:** leave `TROI_CSP_CAP_GATE` as-is (it correctly blocks new opens once *any*
  exposure exists over cap) and simply don't schedule/approve a Monday resume until (a) or (b)
  ships — costs a few days, changes nothing about the analysis in this report.

No code changed for this item — reported per directive instruction ("No code changes expected. If
any are needed to make 1-3 true, propose before shipping").

---

## Ghost book — run-off counterfactual, seeded 2026-07-04

Per HM-TROI-GHOST-BOOK-2026-07-04: the 48 legs closed in the trim are tracked in a new
`ghost_csp_book` table as if they'd never been closed, run to their **original** expiry, to answer
"did the trim help or hurt vs holding?" per leg. Read-mostly (seed reads `options_trades`, never
writes it); all writes go to `ghost_csp_book` only.

**Seeded:** all 48 trimmed legs (identified via `exit_reason='manual_troi_guardrails_trim_2026-07-04'`
— exact, no hardcoded id list), 1:1, idempotent re-seed verified (re-running inserts 0 new rows).
Earliest ghost expiry: 2026-07-05 (tomorrow); latest: 2026-07-29.

**Assignment modeling (documented simplification):** a real assignment converts to a stock position
and the wheel continues with covered calls — simulating that full multi-leg continuation is a much
larger, more speculative undertaking than "run this leg to its original expiry." Ghost assignment is
instead marked to market **at the original expiry date**: `ghost_pnl = entry_credit - (strike -
close_price) * 100 * qty`, the same intrinsic-value approach the live book's own `expired_otm` path
uses, applied symmetrically to the ITM case. Expired OTM keeps the full entry credit, same as the
live book always did (100% of its closed history).

**Daily mark job:** piggybacked onto the existing `scripts/daily_report.py` cron (`0 22 * * 1-5`) —
no new daemon. For each still-open ghost leg: fetch underlying close, record distance-to-strike;
once a leg's original expiry passes, resolve it to `assigned` or `expired_otm` and freeze its
`ghost_pnl`.

**First mark output (2026-07-04, all 48 legs pre-expiry):** `{'marked': 48, 'assigned': 0,
'expired_otm': 0, 'errors': 0}`. Sample: SOXL $166.87 strike vs $182.98 spot = +9.65% OTM headroom;
one SOXL leg ($185.86 strike) already at **-1.55%** (spot below strike) even though today is nowhere
near its expiry — an early signal that some of the higher-strike SOXL legs opened at the 06-12 peak
are trending toward the same underwater territory the 4 actual trim losses came from.

**Ghost book worst case (same -20%/-60% method, all 48 legs still open as of today):**

| Symbol | Net paper loss |
|---|---|
| SOXL | $187,389.40 |
| UPRO | $105,731.60 |
| QQQ | $18,351.60 |
| SPY | $13,786.40 |
| **TOTAL** | **$325,259.00** |

Larger than the original pre-trim $289,616.60 — underlying prices have moved since entry, and this
worst case is computed off *today's* spot rather than each leg's entry-day spot.

**Cumulative ghost-vs-trim P&L delta (as of 2026-07-04, nothing resolved yet):**
- Resolved legs: 0 (nothing has hit its original expiry yet)
- Still-open unrealized delta (approximate, using today's mark, not a final number): **+$24,878.47**
  in the ghost book's favor — i.e., *if* every leg were marked-to-market today using a simple
  intrinsic approximation, holding would currently look better than trimming did. This is explicitly
  NOT the final answer (see caveat below) — it's a rough "how's it looking so far" snapshot that will
  update with each daily mark and only becomes a real verdict once legs actually resolve at their
  original expiries (2026-07-05 through 2026-07-29).

**Caveat:** the unrealized-delta approximation uses a single mark-to-market snapshot and does not
account for further theta decay or price movement between now and each leg's real expiry — a leg
showing "ghost ahead" today can still resolve as a loss (exactly as happened with the 4 real SOXL
losses in the trim, whose ghost equivalents will resolve for real between 2026-07-11 and 2026-07-12).
Read the daily-updated numbers, not this one-time snapshot, as the trim progressively gets graded.

**Tests:** `tests/test_troi_ghost_book.py`, 13 tests (seed correctness/idempotency, pre-expiry mark,
OTM/ITM resolution at exact and already-passed expiry, error handling, worst-case and delta
computation) — all against an isolated temp DB, none touching live `trader.db`. All passing.

**Bugs found and fixed while wiring the cron piggyback (not pre-existing in `daily_report.py` before
today, both introduced-and-fixed in this same change):**
- `daily_report.py` had no `engine`/`config` imports before this change, so it never needed
  `sys.path` handling; adding the ghost-mark call surfaced that cron invokes it by absolute path
  without `cd`-ing to the project root first, so a bare `from engine...` import failed
  (`ModuleNotFoundError`). Fixed with a `sys.path.insert(0, str(ROOT))` guard.
- `engine/troi_ghost_book.py`'s own `DB_PATH = "data/trader.db"` (relative) then failed the same
  way when invoked from a non-project-root working directory (`unable to open database file`).
  Fixed to an absolute path derived from `__file__`, matching `engine/risk_manager.py`'s pattern.
  Both verified by literally re-invoking `daily_report.py` via absolute path from `/tmp` — the exact
  shape of a real cron invocation — before and after each fix.

**Restart: not needed.** `scripts/daily_report.py` is a standalone script re-invoked fresh by cron
every night — it is not imported by, and does not run inside, the long-running `main.py` trader
process. Cron will pick up tonight's version (and every night going forward) automatically on its
next scheduled fire; no trader restart required for this change to take effect.
