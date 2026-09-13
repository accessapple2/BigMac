# Relay — gate efficacy funnel, market-closed schedule audit, neo-matrix trace + halt-gate gap. 2026-09-13

Read-only investigation per RULE #1 as stated for this task: nothing
deleted, nothing rewritten, no code changes — **with one explicit,
narrow exception the Admiral authorized mid-turn**: a stale one-line
doctring correction in `engine/crew_scanner.py` (see Q4 addendum). No
other file was touched. No restarts, no gate changes, no DB writes.

## Q1 — Are the gates rejecting losers or winners?

**Method**: every `decision_audit` row with `event_type='gate_reject'`
joined to `signals` where `signal IN ('BUY','BUY_CALL')`, last 90 days
(2026-06-15 → 2026-09-11). 19,894 total rejected BUY-family signals,
692 unique symbols. `reference_price` populated on only 14% of rows (same
gap the 2026-09-11 analysis hit), so forward returns were computed the
same way that analysis did: batch-fetched daily closes from yfinance,
anchor = last close at/before the signal's timestamp, forward = close 1
and 5 trading days later. 18,424 of the 19,894 (92.6%) fall into the 7
requested buckets by `gate_verdict` text match; the remaining 1,470
(7.4%) are smaller reasons outside the requested list (Bridge Voter,
MAX_POSITIONS_REACHED, position-size limits, direction guard, daily
trade limit, execution failures, GEX-wall rejections) and are not
included in the table below — flagging for completeness, not omitted
silently.

**SPY baseline, same 90-day window, same anchor/forward methodology, one
observation per unique signal-date (43 unique dates, not one per
signal — avoids reweighting toward days with more rejected signals):**

| | n | mean | median | win rate |
|---|---|---|---|---|
| SPY 1-day | 42 | -0.056% | -0.194% | 35.7% |
| SPY 5-day | 37 | -0.246% | -0.281% | 37.8% |

This window was a net-negative, choppy stretch for SPY itself (worth
keeping in mind — a category beating this baseline is not the same as
that category making money in absolute terms).

**By rejection reason:**

| Category | n rejected (total) | n resolved (r1) | 1d mean | 1d median | 1d WR | n resolved (r5) | 5d mean | 5d median | 5d WR |
|---|---|---|---|---|---|---|---|---|---|
| `stale_signal` | 4,473 | 4,256 | +0.157% | -0.016% | 49.7% | 2,637 | -0.102% | -0.485% | 45.1% |
| `market_closed` (all `[HM-MARKET-CLOSED]` variants) | 6,576 | 5,994 | -0.007% | -0.038% | 48.9% | 4,255 | -0.884% | -0.881% | 40.0% |
| `Blocked by learning engine` | 2,178 | 2,129 | **-0.648%** | **-0.703%** | **35.0%** | 1,088 | **-3.065%** | **-2.830%** | **27.0%** |
| `LOW_CONVICTION: *` | 3,263 | 2,810 | +0.021% | +0.004% | 50.1% | 1,572 | -1.099% | -0.662% | 42.5% |
| `BENCH: rating D` | 1,144 | 1,120 | -0.101% | -0.116% | 47.0% | 1,120 | -0.566% | -0.429% | 45.2% |
| `REGIME-ROUTER: * avoid-list` | 790 | 773 | **+0.187%** | **+0.199%** | **54.7%** | 260 | **-2.010%** | **-1.730%** | **27.7%** |
| `regime_mismatch` (exact verdict) | 0 | — | — | — | — | — | — | — | — |

**`regime_mismatch` doesn't occur among rejected BUYs at all in this
window** — 0 rows. I didn't trace whether this verdict is dead code for
the BUY path specifically or only ever fires on SELL/SHORT direction
checks; flagging as an open question rather than asserting either.

**Verdict, per gate:**

- **`Blocked by learning engine` — clearly and unambiguously earning
  its keep.** Worst-performing rejected population by a wide margin on
  every metric, both horizons. 5-day mean is 12x worse than SPY's
  same-period mean. Not rejecting winners — the opposite.
- **`LOW_CONVICTION` — earning its keep, confirms the 2026-09-11
  McCoy-only finding at 10x the scale.** Coin-flip at 1-day (50.1% WR,
  near-zero mean/median), decays to a modest loser at 5-day, worse than
  SPY on 3 of 4 five-day metrics. Not a suppressed-winner population.
- **`BENCH: rating D` — earning its keep**, modest loser on mean/median
  at both horizons, no evidence of suppressed winners.
- **`stale_signal` — neutral/mixed, not a clear case either way.** Beats
  SPY's (unusually weak, this window) win rate and mean at both
  horizons, but its own 1-day win rate is still just under 50% and it
  turns into a small loser by day 5. Reads as noise, not alpha.
- **`market_closed` — not a judgment gate** (there's no discretion —
  the market is provably shut, no alternative action exists), so
  "earning its keep" doesn't really apply. For what it's worth the
  underlying signal quality is mediocre-to-poor, especially at 5 days.
- **`REGIME-ROUTER` avoid-list — the one finding that looked, at first
  glance, like a gate rejecting real winners, and isn't once you look
  past 1 day.** At 1-day this is the single best-performing rejected
  population in the whole set: mean +0.187% vs SPY -0.056% (+24bps
  edge), median +0.199% vs SPY -0.194% (opposite sign), win rate 54.7%
  vs SPY 35.7% (+19pts) — the only category clearing 50%. **By day 5 the
  same population reverses hard**: win rate collapses to 27.7% (worse
  than SPY), mean -2.010% (8x worse than SPY), median -1.730%. This is
  the signature of a bear-regime dead-cat bounce — a 1-day pop that
  gives it all back and more by the time a SWING-timeframe system would
  actually be evaluating the trade. **Read correctly at the 5-day
  horizon this system actually trades on, REGIME-ROUTER is not
  rejecting winners — it's one of the better-targeted gates here.**
  Caveat: the 5-day sample (n=260) is much thinner than the 1-day
  sample (n=773), drawn from a narrower, more recent slice of the
  window — didn't confirm whether that's pure data-availability lag or
  something else.

**Overall answer to "which gates are rejecting money":** none of the six
populated categories show both a win rate meaningfully above 50% *and* a
positive median *and* persistence to 5 days — the standard the
2026-09-11 analysis itself set for "a systematically profitable
population being wrongly suppressed." That finding holds at 20x the
sample size and fleet-wide, not just for McCoy. **No gate examined here
should be loosened on this evidence.** If anything, `Blocked by learning
engine` and `REGIME-ROUTER` are the two strongest-performing gates in
the entire funnel once judged on the outcomes that actually matter.

**Counterfactual** (equal-weighted, one unit per rejected trade, simple
returns, not compounded, not risk-adjusted — a directional read, not a
backtest): summed 1-day returns are positive for `stale_signal` (+667%
notional across 4,256 trades, i.e. ~+0.16% average) and `LOW_CONVICTION`
(+58%, ~+0.02% average) and `REGIME-ROUTER` (+144%, ~+0.19% average);
negative for `learning_engine` (-1,381%, ~-0.65% average) and
`bench_rating` (-113%, ~-0.10% average) and `market_closed` (-39%). At
5-day, every single category's aggregate mean turns negative, including
the three that looked positive at 1-day. **Taking every rejected BUY in
every category would have net lost money by day 5 in aggregate** — full
numbers in the JSON artifact (not committed, scratch file
`/tmp/hm_rejected_buy_forward_return_result.json`, reproducible from
this doc's methodology).

## Q2 — Why do decisions fire outside market hours?

**Confirmed: already fixed for the dominant contributor, one open
unfixed instance remains.**

The fleet's main tiered scanner (`main.py::run_scanner`, `schedule.every(2).minutes`)
sweeps three tiers on elapsed-time cadences via `_get_scan_interval()`
(`main.py:368`) that **never actually stop** — 5-min pre-market/market,
10-min after-hours, 30-min evening/overnight, 60-min weekends, by design
("Dilithium Crystal Protocol" pre-market intelligence). Agents in these
tiers generate real BUY/SELL decisions on that cadence regardless of
session; those decisions get correctly blocked at the order layer
(`engine/paper_trader.py`'s `market_closed_reason` gate) rather than
ever reaching Alpaca — so nothing has misfired, but LLM cycles were
spent on decisions that could never execute.

**Per-tier breakdown:**

| Tier | Members | Interval | Market-hours aware? | Status |
|---|---|---|---|---|
| Tier 1 | `mlx-qwen3` only | 30 min | No (elapsed-time only) | Decommissioned agent (confirmed dead 2026-09-12) — ~0 live contribution today |
| Tier 2 | `ollama-qwen3` (Scotty) only, since 2026-09-11 | 2 hours | **No (elapsed-time only)** | **Live, unfixed** — 262 of 4,594 market-closed hits (5.7%) over the 30-day window this investigation started from |
| Tier 3 | 10 benched agents | 4-hour min gap, but gated to two narrow windows via `_tier3_window_open()` (`main.py:359`, 6:30-7:00 AM MST + 12:45-1:30 PM MST) | **Yes** | Correctly designed, not a contributor |

**McCoy (`ollama-plutus`) was Tier 2's other member until 2026-09-11**
(`main.py:302`, `HM-XO-PLAN-2026-09` Phase 1.2) — pulled off elapsed-time
scanning (~2,095 signal-emits/day measured, ~10x its own target) and
replaced with `run_mccoy_screened_scan()` (`main.py:3206`), two
deterministic slots at 9:35 AM / 12:30 PM ET, weekdays only. Verified
live: McCoy's market-closed hits ran 138-798/day through 09-11, then
**zero every day since**. That accounts for 96% of the original 4,594
figure and is closed, not open.

**Scotty (`ollama-qwen3`) is now the sole Tier 2 member, running the
identical architecture McCoy had before the fix — same elapsed-time
2-hour cadence, same full 600-900-symbol active universe, same missing
market-hours gate.** This is real, live, unaddressed waste on a smaller
scale (262/30d vs McCoy's 4,404/30d, roughly matching the difference in
how often each agent's stated confidence clears the bar to attempt a
buy() call outside hours — not investigated further).

**Cost**: each Tier-2 firing invokes Ollama inference against the full
active universe; McCoy's identical old setup measured ~175 signal-emits
per firing. At a 2-hour cadence there are 12 firings/day; the regular
session is 6.5 of 24 hours, so roughly 9 of 12 firings/day land outside
market hours — a comparable majority of that inference spend produces
decisions that can only ever end in a `market_closed` reject, never a
live paper trade. **Distortion**: every fleet-wide funnel metric built
from `decision_audit` (including every one built this week) inherits
this — a fixed ~5.7%-and-shrinking slice of "rejected decisions" is
structurally un-actionable from the moment it's generated, inflating
funnel size without reflecting anything about signal quality.

**Proposed fix (not built, per instruction)**: mirror the McCoy fix —
either (a) pull `ollama-qwen3` off elapsed-time Tier 2 onto its own
deterministic market-hours-only slots, same pattern as
`run_mccoy_screened_scan()`, or (b) if Tier 2's actual intent is
pre-market/after-hours *intelligence-gathering* rather than live
trading, gate the intent explicitly — mark those off-hours decisions
observation-only (don't attempt `buy()`/`sell()` for them at all) rather
than generating and then rejecting live-executable-looking signals. (a)
is the smaller change and mirrors a fix already proven to work; (b) is
more invasive but might be the more honest fix if off-hours scanning is
supposed to inform, not attempt to trade. Not deciding between them here.

## Q3 — What is neo-matrix, and why does it trade 5x McCoy?

**neo-matrix ("Neo")**: `provider='matrix'`, `model_id='8000 /
Independent'` — a deterministic rule-based momentum strategy, not an
LLM. Named the fleet's momentum-cluster owner in `CLAUDE.md`'s
Duplicate Role Policy (Neo/Chekov/Navigator consolidated to Neo).

**Its buy path shares zero infrastructure with McCoy's.** McCoy's
25,440 (30d) decisions all flow through `arena.run_scan()` →
`decision_audit`. neo-matrix's BUY path
(`engine/crew_scanner.py::_hm_an2_consume_signal_center`, line 2499)
consumes Signal Center's own momentum-bridge feed directly and calls
`paper_trader.buy()` — same terminal executor as everyone else, reached
via a completely separate, decision_audit-free trigger. **Zero
`decision_audit` rows for neo-matrix in 30 days; its last one ever is
2026-07-13** — the day it was re-halted.

**Current state: `halt_mode='full'`.** All 22 of its trades this month
are SELL, on exactly HL and AG (the two symbols named in its own 07-13
halt note), fractional sizes, clustered entirely in a 3-day burst
(Aug 25-27), nothing since — see Q4 addendum below for the full trace
of how that happened while fully halted. Realized P&L on those 22
trades: **+$26.35, 22/22 "win rate"** — but this is close to tautological,
not a skill signal: the mechanism that fired them (`_check_scaled_exits`,
see Q4) only sells when a position is already up 3%/5%/8%, so every exit
it produces is a winner by construction, independent of neo-matrix's own
decision quality.

**m5-allocator ("M-5 Multitronic")**: `provider='rule-based'`,
`model_id='regime-rules-v1'`, `halt_mode='active'`, `crew_role='baseline'`
— a deterministic SPY/AGG regime-rotation allocator, evidently used as
the fleet's benchmark rather than a competing strategy. 10 decisions/30d
(1 gated, `regime_mismatch`), 11 trades — small periodic SPY/AGG
rebalancing buys plus two profitable SELLs (+$17.49, +$23.37, realized
+$40.86 total, 2/2 settled trades won). `execution_type='simulated'`
throughout — no real Alpaca fill for any of its trades either.

**ollie-auto ("Ollie")**: `provider='holly'`, `halt_mode='exit_only'`
since 2026-07-17 (`HM-PG-ESCALATION`: Proving Ground kill-warning
unacknowledged 10 days, auto-halted new entries pending a ship/kill
decision). All 5 trades this month are SELL, all realized-positive
(+$10.71, +$2.67, +$4.39, +$1.20, +$3.44 = **+$22.41, 5/5 win**), which
is exactly what `exit_only` is supposed to permit — **this is the
correctly-working version of the mechanism that's broken for
neo-matrix.** Confirmed via `paper_trader.sell()`'s own halt-gate
comment: "exit_only PERMITS sells, only 'full' blocks." 2 of the 5 trades
have no matching `decision_audit` row either (same missing-instrumentation
pattern as neo-matrix, smaller in magnitude) — not chased further.

**Ranking, 5 trading seats, last 30 days:**

| Seat | Decisions | Trades | Decisions/trade | Realized P&L | Win rate (settled) | Gate chain |
|---|---|---|---|---|---|---|
| ollama-plutus (McCoy) | 25,440 | 4 (all open, none settled) | 6,360 | $0 | n/a — nothing settled | LLM fleet, `arena.run_scan`/`decision_audit` |
| neo-matrix | 0 | 22 | ~0 (artifact of no logging) | +$26.35 | 100% (tautological — profit-take-only exits) | Own — Signal Center feed / scaled-exit sweep, **currently exploiting a halt-gate gap** |
| m5-allocator | 10 | 11 (9 open, 2 settled) | 0.9 | +$40.86 | 100% (n=2) | Own — deterministic regime-rules, `crew_role='baseline'` |
| ollie-auto | 2 | 5 (all closed) | 0.4 | +$22.41 | 100% (n=5) | Own — legitimately-gated `exit_only` wind-down |
| desk-manual | 2 | 2 (both open) | 1.0 | $0 | n/a | Human, `is_human=1` |

**Which seat is "carrying the fleet"**: by realized dollars this month,
m5-allocator narrowly leads ($40.86) over neo-matrix ($26.35, under an
active halt-gate bug) over ollie-auto ($22.41, a legitimate wind-down).
McCoy — the seat generating 99.98% of all fleet decision volume and the
one the system is nominally built around — **produced zero settled
outcomes this period**; its 4 stock BUYs are all still open, and even
those are `execution_type='simulated'`, not real Alpaca fills (a
separate, still-open gap from the options-specific real-fill work
shipped 2026-09-12 — that work didn't touch McCoy's stock-BUY path at
all). **None of the four comparison seats share McCoy's gate chain.**
Every one of them is either a fully separate deterministic/rule-based
system (neo-matrix, m5-allocator), a distinct provider under its own
halt tier (ollie-auto), or a literal human (desk-manual). The fleet's
actual trade count this month is being carried almost entirely by
non-LLM, non-McCoy-pattern paths — worth knowing before any conversation
about loosening McCoy's own gates, since "the fleet trades 44 times a
month" and "McCoy's gates are too tight" are not really describing the
same system.

## Q4 addendum (mid-turn) — the neo-matrix halt-gate trace

**1. Which code path executed the 22 sells while `halt_mode='full'`?
Traced to the call site.**

`main.py:5388` `schedule.every(2).minutes.do(run_crew_scanner_job)` →
`run_crew_scanner_job()` (`main.py:3980`, gated to
`RiskManager.is_market_hours()` and weekdays — correctly market-hours-only) →
`run_scan_cycle` → `_run_scan_cycle_inner` → `_run_scan_cycle_body`
(`engine/crew_scanner.py:4177`) → line 4219:
`scaled_exits = _check_scaled_exits(volatile_day=volatile_day)` →
`_check_scaled_exits()` (`engine/crew_scanner.py:2708`) iterates
`_SCALED_EXIT_TIERS` (line 2056 — `{"neo-matrix": [...], "deepseek-7b-grok4":
[...], "ollama-qwen3": [...]}`), reads each member's open positions via
`get_portfolio(player_id)`, computes `pnl_pct`, and when a tier threshold
(3%/5%/8% for neo-matrix) is met calls `paper_trader.sell_partial()`
(line 2751) directly — **with no `ai_players.halt_mode` lookup anywhere
in `_check_scaled_exits()`, and none inside `sell_partial()`
(`engine/paper_trader.py:2237`) either.** This matches the trades
exactly: `reasoning` text on all 22 rows literally reads "Scaled exit
T1/T2/T3: +NN.N% — selling NN% (N.NN sh)", real distinct
`alpaca_order_id` per trade (individually submitted, not a broker-side
bracket order).

**2. Does 'full' halt block position-closing exits by design, or is
that a gap? Read against the codebase's own stated doctrine.**

**It's a gap, not an ambiguous doctrine question** — the system has
already, elsewhere, implemented and documented the opposite intent for
this exact scenario. `paper_trader.sell()` (`engine/paper_trader.py:1969`)
carries an explicit halt gate whose own comment states the design plainly:
*"halt_mode-aware; exit_only PERMITS sells, only 'full' blocks."*
`paper_trader.short_sell()` has the identical gate, added specifically
to close a prior instance of this same bug class — its own comment
reads *"CLOSED GAP: short_sell() had NO halt_mode check at all before
this."* `engine/guardian_sweep.py`'s docstring independently confirms
the same tiering from the other direction: it exists to sweep out
`halt_mode='exit_only'` players' open positions — i.e., `exit_only` is
specifically the tier designed to permit/require exits, which only
makes sense if `full` is the harder stop that blocks everything,
exits included. **`sell_partial()` and `close_options_trade()`
(`engine/options_exec.py:157`, used by both canonical option-exit
checkers) have no halt_mode check of any kind** — neither the
market-closed gate nor the halt gate that `sell()`/`short_sell()` both
carry. This is the same bug class as the one already found and fixed
once for `short_sell()`, just never generalized to the other two
position-closing functions.

**3. How many other halted seats could still fire? Every `halt_mode='full'`
player checked against every path that can reach an order.**

71 players are currently `halt_mode='full'`. Two exposure paths checked:

- **Stock scaled-exit gap** (`_SCALED_EXIT_TIERS`, 3 members total: `neo-matrix`,
  `deepseek-7b-grok4`, `ollama-qwen3`) — **all three are currently
  `halt_mode='full'`.** Only `neo-matrix` currently holds a nonzero open
  stock position (residual 0.0478 sh AG, 0.0613 sh HL — the ~15% tail
  left after its T1/T2/T3 tiers already fired in August), so it's the
  only one with *live* exposure right now; `deepseek-7b-grok4` and
  `ollama-qwen3` share the identical code gap but have zero open
  positions to act on at present — latent, not firing.
- **Canonical options-close gap** (`close_options_trade`, no player
  filter at all in its query — `SELECT ... FROM options_trades WHERE
  status='open' AND structure IN (...)`, no join to `ai_players`) —
  checked every currently-open row: all three open canonical
  options_trades belong to `swingdesk-manual`, which is **not**
  `halt_mode='full'`. So this gap is real at the code level (same
  missing check) but has zero live exposure right now — no halted
  player currently holds a matching open option position.

**Net finding**: the gap is real and currently live for exactly one
seat (`neo-matrix`, small residual stock position), latent for two more
(`deepseek-7b-grok4`, `ollama-qwen3`, zero open positions right now), and
latent at the code level for the options-close path with zero current
exposure. Not fixed — flagged, per instruction, and kept moving rather
than patched.

**Comment fix (the one authorized code change this session)**: the
stale docstring at `engine/crew_scanner.py:2511` asserted "neo-matrix is
halt_mode='ACTIVE'" as a settled 2026-05-31 fact — six weeks stale by
07-13 and still wrong as of today. Corrected in place to state the
current status is `halt_mode='full'` (re-halted 07-13), explain that the
BUY path this docstring describes is therefore now correctly blocked at
`paper_trader.buy()`'s own halt gate, and added an explicit instruction
not to hardcode a player's halt state in a comment again — check
`ai_players.halt_mode` live. `py_compile` verified clean. No other file
touched.

## What this doesn't answer (flagged, not chased further this pass)

- Exact root cause of why `sell_partial()`/`close_options_trade()` never
  got the halt-gate treatment `sell()`/`short_sell()` did — whether it
  was an oversight at the time `short_sell()`'s gap was closed, or these
  two functions simply didn't exist yet then. Not traced.
- Whether `regime_mismatch` is dead code for the BUY path or structurally
  can't apply to BUY signals by design.
- Why 2 of ollie-auto's 5 trades and all of neo-matrix's 22 have no
  matching `decision_audit` row — same class of missing instrumentation,
  different mechanism per seat, not chased to a common root cause.
- Whether McCoy's stock-BUY path (`execution_type='simulated'`, zero
  real Alpaca fills) needs the same real-fill treatment the options
  strategies got 2026-09-12 — flagged as a real, open gap, not scoped
  or built here.
