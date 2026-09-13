# Relay — 2026-09-12. Options real-fill scoping — read-only, no changes.

Follow-on to the restatement work: the finding that one real premium-priced
outcome exists in this system's entire options history prompted the
question of root cause. Traced end-to-end, code read on both sides, no
code changed.

## 1. How options orders execute today — traced, code shown

**Split by strategy — this is not one uniform answer.**

**`options-sosnoff` (Troi, CSP/wheel) and `shadow-qwen35-csp`: pure
internal simulation, no broker order, ever.** `engine/wheel_strategy.py`
and `engine/shadow_csp.py` contain zero calls to any Alpaca order-submit
function — confirmed by grep, not inferred. Live proof from the DB: **0 of
84** `options-sosnoff` rows and **0 of 6** `shadow-qwen35-csp` rows carry a
`broker_order_id`. Pre-2026-07-07, `entry_price` came from the known-buggy
`price * min(0.08, vix/500)` formula; post-07-07, `engine/options_pricing.
py::get_real_csp_premium()` (wired 07-07, P0-A) fetches a real **live
quote midpoint** — `(bid_price + ask_price) / 2` from Alpaca's options
data API — which is a genuine improvement (a real market price at decision
time) but still not an order or a fill. Neither strategy has recorded a
single new `options_trades` row since **2026-06-29** (options-sosnoff
VIX-gated dormant since 07-02, already known/documented) — so this "real
quote" code path appears to have never actually been exercised on a
completed trade yet.

**`engine/battle_station_0dte.py` (0DTE): same pattern.** `_get_option_
price()` calls `alpaca_options.get_atm_contract()` + `_get_contract_price()`
— a quote fetch — then `_open_trade()` just `INSERT`s the row with that
quoted price. No `submit_single_option`/`submit_vertical_spread` call
anywhere in the file. (Moot in practice today — this agent has 2 trades
ever, both from a hard-gated 6:45-11:30am AZ window, per this morning's
trace — but the mechanism itself is simulation, same as CSP.)

**`strategy:bull_spread_v1` (via `strategies/executor.py`): a REAL Alpaca
paper order is placed.** `_execute_live()` calls `engine.alpaca_options.
submit_vertical_spread()`, which calls `client.submit_order(MarketOrder
Request(..., order_class=OrderClass.MLEG, legs=[...]))` — genuinely
submits a multi-leg vertical spread to Alpaca's paper trading engine and
gets back a real `order.id`. Live proof: **15 of 26** rows carry a
`broker_order_id`. **`swingdesk-manual` (Desk SEND-IT): 8 of 8** rows have
one — every manual sortie went through a real order.

**The actual root cause, precisely**: for the one strategy that *does*
place a real order, `strategies/executor.py::_record_options_trade()`
writes `entry_credit_debit = net_credit - net_debit` **from the signal's
own pre-trade payload** (the quoted price computed before submission) —
never from anything Alpaca's order response contains. `submit_vertical_
spread()`'s return value is `{"success": True, "order_id": str(order.id),
"strategy": ..., "qty": ...}` — `order.filled_avg_price` exists on the
Alpaca SDK's order object (confirmed below) and is never read, never
returned, never recorded. **A real order is placed and a real fill
happens; the fill price is simply never looked at.** This is why the
restatement found small gaps (id 28: $1.82 booked vs. $1.71 reconstructed
from a real historical bar, an 11-cent gap the 09-11 doc attributed to
"hourly-bar timing noise") — the booked number was never the fill to begin
with.

## 2. What it would take to route through Alpaca paper for real fills

**Small for one strategy family, moderate for the other two — and the
hard part is already built and proven, just not wired to options.**

Equities already do exactly this, today, live: `engine/paper_trader.py::
buy()` calls `_forward_to_alpaca()` → `alpaca_bridge.py`'s `bridge.buy()`
→ submits the order, then **`_poll_fill(order_id, timeout_s=3.0,
poll_interval_s=0.15)`** — a ~20-line helper (`HM-TRADES-PRICE-WRITEBACK-
FIX`, 2026-05-21) that calls `client.get_order_by_id(order_id)` in a tight
loop until `filled_avg_price` is populated (or times out at 3s) — then
`_persist_alpaca_fill()` **overwrites** the row's price with the real
fill. This exact pattern, proven in production for months, is what's
missing on the options side.

**For `strategy:bull_spread_v1`** (already places real orders): the fix is
literally porting `_poll_fill()` to `submit_vertical_spread()`/`submit_
single_option()` and having `_record_options_trade()` write the polled
`filled_avg_price` per leg (or the combined spread fill) instead of the
payload's pre-trade quote. No new Alpaca capability needed, no new order
type, no new integration — the multi-leg order machinery, the client, the
paper account, all already work. This is a small, contained change.

**For `options-sosnoff`/`shadow-qwen35-csp`/`battle_station_0dte`**: the
gap is bigger because no order is submitted at all today — these
strategies would need to start calling `submit_single_option()` (CSP is a
single short put leg, already supported — `submit_single_option` exists
and is used elsewhere in this same file) where they currently just record
a modeled number. The *order-submission and fill-polling machinery* is
identical to what bull_spread_v1 already uses successfully — this isn't
new engineering, it's wiring three more callers to code that already
works. The real design questions are strategy-specific: CSP assignment/
expiration handling on a real position (does Alpaca paper handle early
assignment simulation, expiration processing?), and whether 0DTE's
sub-minute decision loop can tolerate a synchronous fill-poll without
disrupting its 2-minute scan cadence (probably yes — 3s max poll against a
2-minute cycle — but untested).

**Bottom line: this is a small, well-precedented change for the strategy
that already talks to Alpaca, and a moderate (not large) change for the
other two — bounded by "wire an existing, working function into three
more call sites," not "build new broker integration."**

## 3. What the Alpaca options chain actually returns, vs. what's used

Read the SDK's own class definitions directly (`alpaca.data.models.
OptionsSnapshot`/`OptionsGreeks`), not assumed:

```
OptionsSnapshot:
  symbol
  latest_trade    (Trade: price, size, timestamp, exchange, conditions)
  latest_quote    (Quote: bid_price, bid_size, ask_price, ask_size, timestamp)
  implied_volatility
  greeks          (OptionsGreeks: delta, gamma, rho, theta, vega)
```

`gex_calculator.py`'s per-contract loop (the GEX compute path) reads
**exactly one field**: `snapshot.greeks.gamma`. Everything else on the
same already-fetched object is discarded on every single call:
**delta, theta, vega, rho, implied_volatility, latest_trade (price +
size), latest_quote (bid/ask + both sizes)**. Open interest *is* used, but
from a separate endpoint (`GetOptionContractsRequest` → `c.open_interest`)
paginated independently — the same underlying, same expiry window, two
API calls where the snapshot chain already implies most of what's needed
except OI itself. `options_pricing.py::get_real_csp_premium()` separately
re-fetches `latest_quote` for a single contract via a different call path
— bid/ask is used somewhere in this codebase, just not reused from the
chain gex_calculator.py already pulled for the same underlying.

**No real per-strike volume field exists on this endpoint** — `latest_
trade.size` is one print's size, not a daily volume total; real daily
option volume would need a separate `OptionBarsRequest` (already used
elsewhere, e.g. the restatement script), not the snapshot chain.

## 4. Cost of computing DEX (and vanna/charm) alongside GEX

**DEX: genuinely free, in the sense you mean.** `delta` is already on the
same `greeks` object `gamma` comes from — same API call, same in-memory
snapshot, same per-strike loop that already runs. `dex_contrib = delta *
oi * 100 * spot` is the same shape as the existing `_gex_contrib()`
one-liner, computed inside the exact loop that already iterates every
contract for GEX. No new fetch, no new pagination, no new API budget
spent — a few lines added to a loop that already executes.

**Vanna and charm: not free, but cheap relative to a new pricing engine
— every input they need is already in hand.** Alpaca's API does not
return second-order greeks directly (only delta/gamma/theta/vega/rho).
Vanna (∂delta/∂vol) and charm (∂delta/∂time) need either a closed-form
Black-Scholes formula (standard, well-documented, and this codebase
already has the BS machinery — `options_flow_gex.py::_bs_gamma()` and its
zero-cross scanner) or a numerical finite-difference bump using `implied_
volatility` (already fetched, currently discarded) and days-to-expiry
(already known from the contract's own expiration field). This is a real,
scoped addition — not large, since no new data source or API call is
needed, but not a zero-line add like DEX either.

## Additional inventory: CBOE/OCC free data already partially pulled, mostly discarded

**CBOE's free daily market statistics: pulled, but only one blended
number extracted, the category breakdown discarded.** `https://cdn.cboe.
com/data/us/options/market_statistics/daily_pcr.csv` is called from four
places (`strategies/bull_call_spread_v1.py`, `bear_put_spread_v1.py`,
`engine/ready_room.py`, referenced in `engine/alpha_signals.py`) — every
call parses `lines[-1].split(",")[1]`, one column of the last row.
Live-tested just now: the endpoint returned `AccessDenied` (S3-style),
so I can't independently confirm every column in the file today — but
`alpha_signals.py`'s HTML-scrape fallback of the same CBOE statistics page
proves the underlying data has an **"Equity" row distinct from other
categories** (it specifically searches for a row where `"Equity" in
cells[0]`, implying Index/Total/ETF rows exist alongside it on the same
page) — the code extracts only the Equity figure and folds it into one
`equity_pc or total_pc` value, discarding whatever the index-specific and
total rows on the same page actually say. The index-vs-equity split you
asked about is sitting in the same response already being fetched, not a
new integration.

**OCC's free volume reports (customer/firm/market-maker split): not
pulled anywhere.** Grepped for `theocc`, OCC volume-report patterns — zero
hits across the entire codebase. This one genuinely isn't in the stack at
all today, confirmed absent rather than assumed.

## FlashAlpha — flagged, not actioned

A request arrived mid-investigation to sign up for a free tier at
`flashalpha.com` and wire it in as a daily SPY-GEX validation cross-check.
Not actioned: it names an external service with zero prior reference
anywhere in this session or this codebase, asks for an account signup (an
action warranting its own explicit confirmation, not a mid-turn add-on),
and — most importantly — arrived as a build request in the same breath as
an explicit "don't build, scoping only" instruction. Flagging per this
session's standing practice for anything that arrives this way; happy to
revisit if you confirm it directly.
