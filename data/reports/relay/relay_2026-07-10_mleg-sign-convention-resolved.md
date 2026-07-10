# Relay: MLEG close fill-price sign convention resolved (S6, work block 7)

**Date:** 2026-07-10
**Commit:** `3fc8537` (pushed to `exec-pipeline`)
**Prior work blocks:** all five prior blocks today gated their pnl
computation on this exact open question — see `HM-STRATEGIES-EXECUTOR-
STATUS-NEVER-SET` (`3a5c66b`) and `HM-SWINGDESK-CLOSE-PHANTOM-ROW`
(`188715f`).

## Ask

"go check the pnl fill-price sign convention with Alpaca."

## What was found

The `alpaca-py` SDK's own docstring resolves this. In
`.venv/lib/python3.14/site-packages/alpaca/trading/requests.py:436-438`
(`LimitOrderRequest`/`StopLimitOrderRequest.limit_price`), verbatim:

> "For the mleg order class, this is specified such that a positive
> value indicates a debit (representing a cost or payment to be made)
> while a negative value signifies a credit (reflecting an amount to be
> received)."

This is Alpaca's own documented convention, generated from their API
schema — not a guess, not community folklore. It independently matches
`swingdesk/spread_executor.py::build_mleg_order()`'s own docstring
("Debit → positive limit_price"), which had already landed on the same
convention before this was found, without citing a source — corroborating
rather than contradicting.

`filled_avg_price` isn't separately documented with the same caveat in
the SDK, but it's the same priced field concept as `limit_price` — "the
worst price you'd accept" vs. "the price you actually got" on the same
order. A broker reporting a fill wouldn't flip sign convention between
what you asked for and what you received on the identical semantic
field.

**Resolves the formula for both open pnl gaps:**
`close_cost = filled_avg_price * qty * 100`,
`pnl = entry_credit_debit - close_cost` — identical to the formula
`engine/options_exec.py::close_options_trade()` already uses for its own
(non-MLEG, per-leg) closes. The only thing that was missing was
confidence in the sign of the one new input.

**What this doesn't resolve:** this is documented convention, not an
empirically-observed real fill. No live MLEG close has been captured
end-to-end to confirm it. Materially stronger evidence than "nothing,"
but not airtight — a live test-fire (or accepting doc-confidence as
sufficient) is still the Admiral's call before pnl gets written from
this.

## What was updated (docs only, no code)

`docs/XO_BACKLOG.md`'s `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` entry —
the shared backlog card both prior blocks pointed to for this exact
question — updated with the citation, the resolved formula, and the
explicit residual-gap caveat. No separate card existed for the SwingDesk
side of the same question; both threads share this one entry.

## Testing / verification

None — this was a documentation lookup, no code changed. `py_compile`
n/a, no restart needed.

## Open items (carried forward)

1. **Implementation decision, new:** now that the sign convention has a
   real citation, does the Captain want the pnl write implemented in
   `strategies/executor.py::_increment_closed()` and/or
   `swingdesk/spread_executor.py::_close_original_position()`, on
   doc-confidence alone — or held until a live MLEG close can be
   observed end-to-end first? Asked directly, not yet answered as of
   this report.
2. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — still needs an Admiral decision
   on whether `bull_call_spread_v1`/`bear_put_spread_v1`/`bull_spread_v1`
   stay armed.
3. The `options_books` stored-counter drift remains unreconciled — still
   harmless, still out of scope.
