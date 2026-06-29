# EMIT-TIME `acted` TAGGING — DESIGN SPEC
**Status: DESIGN ONLY — not built. Build with Admiral review (touches the fire path).**
**Replaces the structurally-dead retrospective join for `acted_by_fleet`.**

## The problem this fixes
`acted_by_fleet` is computed *retrospectively* by joining `signal_observations` to `trades` on ticker+direction+window. Confirmed dead end (2026-06-29): scanners observe 2,179 tickers; the fleet trades ~16/week. The populations barely overlap, so the join yields ~0.02% and can't answer "did the fleet act on its best signals." Even with the timestamp-format fix, the *approach* is wrong — you can't reconstruct the link after the fact at this universe ratio.

## The fix (one line of doctrine)
**Stamp the observation at the moment the fleet fires, not by joining later.** When the fleet places a paper order, it *is* trading that ticker right then — so the matching live observation is known with certainty. Capture the link at the moment of truth; stop reconstructing it from mismatched populations.

## Hook point
In `paper_trader.py`, **immediately AFTER an Alpaca paper order is confirmed placed** — never before, never in the decision path.
- Pure side-effect, wrapped in `try/except`: a tagging failure logs and continues. It must NOT alter, delay, or block what the fleet trades. (Identical discipline to the original observe-first emit hooks.)
- RULE #1 untouched: this reads observations + sets outcome fields. It changes nothing about whether/what trades.

## What it does on each fire
Given a confirmed trade `{ticker, direction, executed_at, trade_id}`:
1. Find open `signal_observations` WHERE
   `ticker = trade.ticker`
   AND normalized direction matches (BULL↔BUY, LONG↔BUY, BEAR↔SELL, SHORT↔SELL)
   AND `executed_at` falls within the observation's `[ts, expiry]` validity window
   AND `acted_by_fleet IS NULL` (set-once).
2. For each match: set `acted_by_fleet = 1`, `fleet_trade_id = trade.trade_id`, `acted_at = now()`.
3. Multiple matches (several sources fired the same ticker) → stamp all in-window; all credited to the same trade. Correct: multiple sources predicted it, fleet acted, all earn credit.
4. No match (fleet traded something no scanner observed) → no stamp. Correct: a fleet-originated trade, not a signal-acted one.

## Schema
`signal_observations` already has `acted_by_fleet` (null until set) and `fleet_trade_id` (null). Add `acted_at TIMESTAMP NULL` if absent. **No new table.**

## Append-only compliance
These are **set-once outcome fields**, set from NULL, never overwritten — the exact pattern the evaluator already uses for `fwd_return_*`. This is not a history rewrite and not a DELETE/DROP/TRUNCATE. Consistent with sacred-data doctrine.

## Coexistence with the retrospective evaluator
- Emit-time is **authoritative going forward.** Keep the retrospective join only as a no-op fallback.
- **Guard:** the retrospective evaluator may set `acted_by_fleet` only when still NULL — it must NEVER overwrite an emit-time stamp. (One-line WHERE clause.)
- Historical pre-deployment observations stay `acted=null`/retro-only — forward-only, by design. No backfill.

## Why this also powers Carrier Rung 4
When Rung 4 (paper sortie from an alert) fires, it fires *from a specific observation* — so it can stamp `acted_by_fleet=1` directly, with `fleet_trade_id` and the source. **The carrier generates clean acted-data as a byproduct of acting.** Emit-time tagging is the shared mechanism: the fleet's normal fires and the carrier's sorties both stamp the same way. This is the measurement loop closing.

## Build checklist (when greenlit, with review)
1. Add `acted_at` column if missing (additive, set-once).
2. `tag_acted(trade)` — the match+stamp function, idempotent, set-once.
3. Hook AFTER confirmed order placement in `paper_trader.py`, in try/except.
4. Add the NULL-only guard to the retrospective evaluator.
5. Verify on a SAFE/no-op test fire: matching observation gets stamped once; re-fire doesn't double-stamp; a no-match fire stamps nothing; a real fleet trade does NOT change because of tagging.

## Explicitly OUT OF SCOPE
- No backfill of historical observations. No change to trade behavior. No execution added (Rung 4 is separate + gated). Build only after Admiral review — never as an unattended overnight job.
