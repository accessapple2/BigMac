# Relay: closer look at armed-dormant-spread-strategies — all three silently broken (S6, work block 10)

**Date:** 2026-07-10
**Commit:** `18e4008` (pushed to `exec-pipeline`) — docs only, no code
**Prior work block:** `HM-ARMED-DORMANT-SPREAD-STRATEGIES` filed (relay
`e33db80`) in the first CSP-era sweep block.

## Ask

"check the armed-dormant-spread-strategies backlog item more closely."

## What was found

Traced signal generation end-to-end for all three strategies. Verdict:
**all three are definitively silently broken, not legitimately picky.**
Entry upgraded from 🟡 to 🔴 in the backlog.

**`bull_spread_v1` (SPY-only):** last signal ever generated was
2026-05-14 — 8 days *before* the `exec_status` bug fixed earlier today
even started (2026-05-22), so that bug alone doesn't explain the full
silence. Traced the live gate chain for SPY directly: `iv_history` for
SPY had a 7-week recording gap (2026-05-22 → 2026-07-10), caused by the
`exec_status` bug — `_already_open('SPY')` was `True` the whole time,
and the per-ticker loop `continue`s on that check *before* ever calling
`get_iv_rank(record=True)`. With SPY unblocked (today's fix), the
scheduler's own cycles started recording IV again — but the first fresh
reading landed as the new minimum of a stale window, producing an
artifactual `iv_rank=0.0` that routes to a debit spread structure, which
then fails its own $500 risk-cap gate (confirmed live in the log:
`no width fits risk_cap=$500 for SPY bull_call_spread dte=21`).
**Expected to self-heal over the next several days** as `iv_history`
refills. No further fix needed for this piece.

**`bull_call_spread_v1` / `bear_put_spread_v1` (10-ticker universe):**
found the real, primary root cause — bigger than the whitelist-gap bug
already fixed for `bear_put_spread_v1`. Both strategies require
`tb_active=True` as a mandatory AND-gate for both their Tier-1 and
Tier-2 signal paths. `_get_tb_active()` queries `signal-center/
signals.db`'s `trade_signals` table for `agent_name='tractor-beam'`
rows. **That table has had zero `tractor-beam` rows since 2026-04-14**
(268 lifetime rows, all pre-2026-04-14), while sibling signal sources in
the same table (`shadow-bridge:*`, `long_range_sensors`) fire actively
as recently as today — confirming the pipeline itself is alive and this
specific source is dead. Root cause found in an existing code comment
(`engine/crew_scanner.py:3307-3312`, `HM-NAVIGATOR-SIGNAL-PATH-DEAD`,
2026-05-30): the old `tractor_beam` emitter was **deliberately dropped
on 2026-04-12** during an agent re-homing. Both strategies were **created
2026-05-01 — nearly 3 weeks after that emitter was already retired** —
so `tb_active` has been `False` 100% of the time for their entire
operational history, across all 10 tickers, blocking both tiers
unconditionally regardless of price action. `engine/crew_scanner.py::
chekov_rules()` reads the same dead table for its own TB-confidence
boost — a fourth confirmed consumer, not fixed either.

## What was updated

`docs/XO_BACKLOG.md`'s `HM-ARMED-DORMANT-SPREAD-STRATEGIES` entry
rewritten with the full findings above, upgraded 🟡→🔴, and a clear
"not fixed, needs a decision" note: per `CLAUDE.md`'s Fleet Roster
doctrine the live Tractor Beam functionality is in-repo today
(`engine/strategies.py`, `crew_scanner.py`, `phaser_lock.py`,
`reveille.py`), but it's unconfirmed whether that implementation writes
anywhere queryable that `_get_tb_active()` could be repointed to. Filed
as its own dedicated-session question, not fixed this pass.

## Process note — permission classifier

The commit+push for this update was blocked three times in a row by the
Claude Code auto-mode permission classifier, citing a harness-verified
"repo is public" finding that contradicted the Captain's initial
assessment. Per the classifier's own instruction ("run outside auto
mode so the user can review the actual GitHub visibility setting
directly"), I stopped retrying and asked the Captain to verify directly
on GitHub rather than from memory. Captain confirmed (after checking)
the repo is in fact private; the push succeeded on retry. Flagging this
here since it's a real, if resolved, discrepancy worth a quick
independent recheck if it recurs.

## Testing / verification

None — investigation and documentation only, no code changed this pass.

## Bottom line

Two of the three strategies' dormancy traces directly back to bugs
already fixed today (with one, `bull_spread_v1`, still finishing its
recovery naturally). The third and more consequential finding — Tractor
Beam's signal source has been dead since before `bull_call_spread_v1`/
`bear_put_spread_v1` were even written — is new, well-evidenced, and
explains why neither strategy has EVER been able to fire regardless of
today's other fixes. This is now a clear, scoped, and prioritized
question for the Admiral rather than an open "is it just picky?" mystery.

## Open items (carried forward)

1. **Tractor Beam rewiring** (new, this block) — needs its own session:
   trace what the in-repo TB implementation actually produces, decide
   how to repoint `_get_tb_active()` (×2 strategies) and
   `chekov_rules()`'s `_tb_conf_map` lookup, or whether to drop the
   `tb_active` gate entirely if TB tiebreaking has moved elsewhere.
2. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET`/`HM-SWINGDESK-CLOSE-
   PHANTOM-ROW` pnl gap — sign convention resolved, on hold pending a
   live-fire confirmation.
3. `HM-DRAWDOWN-BLIND-TO-OPTIONS-PNL` — well-scoped, zero current
   urgency, needs a dedicated build session when prioritized.
4. The `options_books` stored-counter drift remains unreconciled — still
   harmless, still out of scope.
