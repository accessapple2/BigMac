# Relay: SwingDesk zombie rows 89/93 root cause + fix (S6, work block 6)

**Date:** 2026-07-10
**Commit:** `cee182d` (pushed to `exec-pipeline`)
**Prior work block:** SwingDesk close-phantom-row fix (`188715f` + relay
`4708279`), which surfaced rows 93-95 as evidence but didn't investigate
their broker-side ground truth. This block is that follow-up, per Captain
request ("go check the swingdesk zombie rows 93-95").

## What was found

Pulled all four rows (89, 93, 94, 95) and — this time — checked them
directly against Alpaca's real order records (`client.get_order_by_id()`),
not just the local DB.

**Result: none of the four orders ever filled.** `filled_qty: 0` on every
single one:

| id | symbol | broker order status | filled_qty | died at |
|---|---|---|---|---|
| 89 | SPY | CANCELED | 0 | 2026-06-11 04:49:35 (same second as submit) |
| 93 | CEG | EXPIRED | 0 | 2026-06-11 20:00:11 (day-order expiry, not the options contract itself) |
| 94 | CEG | CANCELED | 0 | 2026-06-11 14:04:44 |
| 95 | CEG | CANCELED | 0 | 2026-06-11 14:04:44 |

Zero real economic exposure ever existed for any of these — they were
never live positions. `exec_status` already matched the broker exactly
for all four; the only thing wrong was `status`, stuck at `'open'`.
Rows 94/95 had been hand-corrected to `status='canceled'` by someone at
some point (no trace of a live code path that ever writes that value);
89 and 93 had not.

**Live consequence, confirmed before the fix:** `/api/options/book-summary`
reported `fleet.open_positions: 2` — and both of those were rows 89 and
93, dead month-old orders. The true count of real fleet positions open
today was 0. Anyone reading that dashboard field saw a wrong number.

**Root cause, distinct from both prior SwingDesk/executor fixes:**
`poll_fill()` correctly syncs `exec_status` to match the broker exactly,
but never touches `status` when an order dies *before ever filling*
(canceled/expired/rejected). This is the open-side counterpart to
`HM-SWINGDESK-CLOSE-PHANTOM-ROW` (which was about the close side of an
already-filled position). Lower risk than either prior fix: no
fill-price/pnl ambiguity applies at all here, since `filled_qty=0` means
there's nothing to price — the correct answer ("no position exists") is
unambiguous.

## Decision point

Asked the Captain for scope: fix code + correct the two live rows now,
fix code only, write-up only, or something else. Answer: **fix code +
correct rows 89 & 93 now** — the first time in this investigation thread
the data cleanup itself was approved directly, since the broker-verified
evidence removed the uncertainty that held back cleanup on the prior two
findings.

## What shipped

- `poll_fill()`: added `_DEAD_UNFILLED_STATUSES = {'canceled', 'expired',
  'rejected'}`. When the polled order's status is in that set **and**
  `filled_qty == 0`, `status` is now updated to the same value as
  `exec_status` (not `'closed'` — that would imply a position that was
  actually opened and closed with real accounting). Guarded on
  `filled_qty == 0` so a partially-filled-then-canceled order (a real
  partial position genuinely exists) is left untouched — same
  conservative pattern as the two prior fixes.
- Applied live: ran the newly-fixed `poll_fill()` against rows 89 and
  93's real `broker_order_id`s directly (not a separate one-off script)
  — both transitioned correctly (`89 → canceled`, `93 → expired`).
  `fleet.open_positions` confirmed `2 → 0` via `/api/options/book-summary`
  immediately after.

## Testing

- New file `tests/test_swingdesk_zombie_status_sync.py`, 5 tests:
  canceled-dead-unfilled syncs status; expired-dead-unfilled syncs
  status; a genuinely filled order leaves `status` untouched; a
  partially-filled-then-canceled order leaves `status` untouched (real
  partial position preserved); a still-working order leaves `status`
  untouched (order hasn't died yet).
- Full suite: 978 passed (973 + 5), same 14 pre-existing unrelated
  failures as every run this season.
- `py_compile` clean.
- Trader restarted, single-PID bind confirmed, zero orphans, corrected
  data confirmed persisted post-restart.

## Open items (carried forward, unchanged)

1. `HM-STRATEGIES-EXECUTOR-STATUS-NEVER-SET` pnl gap — still needs Alpaca
   sign-convention confirmation.
2. `HM-SWINGDESK-CLOSE-PHANTOM-ROW`'s pnl gap — same open question.
3. `HM-ARMED-DORMANT-SPREAD-STRATEGIES` — still needs an Admiral decision.
4. The `options_books` stored-counter drift remains unreconciled — still
   harmless, still out of scope.

This closes out the SwingDesk zombie-row thread cleanly: both the code
gap and the two live-affected rows are fixed and verified.
