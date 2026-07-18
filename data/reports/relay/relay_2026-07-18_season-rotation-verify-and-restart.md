# Relay: HM-SEASON-ROTATION-BLANKET-REACTIVATE — independent verify + the gap that mattered

**Date:** 2026-07-18, follow-on to the same-day fix (commit `af8519b`) and its
relay report `relay_2026-07-18_season-rotation-blanket-reactivate-fix.md`.

## What this pass found and did that the prior pass didn't

The code fix (commit `af8519b`, `engine/season_manager.py` +
`main.py` + `tests/test_season_rotation_reactivation_scope.py`) was already
complete, tested (6/6 new tests, 34/34 commit-gate+related), dry-run-verified,
and pushed to `origin/exec-pipeline` before this pass started. Independently
re-ran the dry-run SQL and the live `_dry_run_unhalt_scope()` call myself —
numbers match the prior report exactly:

```
active_before=11, old WHERE would touch=79 (68 wrongly halted-agents caught),
new WHERE would touch=11 (0 wrongly caught), safe=True
```

`pytest tests/test_season_rotation_reactivation_scope.py -q` → 6 passed, confirmed fresh.

**The gap: the fix was committed but never loaded.** `main.py` (the live
trader process, PID 93091) had been running continuously since **10:58:41
MST**, and the fix commit landed at **15:51:44 MST** — five hours later.
Python doesn't hot-reload; the running process was still executing the old,
unscoped `rotate_season()`/`start_season()` from memory. Sunday night's
scheduled `run_season_rotation()` (`main.py:5521`, fires inside this same
long-lived process) would have run the **stale, buggy, pre-fix code**
regardless of what was on disk or in git — silently re-triggering the exact
65-agent blanket reactivation this whole effort exists to prevent, with the
fix looking "shipped" the entire time.

**Fixed:** ran `zsh scripts/trader_restart.sh` (market closed, doctrine
pre-authorizes this restart — "Scotty handles git push + launchctl kickstart
... no Captain handoff"). Orphan-prevention + single-writer gate passed
cleanly: old PID 93091 killed, WAL checkpointed, new PID 48815 bound `:8080`
as the sole `trader.log` writer. Verified post-restart:

- `curl http://127.0.0.1:8080/api/health` → `server_up:true`, zero
  `scheduler_errors`, clean `trader_error.log` (no startup exceptions).
- `from engine.season_manager import _UNHALT_ELIGIBLE_WHERE` in the *live*
  interpreter path confirms the fixed clause (`... AND halt_reason IS NULL`)
  and `ROTATION_REACTIVATION_MARGIN=10` are what's actually loaded now.
- Sunday's scheduled rotation will now genuinely run the fixed code — the
  gap that would have silently defeated today's entire fix is closed.

## `ollie-auto` reconciliation — actually performed this pass (was report-only before)

The prior relay (§6) found but explicitly did not act on two orphaned
Alpaca-side positions (`BLK` qty 0.15 / avg $1028.67, `PM` qty 0.14 / avg
$177.51 — real broker positions with zero internal `positions` rows after
the rotation bug's `DELETE FROM positions`, meaning `ollie-auto`'s own exit
logic had no visibility into them). Re-verified live via
`engine.reconciliation.get_alpaca_positions()` — numbers unchanged, both
positions still present and still orphaned.

This directive's explicit instruction ("EXCEPTION — reconcile ollie-auto's
book against live Alpaca before Monday open; confirm no orphaned broker-side
positions exist") reads as a decision already made, not a question — so this
pass executed it:

```sql
INSERT INTO positions (player_id, symbol, qty, avg_price, asset_type, opened_at, high_watermark)
VALUES
  ('ollie-auto', 'BLK', 0.15, 1028.672667, 'stock', '2026-06-01 15:29:20', 1028.672667),
  ('ollie-auto', 'PM',  0.14, 177.514,     'stock', '2026-06-04 15:14:17', 177.514);
```

- `qty`/`avg_price`: live Alpaca truth (broker of record), re-fetched
  immediately before the write.
- `opened_at`: recovered from `data/backups/trader_2026-07-12.db` (the last
  clean pre-rotation snapshot) — harmless metadata, more accurate than
  "now."
- `high_watermark`: set equal to `avg_price` (treated as a freshly-noticed
  position with no tracked favorable excursion yet — deliberately not
  carrying forward the stale 07-12 backup's `high_watermark`, since price
  has moved since and an injected stale value could cause a trailing-stop
  system to fire on bad information).

**Verified post-write, byte-for-byte match:** `positions` table now shows
`ollie-auto: BLK qty=0.15 avg=1028.672667, PM qty=0.14 avg=177.514`, and a
fresh `get_alpaca_positions()` call confirms the identical numbers on the
broker side. `ollie-auto`'s exit/stop-loss logic now has visibility into
both — no orphaned broker-side positions remain for this agent.

## Cash resets

No action, per the Admiral's prior instruction (accepted as-is). Added a
permanent backlog note (`docs/XO_BACKLOG.md`,
`HM-SEASON-ROTATION-CASH-RESET-2026-07-18`) so the $7000 figures on the 65
restored agents aren't rediscovered cold as a fresh discrepancy later.

## Sunday trigger status (re-confirmed)

Still armed, `main.py:5544` (`schedule.every(30).minutes.do(run_season_rotation)`),
unchanged from the prior pass — and now genuinely backed by the fixed code
in the running process, not just on disk. No further action needed before
the 2026-07-19 23:59 MST window.

## Files/state touched this pass

- No new code changes — `engine/season_manager.py`/`main.py`/the test file
  were already correct and committed (`af8519b`).
- `docs/XO_BACKLOG.md` — new entry (cash-reset note + ollie-auto closure).
- `data/trader.db` — live write: 2 rows inserted into `positions` for
  `ollie-auto` (see SQL above). Not a sacred-data violation (insert, not
  delete; reconciling toward broker truth per explicit instruction).
- Trader process restarted (`scripts/trader_restart.sh`), verified healthy,
  single-writer gate passed.
