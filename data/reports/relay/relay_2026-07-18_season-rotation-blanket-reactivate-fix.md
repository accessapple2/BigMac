# Relay: HM-SEASON-ROTATION-BLANKET-REACTIVATE

**Date:** 2026-07-18, RED-adjacent, resolved before the 2026-07-19 23:59 MST deadline.

## 1. The fix — exact WHERE clause diff

**Before (buggy, no carve-outs):**
```sql
UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL
WHERE id NOT IN ('webull','alpaca-mirror') AND id != ?   -- ? = neo-matrix
```

**After:**
```sql
UPDATE ai_players SET halt_mode='active', halt_reason=NULL, halted_at=NULL
WHERE id NOT IN ('webull','alpaca-mirror') AND id != ? AND halt_reason IS NULL
```

One added clause: `AND halt_reason IS NULL`. Verified empirically against live data before writing any code — **100% clean partition, zero exceptions either direction**: every one of the 11 currently-active agents has `halt_reason IS NULL`; every one of the 68 currently-halted agents (retired, HM-item halts, `HM-ROSTER-CAP` exclusions, `HM-BM`/`HM-BN.1` bakeoff clones, `exit_only`) has a non-NULL `halt_reason`. This directly satisfies "must NEVER modify agents with halt_reason set."

Applied identically to both `rotate_season()` and `start_season()` (`engine/season_manager.py`) — both had the same vulnerable pattern; `start_season()` is manually-invoked but carries the identical landmine, so it got the same fix for consistency.

## 2. Dry-run affected-row counts (read-only SELECT, no write — run before any code shipped)

```
OLD (buggy) WHERE clause: 79 rows touched, of which 68 are currently halted (would be wrongly reactivated)
NEW (fixed) WHERE clause: 11 rows touched, of which  0 are currently halted (would be wrongly reactivated)
```

The 11 rows the new clause touches are exactly the 11 currently-active agents (`capitol-trades`, `desk-manual`, `enterprise-computer`, `m5-allocator`, `ollama-plutus`, `ollama-qwen3`, `ollie-machine`, `options-sosnoff`, `qwen3-4b-audition`, `qwen3-8b-flash`, `trade-desk`) — the UPDATE becomes a no-op refresh under current data, exactly as intended.

## 3. Belt-and-suspenders assertion

Implemented as a **pre-write dry-run check**, not a post-write rollback — cleaner and trivially safe: `_dry_run_unhalt_scope(conn)` runs two read-only `SELECT COUNT(*)` queries (current active count, and count matching the new eligible-rows WHERE clause) *before* `rotate_season()`/`start_season()` do **any** write (including `save_season_summary`, which is idempotent and safe to defer). If `would_affect > active_before + ROTATION_REACTIVATION_MARGIN` (margin = 10, a module constant in `engine/season_manager.py`), the function:
- Logs a `[bold red]` console error with the exact counts.
- Sends an `AlertLevel.RED_ALERT` NTFY via `engine.alert_channels.send_alert` (`alert_type="hm-season-rotation-aborted"`, 1h rate limit).
- Returns `None` (`rotate_season()`) or `{"error": ..., "scope": ...}` (`start_season()`) **without touching the database at all** — no season number advance, no cash reset, no halt_mode change, nothing to roll back because nothing was written.

`main.py`'s `run_season_rotation()` caller was updated to handle the `None` return distinctly from a successful rotation (logs the abort clearly instead of printing "Season auto-rotation complete → Season None").

This directly satisfies "a season rotation should never 7x the fleet" — the exact 12→76 blowup from 2026-07-12 would have been caught (76 ≫ 12+10=22) and aborted before a single row changed.

## 4. Testing

Added `tests/test_season_rotation_reactivation_scope.py` (6 tests, all passing) against a real temp-file SQLite DB (WAL mode requires file-backed, not `:memory:`) with `engine.season_manager.DB` monkeypatched for isolation:
- The eligible-rows WHERE clause never matches a row with `halt_reason` set (retired/roster-cap/bakeoff/exit-only all excluded).
- Scope check correctly reports safe when `would_affect == active_before`.
- Scope check correctly reports unsafe when a simulated blowup (2 active, 20 NULL-reason halted rows) exceeds the margin.
- `rotate_season()` aborts and leaves `ai_players`/`settings.current_season` **completely byte-for-byte unchanged** when unsafe.
- `rotate_season()` proceeds normally when safe, and every halted agent's `halt_mode`/`halt_reason` survives untouched.
- `start_season()` mirrors the same abort behavior.

One thing caught and fixed during test-writing: `engine.war_room.save_hot_take` (called at the end of a successful rotation) has its **own separate hardcoded `DB = "data/trader.db"`** constant, unaffected by monkeypatching `season_manager.DB` — an unmocked "safe" test would have written real test data into the live production `war_room` table. Mocked it (`unittest.mock.patch("engine.war_room.save_hot_take")`) in the one test that lets rotation complete. Verified post-test-run against the live DB: `war_room` table's most recent `SEASON` row is still the real 2026-07-12/13 event (`id=114908`), no test pollution.

Full suite: `pytest tests/ -q` → 1066 passed, 14 pre-existing failures (all unrelated — `test_bbkc_squeeze_scanner`, `test_conviction_stop_shadow`, `test_fleet_trail_conviction_scale`, `test_ollama_cancel_on_timeout`, `test_quality_gate_hold`, `test_universe_filter`, `test_war_room_instrumentation` — none touch `season_manager.py`/`main.py`/halt_mode). The commit-gate suite (`.githooks/pre-commit`: `test_otasty_shadow_invariants.py` + `test_kirk_holdings_guard.py`) plus the new test file plus the related `test_halted_at_enforcement_trigger.py`: **34/34 passed.**

Live re-verification after code changes (read-only, matches §2): `active_before=11, would_affect=11, safe=True`.

## 5. Sunday trigger status

**Not disabled — the fix shipped and was tested with high confidence today**, per the directive's own framing ("if the code fix can't be confidently completed and tested today: disable the trigger instead"). The trigger (`main.py:5537`, `schedule.every(30).minutes.do(run_season_rotation)`) remains active and will fire its next check window Sunday 2026-07-19 23:55-23:59 MST as normal — it will now either (a) run a correctly-scoped rotation that only refreshes the 11 already-active agents (a no-op for halt_mode, real season-number/cash-reset/position-cleanup effects proceed as designed), or (b) abort safely with an NTFY if something unexpected changes the picture before then.

## 6. `ollie-auto` orphaned-position reconciliation (Admiral loose-end item)

Checked live Alpaca positions (`engine.reconciliation.get_alpaca_positions()`) against `ollie-auto`'s internal `positions` table rows.

**Finding: two orphaned broker-side positions exist.**
```
Alpaca (real, live):  BLK qty=0.15  market_value=$160.83
                       PM  qty=0.14  market_value=$27.02
ollie-auto internal:  0 rows (deleted by the 2026-07-12 rotation bug)
```
These are real paper positions still sitting on Alpaca that `ollie-auto`'s own strategy/exit logic no longer knows it holds (its internal book shows zero, so no stop-loss/exit signal will ever fire for them from ollie-auto's side). `alpaca-mirror` independently already reflects these same two positions correctly (it's a passive 5-min broker-sync mirror, not an active decision-maker) — so the *data* isn't lost, but the *decision-making loop* for these ~$187.85 combined is currently orphaned. Per your instruction, **not restored** — reporting only. Recommend a deliberate decision: either manually re-seed these two rows into `ollie-auto`'s `positions` table (source of truth: the live Alpaca query above) so its exit logic resumes managing them, or explicitly accept they're now effectively "alpaca-mirror-only" positions with no active strategy attached.

## 7. Cash resets (Admiral loose-end item)

Accepted as-is per your instruction — no action taken. Noting for the backlog: all 65 restored agents' `cash` column still reflects the $7000 season-reset value from 2026-07-12, not restored to pre-rotation figures.

---

**Files changed:** `engine/season_manager.py` (WHERE clause fix + dry-run safety check, both functions), `main.py` (abort-aware caller), `tests/test_season_rotation_reactivation_scope.py` (new, 6 tests). No sacred-DB writes performed by this fix itself — it's a code change, tested against a temp DB and dry-run-verified read-only against the live DB.
