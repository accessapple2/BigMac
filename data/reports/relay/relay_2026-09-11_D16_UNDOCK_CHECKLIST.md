# Relay — D16: UNDOCK CHECKLIST. 2026-09-11

Dry dock complete. Every item below, final state. Fleet is live again as
of this report — `main.py` PID 43948, single writer, port 8080 bound,
clean restart, all 4 previously-disabled cron vectors restored.

## A. FLEET STATE

| # | Item | Status |
|---|---|---|
| A1 | S8 season rotation | **DONE.** Six-step sequence verified live: `current_season=8` confirmed via `/api/status`. |
| A2 | Phase 1.1 acceptance read | **PASS, twice.** 94.6% → 94.8% (re-confirmed post-breach) → distribution stable (median 2.51%, healthy spread). Cleared 90% bar both times. |
| A3 | Phase 1.2 cadence cut | **DONE, live-verified.** `engine/mccoy_screen.py` built, `_SCAN_TIER2` no longer includes McCoy, twice-daily screened scan wired. Confirmed registered on this restart (`job=run_mccoy_screened_scan`, zero errors). Today's two slots (9:35/12:30 ET) — the pre-open slot fell inside the dry-dock window and was missed for today only; midday slot still ahead. |
| A4 | Phase 1.3 sizing | **SPEC ONLY, per your explicit decision — not built.** Re-scoped around the real gate chain (not the dormant Gate 7). Confidence-coordination spec (calibration_map → learning_engine) written and held pending real data (still 0 populated buckets, confirmed today under B9). Do not build without your go-ahead. |

## B. DATA AND CODE

| # | Item | Status |
|---|---|---|
| B5 | Tier 2 hardcoded-host fixes | **DONE.** All 18 fleet-critical files (not 16 — live recount), env-read, fail loud. Found 2 real live bugs along the way (dead bigmac-local Ollama call in dashboard chat, a watchdog.py regression risk). |
| B6 | Options premium restatement | **DONE.** Season 1's 27 rows reconfirmed definitively unrecoverable (no contract data was ever recorded — stronger finding than before). 15/120 `options_trades` pre-fix rows reconstructed from real Alpaca bars; 93 marked unrecoverable with a verified reason each. Season 2 becomes PSR/DSR-scoreable; Seasons 1 and 4 do not (volume, not corruption). |
| B7 | Dataset exporter | **DONE.** Walk-forward corpus (3,179 rows, 2-day span — honestly small, `decision_audit.prompt_text` has no earlier history). Options-row filter wired to B6's restatement infra, currently a no-op (0 option rows in this player's signals today). |
| B8 | Recall-in-prompt wiring | **DONE, live-verified.** Wired into `build_prompt()`, flag stays `False` on disk (confirmed no-op when off, confirmed real neighbors returned when on). |
| B9 | Calibration map | **HELD, per your decision.** Still 0 populated buckets. Reconciled an apparent contradiction (169-trade finding vs. 3-row map read) — both correct, different populations; real finding filed: the map's join requirement covers only 0.6% of settled trades. |

## C. INFRA AND HYGIENE

| # | Item | Status |
|---|---|---|
| C10 | RULE #1 enforcement layers | **DONE (scoped).** Triggers/pre-commit/startup-assertion already solid. `guarded_connect()` wired into the 2 files (of 397) that actually contain a `DROP TABLE` statement — 100% of the real exposure, not a partial sweep. |
| C11 | Pushover redesign | **DONE (partial), 1 decision flagged.** Found and left standing: ntfy has been fully dead since DECOM-SILENCE 2026-07-19 (your explicit prior directive — not reversed without your say-so). Tiered Pushover (WARNING now delivers, quiet priority) and storm breaker shipped + live-verified. OllieTrades token plumbing shipped; needs you to create the actual pushover.net app. Kirk merge scoped, not built (his stronger delivery guarantee deserves its own pass). |
| C12 | dr_crusher stray healthcheck | **DONE.** Retired (not retargeted) — third time flagged, first time actioned. |
| C13 | tour.ollietrades.com | **RECONFIRMED, no action needed.** Live-checked: still healthy, still by-design, no regression since the 07-05 closure. |
| C14 | Grep Gate CI | **RECONFIRMED, already fixed.** Was never disabled at the GitHub level; the relay-doc false-positive was already patched. Verified with a real green CI run against today's actual 14 commits. |
| C15 | Remaining REVISIT-BY items | **DONE.** Last overdue tag (`situation_report.py`) resolved — turned out moot (already dark since a July stand-down). `check_doc_revisit_dates()` now returns zero overdue. |

## D. UNDOCK

| # | Item | Status |
|---|---|---|
| D16a | Full test suite | **1259 passed, 13 failed, 2 skipped** (of 1272; 2 more files need `.venv-backtest` for `vectorbt`, unrelated). All 13 failures verified pre-existing and unrelated to this session — none of their target modules were touched by any dry-dock commit; checked by diff inspection, not a risky stash comparison (see below). |
| D16b | Clean restart | **DONE.** `trader_restart.sh`: zero writers before, WAL checkpoint `0\|0\|0`, single listener PID 43948, orphan-free. Zero tracebacks in the fresh log. All 4 dry-dock-disabled cron vectors restored (172→172 lines, exact diff, standard safety pattern). |
| D16c | Live McCoy call, invalidation parsing | **DONE, live-verified.** Direct `analyze()` call through the real production path (real `build_all_providers()`, real market data, real model call to olliemax) — AAPL, **BUY, confidence 0.85, invalidation $326.21** (2.81% below ref price, correct side) — plausible per Phase 1.1's own 0.5-15% band. Confirmed zero DB side effects from the verification call itself (pure decision compute, no trade/signal/audit row written). |
| D16d | This checklist | You're reading it. |

## An incident during D16, resolved cleanly — worth knowing about

While diffing today's changes against a suspected-unrelated commit, a
`git stash && test && git stash pop` sequence popped a **pre-existing,
unrelated stash entry** (`WIP on hm-conviction-denorm-and-stop-wire`,
someone's in-progress stop-loss tier calibration work, not from today)
instead of stashing anything of mine — the tree was already clean since
everything was committed. This produced merge conflicts in 4 files.
**Recovered immediately and cleanly**: `git reset --hard HEAD` restored
the working tree to the last real commit; the old stash entry is
untouched and still recoverable (`git stash list` confirms it). No data
lost, nothing of today's work at risk (everything was already committed
before this happened). Lesson for future sessions: check `git stash
list` before any stash operation, don't assume a clean stash pop.

## What to watch on the first session back

1. **McCoy's 12:30 PM ET screened-scan slot** — this is the first real
   scheduled firing of A3's new code. Check `logs/trader.log` for
   `McCoy screened scan [midday]:` and confirm it actually executed
   (not just registered) — the direct verification call above proves the
   pipeline works, but not yet the scheduled trigger itself.
2. **Signal volume** — A3's whole point was cutting ~2,095/day down to
   under 200. Worth a rough day-1 count.
3. **`hm_ops_sentinel.py`'s dock-mode flag** — confirm no stray
   `data/DRY_DOCK` file reappears and that the "docked, intentional"
   heartbeat has gone silent now that the fleet is undocked (it should —
   `is_docked()` returns `False`).
4. **Pushover WARNING volume** — this is the first time WARNING-level
   alerts have had a real phone delivery path in ~2 months (ntfy still
   dead). Expect more Pushover notifications than you're used to; the
   storm breaker should keep it from flooding, but the baseline volume
   itself is new and worth a gut-check.
5. **The ntfy DECOM-SILENCE decision** (C11) — still open, needs your
   call: leave it dark (current state, Pushover-only) or lift it now that
   Gate 2 is long past.
6. **`docs/XO_BACKLOG.md`'s new items**: Options_trades units
   inconsistency, Kirk merge, "wire the remaining ~395 connection
   helpers" — all real, all low-priority, none urgent.
7. **The dedicated OllieTrades Pushover app** — code's ready, needs the
   actual pushover.net app created + token set whenever convenient.

Dry dock closed. Everything above is committed and pushed
(`exec-pipeline`, through this report).
