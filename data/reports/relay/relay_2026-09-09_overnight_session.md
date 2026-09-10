# Relay — Overnight session, 2026-09-09 night → 2026-09-10

## VERDICT
**In progress, checkpointed mid-session (context pressure).** Read-only
audit (items 1-5) mostly done, findings below. Code changes (items 6-10)
**not yet started** except the CLAUDE.md standing-order line (done, staged
below). No restart has happened this session. Next session: re-read this
doc's checklist, then continue in the stated order — RULE #1 DELETE-side
layers, disk-alert volume, status page olliemax line, keep_alive override,
Bridge fixes — committing after each, restart only if the tree state is
certain (see hard rule below).

**RULE #1 held throughout:** every finding below is read-only; the one
write made (signals_v2 rec #1 verification) confirmed UPDATE, not DELETE,
already in place. Nothing in this session has deleted or dropped anything.

## Hard rules for whoever continues this (restated, still binding)
RULE #1 on everything. Read-only first (done, mostly). Code changes only
where nothing reaches a decision or exit path. ONE trader restart, by
04:00 MST, followed by a verified live McCoy call with the invalidation
field still parsing. Nothing touched after 04:30. Anything uncertain gets
written up, not done. **New, from mid-session:** if you're not certain of
the working tree's state after a compaction, do NOT restart — commit
what's clean, note it, leave the restart for the morning session. A
missed restart costs nothing; a half-applied one costs the open.

---

## Checklist

### Read-only (items 1-5) — done, findings below
- [x] 1. Never-delete audit
- [x] 2. crew_decisions consumers
- [x] 3. The 393-decision hour on 9/9
- [x] 4. 8/31 origin_healthcheck flapping
- [x] 5. Verify-or-close list (4 of 6 resolved, 2 not located)

### Changes (items 6-10) — NOT STARTED
- [ ] 6. RULE #1 DELETE-side layers (CLAUDE.md rule, DB triggers +
      startup assertion, connection-layer guard, pre-commit hook + test)
- [ ] 7. Sentinel disk alert on the right volume
- [ ] 8. Status page olliemax line
- [ ] 9. Provider keep_alive override removed
- [ ] 10. Bridge: Gamma Map live-gex path, Autopilot true source, Season 6
      label, Crew Dissent/Riker cleanup
- [ ] ONE restart, by 04:00 MST, verified live McCoy call + invalidation
      field parsing
- [x] CLAUDE.md standing order line added (staged, not yet committed —
      see below)
- [ ] XO_BACKLOG.md consolidation (separate section below, in progress)

### Open question found mid-session — RESOLVED
`git config core.hooksPath` = `.githooks` — a custom hooks dir, not
`.git/hooks/`. The real pre-commit hook (`.githooks/pre-commit`) runs
exactly `pytest -q tests/test_otasty_shadow_invariants.py
tests/test_kirk_holdings_guard.py` (19 tests combined) — a fixed,
narrow, always-safe subset, not the full suite. **Correction to tonight's
earlier HM-FALSE-RED-ALERT relay entry:** it attributed the recurring
false alerts to "the pre-commit hook runs the full suite on every
commit" — that's not what actually happens. The real trigger is any
manual/interactive full-suite `pytest tests/` invocation (which this
session ran repeatedly for verification) hitting
`test_season_rotation_reactivation_scope.py`, not an automated per-commit
hook. **The fix already shipped is still correct and complete regardless**
— `_under_pytest()` blocks real sends under any pytest context, hook or
manual — only the causal narrative needs this correction, not the code.
For item 6d: the DELETE-guard test needs to be added to
`.githooks/pre-commit`'s explicit file list to actually run pre-commit,
not just exist under `tests/`.

---

## Findings

### 1. Never-delete audit
Row counts by season:
- `trades`: 1→262, 2→197, 3→674, 4→14, 5→144, 6→1401, 7→89
- `signals`: 1→16,019, 2→3,028, 3→6,640, 4→5,782, 5→20,494, 6→23,146, 7→32,453
- `decision_audit`: no season column, 134,336 total
- `agent_ratings`: no season column, 42,499 total

**DELETE/DROP/TRUNCATE grep across engine/ scripts/ agents/** — exactly
one hit: `scripts/hm_memorial_day_local_reconcile.py` (`DELETE FROM
trades WHERE id = ?`). Verified: a one-off, hardcoded-row-ID (2547-2555),
archive-then-delete script per `docs/DOCTRINE.md` Rule #2, for a specific
documented incident. Not in crontab, no other caller — dormant historical
artifact, not a live risk. Zero DROP/TRUNCATE hits anywhere.

**signals_v2 rec #1 "expire" — confirmed UPDATE, never DELETE.** Both
`scripts/hm_signals_v2_expire_pre_reorder_active_backlog.py` and
`scripts/hm_signals_v2_expire_dead_letter_20260829.py` do
`UPDATE signals_v2 SET status='expired', ...` — zero DELETE statements in
either file.

**UPDATE audit (for the scoped session — no triggers on these tonight,
per instruction):**
- `trades`: `engine/paper_trader.py` (alpaca_order_id/status,
  entry_price, exit_price+realized_pnl on fill/close), `engine/trade_desk_autopilot.py`
  (stop_loss_order_id on OCO placement)
- `signals`: `engine/paper_trader.py:4073` (execution_status,
  rejection_reason)
- `decision_audit`: **zero UPDATE sites anywhere** — fully append-only in practice
- `agent_ratings`: **zero UPDATE sites anywhere** — fully append-only in practice
- `signals_v2`: `engine/events_bus.py`, `engine/execution_router.py`,
  `engine/events_bus_consumer.py` (status transitions), plus the two
  expire scripts above

### 2. crew_decisions consumers
- `dashboard/app.py::api_crew_decisions` → `engine.crew_scanner.get_crew_decisions()`
  — the Bridge's crew-decisions feed panel.
- `main.py`'s Dr. Crusher healthcheck — `MAX(created_at)` in the last 10
  minutes during market hours, warns on scan-liveness stall.
- `engine/crew_scanner.py` internally — a per-player daily-executed-count
  gate and a dedup existence check, both feeding its own gating logic.

### 3. The 393-decision hour on 9/9
Resolved the hour-label mismatch: the original note said "hour 15,"
which is **15:00 UTC = 08:00 MST**, not clock-hour-15 local. McCoy
(`ollama-plutus`) that hour: 293 `gate_reject` + 100 `signal_emit` = 393.
Zero trades. Breakdown of the 293 rejects: `stale_signal` 147,
`regime_mismatch` 61, `REGIME-ROUTER: long_equity not approved in
BEAR_CROSS` 46, `LOW_CONVICTION` 34, GEX dealer-wall ~10. **Not a bug** —
BEAR_CROSS regime blocking most equity BUYs, continuous-scan volume
producing signals that go stale before being dequeued, and normal
conviction/GEX gates. This is exactly the failure mode Phase 1.2
(screen-then-score, twice daily) is built to fix.

### 4. 8/31 origin_healthcheck flapping
6 restart events, 10:45-13:46 MST, across both `main.py` (8080) and
signal-center (9000). One confirmed real cause: `sentinel_main_py_down`
fired at 12:10:49 (main.py genuinely not running via `pgrep`), a restart
followed at 12:11:03. **Could not fully explain the whole 3-hour,
multi-service pattern** — `logs/trader.log.1.gz` doesn't cover that date
range (0 matching lines), no surviving logs from that far back. Partial
root cause confirmed, not complete. Not pursued further given time
budget — would need whatever log retention exists elsewhere, if any.

### 5. Verify-or-close (6 items)
- **gex_collector cron fix — NOT resolved, needs a decision.** Crontab
  line is commented out; `logs/gex_collector.log`'s last lines are
  repeated `can't open file '.../scripts/hm_gex_daily_collect.py'` — the
  script no longer exists. Unclear whether this is a deliberate
  retirement (GEX moved to `engine/gamma_context.py`'s live path per
  CLAUDE.md's 2026-06-24 "Gamma grounding" section) or an unresolved gap
  someone silenced by commenting the cron line. **Needs a decision, not a
  guess** — flagging rather than assuming either way.
- **Three-popup banner UX — not located.** Searched every relay doc and
  `docs/*.md`; the closest hit (`docs/UX_SPRINT_2026-04-28.md`) is a
  different issue (SELL NOW card visual hierarchy, not multiple popups
  stacking). Need a pointer to the actual ticket.
- **Reveille empty-output — verified, working as designed.** The
  degraded-gracefully guard (in place since 2026-08-28) correctly shows
  `"[SITREP] Synthesis unavailable — LLM returned no output. No brief
  content fabricated."` rather than inventing content, when the LLM
  (`plutus-v1`, the qwen3:8b alias) returns empty. Most recent occurrence
  today 12:58 MST. Root cause is intermittent LLM flakiness (consistent
  with today's known model-thrashing/queue-contention), not a Reveille
  defect.
- **Sentinel repoint off retired logs — verified, done correctly for
  both.** `riker_synthesis`'s heartbeat check was explicitly removed from
  the sentinel's active call list (2026-08-29, code-level, with a comment
  explaining why re-enabling it would false-fire forever). `gex_collector`
  is naturally excluded from the sentinel's live-crontab-reading check
  since its cron line is commented — no separate repoint needed for that
  one, the mechanism already self-excludes retired cron lines.
- **regime_refresh intraday rows — verified, working as designed.**
  `regime_history` is `INSERT OR REPLACE` keyed on `date` (one row per
  day, kept current), not one row per 15-minute tick — confirmed in both
  `engine/regime_ma.py`'s save path and `scripts/regime_refresh_runner.py`'s
  fallback upsert. Today's row has been refreshed as recently as tonight.
- **Four Bridge LOW defects — not located.** No matching ticket found by
  this description anywhere searched. Need a pointer.

---

## New tonight, not yet acted on

### Polygon: Massive Stocks Starter live (same key, unlimited calls, still 15-min delayed)
**Do not touch the limiter tonight — stays in shadow mode, as instructed.**
For tomorrow, after close:
1. Raise `engine/polygon_rate_limiter.py`'s assumed cap from 5/min to a
   sane internal ceiling (~100/min).
2. Report tomorrow's Polygon 429 count as the first regression metric —
   expect near zero.
3. Re-scope `bk_orb`: with the rate cap effectively gone, does the
   direct-path pagination fix still matter? Needs re-evaluation against
   the new ceiling, not assumed still-needed.
4. Key rotation is planned for after close, with a verify — not done
   here, just noting it's coming.

### CLAUDE.md standing order — added, staged
Added directly under RULE #1 in `autonomous-trader/CLAUDE.md`: read
`docs/XO_PLAN_2026-09.md` and `docs/XO_BACKLOG.md` at the start of every
session, print open items as a numbered list with owner + due date before
taking new work. **Staged, not yet committed** — commits with this relay
doc.

### XO_BACKLOG.md consolidation — in progress separately, see that file's diff
Every unfinished item from the 9/9 improvement sweep and the Day 2 report
being folded into `docs/XO_BACKLOG.md` with owner (Scotty/Steve/decision),
target date or "unslotted", and blocker. Per instruction: remove a line
only when confirmed done, never when it's just old.
