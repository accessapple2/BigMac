# Decision Desk Regime Bypass + False Red Alert Recurrence — 2026-09-09 (afternoon)

Continues [[relay 2026-09-09 30b-revert-and-think-leak]] (morning: 8B/30B/instruct-2507 saga, still live on `qwen3:30b-a3b-instruct-2507-q4_K_M`, no reverts, no threshold breaches all day).

**VERDICT: Decision Desk manual regime-bypass shipped and used for real — 2 real Alpaca paper trades placed (TMO, SNPS). Found and fixed a pre-existing bug (Desk could never execute anything, ever, since the feature shipped 2026-07-05) along the way. One real gap remains open: neither position has an automated exit path. Separately, a 12-day-recurring unexplained false RED_ALERT was re-investigated, same "source unidentified" conclusion as 08-29.**

---

## Decision Desk: regime bypass, per-hop trace, two real fills

Built exactly to spec (double-gated, regime verdict logged not enforced, every other gate enforced, `desk-manual` seat, Alpaca paper only, broker timestamps not our clock):

- `engine/paper_trader.py::buy()` gained `bypass_regime` (default `False`, set **only** by `dashboard/app.py::desk_execute_signal`). When set, `check_regime_fit()` still runs and its verdict is still logged — just not enforced. Every other gate (stale_signal, LOW_CONVICTION, GEX, quality gate, concentration cap, grade-B fleet gate) stays fully enforced.
- `desk-manual` added to `_EXECUTION_PORTFOLIO_BY_PLAYER` → `"Alpaca Paper"` (`execution_mode=auto`) so a Desk execution places a **real** Alpaca paper order, not a simulated bookkeeping entry — required for genuine `ack`/`fill` broker timestamps to exist at all.
- New `desk_execution_trace` table (setup_db.py), keyed by `signal_id`: `scan_ts`, `gate_ts`/`gate_verdict`, `order_ts`/`order_id`/`order_status`, `ack_ts`, `fill_ts`, `fill_price`. `prompt_ts`/`model_ts` intentionally NULL — not retrofitted (would need timing threaded through `analyze_chain()`→`save_signal()`, out of scope this pass).
- `engine/alpaca_bridge.py`'s `buy()`/`sell()` now also return `submitted_at`/`filled_at` straight from Alpaca's own order object (one extra `get_order_by_id()` after `_poll_fill()` confirms a fill) — broker-side timestamps, not inferred.
- Frontend (`bridge-v2.html`): regime-rejected rows now show an amber **"SEND IT — REGIME BYPASS"** button when fresh (inside the timeframe's stale budget), mirroring the backend claim extension exactly, so there's no button that just 400s. Verified via an offline render harness (real browser, published as a private artifact, since localhost was categorically unreachable from the browser automation tool) — 4 mock states, all pass.
- Backend claim extended: a `REJECTED` signal is now claimable if its rejection was the regime block specifically **and** it's still inside its timeframe's stale budget (`events_bus._STALE_BUDGET_S`, SWING=3600s today). Everything else stays `PENDING`-only.

### Bug found and fixed: Decision Desk had NEVER executed a trade, ever
`paper_trader.buy()`'s "GUARD: Never auto-trade human portfolios" (`_is_human_player`) unconditionally blocked `desk-manual` — which is deliberately `is_human=1` (it represents a human's manual action, not an AI agent) — including the Desk's own execute endpoint. Confirmed live: `desk-manual` had **zero trades ever recorded** before today's fix, despite the feature shipping 2026-07-05. Fixed by skipping the guard when `bypass_regime=True` (exclusively set by the one authorized caller). First real trade (TMO) fired minutes after this fix.

### Two real executions

| | VZ (107027) | TMO (107025) | SNPS (107056) |
|---|---|---|---|
| Confidence | 0.85 | 0.88 | 0.87 |
| Regime bypass | ✅ logged | ✅ logged | ✅ logged |
| Quality gate | ❌ 2/5 (earnings −22.0%, revenue −0.7%) | not reached — passed | passed (4/5, earnings +89.3%, revenue +42.4%) |
| Result | **Not executed** — correctly blocked by a *different*, still-enforced gate | **Executed**, filled $605.62 | **Executed**, filled $394.816 |

VZ is not a bug — the regime bypass worked exactly as designed; VZ separately failed fundamentals, a gate that was always there but never previously reached for McCoy's regime-blocked signals (regime_router always rejected first). Real finding: **bypassing the regime gate doesn't guarantee a fill** — pre-screened the rest of the hour's regime-blocked candidates against the quality gate before pushing further (14 of 15 passed; only NSC failed).

**Trace, both fills** (elapsed split into operator-wait vs. system time):

| Hop | TMO | SNPS |
|---|---|---|
| scan (signal created) | 18:03:04 | 18:46:15 |
| **operator wait** (scan→gate) | 3041s | 689s |
| gate (regime, bypassed) | 18:53:45.13 | 18:57:44.50 |
| **system time** (gate→order) | 12.0s | 4.0s |
| order (submitted) | 18:53:57.56 | 18:57:48.23 |
| ack (Alpaca `submitted_at`) | 18:53:57.437 | 18:57:48.095 |
| fill (Alpaca `filled_at`) | 18:53:57.448 | 18:57:48.102 |
| **fill→ack** | ~0s (instant) | ~0s (instant) |

Both real Alpaca paper fills landed within ~10ms of order submission — the entire "system time" (gate-bypass to fill) is 4–12 seconds, dominated by the quality-gate/fundamentals lookup, not the broker. Slippage: TMO 16¢/0.026% (desk quote $605.46 vs fill $605.62), SNPS 15¢/0.037% (quote $394.67 vs fill $394.816).

### Open, unresolved: no automated exit path for either position
Traced every rules-based exit mechanism in `crew_scanner.py`:
- `_check_mccoy_target_relative_exits()` / `_update_mccoy_trailing_stops()` — hardcoded to `MCCOY_PLAYER_ID = "ollama-plutus"`, never touches `desk-manual`.
- `_check_hard_stops()` (the generic −8% safety net meant to cover *every* player) — does query all position-holders including `desk-manual` now, but its `sell()` call hits the **same** `_is_human_player` guard I patched in `buy()` — except I only patched `buy()`. `sell()` and `sell_partial()` still have the bare, unconditional guard.

Net: TMO and SNPS have **zero** automated exit path right now — not McCoy's, not the generic hard stop. Both are flat (TMO +0.04%, SNPS −0.12% as of this writing) so there's no live emergency. **Decision deferred to the Admiral**, not made unilaterally: patch `sell()`/`sell_partial()` the same way (lets the hard stop protect them), or leave them exitable only by hand (Alpaca dashboard / direct DB) until closed. Given `DESK_EXECUTE_ENABLED` is intentionally left to die at the next restart (see below), this gap doesn't grow — no new orphans after that restart, just these two existing ones until closed one way or another.

### DESK_EXECUTE_ENABLED — left to die by design
Set as an ephemeral env export for each of today's restarts (`.env` write was blocked by permission settings — arguably the safer outcome anyway: it reverts to off on any future restart unless deliberately re-enabled, rather than silently persisting). No action needed to turn it off; the next restart for any reason does it.

---

## False RED_ALERT recurrence — HM-FALSE-RED-ALERT, 2026-08-29 → 2026-09-09

A RED_ALERT fired today (~12:29 MST) reporting a season-rotation abort — "31 vs 1 active, margin=10, before Season 2" — byte-identical to a ticket already filed and investigated on **2026-08-29** (`docs/XO_BACKLOG.md`), which concluded "source unidentified." Re-ran every check that investigation did, live, today, and got the identical result:

- `season_manager.get_current_season()`: plain uncached `SELECT`, returns `7` right now.
- `_dry_run_unhalt_scope()`'s live COUNT queries: `active_before=8`, `would_affect=8` — exact match, zero drift.
- `trader.log` + `.1.gz` (weeks of history): zero rotation-attempt lines. Job is Sunday-only; today is Wednesday.
- No phantom/alternate `trader.db` anywhere on disk reproduces these numbers (checked two candidates, both dead/unrelated).
- `reset_season2.py` (repo root) is the historical origin of the "Season 2" *label* (the original March 2026 migration, no dry-run/margin logic) but sends no alerts and shows no evidence of having run.

**Real, related bug found — not confirmed as this alert's cause:** `engine/agent_ratings.py:35` evaluates `_CURRENT_SEASON` at module-import time rather than per-call, freezing for a long-lived process's lifetime (fallback also stale: hardcoded `6`, real season is `7`). Documented, not fixed this pass, not claimed as root cause.

**Shipped, unrelated to root-causing this alert:** `alert_channels.py::_send_pushover()` now sends an explicit `timestamp` field (`int(time.time())`) — it previously sent none, leaving Pushover's own receipt-time default in control of the displayed time, one plausible (not confirmed) explanation for a ~12-day-old display. Removes a variable; doesn't explain the recurrence.

**Conclusion unchanged from 08-29: source unidentified.** Two occurrences, 12 days apart, byte-identical payload, neither traceable to a live execution path on this system. Full S8 rotation sequence proposed (not executed) separately, preserving the existing margin gate and halt_mode scoping.

## Code shipped (commits, pushed to exec-pipeline)
- `59fcaeb` — Decision Desk regime bypass, human-guard fix, per-hop trace, real Alpaca paper routing.
- `25ac999` — Frontend SEND-IT button for regime-rejected rows.
- `b77ddd2` — False-alert re-investigation writeup + Pushover timestamp fix.
