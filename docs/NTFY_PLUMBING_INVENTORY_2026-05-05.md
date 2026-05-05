# NTFY Plumbing Quick-Check — 2026-05-05
*Read-only inventory. No fixes applied. Output drives next decision.*

## Question
HM-U posture (commit cfc452f) declared NTFY-on-first-error-class-per-day for architecture-class paths (broker-submit, halt_mode writes, position-of-record writes). What plumbing exists today, and what's the coverage gap?

## Plumbing inventory

### Modules / files
- `engine/ntfy.py` — 12 named domain wrappers + low-level `_send` / `_fire` (priority + tag passthrough). Defaults to `ollietrades-crew`.
- `engine/alert_channels.py` — multi-channel send (ntfy + email + DB notification log) with **5-minute per-`alert_type` rate limit** (`RATE_LIMIT_SECS = 300`), severity classes (`AlertLevel.RED/INFO/WARNING`), and a CIC command handler. Defaults to `ollietrades-admin`. **This is the richer / posture-aligned dispatcher.**
- `engine/red_alert.py` — Volume Red Alert subsystem (separate domain, separate write path).
- `engine/pattern_alerts.py`, `engine/orcl_gex_alerts.py`, `engine/dynamic_alerts.py` — pattern/GEX-specific alert paths.
- `engine/telegram_alerts.py` — Telegram path; gated to "disabled" per healthcheck startup.
- `logs/uhura_watch/ntfy_dedup.json` — file-based dedup state (Uhura watchdog has its own per-class-per-day-ish dedup mechanism).
- `agents/scotty/scotty.py`, `scripts/situation_report.py`, `scripts/ghost_advisor.py`, `scripts/uhura_watch.py`, `watchdog.py`, `healthcheck.py` — direct ntfy.sh callers (each rolls its own `urllib.request.urlopen` POST).

### Wrapper functions
12 named helpers in `engine/ntfy.py`:
- `notify_ollie_buy`, `notify_ollie_tp` (Ollie super-trader fills)
- `notify_tpol_buy`, `notify_tpol_sell` (T'Pol DayBlade fills)
- `notify_regime_change` (regime transitions)
- `notify_crusher_restart` (Dr. Crusher healthcheck)
- `notify_post_earnings_short` (PED, agent retired today)
- `notify_bear_breakdown`
- `notify_bull_spread`, `notify_bear_spread` (gate-flipped strategies — fired on success)
- `_send` / `_fire` (low-level, priority/tag passthrough)

5 helpers in `engine/alert_channels.py`:
- `send_alert(level, title, body, alert_type)` — primary dispatcher with rate limit
- `alert_info`, `alert_warning`, `alert_red` — severity-tagged thin wrappers
- `send_test_alert`, `handle_cic_command` — meta/control

### Direct ntfy.sh API call sites (bypass all wrappers)
~7 scripts — each rolls its own `urllib.request.urlopen` POST. Mostly ops/monitoring scripts run as separate launchd jobs:
- `watchdog.py`, `healthcheck.py`, `agents/scotty/scotty.py`, `scripts/situation_report.py`, `scripts/ghost_advisor.py`, `scripts/uhura_watch.py`

### Channels
- **`ollietrades-admin`** — primary admin channel. Used by alert_channels (default), watchdog, healthcheck, riker_synthesis, ghost_advisor, situation_report.
- **`ollietrades-crew`** — primary crew channel. Used by ntfy.py wrappers (default), long_range_sensors, orcl_gex_alerts.
- **`ollietrades-scotty`** — Scotty agent's private channel.
- **`ollietrades-watch`** — Uhura watch channel.

## Architecture-path coverage matrix

| Path category | Sites | NTFY today | Gap |
|---|---|---|---|
| **Broker-submit** (alpaca_options.py `submit_single_option` / `submit_vertical_spread` / `submit_iron_condor` / `close_options_position` / `close_all_options`) | 5 functions, 4 error catches at lines 254, 300, 333, 376, 382 | **🔴 Zero** — `grep ntfy\|notify\|alert engine/alpaca_options.py` returns 0 hits | Every error catch in this file logs `console.log(...)` and returns; no NTFY plumbing reaches them |
| **paper_trader's `_forward_to_alpaca`** (the central forward + SHORT-GUARD path) | 1 function with multiple error/skip branches | **🔴 Zero** | The 181-skip noise pattern from HM-I never NTFYed; same pattern for any actual broker submit failures forwarded through this path |
| **halt_mode writes** | 5 sites: `reset_season2.py:50,51`, `engine/season_manager.py:157,263`, plus dashboard halt UPDATE paths at `dashboard/app.py:5136,5199` | **🔴 Zero** | No NTFY at any halt_mode UPDATE; today's 8 halts (4 production options agents + 4 ghosts) and HM-I-β-Item3's webull→alpaca-mirror migration all proceeded silently |
| **`positions` table writes** | 8 INSERT sites: `paper_trader.py:1004,1009,2208`, `dayblade.py:405`, `metals_tracker.py:401`, `webull_client.py:311`, `alpaca_portfolio_sync.py:152`, `matrix_bridge.py:173` | **🔴 Zero** at the write sites | Caller-level success NTFYs exist (notify_ollie_buy etc.); no error-side NTFY at the actual INSERT failure path |
| **`options_trades` writes** | 2 INSERT sites: `strategies/executor.py:363`, `engine/options_exec.py:93` | **🔴 Zero** at the write sites | Strategy-level `notify_bull_spread` / `notify_bear_spread` fire on success; no NTFY if the INSERT itself fails |
| **`alpaca_portfolio_sync`** (Item 3 broker-mirror sync) | 1 main sync function | **🔴 Zero** | Sync errors (broker-API timeouts, JSON shape changes) silently log without NTFY |
| **`engine/halt_gate.py`** (the gate API) | Read-only helpers | **🟢 Adequate (n/a)** | Gate is read-only; no NTFY-able write events here |

### Where NTFY *does* fire today (success-side / non-architecture)
**🟡 Partial — success-side coverage exists; HM-U posture targets error-side which is the gap.**
- `notify_ollie_buy` / `notify_ollie_tp` — fired on Ollie super-trader execution (8 sites in crew_scanner.py).
- `notify_tpol_buy` / `notify_tpol_sell` — DayBlade T'Pol on fills.
- `notify_bull_spread` / `notify_bear_spread` — gate-flipped strategies on successful submit (these fired this morning when row 14 filled at broker_order_id 88d58691).
- `notify_regime_change` — main.py:454 on MA-cross transition.
- `notify_crusher_restart` — Dr. Crusher healthcheck.
- `_fire(P_MAX)` at main.py:3809 — high-priority dispatch (need to read context to see what it covers).
- Volume Red Alert (`engine/red_alert.py`) — its own subsystem with own dedup.
- Uhura watch (`scripts/uhura_watch.py`) — file-based dedup (`logs/uhura_watch/ntfy_dedup.json`).
- Riker XO synthesis (`engine/riker_synthesis.py`) — every 10 min market hours.

## Recent NTFY activity in logs
- `tail -1000 logs/trader.log | grep -iE "ntfy|notify|sent.*alert|POST.*ntfy"` — **0 hits**. This is consistent with success-side NTFYs going through wrappers that don't log "ntfy" by name (they log per-domain, e.g. `[notify_ollie_buy] ...`).
- `tail -2000 logs/trader.log | grep -iE "ntfy\.sh|ollietrades-admin|ollietrades-crew"` — **0 hits**. Either NTFYs aren't firing in current quiet window, or the wrappers POST without logging the channel.

## Verdict — three categories of gap

### 🟢 Adequate coverage
- **None of the HM-U architecture-class error paths.** Success-side coverage is rich (trade fills, regime changes, Ollie/T'Pol/spread fires) but that's orthogonal to what HM-U calls out.

### 🟡 Partial coverage
- **`engine/alert_channels.py` infrastructure** is *posture-aligned but unused at error sites*. It has rate limiting (5 min per alert_type — close to but not exactly "per day"), severity classes, and multi-channel dispatch. The implementation primitives for the HM-U posture exist; nothing has been wired to use them at the architecture-path error catches.

### 🔴 Zero coverage
**All HM-U architecture-class error paths:**
1. `engine/alpaca_options.py` — 4 submit error catches + 2 close error catches (lines 49/105/157/216/254/297/333/376/382 — overlaps with HM-AA-broad's 6-site target)
2. `engine/paper_trader.py::_forward_to_alpaca` — SHORT-GUARD skip path (the source of HM-I's 181 daily skips), plus error catches in `buy()` / `sell()` / `sell_partial()` failure branches
3. `shared/alpaca_portfolio_sync.py::run_full_alpaca_sync` — broker-API failure / sync-target divergence detection
4. All `halt_mode = ?` UPDATE sites (5 sites)
5. All `positions` and `options_trades` INSERT failure paths (10 sites total)

## Recommended next step (Admiral picks)

### Option A — Small targeted fixes (~30-45 min Scotty session)
**Conditions met:** infrastructure exists (`engine.alert_channels.send_alert` is the right primitive), 🔴 zero-coverage gaps map to ~10-15 single-line additions across 3-4 files. Each fix is `from engine.alert_channels import send_alert; send_alert(AlertLevel.WARNING, f"submit_{fn} {type(e).__name__}", repr(e), alert_type=f"arch_path_{fn}_{type(e).__name__}")`.

**Caveat:** `alert_channels.RATE_LIMIT_SECS = 300` is 5-min dedup, not the HM-U-specified "per day." Would need either:
- Pragmatic acceptance of 5-min dedup as approximation
- A small extension allowing `send_alert(..., rate_limit_secs=86400)` per-call override (~10 lines in alert_channels.py)

**Pros:** small, additive, uses existing infrastructure, directly addresses HM-U's error-side gap.
**Cons:** the `rate_limit_secs` knob extension is technically scope creep beyond pure wiring.
**Effort:** 30-45 min.

### Option B — HM-V design + build session (~1-2 hr)
**Conditions:** if the Admiral wants strict per-class-per-day semantics, NTFY plumbing redesign for centralized policy enforcement, OR a tracker table in `data/trader.db` for cross-restart suppression state.

**Pros:** clean implementation of HM-U posture exactly as written, durable across restarts.
**Cons:** larger surface, decisions needed (in-process set vs DB tracker, message format consistency, channel routing per category).
**Effort:** 1-2 hr design + build.

### Option C — Defer
**Conditions:** if the partial 🟡 success-side coverage is "good enough" for current operations and the 🔴 architecture-path gaps are acceptable risk for now. Posture stays as policy; implementation propagates organically as paths are touched (HM-AA-extension pattern).

**Pros:** zero engineering effort.
**Cons:** the 🔴 zero-coverage gap is exactly what HM-U was meant to close — deferring undermines the posture's stated purpose. Future BTO-class bugs continue to surface only via downstream symptoms (ghost rows, min-hold blocks) rather than first-error NTFY.

## What I (Scotty) deliberately did NOT do
- Did not implement any NTFY calls at architecture-path error sites
- Did not extend `alert_channels.RATE_LIMIT_SECS` configurability
- Did not modify any architecture-path code
- Did not pick A/B/C — the Admiral picks. Note that this inventory leans toward Option A being viable (infrastructure exists, gaps are small), but the per-day-vs-5-min knob is a real architectural choice the Admiral should make consciously.

## One-line summary
Infrastructure exists (`engine/alert_channels.send_alert` with 5-min rate limit); 12+ success-side NTFYs already fire across the codebase; **zero error-side NTFYs reach any HM-U architecture-class path today.** Option A wires ~10-15 sites in ~30-45 min using existing primitives.
