# HM-AN Phase 2 — Discovery Report

**Date:** 2026-05-12
**Phase:** AN2.0 (Discovery, no code changes)
**Auditor:** Scotty (Opus 4.7)
**Status:** HALT — Captain Q1 (scope) + Q2 (halt_mode posture, if C chosen)

## TL;DR

- **Signal Center is a substantial Flask service** at `signal-center/server.py` (127k LOC, 30+ endpoints, 11 tables, 675MB DB with 1+ year of history).
- **Phase 1 read bridge is wired** in 3 places: `engine/momentum/bridge.py` (heartbeat + signal fetch), `dashboard/app.py:11202` `/api/signal-center/top`, and `engine/ghost_scoring.py` (pulls BUY signals + scores them).
- **Write direction already exists partially** — `engine/long_range_sensors.py:27` posts whale-detection signals to Signal Center's `POST /api/signal`. So "bidirectional" isn't green-field; some pipes are there.
- **neo-matrix is halted with a literal HM-AN reference** in its `halt_reason`: *"Waiting on HM-AN port-9000 bidirectional bridge. Exit-only until shipped. 2026-05-11"*. Captain wrote the halt note pointing at this very ticket. Last trade 2026-04-21 — 3 weeks dormant.

## Signal Center inventory

### DB schema (`signal-center/signals.db`, 675MB)
| Table | Purpose |
|---|---|
| `trade_signals` | Inbound signals; status NEW/EXECUTED/DISMISSED; the canonical signal ledger |
| `signal_outcomes` | Tracks tracked_entry, tracked_high/low, would_hit_tp/sl, theoretical_pnl, actual_pnl per signal_id |
| `signal_history` | Time-series of raw signal values (volume_radar, vix, smart_money, etc.) — 20+ named signals, ~2 entries each per day |
| `predictions`, `prediction_results`, `prediction_accuracy` | Daily snapshot prediction system (tp1/tp2/sl + result checking) |
| `execution_log` | Per-execution audit: signal_id, alpaca_order_id, fill_price, status |
| `intelligence_feed` | Generic key/value feed table |
| `daily_snapshot` | Master score + grade per day |
| `base_rate_features`, `base_rate_ingest_log` | Base-rate analysis substrate |

### Key endpoints (30+ total; AN2-relevant subset)

```
POST /api/signal                          ← write new signal (LRS already uses this)
GET  /api/signals                         ← list signals
GET  /api/signals/active                  ← active (status=NEW)
GET  /api/signals/<id>                    ← single
POST /api/signals/<id>/execute            ← mark executed (signal_id + alpaca_order_id?)
POST /api/signals/<id>/dismiss            ← dismiss
POST /api/predictions/snapshot            ← write daily snapshot
GET  /api/predictions/check               ← outcome check
GET  /api/predictions/top5                ← top picks
GET  /api/quant-signals                   ← read quant feed
POST /api/import                          ← bulk import
```

### POST /api/signal payload (verified via reading server.py:1850-1900)

```json
{
  "symbol": "AAPL",
  "action": "BUY",
  "confidence": 75,
  "type": "SWING",
  "agent": "neo-matrix",
  "model": "8000 / Independent",
  "reasoning": "...",
  "sources": [...],
  "timeframe": "SWING",
  "price": 0.0,
  "stop_loss": 0.0,
  "take_profit": 0.0,
  "context_summary": "..."
}
```

Server INSERTs into `trade_signals` with status=NEW, calls `_update_outcome(signal_id, entry, sl, tp)` to seed outcome tracking, then SSE-pushes to live subscribers.

### Auth model

Session-based (Flask). `session.get("authenticated") is True` gates protected endpoints. Phase 1 bridge handles auth via a shared `_session_cookie` global at `dashboard/app.py:991` (M2M localhost auth) — already working for read endpoints. Same pattern can be reused for write endpoints.

## Phase 1 read bridge (what's already wired)

| Site | Purpose |
|---|---|
| `engine/momentum/bridge.py` | Heartbeat (`check_signal_center_health`) + `fetch_recent_signals`. The HM-AN Phase 1 wiring. |
| `dashboard/app.py:991` | M2M localhost auth helper |
| `dashboard/app.py:11082, 11202` | `SC_DB = signal-center/signals.db` + `/api/signal-center/top` reading directly from the file |
| `dashboard/app.py:18011-18043` | `# === HM-AN Phase 1 ===` endpoint block (heartbeat + recent) |
| `engine/ghost_scoring.py` | Pulls BUY signals (confidence >= 70) from Signal Center DB into `ghost_trades`, scores against Alpaca bars. NO trade execution. |

## Write direction — what's already there

`engine/long_range_sensors.py:27` defines `SIGNAL_CENTER_URL = "http://localhost:9000/api/signal"` and posts whale-volume detections. So one write path exists; trade execution + outcome write-back do NOT.

## neo-matrix state

```
id              neo-matrix
display_name    Neo
provider        matrix
model_id        "8000 / Independent"     ← independent port-8000 service (Plutus-9b)
cash            $7,173.69
is_active       1
halt_mode       exit_only                ← HALTED — entries blocked, exits allowed
halt_reason     "Waiting on HM-AN port-9000 bidirectional bridge.
                 Exit-only until shipped. 2026-05-11"
can_trade_live  0
season          6
crew_role       active
role            production
```

Last 5 trades — all 2026-04-21 NVDA Autopilot RSI trims (`RSI 91 > 80 EXTREME OVERBOUGHT`). 3 weeks dormant. `bridge_vote.py:43` lists Neo as a voter; `paper_trader.py:1531/1595` marks neo-matrix as exempt from allocation policy and forwarded to Alpaca. `crew_scanner.py:249` declares `ACTIVE_SCANNERS = ["neo-matrix"]` — neo-matrix runs every cycle as the always-on, non-Ollama scanner.

## Q1 — scope candidates

### A. Captain manual triggers (~1 hr)
- Add `POST /api/signal-center/manual-signal` to `dashboard/app.py` that proxies to Signal Center `POST /api/signal` with Captain-supplied fields.
- Optionally a dashboard UI button (HM-AO-β-2 frontend session has been raised — could fold there).
- **Value:** unblocks "Captain saw a setup, wants to log it as a signal and let it propagate to consumers"
- **neo-matrix dependency:** none. Doesn't fix the halt.

### B. Trader writes execution outcomes (~1.5 hr)
- Hook `paper_trader.py` close-trade path: when a trade originating from a Signal Center signal closes, POST to `/api/signals/<id>/execute` (or update `execution_log`).
- Requires linking `trades.signal_id` (need to check if this column exists) — likely needs a small schema add to trader.db.
- **Value:** Signal Center gets ground-truth execution outcomes, enabling future predictor scorecard.
- **neo-matrix dependency:** none. Doesn't fix the halt.

### C. neo-matrix consumes Signal Center signals (~2-3 hr) ← Captain's halt_reason explicitly points here
- In `crew_scanner.py:_scan_single_agent` when `player_id == "neo-matrix"`, add a path that:
  1. Fetches `GET /api/signals/active` (status=NEW, confidence ≥ threshold)
  2. For each, runs the existing gate chain (HM-AF cannibalization, BSM ceiling, earnings blackout, conviction, daily-limit, fleet-exposure)
  3. If gates pass and `halt_mode != 'exit_only'`, executes via paper_trader
  4. Either way, log the decision and POST to `/api/signals/<id>/execute|dismiss` with outcome
- Preserve every existing gate per CLAUDE.md.
- **Value:** lifts the literal halt_reason, ends neo-matrix's 3-week dormancy, gives Signal Center signals a real consumer.
- **Captain Q2 attached:** keep `halt_mode='exit_only'` for first observation cycle, or lift?

### D. Full bidirectional (A + B + C, ~4-5 hr)
- Ship A + B + C in sequenced commits.
- **Value:** complete write-side complement.
- **Risk:** larger blast radius; harder to revert one piece if a downstream consumer misbehaves.

## Q2 — neo-matrix halt_mode posture (if Captain picks C or D)

Recommend: **keep `halt_mode='exit_only'` for the first observation window**. With C wired:
- neo-matrix will see Signal Center signals every scan cycle
- Gate decisions log to trader.log
- Would-be entries are blocked by `exit_only` (sells still pass through)
- Captain observes 30-60 min of would-be trades → flips halt_mode manually if behavior looks right
- Single SQL: `UPDATE ai_players SET halt_mode = NULL, halt_reason = NULL WHERE id = 'neo-matrix';`

The alternative (lift halt_mode same-commit-as-C) is faster but harder to roll back if the new code path makes bad calls under live execution.

## Scotty's recommendation

**C alone, with Q2 = exit_only first.** Reasons:
1. Captain's halt_reason text literally names HM-AN bidirectional bridge as the blocker — C resolves that.
2. A + B are valuable but neither addresses the 3-week neo-matrix dormancy.
3. Observation cycle minimizes risk; Captain can flip halt_mode once observed behavior is acceptable.

If Captain wants A or B alongside C, can fold into D (sequenced commits).

## HALT

Awaiting Captain Q1 (A/B/C/D) + Q2 (exit_only first vs lift in same commit). NTFY fired.

## Anchors landed

None — discovery only. AN2.0 budget consumed; no commits this phase.
