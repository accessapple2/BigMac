# Audit #6X Investigation — May 4, 2026
*Conducted by Scotty (Claude Code Opus 4.7), read-only investigation. ~90 min.*
*Investigates the REAL gate-flip prerequisite identified in `AUDIT_6_INVESTIGATION_2026-05-04.md` as out-of-scope.*

---

## Executive Summary

1. **The signal-center scorecard system is healthy and producing real data.** Endpoint `/api/signals/scorecard` at `127.0.0.1:9000` returns HTTP 200 in ~19ms. `signals.db::trade_signals` has 1,147 rows; `signal_outcomes` has 1,147 rows (**100% coverage**, 1:1). Last writes: `trade_signals` 2026-05-04T07:00:44 (~40 min ago at investigation time); `signal_outcomes` 2026-05-04T07:45:46 (~5 min ago). The outcome-tracker background daemon runs continuously (15-min interval during market hours).
2. **No production consumer of `/api/signals/scorecard` exists.** Endpoint is wired but unconsumed. Dashboard `/api/signal-center/top` reads `signals.db::intelligence_feed` directly (not the scorecard); the dashboard's "Ghost Scorecard" panel calls a *different* endpoint (`/api/ghost/scorecard` over `engine/ghost_trader.py`). The signal-center scorecard is a self-contained working system with no UI rendering it.
3. **Gate-flip readiness verdict: ⚠ PARTIAL.** Data quality is gate-flip ready. Frontend integration is not. Per `UX_SPRINT_2026-04-28.md` Priority 1, the "calibration column" was meant to extend `/api/agents/scoreboard` (a *different* endpoint at `dashboard/app.py:17438`, port 8080) — that endpoint reads `data/trader.db::trades`, not `signals.db`. **The two scorecard systems live on different DBs, different ports, different tables, and were never linked.** Building the calibration column requires either a new endpoint over `signals.db::trade_signals + signal_outcomes`, or extending `/api/agents/scoreboard` with a join across DBs.
4. **`UX_SPRINT_2026-04-28.md` Priority 1 cites a non-existent table.** The doc says "Calibration data source: Already exists at signals.db ghost_predictions table." **`ghost_predictions` does not exist.** The closest tables are `predictions` (725 rows, last write 2026-05-03), `prediction_results` (73), `prediction_accuracy` (4). Either the UX_SPRINT was speculative about a planned name, or `ghost_predictions` was renamed at some point. The actual calibration data is in `signal_outcomes.would_hit_tp` joined to `trade_signals.confidence`.
5. **Three independent agents (`tractor-beam`, `chekov`, `navigator`) drive 63% of the scorecard data** (730 of 1,147 signals). Per the UX_SPRINT diagnostic snapshot, these were the agents flagged as underperforming. Today's data: hit_tp rates 34-35% across all three, avg_pnl +1.74% to +2.61%. **The scorecard system can answer the calibration question right now via direct SQL** — only the panel to render it is missing.

**Recommendation summary:** Gate-flip is not data-blocked. It is *frontend-blocked*. The scorecard system has been quietly producing trustable calibration data for weeks. Pick a frontend strategy (extend existing `/api/agents/scoreboard` with `signals.db` cross-join, or build a new `/api/agents/calibration` endpoint, or consume `/api/signals/scorecard` directly from a new dashboard panel) and ship a leaderboard column. Effort: medium (4-6 hr).

---

## What We Investigated

| Aspect | Location |
|---|---|
| Endpoint | `signal-center/server.py:2121` — `@app.route('/api/signals/scorecard', methods=['GET'])` |
| DB | `signal-center/signals.db` (668 MB) |
| Tables | `trade_signals`, `signal_outcomes` |
| Service | `com.trademinds.signal-center` PID 53068, listening on `127.0.0.1:9000` (Thu 06:00 boot) |
| Outcome tracker | `signal-center/server.py:467` — `_outcome_tracker_loop()`, 15-min interval, weekday-only, 6 AM–8 PM local |
| Writers | `server.py:1874` (INSERT trade_signals), `server.py:517` (INSERT signal_outcomes), `server.py:538` (UPDATE signal_outcomes) |

---

## State of the System

### `trade_signals`

**Schema (verbatim from sqlite3):**

```sql
CREATE TABLE trade_signals (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    type          TEXT NOT NULL DEFAULT 'SWING',
    symbol        TEXT NOT NULL,
    action        TEXT NOT NULL,
    entry_price   REAL,
    stop_loss     REAL,
    take_profit   REAL,
    confidence    INTEGER,
    agent_name    TEXT,
    model_used    TEXT,
    reasoning     TEXT,
    context_json  TEXT,
    sources_json  TEXT,
    timeframe     TEXT DEFAULT 'SWING',
    status        TEXT NOT NULL DEFAULT 'NEW',
    created_at    TEXT DEFAULT CURRENT_TIMESTAMP,
    executed_at   TEXT,
    dismissed_at  TEXT
);
```

**Row count:** 1,147 (confirmed via `SELECT COUNT(*)`).

**Last write timestamp:** `2026-05-04T07:00:44.059121` (~40 min before investigation).
**First write timestamp:** `2026-04-14T07:22:55.334151` (~20 days of history).

**Status breakdown:**
| status | count |
|---|---:|
| RESOLVED | 598 |
| EXPIRED | 353 |
| NEW | 175 |
| AMBIGUOUS | 21 |

**Writer (single):** `signal-center/server.py:1874-1890` — POST endpoint that accepts a JSON payload and INSERTs. Triggered by external agents posting signals to port 9000.

### `signal_outcomes`

**Schema (verbatim):**

```sql
CREATE TABLE signal_outcomes (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    signal_id           INTEGER NOT NULL,
    tracked_entry       REAL,
    tracked_high        REAL,
    tracked_low         REAL,
    tracked_current     REAL,
    would_hit_tp        INTEGER DEFAULT 0,
    would_hit_sl        INTEGER DEFAULT 0,
    theoretical_pnl     REAL,
    actual_pnl          REAL,
    tracking_start      TEXT DEFAULT CURRENT_TIMESTAMP,
    last_updated        TEXT DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (signal_id) REFERENCES trade_signals(id)
);
```

**Row count:** 1,147 (1:1 coverage with `trade_signals`).

**Last write timestamp:** `last_updated = 2026-05-04T07:45:46.307906` (~5 min before investigation).
**First tracking start:** `2026-04-24T19:48:57`.

**Outcome distribution:**
| | count |
|---|---:|
| `would_hit_tp = 1` | 334 (29%) |
| `would_hit_sl = 1 AND would_hit_tp = 0` | 264 (23%) |
| Neither | 549 (48%) |
| **Total** | **1,147** |
| Avg theoretical_pnl | +2.107% |

**Writer:** `server.py:517` (initial INSERT when a new signal arrives).
**Updater:** `server.py:538` (UPDATE during outcome-tracker poll).

### `/api/signals/scorecard` endpoint

**Path:** `GET /api/signals/scorecard?days=30` (configurable window via query param).

**HTTP status:** 200 ✅
**Response size:** 1,572 bytes (default `days=30` window).
**Latency (warm):** ~19 ms (`time curl` measured).
**Latency (days=90):** ~15 ms.

**Sample response (verbatim from curl):**

```json
{
  "by_type": [...4 rows: SWING, etc...],
  "by_agent": [...13 rows...],
  "overall": {
    "total": 1147,
    "hit_tp": 334,
    "hit_sl": 264,
    "avg_pnl": 2.1069,
    "total_pnl": 2416.67
  },
  "missed_count": 0,
  "missed_top": [...]
}
```

**Top 5 agents by signal count (from response):**

| agent_name | total | hit_tp | hit_tp% | avg_pnl |
|---|---:|---:|---:|---:|
| tractor-beam | 268 | 92 | 34.3% | +1.74% |
| chekov | 232 | 81 | 34.9% | +2.61% |
| navigator | 230 | 78 | 33.9% | +2.52% |
| morning_briefing | 169 | 55 | 32.5% | +1.69% |
| morning_briefing_premarket | 57 | 26 | 45.6% | +2.82% |

*(For comparison, UX_SPRINT_2026-04-28.md cited chekov PF 0.86, tractor-beam PF 0.63, navigator PF 0.20 from a snapshot taken on 2026-04-25. Today's avg_pnl values differ because they're computed from outcomes including unresolved signals — a different denominator. The actual PF requires separating `gross_win / abs(gross_loss)`, which the current scorecard SQL does not compute.)*

### Outcome tracker daemon

**Source:** `signal-center/server.py:467-555`, started as `daemon=True` thread at line 559.

**Cadence:** `time.sleep(900)` — fires every 15 minutes.
**Active hours:** weekdays only, 6 AM–8 PM local (`now.weekday() < 5 and 6 <= now.hour < 20`).
**Per cycle:**
1. Pulls open signals from last 7 days (`status IN ('NEW', 'EXECUTED')`).
2. Fetches current prices from yfinance (`yf.Tickers(...).fast_info.last_price`).
3. Updates `signal_outcomes`:
   - Tracks `tracked_high` / `tracked_low` / `tracked_current`
   - Sets `would_hit_tp = 1` if `tracked_high >= tp`
   - Sets `would_hit_sl = 1` if `tracked_low <= sl`
   - Freezes `theoretical_pnl` at TP/SL price the moment the flag flips
4. When TP/SL flag flips, marks the parent `trade_signals.status = 'RESOLVED'`.

**Health evidence:** Last `signal_outcomes.last_updated` is 5 min before investigation (`2026-05-04T07:45:46`). 1,147/1,147 rows have outcome tracking. The daemon is alive and writing on cadence.

### Dashboard consumers

**No dashboard panel currently consumes `/api/signals/scorecard` (port 9000).** Verification:
- `grep -rn "/api/signals/scorecard" dashboard/` — only matches `signal-center/server.py.bak*` (backup files of the same server).
- `dashboard/static/index.html` "Ghost Scorecard" panel (line 9591, `id="section-ghost-scorecard"`) calls `fetchGhostScorecard()` which hits `/api/ghost/scorecard` — a *different* endpoint at `dashboard/app.py:16960` over `engine/ghost_trader.py::get_scorecard`.
- `dashboard/static/index.html:2859` calls `/api/ready-room/scorecard` — yet another different endpoint.
- The dashboard *does* read `signals.db::trade_signals` directly (`dashboard/app.py:10986, 10997, 11048`) for the `tractor-beam/chekov/navigator` BUY-feed at `/api/signal-center/top` (line 11092) — but only for *signal listing*, not for *scorecard aggregates*.

**Net:** the scorecard endpoint at port 9000 is a fully-functional, fully-populated, quietly-running orphan. No HTML/JS/Python consumer hits it.

---

## Gate-Flip Readiness Assessment

**Verdict: ⚠ PARTIAL — ready on the data side, blocked on the frontend side.**

### What's ready ✅

- Endpoint exists, returns 200, fast.
- DB has 20 days of signal history with 100% outcome coverage.
- 13 agents are being tracked, including the three (`tractor-beam`, `chekov`, `navigator`) flagged in UX_SPRINT Priority 1.
- Outcome tracker daemon is alive and writing; data freshness is ~5 minutes.
- Gross_win / gross_loss / hit_tp / hit_sl primitives needed for Sharpe / Sortino / max-drawdown / calibration columns are all available via straightforward SQL over `trade_signals + signal_outcomes`.

### What's missing ⚠

1. **No dashboard panel renders the scorecard data.** UX_SPRINT promised "Calibration column: 'When agent says 0.85 confidence, realized hit rate was X%'" — that column does not exist anywhere in the dashboard.
2. **`/api/agents/scoreboard` (the endpoint UX_SPRINT named) reads from `data/trader.db::trades`**, not `signals.db::trade_signals`. The two databases are not joined anywhere. UX_SPRINT's plan was to extend this endpoint, but it would require a cross-DB read (sqlite ATTACH DATABASE, or a Python join, or a new endpoint).
3. **The currently-served `/api/agents/scoreboard` endpoint produces a different scoreboard.** It pulls from `data/trader.db::trades` (the Alpaca-paper trades, not the SWING signals). Its current scoreboard reflects Alpaca-paper closed positions, not `tractor-beam/chekov/navigator` signal outcomes.
4. **No `ghost_predictions` table exists.** UX_SPRINT named it as the "Calibration data source" but it doesn't exist in `signals.db`. The actual data is in `signal_outcomes.would_hit_tp` joined to `trade_signals.confidence` — different schema, different name.
5. **PF (profit factor) is not computed in the current scorecard endpoint.** The endpoint returns `hit_tp`, `hit_sl`, and `avg_pnl`, but PF requires `gross_win / abs(gross_loss)`. UX_SPRINT cited PF numbers (chekov 0.86, etc.) that came from a one-off diagnostic, not this endpoint. To put PF in the leaderboard, the SQL needs `SUM(CASE WHEN theoretical_pnl > 0 THEN theoretical_pnl ELSE 0)` and corresponding loss aggregator.

### What's NOT blocking gate-flip

- Halted-player rows (audit #1): scorecard system is on a separate DB (`signals.db`, not `data/trader.db`). HM-C's halted_emit filter is irrelevant to this scorecard. Verified: no halted-player concept exists in `trade_signals` schema.
- Service availability: signal-center is healthy, has been running since Thursday.
- Data freshness: 5 minutes old at investigation.

### Effort to close the partial-readiness gap

| Path | Effort | Risk |
|---|---|---|
| **A. Extend `/api/agents/scoreboard`** with `signals.db` cross-DB read (sqlite ATTACH or Python join) | 3-5 hr | Medium — cross-DB query is not the existing pattern; need to handle missing-data case |
| **B. Build new `/api/agents/calibration` endpoint** that reads only `signals.db::trade_signals + signal_outcomes`, returns Sharpe/Sortino/PF/calibration buckets | 4-6 hr | Low — additive endpoint, no existing behavior changes |
| **C. Add a dashboard panel that consumes `/api/signals/scorecard` directly** (uses signal-center port 9000) | 2-3 hr | Low — JavaScript fetch, render, no new SQL |

**Recommendation:** Option B (new endpoint over `signals.db` only). Cleanest separation: dashboard reads from `data/trader.db` for trades, dashboard reads from `signals.db` for signals — two endpoints, two queries, no cross-DB ATTACH. Frontend gets one extra fetch call, gains the columns UX_SPRINT promised.

---

## Open Questions for the Admiral

1. **Does the gate-flip require the frontend column, or does it just require the data to exist for manual SQL inspection?** Investigation confirms the data is solid. If gate-flip's "calibration check" is "Scotty runs a SELECT and inspects", we're ready today. If it's "the leaderboard panel shows hit-rate-by-confidence-bucket and the Admiral eyeballs it before flipping", we have 4-6 hours of frontend work.
2. **Which Option (A / B / C) for closing the partial-readiness gap?** B is recommended; A is faster if you want everything in one endpoint; C ships a new panel with minimum SQL.
3. **`UX_SPRINT_2026-04-28.md` cites a non-existent `ghost_predictions` table.** Update that doc to reference the real source (`trade_signals + signal_outcomes`)? Or leave it as a known-stale planning artifact?
4. **Is `/api/agents/scoreboard` (port 8080, over `data/trader.db::trades`) the right surface for the calibration column?** That endpoint scores the *Alpaca paper trades* — closed positions with realized P&L. The signal-center scorecard tracks *theoretical* outcomes against TP/SL targets for SWING signals. Different things. If the leaderboard column is supposed to be about *agent skill at picking SWING setups*, it belongs in a new endpoint over `signals.db`. If it's about *agent skill at making Alpaca-paper trades that close green*, the existing `/api/agents/scoreboard` is already right and just needs Sharpe/Sortino/calibration math added to its existing SQL.
5. **Should `signals.db` outcome tracker continue using yfinance, or should we add the Polygon.io feed (Pending TODO in CLAUDE.md)?** Currently yfinance is rate-limited and best-effort (`info.last_price` can fail silently). If gate-flip leans heavily on outcome accuracy, paying $29/mo for Polygon's tighter feed is the obvious upgrade.
6. **The orphaned `predictions` table (725 rows, last write 2026-05-03) — what fills it, what reads it, is it relevant to gate-flip calibration?** Out of scope for #6X but worth a 30-min separate look.

---

## Coverage Gaps

This investigation deliberately did not look at:

- **`predictions` / `prediction_results` / `prediction_accuracy` tables** in `signals.db` — exist with data, no clear consumer in dashboard. Out of #6X scope.
- **`signal_history` (79,805 rows)** — appears to be a write-only audit log; not investigated.
- **`base_rate_features` (205,348 rows)** — clearly a separate machine-learning feature store, not directly part of the scorecard.
- **`intelligence_feed` (15,315 rows)** — fed via SCREENER and VOLUME_SPIKE pipelines, consumed by `dashboard/app.py:11092` for `/api/signal-center/top`. Confirmed working but not deeply investigated.
- **The 668 MB size of `signals.db`** — flagged as anomalous (3× larger than `data/trader.db`'s 224 MB). Worth a separate space-audit pass to identify the heaviest table; not in scope for #6X.
- **The single backup `signals.db.bak.20260502` that we just removed in HM-G** — that file's deletion does not affect this audit (the live `signals.db` is intact and writing).
- **Whether `dashboard/app.py:11092` `/api/signal-center/top` (intelligence_feed reader) is the panel UX_SPRINT meant to extend** — the doc names `/api/agents/scoreboard` specifically, so this investigation took the doc at its word. If UX_SPRINT actually meant `/api/signal-center/top`, the readiness picture changes.
- **Latent bug from #6A:** `engine/brain_context.py:156` `s.get("symbol")` vs schema `ticker`. Not fixed. Not relevant to #6X.

---

## Citations

**Code references (file:line):**

- `signal-center/server.py:2121` — `@app.route('/api/signals/scorecard', methods=['GET'])` — the endpoint
- `signal-center/server.py:2125-2189` — full SQL of the scorecard endpoint (by_type, by_agent, overall, missed_top)
- `signal-center/server.py:1874` — `INSERT INTO trade_signals` (the writer)
- `signal-center/server.py:517` — `INSERT INTO signal_outcomes` (initial outcome row)
- `signal-center/server.py:538` — `UPDATE signal_outcomes` (outcome tracker update)
- `signal-center/server.py:467-555` — `_outcome_tracker_loop()` daemon
- `signal-center/server.py:559` — daemon thread start
- `dashboard/app.py:11092-11175` — `/api/signal-center/top` (reads `signals.db::intelligence_feed` directly, NOT `trade_signals`)
- `dashboard/app.py:10986, 10997, 11048` — direct reads of `signals.db::trade_signals` for the dashboard's tractor/chekov/navigator BUY feed
- `dashboard/app.py:17438` — `/api/agents/scoreboard` (over `data/trader.db::trades`, NOT signals.db) — the endpoint UX_SPRINT_2026-04-28 named
- `dashboard/static/index.html:9591` — "Ghost Scorecard" panel (consumes `/api/ghost/scorecard`, a *different* endpoint)

**DB queries + results (signal-center/signals.db):**

```
SELECT COUNT(*) FROM trade_signals;                      → 1147
SELECT MAX(created_at), MIN(created_at) FROM trade_signals;
                                                         → 2026-05-04T07:00:44 / 2026-04-14T07:22:55
SELECT COUNT(*) FROM signal_outcomes;                    → 1147
SELECT MAX(last_updated) FROM signal_outcomes;           → 2026-05-04T07:45:46
SELECT MIN(last_updated) FROM signal_outcomes;           → 2026-04-24T19:48:57
SELECT status, COUNT(*) FROM trade_signals GROUP BY status;
                                                         → RESOLVED 598, EXPIRED 353, NEW 175, AMBIGUOUS 21
SELECT SUM(would_hit_tp), SUM(would_hit_sl AND NOT would_hit_tp), SUM(NOT would_hit_tp AND NOT would_hit_sl), AVG(theoretical_pnl) FROM signal_outcomes;
                                                         → 334, 264, 549, +2.107%
SELECT COUNT(*) FROM predictions;                        → 725
SELECT MAX(created_at) FROM predictions;                 → 2026-05-03T13:05:42
```

**Endpoint smoke test (curl):**

```
GET http://127.0.0.1:9000/api/signals/scorecard
→ HTTP 200, 1572 bytes, ~19ms warm

GET http://127.0.0.1:9000/api/signals/scorecard?days=90
→ HTTP 200, ~15ms warm

GET http://127.0.0.1:8080/api/agents/scoreboard
→ HTTP 200, 5115 bytes, ~125ms (cold)
```

**Service status:**

```
$ launchctl list | grep signal-center
53068    -15    com.trademinds.signal-center

$ ps aux | grep signal-center
PID 53068, started Thursday 06:00, ~4 days uptime
4:02 minutes CPU time

$ lsof -iTCP -sTCP:LISTEN -n -P | grep 9000
Python  53068  bigmac  TCP 127.0.0.1:9000 (LISTEN)
```

**Doc references:**

- `docs/UX_SPRINT_2026-04-28.md:21-46` — Priority 1 (Risk-adjusted Leaderboard) verbatim
- `docs/AUDIT_6_INVESTIGATION_2026-05-04.md` — predecessor investigation that flagged this as out-of-scope
- `CLAUDE.md` (Pending TODOs) — Polygon.io activation that would tighten outcome-tracker price feed

— Lt. Cmdr. M. Scott, 2026-05-04 09:30 MST
