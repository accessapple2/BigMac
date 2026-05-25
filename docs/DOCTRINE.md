# USS TradeMinds — Data Preservation Doctrine

> Established 2026-05-25 following the HM-DATA-INTEGRITY-FORENSICS audit
> that surfaced an active "delete-without-archive" endpoint (locked in
> commit `45e57e1`; properly fixed by HM-CLEAN-STALE-ARCHIVE-NOT-DELETE).

---

## Rule #1 — Never delete data

**Trade data is gold. Equity history is gold. Signal history is gold.**

The fleet's value is the audit trail. A trade row, an equity snapshot,
or a signal emission represents a decision the fleet made under a
specific market state — losing that record is losing the ability to
study, calibrate, or recover from it.

Gold tables (non-exhaustive — anything modeling a historical event):
- `trades`
- `signals` / `signals_v2`
- `portfolio_history` / `real_portfolio_history` / `ghost_equity_history`
- `season_history`
- `bridge_votes` / `bridge_consensus` / `debate_history_v2`
- `crew_decisions`
- `player_funding_events`
- Any future per-event log table

Non-gold tables (current-state mirrors, computed indices, caches):
- `positions` (live state — history lives in `trades`)
- `portfolio_positions` (Alpaca sync mirror — history lives in `trades`)
- `rs_rank` / `minervini_trend` (nightly INSERT-replace)
- `premarket_scan` / `gex_levels` / `volume_daily_log` (rolling windows)
- `backtest_equity_curve*` / `extras_*` (per-run, replaceable)

When in doubt: **assume gold.** Promotion from gold to non-gold requires
Admiral signoff.

## Rule #2 — Archive-then-delete pattern

Any endpoint, script, or migration that needs to remove rows from a gold
table MUST use the archive-then-delete pattern:

### Required schema

A paired `<table>_archived` mirror with at minimum these audit-trail columns:

```sql
CREATE TABLE <table>_archived (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    original_row_id     INTEGER NOT NULL,    -- source table's id
    -- ... mirror all data columns from source ...
    archived_at         TEXT NOT NULL DEFAULT (datetime('now')),
    archived_by         TEXT NOT NULL,       -- function/endpoint name
    archive_reason      TEXT NOT NULL,       -- free-form context
    archive_session_id  TEXT NOT NULL,       -- UUID grouping a batch
    restored_at         TEXT                 -- NULL unless restored
);
CREATE INDEX idx_<table>_archived_session ON <table>_archived(archive_session_id);
```

### Required transaction shape

```python
session_id = str(uuid.uuid4())
conn = _conn()
try:
    # 1. SELECT candidates (full row data, not just IDs).
    rows = conn.execute("SELECT ... FROM <table> WHERE ...").fetchall()

    # 2. INSERT each into archive with audit metadata.
    archived = 0
    for r in rows:
        conn.execute(
            "INSERT INTO <table>_archived (original_row_id, ..., "
            " archived_by, archive_reason, archive_session_id) "
            "VALUES (?, ..., ?, ?, ?)",
            (r["id"], ..., "<endpoint_name>", "<reason>", session_id),
        )
        archived += 1

    # 3. DELETE matching rows from source.
    d = conn.execute("DELETE FROM <table> WHERE ...")
    deleted = d.rowcount

    # 4. Consistency check — archived must equal deleted.
    if archived != deleted:
        raise HTTPException(500, f"mismatch archived={archived} deleted={deleted}")

    # 5. Commit only after the check passes.
    conn.commit()
    return {"ok": True, "archived_count": archived,
            "deleted_count": deleted, "session_id": session_id}
except Exception:
    conn.rollback()
    raise
finally:
    conn.close()
```

### Required restore path

Every archive-capable endpoint must have a paired
`restore-from-archive` endpoint that reverses a batch by `session_id`:
- SELECT archived rows for this session where `restored_at IS NULL`
- INSERT each back into the source table (auto-assigned new id)
- UPDATE archive row `restored_at = datetime('now')`
- All in a single transaction; mismatch -> rollback
- Idempotent: re-running the same session_id skips already-restored rows

### Reference implementation

- Schema: `scripts/migrations/hm_clean_stale_archive_not_delete.sql`
- Archive endpoint: `dashboard/app.py::clean_stale_snapshots`
- List endpoint: `dashboard/app.py::list_archived_portfolio_history`
- Restore endpoint: `dashboard/app.py::restore_portfolio_history_from_archive`
- Tests: `tests/test_hm_clean_stale_archive_not_delete.py`

## Rule #3 — Future violations are tickets, not silent fixes

When a `DELETE FROM <gold_table>` is discovered in the codebase:

1. **Bank the finding under HM-DATA-INTEGRITY-FORENSICS as a sub-ticket.**
2. **If the path is reachable in production (live endpoint, scheduled
   job, called from a hot path):** emergency-lock immediately. Acceptable
   exception to the normal merge process — push the lock to main
   directly. Then file the proper archive-then-delete fix as a separate
   ticket. The lock pattern is:
   ```python
   from fastapi import HTTPException
   raise HTTPException(
       status_code=403,
       detail="DISABLED <date> — <ticket-id> doctrine violation pending fix"
   )
   ```
   Preserve the original function body below the lock as forensic evidence.
3. **If the path is dormant (only callable from archive/test scripts,
   never wired to a live endpoint):** file the ticket but don't emergency-
   lock. Fix in normal order.
4. **No silent rewrite.** Every doctrine fix gets a ticket + a commit
   that names the violation it closes.

### Audit cadence

Quarterly: grep `DELETE FROM` / `TRUNCATE` / `DROP TABLE` across the
codebase. Categorize each hit as SAFE / DEFENSIBLE / VIOLATION using
the gold/non-gold list in Rule #1. Bank any new VIOLATION immediately.

## Rule #4 — Never trade on closed markets

The fleet does not generate signals, submit orders, or execute trades
when US markets are closed.

Closed states (per `engine/market_calendar.py::MarketStatus`):
- `CLOSED_WEEKEND` (Saturday, Sunday)
- `CLOSED_HOLIDAY` (NYSE-observed federal holidays — see `US_HOLIDAYS`)
- `CLOSED_EARLY` (past 1pm ET on early-close days — Black Friday,
  Christmas Eve, day before July 4 when applicable)
- `CLOSED_BEFORE_HOURS` (before 9:30 ET)
- `CLOSED_AFTER_HOURS` (after 4:00 ET on regular days)

### Required gates

Every signal-emission and order-submission path MUST consult
`engine.market_calendar.market_closed_reason()` BEFORE producing output.
A non-None return blocks the action and surfaces the reason for logging
and rejection-record bookkeeping.

Hard gates instrumented in HM-MARKET-HOLIDAY-CALENDAR Phase B:
- `engine/paper_trader.py::buy()`
- `engine/paper_trader.py::sell()`
- `engine/paper_trader.py::short_sell()`
- `engine/alpaca_bridge.py::buy()`
- `engine/alpaca_bridge.py::sell()`
- `engine/alpaca_bridge.py::short_sell()`
- `engine/alpaca_options.py::execute_options_signal()` (options-spread fwd path)

Soft updates:
- `engine/risk_manager.py::is_market_hours()` — holiday-aware (returns
  False on NYSE holidays; battle_station + UI widgets pick up the fix
  transparently).
- `dashboard/app.py::fleet_pulse()` — calendar-driven `market_status` +
  `holiday_name` + `next_market_open` fields in API response.
- `dashboard/static/index.html::renderFleetPulse()` — banner shows
  `🛌 HOLIDAY · {NAME}` / `🛌 WEEKEND` distinctly from generic `STANDBY`.

### Required gate pattern

```python
from engine.market_calendar import market_closed_reason as _mcr
_r = _mcr()
if _r is not None:
    # log + return blocked-result with reason captured for forensic record
    log.warning(f"[HM-MARKET-CLOSED] <action> <symbol> blocked — {_r}")
    return None  # or appropriate negative-response shape
```

Extended-hours intent: gates block all non-OPEN states regardless of
extended_hours kwarg. Alpaca rejects pre/post-market non-LIMIT
submissions anyway, and the trader has no doctrine for pre/post-market
trading. Future override possible if needed.

### Reference incident

**HM-MARKET-HOLIDAY-CALENDAR**, Memorial Day 2026-05-25. The trader
fired 6 Alpaca orders + 2 simulated positions on a market-closed day
because production had no holiday calendar (only backtest scripts did,
via `US_HOLIDAYS` constants local to those scripts). Cancellation arc:
- Stage 1 `6cdf9d5` — 6 Alpaca orders cancelled, 0/0 filled
- Stage 2 `c35aa51` — 11 local rows archived (Rule #2 archive-then-delete)
- Stage 3 `02d3558` — `neo-matrix` + `ollie-auto` halted
- Stage 4 Phase A `7d55d35` — `engine/market_calendar.py` created
- Stage 4 Phase B `3cd4838` — 7 hard gates + 1 soft update instrumented
- Stage 4 Phase C `bf54ee8` — dashboard banner holiday-aware
- Stage 4 Phase D (this commit) — Rule #4 codified

### Future maintenance

`US_HOLIDAYS` dict in `engine/market_calendar.py` must be reviewed and
extended annually. Bank a calendar-year-end ticket each December for the
upcoming year. Good Friday + observed-day logic require special
attention (date varies, observation shifts when Saturday/Sunday).

### Banked future work

- **HM-MARKET-CALENDAR-INTERNATIONAL** — non-US markets (London/Tokyo/etc.)
  when the fleet expands abroad.

## Cross-references

- HM-DATA-INTEGRITY-FORENSICS parent ticket — `docs/XO_BACKLOG.md`
- Emergency lock pattern — `45e57e1`
- Archive-then-delete reference — `04b00f3` (schema), `8c9b942` (endpoint),
  `a5054c0` (recovery), `b634dd0` (tests)
- HM-MARKET-HOLIDAY-CALENDAR — `7d55d35` (calendar), `3cd4838` (gates),
  `bf54ee8` (banner), `4588639` (Rule #4 codify)
- HM-RISK-MANAGER-CONVICTION-STOP-WIRE + HM-FLEET-TRAIL-CONVICTION-SCALE —
  `3760345` / `568cb81` / `21a5347` (paired Rule #5 implementations)
- Sacred Data Rule reaffirmation — `CLAUDE.md` ("SACRED DATA RULES" section)

## Rule #5 — Conviction-scaling is symmetric across stop layers

Every stop-loss / trail-stop / options-exit width that the fleet uses
MUST follow the same doctrine when conviction-scaling is introduced:

1. **Same tier table boundaries.** Per-layer width values differ (entry
   stop = 12/15/18%, fleet trail = 3/4/5%), but the conviction
   boundaries (0.80 / 0.90) and the floor invariant (no tier ever
   produces a value TIGHTER than the flat baseline) are shared.

2. **Same allow-list gate.** Only `RiskManager.AI_SIGNAL_PLAYERS`
   players receive conviction-scaled widths. Non-allow-list players
   (alpaca-mirror, enterprise-computer metals tracker, dalio-metals)
   inherit the flat baseline regardless of conviction column state.

3. **Same flag pattern.** Each layer gets its own
   `CONVICTION_SCALED_<LAYER>_ENABLED` env flag, default OFF. Flags are
   INDEPENDENT — Admiral can enable scaled-stops without scaled-trail
   (or vice-versa) to shadow-validate each layer separately before
   coupling.

4. **Same NULL-conviction fallback.** Allow-list player with NULL
   `positions.conviction` (categorical NULL — e.g. legacy row before
   HM-POSITIONS-CONVICTION-DENORM backfill) inherits the flat baseline
   value, NOT the bottom tier. The floor invariant guarantees that
   "no conviction known" never produces tighter stops than "no
   conviction scaling at all".

### Reference paired implementations

| Layer | Helper | Flag | Phase 6 commit |
|---|---|---|---|
| Entry stop-loss (L825 of risk_manager.py) | `engine.stops.get_stop_loss_pct` (0.12/0.15/0.18) | `CONVICTION_SCALED_STOPS_ENABLED` | `568cb81` |
| Fleet trail (L800 of risk_manager.py) | `engine.stops.get_trail_pct` (0.03/0.04/0.05) | `CONVICTION_SCALED_TRAIL_ENABLED` | `21a5347` |

### Still asymmetric (banked for future symmetric ship)

- **Options stop-loss** (`risk_manager.py:738`, uses `opt_sl_pct` const)
  is not yet conviction-scaled. Banked as `HM-OPTIONS-CONVICTION-STOP-WIRE`
  in XO_BACKLOG. Same pattern when it ships: paired helper in
  `engine.stops`, paired flag, paired allow-list gate.

### Doctrine implication for new stop-layer additions

Any NEW stop layer added to the trader must consult this rule before
shipping a flat-rate width. If the new layer applies to AI_SIGNAL_PLAYERS,
it MUST follow the paired-implementation pattern from day one.
