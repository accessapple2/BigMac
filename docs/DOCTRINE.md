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
- HM-RISK-MANAGER-CONVICTION-STOP-WIRE + HM-FLEET-TRAIL-CONVICTION-SCALE
  + HM-OPTIONS-CONVICTION-STOP-WIRE — `3760345` / `568cb81` / `21a5347` /
  `24332ea` (paired Rule #5 implementations — conviction-scaling trio
  fully realized)
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

| Layer | Helper | Tier table | Flag | Phase B commit |
|---|---|---|---|---|
| Entry stop-loss | `engine.stops.get_stop_loss_pct` | 0.12 / 0.15 / 0.18 | `CONVICTION_SCALED_STOPS_ENABLED` | `568cb81` |
| Fleet trail | `engine.stops.get_trail_pct` | 0.03 / 0.04 / 0.05 | `CONVICTION_SCALED_TRAIL_ENABLED` | `21a5347` |
| Options stop-loss | `engine.stops.get_options_stop_pct` | 0.30 / 0.40 / 0.50 | `CONVICTION_SCALED_OPTIONS_STOP_ENABLED` | `24332ea` |

The conviction-scaling trio is fully realized. Doctrine boundaries
(0.80 / 0.90) are shared across all three; per-layer width values
differ; flags are independent so each layer can be shadow-validated
in isolation.

### Floor invariant — exception for options

Rule #5's "no tier produces a width TIGHTER than the flat baseline"
holds for the equity layers (stops + trail). It does **NOT** hold for
the options layer.

The options tier table inverts the direction:
- Top tier (conv >= 0.90) preserves the current 0.50 baseline.
- Low tier (conv < 0.80) is **0.30 — tighter than the 0.50 baseline.**

Rationale (Admiral-locked 2026-05-25, HM-OPTIONS-CONVICTION-STOP-WIRE):
options premium is uniquely vulnerable to theta decay and IV crush.
A tighter stop on a low-conviction option bet cuts capital risk faster
when the original thesis was already uncertain. The high-conviction
band keeps the existing room.

This is the only documented exception to the universal floor invariant.
Any future stop layer (equity, fixed-income, futures, etc.) that wants
to invert the floor must do so via an EXPLICIT amendment to Rule #5
referencing the per-asset decay characteristic that justifies it — not
implicitly through tier-table values that happen to dip below baseline.

### Doctrine implication for new stop-layer additions

Any NEW stop layer added to the trader must consult this rule before
shipping a flat-rate width. If the new layer applies to AI_SIGNAL_PLAYERS,
it MUST follow the paired-implementation pattern from day one.


---

> Relocated from CLAUDE.md (HM-PRIME Part C).

## Doctrine Lessons (distilled from sprint sessions)

### Multi-path scanning is implicit resilience — preserve it deliberately (2026-05-29)
When the arena scan stalls (`run_scan` holds `_scan_lock` unboundedly, §C), the
`crew_scanner` keeps producing signals (`sig#` advances) — the fleet doesn't go
signal-dark. Two independent scan paths (arena/`_SCAN_TIER` + `crew_scanner`) mean
one can hang without total signal-flow loss. This redundancy wasn't designed as
fault-tolerance but functions as it. **Preserve it deliberately:** don't
consolidate the two scan paths into one for "simplicity," and when fixing the §C
stall (HM-RUN-SCAN-WATCHDOG) keep `crew_scanner` independent of the arena lock.
Same family as "alarms must not share a failure mode" — independent paths survive
independent failures.

### Measurement-instrument bugs: boundary-isolate before reporting rates (2026-05-29)
The analysis tooling keeps biting us as badly as the bugs. Two instances: **date-less
log lines** (2026-05-29 — `trader_error.log` `[LRS]` lines carry HH:MM:SS but no date, so
`grep | uniq -c` by hour silently aggregates *multiple days* into one bucket → the "30-53/hr
drift baseline" was multi-day-per-bucket, ~6× the true single-night rate); and **rich-console
wrapping** (2026-05-28 — `console.log` wraps long lines, so naïve `grep`/`wc` of wrapped
output miscounts). **Rule: any time-window rate analysis MUST explicitly state the
day-boundary verification (how the post-restart/today segment was isolated — line offset,
restart marker, contiguous-ascending-timestamp block) BEFORE reporting a rate.** A rate
without a stated boundary method is suspect. Verify the instrument, not just the result.

### Trace EVERY sub-fetch to its leaf before fixing a multi-fetch block (2026-05-29)
When a hang localizes to a block that bundles multiple sub-fetches, trace **every**
fetch to its leaf — or instrument to distinguish them — **before** committing to a fix.
Don't stop at the first HTTP call and assume. HM-RUN-SCAN-WATCHDOG Loop 5B localized the
stall to `build_scan_context`'s `ctx:catalyst` block, traced it to the Finnhub
`/calendar/earnings` call, and shipped a cache+deadline on `_fh_get` — but the real hang
was two levels deeper (`get_earnings_countdown`'s per-symbol Alpha Vantage enrichment +
`get_trending_tickers`'s per-symbol Yahoo loop), so the fix didn't move the needle. The
**no-deadline-trip test** (ctx:catalyst still hung 200s+ with zero `TOTAL-deadline` logs ⇒
the hang is NOT in `_fh_get`) caught the mis-attribution — but only **after** a restart
cycle. **Cost of one wrong-fix restart cycle > cost of one extra instrumentation pass to
confirm the cause.** When a block fans out, split-marker first (same family as
[[boundary-isolate]] and layered-bottleneck). Related: external-fetch-discipline bug class
(unbounded per-item external fetch, cold cache, every agent re-pays) — now at 5 instances
(Loop 1 Yahoo indicators, Loop 3, 5B Finnhub calendar, earnings AV enrichment, trending Yahoo).

### Code comments document INTENT — divergence from implementation is a signal (2026-05-29)
When implementation diverges from a comment, investigate — it's code drift, a wrong
comment, or a design that was never realized. `build_scan_context`'s "Build shared scan
context once per model" comment vs the per-agent-call reality (every one of 19 agents
rebuilds all 14 blocks, re-paying market-wide fetches) is example #1: the comment captured
the *intended* design; the code never implemented the sharing. Aligning code to its own
documented intent (cache the market-wide blocks once per cycle) is the highest-leverage fix.

### Verify the MODEL of the system before designing a fix against it (2026-05-29)
The design model you carry of "how the code is shaped" is itself a verify-before-fix
surface. HM-RUN-SCAN-WATCHDOG Loop 5D proposed "cache 12 market-wide blocks once / 3
agent-specific fresh per agent" — a model assuming every agent computes a unique context.
Reading the actual code showed the real variation is only 3-4 *profile* branches
(energy-arnold / options-model / chekov-gate), so the correct cache key is `(cycle,
profile)` (~4 builds/cycle), not per-block. The right cache key follows the **actual
variation pattern, not the proposed model**. Confirm the model reflects the code before
designing structure against it — same family as [[trace-every-subfetch-to-leaf]] and
boundary-isolate.

### Restart verification: confirm the LISTENER died, not "something on the port" (2026-05-29)
`lsof -ti tcp:8080` returns **every** process with a connection to the port — clients and
child connections too, not just the LISTEN owner. Killing a transient client leaves the real
trader running on **old bytecode** — a *silent restart failure* (the relaunch can't bind, so
it exits, and the stale process keeps serving). Near-miss 2026-05-29: killed PID 22601 (a
client) thinking it was the trader; listener 26349 survived on Loop-5D bytecode; the new
process bound in "2s" (suspiciously fast) — the tell that nothing actually restarted. Caught
by re-checking the LISTEN owner. **Correct pattern:** target the listener specifically —
`lsof -tiTCP:8080 -sTCP:LISTEN` — and verify post-restart that the **new PID differs from the
old listener PID** AND the bind took the normal ~40s (a ~2s bind means the old process never
died). **Restart success = new-PID-bound + old-PID-gone, verified — not assumed.** Same class
as "deployed ≠ working" / [[kickstart-after-backend-merge]]: *restarted ≠ new bytecode active.*
Verify the state transition; don't assume the command did what you intended.

**ESCALATION 2026-05-29 — orphan traders (HM-RESTART-ORPHAN-PREVENTION):** killing only the LISTENER
isn't enough. A process can **free the listener but keep running its scan loop** as an orphan. Today
two traders ran in parallel 2.6h (PID 29543 from 15:15 + the live listener) — the orphan ran OLD code,
double-scanned, and polluted the shared `trader.log` with stale phase lines that made a *correct* fix
look failed → a multi-restart phantom chase. **"Port freed" ≠ "process dead."** Restart MUST: (1) kill
ALL `main.py` procs (`pkill -f main.py` — note the binary is `Python` capitalized, so `grep
python.*main.py` MISSES it; match `main.py`), and (2) confirm **single-writer** via `lsof
logs/trader.log` (exactly one Python PID) BEFORE trusting any post-restart measurement. A shared log
with two writers is a measurement-instrument bug that survives every boundary trick.

### When you can't validate a content filter, bound QUANTITY not content (2026-05-30)
A scan/screen that's too big has two fix shapes: (a) a **content filter** (keep the "relevant" subset)
or (b) a **quantity bound** (process N per cycle, rotate, full coverage over time). Choose by whether
you can VALIDATE the content criteria. The §C floor (McCoy/Dax CSP sellers analyzing all 307) looked
like a content-screen problem — until verify-before-fix found there was **nothing to validate against**:
they'd signaled on ~all 312 symbols (no selectivity to learn from) AND there was no cached
options/IV universe to screen on (a real CSP screen would need the per-symbol fetches we'd just killed).
**A content filter you can't validate risks silent PERMANENT alpha loss** (you drop a profitable name
and never know). **A quantity bound (bounded-rotation) loses nothing** — every name still scans, just
rotated across cycles — and for a SLOW strategy (CSP hold-to-expiry) temporal coverage (~12h full sweep)
is fine; you don't need every name every cycle. Rule: **no known-good-set + no cached universe to filter
on ⇒ bound the quantity, don't guess the content.** (Rotation cursor pattern already exists:
`_ALPHA_PAIR_IDX` in crew_scanner.py — but persist the offset to `settings` so restarts don't re-scan
the head and starve the tail.)

Full session narratives in `docs/CLAUDE-archive-2026-05.md`. Rules below are
load-bearing today.

### §C-arc lessons: deletion, stale paths, and spikes-mask-floors (2026-05-29/30)
Three lessons from the multi-day HM-RUN-SCAN-WATCHDOG arc, consolidated:
- **The cheapest fix to an expensive operation is sometimes deleting it.** §C's biggest infer cost
  (deepseek scanning 307 via LLM) wasn't a perf bug — it was a *path that shouldn't run at all*
  (Spock was converted to deterministic `spock_rules`; the arena LLM was a leftover). Before
  optimizing HOW an expensive path runs, ask whether it should run. deepseek + ollama-coder were both
  free deletions (zero coverage loss).
- **Role-conversions leave stale parallel paths — and they bite repeatedly.** When an agent is
  converted (LLM→deterministic, scanner→bench), the OLD path is often left wired. Bitten ~6× this arc:
  deepseek + ollama-coder (arena LLM leftover after rules-conversion), Worf in 3 roster lists, navigator
  (re-homed to trade-only, old `tractor_beam→save_signal` emitter dropped → dead `signals`). **When you
  convert an agent's role, enumerate + remove ALL old paths, don't just add the new one.** Same family as
  [[agent-state-must-reconcile-across-all-sources]].
- **Spikes mask the floor — fixing a hang reveals the next, slower bottleneck.** §C had layered causes:
  the loud spikes (catalyst 540s, indicators 552s, quote_summary) masked a quiet legitimately-long
  *floor* (analyze-all-307). Each spike-fix unmasked the next layer. **The soak metric (zero HELD>60s) is
  the true closure signal, not "fixed cause X"** — and a remaining "stall" may be a legitimate-but-slow
  floor (fix by bounding/reducing work), not a hang (fix by bounding the call). Distinguish them before
  reaching for a deadline.

### Agent state must reconcile across ALL sources (HM-WORF-DRIFT-RECONCILE, 2026-05-29)
When an agent's state lives in N sources, **all N must reconcile or the system
lies to future-session diagnostics.** Worf (`qwen3-8b-flash`) was benched S6.1
(−0.36%) but still appeared "active" in 6 places — `ADVISORY_CREW` (correct),
`_SCAN_TIER2` (stale), `SNIPER_AGENTS` (stale), `ai_players` active (correct —
load-bearing), the WR provider rotation (correct), and the Fleet Roster doc
("~25 sigs/day", stale). A morning diagnostic wrongly read "in WR rotation +
active" as healthy. **`ADVISORY_CREW` is canonical for benched-but-keeping-for-
bridge-vote agents** = no individual scanning, but `ai_players` stays `active`
(+`is_active=1`,`is_paused=0`) because `war_room.py` skips
`halt_mode!='active'`/`is_active=0`/`is_paused=1` — so an "active" row is
*required* to keep the bridge vote, NOT drift. Deeper bench (no bridge vote
either) = `exit_only`/`is_paused=1` (Uhura, Sulu). Before "fixing" an agent's
`ai_players` state, check whether WR/scan paths depend on it. **Worf reconcile
CLOSED 2026-05-29:** all 3 residual scanner memberships removed —
`_SCAN_TIER2` (main.py, only `ollama-plutus`+`ollama-qwen3` remain),
`SNIPER_AGENTS` (proving_ground.py), and `RULES_SCANNERS` (crew_scanner.py, the 3rd
location, found in the fleet-review sweep). The earlier "Uhura/Troi/Trip still in
`_SCAN_TIER2`" note was itself stale (inverse drift — doc lagged code): live
`main.py:236` had already pruned them. **Lesson:** a roster-drift sweep must enumerate
ALL membership lists (`_SCAN_TIER2`, `SNIPER_AGENTS`, `RULES_SCANNERS`, `ADVISORY_CREW`,
`ai_players`) in one pass — Worf lived in 3 of them and took 2 sweeps to fully clear.

### Diagnostics first (HM-CD-migrate, HM-BP, 2026-05-13)
Never modify production code paths before reading current behavior via
`grep + sqlite + log inspection`. HM-CD-migrate almost shipped as a Polygon
migration based on stale assumptions; real cause (Ollama model swap on every
call) emerged only when ollama-coder logs were read in context. HM-CD-instr
instrumentation was the savior.

### Console init verification (HM-CONSOLE-INIT, 2026-05-13)
`logger.* → console.log` flips are NOT safe defaults. Before flipping, verify
the target module has `from rich.console import Console` and `console =
Console()` at module scope. Runtime-smoke each touched module with
`python -c "import engine.X; engine.X.console.log('test')"` before commit.
`py_compile` catches syntax errors but NOT undefined-name errors.

Cycles can "succeed" from the scheduler's view while never finishing emitting
their lines, if `try/except` swallows the `NameError`. Consider
distinguishing programming-error subclasses (`AttributeError`, `NameError`,
`ImportError`) from operational errors and NTFY-ing the first class.

### Positions table is canonical (HM-STALE-TRIM-OBS-V2, 2026-05-13)
For "is position open" queries, anchor on `positions` table joined to
`trades` for context. Trades-table arithmetic (BUY without matching exact-qty
SELL = stale) produces false-positives on partial scale-outs. Always filter
to `halt_mode='active'` players to exclude halted zombies.

### Ollama keep_alive per-model lookup (HM-CD-migrate, 2026-05-13)
Universal `keep_alive: "30s"` (legacy 16GB constraint) forces full model
reload on every call. Per-model `_HM_CD_KEEP_ALIVE` lookup: high-frequency
models (qwen3:8b=7 agents, qwen2.5-coder:7b=2 agents) get 30m residency;
alpha-squad rotation 10m; rare models keep 30s default. Hardware reality is
RTX 5080 with 16GB VRAM (corrected 2026-05-28 HM-AUDIT-T0 — NOT the "8GB"
previously recorded here; live /api/ps showed 10.6GB co-resident); budget is
two 7–8B models co-resident, but 14B-vs-14B rotation still swaps.

### Three-book broker reconciliation (2026-05-20)
Verify ALL three books (real-money / Alpaca paper / fleet) before declaring
exposure resolved. NVDA "closed on Webull" hid two errors: Webull was
liquidated 5/13 (nothing to close) AND Alpaca paper still held 12.34 sh
ghost. Cross-check pattern: query all three independently, reconcile.

### Frontend Ship Rule (HM-BJ.E4, 2026-05-12)
See "Frontend Ship Rule" section above. Browser hover/click smoke is
mandatory for non-trivial JS changes — repeated here because it's the most
commonly skipped rule.

### Verify data-source tier before locking spec (HM-LESSON, 2026-05-27)
For any spec that names an external API/streaming source as "available",
verify the actual tier capability BEFORE locking the spec. A live probe of
auth + first subscribe is a 60-second test that prevents downstream rework.
Polygon Stocks Starter ($29) was assumed to include WS trades; live probe
found it does not. Pivot to Alpaca IEX cost zero $ but one module rewrite.
See `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`.


---

> Relocated from CLAUDE.md (HM-PRIME Part C).

## Error Handling Posture (established 2026-05-05)

After HM-Z (BTO bug, commit 306dcf6) and HM-AA (empty-body errors, commit
a9d0649) surfaced two silent-failure cases in 12 hours, the posture is:

**1. Bare `except Exception` is acceptable when the handling correctly
accommodates unknown failures.** Bare except is *not* the bug. The BTO bug
was a bug because the handling (return error dict; caller misread it) was
wrong. Some places legitimately want broad catch — per-agent cycles where
one agent's crash shouldn't take down the fleet. When you write `except
Exception as e:`, ask: "if `e` is a programming error (AttributeError,
ImportError, NameError) instead of an operational error (APIError,
ConnectionError), does my handler do the right thing?" If the handler treats
those identically and that's wrong, narrow the except.

**2. Error logs capture type + repr, not just str.**
```python
# Avoid:
except Exception as e:
    console.log(f"foo error: {e}")
# Prefer:
except Exception as e:
    console.log(f"foo error: {type(e).__name__}: {e!r}")
```

**3. NTFY on first occurrence per error class per day for architecture-class
paths.** Architecture-class paths:
- Every broker-submit code path (`submit_*` in `alpaca_options.py`, future
  webull/IBKR adapters)
- `halt_mode` writes (anything transitioning an `ai_players` row's halt state)
- Position-of-record writes (mutations to `positions`, `options_trades`,
  sync destinations)

Threshold: first occurrence of an error class within a 24h window NTFYs.
Subsequent same-class occurrences within window suppress (avoid alert
fatigue). New class within window = new NTFY. Window resets at midnight.

**Caveat (Day-2 lesson 2026-05-05):** `engine.alert_channels._rate_state` is
in-memory per process. `rate_limit_secs=86400` means "first per error class
per process lifetime", not per 24h wall-clock. Service restarts reset dedup.
Persist-to-settings deferred.

Non-architecture paths (legacy fleet signal cycles, Polygon timeouts, Ollama
timeouts, transient noise) do NOT NTFY. Log only.

**4. Going-forward, not retroactive.** Hundreds of `except Exception as e:`
blocks exist. Posture applies to new code changes that touch exception
handling and to paths where we discover and investigate a real error. Old
code stays as-is until naturally touched.
