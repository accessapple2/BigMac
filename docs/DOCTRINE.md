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

### Reboot posture, proven not assumed (2026-07-05)
A real planned power-cycle surfaced that `status_page` (status.ollietrades.com)
had neither crash-respawn nor healthcheck coverage and failed to auto-start —
full boot-inventory table, root cause, and the fix (LaunchDaemon promotion +
healthcheck wiring) live in [`docs/REBOOT_POSTURE.md`](REBOOT_POSTURE.md).

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

### Guarded+honest is the only decision-grade backtest number (2026-07-04)
`fleet_realism_sweep.py` (HM-FLEET-REALISM-SWEEP 2026-07-03/04) ran every
roster agent's full signal history through `backtest_player` in both modes.
Raw-mode "returns" are compounding artifacts of unbounded re-entry — e.g.
options-sosnoff raw = +22,389% return_pct vs guarded (honest, reentry-limited,
cost-modeled) +25.23%. Any number without both guardrails and the cost model
applied is not decision-grade, full stop. **Rule: cite guarded+honest only.**
Raw-mode is diagnostic (it's how spam_rate_pct is derived) — never a
performance claim.

Every frontier cloud-API agent (claude-sonnet, claude-haiku, gpt-4o,
gemini-2.5-pro, gemini-2.5-flash, grok-3) posted <9% guarded-honest return
with 48-84% spam rates on this task — underperforming the local/self-hosted
agents. **Correction on first draft of this note:** the top-5 guarded-honest
slots are NOT confirmed "Qwen family" — checking `ai_players.model_id` (not
the agent id/display_name, per `config.py`'s own HM-MODEL-CONFIG-STALENESS
warning) shows ollama-qwen3 and mlx-qwen3 both actually run `ministral-3:3b`
despite their names; only options-sosnoff runs true `qwen3:8b`; ollama-plutus
runs the custom `plutus-v1` fine-tune; dayblade-sulu is rule-based, no LLM in
its signal loop. The real, verified claim: **local/self-hosted models (Qwen,
Ministral, and the custom plutus fine-tune) dominate the top slots; every
frontier cloud-API agent underperforms them on this task.** Spam rate (raw
reentry_blocked/signals_tested) tracks inversely with guarded performance:
qwen3-8b-flash 0% spam / 83.3% WR vs ollama-local 85.1% spam / 30% WR.
Lesson: verify `model_id` from the DB before naming a model family in
doctrine — the agent id/display_name is not reliable evidence of what's
actually running (same trap `HM-MODEL-CONFIG-STALENESS` already documented,
just rediscovered in a different analysis).

**Clean-window re-run (2026-07-04, `fleet_realism_sweep_clean_20260704_213532.json`,
signals >= 2026-05-14 only):** As of 2026-07-04, no fleet ranking is
trustworthy — 17/22 agents have zero post-GATE-0 signals. All tier
assignments are provisional pending forward data. The July 24 kill gate is
the first evaluation on fully clean data; nothing before it should be cited
as a performance baseline.

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

### A realism model must mirror the config in force at emit time, not today's (HM-BACKTEST-REALISM-FIX, 2026-07-04)
**Backtest replay must mirror the dispatch pipeline AND the config in force
at emit time; a realism model that applies today's budgets to yesterday's
signals is a second lookahead bias.** HM-BACKTEST-REALISM (2026-07-03) added
a dispatch-staleness check to `engine/backtester.py` to fix exactly this
class of bug (the backtester replaying every signal regardless of whether
live dispatch would have let it expire) — but the fix itself replayed
`events_bus._STALE_BUDGET_S`'s *current* value (`swing` just widened
30s→3600s in the same commit) against historical signals that lived under
the *old* 30s rule, and the SELECT feeding it never even fetched the
signal's real `timeframe` column, so it silently fell back to a hardcoded
assumption. Net effect: the mechanism was inert (never expired anything)
and, had it fired, would have graded pre-cutover history against a budget
that didn't exist yet. Two independent lessons, both required: (1) a
realism/parity model needs an emit-time-aware lookup of whatever config
governed dispatch at that historical instant, not the module's present-day
constant; (2) verify the model's own input data before trusting its output
— `expired_pre_dispatch=0` looked like "nothing ever expires" when it
actually meant "this check can't fire." See also the separate finding from
the same fix: even corrected, the staleness *poll-race* model only applies
to a source genuinely dispatched async via `signals_v2` →
`events_bus_consumer` — applying it to a source confirmed (empirically, via
`trades.signal_id` → `signals.created_at` latency) to dispatch
`save_signal()` → `buy()` synchronously coin-flips real historical trades on
their timestamp's seconds digit, a THIRD unrelated bias masquerading as
realism. `ENFORCE_STALENESS` and `ENFORCE_REENTRY` are now split flags for
exactly this reason — same family as [[verify-the-model-of-the-system]].

### Roster quality is enforced at the door, not by periodic culls (HM-ROSTER-CAP, 2026-07-04)

This is the third cull cycle in roughly two months (deepseek 2026-06-07,
Webull book 2026-05-13, the Tier 3 cut the day before this entry) — three
separate occasions where roster bloat was discovered late and fixed by a
one-time sweep. A periodic cull is a symptom, not a cure: it treats the
roster ceiling as something to re-derive under pressure instead of an
always-on invariant. **The fix is a structural bar, not another cleanup.**

Two mechanisms, both load-bearing:

1. **Hard cap (`config.MAX_ACTIVE_AGENTS = 8`).** Enforced in
   `setup_db.py`'s startup roster check — the same unconditional-enforcement
   mechanism that already reverts runtime `model_id` edits every boot, so it
   runs whether or not anyone remembers to check. It never auto-halts an
   already-active agent (that decision stays with the Admiral); it only
   blocks a *new* seat from defaulting to active once the roster is already
   at cap, and it logs loudly, every startup, for as long as the roster sits
   over cap.
2. **Audition gate (`config.AUDITION_CRITERIA`).** No agent activates or
   reactivates without a passing paper audition on clean-window data
   (`clean_window_start` — same cutoff as `engine.trades_filter.GARBAGE_FLOOR`
   / `engine.crew.weekly_tuning_crew.CLEAN_CUTOFF`): ≥20 guarded trades,
   spam_rate_pct < 30%, positive honest guarded return, friction_to_pnl <
   0.15. Auditions run in tracking/shadow mode — signals logged, gated from
   execution — so a failed audition costs nothing.

**One-in-one-out is the anti-bloat mechanism, not a suggestion.** Once the
roster is at cap, activating a new agent requires naming the incumbent it
replaces, with a clean-window head-to-head in the proposal. No exceptions —
without this, the cap alone only stops growth past 8, it doesn't force
anyone to justify *which* 8.

No agent trades without passing a clean-data audition; the cap forces every
addition to name its replacement. That sentence is the whole doctrine: a
check that runs automatically, every startup, beats a rule that has to be
remembered and re-applied under pressure each time bloat is noticed.

### Both mechanisms wired live (HM-AUDITION-SCORING, 2026-07-05)

1. **Cap now genuinely counts EXECUTING agents only.** `setup_db.py`'s
   `active_count` query excludes `engine.trades_filter.TRACKING_PLAYERS`
   (dalio-metals, enterprise-computer, schwab) — those seats never place a
   fleet order, so they never consumed a slot. `halt_mode='exit_only'`
   (draining/wind-down agents, e.g. gemini-2.5-flash until its last open
   position closes) was already excluded by construction (`halt_mode=
   'active'` filter) — no code change needed there, just made explicit in
   the comment. Verified against live `data/trader.db` (read-only): active
   count drops from 15 to 14 once enterprise-computer (a tracking-route
   seat with halt_mode='active') is excluded.
2. **`weekly_tuning_crew.py` now runs a real audition pass** (`_run_auditions`,
   Agent 4). Every non-executing candidate (halt_mode != 'active', excluding
   humans/manual-desks, bakeoff audit-trail clones, the broker mirror, the
   structural exit-only guardian, and tracking-route players) is scored
   against `config.AUDITION_CRITERIA` using the exact
   `fleet_realism_sweep_clean_window.py` methodology
   (`backtest_player(start_date=CLEAN_CUTOFF, ...)`), and gets a
   `pass`/`fail`/`insufficient_data` row written to `model_adjustments`
   (`adjustment_type='audition_proposed'`) every week. "Accumulate across
   weeks" needs no separate counter — the backtest always replays from
   `CLEAN_CUTOFF` through "now," so a candidate's trade count grows on its
   own as real history accrues. Each proposal row's `reason` includes a
   `REPLACES: <blank>` field per the one-in-one-out doctrine above — the
   Admiral fills in the incumbent (or `NONE` for an empty slot) before
   running the activation SQL. Dry-run verified against a throwaway copy
   of `data/trader.db`: numbers for deepseek-7b-grok4/ollama-coder/
   ollama-qwen3 matched `fleet_realism_sweep_clean_window.py`'s own report
   exactly, confirming the reimplementation is correct.

**Known scope gap, reported not papered over:** an audition only gains new
data while its candidate is still being scanned — a `halt_mode='full'`
agent's clean-window numbers are frozen at whatever it produced before
being cut, because `build_all_providers` never even instantiates a `full`
player (Inconsistency map: `drafts/AGENT-RULES-REVIEW-2026-07-03.md` §6).
The doctrine text above describes auditions running "in tracking/shadow
mode — signals logged, gated from execution," but no generic mechanism
exists today that lets a candidate scan and emit signals while being
structurally blocked from executing a BUY; the only working example is
`ollie-machine`'s bespoke SIM loop. Building that generic shadow-scan gate
(a fourth halt state, or a `can_trade_live`-style flag enforced at the
`paper_trader` buy path independent of `halt_mode`) is unbuilt — ticketed
in `docs/XO_BACKLOG.md`.

**Also discovered while building this: the clean-window sweep methodology
has a blind spot.** `fleet_realism_sweep.py` / `_clean_window.py` and this
audition scorer all measure activity via the `signals` table. Agents that
route through the signal-center bridge/consensus path instead of the
standard scan→`signals`-table pipeline (confirmed live: `neo-matrix` — 71
trades since 2026-05-14, 0 rows in `signals`; also `cto-grok42` 6 trades,
`trade-desk` 1 trade) show as `clean_signals_in_db=0` / "cannot assess"
even though they are genuinely, measurably active. `neo-matrix` specifically
has 34 clean (non-contaminated) closed trades, +$90.58 realized, 91.2% WR
since the clean cutoff — real positive performance the sweep/audition
scorer currently cannot see. Any roster ranking done off `signals`-table
sweep output alone will misclassify these agents as unmeasured. See
`docs/XO_BACKLOG.md` for the follow-up (score `trades`-table realized P&L
directly for signal-center-routed agents, don't rely on `signals` alone).

### Session hygiene (2026-07-05)

Three standing rules for how Scotty (Claude Code) sessions on this repo are
run, distilled after COST-DISCIPLINE-IMPLEMENT surfaced how much silent spend
and stale state accumulates when they aren't followed:

1. **All Scotty sessions launch from `~/autonomous-trader`, never `~`.**
   Relative-path env loading (`OT_ROOT`-relative `.env`/`data/trader.db`
   reads), the `.claude/settings.json` scoped `Write`/`Edit` permissions
   (HM-HELM), and every script's `cd {root} && ...` cron convention all
   assume the repo root as cwd. Launching from `$HOME` risks silently reading
   or writing the wrong `.env`/DB, or permission-scoping against the wrong
   tree.
2. **Fresh session per directive.** Don't chain unrelated directives into one
   long-running session — context accumulated from a prior, unrelated task
   is dead weight at best and a source of cross-directive confusion at worst
   (stale file-read cache, stale assumptions about what's already fixed).
3. **`/compact` at context ≥50%.** Compact proactively rather than waiting
   for a forced/automatic compaction near the limit — a deliberate compact at
   the halfway point keeps the working summary accurate and gives headroom
   for verification steps (test inserts, live greps, agent sub-calls) later
   in the same directive without an uncontrolled context cliff.

### Credentials never transit Scotty chat (2026-07-05)

Where a remote fix needs privileged (sudo) execution on another box, grant a
scoped `NOPASSWD` sudoers rule for the exact commands needed instead of
passing a password through chat. Pattern used on Ollie Max during the
TRIP-POSTURE-CLOSEOUT softdog fix: `/etc/sudoers.d/bigmac-softdog`, grown
incrementally to the minimum command set actually required (`modprobe`,
`tee`, `mkdir`, `systemctl restart watchdog.service`, `systemctl status
watchdog.service`, `systemctl daemon-reload`) rather than a blanket
`NOPASSWD: ALL`. Remove the sudoers rule (or narrow it back down) once the
fix it was scoped for is verified and no longer needed. See
`docs/REBOOT_POSTURE.md` for the incident this pattern came from.

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
