# Relay — 2026-08-29 — GEX collector dead since 2026-07-21 (root cause + fix + sentinel)

## Directive
"GEX/options-flow collector dead since 2026-07-21" — find why, fix + backfill,
impact-audit consumers, add a staleness sentinel, apply the same freshness
check to other daily collectors.

## Root cause (two layers)

**Layer 1 — the collector script was deleted from disk, not from git.**
On 2026-07-22 the Admiral gave a full "final stand-down" order (fleet's
declared last trading day). Because crontab writes were broken that night
(`crontab: Interrupted system call`, the pre-existing HM-CRONTAB-EINTR bug),
the session executing the stand-down worked around it by **renaming 23
cron-invoked scripts** to `*.quietdown-disabled-2026-07-22` instead of
editing crontab directly, and staged a cleaned-up crontab at
`backups/crontab_20260722_quietdown_STAGED_NOT_YET_INSTALLED.txt` for
install "the moment crontab access works again." `scripts/hm_gex_daily_collect.py`
was one of the 23. The fleet was later revived for continued trading (this
box is clearly live-trading as of today), but the staged crontab was never
installed and the renames were never reversed for 19 of the 23 scripts —
only the 4 needed to literally run the trader again
(`trader_restart.sh`, `watchdog_supervisor.sh`,
`signal_center_reboot_start.sh`, `kirk_briefing.py`) got restored, ad hoc.
The other 19 — including **`scripts/hm_ops_sentinel.py`, the exact mechanism
that should have caught this** — sat disabled and unnoticed. `crontab -l`
never got fixed either, so cron kept literally invoking the old (now
missing) path every day: `logs/gex_collector.log` shows "can't open file
... No such file or directory" on every 13:05 AZ run since. This is a
same-failure-mode blind spot: the watchdog died in the same incident as the
thing it watches (CLAUDE.md already names this exact anti-pattern under
"Alarms must not share a failure mode with what they watch").

An unrelated 2026-08-28 session (429-remediation pass) independently found
2 of the 19 (`hm_ops_sentinel.py`, `iren_flip_watch.py`) erroring and
commented out their cron lines with a note to restore "alongside the
standdown-reversal decision" — see the still-open
`QUESTION_fleet-standdown-reversal.md`. The other 17, including GEX, were
not caught by that pass.

**Layer 2 — Polygon options/snapshot access is now 403 NOT_AUTHORIZED,
account-wide.** Independent of the file issue. Reproduced live today:

```
GET /v3/snapshot/options/SPY        -> 403 NOT_AUTHORIZED "not entitled"
GET /v2/snapshot/.../tickers/SPY    -> 403 Forbidden
GET /v2/last/trade/SPY              -> 403 Forbidden
```

The **same three endpoints, same key, same "Stocks Starter + Options
Starter" tier, returned 200 OK with real data** in the 2026-07-01
HM-POLYGON-PROBE (`docs/XO_BACKLOG.md:7267`). Something changed the
account's entitlement between 07-01 and now. The most plausible trigger is
the 07-22 "final" stand-down itself — cancelling a $58/mo data subscription
is a very ordinary cost-cutting move for infrastructure believed to be
permanently retired, and it lines up: the collector's own last log lines
before the rename show "no chain/spot" errors for both SPY and QQQ on
2026-07-22, the same day. (The `/v2/aggs/.../prev` bars endpoint returned
429 rate-limited, not 403 — so *some* Polygon access remains; this looks
like a tier downgrade, not a fully dead key.) **This is an account/billing
matter, not something fixable from the terminal — it needs the Admiral to
check the Polygon (now polygon.io redirects to massive.com) subscription
dashboard.** Restoring it requires no further code change; the collector
and `gamma_context.py` will self-heal on their next run once entitlement is
back.

## Fix shipped (commit `85b5f7e`, branch `exec-pipeline`, not yet pushed)

- `scripts/hm_gex_daily_collect.py` — restored (content was identical to
  git HEAD; nothing was actually lost, just missing from the working tree).
  Verified it runs clean now (`2026-08-29 not a trading day -- skipping`,
  exit 0) — will attempt its real fetch Monday 2026-08-31 13:05 AZ and hit
  the Polygon 403 until Layer 2 is resolved.
- `scripts/hm_ops_sentinel.py` — restored + extended with
  `check_collector_freshness()`: checks `data/flow_gex.db` `gex_snapshots`
  (per underlying) and `data/trader.db` `fred_carts` against
  `market_hours_elapsed()` (not wall-clock — a weekend gap doesn't
  false-fire), threshold 7.0 market-hours (> one RTH session). Fires
  `sentinel_collector_stale_gex_<sym>` (RED) / `sentinel_collector_stale_fred_carts`
  (WARNING) — both match `OPS_ALERT_TYPE_PREFIXES`'s `"sentinel_"` prefix,
  so `classify_alert_stream()` routes them to the **ops** stream (the
  "SYSTEM channel" in the directive), not the trading-signal stream.
  Verified live against the real stale timestamp: 182.0 market-hours
  elapsed since the last GEX snapshot (well past the 7h threshold) — this
  will RED_ALERT the instant it next runs during market hours.
  Also **retired `check_riker_heartbeat`**: `riker_synthesis` was
  permanently retired 2026-06-24 (CLAUDE.md), `rikers_log`'s last row is
  literally the stand-down's own kill timestamp
  (2026-07-22 20:21:23) — re-enabling that check unmodified would have
  fired a false RED_ALERT on every single 5-min tick forever. Function kept
  in place (not deleted) per this repo's Archive Convention, just no longer
  called from `main()`.
- Crontab: re-enabled the `hm_ops_sentinel.py` line (`*/5 * * * *`), backed
  up first to `~/backups/cron/`. **Crontab write succeeded** — the EINTR
  bug that blocked the 07-22 install did not reproduce this session (may
  have been transient, or specific to whatever else was happening that
  night; not re-investigated).
- Did **not** touch the other 17 renamed-but-not-restored scripts
  (`daily_report.py`, `eod_report.py`, `situation_report.py`,
  `regime_refresh_runner.py`, `source_health_watcher.py`,
  `origin_healthcheck.sh`, `door1_kill_gate_check.py`,
  `ollie_machine_kill_gate_check.py`, `q_dissent_watch.py`,
  `kimi_cut_watch.py`, `recall_refresh_run.sh`, `ollama_prewarm.sh`,
  `fleet_realism_sweep_clean_window.py`, `agents/uhura_agent.py`,
  `engine/fleet_auditor.py`) or `iren_flip_watch.py`/
  `engine/riker_synthesis.py` (both correctly staying dead — IREN watch
  was self-removing by design, riker is permanently retired). This is
  scoped to what the directive asked for (GEX) plus the one sentinel it
  explicitly requested; the broader stand-down-reversal question is still
  open and belongs to that separate decision, not this fix.

## Impact audit — who reads GEX/gamma regime, and what was actually stale

GEX levels are read through **three separate, parallel implementations**,
not one — worth flagging as its own fragmentation issue, out of scope to
consolidate here:

1. **`engine/canonical_gex.py`** (3-tier: intraday in-process cache →
   `data/flow_gex.db` daily row → live compute) — feeds the Bridge's
   `/api/market/gex` endpoints (`dashboard/app.py`). This is the pipeline
   the ticket's evidence came from. Tier 2 (the daily row) was genuinely
   frozen at 2026-07-21 13:05 ET for 39 days — that's what a cold-cache,
   market-closed request serves, confirmed reproducing this exact "as_of
   2026-07-21" behavior right now. Tier 1 (intraday, refreshed every 15 min
   RTH by `main.py::run_gex_snapshot_refresh`, in-process, independent of
   the dead cron) and tier 3 (live compute) both call
   `engine.options_flow_gex.compute_gex()` — which is **also** hitting the
   Layer-2 Polygon 403 right now, confirmed by direct reproduction. So
   during market hours the Bridge display has likely *also* been serving
   degraded/error data, not just the frozen snapshot after-hours — this
   wasn't isolated to the daily cron.
2. **`engine/gamma_context.py`** — independent implementation (own Polygon
   calls, not shared with `options_flow_gex.py`), feeds "War Room" /
   Ready Room / the LRS tactical gamma read per CLAUDE.md's "Gamma
   grounding" doctrine. **Also hitting the same Layer-2 403 right now** —
   confirmed live (`available: False`, `note: 'chain unavailable'`). LRS
   log lines from 2026-08-28 show moving, plausible-looking wall levels
   ("call resistance $772.0" etc.), so this may be intermittent rather
   than continuously broken all week, or those calls landed before
   whatever narrowed the entitlement further — **not fully traced**,
   flagging as a residual gap rather than asserting either way.
3. **`engine/gex_overlay.py`** → `engine/gex_scanner.py` — feeds
   `battle_station.py`'s 0DTE morning-levels pipeline
   (`generate_morning_briefing`). Checked its failure handling
   specifically since this one gates 0DTE levels: on a GEX fetch failure
   it logs a warning and proceeds with an **empty dict**, so
   `gamma_flip`/`call_wall`/`put_wall` all come back `None` and the
   directional-gating code (`if gamma_flip and spot > 0: ...`) is skipped
   entirely — **fails safe, does not trade on stale or wrong levels.**
   `gex_scanner.py` itself traces back toward the legacy CBOE pipeline that
   `main.py`'s own comment already calls "[DORMANT... no longer
   scheduled]" for a *different* scheduler entry point
   (`run_gex_refresh`); whether `gex_overlay.py`'s separately-scheduled
   `run_gex_overlay_update` (still active, every 15 min) is meaningfully
   live or also quietly degraded was **not traced this pass** — real gap,
   flagging for a follow-up rather than guessing.

**Season 7 / trades gated on stale GEX:** no `options_trades` or `signals_v2`
row was found this pass tying an actual fill to `flow_gex.db`'s frozen
07-21 numbers specifically — the two live tactical paths (`gamma_context.py`
for War Room/LRS, `gex_overlay.py` for battle_station 0DTE, both fail-safe
or independently-computed rather than reading the frozen daily table) are
the ones that would actually gate a trade, and neither reads the stale
`flow_gex.db` row. The **frozen daily snapshot's blast radius looks mostly
confined to the Bridge display** (a human looking at a stale number), not a
demonstrated live execution-gating path — but this is bounded by what I
could trace in one pass, not a clean "no impact" verdict, especially given
`gamma_context.py`'s live compute is independently broken by the same
Polygon issue right now.

## Other daily collectors checked (per directive: fear-greed, congress, macro/FRED)

- **fear-greed** (`engine/fear_greed.py`) — no persisted collector; computed
  live on every call (VIX + SPY RSI + breadth, no daily table). No
  "collector died" failure mode exists for it — nothing to add a
  staleness check to.
- **congress** (`engine/congress_scraper.py`) — same shape: 30-min TTL
  live-scrape cache, no daily cron. Already has its own dedicated watchdog
  (`_record_scrape_health`, NTFYs after 3 consecutive zero-result scrapes)
  — a freshness check would be redundant.
- **macro/FRED** (`engine/fred_data.py` CARTS) — genuinely a daily-cadence
  collector (`main.py::run_carts_persist`, 06:00 AZ, in-process not
  external cron) with a durable `fred_carts.fetched_at` column — this DOES
  share GEX's shape (a "last successful run" timestamp that can go stale
  silently) even though it isn't cron-external, so it's now covered by
  `check_collector_freshness()` above. Confirmed currently healthy:
  `MAX(fetched_at) = 2026-08-29 13:01:11` (today).

## Other findings surfaced by restoring the sentinel (not fixed, flagging for awareness)

Re-enabling `hm_ops_sentinel.py` immediately surfaced two pre-existing,
previously-silent problems (dry-run output, market closed so these are the
non-market-hours checks only):
- **114 "database is locked" occurrences** in the last 10-min window of
  `trader_error.log` at the moment of this test.
- **`signals_v2` pending queue: 11,663 rows, oldest pending 227.5
  market-hours old** (~35 trading days — since 2026-07-10, predating even
  the stand-down). Matches the pre-existing HM-SIGNALS-V2-FIFO-STARVATION
  entry in `docs/XO_BACKLOG.md`.

Neither is new — both predate this session — but neither has been visible
to anyone since 07-22 either, for the same reason GEX wasn't. Not
investigated or fixed this pass (out of scope), flagging so they don't
silently look "newly discovered and ignored."

## Not done / open

- Layer 2 (Polygon entitlement) needs an Admiral action on the vendor
  account — nothing further to do in code until that's resolved.
- The other 17 quietdown-disabled scripts and the crontab's remaining
  stale/commented lines — left exactly as found, per the still-open
  `QUESTION_fleet-standdown-reversal.md`.
- `gex_overlay.py` → `gex_scanner.py`'s actual live health not traced.
- Newly-surfaced lock-error and signals_v2-backlog findings above — not
  triaged.
- This commit is **local only, not pushed** (repo doctrine:
  [[feedback_push_posture_no_autopush]] — no auto-push by design).
