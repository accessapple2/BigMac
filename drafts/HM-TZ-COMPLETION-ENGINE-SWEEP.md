# HM-TZ-COMPLETION — engine-surface naive-local timestamp sweep

**STATUS: CLOSED 2026-06-02** — full surface swept; all DB-datetime writers canonicalized,
both reader-side stragglers fixed. End-state confirmation at the bottom. Activation rides
the post-RTH restart batch (no separate restart).

**Opened:** 2026-06-02 (spun out of HM-SOURCE-HEALTH-WATCHER TZ investigation)
**Priority:** MED — none of these are grid-feeding (registered source) producers; all
grid + decision-path stragglers were already fixed (see "Already done" below). These are
the *remaining* `datetime.now().isoformat()` (local, naive) writers that the HM-TZ
Stage-3 migration did not convert. Each is a bug ONLY if some consumer reads the value
as UTC for logic; internally-consistent display/scan metadata is harmless.

## Methodology (per writer site)
For each site below:
1. Identify the sink — JSON file, DB column, or HTTP response field.
2. Find all **consumers** of that value. Does any consumer do time math / comparison /
   range-query / store-and-reconsume-as-UTC? (vs. pure display, where AZ/local is fine.)
3. If a UTC-expecting consumer exists → **canonicalize the writer** to space-UTC
   (`datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')`, matching server.py:551 /
   the HM-TZ Stage-3 idiom) and, if historical rows feed logic, a Group-A backfill
   (`WHERE <col> LIKE '%T%'` → `+7 hours`, backup-first).
4. If only display consumers → leave as-is OR canonicalize for consistency (no urgency).

## Candidate writer sites (regenerate the full list with the grep at the bottom)
| Module | Line(s) | Field | Sink (verify) | Consumer-reads-as-UTC? (TODO) |
|---|---|---|---|---|
| signal-center/expectancy_engine.py | 190 | `daily_bars.fetched_at` | **DB** | **NO reader** — only reader (`:242`) selects `symbol,date,high,low,close`; `fetched_at` is unread provenance. **CANONICALIZED ✅** |
| engine/full_universe.py | 125 | `universe_stocks.updated_at` | **DB** | reader `app.py:7173` = `MAX(updated_at)` **status-display only** (weekly refresh → 7h immaterial). **CANONICALIZED ✅** |
| engine/rallies_intel.py | 102,142,351,**593** | `rallies_models.updated_at` + trade/alert/stats rows | **DB** | no time-math reader of these cols (rallies READERS hit `rallies_portfolios.scraped_at`/`rallies_debate_log.scraped_at` — different cols). **CANONICALIZED ✅ (all 4)** |
| engine/minervini_filter.py | 372,446 | `scanned_at` | JSON payload | **NO time-math reader** — display/scan metadata. Left as-is (out of DB scope). |
| engine/squeeze_scanner.py | 364 | `scanned_at` | JSON payload | same — no reader. Left. |
| engine/gex_scanner.py / gex_engine.py | 192 / ×5 | `updated` | JSON payload | same — no reader. Left. |
| engine/regime_detector.py | 103,128 | `updated` | regime payload | label is consumed, but the `updated` ts has **no time-math reader**. Left. |
| engine/whisper_network.py | 49,101 | `detected_at` | `/api/whisper` payload | **not persisted as-is** — `save_smart_money_signal` restamps via `utc_now_str` (UTC); whisper payload `detected_at` is display. Left. |
| engine/insider_tracker.py | 131 | `scanned_at` | payload | no reader. Left. |
| engine/chart_analyzer.py | 117 | `analyzed_at` | payload | no reader. Left. |
| engine/realtime_monitor.py | 52 | `triggered_at` | payload | no reader. Left. |
| engine/picard_strategy.py | 257 | `generated_at` | picard_briefings | low-pri; verify if `picard_briefings` ts is read. Left (no time-math reader found). |
| engine/deep_scan.py | 332 | `scan_universe.last_updated` | **DB** | no time-math reader (only a backfill script UPDATEs it). **CANONICALIZED ✅** |
| engine/deep_scan.py | 513/514/614/615 | `deep_scan_results.scan_date`/`scan_time` | **DB (date/time-only keys)** | local-equality readers (`:788/:802/:823` vs local `today`) are consistent; `signal_bridge.py:109` reads `scan_date >= date('now','-1 day')` (UTC) — the `-1 day` buffer absorbs the ≤1-day local/UTC offset → **functionally safe**. Left (date-only key, not a datetime col; coordinated trading-day redefinition would be the "proper" fix, low value). |
| engine/correlation.py | 113…319 (×8) | `"updated"` | **JSON payload** | no INSERT/UPDATE in file — pure returned-matrix display. No reader. Left. |
| engine/premarket_scanner.py | 54/126/130/670/762 | `scanned_at`/`analyzed_at`/`scan_date` | **payload + date-only key** | `:54 scanned_at` NOT stored (absent from the `:691 premarket_scan` INSERT); `scan_date` is a date-only key (DELETE/INSERT equality). No time-math reader. Left. |
| engine/first_officer.py | 255…413 (×5) | `"timestamp"` | **JSON payload** | no INSERT/UPDATE in file (only SELECTs) — advisory response metadata. No reader. Left. |

### Verdict so far
**No genuine data-integrity bug in the engine surface.** Every time-math reader found uses
a multi-day window (`>7d`, `-10d`) where a 7h skew is noise, or reads a different column,
or the tight-comparison writer is already UTC (`smart_money.utc_now_str`). The 5 **DB-column**
writers (expectancy/full_universe/rallies ×4 — actually counting 593, 5 sites) were
canonicalized for HM-TZ completeness. The JSON-payload/display fields were verified to have
**no time-math reader** and left as-is (they're transient, not DB datetimes — out of the
migration's canonicalization scope; revisit only for cosmetic consistency).

### Reader-side stragglers — FIXED this pass
Read a (UTC) column against a LOCAL boundary; swapped the boundary to UTC so column and
comparison agree:
- `engine/volume_scanner.py:526` — `date('now','localtime','-10 days')` → `date('now','-10 days')`.
  **FIXED ✅** (`detected_at` written UTC via `utc_now_str` at `:313/:425`).
- `dashboard/app.py:7536-7537` — `datetime.now()` → `_dt.now(_tz.utc).replace(tzinfo=None)`
  (naive-UTC, matching the UTC `detected_at` column). **FIXED ✅**
- NOT a bug — left intentional: `volume_scanner.py:506` / `:524` use `date(detected_at,'localtime')`
  on **both** sides (or as a local-trading-day grouping) — internally consistent local-day
  semantics, not a UTC mismatch.

## Already done (do NOT re-open — grid + decision path are clean)
- `source_gate` ↔ `TimezoneRoute` raw-UTC opt-out (`X-Raw-Timestamps:1`) — fixes all
  `bridge_iso:` HTTP-read sources (movers/scanner_status/cto_briefing/holdings_top + the
  hidden 7h inflation on the daily ones).
- Residual local writers `server.py:2160` (predictions), `:2229` (scorecard) → space-UTC.
- Producer-side grid stragglers `riker_synthesis.py:127`, `morning_briefing.py:395/1164`
  → space-UTC.
- `trade_signals.created_at` OOS backfill (24 `'T'`-format rows → +7h UTC; backup:
  `signal-center/trade_signals.bak_HM-TZ-OOS-BACKFILL_*.sql`).
- Bridge **consumer** audit: `source_gate` was the only backend consumer parsing a bridge
  timestamp for logic — no other decision/trading path is skewed.
- `predictions` backfill: NOT needed (scoring keys on `snap_date` date-only).

## Regenerate candidate list
```
grep -rn "datetime\.now()\.isoformat()\|datetime\.now()\.strftime(['\"]%Y" --include="*.py" \
  signal-center/ engine/ dashboard/ agents/ scrapers/ \
  | grep -vE "timezone\.utc|utcnow|- timedelta|strftime\(['\"]%H:%M['\"]\)|strftime\(['\"]%Y%m%d_"
```
Note: many hits are HTTP response `ts`/SSE-event fields (signal-center is Flask:9000, NOT
behind TimezoneRoute) or display strings — those are harmless. Filter to DB-writes /
stored-and-reconsumed values.

## CLOSEOUT — genuine end state (2026-06-02)

**Every DB-datetime writer is UTC.** Canonicalized this arc (space-UTC,
`datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')`):
- Grid/source writers: `server.py:2160/:2229`, `riker_synthesis.py:127`,
  `morning_briefing.py:395/1164`, `trade_signals` (`:2921`, Stage-3) + 24-row OOS backfill.
- Engine DB columns: `expectancy_engine.py:190` (daily_bars.fetched_at), `full_universe.py:125`
  (universe_stocks.updated_at), `rallies_intel.py:102/142/351/593`, `deep_scan.py:332`
  (scan_universe.last_updated).
- Already-UTC (no change): `signal_outcomes:551`, `daily_snapshot`/`execution_log`/
  `intelligence_feed` (CURRENT_TIMESTAMP), `bridge_consensus`/`holdings_top`/`cto_briefing`/
  `movers`/`scanner_status` producers, `volume_scanner.py:313/425` (utc_now_str).

**No time-math reader on a TZ-skewed datetime column.** Both reader-side mismatches fixed
(`volume_scanner.py:526`, `app.py:7537`) so UTC columns are compared against UTC boundaries.

**Out of scope (documented, not datetime columns / not bugs):**
- Date-only / time-only KEYS (`scan_date`, `scan_time`, `today`): local-written, read by
  local-equality (consistent) or by buffered windows (`signal_bridge.py:109`'s `-1 day`
  absorbs the ≤1-day local/UTC offset → safe). A "proper" trading-day redefinition is
  possible but low-value; left as a documented non-bug.
- JSON-payload / display fields (`scanned_at`/`detected_at`/`updated`/`analyzed_at`/
  `triggered_at`/`generated_at`/`timestamp` in returned dicts and SSE events): transient,
  verified **no time-math reader**, not DB datetimes. Left as-is.
- Intentional local-trading-day grouping: `volume_scanner.py:506/:524` (`localtime` on both
  sides) — internally consistent, not a mismatch.

**Activation:** all edits ride the post-RTH restart batch (dashboard + signal-center +
metals + DMS). No separate restart. All touched files py_compile clean (3.14 + 3.9).
