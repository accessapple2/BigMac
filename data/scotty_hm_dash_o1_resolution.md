# HM-DASH.4 — O1 Resolution

**Date:** 2026-05-12
**Origin:** Observation O1 from `data/scotty_hm_dash_session1_report.md` (commit `006d644`)
**Status:** ✅ Resolved at commit `d1dfb8a`
**Approach:** Option B (separate `squeeze_candidates` table; squeeze_watch byte-identical)

## What O1 said

> `_MIN_PERSIST_SCORE = 5` is conservative. Today's live `run_scan` produced 4 Polygon-enriched candidates with scores 3-4. None were persisted. ATYR (DTC=25.28) and ABR (DTC=19.74) are objectively interesting squeeze names by traditional metrics. The scanner's score formula is heavily weighted on float size (small float = more pts); large-float names with high SI + high DTC may consistently miss the persist bar.

## What HM-DASH.4 ships

A second table (`squeeze_candidates`) captures the 3-4 score range without changing `squeeze_watch` semantics. The dashboard endpoint UNIONs both tables and parameterizes the score floor.

### Schema (new table)

```sql
CREATE TABLE squeeze_candidates (
    -- same columns as squeeze_watch …
    days_to_cover   REAL,   -- HM-DASH.4: real column (squeeze_watch has DTC in notes only)
    -- … minus ntfy_sent / ntfy_deferred (candidates don't NTFY by design)
);
```

Migration: `scripts/migrations/add_squeeze_candidates_table.sql`. Idempotent (CREATE TABLE IF NOT EXISTS) and applied via direct sqlite3 invocation pre-restart. Module-level guard also ensures the table exists at persist time.

### Persist routing (engine/squeeze_scanner.py)

```
score < 3       → drop (was drop)
3 <= score < 5  → INSERT INTO squeeze_candidates  (was drop)
score >= 5      → INSERT INTO squeeze_watch       (was INSERT INTO squeeze_watch — UNCHANGED)
```

The `>=5` branch is byte-identical to pre-DASH.4 — same dedup logic, same INSERT statement, same fields. Verified by reading the diff (`git diff main~ engine/squeeze_scanner.py` shows only additive blocks around the existing path).

### Endpoint (dashboard/app.py `/api/squeeze/candidates`)

| Param | Default | Range | Effect |
|---|---:|---|---|
| `limit` | 20 | 1-100 | row cap |
| `tier` | `""` | WATCH \| ALERT \| PRIORITY | legacy `threshold_tier` filter (applies to watch rows only — squeeze_candidates skipped when set) |
| `min_score` | 3 | 1-10 | composite floor = min_score * 10 |

Response now includes new `tier` field per row:
- `tier: "watch"` for rows from `squeeze_watch` (composite >= 50)
- `tier: "candidate"` for rows from `squeeze_candidates` (composite 30-40)

`days_to_cover` always populated:
- candidates: REAL column
- watch: parsed from `notes` via regex `days_to_cover=([\d.]+)`

Sort order: `composite_score DESC, days_to_cover DESC, scan_ts DESC`.

## Live verification (post-restart pid 42310)

```
$ run_scan(force=True)
  persist_summary: {inserted: 0, candidates_inserted: 4, ...}

$ sqlite3 squeeze_candidates
  ASTS  composite=40  DTC=2.96   notes="raw_score=4; ... si_source=polygon"
  ABR   composite=30  DTC=19.74  notes="raw_score=3; ... si_source=polygon"
  ATYR  composite=30  DTC=25.28  notes="raw_score=3; ... si_source=polygon"
  BKSY  composite=30  DTC=3.75   notes="raw_score=3; ... si_source=polygon"

$ curl /api/squeeze/candidates?min_score=5
  count=1   (BKSY watch tier — historical squeeze_watch row, composite=50)

$ curl /api/squeeze/candidates?min_score=3
  count=5   (1 watch + 4 candidates, sorted by composite then DTC)
    BKSY  composite=50  tier=watch       DTC=4.01
    ASTS  composite=40  tier=candidate   DTC=2.96
    ATYR  composite=30  tier=candidate   DTC=25.28   ← surfaced by DASH.4
    ABR   composite=30  tier=candidate   DTC=19.74   ← surfaced by DASH.4
    BKSY  composite=30  tier=candidate   DTC=3.75
```

ATYR (DTC=25.28) and ABR (DTC=19.74), the two "objectively interesting squeeze names" called out in O1, are now reachable from the dashboard.

## Carry-forward observation for Session 2

The same symbol can appear in both tiers if it persisted to squeeze_watch in a prior scan AND to squeeze_candidates in a later scan (or vice versa). Example: today's payload shows BKSY twice — once as 'watch' (composite=50 from yesterday's run with score=5) and once as 'candidate' (composite=30 from today's run with score=3).

The Session 2 frontend may want to dedupe by symbol, keeping the highest-tier instance. This was deliberately not implemented in HM-DASH.4 because:
1. Captain spec said "Each returned row tagged with `tier`" — no cross-table dedupe requested
2. Both rows have legitimate informational value (yesterday's higher conviction vs today's lower conviction)
3. The frontend can dedupe with one line of JS if desired

## Anchors

```
HM-DASH.4   engine/squeeze_scanner.py             5 anchored sites
HM-DASH.4   dashboard/app.py                      1 wrapped block (within the existing HM-DASH.3 anchor)
HM-DASH.4   scripts/migrations/                   1 new SQL file
```

`git grep "HM-DASH.4"` on `main` returns the full audit trail at commit `d1dfb8a`.
