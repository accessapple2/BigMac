# HM-BB.E — Legacy entry_price Backfill — Closure Report

**Date:** 2026-05-12
**Operator:** Scotty (Opus 4.7)
**SQL author / approver:** Captain (handoff approved inline)
**Scope:** Single UPDATE on `data/trader.db::ghost_trades` to backfill the `entry_price` column added by HM-BB.

---

## TL;DR

16 of 16 rows in `ghost_trades` had `entry_price IS NULL` (legacy rows from before HM-BB's schema enrichment). Backfilled from each row's existing `price` column. Post-backfill: `still_null = 0`. API `/api/ghost-trades` now returns real float values where it previously returned NULL.

No code change. No service restart. Pure data fix.

---

## Pre-flight state

```
sqlite> SELECT COUNT(*) AS total, SUM(CASE WHEN entry_price IS NULL THEN 1 ELSE 0 END) AS null_entry FROM ghost_trades;
total  null_entry
-----  ----------
16     16
```

Note vs directive: directive said "16 legacy rows left NULL" implying some non-NULL rows existed. Reality is the entire table is 16 rows, all NULL. Same operational outcome; flagged for visibility — HM-BB's runtime population path has not yet fired against any rows in production.

### Sample (5 NULL rows, pre-update)

| id | symbol | side | price    | entry_price |
|----|--------|------|----------|-------------|
| 1  | CSCO   | BUY  | 90.46    | NULL        |
| 2  | META   | BUY  | 605.1523 | NULL        |
| 3  | BWXT   | SELL | 216.39   | NULL        |
| 4  | CRWD   | SELL | 445.75   | NULL        |
| 5  | MU     | SELL | 517.16   | NULL        |

### Schema confirms `price` (col 5, REAL NOT NULL) is the right source

```
5|price|REAL|1||0          ← source (NOT NULL)
6|fill_price|REAL|0||0
12|entry_price|REAL|0||0   ← target
14|exit_price|REAL|0||0
```

---

## Backup (WAL-safe)

```
sqlite3 data/trader.db "PRAGMA wal_checkpoint(TRUNCATE);"  → 0|0|0  (clean)
cp data/trader.db      data/trader.db.pre-hm-bbe-20260512_0617      (272 MB)
cp data/trader.db-shm  data/trader.db.pre-hm-bbe-20260512_0617-shm   (32 KB)
cp data/trader.db-wal  data/trader.db.pre-hm-bbe-20260512_0617-wal   (0 B)
```

Gitignore (HM-CLEAN `data/*.db.pre-*`) catches all three — no risk of fat-file commit.

---

## BBE.1 — UPDATE applied

```sql
BEGIN;
UPDATE ghost_trades
   SET entry_price = price
 WHERE entry_price IS NULL
   AND price IS NOT NULL;
SELECT changes() AS rows_updated;
COMMIT;
SELECT COUNT(*) AS total,
       SUM(CASE WHEN entry_price IS NULL THEN 1 ELSE 0 END) AS still_null
  FROM ghost_trades;
```

Result:

```
rows_updated
------------
16

total  still_null
-----  ----------
16     0
```

Matches directive expectation (`rows_updated=16, still_null=0`).

---

## BBE.C — Verify

### SQL sample (5 rows post-backfill — note `price == entry_price` exactly)

| id | symbol | side | price    | entry_price |
|----|--------|------|----------|-------------|
| 1  | CSCO   | BUY  | 90.46    | 90.46       |
| 2  | META   | BUY  | 605.1523 | 605.1523    |
| 3  | BWXT   | SELL | 216.39   | 216.39      |
| 4  | CRWD   | SELL | 445.75   | 445.75      |
| 5  | MU     | SELL | 517.16   | 517.16      |

### API check — `/api/ghost-trades?limit=3` (most-recent 3, id desc)

```
id=16 TER  BUY   entry_price=365.985
id=15 CEG  SELL  entry_price=293.645
id=14 AMGN BUY   entry_price=329.09
```

Previously these would have returned `null` for `entry_price`. Confirmed populated post-backfill — read path COALESCE no longer needed for backfilled rows (still useful for future safety).

---

## Reversibility

Restore path if needed:

```bash
cp data/trader.db.pre-hm-bbe-20260512_0617       data/trader.db
cp data/trader.db.pre-hm-bbe-20260512_0617-shm   data/trader.db-shm
cp data/trader.db.pre-hm-bbe-20260512_0617-wal   data/trader.db-wal
```

(Service is idle on this table for reads-only at the moment; if active, stop trader first.)

---

## Follow-ups (none blocking)

- HM-BB's runtime population code hasn't written entry_price on any row yet — first ghost-trade emitted post-HM-BB will tell us whether the writer is wired. Worth a quick check tomorrow on the next ghost-trade row.
- Closure-report style follows the data/scotty_hm_*.md convention already tracked in repo.
