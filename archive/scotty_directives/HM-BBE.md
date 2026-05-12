# 🔧 SCOTTY — HM-BB.E: Backfill Legacy entry_price
### Tiny SQL Cleanup · Opus 4.7 · Discover → Update → Verify

> **Captain's orders, Mr. Scott:** HM-BB shipped schema enrichment yesterday with 16 legacy rows left NULL on the new `entry_price` column. Backfill them now using each row's existing `price` column as the source (HM-BB.D closure noted COALESCE was already working for reads, but the underlying NULLs are still there). Tiny scope, one UPDATE, one verify. NO service restart required (read-side COALESCE handled this; we're cleaning up underlying data).

## Pre-flight

```bash
cd ~/autonomous-trader
git status --short
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT COUNT(*) AS total, SUM(CASE WHEN entry_price IS NULL THEN 1 ELSE 0 END) AS null_entry FROM ghost_trades;"
```

## Phase BBE.0 — Discovery (NO writes)

```bash
echo "── Sample 5 NULL-entry rows: confirm price column exists + has values ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT rowid, symbol, side, price, entry_price FROM ghost_trades WHERE entry_price IS NULL LIMIT 5;"

echo ""
echo "── Confirm 'price' column is the right source ──"
sqlite3 ~/autonomous-trader/data/trader.db "PRAGMA table_info(ghost_trades);" | grep -E "price|entry"

echo ""
echo "── Backup before UPDATE ──"
TS=$(date +%Y%m%d_%H%M)
cp data/trader.db "data/trader.db.pre-hm-bbe-${TS}"
ls -lah data/trader.db.pre-hm-bbe-* | tail -1
```

HALT. Show me the 5 sample rows + confirm `price` has non-NULL values + backup created. Captain confirms before BBE.1.

## Phase BBE.1 — Apply UPDATE (Captain runs SQL handoff)

Draft this SQL block; Captain executes manually (sacred-DB rule):

```sql
BEGIN;
UPDATE ghost_trades
   SET entry_price = price
 WHERE entry_price IS NULL
   AND price IS NOT NULL;
SELECT changes() AS rows_updated;
SELECT COUNT(*) AS total, SUM(CASE WHEN entry_price IS NULL THEN 1 ELSE 0 END) AS still_null FROM ghost_trades;
COMMIT;
```

Expected: rows_updated=16, still_null=0.

## Phase BBE.C — Verify

```bash
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT rowid, symbol, entry_price FROM ghost_trades ORDER BY rowid LIMIT 5;"
curl -s http://localhost:8080/api/ghost-trades?limit=3 | python3 -m json.tool | head -30
```

No commit, no push needed — this is a pure data backfill (no code change). Closure report `data/scotty_hm_bbe_report.md` documents commands + before/after.

Commit ONLY the closure report: `docs(scotty): HM-BB.E — legacy entry_price backfill (SQL by Captain)`. Push inline.

ntfy: `🏁 HM-BB.E complete — 16 legacy entry_prices backfilled`.
