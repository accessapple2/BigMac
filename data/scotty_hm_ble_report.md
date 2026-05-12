# 🔧 HM-BL.E — Stale Positions Discovery (HALT FOR CAPTAIN)

**Author:** Scotty (Opus 4.7)
**Date:** 2026-05-12
**Status:** HALT — Captain decision needed on cleanup approach
**Prior art:** BKBL closure flagged `positions` table for "stale 0-qty rows" containing delisted ATH (Athene Holding, delisted Jan 2022).

---

## Discovery findings

### Original BKBL premise: WRONG

The BKBL closure stated: *"ATH was delisted Jan 2022 yet still has a row. Likely a Schwab/Alpaca sync retention bug that never zero-removes positions."* That framing assumed a zero-qty stale row.

**Actual state:**
- `SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL` → **0 rows**
- Total `positions` rows: 44 (all non-zero)
- No "zero-removes positions" bug exists. The proposed cleanup SQL (`DELETE WHERE qty = 0 AND updated_at < N days`) would target an empty set.

### Real issue: capitol-trades opened a NEW position on delisted ATH

```
id   player_id       symbol  qty     avg_price  opened_at
---  --------------  ------  ------  ---------  -------------------
592  capitol-trades  ATH     1.2344  83.33      2026-05-07 06:44:54
```

- Player: `capitol-trades` (Congressional STOCK Act copy-trader)
- Opened: **2026-05-07** — only 5 days ago, well after ATH was delisted in Jan 2022
- Avg cost: $83.33 (the price seems to be stale broker data — Yahoo error confirms "No data found, symbol may be delisted")
- This is the same ATH symbol that HM-BL/HM-BL-broad memoization handles in the read path. The position itself is NEW, not stale.

### Root cause analysis

`engine/capitol_fund.py` sources symbols from `engine.congress_tracker.get_top_congress_buys(30)`. Congressional STOCK Act filings include trades members made BEFORE delisting. The function filters candidates by `buy_count >= MIN_BUY_COUNT` and `ticker not in held_symbols`, but performs **no listing-status check** before passing to `paper_trader.buy()`.

```python
candidates = [
    t for t in top_buys
    if t["buy_count"] >= MIN_BUY_COUNT
    and t["ticker"] not in held_symbols
    and not _already_bought_today(t["ticker"])
    and t.get("ticker")
]
```

Missing: a check like `is_listed(t["ticker"])` or even reusing `yf_safe.is_delisted(t["ticker"])` from HM-BL.

### Other oddities surfaced (informational, not in scope)

While surveying the `positions` table, a few rows stood out — these may or may not be issues, listed here for awareness:

| player_id | symbol | qty | avg_price | opened_at | note |
|---|---|---|---|---|---|
| `enterprise-computer` | `GC=F` | 1.0 | 5249.99 | 2026-03-26 | Gold futures tracker, expected per CLAUDE.md two-book bridge |
| `enterprise-computer` | `SI=F` | 35.0 | 81.99 | 2026-03-26 | Silver futures tracker, expected |
| `energy-arnold` | `QQQ` | **-0.8967** | 562.58 | 2026-03-27 | Short (negative qty); confidence-bimodal agent per CLAUDE.md fleet-reality |
| `gemini-2.5-flash` | `ONDS` | **-58.8948** | 8.8 | 2026-03-30 | Large short on small-cap |
| `gemini-2.5-flash` | `IREN` | **-13.2929** | 35.09 | 2026-03-30 | |
| `dalio-metals` | `GOOGL` | 5.9831 | 12.93 | 2026-03-30 | Non-metals position on metals-desk player — possibly a routing bug or intentional, unclear |
| `dalio-metals` | `ONDS` | -76.514 | 7.88 | 2026-03-30 | |
| `capitol-trades` | `FMAO` | 0.6606 | 26.5196 | 2026-04-24 | The FMAO triple-entry remnant per CLAUDE.md 2026-04-25 drydock note |

These are existing-state observations, not HM-BL.E proposals. Flagging in case any look obviously wrong to Captain.

---

## Schema clarification

The directive's proposed SQL referenced columns that don't exist in the current schema:

```
sqlite> PRAGMA table_info(positions);
0|id|INTEGER
1|player_id|TEXT
2|symbol|TEXT
3|qty|REAL
4|avg_price|REAL          ← directive said avg_cost (wrong)
5|asset_type|TEXT
6|option_type|TEXT
7|strike_price|REAL
8|expiry_date|TEXT
9|opened_at|TIMESTAMP     ← directive's "updated_at" doesn't exist
10|high_watermark|REAL
```

There is **no `market_value`, `unrealized_pl`, or `updated_at` column on `positions`**. Market values are computed at read time by joining live price data; updates are tracked via the audit logs and `paper_trader.py` mutations, not a column.

---

## Recommended approaches

### Option A — Minimal: delete just the ATH row (5-line SQL)

Targeted to the one known-delisted position. Per sacred-data rule, archive first:

```sql
-- Save the row for audit trail
INSERT INTO positions_archive_hmble (id, player_id, symbol, qty, avg_price, asset_type, opened_at, archived_at, reason)
SELECT id, player_id, symbol, qty, avg_price, asset_type, opened_at,
       CURRENT_TIMESTAMP, 'HM-BL.E: delisted ATH, opened 2026-05-07 by capitol-trades'
FROM positions WHERE symbol = 'ATH' AND player_id = 'capitol-trades';

-- Delete the position
DELETE FROM positions WHERE symbol = 'ATH' AND player_id = 'capitol-trades';

-- Verify
SELECT COUNT(*) FROM positions WHERE symbol = 'ATH';  -- expect 0
```

**Requires:** create `positions_archive_hmble` table first (one-time). Or skip archive if Captain prefers a straight delete.

### Option B — Structural: harden capitol_fund.py with listing check

Add to `engine/capitol_fund.py` candidate filter:

```python
from engine.yf_safe import is_delisted

candidates = [
    t for t in top_buys
    if t["buy_count"] >= MIN_BUY_COUNT
    and t["ticker"] not in held_symbols
    and not _already_bought_today(t["ticker"])
    and not is_delisted(t["ticker"])  # NEW — skip cached delisted
    and t.get("ticker")
]
```

Caveat: `yf_safe._DELISTED_CACHE` only populates after a failed yfinance call. For new tickers never tried, this won't catch them. A live `yfinance` listing-status check is heavier — would slow scan. Could be combined with the in-process cache for amortized cost.

### Option C — Both A and B (recommended)

Run Option A to clean the existing position. Apply Option B to prevent future occurrences.

### Option D — Defer

Single delisted position with qty=1.2344 (notional ~$103) is harmless if left alone. Capitol-trades won't re-buy because `ticker not in held_symbols` filters it out. Defer cleanup until/unless it surfaces in a bigger audit.

---

## SQL handoff block (Captain runs manually, sacred-DB rule)

If Captain chooses Option A (and approves the archive table convention):

```sql
-- ─── HM-BL.E: stale capitol-trades ATH position cleanup ────────────────

-- 1. Create archive table (idempotent)
CREATE TABLE IF NOT EXISTS positions_archive_hmble (
    archive_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    id           INTEGER NOT NULL,
    player_id    TEXT    NOT NULL,
    symbol       TEXT    NOT NULL,
    qty          REAL,
    avg_price    REAL,
    asset_type   TEXT,
    opened_at    TIMESTAMP,
    archived_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reason       TEXT
);

-- 2. Archive the ATH row
INSERT INTO positions_archive_hmble (id, player_id, symbol, qty, avg_price, asset_type, opened_at, reason)
SELECT id, player_id, symbol, qty, avg_price, asset_type, opened_at,
       'HM-BL.E: delisted ATH, opened 2026-05-07 by capitol-trades; archived 2026-05-12'
FROM positions
WHERE symbol = 'ATH' AND player_id = 'capitol-trades';

-- 3. Delete from active positions
DELETE FROM positions WHERE symbol = 'ATH' AND player_id = 'capitol-trades';

-- 4. Verify
SELECT COUNT(*) AS ath_remaining FROM positions WHERE symbol = 'ATH';
SELECT * FROM positions_archive_hmble WHERE symbol = 'ATH';
```

---

## Questions for Captain

1. **Approach**: A (delete only) | B (harden capitol_fund) | **C (both, recommended)** | D (defer)?
2. **Archive convention**: do you want a `positions_archive_hmble` table, or prefer to handle archival differently (e.g., a single `positions_archive` table for all future cleanups)?
3. **The 8 oddities above** (energy-arnold shorts, dalio-metals non-metals, FMAO remnant): in scope for follow-up, or known-and-fine?

HALT — awaiting Captain decision.
