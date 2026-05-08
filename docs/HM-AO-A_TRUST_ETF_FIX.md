# HM-AO-α — Trust-ETF Reclassification Fix

**Author:** Scotty 3.3 Phase 6
**Date:** 2026-05-08
**Trigger:** Phase 4 audit Top 10 (`docs/SCOTTY_INFRA_AUDIT.md`) +
Grok-diff finding (`reports/grok_diff_2026-05-08.md` §2.A & §2.F)
**Pre-snapshot:** `backups/trader.db.pre-hm-ao-a-20260508_065400` (257.8 MB)

---

## 1. Problem

5 of the largest physical-trust ETFs by AUM/volume — **GLD, GLDM,
IAU, SIVR, SLV** — were missing from the dynamic active universe
(`engine/universe.get_active_universe()`) despite trading 2.3M-25M
shares/day each.

Root cause was a 2-step Polygon-data + filter-logic interaction:

1. **Polygon's `/v3/reference/tickers/<sym>` returns `ticker_type='CS'`**
   for trust structures (GLD/GLDM/IAU = SPDR/iShares gold trusts;
   SIVR/SLV = silver trusts). Polygon classifies them as common stock.
2. **Trusts have NULL `market_cap`** because trusts report AUM, not
   market cap. Polygon's response has `market_cap=None`. yfinance
   fallback also returns None for these.
3. **Universe filter (`engine/universe.py:79-80`) splits on `ticker_type`:**
   ```sql
   (ticker_type = 'CS'  AND market_cap >= ? AND dollar_volume >= ?)
   OR
   (ticker_type = 'ETF' AND dollar_volume >= ?)
   ```
   The CS branch requires `market_cap >= MIN`. The ETF branch requires
   only dollar volume. With `ticker_type='CS'` AND `market_cap=NULL`,
   the trusts fail the CS branch and never reach the ETF branch.

The 3 metals ETFs **GDX, GDXJ, SILJ** are correctly classified as `ETF`
by Polygon (they are mining-equity ETFs, not trusts) and pass cleanly.

---

## 2. Investigation

### Pre-fix state

```
$ sqlite3 trader.db "SELECT symbol, ticker_type, market_cap, last_updated
                     FROM scan_universe
                     WHERE symbol IN ('GLD','GLDM','IAU','SIVR','SLV','GDX','GDXJ','SILJ')";
GDX    | ETF | (NULL) | 2026-05-07T21:31:50  ← correct
GDXJ   | ETF | (NULL) | 2026-05-07T21:31:50  ← correct
GLD    | CS  | (NULL) | 2026-05-03T20:30:01  ← BUG
GLDM   | CS  | (NULL) | 2026-05-03T20:30:01  ← BUG
IAU    | CS  | (NULL) | 2026-05-03T20:30:01  ← BUG
SILJ   | ETF | (NULL) | 2026-05-07T21:31:50  ← correct
SIVR   | CS  | (NULL) | 2026-04-26T20:30:52  ← BUG
SLV    | CS  | (NULL) | 2026-05-03T20:30:01  ← BUG
```

### Loader inventory (`engine/universe_refresh.py`)

```python
# Step 2 loop, line ~327
mc, ttype = _fetch_ticker_details_polygon(api_key, sym)

if ttype == "ETN": ... skip
if ttype == "ETF" and INCLUDE_ETFS:
    c["ticker_type"] = "ETF"
    finalists.append(c)              # ← trust ETFs need to land here
    continue

if ttype not in ("CS", None): ... skip
# CS branch with market_cap requirement → trust ETFs die here
if mc is None: ... fallback yfinance → also None → skip
if mc < MIN_MARKET_CAP: skip
```

The ETF branch is exactly what trust ETFs need; we just have to coerce
their type before the branching logic.

---

## 3. Decision — both Option X + Option Y

The Phase 6 brief offered three options; both X and Y were applied for
defense in depth:

- **Option Y (preferred for durability)** — whitelist override in the
  loader so the next weekly refresh classifies them correctly without
  re-applying any SQL.
- **Option X (one-shot UPDATE)** — fix the 5 existing rows immediately
  so the universe surfaces them now, before next Sunday's refresh.

Option Z (filter-level hack) was rejected per the brief.

---

## 4. Files changed

### 4.A `engine/universe_refresh.py` — Option Y whitelist

Added `TRUST_ETF_OVERRIDES` frozenset constant at the filter-config
section (line ~85, alongside `MIN_MARKET_CAP`, `MIN_DOLLAR_VOLUME`,
`INCLUDE_ETFS`) and a 4-line override block in the Step-2 per-symbol
loop:

```python
# HM-AO-α 2026-05-08: physical-trust ETF override. Polygon classifies
# GLD/GLDM/IAU/SIVR/SLV as CS despite their being trust ETFs.
# Coerce ttype to ETF so they reach the ETF branch (dollar-volume only).
if sym in TRUST_ETF_OVERRIDES and ttype != "ETF":
    log.info("  trust_etf_override %s polygon_type=%s -> ETF", sym, ttype)
    ttype = "ETF"
```

The override fires **before** the ETN / ETF / CS branching, so the
existing logic flows naturally. A log line records each override fire
for audit visibility.

### 4.B `scripts/migrations/hm_ao_a_trust_etf_reclassify.sql` — Option X

```sql
UPDATE scan_universe
   SET ticker_type  = 'ETF',
       last_updated = datetime('now')
 WHERE symbol IN ('GLD', 'GLDM', 'IAU', 'SIVR', 'SLV')
   AND ticker_type = 'CS';
```

Idempotent (the `AND ticker_type = 'CS'` guard prevents re-running on
already-corrected rows). Applied once on 2026-05-08.

### 4.C `tests/test_universe_filter.py` — 5 tests

- `test_trust_etf_overrides_constant` — whitelist is exactly the
  5 expected names, frozenset
- `test_trust_etfs_in_active_universe` — all 5 land in
  `get_active_universe()` after migration
- `test_trust_etfs_classified_as_etf` — `scan_universe.ticker_type`
  is `'ETF'` for all 5
- `test_other_etfs_unchanged` — GDX/GDXJ/SILJ regression guard
- `test_no_other_cs_with_null_market_cap_was_widened` — bulk
  reclassification guard (AAPL/MSFT/etc. still `CS`)

---

## 5. Verification

### 5.A Post-migration row state

```
$ sqlite3 trader.db "SELECT symbol, ticker_type, market_cap, last_updated
                     FROM scan_universe
                     WHERE symbol IN ('GLD','GLDM','IAU','SIVR','SLV')
                     ORDER BY symbol";
GLD   | ETF | (NULL) | 2026-05-08 13:54:49
GLDM  | ETF | (NULL) | 2026-05-08 13:54:49
IAU   | ETF | (NULL) | 2026-05-08 13:54:49
SIVR  | ETF | (NULL) | 2026-05-08 13:54:49
SLV   | ETF | (NULL) | 2026-05-08 13:54:49
```

### 5.B Active universe inclusion

```
$ python3 -c "from engine.universe import get_active_universe;
              u = get_active_universe();
              for s in ('GLD','GLDM','IAU','SIVR','SLV'):
                print(s, '✓' if s in u else '✗')"
GLD ✓
GLDM ✓
IAU ✓
SIVR ✓
SLV ✓
```

Universe size: **1,223 → 1,228** (+5, exactly as expected).

### 5.C Test suite

```
$ python3 -m pytest tests/test_universe_filter.py -v
tests/test_universe_filter.py::test_trust_etf_overrides_constant PASSED
tests/test_universe_filter.py::test_trust_etfs_in_active_universe PASSED
tests/test_universe_filter.py::test_trust_etfs_classified_as_etf PASSED
tests/test_universe_filter.py::test_other_etfs_unchanged PASSED
tests/test_universe_filter.py::test_no_other_cs_with_null_market_cap_was_widened PASSED
========================== 5 passed in 0.02s ==========================
```

---

## 6. Sacred-rules audit

| Rule | Outcome |
|---|---|
| ❌ No bulk reclassification beyond the 5 named symbols | ✅ WHERE clause is a literal IN (...) of those 5; `test_no_other_cs_with_null_market_cap_was_widened` is the regression guard |
| ❌ No changes to universe filter logic for non-trust-ETF cases | ✅ filter logic in `engine/universe.py` is untouched; only the loader pre-coerces `ticker_type` |
| ✅ ONE one-shot DB UPDATE | ✅ 5 rows |
| ✅ Pre-snapshot mandatory | ✅ `backups/trader.db.pre-hm-ao-a-20260508_065400` (257.8 MB) |
| ✅ Whitelist preferred for durability | ✅ Option Y shipped (loader-level), Option X shipped alongside |

---

## 7. Follow-up

- **Next Sunday's universe refresh** (com.ollietrades.universe-refresh
  plist, 14:00 MST) will exercise the new `TRUST_ETF_OVERRIDES` path.
  If anything goes wrong (override not firing, ETF branch rejecting),
  the fail-safe `MIN_FINAL_COUNT <= len(finalists) <= MAX_FINAL_COUNT`
  guard will retain the prior universe and ntfy. Verify the override
  log lines (`trust_etf_override GLD polygon_type=CS -> ETF`) appear
  in next Sunday's refresh output.
- **No agent currently emits live signals against any of these 5
  symbols.** Visibility is necessary but not sufficient — actual signal
  generation needs an agent's prompt or rule set to include them.
  `dalio-metals` is the natural home (advisory / Enterprise Computer
  tracking) but that's a separate scope per the Two-Book Bridge Policy.

---

## 8. Halt condition

5-row UPDATE applied + 5 tests added + loader override committed.
**No service restart performed by this fix** — Universe-refresh plist
fires Sunday; no need to manually rerun. The trader process reads
`scan_universe` on demand, so the 5 new ETF rows are available to the
running trader without restart.
