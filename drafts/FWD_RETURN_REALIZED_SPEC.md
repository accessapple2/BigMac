# FWD_RETURN_REALIZED — Design Spec
**Status: DESIGN ONLY. Do not build without Admiral greenlight.**
**Supersedes:** the `deep_scan_results.target_price` proxy currently stored in `fwd_return_1d`.
**Decision banked (2026-06-29):** Option B — add `fwd_return_1d_realized REAL` column. Keep `fwd_return_1d` (scanner-projected proxy) as a calibration signal.

---

## The problem
`fwd_return_1d` in `signal_observations` is populated by `signal_evaluator.py` as:

```python
fwd_return_1d = (target_price - entry_price) / entry_price
```

where `entry_price` and `target_price` come from `deep_scan_results` — the scanner's own projected targets. This is **scanner optimism, not market reality.** Every per-source alpha number is contaminated. Deployment authority cannot be established from these values.

---

## The fix
Add `fwd_return_1d_realized REAL NULL` to `signal_observations`. Populate it by fetching **actual market price** at signal `ts` (entry) and at `expiry` (exit) from Alpaca historical bars. This is the realized P&L of holding from signal fire to expiry.

```
fwd_return_1d_realized = (close_at_expiry - close_at_ts) / close_at_ts
```

---

## Price source

**Primary: Alpaca historical bars** (`/v2/stocks/{symbol}/bars`)
- Already authenticated, already the paper broker — zero new credentials.
- `timeframe=1Day`, `start=ts_date`, `end=expiry_date`, field: `c` (close).
- Entry proxy: daily close on the date of `ts` (or prior close if signal fired after market close).
- Exit proxy: daily close on the date of `expiry`.
- Limitation: daily resolution only — intraday precision would need minute bars. 1-day close is correct for the current `expiry` window (6h–24h typical obs window).

**Cross-check (advisory, not primary): Schwab historical**
- RULE #1: Schwab is READ-ONLY. We can read price history if the endpoint supports it, but the primary write path uses Alpaca only.
- Use Schwab only to sanity-check Alpaca data quality on a spot sample. Never write Schwab-sourced values to the DB.

---

## Schema change

```sql
ALTER TABLE signal_observations
  ADD COLUMN fwd_return_1d_realized REAL;
```

- Additive, nullable, set-once, never overwritten — consistent with append-only doctrine.
- `fwd_return_1d` (scanner-projected) is **preserved unchanged** — it becomes the "calibration" column: comparing `fwd_return_1d_realized` vs `fwd_return_1d` per source tells us how well each scanner calibrates its targets to actual outcomes.
- `fwd_return_1h_realized` and `fwd_return_exp_realized` can follow the same pattern later (intraday bars, expiry-exact window). Out of scope for this spec.

---

## Evaluator wiring (forward-only)

In `engine/signal_evaluator.py`, `evaluate_pending()`, replace the `deep_scan_results` join:

```python
# CURRENT (remove):
ds = conn.execute(
    "SELECT entry_price, target_price FROM deep_scan_results
     WHERE symbol=? AND scan_date >= ? ORDER BY scan_date DESC LIMIT 1",
    (ticker, ts[:10])
).fetchone()
if ds and ds["entry_price"] and ds["target_price"]:
    fwd_return_1d = round((tp - ep) / ep, 6)

# REPLACE WITH:
fwd_return_1d_realized = _fetch_realized_return(ticker, ts, expiry)
```

New helper `_fetch_realized_return(ticker, ts_iso, expiry_iso) -> float | None`:
- Calls Alpaca `GET /v2/stocks/{ticker}/bars?timeframe=1Day&start=...&end=...`.
- Extracts close on `ts_date` (entry) and close on `expiry_date` (exit).
- Returns `(exit_close - entry_close) / entry_close` or `None` on any error.
- Wraps entirely in `try/except` — a price fetch failure must never block evaluation.
- Uses the live `.venv`'s `alpaca_trade_api` or `requests` with the existing `ALPACA_API_KEY` / `ALPACA_API_SECRET` from `.env`.

---

## Backfill plan (11,031 evaluated rows)

Same pattern as `nightcrew_fwd_return_backfill.py`:

1. `ALTER TABLE signal_observations ADD COLUMN fwd_return_1d_realized REAL` (idempotent: wrap in try/except).
2. Fetch all rows where `evaluated_at IS NOT NULL AND fwd_return_1d_realized IS NULL` (the full evaluated set).
3. For each: call Alpaca bars for `(ticker, ts_date, expiry_date)`.
4. Write `fwd_return_1d_realized` to the row.
5. Throttle to ~5 req/s (Alpaca paper rate limit is generous, but play safe alongside live trader).
6. Cache bars per `(ticker, date_range)` — many obs share the same ticker+date, so de-duplicate fetches. Expect ~500–800 unique (ticker, date) pairs across 11,031 rows, not 11,031 separate API calls.
7. Estimated runtime: 3–6 min at 5 req/s with caching.

**Backfill is a standalone script** (same pattern as `nightcrew_fwd_return_backfill.py`), not wired into the live scheduler. Run once; re-running is safe (skips rows where `fwd_return_1d_realized IS NOT NULL`).

---

## What the alpha read looks like after

`/api/observations/summary` and `/api/signal-observations/readout` both group by source and report `avg_fwd_*`. After the rewire:
- `avg_fwd_1d` — scanner-projected (calibration reference)
- `avg_fwd_1d_realized` — actual market return (the real alpha read)

**The deployment gate:** when `avg_fwd_1d_realized` for a source is positive, consistent across the rolling window, and statistically meaningful (sample ≥ N) — that source earns Rung 4 consideration. Not before.

---

## Also needed: /api/observations/summary update

Add `avg_fwd_1d_realized_not_acted` and `avg_fwd_1d_realized_acted` to the summary endpoint's per-source block, reading from the new column. This is the actual alpha readout.

---

## Build checklist (when greenlit)

1. [ ] `ALTER TABLE` migration (additive, safe to run live — WAL mode).
2. [ ] `_fetch_realized_return()` helper in `signal_evaluator.py` (Alpaca bars, try/except, None fallback).
3. [ ] Wire into `evaluate_pending()` — populate `fwd_return_1d_realized` alongside existing `fwd_return_1d`.
4. [ ] Backfill runner script for 11,031 evaluated rows.
5. [ ] Update `/api/observations/summary` to include `avg_fwd_1d_realized_*` fields.
6. [ ] Dry-run on 10 rows, verify Alpaca bars return sensible closes.
7. [ ] Full backfill run, capture `/api/observations/summary` — this is the real alpha read.
8. [ ] Restart to activate evaluator change (scheduler job).

**Estimated effort:** 4–5h (helper + backfill script + endpoint update + verify).

---

## Explicitly out of scope

- Schwab price writes (RULE #1).
- Intraday bars / minute-level precision (follow-on, not this spec).
- Overwriting `fwd_return_1d` (Option A rejected by Admiral — keep both columns).
- Any change to trade execution or routing.
- Emit-time acted tagging (sequenced behind this — see `EMIT_TIME_ACTED_TAGGING_SPEC.md`).
