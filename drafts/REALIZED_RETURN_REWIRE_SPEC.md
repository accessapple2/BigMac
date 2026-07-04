# REALIZED-RETURN REWIRE — Design Spec (P1)
**Status: DESIGN ONLY. Do not build without Admiral greenlight.**
**This is the gate on all real alpha and any Rung 4 sortie.**
**Admiral decisions already banked (2026-06-29):**
- Option B confirmed: ADD `fwd_return_1d_realized REAL NULL` column. Keep `fwd_return_1d` (scanner-projected proxy) as calibration signal. Do NOT overwrite.
- See also: `drafts/FWD_RETURN_REALIZED_SPEC.md` (earlier draft, same decision).

---

## The problem
`fwd_return_1d` in `signal_observations` is populated by `signal_evaluator.py` as:

```python
fwd_return_1d = (target_price - entry_price) / entry_price
```

where `entry_price` and `target_price` come from `deep_scan_results` — the scanner's own projected targets. This is **scanner optimism, not market reality.** Every per-source alpha number in `/api/observations/summary` is contaminated. Deployment authority cannot be established until this is fixed.

---

## The fix
Add `fwd_return_1d_realized REAL NULL` to `signal_observations`. Populate it by fetching **actual market price** at signal `ts` (entry) and at `expiry` (exit) from Alpaca historical bars. This is the realized P&L of holding from signal fire to expiry.

```
fwd_return_1d_realized = (close_at_expiry - close_at_ts) / close_at_ts
```

---

## Price source

**Primary: Alpaca historical bars** (`GET /v2/stocks/{symbol}/bars`)
- Already authenticated, already the paper broker — zero new credentials.
- `timeframe=1Day`, `start=ts_date`, `end=expiry_date`, field: `c` (close).
- Entry proxy: daily close on the date of `ts`.
- Exit proxy: daily close on the date of `expiry`.
- Limitation: daily resolution only — correct for current obs window (6h–24h expiry typical).

**Cross-check (advisory, read-only): Schwab historical**
- RULE #1 absolute: Schwab is READ-ONLY. May read price history for spot-checking Alpaca data quality on a sample. Never write Schwab-sourced values to the DB.

---

## Schema change (additive, append-only)

```sql
ALTER TABLE signal_observations
  ADD COLUMN fwd_return_1d_realized REAL;
```

- Nullable, set-once, never overwritten — consistent with append-only doctrine.
- `fwd_return_1d` (scanner-projected) is **preserved unchanged** as a calibration column.
- Comparing `fwd_return_1d_realized` vs `fwd_return_1d` per source tells us how well each scanner calibrates its targets to actual outcomes — this is the **scanner calibration signal**.

---

## Rows with uncomputable realized return

Flag as NULL (never fake):
- Signal `ts` or `expiry` is missing → NULL, skip.
- Alpaca bars return no data for that ticker/date range (delisted, holiday, no history) → NULL.
- API error / timeout → NULL, leave for re-run.
- Expiry has not yet passed (in-window pending obs) → NULL, evaluator fills forward-only.

Do NOT impute, interpolate, or forward-fill a missing price. NULL is the honest answer.

---

## Evaluator wiring (forward-only)

In `engine/signal_evaluator.py`, `evaluate_pending()`, add alongside the existing `deep_scan_results` join:

```python
fwd_return_1d_realized = _fetch_realized_return(ticker, ts, expiry)
# write to signal_observations alongside existing fwd_return_1d (keep both)
```

New helper `_fetch_realized_return(ticker, ts_iso, expiry_iso) -> float | None`:
- Calls Alpaca `GET /v2/stocks/{ticker}/bars?timeframe=1Day&start=...&end=...`.
- Extracts close on `ts_date` (entry) and close on `expiry_date` (exit).
- Returns `(exit_close - entry_close) / entry_close` or `None` on any error.
- Wrapped entirely in `try/except` — a price fetch failure must never block evaluation.

---

## Backfill plan (11,031 evaluated rows)

1. `ALTER TABLE signal_observations ADD COLUMN fwd_return_1d_realized REAL` (idempotent — wrap in try/except).
2. Fetch all rows where `evaluated_at IS NOT NULL AND fwd_return_1d_realized IS NULL`.
3. For each: call Alpaca bars for `(ticker, ts_date, expiry_date)`.
4. Write `fwd_return_1d_realized` (NULL if uncomputable — never fake it).
5. Cache bars per `(ticker, date_range)` — many obs share the same ticker+date. Expect ~500–800 unique (ticker, date) pairs across 11,031 rows.
6. Throttle to ~5 req/s alongside live trader.
7. Estimated runtime: 3–6 min.

**Backfill is a standalone script** (same pattern as `_nightcrew/nightcrew_fwd_return_backfill.py`). Re-running is safe (skips rows where `fwd_return_1d_realized IS NOT NULL`).

---

## What the alpha read looks like after

`/api/observations/summary` per-source block gains:
- `avg_fwd_1d` — scanner-projected (calibration reference, existing)
- `avg_fwd_1d_realized` — actual market return (the real alpha read, new)

**The deployment gate:** when `avg_fwd_1d_realized` for a source is positive, consistent across the rolling window, and statistically meaningful (sample ≥ N, DSR ≥ 0.95, PBO ≤ 0.30) — that source earns Rung 4 consideration. Not before.

---

## Build checklist (when greenlit)

1. [ ] `ALTER TABLE` migration (additive, safe to run live — WAL mode).
2. [ ] `_fetch_realized_return()` helper in `signal_evaluator.py` (Alpaca bars, try/except, None fallback).
3. [ ] Wire into `evaluate_pending()` — populate `fwd_return_1d_realized` alongside existing `fwd_return_1d`.
4. [ ] Backfill runner script for 11,031 evaluated rows.
5. [ ] Update `/api/observations/summary` to include `avg_fwd_1d_realized_*` fields.
6. [ ] Dry-run on 10 rows; verify Alpaca bars return sensible closes.
7. [ ] Full backfill run; capture `/api/observations/summary` — this is the real alpha read.
8. [ ] Restart to activate evaluator change (scheduler job).

**Estimated effort:** 4–5h (helper + backfill script + endpoint update + verify).

---

## Explicitly out of scope

- Schwab price writes (RULE #1).
- Intraday bars / minute-level precision (follow-on, not this spec).
- Overwriting `fwd_return_1d` (Option A rejected by Admiral — keep both columns).
- Any change to trade execution or routing.
- Emit-time acted tagging (sequenced behind this — see `EMIT_TIME_ACTED_TAGGING_SPEC.md`).
- Rung 3 / Rung 4 (gated on this spec landing first).
