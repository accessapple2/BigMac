# NIGHT CREW REPORT — 2026-06-29
**Branch:** exec-pipeline · **Reported by:** Scotty · **Admiral-review required before any further alpha work**

---

## ⚠ HEADLINE: FWD_RETURN_1D IS NOT REALIZED RETURN — ALPHA READ INVALID

`fwd_return_1d` is computed in `engine/signal_evaluator.py` as:

```python
fwd_return_1d = round((target_price - entry_price) / entry_price, 6)
```

where `entry_price` and `target_price` come from `deep_scan_results` — the scanner's **own projected target at signal time**, not what the market actually delivered.

**Every per-source `avg_fwd_*` in `/api/observations/summary` is a measure of scanner optimism, not measured edge.** The numbers are high and suspiciously consistent across sources because `bk_avwap`, `deep_scan`, and `bk_orb` all tend to fire on the same tickers the deep scanner projected bullishly on the same day.

**VERDICTS:**
- The alpha read is NOT trustworthy.
- NO source earns Rung 4 deployment authority from these numbers.
- Deployment authority is NOT established.
- This is an Admiral-review item before any further alpha work proceeds.

---

## PROXY NUMBERS (LABELED — scanner-projected, NOT realized, do NOT bank)

Snapshot captured 2026-06-29 ~04:27 UTC. Drain mechanically complete for all scoreable rows.

```
total obs:       11,057
evaluated:       10,628  (415 in-window pending — score when expiry closes)
fwd_return fill: 22.9%   (2,531 rows have fwd_return_1d; remainder evaluated,
                          no deep_scan match for that ticker/date)
```

| Source | N (evaluated) | avg_fwd_1d_not_acted | Note |
|---|---|---|---|
| `bk_avwap` | 8,103 | +9.08% | scanner-projected, NOT realized |
| `deep_scan` | 1,904 | +9.17% | scanner-projected, NOT realized |
| `bk_orb` | 451 | +10.12% | scanner-projected, NOT realized |
| `uhura` | 34 | +3.06% | scanner-projected, NOT realized |
| `bk_box` | 117 | +0.44% | scanner-projected, NOT realized |
| `fred_bankrate` | 5 pending | — | — |
| `grok_kirk_scan` | 22 pending | — | — |

**One fleet-acted observation** (`bk_orb`): acted avg_fwd_1d = +5.04% (scanner-projected).
`acted_by_fleet` structurally ~0 across all sources — retrospective join dead-end confirmed.
`by_grade`: 11,017 null (pre-fix rows), 4 grade-A, 22 grade-B (forward-only; populating correctly).

**`/api/measurement-health` at snapshot:**
```
evaluator status: OK  (fill_rate 22.9% > 5% threshold)
filled_1d:  2,531 / 11,057
last_run:   2026-06-28 21:26:55
```

---

## P1 OPEN ITEM: REWIRE fwd_return_1d TO REALIZED RETURN

**Design — do not build without Admiral greenlight.**

### What it needs to do
For each evaluated observation: compute actual market price change from signal `ts` to `expiry` (or nearest close), using a real price feed. Replace the `deep_scan_results.target_price` proxy entirely.

### Price source
**Alpaca historical bars** — already integrated, authenticated, used by the fleet. Endpoint: `GET /v2/stocks/{symbol}/bars` with `start`, `end`, `timeframe=1Day`. Returns OHLCV. Use `close` at signal date as entry proxy; `close` at expiry date (or last available) as exit.

No Schwab reads (RULE #1). No new external APIs needed.

### Schema change
`fwd_return_1d` already exists as a nullable REAL column. The backfill would SET it where currently NULL or overwrite the current scanner-projected values. **Overwriting existing values is a schema mutation decision** — requires Admiral call on whether to:
- (A) Overwrite `fwd_return_1d` with realized return (cleanest; single source of truth)
- (B) Add `fwd_return_1d_realized REAL` column alongside the existing proxy (preserves history; two fields to track)

Recommendation: Option A — the proxy values are not meaningful; keeping them creates confusion. But Admiral decides.

### Backfill plan
1. For each `signal_observations` row where `fwd_return_1d IS NOT NULL` (2,531 rows) OR `evaluated_at IS NOT NULL` (all evaluated rows): fetch Alpaca daily bars for `(ticker, ts_date, expiry_date)`.
2. Compute `realized = (close_at_expiry - close_at_signal) / close_at_signal`.
3. Write to `fwd_return_1d` (or new column, per Admiral decision).
4. Rate-limit against Alpaca: batch by ticker, cache per-ticker bars, ~2,531 unique (ticker, date) pairs max. Estimate: a few minutes at conservative rate.
5. Forward: wire `signal_evaluator.py` to fetch realized return at eval time instead of `deep_scan_results` join.

### Effort
~3–4h total: Alpaca bars fetch helper, backfill runner (same pattern as `nightcrew_fwd_return_backfill.py`), evaluator wiring, dry-run verification. No trading logic. No restart risk for backfill; evaluator change needs restart.

---

## TASK STATUS

| Task | Status | Notes |
|---|---|---|
| Rung 1 HTML | **BUILT, not yet integrated** | `_nightcrew/carrier_rung1_contact.html` — 5 integration markers; awaits Task 1 integration work |
| Rung 2 spec | **FILED** | `drafts/RUNG2_CONTEXT_ON_ARRIVAL_SPEC.md` |
| Emit-time acted tagging spec | **FILED** | `drafts/EMIT_TIME_ACTED_TAGGING_SPEC.md` — Admiral-review gate, touches fire path |
| Backfill drain | **Mechanically complete** | 10,628/11,057 evaluated; 415 in-window pending; drained the wrong metric (scanner-projected, not realized) |
| Alpha read | **INVALID** | See headline — no deployment authority established |
| Realized-return rewire | **P1 OPEN** | Design above; do not build without Admiral greenlight |

---

## OPEN QUESTIONS FOR ADMIRAL

1. **Realized return column:** Option A (overwrite `fwd_return_1d`) or Option B (add `fwd_return_1d_realized`)? This decision gates the backfill build.
2. **Rung 1 integration:** greenlight to integrate `carrier_rung1_contact.html` into bridge-v2? Zero execution risk; pure display.
3. **Emit-time acted tagging:** ready for Admiral review of `drafts/EMIT_TIME_ACTED_TAGGING_SPEC.md`? Touches fire path — requires explicit go.
4. **Measurement loop:** once realized-return rewire is built and backfilled, re-run `/api/observations/summary` for the real alpha read. That is the deployment authority gate.
