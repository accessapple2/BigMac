# Relay — B9: calibration_map reconciliation, not a contradiction. 2026-09-11

## The question

Two numbers appeared to describe the same thing and disagree: last
night's calibration read (`relay_2026-09-10_phase2-bakeoff-and-phase13-
spec.md`) reported **169 McCoy trades stated confidence in 0.9-1.0,
realized hit rate 78.6%**. Live-checking `engine/calibration_map.py`
today (ahead of the B9 decision) showed **3 total rows, 0 populated
buckets**. Asked directly: are these different populations, or did one
of the two findings need withdrawing.

## Answer: different populations, both correct, nothing to withdraw

**`calibration_map.py`'s actual methodology** (`load_calibration_data()`):
requires `decision_audit.event_type='trade_fire'` with `trade_id`
populated, JOINed to a `trades` row with **non-NULL `realized_pnl`**.
Reproduced the query by hand, in two steps, to isolate exactly where the
row count collapses:

| Filter applied | Rows |
|---|---|
| `trade_fire` events joined to a real trade (player/date-window match) | 193 |
| ...AND that trade has a **settled** (non-NULL) `realized_pnl` | **3** |

**190 of the 193 linked trades are still open.** The join itself isn't
broken — the bottleneck is that almost none of the `trade_fire`-linked
trades have closed yet.

**Last night's 169-trade number bypasses this join entirely** — it's a
"regime-blind check on `trades.confidence` directly," per its own doc
text, querying `trades.confidence`/`trades.realized_pnl` straight, with
no requirement that a `decision_audit.trade_fire` event ever pointed at
the trade. Reproduced fresh just now: **474 clean closed trades, 168 in
the 0.9-1.0 bucket, 78.6% hit rate** — 168 vs. 169 is one trade's drift
over a day (new closes since last night), not a discrepancy.

## The real finding underneath the apparent contradiction

**The `trade_fire`→`trade_id` linkage `calibration_map.py` depends on is
populated for essentially none of McCoy's settled trades — 3 out of 474
(0.6%).** This isn't primarily a "wait for more data" problem (the B9
hold condition as originally framed) — it's that the map's chosen join
requirement structurally excludes 99.4% of the exact population it needs,
regardless of how much more time passes, unless that linkage itself gets
backfilled or populated more broadly going forward. More trading days
will grow the 474-trade population; they will NOT grow the 3-trade one at
anywhere near the same rate unless something separately starts populating
`trade_id` on `trade_fire` events at write time.

## Disposition

Per the Admiral's decision (B9, this session): **held**, same as the
confidence-coordination decision spec'd earlier today — no gate wired to
`calibration_map` while it has 0 usable buckets. This reconciliation adds
detail to *why* the hold is even more clearly correct than the original
"1 populated bucket" framing suggested, and files the linkage-narrowness
finding as its own backlog decision point (`docs/XO_BACKLOG.md`) —
whether to relax the map's methodology to the same direct-`trades`-table
population last night's finding used, or invest in backfilling the
`trade_id` linkage instead. Not decided or built here.
