# Relay — B7: Dataset exporter. 2026-09-11

## What shipped

`scripts/mccoy_dataset_exporter.py` — walk-forward corpus of McCoy's real
Arena decisions: `prompt_text` (exact production prompt), `decision`
(BUY/HOLD), `invalidation`, `reference_price`, `regime`, `model`,
`fwd_return_1d`/`fwd_return_5d`. RULE #1: read-only against `trader.db`
(joins `decision_audit`↔`signals` on `signal_id`, confirmed 3179/3179 join
integrity before trusting it), writes new files only under `data/exports/`.

**Source and honesty about corpus size:** `decision_audit.prompt_text` only
exists from 2026-09-10 onward (Phase 1.1's structured-invalidation logging
shipped that day — there is no earlier history to pull from). This is a
**2-day, 3,179-row corpus**, not a large one. Reporting that plainly rather
than padding the scope.

## Walk-forward split

Sorted chronologically by `created_at` (never shuffled — shuffling a time
series would leak future rows into train) and split **by position**
(70/15/15), not a hardcoded calendar date. A position-based split is the
right call specifically because the data only spans ~1.5 days today — a
date-boundary split would be too coarse-grained to produce three non-empty,
reasonably-sized sets; position-based degrades gracefully and stays correct
as more days accumulate.

| Split | Rows | Date range |
|---|---|---|
| train | 2,225 | 2026-09-10 00:07 → 2026-09-11 01:16 |
| val | 476 | 2026-09-11 01:16 → 2026-09-11 10:15 |
| test | 478 | 2026-09-11 10:15 → 2026-09-11 13:39 |

## Forward returns

`fwd_return_1d`: 2,103/3,179 (66%) — some symbols failed the yfinance pull
(one confirmed delisted, `$CNTA`; others likely thin/illiquid tickers with
no recent bar). `fwd_return_5d`: **0/3,179** — mathematically expected, not
a bug: the corpus only spans ~1.5 calendar days and 5 *trading* days
haven't elapsed for any signal yet. Both fields are present in every row
(as `null` where unavailable) so a consumer can filter rather than assume.

## Options-row filter — real code, currently a no-op

Directive: "options rows included only where restated." Checked before
writing this script — `signals.asset_type` is **100% `'stock'`** for
`ollama-plutus` today, so this filter has nothing to do yet. Implemented it
as real, tested logic anyway (not a stub): any future `asset_type='option'`
row is dropped unless its `trade_id` resolves to a trustworthy row in
`trades_restated` (`pnl_restated_basis='true_pnl'`) or the new
`options_trades_restated` view built in B6
(`restated_status='reconstructed_from_alpaca_bars'`) — both shipped this
session, so this filter is live-wired to the actual restatement
infrastructure, not a placeholder pointing at nothing. Confirmed
`options_rows_dropped: 0` in this run's summary, as expected.

## Model identity caveat, flagged explicitly in the export

Every row is stamped `"model": "plutus-v1"` (from `ai_players.model_id`).
Per this repo's own live doctrine (`CLAUDE.md`, "Ollama Model Aliases"),
**`plutus-v1` is currently an alias of `qwen3:8b`** — a future consumer
training or evaluating against this corpus needs that context; the tag
name alone is misleading about which weights actually produced these
decisions. Not resolved here (out of scope for an exporter), just carried
forward honestly in the field as-is rather than silently relabeled.

## Files

- `data/exports/mccoy_decision_corpus_{train,val,test}.jsonl` — **not
  committed to git** (87MB/19MB/18MB, over GitHub's single-file limit and
  fully regenerable) — `.gitignore` updated (`data/exports/*.jsonl`).
  Regenerate anytime: `python3 scripts/mccoy_dataset_exporter.py`.
- `data/exports/mccoy_decision_corpus_summary.json` — small, committed —
  row counts, date bounds, forward-return coverage, decision distribution
  (BUY 2,978 / HOLD 201), model id.

## Not done / explicitly out of scope

- No dedup or dataset versioning across runs — re-running overwrites the
  three JSONL files in place (the summary.json's timestamp-free content
  means a diff won't show when it was last regenerated; if that matters
  later, add a `generated_at` field, not done here).
- Corpus quality (is 2,978 BUY vs 201 HOLD a healthy class balance for
  fine-tuning?) is not assessed — that's a training-design question for
  whoever consumes this, not an exporter concern.
