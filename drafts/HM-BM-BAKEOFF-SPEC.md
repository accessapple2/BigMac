# HM-BM-BAKEOFF-SPEC

**Status:** SPEC ONLY. Do not execute until Plutus v6 lands (mid-June).

## Purpose
Pick the best LLM for the Plutus role (financial intelligence officer — trade
critique, risk identification, signal evaluation). Current incumbent:
`plutus-v1` (fine-tuned Qwen2.5-7B shipped 2026-05-27 per
`drafts/HM-PLUTUS-V5-WIN-2026-05-27.md`). Bakeoff decides whether to keep it
or swap.

## DECISIONS LOCKED (2026-05-27 Admiral sign-off, confirmed by Grok review)
- **Candidates:** 4 (`plutus-v1`, `0xroyce/plutus`, `qwen3:14b`, `gemma3:4b`).
  Plutus v6 enters as 5th if ready in time.
- **Test corpus:** 100 random stratified closed trades from last 30 days
- **Scoring:** hybrid — outcome-aligned auto-score for all 100 + human
  spot-check on 10/model (40 total reviews)
- **Timing:** line up with Plutus v6 (mid-June)

## Candidates

| Model | Size | Type | Role today |
|---|---|---|---|
| `plutus-v1` | 7B | Fine-tuned Qwen2.5 | Production Plutus incumbent |
| `0xroyce/plutus` | 7B | Stock 0xroyce | Current fallback |
| `qwen3:14b` | 14B | Stock Qwen3 larger | Reserve with bigger context |
| `gemma3:4b` | 4B | Stock Gemma | Currently powering dalio-metals |

**Excluded:** `qwen2.5-coder` (wrong domain), `qwen3:8b` (likely dominated by
its 14b cousin), `deepseek-r1:14b` (reasoning model with different output
format — reserve for separate eval).

## Test corpus build

Pull 100 trades from the `trades` table where:
- `executed_at` within last 30 days (rolling, computed at corpus build time)
- Trade is CLOSED (paired BUY+SELL or covered-call lifecycle complete)
- `realized_pnl` is non-null

**Stratify** so distribution roughly matches production:
- 40 stock long entries
- 20 covered call writes
- 20 IC squadron / options
- 20 short / hedge plays

Within each stratum: 50/50 winners and losers (or closest available). Random
selection with `numpy.random.seed(42)` for reproducibility.

**Build script:** `scripts/build_bm_corpus.py`. Output: `data/bm_corpus_v1.jsonl`.

Each row contains: `trade_id`, `symbol`, `entry_price`, `exit_price`, `qty`,
`realized_pnl`, `player_id`, `asset_type`, `option_type`, `strike_price`,
`expiry_date`, `opened_at`, `closed_at`, `reasoning`, `strategy_tags`. Plus
pre-trade market context: VIX at entry, SPY price, sector, ATR, recent
`strategy_signals` in the 24h before entry.

## Critique generation

For each candidate × each trade, prompt the model with:

> "You are Plutus. Analyze this trade. Entry/exit/PnL/context/strategy_tags/
> reasoning details supplied. Provide:
> (1) Primary risk you'd have flagged before entry,
> (2) Secondary risks,
> (3) Recommended action if seen live (BUY/HOLD/SELL/HEDGE),
> (4) Trade quality grade A/B/C/D/F,
> (5) One-sentence summary."

**Parameters:** `temperature 0.3`, `max_tokens 512`, `num_ctx 2048` (same as
production Plutus). Use existing Ollama HTTP client at `OLLIE_URL`
(`192.168.1.168:11434`). **Pre-warm each model with a dummy critique** before
starting the timer.

**Output:** `data/bm_critiques_v1.jsonl` (400 rows = 4 models × 100 trades).

## Scoring rubric (5 points per trade, max 500 per model)

Score 0 or 1 on each dimension:

1. **Identified primary risk?** For losers, did the model flag THE risk that
   materialized? For winners, did it flag a plausible counter that would have
   stopped the trade?
2. **Identified secondary risks?** At least one valid secondary risk (sector
   correlation, time-stop pressure, options decay, gap risk).
3. **Recommended action consistent with outcome?** Winners → BUY/HOLD.
   Losers → SELL/HEDGE/avoid.
4. **Trade quality grade matches outcome?** Winners >5% → A/B. Breakevens →
   C. Losers >5% → D/F. Off-by-one is half credit.
5. **Output coherent and on-domain?** No gibberish, in Plutus voice, under
   300 words.

**Auto-score** via `plutus-v1` as judge initially. If spot-check disagreement
>20%, swap to GPT-4 judge.

## Human spot-check

10 random trades per model = **40 total reviews**. Admiral grades each on the
same 5 dimensions. Compute auto-scorer vs Admiral agreement %. If <80%,
recalibrate rubric and re-score before final ranking.

## Execution phases

| Phase | Duration | What |
|---|---|---|
| 1 | ~1 session | corpus build script + `bm_corpus_v1.jsonl` generation, sanity check |
| 2 | ~2-4 hrs (Ollie Max) | generate 400 critiques via `run_bm_bakeoff.py` |
| 3 | ~30 min | auto-score via `score_bm_critiques.py` |
| 4 | ~1 hr | Admiral human spot-check, agreement %, recalibrate if needed |
| 5 | — | leaderboard markdown + recommended action |

## Decision rules

Winner = highest total. But:
- If within **5 points** of incumbent (`plutus-v1`), prefer incumbent (noise).
- If within **10 points** but worse on Risk-ID or Coherence, prefer incumbent.
- If **per-stratum reversal** (new model wins options but loses stocks),
  consider split deployment before global swap.

## Output deliverables

- `data/bm_corpus_v1.jsonl` — test corpus (committed)
- `data/bm_critiques_v1.jsonl` — raw critiques (committed)
- `data/bm_scores_v1.jsonl` — auto-scores (committed)
- `drafts/HM-BM-BAKEOFF-SPOTCHECK-V1.md` — 40 critiques for Admiral review
- `drafts/HM-BM-BAKEOFF-RESULTS-V1.md` — leaderboard, prose summary,
  recommended action

**Sample leaderboard format:**

| Rank | Model | Total | Risk-ID% | Action% | Grade-Match% | Coherent% |
|---|---|---|---|---|---|---|
| 1 | `plutus-v1` | 412 | 87 | 81 | 78 | 99 |
| 2 | `0xroyce/plutus` | 378 | 81 | 75 | 71 | 100 |

## Risks

1. **Auto-scorer bias** — `plutus-v1` judging itself could inflate its score.
   Spot-check catches this; swap to GPT-4 if detected.
2. **Corpus snapshot lag** — build corpus right before Phase 2 execution,
   not weeks before.
3. **Cold model first run** — pre-warm each model with a dummy critique
   before timing.
4. **Token limits** — `gemma3:4b` has smaller default context. Verify
   critique prompt fits before declaring its loss real.

## Sacred rules

- Do **NOT** overwrite production `plutus-v1` until Admiral approves swap.
- Test corpus reproducible: `numpy.random.seed(42)`, rerun must produce
  identical 100.
- Bakeoff scripts in `scripts/`. Results docs in `drafts/`.

## Dependencies

Plutus v6 (corpus per `HM-PLUTUS-V6-CORPUS` spec) should be trained and
registered as `plutus-v2` on Ollie Max BEFORE Phase 2 runs, so it enters as
candidate #5. If not ready by mid-June, run with 4 candidates and queue v6
for a follow-up v2 bakeoff.
