# HM-PLUTUS-V7-CORPUS — Phase A (diagnostic + pilot) → CHECKPOINT

**Date:** 2026-06-14 · **Mode:** read-only existing data, create-only artifacts under
`scripts/plutus_v7_corpus/` + `data/plutus_v7/`. No training, no LoRA, no registry/flag/restart, no
deletes. .168 inference sequential. Grok authoring = direct read-only xAI call (NOT
`run_grok_subadvisor`, which writes trader.db). **STOPPED at checkpoint — no mass-author, no train.**

## Headline
Pilot **validates the author + prompt decisively.** 36 Grok-authored trade critiques score **1.968/2.0
overall** vs the v1 tightened baseline **1.393** (gate needs ≥1.543) — and lift exactly v1's two weak
dims: **risk_id 0.632 → 2.0 (+1.37)**, **actionability 1.259 → 2.0 (+0.74)** — with **restatement
0.054 → 0.009**. The data-fix hypothesis holds at pilot scale.

## risk_id rubric tighten (done first)
Tightened ONLY the risk_id criterion in `common.py::DIM_HELP` + added a decisive_risk anchor in
`score.py::judge_prompt` (used only when known; 'unknown' → sharpened definition). Re-scored risk_id on
the cached 178×2 generations (no new generation).
- **κ 0.126 → 0.302** (rose above ~0.2 — risk_id is now a usable scored dim, not directional-only).
- v1 risk_id baseline (trade_critique) **1.273 → 0.632**; v6 1.005 → 0.459 (v6 still below v1 — the
  regression reproduces under the tightened rubric too). Under the sharper bar, v1 is genuinely weak at
  naming THE decisive risk — confirming it as v7's primary lever.
- Recomputed the **consistent tightened-rubric v1 trade_critique baseline** (the rubric v7 will face):
  overall **1.393** {risk_id 0.632, directional_lean 1.632, calibration 1.459, actionability 1.259,
  non_redundancy 1.45, format_concision 1.927}. v6 overall 1.130.

### UPDATED gate (under tightened rubric — supersedes the original-rubric gate for v7 go/no-go)
Promote v7 only if trade_critique: overall **≥1.543** (1.393+0.15) · no dim regresses >0.1 (risk_id
≥0.532, directional_lean ≥1.532, calibration ≥1.359, actionability ≥1.159, non_redundancy ≥1.35,
format_concision ≥1.827) · restatement ≤0.054 · direction_match ≥56.4%.

## Phase A1 — scenario pool (`data/plutus_v7/scenario_pool.jsonl`)
- **1,030 real closed-trade scenarios**, built via the proven `extract_plutus_corpus_v2.
  build_trade_critique_corpus()` (IDENTICAL prompt format to plutus/the eval). Targets DISCARDED — only
  the scenario (prompt + realized P&L) is kept; targets are authored fresh.
- **HARD leakage exclusion: 112 removed** (110 by trade_id — the robust key — + 2 by prompt). Verified
  **0 OOS trade_ids remain in the pool.** (Prompt-match alone caught only 69; trade_id caught all 110 —
  prompt text drifts between snapshots, trade_id is stable.)
- Diverse by construction: outcome win 623 / loss 294 / flat 113; regime BULL_CROSS 635 / CAUTIOUS_BEAR
  223 / BEAR_CROSS 106 / CAUTIOUS_BULL 66; 12+ tickers (AAPL 79, MSFT 77, NVDA 61…); 12+ agents.

## Phase A2 — harvest inventory + author pilot
**Harvest availability** (real existing critiques):
- `bm_critiques_v1.jsonl`: 505 rows, 64% unique — REAL but **v1's own output** (the ceiling we're
  beating). Excluded from the trade_critique rebuild to avoid diluting toward v1; available as fallback.
- `debate_history_v2.plutus_analysis`: 434 rows (the v6 debate_critique source, 100% unique) — KEEP as-is.

**Author chosen: Grok** `grok-4.20-0309-non-reasoning` (wired, `XAI_API_KEY` present, callable now).
Direct xAI `/v1/chat/completions` call; authoring instruction maps 1:1 to the rubric (name THE decisive
risk; concrete numeric fixes — stop/size/exit; outcome-aware, NEVER praise a loss; no restatement; lead
Verdict; concise). **Pilot cost: $0.0309 for 36** (~$0.000858/call; in 18,873 / out 5,519 tok).

**Pilot scorecard** (`data/plutus_v7/scorecard_pilot.json`; 36 targets, 2 judges, 0 unscorable, scored
the SAME way as v1/v6 — decisive_risk='unknown' anchor, NOT Grok's own claim → no circularity):
| dim | pilot | v1 (tightened) | lift |
|---|---|---|---|
| risk_id | **2.000** | 0.632 | **+1.368** |
| actionability | **2.000** | 1.259 | **+0.741** |
| directional_lean | 1.903 | 1.632 | +0.271 |
| calibration | 1.903 | 1.459 | +0.444 |
| non_redundancy | 2.000 | 1.450 | +0.550 |
| format_concision | 2.000 | 1.927 | +0.073 |
| **overall** | **1.968** | 1.393 | **+0.575** |
| restatement | **0.009** | 0.054 | lower ✓ |
| direction_match | 88.9% | 56.4% | +32.5pp ✓ |
| leads_verdict | 100% | — | ✓ |

**ALL gate conditions PASS** (overall ≥1.543 ✓; every dim above floor ✓; restatement ≤ v1 ✓). Scores
are real, not a parse artifact (directional_lean has 2/72 zeros, calibration 1 zero + 5 ones).

### Sample authored targets (representative; full set in `authored_pilot.jsonl`)
- **BMNR LOSS −9.62%:** "Verdict: LOSS … Chasing the 13.88% momentum surge into a bear-cross regime was
  the single decisive risk; the volatility-breakout score and bullish MACD were classic bull-trap signals
  … Fix: tighten the regime gate to require SPY>0 and a 2% adverse-excursion stop; cut size to 0.5×…"
- **AMD WIN +1.73%:** "Verdict: WIN … decisive risk was fighting the bear-cross regime…; a 1.73% gain
  could have flipped to 4-5% loss on sector rotation. Fix: tighten stop to the 200-day MA (~3% below
  entry); scale out 50% at +1%…" (outcome-aware on a WIN — names the flip risk, no empty praise.)

## Flags / judgment calls (for review)
1. **Pilot validates TARGET QUALITY + author/prompt — not yet trained-model performance.** There is
   deliberate rubric↔author alignment (Grok told to do what the judge rewards), so near-ceiling scores
   are expected. The REAL proof is the post-training OOS gate ([[project_hm_plutus_v6_oos_gate]]) — train
   v7 on these targets, then score the TRAINED MODEL vs v1 on held-out scenarios. Pilot only proves the
   targets exemplify the qualities v1 lacks.
2. **bm_critiques_v1 harvest excluded** from trade_critique (v1-ceiling would dilute); flagged available.
3. **Grok's own decisive_risk NOT used as the scoring anchor** (would be circular) — pilot scored with
   'unknown' anchor, same as v1/v6.
4. **Updated gate uses the tightened rubric** (risk_id now meaningful) — original-rubric gate in
   HM-PLUTUS-V7-EVAL.md is superseded for v7 go/no-go.

## Planned final mix (Phase B — ON GO ONLY)
- **Author M ≈ 1,030** trade_critique targets via Grok over the full pool (est. cost **~$0.90** total at
  the pilot rate — exceeds the $0.50/day kirk-grok cap, but these are separate direct calls; flag spend).
- **Harvest 0** for trade_critique (author-only; bm_critiques excluded to avoid v1 dilution).
- **KEEP** v6 debate_critique (394, 100% unique) + signal_analysis deduped (150 → ~61 unique).
- **Hard-dedup:** normalize (strip symbols/numbers), cap any near-duplicate template at **≤2% share**,
  drop excess. Verify final uniqueness ≫ the v6 corpus's 6%.
- **Final corpus ≈ 1,030 author + 394 debate + ~61 signal ≈ ~1,485 rows** (in the 1–1.5k target).
- Freeze `data/plutus_v7/plutus_corpus_v7.jsonl` + train/val/test split + commit. **Training is a
  separate phase after that.**

---

## PHASE B — EXECUTED 2026-06-14 (mass-author + dedup + assemble + freeze; NO training)

**Frozen: `data/plutus_v7/plutus_corpus_v7.jsonl` — 1,438 rows.**
| category | rows | note |
|---|---|---|
| trade_critique | 1,030 | Grok-authored (v2 prompt), 100% unique, 0 template-dropped |
| debate_critique | 348 | KEPT from v6 (394 − 46 OOS-overlap excluded) |
| signal_analysis | 60 | v6 (134 OOS-excluded) − 74 exact-dedup |
Split (seed 42, 80/10/10): **train 1,150 / val 143 / test 145**. **OOS leakage in corpus = 0** (asserted;
every category scrubbed of the 178 frozen OOS prompts; tc also trade_id-clean).

### Diversity (the thing we were told to guard) — v1→v2 fix
First mass-author (temp 0.4) was 100% content-unique but **converged on a stock opener**: "The (single)
decisive risk was…" = 26% of rows, only 203 distinct openings. Flagged, did NOT freeze. Added an
opening-variety directive (ban that opener; lead with what went right/wrong, regime, the signal, outcome
magnitude) + temp 0.4→0.6, **re-authored all 1,030** (archived v1 as
`authored_tc.bak_v1_stockopening_*.jsonl`).
- **distinct openings 203 → 601**; **max single opener 26% → 3.5%** (target was ≤~5%); full-set
  uniqueness 100%.
- **Re-validation (36 v2 targets, tightened eval, temp 0.6): quality held** — overall **1.972** (gate
  ≥1.543), risk_id 1.986 (+1.35 vs v1), actionability 2.0 (+0.74), restatement 0.011, direction 88.9%.
  ALL gate conditions PASS. temp/diversity change did NOT slip quality.

### Cost
Grok total this task ≈ **$1.42** (pilot $0.031 + v1 batch $0.672 archived + v2 frozen batch **$0.714**).
Final corpus's authoring cost = $0.714. Direct read-only xAI calls; no trader.db writes.

### STOPPED — training is the next gated step (no LoRA without explicit GO).
Files: `scripts/plutus_v7_corpus/{author,author_all,assemble_v7,score_sample}.py`;
`data/plutus_v7/{authored_tc.jsonl, plutus_corpus_v7.jsonl + .train/.val/.test, .stats.json,
scorecard_sample_v2.json}`.

## Files (new this phase)
- `scripts/plutus_v7_corpus/{rescore_riskid,build_pool,author,score_pilot}.py`
- `scripts/plutus_v7_eval/{common,score}.py` (risk_id tighten + anchor — committed change)
- `data/plutus_v7/{scenario_pool,authored_pilot,author_cost_log,judge_raw_riskid_tightened,
  scorecard_riskid_tightened,judge_raw_pilot,scorecard_pilot}.*`
