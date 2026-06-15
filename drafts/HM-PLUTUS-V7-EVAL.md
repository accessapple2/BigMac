# HM-PLUTUS-V7-EVAL — Quality-Eval Harness + v1/v6 Baseline

**Date:** 2026-06-14 · **Mode:** read-only on existing data, create-only artifacts. No training,
no LoRA, no model create/delete, no flag/restart, no sudo, no deletes. Inference .168-only.
**Artifact tree:** `scripts/plutus_v7_eval/` (code) + `data/plutus_eval/` (frozen set, gens, scorecard).

## TL;DR
Built an **absolute, repeatable trade_critique quality eval** (6-dim 0–2 rubric, two judges, plus
deterministic guards) to replace the loose pairwise bakeoff. It **reproduces the known v6 regression**
(self-sanity-check PASS), so v1's baseline is trustworthy as the line a future v7 must beat.

- **v1 trade_critique baseline = 1.607 / 2.0** (the target line). v6-eval = 1.304 (−0.303). 
- Regression holds **per-judge** (qwen3 1.653>1.377; gpt-oss 1.561>1.230) and on the input-echo guard
  (v6 restatement 0.071 > v1 0.054). The eval detects what it must.

---

## Phase 0 — frozen OOS set
- **File:** `data/plutus_eval/oos_set_v1.jsonl` — **178 scenarios**, frozen with stable `id`.
- **Source (labeled):** the held-out **TEST split** `plutus_corpus_v6.test.jsonl` — OOS w.r.t. v1/v6
  *training* (trained on the train split only; v6-eval explicitly excludes val+test).
- **Leakage check:** **0** test prompts appear in train or val. Zero leakage.
- **Actual stratification** (vs the 60/20/10/10 target — deviates because that's what the clean source
  contains; reported, not forced):
  | category | n | % |
  |---|---|---|
  | trade_critique | 110 | 61.8% |
  | debate_critique | 46 | 25.8% |
  | signal_analysis | 16 | 9.0% |
  | market_qa(+variant) | 6 | 3.4% |
- n=178 ≥ 100 overall; **thin categories flagged** (signal_analysis 16, market_qa 6 — per-category
  reads there are indicative only). trade_critique (the gate's purpose) has full n=110.

## Phase 1 — scorer (`scripts/plutus_v7_eval/score.py`)
**Rubric (judge-scored, 0/1/2 each, 6 dims):** risk_id · directional_lean (vs realized) · calibration ·
actionability · non_redundancy · format_concision. Per-dim = mean of the two judges; each judge kept raw
in `judge_raw.jsonl` for agreement.
**Programmatic guards (deterministic, no judge):** `leads_with_verdict` (regex), `direction_vs_realized`
(WIN/LOSS framing vs realized P&L sign), **`restatement_ratio`** (output-vs-prompt trigram overlap — the
direct catch for v6's input-echo tell), `length_in_band`.
**Judges (.168):** qwen3:14b + gpt-oss:20b (same pair as the v6 bakeoff). Defensive JSON parse (strip
fences/`<think>`, retry once, mark unscorable on 2nd fail → excluded from means). **Unscorable: 0/712.**
**GPU discipline:** judges in the OUTER loop → exactly one judge model resident at a time; `keep_alive=30s`,
never pinned. All generations precede scoring.

> **Reasoning-judge fix (built into the harness, reusable for v7):** both judges burn their token budget
> on hidden reasoning before emitting JSON. qwen3:14b honors top-level `think:false` → clean JSON at a
> small budget. gpt-oss:20b *ignores* `think:false` and always reasons into its `thinking` field (which
> counts against `num_predict`); `think:'low'` keeps that short (~870 chars) so a modest budget fits the
> reasoning + the answer. Per-judge config lives in `common.py::JUDGE_CFG`. Smoke-tested green (0
> unscorable, κ + guards computing) before the full 712-call run.

## Phase 2 — generation (`scripts/plutus_v7_eval/generate.py`)
`data/plutus_eval/gen_v1_v6.jsonl` — v1 & v6-eval critique per scenario. **All 178 reused from the
committed v6 bakeoff** (`scripts/plutus_v6/results/bakeoff_gen.json` — identical models, identical
prompts) → **zero generation GPU on the shared box**; plutus gen is temp=0.3 (non-deterministic), so
reusing the committed outputs is also the reproducible choice. Idempotent; `--force-fresh` regenerates
on .168 if ever needed.

---

## Phase 3 — scorecard (`data/plutus_eval/scorecard_v1_v6.json`)

### v1 BASELINE — the target line (per-dim, 0–2)
| dim | ALL (n=178) | **trade_critique (n=110)** | debate_critique (n=46) | signal_analysis (n=16) |
|---|---|---|---|---|
| risk_id | 1.084 | **1.273** | 0.609 | 1.281 |
| directional_lean | 1.264 | **1.627** | 0.587 | 0.969 |
| calibration | 1.264 | **1.650** | 0.554 | 0.812 |
| actionability | 1.053 | **1.464** | 0.043 | 1.312 |
| non_redundancy | 1.371 | **1.668** | 0.696 | 1.344 |
| format_concision | 1.559 | **1.959** | 0.815 | 1.250 |
| **overall** | **1.266** | **1.607** | 0.551 | 1.161 |
| restatement_ratio | 0.051 | 0.054 | 0.029 | 0.102 |
| direction_match % | 56.4 | 56.4 | n/a | n/a |

### v6 contrast + SELF-SANITY-CHECK → **PASS** (eval reproduces the known regression)
| metric (trade_critique) | v1 | v6-eval | Δ |
|---|---|---|---|
| **overall** | **1.607** | 1.304 | **−0.303** ✓ v6 below v1 |
| risk_id | 1.273 | 1.005 | −0.268 |
| directional_lean | 1.627 | 1.286 | −0.341 |
| calibration | 1.650 | 1.345 | −0.305 |
| actionability | 1.464 | 1.073 | −0.391 |
| non_redundancy | 1.668 | 1.336 | −0.332 |
| format_concision | 1.959 | 1.777 | −0.182 |
| **restatement_ratio** | 0.054 | **0.071** | **+0.017** ✓ v6 echoes input MORE |

- v6 regresses on **every** trade_critique dimension, and its restatement_ratio is higher — the exact
  input-echo tell from the diagnostic. **The eval is calibrated.**
- Holds **per-judge independently**: qwen3 v1 1.653 > v6 1.377; gpt-oss v1 1.561 > v6 1.230.
- Secondary cross-check: on **debate_critique**, v6 ≥ v1 (0.601 vs 0.551) — matching the bakeoff's finding
  that v6 only helped where its training targets were real. The eval reproduces *both* directions.

### Inter-judge agreement (per dim, pooled, n=356 pairs)
| dim | Cohen's κ | exact-agree % | mean abs diff |
|---|---|---|---|
| actionability | 0.503 (moderate) | 66.3 | 0.357 |
| format_concision | 0.395 (fair) | 66.9 | 0.413 |
| calibration | 0.378 (fair) | 59.6 | 0.478 |
| directional_lean | 0.299 (fair) | 54.8 | 0.604 |
| non_redundancy | 0.271 (fair) | 53.7 | 0.478 |
| risk_id | 0.126 (slight) | 49.4 | 0.539 |

κ computed (unweighted, 3-level); exact-agreement % + mean-abs-diff reported alongside. **Caveat:**
`risk_id` (κ=0.126) and `non_redundancy`/`directional_lean` carry real per-dim judge noise — treat their
*absolute* values as soft. The **v1>v6 ranking is robust** (both judges agree, large margin), and the
gate uses a +0.15 margin precisely so judge noise can't flip a verdict.

---

## LOCKED go/no-go gate for a future v7 (vs this v1 baseline, on this frozen OOS set)
Promote v7 to a live seat **only if ALL hold on trade_critique:**
1. **overall ≥ 1.757** (v1 1.607 + 0.15).
2. **no dim regresses >0.1** vs v1 — i.e. risk_id ≥1.173 · directional_lean ≥1.527 · calibration ≥1.550 ·
   actionability ≥1.364 · non_redundancy ≥1.568 · format_concision ≥1.859.
3. **restatement_ratio ≤ 0.054** (≤ v1).
4. **direction_match ≥ 56.4%** (≥ v1).
Anything short → keep v1 (current state). v6-eval **fails all four** — correctly.

## How to re-run
- **Full eval (v1 vs v6):** `python3 scripts/plutus_v7_eval/run_eval.py` (idempotent — reuses frozen set,
  gens, and any judged rows; only fills gaps).
- **Score a future v7:** add `"v7": "plutus-v7:latest"` to `PLUTUS_MODELS` in `common.py`, extend the two
  `("v1","v6")` model tuples in `generate.py`/`score.py` to include `"v7"`, then re-run `run_eval.py`;
  compare v7's trade_critique scorecard against the LOCKED gate above.
- **Refresh OOS with post-cutoff real trades later:** delete `data/plutus_eval/oos_set_v1.jsonl`, add a
  post-cutoff source branch to `freeze_oos.py` (closed `trades` + `debate_history_v2` dated after the
  corpus cutoff, read-only), re-run. Everything downstream is idempotent. Bump the filename to
  `oos_set_v2.jsonl` to keep the v1 baseline reproducible.

## Judgment calls made (for review)
1. **OOS source = held-out test split (option b), full 178 rows.** Real post-cutoff trades (option a,
   ~8–24 closed) were too few to stand alone. Used all 178 rather than subsampling to the 120–150 target
   to maximize power and avoid an RNG sampling choice. Stratification therefore reflects the source
   (trade_critique 62%, debate 26%) not the 60/20/10/10 target — reported, not forced.
2. **decisive_risk = "unknown" for all scenarios** — the corpus has no decisive-risk field; per spec the
   judge assesses Risk-ID against the prompt context. (A future OOS refresh could hand-label this.)
3. **Reference completion NOT shown to judges.** The corpus completion is the synthetic template v6
   overfit; using it as gold would bias toward template-matching. Kept in the frozen set for traceability
   only; scoring is absolute against scenario + realized outcome.
4. **Generations reused from the committed bakeoff** (identical models/prompts) for GPU economy on the
   shared box; plutus temp=0.3 makes fresh gen non-reproducible anyway.
5. **Per-judge inference config** (qwen3 `think:false`; gpt-oss `think:'low'` + larger budget) — required
   to get JSON out of the reasoning judges; documented in `common.py::JUDGE_CFG`.
6. **κ reported unweighted (3-level)** plus exact-agreement % and mean-abs-diff; low-κ dims flagged.
7. **direction_vs_realized is a lenient pos/neg keyword heuristic** (a guard, not a judge dim) — reads
   ~56–58% for both models; use it as a relative signal between models, not an absolute accuracy.

## Files (new, create-only)
- `scripts/plutus_v7_eval/{common,freeze_oos,generate,score,run_eval}.py`
- `data/plutus_eval/{oos_set_v1.jsonl,gen_v1_v6.jsonl,judge_raw.jsonl,scorecard_v1_v6.json}`
- `drafts/HM-PLUTUS-V7-EVAL.md` (this doc)
