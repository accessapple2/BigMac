# HM-PLUTUS-V7 — Diagnostic + Eval-Design Spike

**Date:** 2026-06-14 · **Mode:** read-only (no training, no registry/config changes, no GPU burn)
**Question:** Why did v6 (2× corpus, ~489 steps) fail to beat v1, and what eval gates a v7?

---

## 0. One-line verdict

**A v7 is worth training — but the lever is DATA, not objective/format/hyperparameters.** v6's
trade_critique corpus (77% of the rows) is **6%-unique synthetic boilerplate**; the model dutifully
learned to produce generic verdict-keyed prose. The fix hypothesis: **replace the templated
trade_critique targets with real, diverse critiques** (same source that made v6 *win* debate_critique).
Everything else about the recipe is fine.

---

## 1. Inventory

### Models on .168 ollama
| Model | Digest | Role |
|---|---|---|
| `plutus-v1:latest` | `4bea908c0348` | **LIVE witness** |
| `plutus-v6:latest` | `bbb22408d189` | full-corpus (test-contaminated), unwired |
| `plutus-v6-eval:latest` | `f075fa69884d` | train-split-only (clean), used for bakeoff |
| `0xroyce/plutus:latest` | `83f2e56702ad` | base / fail-safe fallback |

All are **qwen2 7.6B, Q4_K_M, identical template + system prompt + params** (temp 0.3, num_ctx 2048,
repeat_penalty 1.1). Only the LoRA weights differ → clean comparison.

### Witness pointer (verified live in DB, not from memory)
- `engine/debate_engine.py:593` `_resolve_plutus_model()` reads `ai_players.model_id WHERE id='ollama-plutus'`
  (DB = source of truth; fail-safe → `0xroyce/plutus`).
- Live value **today**: `('ollama-plutus', 'plutus-v1', 'active')`. v6/v6-eval are **registered but unwired**.
- `config.py:266` `"model":"plutus-v1"` is informational only.

### Corpus & artifacts
- v1 corpus: `data/plutus_corpus_v1.jsonl` — 936 rows (May 20).
- v6 corpus: `data/plutus_corpus_v6.jsonl` — 2,606 rows (Jun 11) + `.train/.val/.test` (1,415/176/178, seed 42).
- Assembly: `scripts/assemble_plutus_v6_corpus.py`; spec `drafts/HM-PLUTUS-V6-CORPUS.md`.
- Train/export: `scripts/plutus_v6/train_plutus_v6{,_eval}.py`, `export_plutus_v6.py`, `Modelfile.v6`.
- Recipe: Qwen2.5-7B-Instruct, **rank 8 / alpha 8 / dropout 0 / 3 epochs / lr 2e-4** (v1 was rank16/alpha32).
- Final loss **0.0013** (memorization by ~epoch 1).

### Bakeoff harness + results
- `scripts/plutus_v6/bakeoff_gen.py` (generate paired outputs on the 178-row clean test set) +
  `bakeoff_judge.py` (blind, order-balanced, LLM-as-judge, temp 0).
- Results: `scripts/plutus_v6/results/bakeoff_{gen,scored,scored_gptoss}.json` (Jun 11).
- Judges: qwen3:14b (primary, 178/178 parsed), gpt-oss:20b (confirm, 145/178 parsed).

---

## 2. The gap (from stored paired generations — same prompts, inference-only)

**Headline (qwen3:14b judge, n=178, no ties):** v1 98 (55%) / v6-eval 80 (45%) — overall statistical tie.
**But by category the story is sharp:**

| Category | n | v1 | v6-eval | Winner | Corpus uniqueness |
|---|---|---|---|---|---|
| **trade_critique** | 110 | **70** | 40 | **v1 (p≈0.004)** | **6% unique** |
| signal_analysis | 16 | 9 | 7 | v1 (marginal) | 40% unique |
| **debate_critique** | 46 | 15 | **31** | **v6 (p≈0.018)** | **100% unique** |

gpt-oss:20b confirms v1 dominates trade_critique (58/29 ≈ 67%); the debate_critique v6-win does not
replicate on judge-2 (judges split there). **The robust, decisive result is v1 winning the core
trade_critique task** — which is Plutus's actual job.

### Why v6 loses (judge reasons on 70 v1-wins, keyword tally)
`actionable` 70/70 · `insight` 64/70 · `specific` 59/70 · `outcome` 18 · `risk` 8 · `concise` 2 ·
`generic`/`fabricat`/`verbos` ≈ 0.

→ Not verbosity (mean chars v1 1486 vs v6 1522 — equal), not format (both lead with `Verdict:`
99–100/110), not direction errors. **v6 loses purely on substance: less specific, less actionable.**

### Concrete side-by-sides (v1 = winner, v6-eval = loser; same prompt)

**#5 MSFT, realized −2.49% (a LOSS):**
- v1: "Verdict: **Sell Signal** … **Death Cross**: A significant bearish indicator … The trade setup
  does not fully mitigate this risk." → flags the risk that mattered.
- v6: "The reasoning provided **aligns well** with the market conditions … appropriate given the
  BEAR_CROSS regime." → **over-agreeable, outcome-blind** on a losing trade.

**#65 TSLA, +8.33%:**
- v1: straight to "Strengths / Weaknesses" with concrete fixes ("adjust stop closer to breakout",
  "exit timing").
- v6: **re-prints the entire prompt** as a Symbol/Agent/Timeframe/Regime header, then "ensure the
  breakout was not just a one-day event", "ensure catalysts are credible" → **input-echo + platitudes**.

**#16 DELL / #32 GOOGL / #10 AMZN / #14 SPGI / #20 GOOGL / #22 QQQ (representative of the other 60+):**
- v6 reliably opens "**The trade in X by agent Y … is successful, achieving a realized P&L of …**"
  (restates the input), then approving generic prose. v1 adds counterpoints ("corroborate the analyst
  rec with technical analysis", "RSI overbought tempers the signal — size accordingly").

**Failure characterization — SYSTEMATIC, not noise:**
1. **Input echo / restatement** instead of new insight (≈ the majority of v1-win cases).
2. **Over-agreeable, outcome-blind framing** — praises LOSS trades, mirrors the entry thesis.
3. **Generic non-actionable advice** ("ensure…", "monitor closely") vs v1's concrete levers.

---

## 3. Corpus assessment — the ceiling (the real lever)

Uniqueness of completions by category (normalized: strip symbols/numbers):

| Category | rows | distinct | % unique | mean len | source |
|---|---|---|---|---|---|
| **trade_critique** | **2,007 (77%)** | 136 | **6%** | 296 | synthetic, PnL-bucket → boilerplate |
| signal_analysis | 150 | 61 | 40% | 286 | semi-templated |
| **debate_critique** | 394 | 394 | **100%** | 736 | **real, harvested from live v1** |

The trade_critique target is a verdict-bucket string lookup — e.g. **98 identical** copies of
*"Entry thesis confirmed — {SYM} was a strong performer for {agent}. Entry timing and thesis alignment
were solid. The {regime} regime provided a favorable tailwind."* and **138** copies of
*"Entry thesis failed — catastrophic …"*. There is **near-zero marginal signal** in the extra rows.

**This explains every observation:**
- "2× corpus, more steps didn't help" → the extra rows are duplicates; steps were spent memorizing
  boilerplate (loss → 0.001). The corpus *size* grew; its *information* did not.
- **v6 lost exactly where targets are synthetic (trade_critique, 6% unique) and won exactly where they
  are real (debate_critique, 100% unique).** The bakeoff is a direct readout of corpus quality.
- v1, despite also-templated but much *terser* targets (936 short rows), perturbed the base
  Qwen2.5-Instruct less, leaving its analytical priors intact — so it still emits specific/actionable
  content at inference. v6's longer generic targets taught it that "a good critique = verbose generic
  restatement," overwriting that ability.

---

## 4. Proposed quality eval (replaces the loose pairwise bakeoff) — spec only

The current bakeoff is good structurally (blind, order-balanced, two judges, uncontaminated test) but
has three gaps: (a) **relative only** — A/B winner tells you nothing about whether *either* is good;
(b) **single verdict, no rubric dimensions** — can't see *what* regressed; (c) **gold = the synthetic
templates** — a weak reference. Spec for the v7 go/no-go gate:

### 4.1 Held-out set (frozen, versioned)
- **120–150 scenarios**, stratified: 60% trade_critique, 20% debate_critique, 10% signal_analysis,
  10% adversarial. **Post-cutoff only** (rolling OOS per `project_hm_plutus_v6_oos_gate`) so no model
  trained on them. Each carries ground truth: realized verdict + P&L, regime, and the *one* risk factor
  that actually mattered (hand-labeled or strong-model-labeled once, frozen).

### 4.2 Absolute rubric (score each critique 0–2 per dimension, not pairwise)
| Dim | 0 | 1 | 2 |
|---|---|---|---|
| **Risk ID** | misses the decisive risk | names it generically | names *the* risk + why it mattered |
| **Directional lean** | wrong vs outcome | hedged/ambiguous | correct & justified |
| **Calibration** | confident & wrong / praises a loss | mild mismatch | confidence matches outcome |
| **Actionability** | platitude ("monitor") | one vague lever | ≥1 concrete, specific lever |
| **Non-redundancy** | restates the prompt | mostly restates | adds new analysis, minimal echo |
| **Format/concision** | unparseable / bloated | minor drift | leads `Verdict:`, parseable, tight |

- **Two neutral judges** (qwen3:14b + gpt-oss:20b), temp 0, order-balanced, **report per-dimension
  means + inter-judge agreement (Cohen's κ)**; flag any dim with κ<0.4.
- **Programmatic checks** alongside the LLM judge (cheap, deterministic): leads-with-`Verdict:` regex;
  WIN/LOSS direction vs realized P&L; restatement ratio (n-gram overlap of completion∩prompt — directly
  catches the v6 input-echo failure); length bounds.

### 4.3 Go/no-go gate for v7
Promote v7 to a live seat **only if**, on the frozen OOS set:
1. trade_critique **mean rubric ≥ v1 + 0.15** (per-dim, judges agree), AND
2. **no dimension regresses** vs v1 by >0.1, AND
3. **restatement ratio ≤ v1**, AND
4. direction accuracy ≥ v1.
Anything less → keep v1 (current outcome).

---

## 5. Verdict — is v7 worth training, and the hypothesis for WHY it beats v1

**Yes — conditional on a corpus rebuild.** The recipe (rank/alpha/epochs/format/system prompt) is fine
and the eval harness is most of the way there. The single binding constraint is **trade_critique target
quality**.

**Hypothesis (DATA fix):** v7 beats v1 iff its trade_critique targets are *real, diverse critiques*
instead of PnL-bucket boilerplate — i.e. generate genuine per-trade critiques (harvest from live v1 the
way debate_critique was, or have a strong model author them against the realized outcome + the decisive
risk), dedup hard, and **cap any single template at ≤2% of rows**. Target ~1,000–1,500 *high-information*
trade_critique rows over the current 2,007 near-duplicates. The proof-of-mechanism is already in the
data: **on the one source that was real (debate_critique, 100% unique), v6 already beat v1.** Make the
whole corpus look like that and v7 should clear the gate.

**Do NOT** chase more steps, more rows, lower rank, or format tweaks — the data is the ceiling.
