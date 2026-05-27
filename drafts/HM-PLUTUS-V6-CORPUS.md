# HM-PLUTUS-V6-CORPUS

**Status:** SPEC ONLY. Do not start training or scraping based on this doc.
**Target:** Plutus v6 fine-tune ready for mid-June 2026.
**Predecessor:** Plutus v1 (corpus v4, 1,199 examples) currently shipped to
production via Dr. McCoy as of 2026-05-27 — see
`drafts/HM-PLUTUS-V5-WIN-2026-05-27.md`.

## Goal

A second-generation Plutus fine-tune trained on a substantially larger and
more diverse corpus, designed to:
- Reduce critique brittleness on edge-case trades the v1 fumbles
- Improve risk-identification recall on multi-leg options and short trades
- Calibrate confidence scoring against real-world outcomes (rallie.ai signal
  reviews provide labeled outcomes the v4 corpus didn't have)
- Hold the production-tested identity tiling at the SYSTEM-prompt layer rather
  than baking it into the corpus (lower overfit pressure on small models)

## Corpus targets

- **Size:** 2,500+ examples (current v5 = 1,259 lines; v4 = 1,199; target is
  ~2× growth)
- **Diversity:** at least 20% of new examples must be **adversarial** —
  trades that look obvious but have a buried risk (early earnings, hidden
  beta exposure, IV crush setup, options-near-ex-div, etc.)
- **Labels:** every example with `outcome` field where available
  (rallie.ai-sourced examples should have ground-truth labels for retro
  evaluation; Plutus-output-review examples may be unlabeled)

## Data sources

### Source 1 — rallie.ai scraper (PRIMARY, not yet verified)

**Existing infrastructure (do not assume working):**
- `engine/rallies_scraper.py` — scraper module
- `engine/rallies_parser.py` — parser
- `engine/rallies_intel.py` — intel layer
- `scripts/scrape_rallie.py` — orchestration script
- `data/rallie_trades.jsonl` (15 rows) and `data/rallie_trades_v2.jsonl`
  (10 rows) — appear to be exploratory pulls, not full corpus runs
- `data/rallies_snapshots/` — empty as of 2026-05-27

**Pre-build verification step (HM-LESSON-VERIFY-DATA-SOURCE-FIRST applied):**
Per the lesson banked at `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md`, the
scraper must be **live-probed** before this spec is locked into a build:

1. Run `scripts/scrape_rallie.py` with a tiny dry-run flag (10 trades).
2. Verify: HTTP 200 from rallie.ai, parser produces structured rows, no
   rate-limit blocks within first 10 fetches, output rows have the fields
   v6 corpus shape requires (ticker, entry, exit, P&L, narration).
3. If scraper is broken: STOP. File HM-RALLIE-SCRAPER-REPAIR before unlocking
   this spec. Recovery via Plutus-output-review alone (Source 2) is
   theoretically possible but cuts the corpus diversity story.
4. If rate-limited: document the limit, design fetch cadence accordingly
   (likely overnight cron over multiple sessions to assemble 1,500+ rows).

**Target contribution to v6:** 1,500-2,000 examples (60-80% of corpus).

### Source 2 — Plutus output review (SECONDARY)

Trade-by-trade review of Plutus v1 production critiques over the next 2-3
weeks. Each critique becomes a corpus row:
- **Confirmed-good critique** → tag as positive example
- **Off-domain / wrong / overconfident** → corrected version becomes the
  training target; the bad output is discarded (not used as a "what not to
  do" — those tend to bake the bad pattern in)

**Target contribution:** 500-1,000 examples (20-40% of corpus).

### Source 3 — Existing v4/v5 retained subset (REGRESSION GUARD)

To prevent regression on cases the v1 already handles well, retain ~300 of
the highest-quality v4/v5 examples (curated, not random) and include them
in v6. Manual selection — pick examples where v1 production critiques are
visibly strong.

## Tag stripping (preserved from v4/v5)

The v4 corpus had `AUTO-STOP`, `AUTO-TARGET`, and similar machine-generated
tags interleaved in critique text. v5 stripped these and saw a notable
quality jump. **Same rules apply for v6:**

- Strip all `AUTO-STOP`, `AUTO-TARGET`, `AUTO-*` tokens
- Strip JSON-like decoration that wraps the actual analysis (`{ "decision":
  ...`) — keep the natural-language critique only
- Strip exhortation tokens (`PLUTUS_NOTE:`, `TRADER_PROMPT:`) unless they're
  part of the canonical chat format

**No identity tiling.** v4 had ~50 examples of "Who are you?" → "I am Plutus,
financial intelligence officer..." style repetition to lock identity. v6
moves identity entirely to the Modelfile SYSTEM prompt. Tradeoff: identity
might be slightly weaker out of the box but the model is freed up to learn
domain content with the parameter budget that would have gone to repetitive
identity reinforcement.

## Training environment (pinned)

**Use `scripts/requirements-plutus-train.txt` verbatim** — this is the
sm_120-capable env validated for v1:

```
torch==2.8.0                # CUDA 12.8, sm_120 kernels
transformers==4.51.3
unsloth==2025.4.7
unsloth_zoo==2025.4.4
peft==0.15.2
trl==0.15.2
bitsandbytes==0.46.0
accelerate==1.6.0
# xformers: DO NOT INSTALL — bundled flash-attn lacks sm_120 kernels
```

**Hardware:** RTX 5080 (sm_120) — same as v1 train. Ollie Max RTX 5060 is the
production inference target; do NOT train on Ollie Max (8GB VRAM insufficient
for LoRA training).

**Python 3.12 (deadsnakes PPA on Ubuntu 26.04).** Do NOT upgrade to 3.13/3.14
— transformers 4.51 has known compat issues there per the v1 work.

## LoRA hyperparameters

| Param | v1 (production) | v6 (proposed) | Rationale |
|---|---|---|---|
| Rank `r` | 16 | **8** | Smaller rank reduces overfitting on the 2× larger corpus. Empirically, rank 16 on 1.2k examples produced strong fit; rank 8 on 2.5k preserves expressive capacity while leaving more headroom for generalization. |
| Alpha | 32 (= 2×r) | **16** (= 2×r) | Keep the alpha:r ratio at 2× per Unsloth defaults |
| Dropout | 0.05 | 0.10 | Slightly more aggressive — the labeled rallie.ai data lets us detect overfit via held-out validation |
| Epochs | 3 | 3 | Same. Longer = overfit risk; shorter = under-trained |
| Target modules | q_proj, k_proj, v_proj, o_proj, gate_proj, up_proj, down_proj | same | Full attention + MLP coverage |
| Batch size | per Unsloth default | per Unsloth default | Don't tune — Unsloth picks based on VRAM |

**Expected param budget:** ~0.30% trained (down from 0.58% at rank 16). Wall
time estimate: 12-15 min for LoRA (up from 8 min for v1, due to 2× corpus).

## Modelfile

Reuse the v1 Modelfile pattern. Unsloth regenerates a stock Qwen2.5 Modelfile
on export — **must overwrite immediately after training** with the OllieTrades
custom block:

```
FROM ./plutus-v6-q4km.gguf
TEMPLATE """{{ qwen2.5 tool-calling chat template — copy from v1 }}"""
SYSTEM """You are Plutus, financial intelligence officer for OllieTrades.
Your role is to analyze trades, evaluate signals, and surface risk that
the routing layer may have missed. Be concise. Be specific. Cite numbers."""
PARAMETER temperature 0.3
PARAMETER repeat_penalty 1.1
PARAMETER num_ctx 2048
```

Settings carried from v1's production-tuned config. Do NOT tune these for v6
unless the bakeoff (HM-BM) shows a defect.

## Output

- **Filename:** `plutus-v6-q4km.gguf`
- **Quantization:** Q4_K_M (same as v1)
- **Expected size:** ~4.5-5.0 GB (slightly larger than v1's 4.46 GB due to
  corpus diversity)
- **Deployment:** ship to Ollie Max as `0xroyce/plutus-v6:latest`. Do NOT
  overwrite the v1 tag — keep both available so the bakeoff (HM-BM) can
  compare side-by-side.

## Evaluation

v6 enters the HM-BM-BAKEOFF-SPEC (June 15) as a candidate:
- 100 representative trades from the last 30 days
- Rubric: critique accuracy, risk ID, trade-quality grade
- Comparison set: plutus-v1 (the production v6-predecessor), `0xroyce/plutus`
  (stock), `qwen3:8b`, `qwen3:14b`, `gemma3:4b`, `ollama-kimi`

**Decision criterion:** v6 ships to McCoy only if it beats v1 on ≥2 of the
3 rubric axes. Otherwise stay on v1, retire v6 to the backtest pool, and
schedule v7 corpus expansion.

## Carry-forward risks

1. **Rallie.ai scraper bit-rot** — `engine/rallies_scraper.py` was last
   exercised when `rallie_trades.jsonl` was last touched. Verify pre-build
   per HM-LESSON-VERIFY-DATA-SOURCE-FIRST.
2. **Rallie.ai rate limits / ToS** — confirm scrape volume is within
   acceptable use. If ToS-ambiguous, prefer API access if available.
3. **Adversarial example sourcing** — the 20% adversarial floor is the
   highest-value diversity ask but also the hardest to source. Plan to spend
   ~30% of corpus-prep time on adversarial cases alone.
4. **Lower LoRA rank may underfit** — if rank-8 produces visibly weak
   critiques in early smoke (after first epoch), abort and re-run at rank 12
   as middle ground. Don't go back to rank 16 — that's the v1 setting and
   would reproduce v1's overfit pressure.
5. **Identity drift** — without identity tiling, the model may drift to
   generic financial-analyst voice. Mitigation: the SYSTEM prompt is
   load-bearing. If post-train smoke tests show identity drift, lengthen
   the SYSTEM prompt rather than re-introducing corpus tiling.

## Build sequence (when unlocked)

1. **Verify rallie.ai scraper** — 10-row dry-run per HM-LESSON-VERIFY-DATA-SOURCE-FIRST
2. **Scrape rallie.ai** to assemble 1,500-2,000 rows over multiple sessions
3. **Plutus v1 output review** — accumulate 500-1,000 corrected critiques
4. **Curate v4/v5 retention subset** — 300 high-quality examples
5. **Assemble `data/plutus_corpus_v6.jsonl`** with tag stripping applied
6. **Train on RTX 5080** with pinned env, LoRA rank 8, 3 epochs
7. **Smoke test** — same protocol as v1 (identity prompt, AVGO critique, UPRO
   risk/reward)
8. **Ship to Ollie Max** as `0xroyce/plutus-v6:latest`
9. **Enter HM-BM bakeoff** — June 15
10. **Promote or retire** based on rubric

## Cross-references

- `drafts/HM-PLUTUS-V5-WIN-2026-05-27.md` — v1 production-ship outcome
- `drafts/HM-LESSON-VERIFY-DATA-SOURCE-FIRST.md` — data-source probe doctrine
- `drafts/HM-BM-BAKEOFF-SPEC.md` (queued as ITEM 7 in HM-CLOSET-POWER-PASTE) —
  June 15 evaluation framework
- `scripts/requirements-plutus-train.txt` — pinned training environment
- `engine/rallies_scraper.py` / `scripts/scrape_rallie.py` — scrape pipeline
  (verification-required)
