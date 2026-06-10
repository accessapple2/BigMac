# HM-FORGE Phase 2.2 — McCoy v6 Retrain Feasibility (SPEC ONLY)

**Goal:** confirm corpus + training path for McCoy v6, GIVEN the Phase 0 VRAM
result (RTX 5080, **16 GB**). No training run here — feasibility + plan only.

## Corpus (free-source, per `project_hm_plutus_v6_corpus`)
No rallie.ai re-sub. Target **2,500** examples from FREE sources:
| Source | Est. count | Notes |
|---|--:|---|
| Own realized outcomes | ~1,252 | McCoy/CSP trades with R-multiple labels |
| Plutus-v1 self-review | 500–1,000 | model-graded verdicts on own history |
| v4/v5 retained | ~300 | curated keepers |
| **Total** | **~2,050–2,550** | hits the 2,500 target band |

**Action before training:** re-confirm live counts (own-outcomes table may have
grown since the estimate) — verify-before-build.

## Base-model decision (gated on 16 GB)
| Option | Train (Unsloth QLoRA) | Serve (Q4) | Verdict |
|---|---|---|---|
| **Keep plutus-v1 base** (~4.7 GB served) | ✅ comfortable on 16 GB | ✅ 4.7 GB | **RECOMMENDED** — proven win, fits, cheap |
| Rebase → ≤14B (e.g. qwen3:14b) | ✅ QLoRA viable on 16 GB | ✅ ~9 GB | Viable if v6 wants a stronger base |
| **Rebase → Qwen3.6-27B** | ❌ QLoRA ~18–22 GB > 16 GB | ❌ Q4 ~18 GB > 16 GB | **NO-GO on current hardware** — both train AND serve exceed 16 GB. Same ceiling as Phase 0.1. |

**Recommendation:** v6 stays on the **plutus-v1 base** (or at most a ≤14B
rebase). The Unsloth **Qwen3.6 rebase path is NOT viable on 16 GB** — it would
require new hardware or an offload-heavy config that defeats the latency goal.

## Training path
- **Unsloth + LoRA** (as v1 shipped — `project_hm_plutus_finetuning_v1`), NOT
  `train_critic.py`.
- Run on .168 in an **isolated venv** (never the live `.venv`); training evicts
  fleet VRAM → **closed-window only**, fleet keep_alive re-warms after.
- Output a new tag (e.g. `plutus-v6`); A/B vs `plutus-v1` in shadow before any
  McCoy cutover. Keep v1 (sacred-data; instant rollback).

## Open items
1. Re-count own-outcomes corpus live (SQL) before locking the 2,500 split.
2. Decide base: plutus-v1 (default) vs ≤14B rebase — orthogonality vs the rest
   of the fleet per Free-Models-First "different lineage" rule.
3. Schedule the closed-window training slot (shares the VRAM constraint with the
   1.3 benchmark and 2.1 vLLM PoC — they cannot run concurrently).
