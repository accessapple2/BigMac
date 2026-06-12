# Plutus v6 — training, GGUF export & bake-off tooling

Run-copies of the scripts used to train, package, and evaluate Plutus v6 on the
`.168` (olliemax / RTX 5080) box, 2026-06-11/12. Paths inside are absolute
`/home/bigmac/...` because they ran there; treat these as a faithful record of
the run, not as portable CLI tools.

## Files
| File | Purpose |
|------|---------|
| `train_plutus_v6.py` | Full v6 LoRA train (rank 8, 3 ep) on the 2,606-row corpus → registered `plutus-v6`. |
| `train_plutus_v6_eval.py` | **Eval variant** — trains on the 1,415-row TRAIN split only (val+test held out) → `plutus-v6-eval`, so the 178-row test set is uncontaminated. |
| `export_plutus_v6.py` / `_eval.py` | unsloth `save_pretrained_gguf` (Q4_K_M). |
| `bakeoff_gen.py` | Generate v1 vs v6-eval critiques for the 178 held-out prompts (grouped by model to avoid VRAM swap). |
| `bakeoff_judge.py` | Blind, order-balanced pairwise LLM-judge. Env: `JUDGE_MODEL`, `OUT`. |
| `Modelfile.v6` / `Modelfile.v6-eval` | ollama Modelfiles; SYSTEM block cloned from v1 (Plutus identity excluded from corpus by design). |

## GGUF gotcha (important)
unsloth's `save_pretrained_gguf` quantize step shells out to `python` (bare name),
which fails on this box (`/bin/sh: python: not found` — only `python3` exists).
The reliable path is **manual**:
```
PYTHONPATH=~/llama.cpp/gguf-py ~/plutus-train-pinned/bin/python \
  ~/llama.cpp/convert_hf_to_gguf.py <merged-hf-dir> --outfile bf16.gguf --outtype bf16
~/llama.cpp/build/bin/llama-quantize bf16.gguf out.Q4_K_M.gguf Q4_K_M
```

## Bake-off result (2026-06-12)
Two independent judges on the clean 178-row held-out test set:
- **qwen3:14b** — v1 98 / v6-eval 80 (overall statistical tie); v6-eval wins debate_critique 31–15.
- **gpt-oss:20b** — v1 90 / v6-eval 55 (29 unparseable); does NOT replicate the debate edge.
- **Robust across both:** v6-eval **regresses on core trade_critique** (v1 ~65%). v1 wins.

Outcome: **no promotion**; v1 stays on all live seats; OOS-gate queued in accumulate
mode. Corpus + split data are intentionally untracked (regenerable via
`scripts/assemble_plutus_v6_corpus.py`).
