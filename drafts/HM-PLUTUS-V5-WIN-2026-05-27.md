# Plutus v5 Successful Retrain — 2026-05-27

## Outcome
Plutus v1 fine-tune working in production. Dr. McCoy now runs the trained model
instead of stock 0xroyce/plutus.

## Root cause of earlier failures
- Original venv on Python 3.14 + transformers 5.5.0 + torch 2.12 broke the
  Unsloth → LoRA → GGUF pipeline for Qwen2.5
- RTX 5080 (sm_120) requires torch ≥2.8 + CUDA 12.8
- xformers 0.0.32 bundled flash-attn has no sm_120 kernels → CUDA crash on
  first training step

## Working pinned environment
- Python 3.12 (deadsnakes PPA on Ubuntu 26.04)
- torch 2.8.0 + CUDA 12.8 (sm_120 capable)
- transformers 4.51.3
- unsloth 2025.4.7 + unsloth_zoo 2025.4.4
- peft 0.15.2, trl 0.15.2
- bitsandbytes 0.46.0, accelerate 1.6.0
- xformers: **UNINSTALLED** (falls back to native SDPA; ~30% slower but works)

## Training stats
- Corpus: plutus_corpus_v4.jsonl (1,199 examples, AUTO-STOP tags stripped)
- 3 epochs, 225 steps total
- LoRA rank 16, 0.58% params trained
- LoRA wall: ~8 min on RTX 5080
- GGUF Q4_K_M wall: ~12 min
- Final size: 4.46 GB (4.91 BPW)

## Modelfile
Used full Qwen2.5 tool-calling chat template + custom SYSTEM prompt
("You are Plutus, financial intelligence officer for OllieTrades").
Temp 0.3, repeat_penalty 1.1, num_ctx 2048.
**CRITICAL**: Unsloth regenerates a stock Qwen Modelfile on export; we
overwrite it after every train.

## Smoke test results
"Who are you?" → "I am Plutus, your financial intelligence officer at
OllieTrades. My role is to analyze trades, evaluate trading signals..."
AVGO trade critique → coherent, on-domain
UPRO risk/reward → math correct (1:1.85 ratio), on-domain
