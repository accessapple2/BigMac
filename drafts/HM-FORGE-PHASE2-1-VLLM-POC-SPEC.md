# HM-FORGE Phase 2.1 — vLLM PoC Spec (SPEC ONLY, no execution)

**Goal:** serve the 1.3 bake-off winner on the existing RTX 5080 (16 GB) via
vLLM with **JSON-schema constrained decoding**, and prove it beats the current
OllamaQueue on War-Room fan-out. Execution is a **separate epic** — this spec
defines scope, success criteria, and rollback only.

## Why
- War Room fans out ~9 providers; memory `[WR-PROVIDER-DUR]` shows ~19m35s with
  VRAM thrashing, and the 90 s timeout **discards** completed-but-late Ollama
  inferences (`project_hm_wr_ollama_queue_starvation`). vLLM continuous batching
  + paged KV directly target this.
- vLLM grammar/JSON-schema guided decoding gives **reliable structured output**
  — sidesteps the qwen3 `"think":False` fragility and the JSON-parse bug class.

## Constraints (from Phase 0)
- **16 GB VRAM is the hard ceiling.** vLLM must serve **ONE** model + KV cache in
  the headroom left by the live fleet, OR replace co-located Ollama entirely for
  that model. A dense 8B at FP16 (~16 GB) **will not fit** alongside KV — the PoC
  model must be **quantized** (AWQ/GPTQ/FP8) or MoE (gpt-oss:20b, ~13 GB MXFP4).
- vLLM and Ollama **cannot both hold large models resident** at once. PoC runs on
  a **different port** (e.g. 8001) and a **closed window**, not against the live
  fleet's resident set.

## Candidate model
Winner of 1.3 (`plutus-v1` vs `gpt-oss:20b`[ vs `gemma4:12b-it-qat`]). If
`gpt-oss:20b` wins, it's the natural vLLM PoC target (MoE fits 16 GB, tool-call +
JSON support). If a dense model wins, require an AWQ/FP8 build first.

## Success criteria (vs current OllamaQueue)
| Metric | Baseline (Ollama) | PoC target |
|---|---|---|
| War-Room fan-out wall-clock | ~19m35s (`[WR-PROVIDER-DUR]`) | ≥3× faster on the same prompt set |
| 90 s-timeout discards | non-zero (starvation) | **zero** discards |
| JSON-validity on structured turns | (measure in 1.3) | ≥ Ollama, ideally 100% via guided decoding |
| Concurrency | FIFO, serialized | ≥4 concurrent without VRAM thrash |

## PoC shape
1. Install vLLM in an isolated venv on .168 (NOT the live `.venv`; mirror the
   vectorbt-isolation doctrine — never import into serving code).
2. Serve the PoC model on `:8001` with `--guided-decoding-backend` + a JSON
   schema matching the War-Room verdict (`{verdict, conviction, reason}`).
3. Replay the 1.3 frozen prompt set at concurrency {1,4,8}; record tok/s,
   wall-clock, discards, JSON-validity.
4. Compare to an Ollama run of the same set/concurrency.

## Rollback plan
- vLLM runs as a **separate** process/port; the live fleet keeps using Ollama at
  `:11434` untouched.
- Abort = `systemctl stop`/kill the vLLM unit; nothing in the trader points at
  `:8001` during the PoC. No fleet base-URL change until a **separate** cutover
  epic with its own Admiral go.
- Zero changes to `config.py` / agent routing in the PoC phase.

## Out of scope (separate epics)
Full fleet cutover, multi-model serving, speculative decoding (DFlash) — only
after the PoC clears the success criteria.
