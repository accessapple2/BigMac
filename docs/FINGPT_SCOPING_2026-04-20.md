# FinGPT LoRA Scoping — 2026-04-20

**Status:** Investigation only. No code changes, no downloads, no training started.
**Decision gate:** Admiral decides scope before any implementation begins.

---

## Hardware Baseline

| Resource | Current | After 96GB upgrade |
|----------|---------|-------------------|
| RAM | 32 GB | 96 GB |
| GPU VRAM | RTX 5060 8 GB | RTX 5060 8 GB (unchanged) |
| Disk | NVMe (fast) | Same |

**Key finding:** 96 GB RAM is a comfort/speed upgrade for 7B LoRA training. It does **not** unlock larger models — GPU VRAM remains the hard ceiling at 8 GB. The only thing that unlocks 13B+ training is a GPU upgrade (RTX 5090 24 GB or larger).

---

## Ollie Box Model Inventory (as of 2026-04-20)

| Model | Disk | Finance relevance |
|-------|------|-------------------|
| qwen3-coder:30b | 18.6 GB | Code tasks |
| qwen3:14b | 9.3 GB | General reasoning |
| deepseek-r1:14b | 9.0 GB | Chain-of-thought |
| 0xroyce/plutus:latest | **5.7 GB** | **Finance-tuned — McCoy's brain** |
| qwen3:8b | 5.2 GB | General |
| llama3.1:latest | 4.9 GB | General / Llama family base candidate |
| qwen2.5-coder:7b | 4.7 GB | Code tasks |
| phi3:mini | 2.2 GB | Lightweight |
| llama3.2:3b | 2.0 GB | Lightweight |
| **Total** | **61.6 GB** | |

Finance-tuned models: **only Plutus**. Everything else is general-purpose.

---

## OllieTrades Training Data Available

`scripts/export_training_data.py` — fully implemented, already handles incremental export.

| Dataset | Rows | Quality |
|---------|------|---------|
| Closed trades (thesis + P&L + strategies) | **653** | High — structured, labeled |
| Convergence signals (with price outcomes) | 5,777 | Medium — some lack 24/48h prices |
| War room debates (consensus vs outcome) | 9,330 | Medium — needs instruction reformatting |
| Total signals | 51,963 | Low — raw, unstructured |

**Data gap for Scope B:** Need ~2,000–3,000 closed trades minimum for useful task-specific LoRA. At current fleet pace: **~3 months away** (assuming ~150–200 new closed trades/month).

---

## FinGPT Landscape

**What it is:** Open-source financial LLM framework (AI4Finance-Foundation, GitHub/HuggingFace). Two components:
1. Pre-trained LoRA adapters — inference-only, no training required
2. QLoRA training pipeline — fine-tune on custom financial data

**Available HuggingFace adapters today:**

| Adapter | Base model | Task | Relevance |
|---------|-----------|------|-----------|
| `FinGPT/fingpt-sentiment_llama2-7b_lora` | Llama 2 7B | Sentiment classification | High — enhances Troi |
| `FinGPT/fingpt-trader_dow30_llama2-7b_lora` | Llama 2 7B | Buy/sell/hold decisions | High — standalone advisor |
| `FinGPT/fingpt-forecaster_dow30_llama2-7b_lora` | Llama 2 7B | Price direction | Medium |
| `FinGPT/fingpt-ra-llama2-13b_lora` | Llama 2 **13B** | Robo-advisor | ❌ VRAM OOM |

**Training datasets used by FinGPT team:**
- Financial Phrasebank (50K sentences, sentiment)
- SEC 8-K / 10-Q filings (public domain)
- FiQA (17K financial Q&A examples)
- Bloomberg news (requires license)
- FinGPT curated instruction set (~100K for full instruct-tuning)

---

## VRAM Requirements by Model Size (QLoRA 4-bit)

| Model size | VRAM needed | RTX 5060 8GB | 32GB RAM | 96GB RAM |
|-----------|------------|-------------|---------|---------|
| 7B | 6–8 GB | ✅ tight but OK | ✅ | ✅ faster |
| 13B | 10–14 GB | ❌ OOM | ⚠️ borderline | ✅ |
| 34B | 24+ GB | ❌ | ❌ | ❌ (VRAM gate) |
| 70B | 48+ GB | ❌ | ❌ | ❌ |

**Forced batch size at 7B on 8GB VRAM:** 1–2. Training is slow but works. Gradient checkpointing required.

---

## Three Scopes

### Scope A — Inference with Pre-trained Adapters (1–2 hours)

Use existing FinGPT HuggingFace adapters. Zero training.

**What's needed:**
- `pip install transformers peft accelerate bitsandbytes` (Ollie, separate from Ollama)
- Download `fingpt-trader_dow30_llama2-7b_lora` + Llama 2 7B base
- ~50 LOC inference wrapper → outputs sentiment/verdict JSON
- Wire into fleet as advisory augmentation (pre-scan step for Troi or standalone)

**Blocker:** Llama 2 is a gated HuggingFace model — requires Meta approval (~24 hr, free). Submit request before proceeding. Alternatively, explore if any adapters are compatible with Llama 3.1 8B (already on Ollie).

**Expected output:** Finance-specialist sentiment on each watchlist ticker pre-scan. Not OllieTrades-specific (trained on Dow 30 / public finance data), but validated on real financial benchmarks.

**Recommended fleet role:** Augment Troi's sentiment pass. FinGPT outputs raw sentiment + confidence per ticker before Troi's Ollama call; Troi incorporates it as pre-context. No new voter seat; reuses existing advisory framework.

### Scope B — Custom LoRA on OllieTrades Data (days of work, 3–6 months of data collection first)

Fine-tune 7B model on `trader.db` exported data.

**Prerequisites:**
- ~2,000–3,000 closed trades minimum (currently 653 — ~3 months away)
- Instruction-format conversion of `trades.jsonl` (e.g., "Given [context], was this a good buy? Answer: [win/loss + reasoning]")
- QLoRA 4-bit training on Ollie RTX 5060 (~2–8 hours for 7B on 2K examples)

**Expected output:** Model that understands OllieTrades-specific patterns — convergence counts, regime-aware win rates, fleet-specific thesis language. Becomes a true "institutional memory" advisor.

**RAM note:** 96GB RAM makes this faster (larger batch size, no swap) but doesn't gate it. Can start at 32GB, accept slower training.

### Scope C — Full FinGPT Instruct Tuning (research project, post 96GB)

Train from scratch on full FinGPT corpus (100K+ examples) + OllieTrades overlay.

- 7B model, full LoRA, multi-day run on Ollie
- Or cloud GPU rental (A100 40GB, ~$1–2/hr on Lambda Labs)
- Not worth pursuing until fleet has 6+ months of outcome-labeled data
- 96GB RAM helps but VRAM is still the ceiling — consider GPU upgrade concurrently

---

## Decision Matrix

| Question | Answer |
|----------|--------|
| Should Admiral wait for 96GB? | **No.** Scope A and B both work at 32GB. 96GB is speed, not capability. |
| Smartest first step TODAY | Scope A: request Llama 2 HF access → download `fingpt-trader_dow30` adapter → write inference wrapper |
| Time commitment for Scope A | 1–2 hours once HF access granted |
| Training data gate for Scope B | ~2K–3K closed trades (~3 months at current pace) |
| When does 96GB matter for FinGPT? | Scope B training runs 2× faster, no swap risk. Not a gate. |
| Real VRAM gate | 8GB caps training at 7B models. GPU upgrade needed for 13B+. |
| Scope B data prep already done? | Yes — `export_training_data.py` is production-ready. Needs instruction-format post-processing step only. |

---

## Immediate Action Items (if Admiral approves Scope A)

1. **Request Llama 2 HF access:** https://huggingface.co/meta-llama/Llama-2-7b-hf (~24 hr turnaround)
2. **Verify Llama 3.1 adapter compatibility:** Check if `fingpt-trader` adapters load on `meta-llama/Meta-Llama-3.1-8B` (Llama 3.1 8B already on Ollie, avoids gated model wait)
3. **Write inference wrapper:** `engine/fingpt_advisor.py` — ~50 LOC, PEFT-based, outputs `{ticker, sentiment, confidence, reasoning}` JSON
4. **Wire into Troi pre-context:** Pass FinGPT sentiment as additional context line to `wb_advisory_team.py` Troi call

## Immediate Action Items (if Admiral approves Scope B tracking)

1. Run `python3 scripts/export_training_data.py --stats` monthly to track data accumulation
2. At 2K closed trades: convert `trades.jsonl` to instruction format, run first training experiment
3. Plan: `scripts/prepare_lora_training.py` — converts JSONL to HuggingFace `datasets` format with instruction template

---

## References

- FinGPT paper: "FinGPT: Open-Source Financial Large Language Models" (Yang et al., 2023)
- HuggingFace org: https://huggingface.co/FinGPT
- AI4Finance-Foundation GitHub: https://github.com/AI4Finance-Foundation/FinGPT
- PEFT library (LoRA): https://github.com/huggingface/peft
- QLoRA paper: "QLoRA: Efficient Finetuning of Quantized LLMs" (Dettmers et al., 2023)
