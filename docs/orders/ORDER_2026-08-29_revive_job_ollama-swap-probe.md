# ORDER — REVIVED: ollama-swap-probe (job)

**Date:** 2026-08-29
**Action:** revive
**Reason:** New instrumentation (scripts/ollama_model_swap_probe.py) so Monday's live signal cadence gives a definitive verdict on the cross-model-eviction hypothesis instead of a wall-clock guess -- logs every Ollama resident-model load/evict transition to ollama_model_swap_log, joinable against signals.created_at gaps.
