#!/usr/bin/env bash
# HM-FORGE Phase 1.1 — Ollama Tier-1 perf baseline capture (BEFORE/AFTER the
# FA + KV-q8_0 patch in scripts/ollama_tier1_perf.sh). Run ON .168 (Ollie Max).
# Identical methodology for both captures so the A/B is apples-to-apples.
#
#   ssh olliemax 'bash /tmp/ollama_tier1_baseline.sh BEFORE'   # pre-sudo (FA off / KV f16)
#   ssh olliemax 'bash /tmp/ollama_tier1_baseline.sh AFTER'    # post-sudo (FA on  / KV q8_0)
#
# Appends a timestamped block to stdout; caller tees it into the report doc.
set -euo pipefail
LABEL="${1:-UNLABELED}"
MODEL="${2:-qwen3:8b}"               # the co-residency workhorse the patch targets
PROMPT='Reply with exactly one short sentence about risk management. No preamble.'

echo "===== TIER1 BASELINE [$LABEL] $(date -u '+%Y-%m-%dT%H:%M:%SZ') model=$MODEL ====="
echo "-- effective ollama env (FA/KV flags) --"
systemctl show ollama --property=Environment 2>/dev/null | tr ' ' '\n' | grep -iE 'FLASH_ATTENTION|KV_CACHE|OLLAMA_HOST' || echo "(no drop-in env)"
echo "-- nvidia-smi (pre-run) --"
nvidia-smi --query-gpu=memory.total,memory.used,memory.free --format=csv,noheader
echo "-- ollama ps (resident) --"
ollama ps || true
echo "-- timed generation (--verbose eval rate) --"
# --verbose prints load/eval timings to stderr; capture both streams.
ollama run --verbose "$MODEL" "$PROMPT" 2>&1 | grep -iE 'eval rate|eval count|load duration|total duration|prompt eval' || true
echo "-- nvidia-smi (post-run, peak-ish) --"
nvidia-smi --query-gpu=memory.used --format=csv,noheader
echo "===== END [$LABEL] ====="
