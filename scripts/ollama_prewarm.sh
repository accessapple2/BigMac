#!/bin/zsh
# HM-OLLAMA-PREWARM 2026-06-04 · TRIP-READY fix 2026-06-12
# Pre-warms the heavy shared model to prevent the ~06:51 AZ bridge-wedge pattern
# where a cold-start ~5.5GB model load blocks the bridge worker.
#
# TRIP-READY corrections (was never armed + targeted a nonexistent model/host):
#   * model  llama3.1:latest -> qwen3:8b   (the ~5.5-6GB model 7 agents share;
#            llama3.1 is installed on NEITHER bigmac nor Ollie Max)
#   * host   localhost:11434 -> 192.168.1.168:11434  (Ollie Max = heavy-inference
#            host per config.py OLLAMA_URL; bigmac-local holds only small residents)
# NTFYs ollietrades-admin on FAILURE so a silent cold-start wedge can't recur unseen.

LOG="/Users/bigmac/autonomous-trader/logs/ollama_warmup.log"
TS=$(date '+%Y-%m-%d %H:%M:%S')
MODEL="qwen3:8b"
HOST="http://192.168.1.168:11434"

response=$(curl -s --max-time 90 -X POST "$HOST/api/generate" \
  -H "Content-Type: application/json" \
  -d "{\"model\":\"$MODEL\",\"prompt\":\"ready\",\"stream\":false,\"keep_alive\":\"2h\"}")

if echo "$response" | grep -q '"done":true'; then
  echo "$TS  warmup OK ($MODEL @ $HOST)" >> "$LOG"
else
  echo "$TS  warmup FAILED ($MODEL @ $HOST): $response" >> "$LOG"
  curl -s -m 10 -H "Title: 🔴 Ollama prewarm FAILED" \
    -d "$TS qwen3:8b @ .168 did not warm — morning bridge-wedge risk: $response" \
    https://ntfy.sh/ollietrades-admin >/dev/null 2>&1
fi
