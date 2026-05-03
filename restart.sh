#!/bin/bash
# OllieTrades Restart Script
# Usage: bash ~/autonomous-trader/restart.sh
# Evicts idle Ollama models to free RAM, then restarts the dashboard.

cd "$(dirname "$0")"
echo "=== OllieTrades Restart ==="

# Evict idle models to free RAM
echo "Evicting idle Ollama models..."
# Legacy model cleanup (retired 2026-04-20, stop if still resident in memory)
_pfx=qwen3.5; _sfx=9b; ollama stop "${_pfx}:${_sfx}" 2>/dev/null || true; unset _pfx _sfx
ollama stop 0xroyce/plutus 2>/dev/null || true
ollama stop llama3.1       2>/dev/null || true
ollama stop mistral:7b     2>/dev/null || true
ollama stop qwen3:14b      2>/dev/null || true
sleep 2

# Kill existing dashboard
echo "Stopping dashboard..."
pkill -9 -f "main\.py"     2>/dev/null || true
lsof -ti :8080 | xargs kill -9 2>/dev/null || true
sleep 3

# Restart
echo "Starting dashboard..."
source venv/bin/activate
nohup python3 main.py > scanner.log 2>&1 &
echo "Dashboard PID: $!"

sleep 4
if curl -sf http://localhost:8080/api/health > /dev/null 2>&1; then
    echo "Dashboard healthy at http://localhost:8080"
else
    echo "Health check pending -- check scanner.log"
fi
echo "=== Restart complete ==="
