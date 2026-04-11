#!/usr/bin/env bash
# Restart USS TradeMinds — always uses venv/bin/python3, never .venv
set -e
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PYTHON="$SCRIPT_DIR/venv/bin/python3"

if [ ! -x "$PYTHON" ]; then
  echo "ERROR: $PYTHON not found or not executable" >&2
  exit 1
fi

echo "Stopping existing processes..."
pkill -9 -f "main\.py" 2>/dev/null || true
lsof -ti :8080 | xargs kill -9 2>/dev/null || true
sleep 3

# Trim Ollama to plutus + mistral:7b only — evict everything else from GPU/RAM
if command -v ollama &>/dev/null && curl -s http://localhost:11434/api/tags >/dev/null 2>&1; then
  echo "Trimming Ollama to plutus + mistral:7b..."
  ollama list 2>/dev/null | tail -n +2 | awk '{print $1}' | while read -r model; do
    if ! echo "$model" | grep -qiE "plutus|mistral"; then
      curl -s -X POST http://localhost:11434/api/generate \
        -d "{\"model\":\"$model\",\"keep_alive\":0}" \
        --max-time 5 >/dev/null 2>&1 || true
      echo "  Evicted: $model"
    fi
  done
fi

echo "Starting USS TradeMinds with $PYTHON..."
cd "$SCRIPT_DIR"
nohup "$PYTHON" main.py >> scanner.log 2>&1 &
echo "PID: $! — tailing scanner.log (Ctrl-C to stop tailing)"
sleep 2
tail -f scanner.log
