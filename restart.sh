#!/bin/bash
# OllieTrades Restart — canonical (HM-RESTART-SH-FIX 2026-05-28)
#
# Uses .venv (the live runtime — the trader runs under
# /opt/homebrew/.../python@3.14 via ./.venv) and the correct log sinks
# (logs/trader.log for rich console output, logs/trader_error.log for stdlib
# logging + NTFY) per the Logging Sink Split doctrine in CLAUDE.md.
#
# Prior version was stale: it sourced the OLD venv/ (missing parity) and
# redirected everything to scanner.log, which broke log-based verification
# (e.g. the [OLLAMA-QUEUE-SWAP] / NTFY checks read logs/trader_error.log).
#
# Ollama lives on the Ollie Box (off bigmac) since the MSI migration, so there
# is no local model eviction step anymore.
cd "$(dirname "$0")"
echo "=== OllieTrades Restart ==="

# Stop the existing trader. Kill by listening port + by name; this script's own
# command line does not contain 'main.py', so pkill -f won't self-match.
echo "Stopping trader..."
lsof -ti :8080 2>/dev/null | xargs kill 2>/dev/null || true
pkill -f "main\.py" 2>/dev/null || true
sleep 3

echo "Starting trader (.venv)..."
nohup ./.venv/bin/python3 main.py >> logs/trader.log 2>> logs/trader_error.log &
echo "Trader PID: $!"

sleep 5
if curl -sf http://localhost:8080/healthz > /dev/null 2>&1; then
    echo "Trader healthy at http://localhost:8080"
else
    echo "Health check pending — see logs/trader_error.log"
fi
echo "=== Restart complete ==="
