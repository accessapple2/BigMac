#!/bin/bash
# Run during market hours tomorrow to identify what's actually eating bigmac's RAM
echo "=== bigmac memory diagnostic $(date) ==="
echo ""
echo "--- Ollama models currently loaded ---"
ollama ps
echo ""
echo "--- Top 15 processes by RSS memory ---"
ps aux | sort -nrk 4 | head -15 | awk '{printf "%-10s %5s%% %6sMB %s\n", $1, $4, int($6/1024), $11}'
echo ""
echo "--- Memory summary ---"
top -l 1 -n 5 -o mem | head -15
echo ""
echo "--- Swap activity ---"
vm_stat | head -10
