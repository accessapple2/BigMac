#!/usr/bin/env bash
# HM-OLLAMA-TIER1-PERF (2026-06-06) — enable Flash Attention + KV-cache q8_0 on
# Ollie Max (.168, RTX 5080 Blackwell). Low-risk, reversible. Halves KV-cache VRAM
# (vs f16) to fix the qwen3.5:9b 3-model co-residency squeeze. Server-level env vars
# (read at `ollama serve` startup) → require a systemd drop-in + restart (sudo).
#
# RUN ON .168 WITH SUDO:
#   scp scripts/ollama_tier1_perf.sh bigmac@192.168.1.168:/tmp/ \
#     && ssh -t bigmac@192.168.1.168 'sudo bash /tmp/ollama_tier1_perf.sh'
# REVERT (back to exactly where we were):
#   ssh -t bigmac@192.168.1.168 'sudo bash /tmp/ollama_tier1_perf.sh --revert'
set -euo pipefail

DROPIN=/etc/systemd/system/ollama.service.d/override.conf

if [[ "${1:-}" == "--revert" ]]; then
  rm -f "$DROPIN"
  systemctl daemon-reload
  systemctl restart ollama
  echo "REVERTED — Flash Attention + KV-q8 removed, ollama restarted. Back to f16/FA-off."
  exit 0
fi

mkdir -p "$(dirname "$DROPIN")"
cat > "$DROPIN" <<'EOF'
[Service]
Environment="OLLAMA_FLASH_ATTENTION=1"
Environment="OLLAMA_KV_CACHE_TYPE=q8_0"
EOF
systemctl daemon-reload
systemctl restart ollama
sleep 2
echo "APPLIED — Flash Attention + KV-cache q8_0 enabled, ollama restarted."
echo "Effective env:"
systemctl show ollama --property=Environment | tr ' ' '\n' | grep -i OLLAMA_ || true
