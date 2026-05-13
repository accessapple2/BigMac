# HM-CD-MIGRATE-DAEMON — Ollie Box ollama.service config audit

**Trigger:** HM-CD-migrate per-request keep_alive is being ignored by Ollie Box runtime.
Direct curl with `keep_alive=30m` returns successfully but /api/ps shows
expires_in=+0.0min VRAM=0.00GB. Per-request directive is overridden upstream.

**Hypothesis ranking:**
1. systemd unit sets `OLLAMA_KEEP_ALIVE=0` or short value (most likely)
2. `OLLAMA_MAX_LOADED_MODELS=1` forcing single-model residency
3. Ollama 0.21.0 known bug
4. VRAM pressure forcing immediate eviction

**Phase 1 — Diagnostics on Ollie Box (read-only)**

SSH to Ollie Box and gather, in one paste:

```bash
ssh ollie@192.168.1.166 'bash -s' <<'OLLIE_EOF'
echo "════════════════════════════════════════════════════"
echo "  Ollie Box Ollama daemon audit"
echo "════════════════════════════════════════════════════"

echo ""
echo "── 1. systemd unit Environment block ──"
systemctl cat ollama 2>/dev/null | grep -iE "Environment|ExecStart"

echo ""
echo "── 2. Process actual environment (what the running daemon sees) ──"
OLLAMA_PID=$(pgrep -f "ollama serve" | head -1)
if [ -n "$OLLAMA_PID" ]; then
  echo "  PID: $OLLAMA_PID"
  cat /proc/$OLLAMA_PID/environ 2>/dev/null | tr '\0' '\n' | grep -iE "OLLAMA|CUDA" | sort
else
  echo "  ollama process not found"
fi

echo ""
echo "── 3. Daemon version + system info ──"
ollama --version 2>&1
echo ""
curl -s http://localhost:11434/api/version

echo ""
echo "── 4. GPU + VRAM state ──"
nvidia-smi --query-gpu=name,memory.used,memory.free,memory.total,utilization.gpu --format=csv,noheader

echo ""
echo "── 5. Active service status ──"
systemctl status ollama --no-pager -l 2>&1 | head -20

echo ""
echo "── 6. Recent daemon logs for keep_alive / unload mentions ──"
journalctl -u ollama --no-pager -n 50 2>/dev/null | grep -iE "keep_alive|unload|evict|memory" | tail -20
OLLIE_EOF
```

**Phase 2 — Fix (only after Phase 1 confirms root cause)**

Most likely fix: edit /etc/systemd/system/ollama.service (or wherever the unit lives),
add or update under `[Service]`:
