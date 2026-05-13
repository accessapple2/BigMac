# HM-CD-MIGRATE-GPU-RECOVERY Phase 3 — Recovery (refined)

**Trigger:** Phase 1 + Phase 2 confirmed Ubuntu HWE kernel 6.17.0-23 active,
matching linux-modules-nvidia-580-open-6.17.0-23-generic missing on disk,
GPU offline since 2026-05-12 19:41 reboot. Hardware present (PCI 01:00.0,
device 2d05 = NVIDIA Ada/Blackwell). Driver source in /usr/src/nvidia-580.126.09.

**Captain go/no-go required. Downtime ~5 min for reboot path.**

## Phase 3a — Pre-flight (read-only, run any time)

```bash
ssh bigmac@bigmac 'ssh ollie "bash -s"' <<'PRE_EOF'
echo "════════════════════════════════════════════════════"
echo "  GPU recovery pre-flight"
echo "════════════════════════════════════════════════════"

echo ""
echo "── 1. Running kernel ──"
uname -r
echo "  expected: 6.17.0-23-generic"

echo ""
echo "── 2. Held / broken packages ──"
sudo dpkg --audit 2>&1 | head -20
echo "  Held packages:"
apt-mark showhold 2>/dev/null

echo ""
echo "── 3. Apt update + see what's upgradable (nvidia/kernel focus) ──"
sudo apt update 2>&1 | tail -5
echo ""
sudo apt list --upgradable 2>/dev/null | grep -iE "nvidia|linux-modules|linux-image"

echo ""
echo "── 4. apt install -f DRY RUN ──"
sudo apt install -f --dry-run 2>&1 | grep -iE "would|new|removed|upgraded|nvidia" | head -20

echo ""
echo "── 5. Resolution plan for the target package ──"
KMOD="linux-modules-nvidia-580-open-\$(uname -r)"
echo "  Looking for: \$KMOD"
echo ""
echo "  Repository availability:"
apt-cache policy "\$KMOD" 2>/dev/null | head -10
echo ""
echo "  Install simulation:"
sudo apt install -s "\$KMOD" 2>&1 | grep -iE "Inst|Conf|Remv|broken|error" | head -20

echo ""
echo "── 6. DKMS state (Path 2 fallback readiness) ──"
ls -la /usr/src/nvidia-580* 2>/dev/null | head -3
echo ""
dkms status 2>&1 | head -10
PRE_EOF
```

**Captain reviews. Looking for:**
- Step 2: no unrelated held/broken packages
- Step 3: nvidia kernel module listed as upgradable
- Step 5: install simulation shows clean resolution (no removed-dependencies surprise)
- Step 6: dkms shows nvidia/580.126.09 with built status for the target kernel

If clean → Path 1 (3b). If apt install simulation looks risky → try Path 2 (3b-alt) first.

## Phase 3b — Path 1: APT recovery (recommended, ~5 min downtime)

```bash
ssh bigmac@bigmac 'ssh ollie "bash -s"' <<'P1_EOF'
set -e

echo "════════════════════════════════════════════════════"
echo "  GPU recovery — Path 1 (apt + reboot)"
echo "════════════════════════════════════════════════════"

echo ""
echo "── Step 1: apt update ──"
sudo apt update

echo ""
echo "── Step 2: apt install -f (auto-fix broken deps) ──"
sudo apt install -f -y

echo ""
echo "── Step 3: Install matching nvidia kernel module ──"
sudo apt install -y "linux-modules-nvidia-580-open-\$(uname -r)"

echo ""
echo "── Step 4: Verify module on disk ──"
dpkg -l "linux-modules-nvidia-580-open-\$(uname -r)" 2>&1 | tail -3
echo ""
ls /lib/modules/\$(uname -r)/kernel/drivers/video/ 2>/dev/null | grep nvidia
ls /lib/modules/\$(uname -r)/updates/dkms/ 2>/dev/null | grep nvidia || true

echo ""
echo "── Step 5: Pin nvidia meta-package to prevent future drift ──"
sudo apt-mark hold linux-modules-nvidia-580-open-generic-hwe-24.04 2>&1 || true

echo ""
echo "── Step 6: Trigger reboot (SSH session will drop) ──"
echo "  Rebooting in 10s. Reconnect after ~3 min."
sleep 10
sudo reboot
P1_EOF
```

## Phase 3b-alt — Path 2: DKMS rebuild (no reboot, only if Path 1 dry-run looks unsafe)

```bash
ssh bigmac@bigmac 'ssh ollie "bash -s"' <<'P2_EOF'
set -e

echo "════════════════════════════════════════════════════"
echo "  GPU recovery — Path 2 (DKMS, no reboot)"
echo "════════════════════════════════════════════════════"

echo ""
echo "── Step 1: DKMS autoinstall for the running kernel ──"
sudo dkms autoinstall -k \$(uname -r) 2>&1

echo ""
echo "── Step 2: Verify the module built ──"
dkms status nvidia 2>&1

echo ""
echo "── Step 3: Load it ──"
sudo modprobe nvidia 2>&1 || {
  echo "  modprobe failed — fall back to Path 1 (reboot)"
  exit 1
}

echo ""
echo "── Step 4: Verify nvidia-smi works ──"
nvidia-smi --query-gpu=name,driver_version --format=csv,noheader

echo ""
echo "── Step 5: Restart ollama to re-scan for GPU ──"
sudo systemctl restart ollama
sleep 5

echo ""
echo "── Step 6: Test GPU offload via curl ──"
curl -s --max-time 30 http://localhost:11434/api/generate \
  -d '{"model":"qwen2.5-coder:7b","prompt":"test","stream":false,"keep_alive":"30m"}' \
  | python3 -c "import sys,json; d=json.load(sys.stdin); print('  duration_s:', round(d.get('total_duration',0)/1e9, 2))"

echo ""
echo "── Step 7: Confirm offload in daemon log ──"
journalctl -u ollama --no-pager -n 20 | grep -iE "offload.*GPU" | tail -3

echo ""
echo "  ★ If 'offloaded N/N layers to GPU' is in step 7, Path 2 succeeded."
echo "  ★ If still 0/N, fall through to Path 1 reboot."
P2_EOF
```

## Phase 3c — Post-recovery verification

```bash
# Wait for Ollie if Path 1 was used (skip if Path 2 succeeded)
until ssh -o ConnectTimeout=5 bigmac@bigmac 'ssh -o ConnectTimeout=5 ollie "uptime"' 2>/dev/null; do
  echo "  Ollie not yet responsive, waiting 30s..."
  sleep 30
done

ssh bigmac@bigmac 'ssh ollie "bash -s"' <<'VER_EOF'
echo "════════════════════════════════════════════════════"
echo "  GPU recovery — post-recovery verification"
echo "════════════════════════════════════════════════════"

echo ""
echo "── Uptime ──"
uptime

echo ""
echo "── nvidia module loaded? ──"
lsmod | grep nvidia

echo ""
echo "── nvidia-smi ──"
nvidia-smi --query-gpu=name,memory.used,memory.free,memory.total,driver_version,temperature.gpu --format=csv,noheader

echo ""
echo "── Ollama daemon status ──"
systemctl is-active ollama
curl -s http://localhost:11434/api/version

echo ""
echo "── GPU-offload smoke test ──"
curl -s --max-time 30 http://localhost:11434/api/generate \
  -d '{"model":"qwen2.5-coder:7b","prompt":"hello","stream":false,"keep_alive":"30m"}' \
  | python3 -c "
import sys, json
d = json.load(sys.stdin)
total_s = round(d.get('total_duration',0)/1e9, 2)
load_s  = round(d.get('load_duration',0)/1e9, 2)
print(f'  load_duration: {load_s}s')
print(f'  total_duration: {total_s}s')
print(f'  done_reason: {d.get(\"done_reason\")}')
print(f'  ★ if total_duration < 5s, GPU is doing the work')
print(f'  ★ if total_duration > 30s, still CPU')
"

echo ""
echo "── Ollama log confirms GPU offload ──"
journalctl -u ollama --no-pager -n 30 | grep -iE "offload|cuda|gpu" | tail -5

echo ""
echo "── /api/ps ──"
curl -s http://localhost:11434/api/ps | python3 -m json.tool | grep -E '"name"|"expires_at"|"size_vram"'
VER_EOF
```

## Post-recovery on bigmac

```bash
# Watch HM-CD-instr cycles drop dramatically:
ssh bigmac@bigmac 'tail -f ~/autonomous-trader/logs/trader_error.log | grep --line-buffered HM-CD-instr'
```

Expected: ollama-coder wall drops from 60-207s → 1-5s. neo-matrix similar.
Cycle wall drops from 220-408s → 30-60s for full 7-agent sweep.

## Followups (after GPU back online)

- Re-evaluate CLAUDE.md HM-CD-migrate doctrine — the 30m keep_alive is now actually meaningful again
- HM-AS-β cadence drift warnings should mostly stop (CPU was contributing to handler walls)
- ollama-llama Polygon ReadTimeout occurrences should drop (slow CPU inference was holding the queue)
- File Ubuntu HWE auto-update concern as ops note: pin nvidia meta-package OR disable unattended kernel upgrades on Ollie Box

