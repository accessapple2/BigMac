# HM-CD-MIGRATE-GPU-RECOVERY — Ollie Box NVIDIA driver lost

**Trigger:** nvidia-smi reports driver communication failure on Ollie Box (192.168.1.166).
Ollama is running CPU-only — all per-agent walls measured today are CPU inference,
not the assumed GPU-accelerated baseline.

**HM-CD-migrate doctrine remains correct** — qwen2.5-coder:7b is staying resident
30min as designed. The bottleneck has just moved from "model swap" to "CPU inference."

## Phase 1 — Diagnostics ONLY (read-only, no touching anything yet)

```bash
ssh ollie@192.168.1.166 'bash -s' <<'GPUDIAG_EOF'
echo "════════════════════════════════════════════════════"
echo "  Ollie Box NVIDIA recovery — diagnostic snapshot"
echo "════════════════════════════════════════════════════"

echo ""
echo "── 1. Uptime + kernel version ──"
uptime
uname -r
echo "  last boot: \$(who -b)"

echo ""
echo "── 2. NVIDIA driver state ──"
lsmod | grep -iE "nvidia|nouveau" || echo "  no nvidia module loaded"
echo ""
nvidia-smi 2>&1 | head -10
echo ""
echo "  Driver package state:"
dpkg -l | grep -iE "nvidia-driver|nvidia-utils|cuda" | head -10

echo ""
echo "── 3. dmesg NVIDIA-related events (last 200 lines, NVIDIA only) ──"
sudo dmesg --ctime 2>/dev/null | grep -iE "nvidia|nvrm|nouveau|gpu" | tail -30 \
  || dmesg 2>&1 | grep -iE "nvidia|nvrm|nouveau|gpu" | tail -30

echo ""
echo "── 4. Recent kernel events around the failure (look for Xid, OOPS, segfault, MCE) ──"
sudo dmesg --ctime 2>/dev/null | grep -iE "xid|oops|segfault|mce|hardware error|fatal" | tail -20 \
  || dmesg 2>&1 | grep -iE "xid|oops|segfault|mce" | tail -20

echo ""
echo "── 5. journalctl for ollama + nvidia in last 24h ──"
journalctl --since "24 hours ago" --no-pager 2>/dev/null | grep -iE "nvidia|cuda|gpu" | tail -20

echo ""
echo "── 6. Unattended-upgrades log (did a kernel update happen?) ──"
ls -la /var/log/unattended-upgrades/ 2>/dev/null | head -5
tail -20 /var/log/unattended-upgrades/unattended-upgrades.log 2>/dev/null

echo ""
echo "── 7. Apt history for nvidia/kernel installs in last 7 days ──"
grep -iE "nvidia|linux-image|linux-headers" /var/log/apt/history.log 2>/dev/null | tail -20

echo ""
echo "── 8. Last time GPU was working (ollama logs with VRAM mentions) ──"
journalctl -u ollama --no-pager 2>/dev/null | grep -iE "vram|cuda|nvidia|gpu" | tail -10

echo ""
echo "── 9. Thermal sensors (rule out overheat shutdown) ──"
which sensors > /dev/null && sensors 2>/dev/null | head -20 || echo "  lm-sensors not installed"

echo ""
echo "── 10. PCI bus check — is the GPU even visible to the kernel? ──"
lspci | grep -iE "nvidia|vga|3d"
GPUDIAG_EOF
```

## Phase 2 — Decision tree based on Phase 1 output

| Phase 1 finding | Path |
|---|---|
| Kernel updated within 7 days, dmesg shows symbol-version mismatch | **Reboot first** (Option B) — DKMS rebuild often auto-fires on reboot |
| dmesg shows Xid error (hardware fault) or OOM-killer hit nvidia | **Soft recovery first** (Option A) — try `sudo modprobe -r nvidia; sudo modprobe nvidia` |
| GPU not in `lspci` output at all | **Hardware fault** — possibly PCIe seating, power, or chip failure. Physical inspection needed. |
| Sensors show recent thermal spike >85°C | **Thermal event** — let cool, then reboot; consider airflow audit |
| All clean, no errors visible | **Mystery** — soft recovery attempt is low risk |

## Phase 3 — Recovery (only after Captain decision)

Recovery commands TBD based on Phase 1 read. Do not execute until Captain has Phase 1 output.

## What the trader fleet is doing right now

- All ollama agents still functional (CPU inference, just slow)
- ollama-coder ~60s/scan, ollama-llama timing out occasionally  
- HM-AS-β drift detection firing (battle_station_monitor lag because CPU-bound agents block the queue)
- neo-matrix HM-AN2.C consume path working — 41 lines + 20 HALTED gate-blocks today
- Alpha Squad rotation still producing signals, just at degraded latency

**Fleet is OK on CPU but it's not optimal.** Captain decides whether to recover GPU now (some downtime) or wait until end-of-day.

