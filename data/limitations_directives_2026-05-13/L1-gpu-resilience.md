# L1 — GPU Recovery Resilience

**Risk:** Ubuntu HWE auto-updates kernel again → GPU goes dark for 36+ hours
until someone notices.

**Probability:** HIGH. HWE updates run automatically. This already happened
2026-05-12 → 2026-05-13. Will happen again.

## Fix (15 minutes, low risk)

Hop on Ollie Box (`ssh ollie` from bigmac) and run:

```bash
sudo apt-mark hold linux-modules-nvidia-580-open-generic-hwe-24.04
sudo apt-mark hold linux-image-generic-hwe-24.04
sudo apt-mark hold linux-headers-generic-hwe-24.04
apt-mark showhold
```

Expected output: shows all 3 packages pinned. Future `apt upgrade` runs will
skip them with a "held back" warning. You manually update them when you
choose to, after backing up state.

## Defense-in-depth: GPU-state ntfy alert

Beyond the pin, add a daily 06:30 AZ cron on Ollie Box that:
1. Runs `nvidia-smi` and parses output
2. If failure or VRAM=0, fires ntfy alert to phone
3. Quick early-warning if any future driver/kernel issue recurs

### Implementation (next session)

Create `scripts/ollie_gpu_health.sh` on Ollie Box with curl-to-ntfy on failure.
Add launchd-equivalent systemd timer on Ollie Box.

Total time: 30 min including testing.

## Captain decision

Pin now (manual SSH step) or wait for next-session shipped automation?
**Recommend pin now** — eliminates entire failure mode in 3 commands.
