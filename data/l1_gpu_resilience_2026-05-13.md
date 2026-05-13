# L1 — GPU Resilience deferred 2026-05-13

Overnight chain attempted automated apt-mark hold on Ollie Box.

## Status: deferred to manual paste

Sudo on Ollie Box requires interactive password (confirmed earlier today).
Non-interactive sudo not configured. Manual command required:

```
ssh ollie
sudo apt-mark hold linux-modules-nvidia-580-open-generic-hwe-24.04 linux-image-generic-hwe-24.04 linux-headers-generic-hwe-24.04
apt-mark showhold  # verify
```

After running: future apt upgrades will skip these packages with a "held back"
warning. Captain manually updates them when ready, after backing up state.

## Why this matters

Today's GPU recovery (commit 4486222) was triggered by Ubuntu HWE auto-update
installing kernel 6.17.0-23 without the matching nvidia-modules. ~12 hours of
degraded fleet performance until discovered. Pin eliminates this failure mode.

## Effort

3 lines of bash, 30 seconds. High value, near-zero risk.

## RESOLUTION 2026-05-13 (post-overnight)

L1 manually completed via Captain SSH session. Three packages confirmed
on hold via `sudo apt-mark hold` on Ollie Box. Verified with
`apt-mark showhold`. Failure mode permanently eliminated unless explicitly
unheld for a future kernel upgrade.
