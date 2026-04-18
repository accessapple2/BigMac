#!/usr/bin/env bash
# uninstall.sh — stop, disable, and unlink all trademinds-*.service units.
#
# Idempotent: safe to re-run after partial uninstalls.
# Source files in ~/autonomous-trader/systemd/ are preserved.
# Requires root (sudo bash uninstall.sh).

set -euo pipefail

UNITS_DST="/etc/systemd/system"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: must run as root. Try: sudo bash $0" >&2
  exit 1
fi

shopt -s nullglob
units=( "$UNITS_DST"/trademinds-*.service )
shopt -u nullglob

if [ ${#units[@]} -eq 0 ]; then
  echo "No trademinds-*.service units in $UNITS_DST — nothing to uninstall."
  exit 0
fi

for unit in "${units[@]}"; do
  name=$(basename "$unit")

  if systemctl is-active --quiet "$name"; then
    systemctl stop "$name"
    echo "stopped:   $name"
  fi

  if systemctl is-enabled --quiet "$name" 2>/dev/null; then
    systemctl disable "$name" 2>/dev/null || true
    echo "disabled:  $name"
  fi

  if [ -L "$unit" ]; then
    unlink "$unit"
    echo "unlinked:  $unit"
  elif [ -e "$unit" ]; then
    echo "WARNING: $unit exists but is NOT a symlink — leaving alone." >&2
  fi
done

systemctl daemon-reload

echo
echo "Uninstall complete. Source units in /home/bigmac/autonomous-trader/systemd/ preserved."
echo "Any systemctl edit drop-ins at /etc/systemd/system/trademinds-*.service.d/ must be removed manually."
