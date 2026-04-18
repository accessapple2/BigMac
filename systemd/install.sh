#!/usr/bin/env bash
# install.sh — symlink trademinds-*.service files from this directory
# into /etc/systemd/system/, then reload systemd.
#
# Idempotent: safe to re-run after adding or editing unit files.
# Requires root (sudo bash install.sh).

set -euo pipefail

UNITS_SRC="/home/bigmac/autonomous-trader/systemd"
UNITS_DST="/etc/systemd/system"

if [ "$(id -u)" -ne 0 ]; then
  echo "ERROR: must run as root. Try: sudo bash $0" >&2
  exit 1
fi

if [ ! -d "$UNITS_SRC" ]; then
  echo "ERROR: source directory $UNITS_SRC does not exist." >&2
  exit 1
fi

shopt -s nullglob
units=( "$UNITS_SRC"/trademinds-*.service )
shopt -u nullglob

if [ ${#units[@]} -eq 0 ]; then
  echo "No trademinds-*.service files in $UNITS_SRC — nothing to link."
  echo "(Create unit files in that directory, then re-run this script.)"
  systemctl daemon-reload
  exit 0
fi

linked=0
unchanged=0
for src in "${units[@]}"; do
  name=$(basename "$src")
  dst="$UNITS_DST/$name"

  # Already a correct symlink? Skip.
  if [ -L "$dst" ] && [ "$(readlink -f "$dst")" = "$(readlink -f "$src")" ]; then
    echo "unchanged: $dst"
    unchanged=$((unchanged + 1))
    continue
  fi

  # Destination exists but isn't our symlink? Bail — operator must investigate.
  if [ -e "$dst" ] && [ ! -L "$dst" ]; then
    echo "ERROR: $dst exists and is NOT a symlink." >&2
    echo "       Remove it manually and re-run, or investigate first." >&2
    exit 1
  fi

  ln -sf "$src" "$dst"
  echo "linked:    $dst -> $src"
  linked=$((linked + 1))
done

systemctl daemon-reload

echo
echo "Summary: $linked newly linked, $unchanged unchanged."
echo "Next:    sudo systemctl enable --now <unit>"
echo "         e.g.  sudo systemctl enable --now trademinds-dashboard.service"
