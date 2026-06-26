#!/bin/bash
# Install (or reinstall) com.trademinds.swingdesk as a LaunchDaemon.
# Must run as root: sudo bash scripts/swingdesk_daemon_install.sh
set -x   # trace every command so failures are visible

PLIST_SRC="/Users/bigmac/autonomous-trader/com.trademinds.swingdesk.plist"
PLIST_DST="/Library/LaunchDaemons/com.trademinds.swingdesk.plist"
LABEL="com.trademinds.swingdesk"

echo "[swingdesk-install] stopping any running instance..."
launchctl bootout system "$LABEL" 2>/dev/null || true
pkill -f "swingdesk/backend.py" 2>/dev/null || true
sleep 1

echo "[swingdesk-install] installing plist..."
cp "$PLIST_SRC" "$PLIST_DST"
chown root:wheel "$PLIST_DST"
chmod 644 "$PLIST_DST"

echo "[swingdesk-install] bootstrapping..."
launchctl bootstrap system "$PLIST_DST"
launchctl enable "system/$LABEL"

echo "[swingdesk-install] starting..."
launchctl kickstart -k "system/$LABEL"
sleep 4

echo "[swingdesk-install] status:"
launchctl print "system/$LABEL" | grep -E "state|pid|program"

echo "[swingdesk-install] port check:"
curl -s localhost:8889/api/tradeable/AAPL && echo ""
