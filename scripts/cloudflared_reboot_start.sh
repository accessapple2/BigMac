#!/bin/zsh
# cloudflared_reboot_start.sh — @reboot wrapper for the Cloudflare
# tunnel that fronts bridge.ollietrades.com → localhost:8080. Sibling
# of scripts/trader_reboot_start.sh and scripts/signal_center_reboot_start.sh.
# Same rationale (gui/$UID LaunchAgent domain doesn't bootstrap from
# SSH on this macOS box; RunAtLoad doesn't fire at boot without a
# logged-in Aqua session — see CLAUDE.md "LaunchAgent Reboot Lifecycle"
# 2026-05-23).
#
# Tradeoff vs LaunchDaemon: no KeepAlive respawn. Acceptable because
# cloudflared maintains its own multi-edge connection retry logic
# (re-registers tunnel connections automatically on transient drops).
# A full process crash would require a manual restart, but that's the
# same posture as the trader and signal-center.

set -u

ROOT_DIR="/Users/bigmac/autonomous-trader"
CONFIG="/Users/bigmac/.cloudflared/config.yml"
CLOUDFLARED="/opt/homebrew/bin/cloudflared"
LOG_DIR="$ROOT_DIR/logs"
LOG="$LOG_DIR/cloudflared-trademinds.log"
REBOOT_LOG="$LOG_DIR/cloudflared_reboot_start.log"

mkdir -p "$LOG_DIR"

echo "[$(date '+%Y-%m-%d %H:%M:%S')] @reboot fired — polling for DNS readiness (cap 900s)" >> "$REBOOT_LOG"

# HM-DNS-RACE (2026-06-14): the fixed `sleep 30` lost a boot-time DNS race —
# at boot+37s the loopback resolver wasn't up, cloudflared's edge discovery
# did `lookup _v2-origintunneld._tcp.argotunnel.com on [::1]:53` → connection
# refused → FATAL exit, and the tunnel never came up until a manual restart.
# Allo (gateway) can take many minutes to restore upstream resolution after a
# power event, so poll until DNS actually answers rather than guessing a delay.
# Cap ~900s (15 min) = 180 tries × 5s, to outlast a slow Allo recovery.
DNS_READY=0
for i in $(seq 1 180); do
  if [[ -n "$(dig +short cloudflare.com A 2>/dev/null)" ]]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] DNS ready after $(( (i - 1) * 5 ))s" >> "$REBOOT_LOG"
    DNS_READY=1
    break
  fi
  # Heartbeat every ~30s (every 6th try) so a slow boot is auditable.
  if (( (i - 1) % 6 == 0 )); then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] waiting for DNS... ($(( (i - 1) * 5 ))s)" >> "$REBOOT_LOG"
  fi
  sleep 5
done

if [[ "$DNS_READY" -ne 1 ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARN: DNS still unresolved after 900s cap — attempting cloudflared start anyway" >> "$REBOOT_LOG"
fi

# Guard: don't double-fire if an operator beat the wrapper to it.
# HM-CLOUDFLARED-DUP-GUARD (2026-05-29): match the BINARY (pgrep -x), not the
# arg string. The prior guard `pgrep -f "cloudflared tunnel"` missed an instance
# started as `cloudflared --config <cfg> tunnel run` (the --config breaks the
# contiguous "cloudflared tunnel" substring) → a duplicate connector started on
# the same tunnel, causing intermittent 502s (edge load-balanced onto a flapping
# connector). `-x cloudflared` matches any invocation order; the zsh wrapper
# itself is "zsh", not "cloudflared", so it won't self-match.
if pgrep -x cloudflared >/dev/null 2>&1; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] cloudflared already running, skipping start" >> "$REBOOT_LOG"
  exit 0
fi

if [[ ! -x "$CLOUDFLARED" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $CLOUDFLARED not executable" >> "$REBOOT_LOG"
  exit 1
fi

if [[ ! -r "$CONFIG" ]]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: $CONFIG not readable" >> "$REBOOT_LOG"
  exit 1
fi

echo "[$(date '+%Y-%m-%d %H:%M:%S')] starting cloudflared: $CLOUDFLARED tunnel --config $CONFIG run" >> "$REBOOT_LOG"

nohup "$CLOUDFLARED" tunnel --config "$CONFIG" run >> "$LOG" 2>> "$LOG" &!

sleep 4
if pgrep -x cloudflared >/dev/null 2>&1; then
  PID=$(pgrep -x cloudflared | head -1)
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] cloudflared started, pid=$PID" >> "$REBOOT_LOG"
  exit 0
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] FATAL: cloudflared failed to start (see $LOG)" >> "$REBOOT_LOG"
  exit 1
fi
