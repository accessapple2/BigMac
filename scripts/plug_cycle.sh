#!/bin/zsh
# scripts/plug_cycle.sh — HM-SHELLY-PREP-V2 (2026-07-01) manual plug control.
#
# zsh, not bash: macOS ships only bash 3.2 (no associative-array support,
# `declare -A` silently breaks under `set -u` — "unbound variable" on any
# lookup). zsh 5.9 is the modern default shell on this box and supports the
# same `declare -A name=([key]=val ...)` syntax correctly. Same convention
# scripts/trader_restart.sh already established for bash-3.2 limitations.
#
# Usage: plug_cycle.sh {bigmac|olliemax|allo} {status|off|on|cycle} [--confirm]
#
# MANUAL TOOL ONLY — not cron'd, not called from any automated path. Talks
# directly to each Shelly Plug US's local IP (cloud disabled per doctrine).
# Gen2+ RPC endpoints tried first (/rpc/Switch.GetStatus, /rpc/Switch.Set);
# falls back to the legacy Gen1 /relay/0 REST API if RPC doesn't respond.
#
# 'cycle' = off, sleep 15s, on, then poll GetStatus until it reports ON or
# 60s elapses (PASS/FAIL either way — never assumes success).
#
# SAFETY RAIL 1: refuses off/cycle against the plug whose hostname matches
# the host THIS script is currently running on — you cannot use this tool to
# power yourself off (status/on are still allowed against yourself).
#
# SAFETY RAIL 2: 'allo' off/cycle requires --confirm. Cutting the router
# almost certainly cuts this script's own network path mid-command if you're
# on that LAN/routed through it — the command fires blind, with no way to
# observe or confirm the result until the router's own watchdog script (see
# shelly_net_watchdog.js) recovers it independently.

set -u

# zsh reassigns $0 to the enclosing function's name inside a function body
# (FUNCTION_ARGZERO, on by default) -- capture the real invocation name here,
# at top level, before any function is defined.
SCRIPT_NAME="$0"

declare -A PLUG_IPS=(
  [bigmac]="192.168.1.245"
  [olliemax]="192.168.1.246"
  [allo]="192.168.1.244"
)

# Hostname this plug corresponds to, for the self-protect rail. NOTE: the
# actual macOS hostname of the "bigmac" project box is "Steves-Mac-mini" (per
# `hostname -s`), NOT the literal string "bigmac" -- verified directly against
# this machine and against olliemax over ssh (whose real hostname IS
# "olliemax", a happy match) before hardcoding this. 'allo' has no entry —
# it's a router, not a compute host this script would ever run ON in the same
# sense; its own risk is covered by SAFETY RAIL 2 (--confirm) instead.
declare -A PLUG_HOSTNAME_MATCH=(
  [bigmac]="Steves-Mac-mini"
  [olliemax]="olliemax"
)

usage() {
  echo "Usage: $SCRIPT_NAME {bigmac|olliemax|allo} {status|off|on|cycle} [--confirm]" >&2
  exit 1
}

[[ $# -lt 2 ]] && usage
TARGET="$1"
ACTION="$2"
CONFIRM_FLAG="${3:-}"

[[ -n "${PLUG_IPS[$TARGET]:-}" ]] || { echo "FAIL: unknown target '$TARGET' (expected bigmac|olliemax|allo)"; exit 1; }
IP="${PLUG_IPS[$TARGET]}"

case "$ACTION" in
  status|off|on|cycle) ;;
  *) usage ;;
esac

# ── SAFETY RAIL 1: refuse off/cycle against the host we're running on ──────
if [[ "$ACTION" == "off" || "$ACTION" == "cycle" ]]; then
  CURRENT_HOST="$(hostname -s 2>/dev/null || hostname)"
  expected_hostname="${PLUG_HOSTNAME_MATCH[$TARGET]:-}"
  if [[ -n "$expected_hostname" && "$CURRENT_HOST" == "$expected_hostname" ]]; then
    echo "FAIL: refusing '$ACTION' against '$TARGET' — this script is running ON that host. Cannot power yourself off."
    exit 2
  fi
fi

# ── SAFETY RAIL 2: allo off/cycle requires --confirm ───────────────────────
if [[ "$TARGET" == "allo" && ( "$ACTION" == "off" || "$ACTION" == "cycle" ) ]]; then
  if [[ "$CONFIRM_FLAG" != "--confirm" ]]; then
    echo "FAIL: cutting the Allo router will likely kill THIS SCRIPT's own network path mid-command."
    echo "      It fires blind — no way to observe the result until the router recovers on its own."
    echo "      Re-run with --confirm to proceed anyway: $SCRIPT_NAME allo $ACTION --confirm"
    exit 3
  fi
  echo "WARN: proceeding against the router we may be routed through. This command fires blind."
fi

# ── RPC helpers (Gen2+ primary, Gen1 /relay/0 fallback) ────────────────────
rpc_get_status() { curl -s --max-time 5 "http://$IP/rpc/Switch.GetStatus?id=0"; }
rpc_set()        { curl -s --max-time 5 "http://$IP/rpc/Switch.Set?id=0&on=$1"; }
legacy_get_status() { curl -s --max-time 5 "http://$IP/relay/0"; }
legacy_set()        { curl -s --max-time 5 "http://$IP/relay/0?turn=$1"; }

# Prints "on"/"off" via whichever API responds; prints nothing (exit 1) on
# total failure (both APIs unreachable).
get_state() {
  local resp
  resp="$(rpc_get_status)"
  if [[ -n "$resp" ]] && echo "$resp" | grep -q '"output"'; then
    echo "$resp" | grep -q '"output":true' && echo "on" || echo "off"
    return 0
  fi
  resp="$(legacy_get_status)"
  if [[ -n "$resp" ]] && echo "$resp" | grep -q '"ison"'; then
    echo "$resp" | grep -q '"ison":true' && echo "on" || echo "off"
    return 0
  fi
  return 1
}

set_state() {
  local want="$1" resp  # on|off
  local rpc_bool="false"; [[ "$want" == "on" ]] && rpc_bool="true"
  resp="$(rpc_set "$rpc_bool")"
  if [[ -n "$resp" ]] && echo "$resp" | grep -qE '"was_on"|^\{\}$'; then
    return 0
  fi
  resp="$(legacy_set "$want")"
  if [[ -n "$resp" ]] && echo "$resp" | grep -q '"ison"'; then
    return 0
  fi
  return 1
}

case "$ACTION" in
  status)
    state="$(get_state)"
    if [[ -z "$state" ]]; then
      echo "FAIL: $TARGET ($IP) unreachable via RPC or legacy API"
      exit 1
    fi
    echo "PASS: $TARGET ($IP) is $state"
    ;;
  on)
    if set_state on; then
      echo "PASS: $TARGET ($IP) turned ON"
    else
      echo "FAIL: $TARGET ($IP) turn-on failed (no response from RPC or legacy API)"
      exit 1
    fi
    ;;
  off)
    if set_state off; then
      echo "PASS: $TARGET ($IP) turned OFF"
    else
      echo "FAIL: $TARGET ($IP) turn-off failed (no response from RPC or legacy API)"
      exit 1
    fi
    ;;
  cycle)
    echo "[plug_cycle] $TARGET ($IP): OFF"
    if ! set_state off; then
      echo "FAIL: $TARGET ($IP) cycle aborted — turn-off failed"
      exit 1
    fi
    sleep 15
    echo "[plug_cycle] $TARGET ($IP): ON"
    if ! set_state on; then
      echo "FAIL: $TARGET ($IP) cycle failed — turn-on failed after off (device may now be OFF and unreachable)"
      exit 1
    fi
    echo "[plug_cycle] $TARGET ($IP): polling for ON (60s timeout)..."
    for i in $(seq 1 30); do
      state="$(get_state)"
      if [[ "$state" == "on" ]]; then
        echo "PASS: $TARGET ($IP) cycled and confirmed ON (${i}x2s polls)"
        exit 0
      fi
      sleep 2
    done
    echo "FAIL: $TARGET ($IP) cycle command sent but did not confirm ON within 60s"
    exit 1
    ;;
esac
