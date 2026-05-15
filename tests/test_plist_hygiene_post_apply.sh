#!/bin/bash
# tests/test_plist_hygiene_post_apply.sh — HM-CD-β verification harness.
#
# Asserts that each plist in the HM-CD-β batch has the expected hygiene
# fields after --apply. Runs in TDD style:
#   - BEFORE apply: this test FAILS (red phase, expected — proves the test
#     actually exercises the change)
#   - AFTER apply: this test PASSES (green phase)
#
# Each plist's expected fields are encoded in the SPECS array below, mirroring
# the PLISTS table in scripts/hm_cd_beta_draft.sh. Format per row:
#   "label|risk|want_wd|want_softfd|want_hardfd|want_stderr"
#
# Usage:
#   bash tests/test_plist_hygiene_post_apply.sh                # all 17 plists
#   bash tests/test_plist_hygiene_post_apply.sh LOW            # one tier
#   bash tests/test_plist_hygiene_post_apply.sh MED
#   bash tests/test_plist_hygiene_post_apply.sh HIGH
#   bash tests/test_plist_hygiene_post_apply.sh <plist-label>  # single plist
#
# Exit non-zero on any failure.

export PATH="/usr/bin:/bin:/usr/sbin:/sbin:/usr/local/bin:/opt/homebrew/bin:$PATH"
set -uo pipefail

LA="$HOME/Library/LaunchAgents"
WORKDIR="/Users/bigmac/autonomous-trader"
SOFT_FD_EXPECTED=16384
HARD_FD_EXPECTED=32768

# (label|risk|want_wd|want_softfd|want_hardfd|want_stderr)
# Matches the PLISTS list in scripts/hm_cd_beta_draft.sh as of HM-CD-β-2026-05-15.
SPECS=(
  # LOW
  "com.trademinds.caffeinate|LOW|1|0|0|0"
  "com.ollietrades.crusher|LOW|1|0|0|0"
  "com.ollietrades.etfregime|LOW|1|0|0|0"
  "com.ollietrades.morning-an2-observation|LOW|1|0|0|0"
  "com.ollietrades.morning-cd-instr|LOW|1|0|0|0"
  "com.ollietrades.stale-trim-obs|LOW|1|0|0|0"
  # MED
  "com.ollietrades.optionsflow|MED|1|0|0|0"
  "com.ollietrades.schwab-watcher|MED|1|0|0|0"
  "com.trademinds.webull-sync|MED|1|0|0|0"
  "com.ollietrades.ollama-keepalive|MED|1|0|0|1"
  # danelfin-update is in the batch list but currently needs NO hygiene change
  # (WD already present). Asserting WD present here for the regression case
  # (so a future config that strips WD would be caught).
  "com.ollietrades.danelfin-update|MED|1|0|0|0"
  "com.ollietrades.ti-email-poller|MED|1|0|0|0"
  "com.ollietrades.ti-picks-watcher|MED|1|0|0|0"
  # HIGH
  "com.trademinds.mcp|HIGH|0|1|1|0"
  "com.trademinds.scanner|HIGH|0|1|1|0"
  "com.trademinds.watchdog|HIGH|0|1|1|0"
  "com.trademinds.tunnel|HIGH|1|1|1|0"
)

filter="${1:-ALL}"

# Resolves "filter matches this spec" for tier or single-label modes.
matches_filter() {
  local label=$1 risk=$2
  case "$filter" in
    ALL)            return 0 ;;
    LOW|MED|HIGH)   [ "$risk" = "$filter" ] && return 0 || return 1 ;;
    *)              [ "$label" = "$filter" ] && return 0 || return 1 ;;
  esac
}

has_key() {
  /usr/bin/plutil -extract "$2" raw -o - "$1" >/dev/null 2>&1
}

get_key() {
  /usr/bin/plutil -extract "$2" raw -o - "$1" 2>/dev/null
}

check_plist() {
  local label=$1 risk=$2 want_wd=$3 want_soft=$4 want_hard=$5 want_err=$6
  local path="$LA/$label.plist"

  if [ ! -f "$path" ]; then
    printf "  [SKIP] %-44s — plist not found\n" "$label"
    return 0
  fi

  local fails=()

  if [ "$want_wd" = "1" ]; then
    if has_key "$path" WorkingDirectory; then
      local actual; actual=$(get_key "$path" WorkingDirectory)
      if [ "$actual" != "$WORKDIR" ]; then
        fails+=("WorkingDirectory=\"$actual\" (expected \"$WORKDIR\")")
      fi
    else
      fails+=("WorkingDirectory missing")
    fi
  fi

  if [ "$want_soft" = "1" ]; then
    if has_key "$path" SoftResourceLimits.NumberOfFiles; then
      local actual; actual=$(get_key "$path" SoftResourceLimits.NumberOfFiles)
      if [ "$actual" != "$SOFT_FD_EXPECTED" ]; then
        fails+=("SoftResourceLimits.NumberOfFiles=$actual (expected $SOFT_FD_EXPECTED)")
      fi
    else
      fails+=("SoftResourceLimits.NumberOfFiles missing")
    fi
  fi

  if [ "$want_hard" = "1" ]; then
    if has_key "$path" HardResourceLimits.NumberOfFiles; then
      local actual; actual=$(get_key "$path" HardResourceLimits.NumberOfFiles)
      if [ "$actual" != "$HARD_FD_EXPECTED" ]; then
        fails+=("HardResourceLimits.NumberOfFiles=$actual (expected $HARD_FD_EXPECTED)")
      fi
    else
      fails+=("HardResourceLimits.NumberOfFiles missing")
    fi
  fi

  if [ "$want_err" = "1" ]; then
    if ! has_key "$path" StandardErrorPath; then
      fails+=("StandardErrorPath missing")
    fi
  fi

  if [ ${#fails[@]} -eq 0 ]; then
    printf "  [PASS] [%-4s] %s\n" "$risk" "$label"
    return 0
  else
    printf "  [FAIL] [%-4s] %s\n" "$risk" "$label"
    for f in "${fails[@]}"; do
      printf "         %s\n" "$f"
    done
    return 1
  fi
}

echo "════════════════════════════════════════════════════════════════"
echo "  HM-CD-β plist hygiene post-apply verification"
echo "  filter: $filter"
echo "════════════════════════════════════════════════════════════════"
echo ""

PASS=0
FAIL=0
CHECKED=0

for spec in "${SPECS[@]}"; do
  IFS='|' read -r label risk wd softfd hardfd err <<< "$spec"
  if ! matches_filter "$label" "$risk"; then continue; fi
  CHECKED=$((CHECKED + 1))
  if check_plist "$label" "$risk" "$wd" "$softfd" "$hardfd" "$err"; then
    PASS=$((PASS + 1))
  else
    FAIL=$((FAIL + 1))
  fi
done

echo ""
echo "── Summary ──"
echo "  Checked: $CHECKED"
echo "  Passed:  $PASS"
echo "  Failed:  $FAIL"

if [ "$CHECKED" -eq 0 ]; then
  echo ""
  echo "  ⚠ filter '$filter' matched zero plists. Did you typo the label?"
  exit 2
fi

[ "$FAIL" -eq 0 ]
