#!/usr/bin/env bash
# HM-ADMIRAL-PREMARKET-CHECK Phase B — safety-gated unhalt companion.
#
# Re-runs scripts/admiral_premarket_check.sh first. If all checks PASS,
# flips neo-matrix + ollie-auto from halt_mode='exit_only' → 'active'.
# Otherwise blocks with a non-zero exit.
#
# Usage:
#   scripts/admiral_unhalt_agents.sh           # dry-run preview, no writes
#   scripts/admiral_unhalt_agents.sh --confirm # execute UPDATE
#
# Guard rails:
#   - The premarket check is the gate. If it exits non-zero, this script
#     refuses to write — Admiral fixes the failing check first.
#   - The --confirm flag is mandatory for the actual UPDATE. Without it,
#     the script prints the SQL it WOULD run, then exits 0.
#   - Only neo-matrix and ollie-auto are touched. No other rows. No other
#     columns beyond halt_mode (halted_at + halt_reason are intentionally
#     preserved as historical record per CLAUDE.md "do not clear" rule).
#   - Post-write, the script SELECTs the two rows and prints them so the
#     Admiral can visually confirm before checking the live trader.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 2

if [ -t 1 ]; then
    GREEN=$(tput setaf 2 2>/dev/null || echo "")
    RED=$(tput setaf 1 2>/dev/null || echo "")
    YELLOW=$(tput setaf 3 2>/dev/null || echo "")
    BOLD=$(tput bold 2>/dev/null || echo "")
    RESET=$(tput sgr0 2>/dev/null || echo "")
else
    GREEN="" RED="" YELLOW="" BOLD="" RESET=""
fi

DB="data/trader.db"
PREMARKET="scripts/admiral_premarket_check.sh"

CONFIRM=0
if [ "${1:-}" = "--confirm" ]; then
    CONFIRM=1
fi

echo "${BOLD}═══════════════════════════════════════════════════════════════"
echo "  HM-ADMIRAL-UNHALT  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "═══════════════════════════════════════════════════════════════${RESET}"
echo ""
echo "${BOLD}Step 1 — Re-run premarket check (gate)${RESET}"
echo "---"
if ! "$PREMARKET"; then
    echo ""
    echo "${RED}${BOLD}ABORT:${RESET} premarket check exited non-zero. Resolve the failing"
    echo "checks above, then re-run this script. No DB writes performed."
    exit 1
fi
echo ""

# ── Step 2: show current state ────────────────────────────────────────────
echo "${BOLD}Step 2 — Current state of neo-matrix + ollie-auto${RESET}"
echo "---"
sqlite3 -header -column "$DB" "
SELECT id, halt_mode, halted_at, substr(halt_reason, 1, 70) AS halt_reason_short
  FROM ai_players
 WHERE id IN ('neo-matrix','ollie-auto')
 ORDER BY id
"
echo ""

# ── Step 3: dry-run vs confirm ────────────────────────────────────────────
SQL_UPDATE="UPDATE ai_players SET halt_mode='active' WHERE id IN ('neo-matrix','ollie-auto') AND halt_mode='exit_only';"

if [ "$CONFIRM" -eq 0 ]; then
    echo "${YELLOW}${BOLD}DRY-RUN${RESET} (no --confirm flag)"
    echo "Would execute:"
    echo "  $SQL_UPDATE"
    echo ""
    echo "To execute for real:"
    echo "  ${BOLD}scripts/admiral_unhalt_agents.sh --confirm${RESET}"
    exit 0
fi

# ── Step 4: real execute ──────────────────────────────────────────────────
echo "${BOLD}Step 3 — EXECUTING unhalt${RESET}"
echo "---"
echo "SQL: $SQL_UPDATE"
echo ""

# Use a real changes count to verify rows affected. SQLite returns
# changes() inside the same connection, so we wrap the UPDATE + check.
RESULT=$(sqlite3 "$DB" "
BEGIN;
$SQL_UPDATE
SELECT 'CHANGED=' || changes();
COMMIT;
" 2>&1)

CHANGES=$(echo "$RESULT" | awk -F= '/^CHANGED=/{print $2}')
CHANGES=${CHANGES:-0}

if [ "$CHANGES" -eq 0 ]; then
    echo "${YELLOW}WARNING:${RESET} 0 rows updated. Either the rows were already"
    echo "active, or they're in an unexpected halt_mode (not 'exit_only')."
    echo "Inspect current state above and decide."
elif [ "$CHANGES" -eq 2 ]; then
    echo "${GREEN}${BOLD}OK:${RESET} 2 rows updated (neo-matrix + ollie-auto → active)"
else
    echo "${RED}${BOLD}UNEXPECTED:${RESET} $CHANGES rows updated (expected exactly 2)"
fi
echo ""

# ── Step 5: verify post-state ─────────────────────────────────────────────
echo "${BOLD}Step 4 — Post-update state${RESET}"
echo "---"
sqlite3 -header -column "$DB" "
SELECT id, halt_mode, halted_at, substr(halt_reason, 1, 70) AS halt_reason_short
  FROM ai_players
 WHERE id IN ('neo-matrix','ollie-auto')
 ORDER BY id
"
echo ""
echo "${BOLD}Step 5 — Reminders${RESET}"
echo "---"
echo "  · halt_mode flip is read on the next cycle by paper_trader.buy/sell."
echo "    No trader restart required."
echo "  · halted_at + halt_reason were preserved as historical record"
echo "    (per CLAUDE.md \"do not clear\" rule)."
echo "  · Conviction-stop flags are still default OFF. Flip those separately"
echo "    after 5-10 day shadow validation."
echo "  · Memorial Day pending QQQ orders (if surfaced by CHECK 5) may now"
echo "    attempt to fill at 09:30 ET open — cancel them at the Alpaca UI"
echo "    before bell if not desired."
echo ""
echo "${GREEN}${BOLD}DONE${RESET} — $(date '+%Y-%m-%d %H:%M:%S %Z')"
exit 0
