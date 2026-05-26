#!/usr/bin/env bash
# HM-ADMIRAL-TRADER-RESTART — controlled restart with safety gates.
#
# Stops the running trader and starts a fresh process so any commits
# shipped after the current PID's start time become live in-process.
# Born from the Memorial Day 2026-05-25 finding that Phase B holiday
# gates lived on disk but not in running bytecode (trader PID 43883
# started Sun May 24, all Memorial Day Phases shipped 24h later).
#
# Usage:
#   scripts/admiral_trader_restart.sh           # dry-run preview
#   scripts/admiral_trader_restart.sh --confirm # execute
#
# Why not launchctl kickstart:
#   `launchctl kickstart gui/$UID/com.trademinds.trader` returns
#   "Domain does not support specified action" on this Mac when run
#   over SSH — verified 2026-05-23 (see CLAUDE.md "LaunchAgent Reboot
#   Lifecycle"). The production startup path is the @reboot cron
#   entry that calls scripts/trader_reboot_start.sh; this script
#   reuses the same nohup/&! detachment pattern.
#
# Safety gates (all must pass; --confirm is the final gate):
#   1. Git working tree clean (uncommitted changes mean the restart
#      would load partial work)
#   2. Conviction flags all False or absent
#   3. neo-matrix + ollie-auto at halt_mode='exit_only'
#      (NEVER restart into a firing state; NULL halt_mode is unsafe)
#   4. Trader PID currently alive (this is restart, not cold-start)
#   5. --confirm flag present

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
PY_VENV=".venv/bin/python3"
ENTRYPOINT="main.py"
STDOUT_LOG="logs/trader.log"
STDERR_LOG="logs/trader_error.log"
RESTART_LOG="logs/admiral_restart.log"
TRADER_BASE="http://127.0.0.1:8080"

CONFIRM=0
if [ "${1:-}" = "--confirm" ]; then
    CONFIRM=1
fi

mkdir -p logs

echo "${BOLD}═══════════════════════════════════════════════════════════════"
echo "  HM-ADMIRAL-TRADER-RESTART  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "═══════════════════════════════════════════════════════════════${RESET}"
echo ""

# ── Gate 1: capture old PID + start time ──────────────────────────────────
echo "${BOLD}Step 1 — Discover current trader state${RESET}"
echo "---"
OLD_PID=$(pgrep -f "python.*${ENTRYPOINT}$" | head -1)
if [ -z "$OLD_PID" ]; then
    echo "${RED}${BOLD}ABORT:${RESET} no trader process found (pgrep returned empty)."
    echo "This script is for RESTART, not cold-start. If the trader is"
    echo "down, use scripts/trader_reboot_start.sh directly."
    exit 1
fi
OLD_LSTART=$(ps -o lstart= -p "$OLD_PID" | xargs)
OLD_EPOCH=$(ps -o lstart= -p "$OLD_PID" | xargs -I {} date -j -f "%a %b %e %T %Y" "{}" "+%s" 2>/dev/null || echo 0)
HEAD_EPOCH=$(git log -1 --format=%ct HEAD)
HEAD_COMMIT=$(git log -1 --format='%h %s')

echo "  Current PID    : $OLD_PID"
echo "  Started        : $OLD_LSTART"
echo "  HEAD commit    : $HEAD_COMMIT"

# Commits since PID start (this is the "what will load" preview)
NEW_COMMITS=$(git log --oneline --since="@$OLD_EPOCH" 2>/dev/null | wc -l | xargs)
echo "  Commits since  : $NEW_COMMITS (these become live after restart)"
echo ""

if [ "$NEW_COMMITS" -gt 0 ]; then
    echo "${BOLD}Commits to be loaded:${RESET}"
    git log --oneline --since="@$OLD_EPOCH" | head -20 | sed 's/^/  /'
    if [ "$NEW_COMMITS" -gt 20 ]; then
        echo "  … and $((NEW_COMMITS - 20)) more"
    fi
    echo ""
fi

# ── Gate 2: git clean? ────────────────────────────────────────────────────
echo "${BOLD}Step 2 — Safety gates${RESET}"
echo "---"

GATES_PASS=0
GATES_FAIL=0
gate_pass() { echo "  ${GREEN}[PASS]${RESET} $1"; GATES_PASS=$((GATES_PASS+1)); }
gate_fail() { echo "  ${RED}[FAIL]${RESET} $1"; GATES_FAIL=$((GATES_FAIL+1)); }

# Tracked-file dirtiness only — untracked .bak/scratch files are fine.
DIRTY=$(git status --porcelain | grep -vE "^\?\?" | head -5)
if [ -z "$DIRTY" ]; then
    gate_pass "Git working tree: no uncommitted tracked changes"
else
    gate_fail "Git working tree DIRTY (tracked changes present):"
    echo "$DIRTY" | sed 's/^/         /'
fi

# Conviction flags
FLAG_STOPS=$(grep   -E "^CONVICTION_SCALED_STOPS_ENABLED="          .env 2>/dev/null | cut -d= -f2)
FLAG_TRAIL=$(grep   -E "^CONVICTION_SCALED_TRAIL_ENABLED="          .env 2>/dev/null | cut -d= -f2)
FLAG_OPTS=$(grep    -E "^CONVICTION_SCALED_OPTIONS_STOP_ENABLED="   .env 2>/dev/null | cut -d= -f2)
FLAG_STOPS=${FLAG_STOPS:-absent}
FLAG_TRAIL=${FLAG_TRAIL:-absent}
FLAG_OPTS=${FLAG_OPTS:-absent}

FLAG_OK=1
case "$FLAG_STOPS" in False|absent) ;; *) FLAG_OK=0 ;; esac
case "$FLAG_TRAIL" in False|absent) ;; *) FLAG_OK=0 ;; esac
case "$FLAG_OPTS"  in False|absent) ;; *) FLAG_OK=0 ;; esac

if [ "$FLAG_OK" -eq 1 ]; then
    gate_pass "Conviction flags: stops=$FLAG_STOPS, trail=$FLAG_TRAIL, options=$FLAG_OPTS (all OFF)"
else
    gate_fail "Conviction flags ON: stops=$FLAG_STOPS, trail=$FLAG_TRAIL, options=$FLAG_OPTS"
fi

# Agent halt state — must be exit_only (or active, which means already
# unhalted and the restart is mid-trading-day, also OK). NULL/missing
# halt_mode is unsafe.
HALT_NEO=$(sqlite3 "$DB"   "SELECT COALESCE(halt_mode,'NULL') FROM ai_players WHERE id='neo-matrix'" 2>/dev/null)
HALT_OLLIE=$(sqlite3 "$DB" "SELECT COALESCE(halt_mode,'NULL') FROM ai_players WHERE id='ollie-auto'" 2>/dev/null)
HALT_NEO=${HALT_NEO:-NULL}
HALT_OLLIE=${HALT_OLLIE:-NULL}

halt_safe() {
    case "$1" in
        exit_only|active|full) return 0 ;;
        *) return 1 ;;
    esac
}
if halt_safe "$HALT_NEO" && halt_safe "$HALT_OLLIE"; then
    gate_pass "Agent halt state: neo-matrix=$HALT_NEO, ollie-auto=$HALT_OLLIE"
else
    gate_fail "Agent halt state UNSAFE: neo-matrix=$HALT_NEO, ollie-auto=$HALT_OLLIE"
fi

# Python interpreter present
if [ -x "$PY_VENV" ]; then
    gate_pass "Python interpreter: $PY_VENV"
else
    gate_fail "Python interpreter MISSING: $PY_VENV"
fi

echo ""
if [ "$GATES_FAIL" -gt 0 ]; then
    echo "${RED}${BOLD}ABORT:${RESET} $GATES_FAIL safety gate(s) failed. Resolve and re-run."
    exit 1
fi
echo "${GREEN}All $GATES_PASS gates passed.${RESET}"
echo ""

# ── Dry-run gate ──────────────────────────────────────────────────────────
if [ "$CONFIRM" -eq 0 ]; then
    echo "${YELLOW}${BOLD}DRY-RUN${RESET} (no --confirm flag)"
    echo ""
    echo "Would execute:"
    echo "  1. SIGTERM PID $OLD_PID (graceful shutdown, 20s grace)"
    echo "  2. SIGKILL if still alive"
    echo "  3. nohup $PY_VENV $ENTRYPOINT \"&!\" (detached)"
    echo "  4. Wait up to 30s for new PID + port 8080"
    echo "  5. Re-run scripts/admiral_premarket_check.sh"
    echo ""
    echo "To execute: ${BOLD}scripts/admiral_trader_restart.sh --confirm${RESET}"
    exit 0
fi

# ── Stop ──────────────────────────────────────────────────────────────────
echo "${BOLD}Step 3 — Stop current trader (PID $OLD_PID)${RESET}"
echo "---"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] SIGTERM $OLD_PID" | tee -a "$RESTART_LOG"
kill -TERM "$OLD_PID" 2>/dev/null

# Wait up to 20s for graceful shutdown
for i in $(seq 1 20); do
    if ! kill -0 "$OLD_PID" 2>/dev/null; then
        echo "  ${GREEN}OK${RESET} — old PID died after ${i}s"
        break
    fi
    sleep 1
done

if kill -0 "$OLD_PID" 2>/dev/null; then
    echo "  ${YELLOW}WARN${RESET} — SIGTERM didn't take after 20s, escalating to SIGKILL"
    kill -KILL "$OLD_PID" 2>/dev/null
    sleep 2
    if kill -0 "$OLD_PID" 2>/dev/null; then
        echo "${RED}${BOLD}ABORT:${RESET} PID $OLD_PID survived SIGKILL. Manual intervention required."
        exit 2
    fi
    echo "  ${GREEN}OK${RESET} — SIGKILL succeeded"
fi
echo ""

# ── Start ─────────────────────────────────────────────────────────────────
echo "${BOLD}Step 4 — Start fresh trader process${RESET}"
echo "---"
echo "[$(date '+%Y-%m-%d %H:%M:%S')] launching: $PY_VENV $ENTRYPOINT" | tee -a "$RESTART_LOG"

# Mirror trader_reboot_start.sh: nohup + & detached. The trailing & is bash's
# disown-on-exit; zsh's `&!` is the same concept but bash doesn't have it.
# We add an explicit `disown` to be sure.
nohup "$PY_VENV" "$ENTRYPOINT" >> "$STDOUT_LOG" 2>> "$STDERR_LOG" &
NEW_BG_PID=$!
disown 2>/dev/null || true

# Wait up to 30s for the new PID to appear and port 8080 to listen.
NEW_PID=""
PORT_LISTENING=0
for i in $(seq 1 30); do
    NEW_PID=$(pgrep -f "python.*${ENTRYPOINT}$" | head -1)
    PORT=$(lsof -ti :8080 2>/dev/null | head -1)
    if [ -n "$NEW_PID" ] && [ -n "$PORT" ]; then
        PORT_LISTENING=1
        echo "  ${GREEN}OK${RESET} — new PID $NEW_PID listening on :8080 after ${i}s"
        break
    fi
    sleep 1
done

if [ -z "$NEW_PID" ]; then
    echo "${RED}${BOLD}ABORT:${RESET} no new trader PID after 30s. Check $STDERR_LOG."
    tail -20 "$STDERR_LOG" 2>/dev/null | sed 's/^/  /'
    exit 3
fi
if [ "$PORT_LISTENING" -eq 0 ]; then
    echo "${YELLOW}WARN:${RESET} PID $NEW_PID alive but port 8080 not listening yet."
    echo "  (may take longer to bind; check manually)"
fi

if [ "$NEW_PID" = "$OLD_PID" ]; then
    echo "${RED}${BOLD}ABORT:${RESET} PID didn't change. Restart did not occur."
    exit 4
fi
echo ""

# ── Verify ────────────────────────────────────────────────────────────────
echo "${BOLD}Step 5 — Post-restart premarket re-check${RESET}"
echo "---"
sleep 3  # let the new process complete its bootstrap
if scripts/admiral_premarket_check.sh; then
    PREMARKET_OK=1
else
    PREMARKET_OK=0
fi
echo ""

# ── Summary ───────────────────────────────────────────────────────────────
NEW_LSTART=$(ps -o lstart= -p "$NEW_PID" | xargs)
echo "${BOLD}═══════════════════════════════════════════════════════════════"
echo "  RESTART SUMMARY"
echo "═══════════════════════════════════════════════════════════════${RESET}"
echo "  Old PID        : $OLD_PID  (started $OLD_LSTART)"
echo "  New PID        : $NEW_PID  (started $NEW_LSTART)"
echo "  HEAD commit    : $HEAD_COMMIT"
echo "  Commits loaded : $NEW_COMMITS"
if [ "$PREMARKET_OK" -eq 1 ]; then
    echo "  Premarket      : ${GREEN}clean${RESET}"
    echo ""
    echo "${GREEN}${BOLD}DONE${RESET} — $(date '+%Y-%m-%d %H:%M:%S %Z')"
    exit 0
else
    echo "  Premarket      : ${RED}FAIL${RESET}"
    echo ""
    echo "${YELLOW}${BOLD}Restart succeeded but premarket re-check FAILed.${RESET}"
    echo "Review failing checks above before unhalting."
    exit 1
fi
