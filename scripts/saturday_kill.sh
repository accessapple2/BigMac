#!/usr/bin/env bash
#
# saturday_kill.sh — Sniper Mode KILL + ollama-llama sunset orchestrator
#
# Toggle-aware: uses the halt mechanism documented in
# docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md §6 (halt_mode='full' on the agent
# row, NOT a sub-mode toggle). NEVER auto-edits dashboard/app.py — prints a
# patch hint instead and leaves the source-of-truth list edits for the
# Admiral to apply with eyes open.
#
# Default mode is --dry-run. Execution requires:
#   ./saturday_kill.sh --execute
#   then interactive "KILL" typed at the confirm prompt.
#
# Refuses to run on a weekday during market hours (Mon-Fri 06:30-13:00 MST)
# regardless of flag. Refuses if the toggle-map verdict is HOLD. Verifies
# all four _EXECUTION_ENABLED flags are still True. Verifies no in-flight
# trade for either kill target.
#
# Usage:
#   bash scripts/saturday_kill.sh                # dry-run (default)
#   bash scripts/saturday_kill.sh --dry-run      # explicit
#   bash scripts/saturday_kill.sh --execute      # asks for KILL confirmation
#

set -u  # treat unset vars as errors; do not -e (we want explicit error handling)

ROOT="${HOME}/autonomous-trader"
DB="${ROOT}/data/trader.db"
TOGGLE_MAP="${ROOT}/docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md"
LOG_DIR="${ROOT}/logs"
LOG_FILE="${LOG_DIR}/saturday_kill.log"
LESSONS_DIR="${ROOT}/docs/lessons"
NTFY_TOPIC="ollietrades-admin"
TODAY=$(date +%Y-%m-%d)
NOW_TS=$(date '+%Y-%m-%d %H:%M:%S %Z')

MODE="dry-run"

mkdir -p "${LOG_DIR}" "${LESSONS_DIR}"

log() {
    local ts msg
    ts=$(date '+%Y-%m-%dT%H:%M:%S%z')
    msg="$*"
    printf '[%s] %s\n' "${ts}" "${msg}" | tee -a "${LOG_FILE}"
}

ntfy() {
    local title="$1"
    local body="$2"
    if command -v curl >/dev/null 2>&1; then
        curl -s -H "Title: ${title}" -H "Priority: default" \
             -d "${body}" "https://ntfy.sh/${NTFY_TOPIC}" >/dev/null || true
    fi
}

die() {
    log "FATAL: $*"
    ntfy "Saturday KILL aborted" "$*"
    exit 1
}

# ── Parse args ────────────────────────────────────────────────────────────
for a in "$@"; do
    case "${a}" in
        --dry-run) MODE="dry-run" ;;
        --execute) MODE="execute" ;;
        -h|--help)
            cat <<'EOF'
Usage: bash scripts/saturday_kill.sh [--dry-run | --execute]

  --dry-run   (default) print every step but mutate nothing
  --execute   prompt for "KILL" and then perform the halt + lesson writes

Always reads the GO/HOLD verdict from
  docs/MODEL_TOGGLE_INFRASTRUCTURE_MAP.md §7
and refuses to run on weekdays during US market hours.
EOF
            exit 0
            ;;
        *) die "unknown arg: ${a}" ;;
    esac
done

log "================================================================"
log "Saturday KILL orchestrator starting (mode=${MODE})"
log "Working dir: ${ROOT}"
log "DB: ${DB}"
log "Toggle map: ${TOGGLE_MAP}"
log "================================================================"

# ── Pre-flight 1 — markets-closed gate ────────────────────────────────────
DOW=$(date +%u)             # 1=Mon ... 7=Sun
HOUR_LOCAL=$(date +%H)      # local 24h
MIN_LOCAL=$(date +%M)
MARKET_OPEN_LOCAL=0
# Heuristic: assume host is on MST (CLAUDE.md is consistent on MST). NYSE
# regular hours are 06:30-13:00 MST. If running this from a different TZ
# the heuristic over-protects, which is fine — better to refuse and let
# the Admiral force --execute on a Saturday.
if [[ "${DOW}" -ge 1 && "${DOW}" -le 5 ]]; then
    if [[ "${HOUR_LOCAL}" -ge 7 && "${HOUR_LOCAL}" -lt 13 ]]; then
        MARKET_OPEN_LOCAL=1
    elif [[ "${HOUR_LOCAL}" -eq 6 && "${MIN_LOCAL}" -ge 30 ]]; then
        MARKET_OPEN_LOCAL=1
    fi
fi

if [[ "${MARKET_OPEN_LOCAL}" -eq 1 ]]; then
    log "Pre-flight 1 ❌  Markets appear OPEN (DOW=${DOW}, ${HOUR_LOCAL}:${MIN_LOCAL} MST). Refusing."
    if [[ "${MODE}" = "execute" ]]; then
        die "Cannot --execute during market hours. Try Saturday after 13:00 MST."
    fi
    log "Continuing dry-run anyway so the rest of pre-flight is exercised."
else
    log "Pre-flight 1 ✓  Markets closed (DOW=${DOW}, ${HOUR_LOCAL}:${MIN_LOCAL})."
fi

# ── Pre-flight 2 — toggle-map verdict gate ─────────────────────────────────
if [[ ! -f "${TOGGLE_MAP}" ]]; then
    die "Toggle infrastructure map missing: ${TOGGLE_MAP}"
fi
VERDICT=$(awk '/^## 7\. Saturday-readiness verdict/{flag=1;next} /^## /{flag=0} flag' "${TOGGLE_MAP}" \
          | grep -oE 'GO-WITH-DOC-FIX|HOLD|\<GO\>' | head -1)
log "Pre-flight 2  Toggle-map verdict: ${VERDICT:-UNKNOWN}"
case "${VERDICT}" in
    GO|GO-WITH-DOC-FIX)
        log "Pre-flight 2 ✓  Verdict permits execution."
        ;;
    HOLD)
        die "Verdict is HOLD per ${TOGGLE_MAP}. Refusing."
        ;;
    *)
        die "Verdict not parseable from ${TOGGLE_MAP}. Refusing."
        ;;
esac

# ── Pre-flight 3 — _EXECUTION_ENABLED sanity ───────────────────────────────
GATE_FILES=(
    "${ROOT}/strategies/bull_call_spread_v1.py"
    "${ROOT}/strategies/bear_put_spread_v1.py"
    "${ROOT}/strategies/bull_spread_v1.py"
    "${ROOT}/strategies/executor.py"
)
GATE_FAILS=0
for f in "${GATE_FILES[@]}"; do
    if grep -qE '^_EXECUTION_ENABLED:?\s*bool\s*=\s*True' "${f}"; then
        log "  ${f}  _EXECUTION_ENABLED=True ✓"
    else
        log "  ${f}  _EXECUTION_ENABLED is NOT True (sanity check)"
        GATE_FAILS=$((GATE_FAILS + 1))
    fi
done
if [[ "${GATE_FAILS}" -gt 0 ]]; then
    log "Pre-flight 3 ⚠️   ${GATE_FAILS} gate(s) not in expected True state. Continuing — sanity-only check."
else
    log "Pre-flight 3 ✓  All 4 _EXECUTION_ENABLED gates True."
fi

# ── Pre-flight 4 — no in-flight trade for either kill target ──────────────
in_flight_count() {
    # Counts very recent fills (last 6h) and any positions still open.
    local pid="$1"
    local recent
    recent=$(sqlite3 "${DB}" \
        "SELECT COUNT(*) FROM trades WHERE player_id='${pid}' AND executed_at > datetime('now','-6 hours');" 2>/dev/null || echo "?")
    local open
    open=$(sqlite3 "${DB}" \
        "SELECT COUNT(*) FROM positions WHERE player_id='${pid}' AND COALESCE(qty,0) != 0;" 2>/dev/null || echo "?")
    echo "${recent}|${open}"
}

OA=$(in_flight_count "ollie-auto")
OL=$(in_flight_count "ollama-llama")
OA_RECENT=${OA%|*}; OA_OPEN=${OA##*|}
OL_RECENT=${OL%|*}; OL_OPEN=${OL##*|}
log "Pre-flight 4  ollie-auto    last-6h trades=${OA_RECENT}  open positions=${OA_OPEN}"
log "Pre-flight 4  ollama-llama  last-6h trades=${OL_RECENT}  open positions=${OL_OPEN}"

if [[ "${OA_RECENT}" =~ ^[0-9]+$ && "${OA_RECENT}" -gt 0 ]]; then
    log "Pre-flight 4 ⚠️   ollie-auto traded in last 6h (${OA_RECENT}). Halt is still safe (post-fill); just note it."
fi
if [[ "${OA_OPEN}" =~ ^[0-9]+$ && "${OA_OPEN}" -gt 0 ]]; then
    log "Pre-flight 4 ⚠️   ollie-auto has ${OA_OPEN} open positions. They will not auto-close on halt; Admiral closes per Saturday checklist."
fi
if [[ "${OL_OPEN}" =~ ^[0-9]+$ && "${OL_OPEN}" -gt 0 ]]; then
    log "Pre-flight 4 ⚠️   ollama-llama has ${OL_OPEN} open positions; halt_mode='full' blocks new opens but exit_only would close. Halt to 'full' anyway per sunset plan."
fi

# ── Halt SQL preview (CLAUDE.md halt SQL pattern) ──────────────────────────
log ""
log "──────── Sniper KILL — halt action plan ────────"
SNIPER_REASON="${TODAY} Sniper trial ended (Day 30/30); KILL per docs/SNIPER_MODE_CLOSURE_PLAN.md + MODEL_TOGGLE_INFRASTRUCTURE_MAP.md §6"
LLAMA_REASON="${TODAY} ollama-llama sunset per docs/AGENT_SUNSET_OLLAMA_LLAMA.md (already exit_only since 2026-04-23; final retire to halt_mode='full')"

cat <<EOF | tee -a "${LOG_FILE}"
[ Sniper kill — ollie-auto ]
SQL:
  UPDATE ai_players
     SET halt_mode  = 'full',
         halted_at  = CURRENT_TIMESTAMP,
         halt_reason = '${SNIPER_REASON}'
   WHERE id = 'ollie-auto';

[ ollama-llama sunset ]
SQL:
  UPDATE ai_players
     SET halt_mode  = 'full',
         halted_at  = CURRENT_TIMESTAMP,
         halt_reason = '${LLAMA_REASON}'
   WHERE id = 'ollama-llama';

[ Source-of-truth list edits — NOT auto-applied; Admiral runs these ]
  dashboard/app.py:1445   remove "ollie-auto" from FLEET_ACTIVE
  dashboard/app.py:1432   remove "ollama-llama" from PROTECTED_AGENTS
  engine/proving_ground.py:34  remove "ollama-llama" from SNIPER_AGENTS
EOF

# ── Mode branch ────────────────────────────────────────────────────────────
if [[ "${MODE}" = "dry-run" ]]; then
    log ""
    log "Mode is dry-run — NO SQL executed, NO files written."
    log ""
    log "Lesson files that WOULD be created on --execute:"
    log "  ${LESSONS_DIR}/SNIPER_MODE_CLOSURE_${TODAY}.md"
    log "  ${LESSONS_DIR}/AGENT_RETIRED_OLLAMA_LLAMA_${TODAY}.md"
    log ""
    log "Run with --execute on Saturday after 13:00 MST to perform."
    log "================================================================"
    exit 0
fi

# ── Interactive KILL confirmation ──────────────────────────────────────────
echo ""
echo "═══════════════════════════════════════════════════════════════════"
echo "About to perform the SNIPER KILL + ollama-llama SUNSET."
echo "Two ai_players rows will move to halt_mode='full'."
echo "Two lesson files will be written to docs/lessons/."
echo ""
echo "Type  KILL  (capitals, no quotes) to proceed, anything else to abort:"
echo "═══════════════════════════════════════════════════════════════════"
read -r CONFIRM
if [[ "${CONFIRM}" != "KILL" ]]; then
    log "User did not type KILL (typed: '${CONFIRM}'). Aborting without changes."
    exit 1
fi

# ── Execute halts ──────────────────────────────────────────────────────────
log ""
log "Confirmation received. Performing halts..."
sqlite3 "${DB}" <<SQL
UPDATE ai_players
   SET halt_mode  = 'full',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '${SNIPER_REASON}'
 WHERE id = 'ollie-auto';
UPDATE ai_players
   SET halt_mode  = 'full',
       halted_at  = CURRENT_TIMESTAMP,
       halt_reason = '${LLAMA_REASON}'
 WHERE id = 'ollama-llama';
SQL
RC=$?
if [[ "${RC}" -ne 0 ]]; then
    die "sqlite3 update returned ${RC}. Investigate before re-running."
fi

# Verify post-state
OA_AFTER=$(sqlite3 "${DB}" "SELECT halt_mode FROM ai_players WHERE id='ollie-auto';")
OL_AFTER=$(sqlite3 "${DB}" "SELECT halt_mode FROM ai_players WHERE id='ollama-llama';")
log "Post-state ollie-auto.halt_mode    = ${OA_AFTER}"
log "Post-state ollama-llama.halt_mode  = ${OL_AFTER}"
if [[ "${OA_AFTER}" != "full" || "${OL_AFTER}" != "full" ]]; then
    die "Post-state verification failed. Expected halt_mode='full' on both rows."
fi
log "Post-state ✓  Both rows confirmed halt_mode='full'."

# ── Write lesson files ────────────────────────────────────────────────────
SNIPER_LESSON="${LESSONS_DIR}/SNIPER_MODE_CLOSURE_${TODAY}.md"
LLAMA_LESSON="${LESSONS_DIR}/AGENT_RETIRED_OLLAMA_LLAMA_${TODAY}.md"

cat > "${SNIPER_LESSON}" <<EOF
# Sniper Mode — closure lesson, ${TODAY}

Source plan: \`docs/SNIPER_MODE_CLOSURE_PLAN.md\` (Scotty 2.4, 2026-05-07)
Halt action: \`UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, halt_reason='${SNIPER_REASON}' WHERE id='ollie-auto'\`
Halt timestamp: ${NOW_TS}

## What ended

The 30-day Sniper Mode proving ground (TRIAL_START 2026-04-10,
TRIAL_DAYS 30, ended Day 30 ${TODAY} EOD MST) terminated by halting
\`ollie-auto\` (the Sniper / Fleet Commander gate) to \`halt_mode='full'\`.

## Lesson — sizing artifact, not alpha

Per Scotty's 2026-05-07 closure plan: the bug-affected scorecard reported
+1259.99 cumulative_return because the rollup aggregated trades from 6
legacy ghost agents (deepseek-7b-grok4, ollama-plutus, qwen3-8b-flash,
gemini-2.5-flash, ollama-llama, grok-4) and summed pnl_pct. The corrected
metric, computed against \`ollie-auto\`'s actual book in trader.db over 30
days, was +0.75% return on a \$10k notional — Sniper was 89× smaller in
absolute return than the rest of the fleet over the same window. The
14.6 daily-Sharpe was a sizing artifact (\$73 avg notional vs Plutus
\$204 / Capitol \$265), not edge.

## What is preserved (sacred-data rule)

- All rows in \`trades\`, \`portfolio_history\`, \`proving_ground.*\`
- The \`ollie-auto\` row in \`ai_players\` (halt_mode='full')
- The Python code paths in engine/proving_ground.py (mute via halt, do not delete)

## Follow-up doc edits (not done by this script)

- Remove \`"ollie-auto"\` from \`dashboard/app.py:1445 FLEET_ACTIVE\`
- Stop or rewrite the proving_ground daily rollup
EOF

cat > "${LLAMA_LESSON}" <<EOF
# ollama-llama — final retirement lesson, ${TODAY}

Source plan: \`docs/AGENT_SUNSET_OLLAMA_LLAMA.md\`
Halt action: \`UPDATE ai_players SET halt_mode='full', halted_at=CURRENT_TIMESTAMP, halt_reason='${LLAMA_REASON}' WHERE id='ollama-llama'\`
Halt timestamp: ${NOW_TS}

## State transition

| Before | After |
|---|---|
| halt_mode='exit_only' since 2026-04-23 ("S6 review: routing zombie") | halt_mode='full' (final retirement) |

## Why now

ollama-llama was the Sniper trial's Uhura slot; with the trial ended
${TODAY}, no role remains for the agent. The 2026-04-23 exit_only halt
prevented new opens but kept the row available for an eventual rehab
that never materialized. With Phase 1 audit + 2026-05-03 fleet
reconciliation flagging the row as a routing zombie, the closure window
is now.

## What is preserved (sacred-data rule)

- The \`ollama-llama\` row in \`ai_players\` (halt_mode='full')
- All historic trades, signals, costs

## Follow-up doc edits (not done by this script)

- Remove \`"ollama-llama"\` from \`dashboard/app.py:1432 PROTECTED_AGENTS\`
- Remove \`"ollama-llama"\` from \`engine/proving_ground.py:34 SNIPER_AGENTS\`
EOF

log "Lesson files written:"
log "  ${SNIPER_LESSON}"
log "  ${LLAMA_LESSON}"

# ── Post-fire verification ─────────────────────────────────────────────────
log ""
log "Post-fire verification (5 min sleep, then re-query)…"
sleep 300

POST_OA_TRADES=$(sqlite3 "${DB}" "SELECT COUNT(*) FROM trades WHERE player_id='ollie-auto' AND executed_at > '${NOW_TS}';")
POST_OL_TRADES=$(sqlite3 "${DB}" "SELECT COUNT(*) FROM trades WHERE player_id='ollama-llama' AND executed_at > '${NOW_TS}';")
POST_OA_SIGS=$(sqlite3 "${DB}" "SELECT COUNT(*) FROM signals WHERE player_id='ollie-auto' AND created_at > '${NOW_TS}';")
POST_OL_SIGS=$(sqlite3 "${DB}" "SELECT COUNT(*) FROM signals WHERE player_id='ollama-llama' AND created_at > '${NOW_TS}';")

log "Post-halt activity (5-min window):"
log "  ollie-auto    trades=${POST_OA_TRADES}  signals=${POST_OA_SIGS}"
log "  ollama-llama  trades=${POST_OL_TRADES}  signals=${POST_OL_SIGS}"

if [[ "${POST_OA_TRADES}" != "0" || "${POST_OL_TRADES}" != "0" ]]; then
    log "⚠️   Post-halt trades observed. The execution gate may have a leak. Investigate before next session."
    ntfy "Saturday KILL — leak detected" "Post-halt trades: ollie-auto=${POST_OA_TRADES}, ollama-llama=${POST_OL_TRADES}. Investigate paper_trader.py halt gate."
else
    log "Post-fire ✓  No new trades from either halted player in 5 min."
fi

ntfy "Saturday KILL complete" \
    "ollie-auto + ollama-llama -> halt_mode='full'. Lessons: docs/lessons/. Post-halt trades: ollie-auto=${POST_OA_TRADES}, ollama-llama=${POST_OL_TRADES}. Doc-fix list edits still pending (FLEET_ACTIVE, PROTECTED_AGENTS, SNIPER_AGENTS)."

log "================================================================"
log "Saturday KILL orchestrator complete."
log "================================================================"
exit 0
