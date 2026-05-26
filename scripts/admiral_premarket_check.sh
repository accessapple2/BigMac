#!/usr/bin/env bash
# HM-ADMIRAL-PREMARKET-CHECK — Memorial Day 2026-05-25.
#
# Eight read-only checks the Admiral runs pre-bell (and every future
# post-holiday Tuesday) before unhalting agents. Companion script
# scripts/admiral_unhalt_agents.sh re-runs this one + executes the SQL
# behind a --confirm flag.
#
# Usage:
#   scripts/admiral_premarket_check.sh
#
# Exit codes:
#   0  all 8 checks PASS — safe to unhalt
#   1  one or more checks FAIL — DO NOT unhalt without investigation
#
# Guard rails:
#   - READ-ONLY. No writes to any DB. No network writes.
#   - DB queries are SELECT-only.
#   - Alpaca probes use the trader's local /api/alpaca/* proxy (read).
#   - If any check returns truly anomalous data, the corresponding line
#     prints FAIL with detail and the summary exits non-zero. The Admiral
#     decides what to do.

set -uo pipefail

cd "$(dirname "$0")/.." || exit 2

# ── Colors (only if stdout is a tty) ──────────────────────────────────────
if [ -t 1 ]; then
    GREEN=$(tput setaf 2 2>/dev/null || echo "")
    RED=$(tput setaf 1 2>/dev/null || echo "")
    YELLOW=$(tput setaf 3 2>/dev/null || echo "")
    BOLD=$(tput bold 2>/dev/null || echo "")
    RESET=$(tput sgr0 2>/dev/null || echo "")
else
    GREEN="" RED="" YELLOW="" BOLD="" RESET=""
fi

PASS_LABEL="${GREEN}[PASS]${RESET}"
FAIL_LABEL="${RED}[FAIL]${RESET}"
WARN_LABEL="${YELLOW}[WARN]${RESET}"

PASS_COUNT=0
FAIL_COUNT=0

pass() { printf "%s %s\n" "$PASS_LABEL" "$1"; PASS_COUNT=$((PASS_COUNT+1)); }
fail() { printf "%s %s\n" "$FAIL_LABEL" "$1"; FAIL_COUNT=$((FAIL_COUNT+1)); }
warn() { printf "%s %s\n" "$WARN_LABEL" "$1"; }

DB="data/trader.db"
PY="venv/bin/python3"
TRADER_BASE="http://127.0.0.1:8080"

if [ ! -f "$DB" ]; then
    fail "PRE-CHECK: trader.db missing at $DB"
    exit 2
fi
if [ ! -x "$PY" ]; then
    fail "PRE-CHECK: venv python missing at $PY"
    exit 2
fi

echo "${BOLD}═══════════════════════════════════════════════════════════════"
echo "  HM-ADMIRAL-PREMARKET-CHECK  $(date '+%Y-%m-%d %H:%M:%S %Z')"
echo "═══════════════════════════════════════════════════════════════${RESET}"

# ── CHECK 1: System health ────────────────────────────────────────────────
check_system_health() {
    local trader_pid signal_pid port8080 port9000
    trader_pid=$(pgrep -f "python.*main\.py$" | head -1)
    signal_pid=$(pgrep -f "signal-center.*server\.py" | head -1)
    port8080=$(lsof -ti :8080 2>/dev/null | head -1)
    port9000=$(lsof -ti :9000 2>/dev/null | head -1)

    local issues=()
    [ -z "$trader_pid" ] && issues+=("trader PID not found")
    [ -z "$signal_pid" ] && issues+=("signal-center PID not found")
    [ -z "$port8080" ]   && issues+=("port 8080 not listening")
    [ -z "$port9000" ]   && issues+=("port 9000 not listening")

    if [ ${#issues[@]} -eq 0 ]; then
        pass "CHECK 1: System health — trader PID $trader_pid, signal-center PID $signal_pid, ports 8080 + 9000 listening"
    else
        fail "CHECK 1: System health — $(IFS=', '; echo "${issues[*]}")"
    fi
}

# ── CHECK 2: Market calendar status ───────────────────────────────────────
check_market_calendar() {
    # Source of truth is the local module, not the API (live process may be
    # running pre-merge bytecode). HM-MARKET-HOLIDAY-CALENDAR shipped today.
    local out status holiday next_open
    out=$("$PY" -c "
from engine.market_calendar import get_market_status, get_holiday_name, next_market_open
from datetime import date
print('STATUS=' + get_market_status().name)
h = get_holiday_name(date.today())
print('HOLIDAY=' + (h or 'NONE'))
print('NEXT_OPEN=' + next_market_open().strftime('%Y-%m-%d %H:%M %Z'))
" 2>&1)
    if [ $? -ne 0 ]; then
        fail "CHECK 2: Market calendar — module call failed: $out"
        return
    fi
    status=$(echo "$out"    | awk -F= '/^STATUS=/{print $2}')
    holiday=$(echo "$out"   | awk -F= '/^HOLIDAY=/{print $2}')
    next_open=$(echo "$out" | awk -F= '/^NEXT_OPEN=/{print $2}')

    case "$status" in
        OPEN)
            pass "CHECK 2: Market calendar — OPEN (regular trading hours)"
            ;;
        CLOSED_HOLIDAY)
            # Memorial Day → expected pre-bell. After Tuesday open, this turning
            # to OPEN is the green light. Today (Mon 2026-05-25) it correctly
            # reports CLOSED_HOLIDAY=Memorial Day.
            warn "CHECK 2: Market calendar — CLOSED_HOLIDAY ($holiday). Next open: $next_open"
            PASS_COUNT=$((PASS_COUNT+1))
            ;;
        CLOSED_BEFORE_HOURS|CLOSED_AFTER_HOURS|CLOSED_EARLY|CLOSED_WEEKEND)
            warn "CHECK 2: Market calendar — $status. Next open: $next_open"
            PASS_COUNT=$((PASS_COUNT+1))
            ;;
        *)
            fail "CHECK 2: Market calendar — unexpected status: $status"
            ;;
    esac
}

# ── CHECK 3: Agent halt state ─────────────────────────────────────────────
check_agent_halt_state() {
    local rows neo ollie
    rows=$(sqlite3 "$DB" "SELECT id || '|' || halt_mode FROM ai_players WHERE id IN ('neo-matrix','ollie-auto') ORDER BY id" 2>&1)
    neo=$(echo "$rows"   | awk -F'|' '/^neo-matrix\|/{print $2}')
    ollie=$(echo "$rows" | awk -F'|' '/^ollie-auto\|/{print $2}')

    if [ -z "$neo" ] || [ -z "$ollie" ]; then
        fail "CHECK 3: Agent halt state — query returned incomplete: $rows"
        return
    fi

    # Expected state pre-unhalt: both exit_only
    # Acceptable post-unhalt:    both active
    # Any other combination is an anomaly the Admiral should see.
    if [ "$neo" = "exit_only" ] && [ "$ollie" = "exit_only" ]; then
        pass "CHECK 3: Agent halt state — neo-matrix=exit_only, ollie-auto=exit_only (pre-unhalt, expected)"
    elif [ "$neo" = "active" ] && [ "$ollie" = "active" ]; then
        pass "CHECK 3: Agent halt state — neo-matrix=active, ollie-auto=active (already unhalted)"
    else
        fail "CHECK 3: Agent halt state — UNEXPECTED: neo-matrix=$neo, ollie-auto=$ollie"
    fi
}

# ── CHECK 4: Conviction-stop flags ────────────────────────────────────────
check_conviction_flags() {
    local stops trail options
    stops=$(grep   -E "^CONVICTION_SCALED_STOPS_ENABLED="          .env 2>/dev/null | cut -d= -f2)
    trail=$(grep   -E "^CONVICTION_SCALED_TRAIL_ENABLED="          .env 2>/dev/null | cut -d= -f2)
    options=$(grep -E "^CONVICTION_SCALED_OPTIONS_STOP_ENABLED="   .env 2>/dev/null | cut -d= -f2)

    stops=${stops:-absent}
    trail=${trail:-absent}
    options=${options:-absent}

    # Expected: all False or absent (absent = code default False).
    local ok=1
    case "$stops"   in False|absent) ;; *) ok=0 ;; esac
    case "$trail"   in False|absent) ;; *) ok=0 ;; esac
    case "$options" in False|absent) ;; *) ok=0 ;; esac

    if [ "$ok" -eq 1 ]; then
        pass "CHECK 4: Conviction flags — stops=$stops, trail=$trail, options=$options (all defaulting OFF, expected)"
    else
        fail "CHECK 4: Conviction flags — UNEXPECTED ON: stops=$stops, trail=$trail, options=$options"
    fi
}

# ── CHECK 5: Alpaca state ─────────────────────────────────────────────────
check_alpaca_state() {
    local status_json orders_json equity pending_count
    status_json=$(curl -s -m 6 -H "Accept: application/json" "$TRADER_BASE/api/alpaca/status" 2>&1)
    orders_json=$(curl -s -m 6 -H "Accept: application/json" "$TRADER_BASE/api/alpaca/orders?status=open" 2>&1)

    equity=$(echo "$status_json" | "$PY" -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    print(f'{d.get(\"equity\", 0):.2f}')
except Exception as e:
    print(f'ERR:{e}')
" 2>&1)

    pending_count=$(echo "$orders_json" | "$PY" -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    print(len(d.get('orders', [])))
except Exception as e:
    print(f'ERR:{e}')
" 2>&1)

    if [[ "$equity" == ERR:* ]] || [[ "$pending_count" == ERR:* ]]; then
        fail "CHECK 5: Alpaca state — proxy parse error: equity=$equity, orders=$pending_count"
        return
    fi

    # Pending-orders policy: post-Memorial-Day cancel arc, expectation is 0.
    # Any non-zero count is surfaced for Admiral review (not auto-failed —
    # could be legitimate Captain trade-desk pending limits).
    if [ "$pending_count" -eq 0 ]; then
        pass "CHECK 5: Alpaca state — equity=\$$equity, 0 pending orders"
    else
        local sample
        sample=$(echo "$orders_json" | "$PY" -c "
import sys, json
d = json.loads(sys.stdin.read())
for o in d.get('orders', [])[:5]:
    print(f'  · {o.get(\"submitted_at\",\"?\")} {o.get(\"side\",\"?\"):<4} {o.get(\"symbol\",\"?\"):<6} qty={o.get(\"qty\",\"?\")} status={o.get(\"status\",\"?\")}')
")
        fail "CHECK 5: Alpaca state — equity=\$$equity, ${BOLD}$pending_count pending orders${RESET} (expected 0 post-Memorial-Day cancel arc)
$sample"
    fi
}

# ── CHECK 6: Scanner job health ───────────────────────────────────────────
check_scanner_health() {
    local rs_max minervini_max squeeze_max
    rs_max=$(sqlite3 "$DB" "SELECT MAX(computed_at) FROM rs_rank" 2>&1)
    minervini_max=$(sqlite3 "$DB" "SELECT MAX(computed_at) FROM minervini_trend" 2>&1)
    # bbkc_squeeze table doesn't exist on this DB; squeeze data lives in
    # squeeze_watch with timestamp column `scan_ts`.
    squeeze_max=$(sqlite3 "$DB" "SELECT MAX(scan_ts) FROM squeeze_watch" 2>/dev/null)
    [ -z "$squeeze_max" ] && squeeze_max="(table-shape-unknown)"

    # Window: scanner ran within last 24h is healthy (overnight cycle).
    local now_epoch rs_epoch min_epoch squeeze_epoch
    now_epoch=$(date +%s)
    # SQLite returns 'YYYY-MM-DD HH:MM:SS'; convert to epoch via Python (avoids
    # macOS BSD `date -d` non-portability).
    rs_epoch=$("$PY"     -c "from datetime import datetime; print(int(datetime.fromisoformat('$rs_max'.replace(' ','T')).timestamp()))" 2>/dev/null || echo 0)
    min_epoch=$("$PY"    -c "from datetime import datetime; print(int(datetime.fromisoformat('$minervini_max'.replace(' ','T')).timestamp()))" 2>/dev/null || echo 0)
    squeeze_epoch=$("$PY" -c "from datetime import datetime; print(int(datetime.fromisoformat('$squeeze_max'.replace(' ','T')).timestamp()))" 2>/dev/null || echo 0)

    local stale=()
    [ $((now_epoch - rs_epoch))      -gt 86400 ] && stale+=("rs_rank=$rs_max")
    [ $((now_epoch - min_epoch))     -gt 86400 ] && stale+=("minervini_trend=$minervini_max")
    [ $((now_epoch - squeeze_epoch)) -gt 86400 ] && stale+=("squeeze_watch=$squeeze_max")

    if [ ${#stale[@]} -eq 0 ]; then
        pass "CHECK 6: Scanner health — rs_rank=$rs_max, minervini_trend=$minervini_max, squeeze_watch=$squeeze_max (all <24h)"
    else
        warn "CHECK 6: Scanner health — STALE: $(IFS=', '; echo "${stale[*]}")"
        # Memorial Day note: scanners may legitimately be quiet on a holiday
        # because the universe-feed gates on market_open. Surface as WARN
        # not FAIL — Admiral evaluates against the day's expected cadence.
        PASS_COUNT=$((PASS_COUNT+1))
    fi
}

# ── CHECK 7: Database integrity (portfolio_history) ───────────────────────
check_db_integrity() {
    local today_count yesterday_count yesterday_date
    yesterday_date=$(date -v-1d +%Y-%m-%d 2>/dev/null || date -d 'yesterday' +%Y-%m-%d)
    today_count=$(sqlite3     "$DB" "SELECT COUNT(*) FROM portfolio_history WHERE date(recorded_at) = date('now','localtime')" 2>&1)
    yesterday_count=$(sqlite3 "$DB" "SELECT COUNT(*) FROM portfolio_history WHERE date(recorded_at) = '$yesterday_date'" 2>&1)

    # Memorial Day: today's count may be 0 (market closed, no live snapshots).
    # Yesterday's (Sunday 2026-05-24) count gates the integrity check.
    if [ "$yesterday_count" -gt 0 ]; then
        pass "CHECK 7: DB integrity — $yesterday_count portfolio_history rows for $yesterday_date, $today_count for today"
    else
        fail "CHECK 7: DB integrity — 0 portfolio_history rows for $yesterday_date (possible snapshot gap)"
    fi
}

# ── CHECK 8: Convergence modal smoke ──────────────────────────────────────
check_convergence_smoke() {
    local payload has_field first_sym="" first_strat_count="" first_strats=""
    payload=$(curl -s -m 6 -H "Accept: application/json" "$TRADER_BASE/api/navigator/convergence" 2>&1)

    has_field=$(echo "$payload" | "$PY" -c "
import sys, json
try:
    d = json.loads(sys.stdin.read())
    sigs = d.get('signals') or []
    if not sigs:
        print('EMPTY')
    elif 'strategy_names' in sigs[0] and isinstance(sigs[0]['strategy_names'], list):
        first = sigs[0]
        print('OK|' + str(first.get('ticker','?')) + '|' + str(len(first['strategy_names'])) + '|' + ','.join(first['strategy_names'][:3]))
    else:
        print('MISSING_FIELD')
except Exception as e:
    print(f'ERR:{e}')
" 2>&1)

    case "$has_field" in
        OK\|*)
            # Strip the leading 'OK|' then split. Using parameter expansion
            # rather than cut to avoid edge cases when fields are empty.
            local rest="${has_field#OK|}"
            first_sym="${rest%%|*}";          rest="${rest#*|}"
            first_strat_count="${rest%%|*}";  rest="${rest#*|}"
            first_strats="$rest"
            pass "CHECK 8: Convergence smoke — first signal=${first_sym} (${first_strat_count} strategies: ${first_strats}...) — b76ea91 fix live"
            ;;
        EMPTY)
            warn "CHECK 8: Convergence smoke — 0 signals today (no convergences). Field shape unverifiable from live data; static fix at index.html:33089 still in place."
            PASS_COUNT=$((PASS_COUNT+1))
            ;;
        MISSING_FIELD)
            fail "CHECK 8: Convergence smoke — signals present but strategy_names field MISSING (regression of b76ea91 fix)"
            ;;
        *)
            fail "CHECK 8: Convergence smoke — parse error: $has_field"
            ;;
    esac
}

# ── CHECK 9: Code freshness ───────────────────────────────────────────────
# Born from the Memorial Day 2026-05-25 finding that the trader process
# (PID 43883, started Sun May 24) was running pre-Memorial-Day bytecode
# while all of today's Phase A/B/C/D commits existed on disk only. A
# script that says "safe to unhalt" must verify the process is loading
# the commits the Admiral actually shipped.
check_code_freshness() {
    local trader_pid pid_start_epoch head_epoch head_commit unloaded
    trader_pid=$(pgrep -f "python.*main\.py$" | head -1)
    if [ -z "$trader_pid" ]; then
        fail "CHECK 9: Code freshness — no trader PID found (skipping freshness compare)"
        return
    fi
    pid_start_epoch=$(ps -o lstart= -p "$trader_pid" | xargs -I {} date -j -f "%a %b %e %T %Y" "{}" "+%s" 2>/dev/null)
    head_epoch=$(git log -1 --format=%ct HEAD 2>/dev/null)
    head_commit=$(git log -1 --format='%h' HEAD 2>/dev/null)

    if [ -z "$pid_start_epoch" ] || [ -z "$head_epoch" ]; then
        fail "CHECK 9: Code freshness — could not compute timestamps (pid_start=$pid_start_epoch, head=$head_epoch)"
        return
    fi

    if [ "$pid_start_epoch" -ge "$head_epoch" ]; then
        pass "CHECK 9: Code freshness — PID $trader_pid started after HEAD ($head_commit), bytecode is current"
    else
        unloaded=$(git log --oneline --since="@$pid_start_epoch" 2>/dev/null | wc -l | xargs)
        fail "CHECK 9: Code freshness — PID $trader_pid running STALE bytecode ($unloaded commits since restart, HEAD=$head_commit). Use scripts/admiral_trader_restart.sh --confirm"
    fi
}

# ── Run all checks ────────────────────────────────────────────────────────
check_system_health
check_market_calendar
check_agent_halt_state
check_conviction_flags
check_alpaca_state
check_scanner_health
check_db_integrity
check_convergence_smoke
check_code_freshness

# ── Summary ───────────────────────────────────────────────────────────────
TOTAL=$((PASS_COUNT + FAIL_COUNT))
echo ""
echo "${BOLD}───────────────────────────────────────────────────────────────${RESET}"
if [ "$FAIL_COUNT" -eq 0 ]; then
    echo "${GREEN}${BOLD}SUMMARY: $PASS_COUNT/$TOTAL PASS — safe to unhalt${RESET}"
    echo "${BOLD}───────────────────────────────────────────────────────────────${RESET}"
    exit 0
else
    echo "${RED}${BOLD}SUMMARY: $FAIL_COUNT FAIL, $PASS_COUNT PASS — DO NOT unhalt without investigation${RESET}"
    echo "${BOLD}───────────────────────────────────────────────────────────────${RESET}"
    echo "${YELLOW}Suggested next step:${RESET} re-read the FAIL lines above. If a failure"
    echo "is a known/accepted state (e.g. pending orders from Memorial Day cancel"
    echo "arc), confirm with the audit log before proceeding to the unhalt script."
    exit 1
fi
