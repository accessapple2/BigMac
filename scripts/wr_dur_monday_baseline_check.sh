#!/bin/bash
# HM-SEASON-ROTATION-BLANKET-REACTIVATE follow-up (2026-07-18): one-shot
# check comparing Monday 2026-07-20's [WR-DUR] war-room cycle wall-times
# against the pre-fix baseline (p50=298.6s, p95=777.7s, last-4-day sample
# as of 2026-07-18 — see data/reports/relay/
# relay_2026-07-18_season-rotation-halt-reset-correction.md). The season-
# rotation halt_mode bug had inflated the active fleet from ~11-12 to 77
# agents; this checks whether cycle times recovered post-fix. NTFY-only —
# no auto-commit/push (doctrine: no auto-push, manual push only). Fires
# once via a same-day cron entry, safe to leave in crontab afterward since
# the date match will not recur for a year.
set -uo pipefail

REPO="$HOME/autonomous-trader"
cd "$REPO"

NTFY_TOPIC="ollietrades-admin"
NTFY_URL="https://ntfy.sh/$NTFY_TOPIC"
LOG="$REPO/logs/wr_dur_monday_baseline_check.log"
mkdir -p "$(dirname "$LOG")"
exec >>"$LOG" 2>&1
echo "=== $(date -Iseconds) wr_dur_monday_baseline_check START ==="

TARGET_DATE="2026-07-20"

ntfy_post() {
    local prio="$1"; shift
    curl -s -H "Priority: $prio" -d "$*" "$NTFY_URL" >/dev/null 2>&1 || true
}

RESULT=$(grep "^\[$TARGET_DATE" logs/trader.log 2>/dev/null | grep "\[WR-DUR\]" | \
    grep -oE 'cycle wall=[0-9.]+s' | grep -oE '[0-9.]+' | \
    .venv/bin/python3 -c "
import sys
vals = sorted(float(x) for x in sys.stdin.read().split())
n = len(vals)
if n == 0:
    print('NO_SAMPLES')
    sys.exit(0)
def pct(p):
    if n == 1:
        return vals[0]
    k = (n - 1) * p
    f = int(k)
    c = min(f + 1, n - 1)
    if f == c:
        return vals[f]
    return vals[f] + (vals[c] - vals[f]) * (k - f)
p50 = pct(0.50)
p95 = pct(0.95)
print(f'{n}|{p50:.1f}|{p95:.1f}|{min(vals):.1f}|{max(vals):.1f}')
")

BASELINE_P50=298.6
BASELINE_P95=777.7

if [ "$RESULT" = "NO_SAMPLES" ] || [ -z "$RESULT" ]; then
    echo "No [WR-DUR] samples found for $TARGET_DATE yet."
    ntfy_post "default" "HM-WR-DUR-MONDAY-CHECK: no [WR-DUR] cycle samples logged yet for $TARGET_DATE (market may not be open / war room hasn't run a cycle). Re-run scripts/wr_dur_monday_baseline_check.sh later today."
    exit 0
fi

N=$(echo "$RESULT" | cut -d'|' -f1)
P50=$(echo "$RESULT" | cut -d'|' -f2)
P95=$(echo "$RESULT" | cut -d'|' -f3)
MIN=$(echo "$RESULT" | cut -d'|' -f4)
MAX=$(echo "$RESULT" | cut -d'|' -f5)

echo "Samples: $N | p50=${P50}s p95=${P95}s | min=${MIN}s max=${MAX}s"
echo "Baseline (pre-fix, 4-day sample as of 2026-07-18): p50=${BASELINE_P50}s p95=${BASELINE_P95}s"

VERDICT=$(.venv/bin/python3 -c "
p50, p95 = $P50, $P95
bp50, bp95 = $BASELINE_P50, $BASELINE_P95
if p50 <= bp50 * 1.1 and p95 <= bp95 * 1.1:
    print('RECOVERED')
elif p50 <= bp50 * 1.5 and p95 <= bp95 * 1.5:
    print('MIXED')
else:
    print('REGRESSED')
")

echo "Verdict: $VERDICT"

MSG="HM-WR-DUR-MONDAY-CHECK ($TARGET_DATE, n=$N): p50=${P50}s p95=${P95}s vs baseline p50=${BASELINE_P50}s p95=${BASELINE_P95}s -> $VERDICT (post season-rotation halt_mode fix, fleet 77->11-12 agents on 2026-07-18)"
if [ "$VERDICT" = "REGRESSED" ]; then
    ntfy_post "high" "$MSG"
else
    ntfy_post "default" "$MSG"
fi

echo "=== $(date -Iseconds) wr_dur_monday_baseline_check END ==="
