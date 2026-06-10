#!/usr/bin/env bash
# HM-FORGE Phase 1.5 — quiet-window upgrade + bake-off + shadow-restart.
# Runs ON bigmac (cron), SSHes to olliemax. Self-guarding, self-disarming.
#
#   bash scripts/hm_forge_phase15.sh --check   # guards only, NO actions (dry-run)
#   bash scripts/hm_forge_phase15.sh           # full window run (cron fires this)
#
# ── SUDO REALITY (verified 2026-06-10) ───────────────────────────────────────
# .168 has NO passwordless sudo (`sudo -n` => interactive auth required). So the
# Ollama UPGRADE (install.sh) and any systemd drop-in re-apply CANNOT run
# unattended. This script therefore:
#   * does the full NO-SUDO subset autonomously (snapshot, smoke, bench,
#     trader restart -> activates the conviction-stop SHADOW, commit/push),
#   * NTFYs the EXACT manual upgrade command at START and again before the
#     bench HOLD, so the Captain can key it live during the 13:07->13:45 window,
#   * re-checks for gemma4 after the hold: 3-way bench if the upgrade landed,
#     else 2-way (plutus-v1 vs gpt-oss:20b) with a logged note.
# We never touch the ollama service here, so there is nothing to "leave down".
set -uo pipefail
# cron has a minimal env — make ssh/curl/git/lsof/python resolvable
export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin:$PATH"

REPO="$HOME/autonomous-trader"
LOG="$REPO/logs/hm_forge_phase15_$(date +%F).log"
DONE="/tmp/hm_forge_phase15.done"
SNAP="/tmp/hm_forge_pre_upgrade.txt"
TOPIC="ollietrades-admin"
HOST="olliemax"
FLEET=(gpt-oss:20b qwen3.5:9b deepseek-r1:14b qwen3:14b plutus-v1:latest \
       gemma3:4b ministral-3:3b qwen2.5-coder:7b qwen3:8b)
UPGRADE_CMD="ssh -t $HOST 'curl -fsSL https://ollama.com/install.sh | sh'   # then re-verify FA/KV drop-in"
CHECK=0; [ "${1:-}" = "--check" ] && CHECK=1

mkdir -p "$REPO/logs"
log(){ echo "[$(date -u +%FT%TZ)] $*" | tee -a "$LOG"; }
ntfy(){ curl -s -m 10 -H "Title: HM-FORGE 1.5" -d "$1" "https://ntfy.sh/$TOPIC" >/dev/null 2>&1 || true; }
anomaly(){ log "ANOMALY: $*"; ntfy "HM-FORGE 1.5 ANOMALY: $*"; }
die(){ anomaly "$*"; log "safe-stop (ollama untouched; rollback if needed: restart ollama + ./scripts/trader_restart.sh)"; exit 1; }

# ── GUARDS ───────────────────────────────────────────────────────────────────
guards(){
  local az_h az_m dow rc
  dow=$(TZ=America/Phoenix date +%u)            # 1-5 = Mon-Fri
  az_h=$(TZ=America/Phoenix date +%H); az_m=$(TZ=America/Phoenix date +%M)
  [ "$dow" -le 5 ] || { echo "GUARD FAIL: weekend (dow=$dow)"; return 1; }
  if [ "$az_h" -lt 13 ] || { [ "$az_h" -eq 13 ] && [ "10#$az_m" -lt 2 ]; }; then
    echo "GUARD FAIL: before 13:02 AZ ($az_h:$az_m)"; return 1
  fi
  [ -f "$DONE" ] && { echo "GUARD FAIL: done-marker present ($DONE)"; return 1; }
  # Polygon market-closed (best-effort; falls back to time guard if no key)
  [ -f "$REPO/.env" ] && . "$REPO/.env" 2>/dev/null || true
  if [ -n "${POLYGON_API_KEY:-}" ]; then
    local ms; ms=$(curl -s -m 10 "https://api.polygon.io/v1/marketstatus/now?apiKey=$POLYGON_API_KEY" 2>/dev/null)
    if echo "$ms" | grep -qiE '"market"[: ]+"closed"'; then :; \
    elif echo "$ms" | grep -qiE '"market"[: ]+"open"'; then echo "GUARD FAIL: Polygon says market OPEN"; return 1; \
    else echo "GUARD WARN: Polygon status indeterminate, using time guard"; fi
  else
    echo "GUARD WARN: no POLYGON_API_KEY, using time guard (post-13:02 AZ weekday)"
  fi
  echo "GUARDS PASS (az=$az_h:$az_m dow=$dow)"; return 0
}

if [ "$CHECK" = 1 ]; then
  echo "=== HM-FORGE 1.5 --check (guards only, no actions) ==="; guards; exit $?
fi

# ── FULL RUN ─────────────────────────────────────────────────────────────────
guards || die "guards failed: $(guards 2>&1 | tail -1)"
cd "$REPO" || die "cannot cd $REPO"
log "=== HM-FORGE 1.5 window START ==="
ntfy "HM-FORGE 1.5 window START. MANUAL UPGRADE (no passwordless sudo on .168): $UPGRADE_CMD"

# a/b snapshot
ssh "$HOST" 'ollama --version; echo "---"; ollama list' > "$SNAP" 2>&1 || die "snapshot ssh failed"
log "snapshot -> $SNAP ($(ssh $HOST 'ollama --version'))"

# c upgrade — SUDO-GATED, cannot run unattended. Surface, do not fail.
if ssh "$HOST" 'sudo -n true' 2>/dev/null; then
  log "passwordless sudo present — upgrading ollama"
  ssh -t "$HOST" 'curl -fsSL https://ollama.com/install.sh | sh' 2>&1 | tee -a "$LOG" || anomaly "upgrade attempt failed"
else
  log "SKIP upgrade: no passwordless sudo on .168. Manual: $UPGRADE_CMD"
  ntfy "HM-FORGE 1.5: ollama upgrade NEEDS your sudo on .168 — key it now to unblock gemma4 + 3-way. $UPGRADE_CMD"
fi

# d drop-in survival (read-only; no sudo needed to inspect)
DROPIN=$(ssh "$HOST" 'systemctl show ollama --property=Environment' 2>/dev/null)
for k in 'OLLAMA_HOST=0.0.0.0:11434' OLLAMA_FLASH_ATTENTION=1 OLLAMA_KV_CACHE_TYPE=q8_0; do
  echo "$DROPIN" | grep -q "$k" || anomaly "drop-in missing $k (re-apply needs sudo: ssh -t $HOST 'sudo bash /tmp/ollama_tier1_perf.sh')"
done

# e fleet present
LIST=$(ssh "$HOST" 'ollama list' 2>/dev/null)
for m in "${FLEET[@]}"; do echo "$LIST" | grep -q "${m%%:*}" || anomaly "fleet model missing: $m"; done
log "fleet list verified (${#FLEET[@]} expected)"

# f smoke each fleet model (short generate)
for m in "${FLEET[@]}"; do
  if ssh "$HOST" "ollama run '$m' 'reply OK' --keepalive 0s" >/dev/null 2>&1; then :; else anomaly "smoke fail: $m"; fi
done
log "fleet smoke done"

# g gemma4 pull (works only if upgrade landed; 412 otherwise — note, don't fail)
if ssh "$HOST" 'ollama pull gemma4:12b-it-qat' >/dev/null 2>&1; then
  log "gemma4:12b-it-qat pulled OK"; GEMMA=1
else
  log "gemma4:12b-it-qat still blocked (412 / upgrade not applied) — bench will be 2-way"; GEMMA=0
fi

# h HOLD until 13:45 AZ (clears Kirk after_close 13:15 ollama fallback), then bench
ntfy "HM-FORGE 1.5: holding to 13:45 AZ for bench. Last chance to key the upgrade: $UPGRADE_CMD"
while :; do h=$(TZ=America/Phoenix date +%H); m=$(TZ=America/Phoenix date +%M); \
  { [ "$h" -gt 13 ] || { [ "$h" -eq 13 ] && [ "10#$m" -ge 45 ]; }; } && break; sleep 60; done
# re-check gemma4 in case the Captain keyed the upgrade during the hold
if [ "$GEMMA" = 0 ] && ssh "$HOST" 'ollama pull gemma4:12b-it-qat' >/dev/null 2>&1; then GEMMA=1; log "gemma4 landed during hold — 3-way"; fi
MODELS="plutus-v1:latest,gpt-oss:20b"; [ "$GEMMA" = 1 ] && MODELS="$MODELS,gemma4:12b-it-qat"
scp -q scripts/hm_forge_bench.py "$HOST:/tmp/" || die "scp bench failed"
SCORE=$(ssh "$HOST" "python3 /tmp/hm_forge_bench.py --models $MODELS --runs 5" 2>&1) || anomaly "bench run errored"
{ echo; echo "<!-- appended by hm_forge_phase15.sh $(date -u +%FT%TZ) ($([ "$GEMMA" = 1 ] && echo 3-way || echo 2-way)) -->"; echo "$SCORE"; } >> docs/HM-FORGE-PHASE0-1-REPORT.md
log "bench appended to docs/HM-FORGE-PHASE0-1-REPORT.md ($MODELS)"

# i trader restart (activates conviction-stop SHADOW) + verify
./scripts/trader_restart.sh 2>&1 | tee -a "$LOG" || anomaly "trader_restart returned nonzero"
sleep 45
WRITERS=$(lsof "$REPO/logs/trader.log" 2>/dev/null | grep -c Python || echo 0)
[ "$WRITERS" -ge 1 ] || anomaly "trader not writing trader.log after restart (writers=$WRITERS)"
pgrep -f "$REPO/main.py" >/dev/null || pgrep -f main.py >/dev/null || anomaly "main.py not running post-restart"
log "trader restart verified (log writers=$WRITERS)"

# j ghost_conviction_stops table exists post-restart
.venv/bin/python -c "import sqlite3,os; d=os.path.expanduser('~/autonomous-trader/data/trader.db'); \
c=sqlite3.connect(d); n=[r[0] for r in c.execute(\"select name from sqlite_master where type='table'\")]; \
print('ghost_conviction_stops:', 'ghost_conviction_stops' in n)" 2>&1 | tee -a "$LOG" | grep -q "True" \
  || log "note: ghost_conviction_stops not yet created (created lazily on first would-fire eval)"

# k commit (surgical) + push if unpushed > 5
git add docs/HM-FORGE-PHASE0-1-REPORT.md 2>/dev/null
git commit -q -m "chore: HM-FORGE 1.5 window — append bake-off scorecard (S6)" 2>/dev/null && log "scorecard committed" || log "nothing to commit"
AHEAD=$(git log --oneline @{u}..HEAD 2>/dev/null | wc -l | tr -d ' ')
if [ "${AHEAD:-0}" -gt 5 ]; then git push 2>&1 | tee -a "$LOG"; log "pushed ($AHEAD ahead)"; else log "hold push ($AHEAD ahead, <=5)"; fi

# l disarm + final NTFY
touch "$DONE"
ANOM=$(grep -c "ANOMALY:" "$LOG" 2>/dev/null || echo 0)
RESULT=$([ "$ANOM" = 0 ] && echo PASS || echo "PARTIAL ($ANOM anomalies)")
ntfy "HM-FORGE 1.5 window DONE: $RESULT. bench=$([ "$GEMMA" = 1 ] && echo 3-way || echo 2-way). $([ "$ANOM" != 0 ] && grep 'ANOMALY:' "$LOG" | tail -3)"
log "=== HM-FORGE 1.5 window END: $RESULT ==="
