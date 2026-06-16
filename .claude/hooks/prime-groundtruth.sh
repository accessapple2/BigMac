#!/usr/bin/env bash
# HM-PRIME Part B — SessionStart ground-truth primer.
# READ-ONLY. Prints a ~5-line live summary of ai_players so the session starts
# grounded in real DB state (not the stale roster counts in CLAUDE.md).
# Safety: opens the DB with mode=ro (no write lock; safe on the live WAL DB).
# Never writes, never blocks session start (always exits 0).
set -uo pipefail

DB="/Users/bigmac/autonomous-trader/data/trader.db"   # verified path (NOT the empty ./trader.db decoy)

# Gate: only emit inside the trader project, so other-project sessions aren't taxed.
payload="$(cat 2>/dev/null || true)"
case "${payload}:${PWD}" in
  *autonomous-trader*) : ;;
  *) exit 0 ;;
esac

[ -r "$DB" ] || exit 0
RO="file:${DB}?mode=ro"
q() { sqlite3 -readonly -cmd ".timeout 250" "$RO" "$1" 2>/dev/null; }

halt=$(q "SELECT group_concat(halt_mode||'='||c,'  ') FROM (SELECT halt_mode, COUNT(*) c FROM ai_players GROUP BY halt_mode ORDER BY halt_mode);")
total=$(q "SELECT COUNT(*) FROM ai_players;")
amods=$(q "SELECT group_concat(model_id||'='||c,' ') FROM (SELECT model_id, COUNT(*) c FROM ai_players WHERE halt_mode='active' GROUP BY model_id HAVING c>1 ORDER BY c DESC);")
singles=$(q "SELECT COUNT(*) FROM (SELECT model_id FROM ai_players WHERE halt_mode='active' GROUP BY model_id HAVING COUNT(*)=1);")

[ -z "$halt" ] && exit 0   # DB unreadable / unexpected — stay silent rather than guess

printf -- '── ground truth · ai_players @ data/trader.db (read-only) ──\n'
printf -- 'halt_mode: %s   (total %s)\n' "$halt" "$total"
printf -- 'active models: %s +%s singletons\n' "$amods" "$singles"
printf -- 'this line is live — if CLAUDE.md states a different active count, it is stale.\n'
exit 0
