#!/bin/zsh
# scripts/gex_flow_collect.sh — HM-GEX-COLLECT daily collector (2026-05-31).
#
# Time-based cron (reboot-survivable on this box; NOT launchd — see CLAUDE.md
# "LaunchAgent Reboot Lifecycle"). STANDALONE: no trader/signal-center dependency —
# just Polygon + data/flow_gex.db.
#
# Fires ~12:45 MST = 15:45 ET during EDT (~15 min before the 16:00 ET close), to
# capture the day's GEX + aggregate flow into data/flow_gex.db as the deflation
# substrate for strategies/validation.py. (Box is Arizona/MST no-DST; ET observes
# DST, so in winter this lands ~14:45 EST — still pre-close, acceptable. Daily is
# fine to start per HM-GEX-COLLECT.)
#
# OBSERVATION-ONLY: options_flow_gex imports no order path; nothing trades.
set -u
ROOT=/Users/bigmac/autonomous-trader
PY="$ROOT/.venv-backtest/bin/python3"
LOG="$ROOT/logs/gex_flow_collect.log"
mkdir -p "$ROOT/logs"
cd "$ROOT" || { echo "[$(date '+%F %T %Z')] FATAL cd" >> "$LOG"; exit 1; }

echo "[$(date '+%F %T %Z')] gex_flow collect START" >> "$LOG"
"$PY" - <<'PYEOF' >> "$LOG" 2>&1
import sys
sys.path.insert(0, "/Users/bigmac/autonomous-trader")
import engine.options_flow_gex as E
r = E.collect(("SPY", "QQQ"))
for u, d in r.items():
    g, f = d["gex"], d["flow"]
    print("  %s: GEX flip=%s total=%.2e %s | flow lean=%s net=$%s unusual=%s" % (
        u, g.get("gamma_flip"), g.get("total_gex", 0), g.get("regime"),
        f.get("lean"), f.get("net_notional"), f.get("unusual_count")))
print("collected OK:", list(r.keys()))
PYEOF
echo "[$(date '+%F %T %Z')] gex_flow collect DONE (rc=$?)" >> "$LOG"
