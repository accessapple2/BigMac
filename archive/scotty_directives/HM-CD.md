# 🔧 SCOTTY — HM-CD: Scan Cycle Polygon Migration
### Opus 4.7 · The big follow-up to HM-CB · Bound the 31-min cycle

> **Captain's orders, Mr. Scott:** HM-CB made candles fast (30s → 0.5s). HM-EQ closed the equity-snapshot gap. But the underlying root cause remains: 668 symbols × 2.8s/symbol = 31-min scan cycle in ai_brain.py. Find what's slow per symbol now (since candles is fast), migrate the bottleneck(s) to Polygon if applicable, target sub-15-min full cycle.

## Phase CD.0 — Discovery (heavy; budget 15-20 min)

```bash
echo "── Per-symbol scan loop entry point ──"
grep -n "for.*symbol\|for sym in" engine/ai_brain.py | head -10

echo ""
echo "── What does each per-symbol iteration call? Time the SLOW path ──"
echo "(Profile a single iteration directly to identify the bottleneck)"
venv/bin/python3 << 'PY'
import sys, time
sys.path.insert(0, '.')

# Import the per-symbol scan function (Scotty: identify actual function name from grep above)
from engine import ai_brain
# Profile each sub-call separately for a representative symbol
sym = "IBM"

# Each candidate sub-call gets timed individually
import cProfile, pstats, io
pr = cProfile.Profile()
pr.enable()

# CALL THE PER-SYMBOL SCAN FUNCTION HERE
# (Scotty: substitute the actual function name)

pr.disable()
s = io.StringIO()
ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
ps.print_stats(20)
print(s.getvalue())
PY

echo ""
echo "── yfinance call sites still in scan loop after HM-BL-broad ──"
grep -rn "yf\.\|yfinance" engine/ai_brain.py | head -10
```

Document `data/scotty_hm_cd_discovery.md`:
- Per-symbol iteration profile (top 10 cumulative-time functions)
- yfinance call sites still in hot path (would benefit from Polygon migration)
- DB query sites in hot path (would benefit from indexing or caching)
- Network call sites (Polygon, Alpaca, internal)
- Q1: which 2-3 bottlenecks are highest-impact to attack first?

**HALT** for Captain to confirm scope.

## Phase CD.1 — Implement (after Captain confirms scope)

Migrate the top 2-3 bottlenecks identified in CD.0. Each gets its own anchor `# === HM-CD.<bottleneck-name> ===`.

Likely candidates based on what we know:
- Per-symbol yfinance fundamental fetches → swap to Polygon `/v3/reference/tickers/{sym}`
- Per-symbol options chain prep → swap to Polygon `/v3/snapshot/options/{sym}` (Options Starter)
- Per-symbol DB queries → add indexes if missing

Single commit per migration. Compile via venv/bin/python3.

## Phase CD.C — Verify (BEFORE PUSH)

```bash
venv/bin/python3 -c "
# Profile the per-symbol scan again after fixes
# Expected: top-line cumulative time drops 50-80% per symbol
"
```

If per-symbol time drops materially (>40%): proceed to push.

If marginal improvement: HALT, document, propose different attack vector.

## Phase CD.D — Push + restart + 30-min soak verification (INLINE)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 30
NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')

echo ""
echo "── 30-min soak — measure actual cycle time post-fix ──"
sleep 1800
echo ""
echo "First full cycle completion log line (should be earlier than 31min mark):"
grep "Session: MARKET\|cycle.*complete\|scan.*complete" ~/autonomous-trader/logs/trader.log | tail -5
```

ntfy: `🏁 HM-CD complete — scan cycle <new wall>, N migrations shipped`.

## Phase CD.E — Closure

`data/scotty_hm_cd_report.md` with: before/after profile, migrations shipped, cycle time delta, parked follow-ups.
