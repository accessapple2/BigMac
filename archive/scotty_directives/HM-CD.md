# 🔧 SCOTTY — HM-CD: Scan Cycle Polygon Migration
### Opus 4.7 · Discovery → ship migrations · Bound the 31-min cycle

> **Captain's orders, Mr. Scott:** HM-CB made candles fast (30s → 0.5s). HM-EQ closed the equity-snapshot gap by decoupling. Root cause that drove those fixes remains: 668 symbols × 2.8s/symbol = 31-min scan cycle in the ai_brain.py main loop. Identify what's slow per symbol NOW (since candles is fast), migrate hot-path bottlenecks to Polygon where applicable, target sub-15-min full cycle.

## Pre-flight

```bash
cd ~/autonomous-trader
git log origin/main --oneline | head -5
git status --short
pgrep -af main.py | head -1
```

## Phase CD.0 — Discovery (heavy; budget 15-20 min)

```bash
echo "── 1. Per-symbol scan loop entry point ──"
grep -n "for.*symbol\|for sym in" engine/ai_brain.py | head -10

echo ""
echo "── 2. What does each per-symbol iteration call? Identify scan function name ──"
grep -nB 2 -A 20 "def scan_symbol\|def process_symbol\|def analyze_symbol\|def _scan_one\|def per_symbol" engine/ai_brain.py | head -60

echo ""
echo "── 3. yfinance call sites still in scan loop after HM-BL-broad ──"
grep -rn "yf\.\|yfinance" engine/ai_brain.py | head -15

echo ""
echo "── 4. DB query sites in scan loop (could need indexes) ──"
grep -rn "sqlite3\.connect\|cursor\.execute\|sql(" engine/ai_brain.py | head -15

echo ""
echo "── 5. Profile a single iteration directly to find bottleneck ──"
venv/bin/python3 << 'PY'
import sys, time, cProfile, pstats, io
sys.path.insert(0, '.')

# Profile per-symbol scan for representative symbol
sym = "IBM"

# Try common scan function names — Scotty: adjust based on grep results above
candidates = []
try:
    from engine.ai_brain import scan_symbol as fn
    candidates.append(('scan_symbol', fn))
except ImportError:
    pass
try:
    from engine.ai_brain import process_symbol as fn
    candidates.append(('process_symbol', fn))
except ImportError:
    pass
try:
    from engine.ai_brain import _scan_one as fn
    candidates.append(('_scan_one', fn))
except ImportError:
    pass

if not candidates:
    print("  ❌ No standard per-symbol function found — Scotty: locate from grep above and inline")
else:
    name, fn = candidates[0]
    print(f"  Profiling {name}('{sym}')...")
    pr = cProfile.Profile()
    pr.enable()
    t0 = time.time()
    try:
        result = fn(sym)
    except Exception as e:
        print(f"  Exception: {type(e).__name__}: {e}")
    dt = time.time() - t0
    pr.disable()
    print(f"  Wall: {dt:.2f}s")
    s = io.StringIO()
    ps = pstats.Stats(pr, stream=s).sort_stats('cumulative')
    ps.print_stats(20)
    print(s.getvalue())
PY

echo ""
echo "── 6. yfinance.Ticker.info / .fast_info / .calendar / .options call sites ──"
grep -rn "\.info\|\.fast_info\|\.calendar\|\.options" engine/ai_brain.py | head -15
```

Document `data/scotty_hm_cd_discovery.md`:
- Per-symbol scan function name + call path
- Top 10 cumulative-time functions from the profile
- yfinance call sites STILL in hot path (after HM-BL-broad)
- DB query sites in hot path  
- Network call sites (Polygon/Alpaca/internal)
- **Q1**: which 2-3 bottlenecks are highest-impact? (Recommend top by cumulative time)

**HALT** for Captain to confirm scope. ntfy.

## Phase CD.1 — Implement migrations (after Captain confirms scope)

Migrate the top 2-3 bottlenecks per Captain's Q1 answer. Each gets its own anchor `# === HM-CD.<bottleneck-name> ===`.

Likely candidates:
- Per-symbol yfinance fundamentals/info → Polygon `/v3/reference/tickers/{sym}` (Stocks Starter)
- Per-symbol options snapshot → Polygon `/v3/snapshot/options/{sym}` (Options Starter)
- Repeated DB query per symbol → add index OR batch query OR cache lookup

Each migration = own commit. Compile via venv/bin/python3.

## Phase CD.C — Verify (BEFORE PUSH)

```bash
echo "── Re-profile per-symbol scan after fixes ──"
venv/bin/python3 -c "
import sys, time
sys.path.insert(0, '.')
# Re-run the profile from CD.0 step 5 against the same symbol
# Expected: top-line cumulative time drops 40%+
"
```

If per-symbol time drops materially (>40%): proceed to push.

If marginal improvement: HALT, document, propose different attack vector.

## Phase CD.D — Push + restart + 30-min soak (INLINE)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 30
NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')
echo "  New PID: $NEW_PID"

echo ""
echo "── 30-min soak — measure actual cycle wall time post-fix ──"
sleep 1800
echo ""
echo "First full cycle completion timing:"
grep "Session: MARKET\|cycle.*complete\|scan.*complete" ~/autonomous-trader/logs/trader.log | tail -5
```

ntfy: `🏁 HM-CD complete — scan cycle <before>→<after>, N migrations shipped`.

## Phase CD.E — Closure

`data/scotty_hm_cd_report.md` with: before/after profile, migrations shipped, cycle time delta, parked follow-ups (any deferred bottlenecks).
