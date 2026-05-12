# 🔧 SCOTTY — HM-BD.G + HM-BD.E: Cache Fix + Scanner Rewrite
### Cold-Path Completion Duo · Opus 4.7 · Discover → Diff → Apply → Verify

> **Captain's orders, Mr. Scott:** Two follow-ups you carved during HM-BD. **HM-BD.G** — your finding that `@timed_cache(300)` writes entries with `time = call_start` so it never warms when generation > TTL (~5 LOC, broadest impact). **HM-BD.E** — rewrite `scan_premarket_gaps()` over `get_bulk_snapshots(get_active_universe())` to kill the 6-minute serial-fetch cold path that the dashboard frontend still pays. Ship as a duo — same context, same file knowledge, one push + restart.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Two related sub-epics, each its own commit.

Mission:
- **BDGE.0** — Discovery for both. NO writes.
- **BDGE.1** — HM-BD.G `@timed_cache` completion-timestamp fix (smaller, ship first).
- **BDGE.2** — HM-BD.E `scan_premarket_gaps()` batched-snapshot rewrite.
- **BDGE.C** — Static verify + cold-path timing.
- **BDGE.D** — Closure + git push + service restart + post-restart cold-path verification (NEW WORKFLOW — inline).

---

## Pre-flight

```bash
cd ~/autonomous-trader

echo "── Prerequisites ──"
git log origin/main --oneline | grep -iE "HM-BD\b|HM-BHBI" | head -5

echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1

echo ""
echo "── Working tree clean ──"
git status --short
```

---

## Standing Rules

1. Sacred DBs: read-only this directive. No `.db` writes.
2. Diff-then-apply for all code edits.
3. One commit per sub-epic (BD.G first, BD.E second).
4. NTFY on commit: `curl -d "✅ HM-BD.X: <one-line>" https://ntfy.sh/ollietrades-admin`.
5. NEW WORKFLOW: git push + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` + post-restart verify INLINE in BDGE.D.
6. HALT after BDGE.0 for Captain confirmation. HALT on errors.

---

## Phase BDGE.0 — Discovery (NO writes)

### HM-BD.G side: @timed_cache structural bug

```bash
echo "── 1. timed_cache implementation ──"
grep -rn "def timed_cache\|class TimedCache\|@timed_cache" dashboard/app.py engine/ 2>/dev/null | head -10

echo ""
echo "── 2. Show the decorator body (~380-396 per your BD.0 finding) ──"
sed -n '370,410p' dashboard/app.py

echo ""
echo "── 3. How many callers depend on @timed_cache? ──"
grep -rn "@timed_cache" dashboard/ engine/ main.py 2>/dev/null | head -20
```

Document:
- Exact decorator implementation
- Where the timestamp is written (call_start vs call_complete)
- Proposed fix: write the cache entry AFTER the wrapped function returns, using `time.time()` at that point
- All callers (so we can sanity check completion-time semantics breaks nothing)
- Captain Q1: any caller that depends on cache-staleness being timed from call_start specifically? (Should be no — that's the bug. But confirm.)

### HM-BD.E side: scan_premarket_gaps batched rewrite

```bash
echo "── 4. Current scan_premarket_gaps implementation ──"
grep -n "def scan_premarket_gaps\|def get_active_universe" engine/premarket_scanner.py engine/market_data.py 2>/dev/null

echo ""
echo "── 5. Full body of scan_premarket_gaps ──"
sed -n '15,80p' engine/premarket_scanner.py

echo ""
echo "── 6. get_bulk_snapshots interface (what does it return per symbol?) ──"
sed -n '318,370p' engine/market_data.py

echo ""
echo "── 7. /api/momentum/premarket implementation (the fast reference) ──"
grep -n "def.*momentum.*premarket\|momentum/premarket" dashboard/app.py | head -10

echo ""
echo "── 8. Current response shape of /api/premarket-gaps ──"
curl -s http://localhost:8080/api/premarket-gaps 2>/dev/null | python3 -c "import sys,json; d=json.load(sys.stdin); print('Keys:', list(d.keys())); print('Sample gap entry:', d.get('gaps',[{}])[0] if d.get('gaps') else 'empty')" 2>/dev/null
```

Document:
- Current scanner: symbol count, per-symbol fetches, filter that selects "gap rows", current output shape
- get_bulk_snapshots return shape — does it include prev_close and current/premarket price, or do we derive?
- gap_pct derivation: typically `(premarket_price - prev_close) / prev_close * 100`
- Direction: gap_up if gap_pct > 0 else gap_down
- Filter threshold: what makes a row "qualify" as a gap in current code
- Universe match: does `get_active_universe()` cover the same set as the current scanner's universe? If not, flag the diff for Captain.
- Captain Q2: universe coverage — use get_active_universe() as-is, OR use scanner's existing universe source via batched snapshots?
- Captain Q3: gap filter threshold — preserve current threshold, or document and propose tuning?

### Discovery report

Write `data/scotty_hm_bdge_report.md` with sections for both sub-epics, recommendations, captain questions.

**HALT.** ntfy: `📋 HM-BDGE discovery complete`.

---

## Phase BDGE.1 — HM-BD.G fix (@timed_cache completion timestamp)

After Captain confirms Q1:
- Diff with `# === HM-BD.G ===` anchor on dashboard/app.py
- The fix: ensure the cache entry's timestamp is set AFTER the wrapped function returns. Typical pattern:
```
  # Before (buggy):
  t0 = time.time()
  cache[key] = (t0, result_placeholder)
  result = func(*args)
  
  # After (fixed):
  result = func(*args)
  cache[key] = (time.time(), result)
```
- Apply, compile: `python3 -c "import py_compile; py_compile.compile('dashboard/app.py', doraise=True); print('clean')"`
- Smoke: call any cached endpoint twice rapidly, confirm second call hits cache
- Commit: `fix(cache): HM-BD.G — write @timed_cache entry at completion time, not call-start`
- ntfy

---

## Phase BDGE.2 — HM-BD.E scanner rewrite

After Captain confirms Q2 + Q3:
- Diff with `# === HM-BD.E ===` anchor on engine/premarket_scanner.py
- Replace serial loop with batched call:
  - Use `get_active_universe()` (or whichever Q2 approved)
  - Call `get_bulk_snapshots(symbols)` once
  - Iterate snapshots → compute gap_pct, direction, apply filter
  - Return same shape as before: `{gaps: [{symbol, direction, gap_pct, premarket_price, prev_close, scanned_at}]}` to preserve dashboard compatibility
- Preserve any ancillary fields (odte_candidate, etc) if present
- Apply, compile check
- Smoke (cold-path timing):
```
  python3 -c "
  import sys; sys.path.insert(0, '.')
  from engine.premarket_scanner import scan_premarket_gaps
  import time
  t0 = time.time()
  result = scan_premarket_gaps()
  print(f'direct call: {time.time()-t0:.3f}s, {len(result)} gaps')
  "
```
- Commit: `perf(scanner): HM-BD.E — rewrite scan_premarket_gaps over get_bulk_snapshots (kills cold path)`
- ntfy

---

## Phase BDGE.C — Static verify

```bash
echo "── HM-BD.G + HM-BD.E anchors ──"
grep -n "HM-BD.G\|HM-BD.E" dashboard/app.py engine/premarket_scanner.py 2>/dev/null | head -10

echo ""
echo "── Compile ──"
python3 -c "
import py_compile
for f in ['dashboard/app.py', 'engine/premarket_scanner.py']:
    py_compile.compile(f, doraise=True)
    print(f'  {f}: clean')
"

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy: `✅ HM-BDGE verify clean`.

---

## Phase BDGE.D — Closure + push + restart + cold-path verification (INLINE)

1. Append closure section to data/scotty_hm_bdge_report.md with commits, cache fix verification, scanner cold-path timing pre/post, expected dashboard behavior.

2. Commit the closure doc.

3. NEW WORKFLOW — push + restart + verify inline:
```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 8
echo "── Post-restart trader PID + port ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1

echo ""
echo "── COLD-PATH VERIFICATION (the moment of truth) ──"
echo "First call should complete in seconds, not minutes:"
time curl -s -o /tmp/pmg_cold.json -w "HTTP %{http_code}\n" --max-time 30 http://localhost:8080/api/premarket-gaps

echo ""
echo "Second call (cache hit):"
time curl -s -o /tmp/pmg_warm.json -w "HTTP %{http_code}\n" --max-time 5 http://localhost:8080/api/premarket-gaps

echo ""
echo "── Gap count parity ──"
python3 -c "
import json
cold = json.load(open('/tmp/pmg_cold.json'))
warm = json.load(open('/tmp/pmg_warm.json'))
print(f'cold: {len(cold.get(\"gaps\",[]))} gaps')
print(f'warm: {len(warm.get(\"gaps\",[]))} gaps')
print(f'match: {len(cold.get(\"gaps\",[])) == len(warm.get(\"gaps\",[]))}')
"
```

4. Report: cold-path time (was 6+ min, now < 5s expected), warm-path time (< 100ms expected), gap count parity vs 244 baseline.

ntfy: `🏁 HM-BDGE complete — dashboard cold-path killed`.
