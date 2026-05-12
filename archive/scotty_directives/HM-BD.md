# 🔧 SCOTTY — HM-BD: Premarket-Gaps Cold-Path Latency
### Endpoint Performance · Opus 4.7 · Profile → Decide → Implement → Verify

> **Captain's orders, Mr. Scott:** During HM-BA you found that `/api/premarket-gaps` returns real data (244 gap rows) but its cold path exceeds `ai_brain.py`'s 5s/10s timeouts, leaving two callers silently seeing HTTP 000. Existing `@timed_cache(300)` only helps post-warm. Three remediation paths were parked. This epic profiles the cold path, picks a path, and ships the fix.

---

## Identity & Mission

You are **Scotty** — Claude Code on Opus 4.7. Discovery-heavy epic. Captain decides on remediation path after BD.0 evidence.

Mission:
- **BD.0** — Profile cold-path latency, characterize the 2 `ai_brain.py` callers, compare endpoint shapes. NO writes. Recommend path.
- **BD.1** — Execute the chosen path (one of α/β/γ — see below). One commit.
- **BD.2** — If migration path chosen: shape adapter + caller updates. Otherwise skipped.
- **BD.C** — Static verify + cold-path timing measurement.
- **BD.D** — Closure report.

Candidate paths (Captain decides after BD.0):
- **α — Pre-warm cache at service boot.** Add FastAPI startup hook that fires `/api/premarket-gaps` once after init. Fixes cold path; cheap.
- **β — Reduce scanner workload.** Trim the 244-symbol universe in `engine/premarket_scanner.py` to something tighter (e.g. high-liquidity only) so even cold runs finish under 5s.
- **γ — Migrate callers to `/api/momentum/premarket` with shape adapter.** Leaves `/api/premarket-gaps` alone; redirects the 2 internal consumers.

Each has different tradeoffs. BD.0 evidence determines which.

---

## Pre-flight check

```bash
cd ~/autonomous-trader

echo "── Prerequisites ──"
git log origin/main --oneline | grep -iE "HM-BA|HM-BB|HM-BC" | head -5

echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -3

echo ""
echo "── Working tree clean ──"
git status --short
```

---

## Standing Rules

1. Sacred DBs: **read-only this directive**. NO writes to any `.db`.
2. Sacred directories: no `rm -rf`.
3. Diff-then-apply: unified diff before every code edit.
4. One commit per sub-phase. Each commit independently revertable.
5. NTFY on commit: `curl -d "✅ HM-BD.X: <one-line>" https://ntfy.sh/ollietrades-admin`.
6. Push gate: do NOT push. Stage commits locally.
7. NO service restart. Captain handles it.
8. HALT after each phase and report — Captain confirms before next phase.

---

## Phase BD.0 — Discovery & Profiling (NO writes)

Five evidence buckets. Write `data/scotty_hm_bd_report.md`.

### 1. Cold-path timing (3 measurements)
Restart the in-process cache (or use a curl that bypasses the @timed_cache by clearing it first, if exposed), then measure:

```bash
echo "── Clear cache + warm-up timing ──"
# If a cache-clear endpoint exists, use it. Otherwise: pick a timing approach:
# Option: kick the service to clear in-memory @timed_cache (but Captain hasn't approved a restart)
# Option: hit /api/premarket-gaps?_force=1 if scanner supports bypass
# Option: just time three sequential calls and note if 1st is slow, 2-3 are fast
time curl -s http://localhost:8080/api/premarket-gaps > /tmp/pmg_1.json
time curl -s http://localhost:8080/api/premarket-gaps > /tmp/pmg_2.json
time curl -s http://localhost:8080/api/premarket-gaps > /tmp/pmg_3.json

echo ""
echo "── Response sanity ──"
python3 -c "import json; d=json.load(open('/tmp/pmg_1.json')); print(f'{len(d.get(\"gaps\",[]))} gaps, keys={list(d.keys())}')"
```

Record: 1st call time (cold or warm — note state), 2nd, 3rd. If all warm because cache was already populated, note that and proceed — the 5-min cache TTL means cold path only re-emerges after idle periods.

### 2. Where the time goes
Profile `engine/premarket_scanner.py`:
- How many symbols scanned (confirm 244 figure)
- What's the per-symbol cost (yfinance batch fetch? sequential? thread pool?)
- Is there a slow N+1 pattern?

```bash
grep -n "yfinance\|yf\.\|download\|Ticker\|concurrent\|ThreadPool\|asyncio" engine/premarket_scanner.py | head -20
wc -l engine/premarket_scanner.py
```

### 3. The two ai_brain.py callers
```bash
grep -n "premarket-gaps\|/api/premarket" engine/ai_brain.py dashboard/app.py 2>/dev/null
grep -n "timeout=" engine/ai_brain.py | head -10
```

For each caller record: function, timeout value, what it does with the response (e.g. lookup specific symbol's gap_pct, or use the full list).

### 4. /api/momentum/premarket alternative shape
```bash
curl -s http://localhost:8080/api/momentum/premarket | python3 -m json.tool | head -40
```

Document: response shape, fields available, whether it covers the same symbol universe as `/api/premarket-gaps`, can the 2 ai_brain callers be served by it with a thin adapter.

### 5. Existing cache mechanics
```bash
grep -n "timed_cache\|premarket-gaps\|premarket_gaps" dashboard/app.py | head -20
```

Confirm:
- The 300s TTL
- Whether there's an existing startup pre-warm hook anywhere (e.g. for other endpoints) we can mirror

### Discovery report

```markdown
# HM-BD Discovery

## 1. Cold-path timing
1st call: <Ns>
2nd call: <Ns>
3rd call: <Ns>
Cold/warm state: <which calls were cold>

## 2. Scanner anatomy
Symbols scanned: <N>
Fetch pattern: <yfinance batch / serial / pooled>
Hotspot: <where the seconds go>

## 3. ai_brain callers
Caller A: function <name>, timeout=<Ns>, uses field <X>
Caller B: function <name>, timeout=<Ns>, uses field <X>

## 4. /api/momentum/premarket shape
Fields: <list>
Symbol overlap: <yes/no/partial>
Adapter feasibility: <easy/moderate/hard>

## 5. Existing cache + pre-warm hooks
Current TTL: 300s
Pre-warm hooks present elsewhere: <yes/no, examples>

## Recommended path (α/β/γ) + rationale

## Captain decisions blocking BD.1
Q1: Pick α / β / γ / hybrid?
Q2: <any sub-decisions specific to recommended path>
```

**HALT.** ntfy: `📋 HM-BD discovery complete`.

---

## Phase BD.1 — Implement chosen path

Path-specific. Each ≤1 file change unless adapter scope requires touching ai_brain.

### If α (pre-warm):
- Add FastAPI startup event handler (or schedule-based warmer) in `main.py` or `dashboard/app.py`
- On startup, fire the `/api/premarket-gaps` handler internally (call the underlying function, NOT a self-loopback HTTP request — avoid bootstrapping deadlocks)
- Anchor: `# === HM-BD.1 α pre-warm ===`
- Commit: `perf(dashboard): HM-BD.1 — pre-warm premarket-gaps cache at startup`

### If β (reduce workload):
- Identify the universe source in `engine/premarket_scanner.py`
- Apply a liquidity/volume filter or trim to top-N watchlist
- Document what symbols are dropped (so we know what data is no longer scanned)
- Anchor: `# === HM-BD.1 β workload reduction ===`
- Commit: `perf(scanner): HM-BD.1 — trim premarket scan universe to <criteria>`

### If γ (migrate callers):
- Build adapter in `engine/ai_brain.py` (or a helper) that converts `/api/momentum/premarket` shape → fields the 2 callers need
- Update both call sites to use the new endpoint via adapter
- Leave `/api/premarket-gaps` untouched (still serves dashboard frontend)
- Anchor: `# === HM-BD.1 γ migration ===`
- Commit: `refactor(ai_brain): HM-BD.1 — migrate premarket callers to /api/momentum/premarket`

Compile check:
```bash
python3 -c "import py_compile; [py_compile.compile(f, doraise=True) for f in ['<edited files>']]; print('clean')"
```

ntfy.

---

## Phase BD.2 — Adapter & callers (γ only, else skip)

Only fires if path γ chosen. Same standards as BD.1; second commit covers any ai_brain caller updates if BD.1 only touched the adapter.

ntfy.

---

## Phase BD.C — Static verify + timing

```bash
cd ~/autonomous-trader

echo "── HM-BD anchors ──"
grep -rn "HM-BD" engine/ dashboard/ main.py 2>/dev/null | head -10

echo ""
echo "── Files compile ──"
python3 -c "
import py_compile
for f in ['main.py', 'engine/ai_brain.py', 'engine/premarket_scanner.py', 'dashboard/app.py']:
    try:
        py_compile.compile(f, doraise=True)
        print(f'  {f}: clean')
    except Exception as e:
        print(f'  {f}: FAIL — {e}')
"

echo ""
echo "── Timing verification (post-change, pre-restart so warm cache still applies) ──"
time curl -s http://localhost:8080/api/premarket-gaps > /dev/null

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

Note: real cold-path verification needs a restart, which is Captain's job. BD.C confirms code-level health only.

ntfy: `✅ HM-BD verify clean`.

---

## Phase BD.D — Closure report

Append to `data/scotty_hm_bd_report.md`:

```markdown
## HM-BD Closure

### Path taken
<α/β/γ> — <one-line rationale>

### Commits staged (not pushed)
<output of: git log origin/main..HEAD --oneline>

### Expected cold-path behavior post-restart
<what changes after Captain restarts>

### Restart needed
Yes — Captain runs `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader`.

### Verification plan post-restart
1. First call to /api/premarket-gaps should complete within <Ns>
2. ai_brain.py callers should no longer see HTTP 000

### Out-of-scope follow-ups
<any remaining concerns, e.g. dashboard frontend if path γ chose, observability for cold-path future)
```

ntfy: `🏁 HM-BD complete — ready for Captain push & restart`.
