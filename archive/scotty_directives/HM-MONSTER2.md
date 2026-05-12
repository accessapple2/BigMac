# 🔧 SCOTTY — HM-MONSTER2: Halt Resolutions + Chart Preview Polish
### Opus 4.7 · 3 ships · Captain answers pre-baked · single push + restart

> **Captain's orders, Mr. Scott:** Resolve both halt-for-Captain items from MONSTER 1 with pre-decided answers, and add lightweight-charts inline preview to the ticker hover tooltip. Each phase = own commit. Single push + restart at end. Auto-mode is on.

## Pre-flight

```bash
cd ~/autonomous-trader
echo "── MONSTER 1 commits on origin? ──"
git log origin/main --oneline | head -8
echo ""
echo "── Working tree ──"
git status --short
echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1
echo ""
echo "── Halt-for-Captain reports present from MONSTER 1? ──"
ls -la data/scotty_hm_ble_report.md data/scotty_hm_bdf_audit_report.md 2>/dev/null
```

---

## Phase M2.1 — HM-BL.E Option C (delete + harden capitol_fund)

**Pre-decided**: Option C (delete stale ATH row AND harden capitol_fund with listing-status filter).

### Step 1: Execute the SQL handoff (Captain pre-approved)
Run the idempotent archive+delete pattern from `data/scotty_hm_ble_report.md`:

```bash
echo "── Backup before sacred-DB write ──"
TS=$(date +%Y%m%d_%H%M)
cp data/trader.db "data/trader.db.pre-hm-ble-${TS}"

echo ""
echo "── Pre-delete state ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT symbol, qty, market_value, datetime(updated_at, 'localtime') FROM positions WHERE symbol = 'ATH';"

echo ""
echo "── Apply HM-BL.E SQL (from data/scotty_hm_ble_report.md) ──"
# Execute the exact SQL block Scotty drafted in M.5 closure report.
# Idempotent — safe to re-run.
```

Run the SQL from your prior report. Verify `still_present=0` after delete.

### Step 2: Harden capitol_fund.py
Add listing-status filter to prevent recurrence. Anchor `# === HM-BL.E ===`.

Pattern: before opening a position, check ticker is not delisted (use existing yf_safe / market_data infrastructure if available, or add a delist guard against a known-bad list).

Compile via venv/bin/python3.

### Step 3: Commit (single commit covers code; SQL is data-only)
`fix(capitol): HM-BL.E — listing-status filter prevents fresh positions on delisted tickers + ATH cleanup`

ntfy.

---

## Phase M2.2 — HM-BD.F-audit Tier-1 (4 sites loud-fail)

**Pre-decided**:
- Q1 scope: Tier-1 (4 sites)
- Q2 exception-class pattern: per-site typed catches matching HM-BD.F (`RequestException, TimeoutError, ConnectionError`, etc.)
- Q3 NTFY threshold: NO NTFY (consistent with HM-BD.F decision)

Apply loud-fail wrappers per `data/scotty_hm_bdf_audit_report.md`:
- L580 Ollama unload
- L1406 signal-center POST
- L711 alpaca-mirror snapshot
- L1029 record_signal

Pattern per CLAUDE.md doctrine (rich.console):
```python
# === HM-BD.F-audit Tier-1 ===
try:
    ...existing call...
except (<typed exceptions>) as e:
    console.log(f"[yellow]<site context> error: {type(e).__name__}: {e!r}[/yellow]")
    <preserve existing fallback>
```

Compile via venv/bin/python3.

Commit: `fix(observability): HM-BD.F-audit Tier-1 — loud-fail wrap 4 silent-pass sites in ai_brain.py`. ntfy.

---

## Phase M2.3 — HM-BJ.E2 lightweight-charts preview in hover tooltip

The lightweight-charts lib is already loaded at `dashboard/static/index.html:2304` per HM-BJ closure report. Add a mini chart inside the existing `.ticker-chip` hover tooltip.

### Step 1: Discovery
```bash
echo "── Find the existing tooltip render path ──"
grep -n "HM-BJ" dashboard/static/index.html | head -10

echo ""
echo "── lightweight-charts library availability ──"
grep -n "LightweightCharts\|lightweight-charts" dashboard/static/index.html | head -5
```

### Step 2: Apply
Add a small chart canvas inside the existing tooltip:
- Container: `<div class="chip-chart" style="width: 240px; height: 80px;"></div>` inside the tooltip
- Data feed: existing `/api/market/candles/${symbol}?period=5d&interval=15m` (already wired for sparkline in HM-BJ; reuse fetch + cache)
- Chart config: minimal — no axis labels, no grid, just a price line (lightweight-charts area or line series)
- Cleanup: destroy chart instance when tooltip hides (prevent memory leak across hovers)
- Fallback: if data fetch fails or returns empty, fall back to existing sparkline SVG

Anchor `// === HM-BJ.E2 ===`.

If lightweight-charts API doesn't fit the use case cleanly (e.g. requires too much config for a tiny preview), **HALT** with discovery findings and defer.

Commit: `feat(frontend): HM-BJ.E2 — inline lightweight-charts preview in ticker hover tooltip`. ntfy.

---

## Phase M2.C — Static verify

```bash
echo "── Anchors ──"
grep -rn "HM-BL.E\|HM-BD.F-audit\|HM-BJ.E2" engine/ dashboard/ main.py 2>/dev/null | head -15

echo ""
echo "── Compile changed Python files via venv ──"
venv/bin/python3 -c "
import py_compile, os, subprocess
changed = subprocess.check_output('git diff --name-only origin/main..HEAD -- \"*.py\"', shell=True, text=True).strip().split('\n')
for f in changed:
    if f and os.path.exists(f):
        py_compile.compile(f, doraise=True)
        print(f'  {f}: clean')
"

echo ""
echo "── Frontend JS syntax (extract <script> block if practical) ──"
ls -la dashboard/static/index.html

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy.

---

## Phase M2.D — Push + restart + verify (INLINE)

```bash
git push origin main

BACKEND_CHANGED=$(git log origin/main~5..HEAD --name-only --pretty=format: | grep -E "^(engine|main\.py|dashboard/app\.py)" | head -1)
if [ -n "$BACKEND_CHANGED" ]; then
  launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
  sleep 30
  NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')
  echo "  New PID: $NEW_PID"
  echo "  Port :8080:"; lsof -ti :8080 | head -1
fi

echo ""
echo "── Endpoint smoke ──"
curl -s -o /dev/null -w "  /api/premarket-gaps  HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/premarket-gaps
curl -s -o /dev/null -w "  /api/ghost-trades/stats  HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/ghost-trades/stats

echo ""
echo "── HM-BL.E validation: ATH row gone? ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT symbol, qty FROM positions WHERE symbol = 'ATH';"
echo "  (above should be empty)"

echo ""
echo "── HM-BD.F-audit validation: any new WARNING lines from typed catches? ──"
tail -100 ~/autonomous-trader/logs/main.log 2>/dev/null | grep -iE "ollama|signal-center|alpaca-mirror|record_signal" | head -5

echo ""
echo "── HM-BJ.E2: frontend live for browser hard-refresh ──"
echo "  Captain: Cmd+Shift+R, hover a ticker chip, look for mini chart inside tooltip"
```

ntfy: `🏁 HM-MONSTER2 complete — 3 ships, halt-queue cleared`.

---

## Closure report

`data/scotty_hm_monster2_report.md` per-phase outcome.
