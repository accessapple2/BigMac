# 🔧 SCOTTY — HM-MONSTER: Multi-Epic Clear-the-Deck Bundle
### Opus 4.7 · 4 ships + 2 audits · Single push + restart at end

> **Captain's orders, Mr. Scott:** Clear the small/medium parked items in one session. Each phase = own commit, independently revertable. HALT only on errors OR phases marked "HALT-FOR-CAPTAIN" (M.5, M.6). Single push + restart at end. Auto-mode is on — use judgment, ship what's obvious, halt what isn't.

## Pre-flight

```bash
cd ~/autonomous-trader
echo "── HM-BLD landed? ──"
git log origin/main --oneline | head -5
echo ""
echo "── Working tree ──"
git status --short
echo ""
echo "── Service alive ──"
pgrep -af main.py | head -1
lsof -ti :8080 | head -1
```

If HM-BLD isn't on origin, push it first before starting M.1.

---

## Phase M.1 — HM-BD.H scanned_at format fix

### M.1.0 Discovery
```bash
echo "── Where does scanned_at get serialized to JSON? ──"
grep -rn "scanned_at" dashboard/app.py engine/premarket_scanner.py | head -10

echo ""
echo "── Compare disk write vs HTTP response shape ──"
echo "Disk format (from premarket_scanner._save_gaps):"
head -3 data/premarket_gaps.json 2>/dev/null | python3 -m json.tool 2>/dev/null | head -10
echo ""
echo "HTTP response format:"
curl -s http://localhost:8080/api/premarket-gaps 2>/dev/null | python3 -m json.tool 2>/dev/null | head -10

echo ""
echo "── FastAPI response_model or custom serializer? ──"
grep -B 2 -A 6 "@app.get.*premarket-gaps" dashboard/app.py | head -20
```

### M.1.1 Apply
If the diff is obvious (datetime auto-serialization, response_model coercion, or jsonable_encoder): fix with `# === HM-BD.H ===` anchor, preserve disk format, commit `fix(api): HM-BD.H — preserve ISO datetime format in /api/premarket-gaps response`.

If discovery reveals it's deeper than 1-file: **HALT** and report — defer to its own epic.

ntfy.

---

## Phase M.2 — HM-BK-residual (second bridge banner)

### M.2.0 Discovery
```bash
echo "── Where else is AlpacaBridge imported with module-level init? ──"
grep -rn "AlpacaBridge\(\)\|alpaca_bridge\." engine/ scripts/ main.py 2>/dev/null | grep -v "test_\|.pyc\|# ===" | head -15

echo ""
echo "── Multiprocessing / fork patterns ──"
grep -rn "multiprocessing\|fork\|Process(" main.py engine/ | head -10

echo ""
echo "── Recent bridge banners ──"
grep "Alpaca Paper Trading bridge initialized" ~/autonomous-trader/logs/trader.log | tail -5
```

### M.2.1 Apply
If second banner traces to a clean fix (missing singleton import, fork inheritance pattern): apply with `# === HM-BK-residual ===` anchor, commit `fix(kirk): HM-BK-residual — second bridge banner deduped at <source>`.

If it's an unavoidable multiprocessing artifact: **HALT** and document as known-acceptable, no commit.

ntfy.

---

## Phase M.3 — HM-BJ.E1 right-click context menu

In `dashboard/static/index.html`, add a right-click handler to the existing `.ticker-chip` class (HM-BJ infrastructure already present).

Context menu items:
- 📊 TradingView — `https://www.tradingview.com/chart/?symbol=${sym}`
- 💹 Yahoo Finance — `https://finance.yahoo.com/quote/${sym}`
- 🦅 Webull — `https://www.webull.com/quote/${sym}`
- 🏦 Schwab — `https://www.schwab.com/research/stocks/quotes/summary/${sym}`
- 🐦 X / Twitter search — `https://twitter.com/search?q=%24${sym}` (uses $TICKER cashtag)

Pattern: position menu near cursor on `contextmenu` event, prevent default browser menu, dismiss on outside-click or Esc.

Anchor: `// === HM-BJ.E1 ===`.

Compile/sanity check via `node --check` on extracted JS if practical.

Commit: `feat(frontend): HM-BJ.E1 — right-click context menu on ticker chips`.

ntfy.

---

## Phase M.4 — HM-BJ.E3 arrow-key navigation

In `dashboard/static/index.html`, extend the existing `.ticker-chip` keyboard handler.

Add: while one chip has focus, arrow-keys navigate to next/previous chip in DOM order (Left/Up = prev, Right/Down = next). Wrap at boundaries. Preserve existing Tab+Enter behavior.

Anchor: `// === HM-BJ.E3 ===`.

Commit: `feat(frontend): HM-BJ.E3 — arrow-key navigation between ticker chips`.

ntfy.

---

## Phase M.5 — HM-BL.E discovery (HALT FOR CAPTAIN)

```bash
echo "── Audit positions table for stale 0-qty rows ──"
sqlite3 -header -column ~/autonomous-trader/data/trader.db "SELECT symbol, qty, ROUND(market_value, 2) AS mv, ROUND(unrealized_pl, 2) AS upl, datetime(updated_at, 'localtime') AS updated FROM positions WHERE qty = 0 OR qty IS NULL ORDER BY updated_at DESC;" 2>/dev/null

echo ""
echo "── How many 0-qty rows total? ──"
sqlite3 ~/autonomous-trader/data/trader.db "SELECT COUNT(*) FROM positions WHERE qty = 0 OR qty IS NULL;" 2>/dev/null

echo ""
echo "── Schema check on positions ──"
sqlite3 ~/autonomous-trader/data/trader.db "PRAGMA table_info(positions);" | head -20

echo ""
echo "── What writes to positions? (the sync we'd need to harden) ──"
grep -rn "INSERT.*positions\|UPDATE.*positions" engine/ scripts/ main.py | head -10
```

Document inventory + propose:
- Cleanup SQL (DELETE WHERE qty = 0 AND updated_at < N days old)
- Root-cause hypothesis (which sync agent retains the stale row)

Closure report `data/scotty_hm_ble_report.md` with SQL handoff block for Captain to run manually (sacred-DB rule).

Commit closure report only: `docs(scotty): HM-BL.E — stale 0-qty positions discovery + SQL handoff`. ntfy.

**HALT** — Captain runs the SQL when ready.

---

## Phase M.6 — HM-BD.F-audit discovery (HALT FOR CAPTAIN)

```bash
echo "── Inventory all 22 silent-pass sites in ai_brain.py ──"
grep -nB 2 -A 5 "except Exception:\s*$\|except:\s*$\|except Exception:\s*pass\|except:\s*pass" engine/ai_brain.py | head -100
```

For each site, classify:
- **HTTP/network call** → recommend loud-fail (HM-BD.F treatment)
- **Internal state / DB write** → recommend keep silent (intentional fallback)
- **Unknown / needs eyes** → flag for Captain

Document in `data/scotty_hm_bdf_audit_report.md`:
- Site count by category
- Recommended scope for loud-fail extension (likely 3-5 sites)
- Captain Q: ship all recommended sites in HM-BD.F2 follow-up, OR cherry-pick?

Commit closure report only. ntfy.

**HALT** — Captain decides next epic scope.

---

## Phase M.C — Static verify

```bash
echo "── HM-MONSTER anchors ──"
grep -rn "HM-BD.H\|HM-BK-residual\|HM-BJ.E1\|HM-BJ.E3" engine/ dashboard/ main.py 2>/dev/null | head -15

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
echo "── Frontend syntax check (extract <script> blocks if practical, else just file-exists) ──"
ls -la dashboard/static/index.html

echo ""
echo "── Commits staged ──"
git log origin/main..HEAD --oneline
```

ntfy.

---

## Phase M.D — Push + restart + verify (INLINE)

```bash
git push origin main

# If any backend file changed, restart trader
BACKEND_CHANGED=$(git log origin/main~10..HEAD --name-only --pretty=format: | grep -E "^(engine|main\.py|dashboard/app\.py)" | head -1)
if [ -n "$BACKEND_CHANGED" ]; then
  launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
  sleep 30
  NEW_PID=$(launchctl list | grep com.trademinds.trader | awk '{print $1}')
  echo "  New PID: $NEW_PID"
  echo "  Port :8080:"; lsof -ti :8080 | head -1
fi

# Frontend-only commits don't need restart (FileResponse serves disk on each request)

echo ""
echo "── Endpoint smoke ──"
curl -s -o /dev/null -w "  /api/premarket-gaps  HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/premarket-gaps
curl -s -o /dev/null -w "  /api/ghost-trades/stats  HTTP %{http_code}  %{time_total}s\n" --max-time 5 http://localhost:8080/api/ghost-trades/stats

echo ""
echo "── scanned_at format check (HM-BD.H validation if shipped) ──"
curl -s http://localhost:8080/api/premarket-gaps 2>/dev/null | python3 -c "import sys, json; d=json.load(sys.stdin); g=d.get('gaps',[]); print(f'  scanned_at sample: {g[0].get(\"scanned_at\") if g else \"empty\"}')" 2>/dev/null

echo ""
echo "── Bridge banner count post-restart (HM-BK-residual validation if shipped) ──"
sleep 60
grep "Alpaca Paper Trading bridge initialized" ~/autonomous-trader/logs/trader.log | tail -3
```

ntfy: `🏁 HM-MONSTER complete — N commits, M ships, X audits parked for Captain`.

---

## Closure report

`data/scotty_hm_monster_report.md` with per-phase outcome:
- M.1 HM-BD.H: SHIPPED | DEFERRED (reason)
- M.2 HM-BK-residual: SHIPPED | KNOWN-ACCEPTABLE (reason)
- M.3 HM-BJ.E1: SHIPPED
- M.4 HM-BJ.E3: SHIPPED
- M.5 HM-BL.E: SQL HANDOFF READY (block in this file)
- M.6 HM-BD.F-audit: SCOPE PROPOSAL READY (Captain Q in this file)
