# 🔧 SCOTTY — HM-BJ.E4: Server-side Scorecard Aggregator
### Opus 4.7 · Backend-safe + Frontend-revert-proof · Per memory #27 frontend-ship rule

> **Captain's orders, Mr. Scott:** Eliminate the 3-second cold-path lag on ticker chip hover. Current tooltip makes 3-4 parallel client-side fetches; replace with one server-side aggregator endpoint that does the parallel work internally with caching. PATTERN: ship backend first (zero frontend risk), then SEPARATELY test frontend swap LOCALLY in browser before committing per memory #27 frontend-ship rule. HM-BJ.E2 broke chips for hours; this epic avoids that fate.

## Pre-flight

```bash
cd ~/autonomous-trader
git log origin/main --oneline | head -3
git status --short
pgrep -af main.py | head -1
lsof -ti :8080 | head -1
```

## Phase E4.0 — Discovery

```bash
grep -n "fetch.*api/market\|fetch.*api/news\|fetch.*api/symbol\|fetch.*api/sentiment\|fetch.*api/mtf\|fetch.*api/gex\|fetch.*api/patterns" dashboard/static/index.html | head -20
grep -n "HM-BJ" dashboard/static/index.html | head -10
grep -n "@app.get.*/api/symbol\|@app.get.*/api/market/candles\|@app.get.*/api/market/sentiment\|@app.get.*/api/news\|@app.get.*/api/market/mtf\|@app.get.*/api/gex\|@app.get.*/api/patterns" dashboard/app.py | head -15
grep -B 1 -A 5 "@timed_cache" dashboard/app.py | head -30
```

Document in `data/scotty_hm_bje4_report.md`: endpoint list, response shape, parallelization fit.

## Phase E4.1 — Backend endpoint

`/api/symbol/{symbol}/scorecard` with `@timed_cache(60)`, parallelized internal fetches, graceful per-sub-fetch failure (HM-BD.F typed-catch). Anchor `# === HM-BJ.E4 ===`.

Commit: `feat(api): HM-BJ.E4 — /api/symbol/{sym}/scorecard aggregator (parallel internal fetches, 60s cache)`.

## Phase E4.2 — Backend verify (HALT-FOR-CAPTAIN)

```bash
git push origin main
launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
sleep 30
time curl -s -o /tmp/scorecard_cold.json "http://localhost:8080/api/symbol/SPY/scorecard"
time curl -s -o /tmp/scorecard_warm.json "http://localhost:8080/api/symbol/SPY/scorecard"
python3 -m json.tool /tmp/scorecard_cold.json | head -40
```

HALT for Captain: response shape + cold-path < 3s + warm-path < 100ms.

## Phase E4.3 — Frontend swap LOCAL ONLY (NO COMMIT)

Single fetch to `/api/symbol/${sym}/scorecard`. Anchor `// === HM-BJ.E4 frontend ===`. Local file edit, NO commit.

## Phase E4.4 — Browser smoke (Captain)

Hard-refresh, hover chips across panels. ✅/⚠️/❌.

## Phase E4.5 — Commit frontend (only if WORKING)

## Phase E4.6 — Closure report
