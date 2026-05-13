# 🔧 SCOTTY — HM-DASH: Dashboard Remodel + Short-Squeeze Focus (Session 1 of 2-3)
### Opus 4.7 · Backend foundation + Race engine · ~4-5 hr · No frontend yet

> **Captain's orders, Mr. Scott:** Resurrect the Dashboard Remodel v1 work paused May 10 and weave short-squeeze focus through Phase 2 + Phase 3. SESSION 1 = backend foundation only: short-squeeze data layer (Polygon short interest + DTC), universe loader (S&P 500 + R1K), and Race engine + endpoint. Frontend Race tile + Scanner is Session 2. NO restart needed for backend additions — new endpoints just appear.

## Pre-flight + Recovery

```bash
cd ~/autonomous-trader
echo "── Existing Dashboard Remodel docs ──"
ls docs/ | grep -iE "dashboard|remodel|race|scanner|squeeze" 2>/dev/null
find . -name "*charter*" -o -name "*dashboard*remodel*" 2>/dev/null | head -10

echo ""
echo "── HM-AN Phase 1 read bridge state (shipped May 10) ──"
grep -n "HM-AN\|signal-center.*bridge\|signal_center" dashboard/app.py | head -10
grep "@app.get.*signal\|@app.get.*bridge" dashboard/app.py | head -5

echo ""
echo "── Any existing Race/Scanner work? ──"
find . -name "*race*" -o -name "*scanner*" 2>/dev/null | grep -v ".pyc\|test_\|node_modules" | head -15

echo ""
echo "── Polygon API key tier check (Stocks Starter covers short interest?) ──"
python3 -c "
import os
from dotenv import load_dotenv
load_dotenv()
print(f'POLYGON_API_KEY: {\"✓\" if os.getenv(\"POLYGON_API_KEY\") else \"✗\"}')
print(f'POLYGON_STOCKS_API_KEY: {\"✓\" if os.getenv(\"POLYGON_STOCKS_API_KEY\") else \"✗\"}')
"
```

## Phase DASH.0 — Discovery + Polygon endpoint validation

```bash
echo "── Polygon short interest endpoints (Stocks Starter tier) ──"
python3 << 'PY'
import os, requests, time
from dotenv import load_dotenv
load_dotenv()
KEY = os.getenv('POLYGON_API_KEY') or os.getenv('POLYGON_STOCKS_API_KEY')

# Probe likely Polygon short-interest endpoints
endpoints = [
    ('Short interest reference', f'https://api.polygon.io/stocks/v1/short-interest?ticker=GME&apiKey={KEY}&limit=5'),
    ('Ticker details (shares outstanding)', f'https://api.polygon.io/v3/reference/tickers/GME?apiKey={KEY}'),
    ('Snapshot (volume + RVOL components)', f'https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/GME?apiKey={KEY}'),
    ('Tickers list (S&P 500 etc)', f'https://api.polygon.io/v3/reference/tickers?market=stocks&active=true&limit=100&apiKey={KEY}'),
]
for name, url in endpoints:
    t0 = time.time()
    try:
        r = requests.get(url, timeout=10)
        dt = time.time() - t0
        status = r.status_code
        data = r.json() if r.headers.get('content-type','').startswith('application/json') else {}
        keys_preview = list(data.keys())[:5] if isinstance(data, dict) else 'non-dict'
        print(f"  {name:45} HTTP {status}  {dt:.2f}s  keys={keys_preview}")
    except Exception as e:
        print(f"  {name:45} ERROR {type(e).__name__}")
PY

echo ""
echo "── Universe source check: S&P 500 + R1K constituents available? ──"
# Polygon v3/reference/tickers can filter by market/index — see what's accessible
```

Document `data/scotty_hm_dash_discovery.md`:
- Existing remodel charter location (if any) — quote key decisions
- HM-AN Phase 1 endpoints inventory
- Polygon endpoints validated for: short interest, shares outstanding, snapshot (for RVOL/volume), tickers list
- **Q1**: Which Polygon endpoints serve short-squeeze data cleanly? If short interest is NOT in Stocks Starter, propose fallback (FINRA monthly disclosure / Yahoo / NASDAQ TotalView)
- **Q2**: Universe source recommendation (Polygon tickers list vs hardcoded S&P 500 file vs other)
- **Q3**: Confirm Session 1 scope — short-squeeze data layer + Race engine backend only, frontend deferred

**HALT** for Captain to confirm Q1 + Q2 (Q3 is yes by default).

## Phase DASH.1 — Short-squeeze data layer (after Captain confirms)

Create `engine/squeeze_data.py`:
- `get_short_interest(symbol)` — returns latest reported SI %, DTC (days to cover), as-of date
- `get_squeeze_score(symbol)` — composite: SI > 20% gets boost, DTC > 5 gets boost, RVOL > 3 gets boost (RVOL from existing market_data)
- Cache results with 1-day TTL (short interest updates monthly anyway; cache aggressively)
- Fallback chain: Polygon primary → fallback to manual lookup if endpoint not in tier
- Anchor `# === HM-DASH.1 ===`
- Per CLAUDE.md doctrine: typed catches with [yellow] console.log for tier-fallthrough events

Compile via venv/bin/python3. Smoke test:
```python
from engine.squeeze_data import get_short_interest, get_squeeze_score
print(get_short_interest('GME'))  # known high-SI ticker
print(get_squeeze_score('AAPL'))  # known low-SI ticker (should score low)
```

Commit: `feat(squeeze): HM-DASH.1 — short-squeeze data layer (Polygon SI + DTC + composite score)`. ntfy.

## Phase DASH.2 — Universe loader

Create `engine/universe.py` (or extend existing):
- `get_universe(name='sp500_r1k')` — returns cached list of ~2,500 tickers
- Refresh weekly (cron or first-call-of-day)
- Source: Polygon tickers reference with active=true filter, OR fallback to hardcoded constituents file
- Persist to `data/universe_sp500_r1k.json` for fast load
- Anchor `# === HM-DASH.2 ===`

Smoke: confirm get_universe() returns ~2500 symbols.

Commit: `feat(universe): HM-DASH.2 — S&P 500 + R1K cached universe loader (weekly refresh)`. ntfy.

## Phase DASH.3 — Race engine + endpoint

Create `engine/race_engine.py`:
- `get_top_movers_since_open(n=20)` — returns top-N % gainers from universe since today's open
- Each row includes: symbol, price, change_pct_since_open, volume, rvol, squeeze_score, squeeze_flag (bool: score >= threshold)
- Parallelize symbol fetches using the module-level executor pattern (per HM-EQ + HM-BJ.E4 lesson)
- @timed_cache(30) for 30s freshness per charter decision
- Anchor `# === HM-DASH.3 ===`

Add endpoint to `dashboard/app.py`:
- `GET /api/race/top-gainers` — returns JSON `{updated_at, gainers: [...], universe_size: N}`
- Frontend will poll this every 30s (Session 2)
- Anchor `# === HM-DASH.3 endpoint ===`

Compile via venv/bin/python3.

Commit: `feat(api): HM-DASH.3 — /api/race/top-gainers endpoint with squeeze flag (30s cache, parallel fetches)`. ntfy.

## Phase DASH.C — Push + verify (NO RESTART needed if only new endpoints added)

```bash
git push origin main

# If race_engine references modified imports in main.py / app.py initialization, restart:
BACKEND_INIT_CHANGED=$(git diff origin/main~3..HEAD --name-only | grep -E "^main\.py$")
if [ -n "$BACKEND_INIT_CHANGED" ]; then
  launchctl kickstart -k gui/$(id -u)/com.trademinds.trader
  sleep 30
fi

echo ""
echo "── Smoke test new endpoint ──"
curl -s "http://localhost:8080/api/race/top-gainers?n=10" | python3 -m json.tool | head -40
echo ""
echo "── Timing ──"
curl -s -o /dev/null -w "  Cold: %{time_total}s\n" "http://localhost:8080/api/race/top-gainers"
curl -s -o /dev/null -w "  Warm: %{time_total}s\n" "http://localhost:8080/api/race/top-gainers"
```

ntfy: `🏁 HM-DASH Session 1 complete — backend foundation + Race engine live`.

## Phase DASH.E — Closure + Session 2 handoff

`data/scotty_hm_dash_session1_report.md` with:
- Discovery findings (Polygon endpoint capabilities)
- Commits shipped
- Endpoint specs (for Session 2 frontend work)
- Open follow-ups for Session 2:
  - Race tile React component (LCARS-themed, mobile responsive 16-18px body, 48px tap targets)
  - Race tile renders top-20 with squeeze flag visual
  - Scanner engine backend (5-min RVOL detection)
  - Scanner endpoint /api/scanner/squeeze-candidates
- Session 3 preview: Detail panel (Phase 4) + live restyle (Phase 5)
