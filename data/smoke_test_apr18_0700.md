# Ollie Dashboard Smoke Test — 2026-04-18 07:00 MST

**Result:** PASS ✓
**Host:** Ollie (G1 Pro, Linux, Py3.10)
**Port:** 127.0.0.1:8090 (non-standard, to avoid bigmac live on :8080)
**Git:** main @ 8ddbcf5

## Test Commands

```bash
cd /home/bigmac/autonomous-trader
venv/bin/python3 -m uvicorn dashboard.app:app --host 127.0.0.1 --port 8090 --log-level info \
  > /tmp/ollie_smoke_test.log 2>&1 &

curl -s http://127.0.0.1:8090/api/status
curl -sI http://127.0.0.1:8090/
curl -s -o /dev/null -w "%{http_code}\n" http://127.0.0.1:8090/api/health
```

## Gaps Closed To Get Here

1. **7 missing Python packages** — installed at bigmac-pinned versions and added to `requirements.ollie.txt`:
   - `pyotp==2.9.0`, `qrcode==8.2`, `anthropic==0.84.0`, `feedparser==6.0.12`,
     `FinNews==1.1.0`, `psutil==7.2.2`, `webull==0.6.1`
2. **`.env` transport artifact** — removed line `TRADEMINDS_DB=/Users/bigmac/...`
   (was stomping the portable `expanduser("~/...")` fallback via `load_dotenv(override=True)`).
   Backup: `.env.beforeDBpathFix.backup` (mode 600, 3471 bytes).

## /api/status Response (live data from data/trader.db)

```json
{
  "status": "running",
  "current_season": 6,
  "active_players": 32,
  "total_trades": 114,
  "total_signals": 0,
  "total_chat_messages": 856,
  "total_news": 7767,
  "total_portfolio_value": 332743.03,
  "cic_usage": {"sonnet_calls_today": 0, "estimated_cost_today": "$0.0000"}
}
```

## Bigmac vs Ollie Diff (same moment)

| Field | bigmac (:8080 live) | Ollie (:8090 snapshot) | Match? |
|-------|---------------------|------------------------|--------|
| status | running | running | ✓ |
| current_season | 6 | 6 | ✓ |
| active_players | 32 | 32 | ✓ |
| total_trades | 114 | 114 | ✓ |
| total_signals | 0 | 0 | ✓ |
| total_chat_messages | 856 | 856 | ✓ |
| total_news | 7846 | 7767 | +79 on bigmac (still ingesting) |
| total_portfolio_value | 332743.03 | 332743.03 | ✓ |

Expected — bigmac is live and ingesting news; Ollie is running against the frozen
DB copy from last night (Apr 17 19:56). Everything else identical.

## Endpoint Probe Summary

| Endpoint | Code | Notes |
|----------|------|-------|
| `/api/status` | 200 | JSON, real data |
| `/api/health` | 200 | |
| `/` | 303 → `/login` | Auth gate |
| `/docs` | 303 → `/login` | Auth-gated Swagger |
| `/openapi.json` | 303 → `/login` | Also behind auth middleware |
| `/api/docs` | 404 | Not registered |
| `/api/players` | 404 | Different URL in this build |

## Non-Fatal Startup Warnings (log)

Worth investigating post-smoke, but did NOT block bind:

1. `backtest_analytics routes not loaded: No module named 'engine.backtest_api'`
2. `intelligence routes not loaded: No module named 'engine.intelligence_api'`
3. `[SSE] Could not register scanner callback: cannot import name '_scan_callbacks' from 'engine.volume_scanner'`

## Hardcoded /Users/bigmac Landmines (6 tracked, not yet fixed)

Do NOT break dashboard smoke test but will bite their respective features:

| File | Line(s) | Risk |
|------|---------|------|
| `signal-center/server.py` | 468 | `_TRADER_DB` hardcoded, no env fallback |
| `dashboard/app.py` | 14037 | `PROJECT = "/Users/bigmac/..."` — admin endpoint |
| `backtest_180d_sim.py` | 16 | `OUTPUT_PATH` |
| `run_comprehensive_backtest.py` | 15, 17, 24 | `sys.path`, `DB_PATH`, `OUTPUT_PATH` |
| `run_rsi_sweep.py` | 3, 87 | `sys.path`, `out_path` |
| (audit) | — | Broader sweep for host-specific paths pending |

## Next Recommended Steps

1. Systemd-wrap uvicorn on Ollie (after choosing port — 8080 once bigmac is retired, or keep 8090 during parallel).
2. Fix signal-center hardcoded DB path — same pattern.
3. Investigate the 3 non-fatal import warnings above.
4. Full path-landmine sweep (TaskList item #6).
