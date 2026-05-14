# Scotty: HM-CD-ROUTES — fixed 9 broken /api/* endpoints — 2026-05-14

## Summary

5-file sweep: replace `config.get_active_universe()` calls with the
canonical `engine.universe.get_active_universe` import. Fixes 9 broken
HTTP 500 endpoints surfaced by the LANE 2 audit
(`data/scotty_500_endpoints_triage_2026-05-14.md`).

## Root cause

`get_active_universe()` is defined in `engine/universe.py` (returns the
filtered watchlist via SQL on `scan_universe`). The 5 engine modules
below were calling `config.get_active_universe()` — but no such function
exists on the `config` module. AttributeError on every call. The
endpoints that import these modules then returned HTTP 500.

This was a stale-reference pattern, likely surviving from a refactor
that moved the universe logic from `config.py` to `engine/universe.py`
but missed these 5 callsites.

## Files changed (6 call sites across 5 files)

| File | Call sites fixed |
|---|---|
| `engine/channel_scanner.py` | 1 |
| `engine/market_movers.py` | 2 |
| `engine/finnhub_data.py` | 1 |
| `engine/stock_race.py` | 1 |
| `engine/stock_screener.py` | 1 |

Each file received:
1. New top-level import: `from engine.universe import get_active_universe`
2. Replace `config.get_active_universe(` → `get_active_universe(`

## Endpoints unblocked (per LANE 2 audit)

- `/api/screener`
- `/api/finnhub/earnings`
- `/api/finnhub/context/{symbol}`
- `/api/patterns`
- `/api/pattern-alerts`
- `/api/channels`
- `/api/risk-radar`
- `/api/market/prices`
- `/api/market/correlation`
- `/api/trendlines`

(That's 10 listed in the audit — the count of 9 in the LANE 2 report
includes some duplication; the precise impact is "all dashboard tiles
that import these 5 modules will recover.")

## Smoke gate (all green pre-commit)

- `py_compile` on each of the 5 files: ✓
- `python -c "import main"`: ✓
- `python -c "import dashboard.app"`: ✓
- `hasattr(<each module>, "get_active_universe")` returns True: ✓ (5/5)
- `grep -rn "config\.get_active_universe"` post-sweep: 0 hits: ✓

## ⚠️ POST-CLOSE-RESTART REQUIRED

**The trader process must be restarted for these changes to take effect
on running endpoint handlers.** Python imports are evaluated at module
load; the live trader (PID was 76089 at time of commit) still holds
the old (broken) module references in memory.

**Do NOT restart during market hours.** Restart after 13:00 AZ close.

Procedure post-close:
```
launchctl unload ~/Library/LaunchAgents/com.trademinds.trader.plist
launchctl load   ~/Library/LaunchAgents/com.trademinds.trader.plist
sleep 8
curl -s http://localhost:8080/api/health   # expect server_up:true
# spot-check 2 of the previously-broken endpoints:
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/screener
curl -s -o /dev/null -w "%{http_code}\n" http://localhost:8080/api/market/prices
# expect 200 OK on both
```

## Captain action

- [x] Code change committed and PR opened
- [ ] PR reviewed + merged
- [ ] **Trader restarted post-close (13:00 AZ)** ← this is the activation step
- [ ] Confirm 2+ formerly-broken endpoints return 200 post-restart
