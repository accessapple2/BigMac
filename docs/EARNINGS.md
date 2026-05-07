# Earnings Code Paths

**Audit:** HM-AR 2026-05-07.
**Purpose:** Disambiguate the three "earnings" code paths in OllieTrades and document the dead path that has confused multiple investigations.

## Three independent earnings paths

OllieTrades has **three distinct code paths** that touch earnings data. They are NOT integrated; each operates independently. Conflating them has caused diagnostic confusion (HM-AQ investigation 2026-05-07 surfaced the dead `earnings_universe` table; this audit untangles the path map).

### 1. Options blackout path — LIVE, SAFETY-CRITICAL

| Aspect | Detail |
|---|---|
| Entry point | `engine/options_selector.py:19 _next_earnings_date(symbol)` |
| Data source | `data/earnings_cache.json` (file-based fast path, 6 h TTL) → yfinance fallback for cache misses |
| Consumers | `engine/options_selector.py:252` — blocks options entries within 3d of earnings or ±5d of expiry |
| State | **LIVE.** Independent of any SQLite table. |

**This is the path that actually protects options trades from earnings risk.** Per CLAUDE.md 2026-04-25 drydock notes: "Earnings blackout (options_selector.py): replaces dead logs-only block; blocks if earnings within 3d of today OR ±5d of expiry; fast-path through `data/earnings_cache.json` (1ms), yfinance fallback, fail-open on errors."

### 2. `run_earnings_scan_inject` (main.py:684) — LIVE (renamed by HM-AR-β 2026-05-07)

| Aspect | Detail |
|---|---|
| Entry point | `main.py:684 def run_earnings_scan_inject()` (was `run_earnings_universe_inject` pre-HM-AR-β) |
| Schedule | Every weekday at 06:00–06:30 AZ via `schedule.every().day.at("06:00").do(...)` |
| Data source | yfinance Ticker.calendar over `_AH_PM_UNIVERSE + morning_briefing._EARN_UNIVERSE` |
| Writes to | **`scan_universe` table** via `engine.deep_scan.inject_earnings_tickers` |
| Module variable | `_earnings_today_tickers` populated for `run_earnings_day_scan()` consumer |
| State | **LIVE.** Renamed in HM-AR-β to fix the prior naming-drift artifact (old name said "earnings_universe" but the function writes to `scan_universe`). |

### 3. `engine/earnings_injector.py` + `earnings_universe` table — RETIRED 2026-05-07 (HM-AR-β)

| Aspect | Detail |
|---|---|
| Module | ~~`engine/earnings_injector.py`~~ → **`archive/earnings_injector.py.retired-20260507`** (HM-AR-β 2026-05-07) |
| Schema | `earnings_universe (id, ticker, added_date, created_at)` with `UNIQUE(ticker, added_date)` — table **left in place** per sacred-data rule (empty, no data to lose, forensic record) |
| Writer | (was at `engine/earnings_injector.py:78`) — archived |
| Reader | (was at `engine/earnings_injector.py:96`) — archived |
| Upstream source | `earnings_impact` table (per archived `get_todays_earnings()`) |
| Schedule | Docstring claimed "Runs at 6:00 AM AZ" — never wired. |
| External callers | NONE — confirmed pre-archive grep across all source dirs. |
| Table state | 0 rows. Has been since creation. |
| State | **RETIRED.** See archive file for original code. |

Rationale: HM-AQ (2026-05-07 morning) surfaced the empty table during the "missed mover" investigation; HM-AR audited and classified DEPRECATED; HM-AR-β formalized retirement.

## Why this matters

**Before this audit**, three things looked like the same system but weren't:
- "earnings_universe is empty" (true; table 3 is dead)
- "run_earnings_universe_inject runs daily" (true; but it injects into table 2 = `scan_universe`, not table 3)
- "options blackout depends on earnings data" (true; via path 1 = yfinance cache, not table 3)

The naming overlap means an operator investigating "is earnings working?" can hit any of three paths and reach contradictory conclusions. Path 1 is the only safety-critical answer.

## Recommended cleanup (HM-AR-β)

See `docs/XO_BACKLOG.md` HM-AR-β. Default recommended path: **formal retirement** of the dead orphan (path 3) — archive `engine/earnings_injector.py` to `archive/` per sacred-data rule, document the empty table as deprecated, and avoid reintroducing the naming confusion. Plus a rename of `run_earnings_universe_inject` → `run_earnings_scan_inject` to fix the lie in path 2's function name.

## Cross-references

- HM-AR — this audit (DOC-only)
- HM-AR-β — cleanup ticket (proposed)
- HM-AQ — "missed mover" investigation that surfaced the empty table
- CLAUDE.md "2026-04-25 Saturday Drydock" — options blackout fast-path documentation
- `engine/options_selector.py:19` — actual blackout enforcement
- `main.py:679` — `run_earnings_universe_inject()` (writes to scan_universe, not earnings_universe)
- `engine/earnings_injector.py` — dead orphan
- `data/earnings_cache.json` — the file the options blackout actually reads
