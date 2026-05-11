# Phase 6 Discovery — 2026-05-10 20:25 MST

Read-only sweep. No edits. No service interaction beyond `curl`/`launchctl list`.

## TL;DR

| Plan assumption | Reality |
|---|---|
| `engine/momentum/universe.py` reusable | ❌ **Does not exist.** Use `engine.universe.get_active_universe()` at `engine/universe.py:112` |
| `engine/momentum/flags.py` reusable | ❌ **Does not exist anywhere.** No `get_flags_bulk` function in the repo. |
| `engine.providers.alpaca_provider.get_snapshots` | ❌ **Does not exist.** Use `engine.market_data.get_bulk_snapshots(symbols)` at `engine/market_data.py:318` — same function Phase 2 race.py uses. |
| Pre-market scanner is greenfield | ❌ **`engine/premarket_scanner.py` already exists** (520 lines: yfinance per-symbol + Finviz scraper + DB write to `premarket_scan`). Different design (watchlist not universe, no volume filter, slow per-symbol calls). Has its own launchd job. |
| `/api/momentum/premarket` is new | ✅ confirmed unregistered, but **`/api/premarket-gaps` already exists** at `app.py:11069` and serves the legacy scanner. Plus `/api/premarket` redirect and `/api/premarket-watchlist`. |
| Race tile already mounted | ✅ shipped Phase 2.4 — lines `34170–34328` of `dashboard/static/index.html` |
| Race **desktop** sidebar nav entry | ❌ **STILL MISSING.** Carry-over from Phase 4-static.0. Mobile entry at line `12796` is the only way users currently reach the Race tab. |
| Scanner tile shipped | ❌ Endpoint returns **404**. Backend never registered. |
| Phase 5 helpers (`momentum-restyle`, `deltaFlash`, `updateHeartbeat`, `.heartbeat-dot`) | ❌ **Still absent.** Phase 5 stuck at 5.0b. Phase 6 plan's HTML uses `.heartbeat-dot` class — would render as un-styled `<span>`. |

## Endpoint health (live, dashboard running on PID 44662)

| Endpoint | HTTP | Note |
|---|---|---|
| `/api/momentum/heartbeat` | 200 | OK |
| `/api/momentum/recent_signals` | 200 | OK |
| `/api/momentum/race?limit=2` | 200 | Returns `{ts, limit, rows: [{rank, ticker, pct_change_since_open, last_price, open_price, volume, market_status: "CLOSED"/"PRE"/"OPEN"/"AFTER"}]}` |
| `/api/momentum/scanner` | 404 | **Carry-over blocker** from Phase 4-static.0 — never shipped. |
| `/api/momentum/detail/AAPL` | 200 | OK, rich response |
| `/api/premarket-gaps` | **000 / empty body** | Endpoint registered but currently returning nothing. Per-symbol yfinance loops in `scan_premarket_gaps()` may be timing out. **Functional gap separate from Phase 6.** |

## engine/momentum/ — actual contents

```
__init__.py   (empty)
bridge.py     (3.4 KB)  — BridgeHealth, check_signal_center_health, fetch_recent_signals
detail.py     (6.8 KB)  — compute_detail, _bars_multi_timeframe, _fundamentals, _recent_signals
race.py       (2.6 KB)  — RaceRow, compute_race, _market_status_now
```

No `universe.py`. No `flags.py`. The plan's `from engine.momentum.universe import get_universe` and `from engine.momentum.flags import get_flags_bulk` will **both fail** as written.

## Existing window-status helper (reusable)

`engine/momentum/race.py::_market_status_now()` (lines 72–85):

```python
def _market_status_now() -> str:
    """Naive US-Eastern session label. Weekend = CLOSED; holiday-unaware."""
    now = datetime.now(_ET)
    if now.weekday() >= 5:
        return "CLOSED"
    mins = now.hour * 60 + now.minute
    if mins < 4 * 60:           return "CLOSED"
    if mins < 9 * 60 + 30:      return "PRE"
    if mins < 16 * 60:          return "OPEN"
    if mins < 20 * 60:          return "AFTER"
    return "CLOSED"
```

Plan's `_window_status()` would duplicate this. **Reuse.** Note label difference: existing returns `"PRE"`, plan expected `"PREMARKET"`. Pick one and stick with it — recommend `"PRE"` for consistency with the already-shipped Race response.

Also available: `engine/risk_manager.py::RiskManager.is_market_hours()` returns `"pre_market"` / `"market"` / `"post_market"` / `False`. More detailed but uses Mountain Time and a `pytz` dep. Recommend not switching to it for Phase 6 — race.py already uses `zoneinfo` + US-Eastern cleanly.

## Existing pre-market scanner (legacy)

`engine/premarket_scanner.py` exists with full implementation:

- **`scan_premarket_gaps()`** at line 19: loops `config.get_effective_watchlist()` (small curated list, ~15 symbols), calls `get_stock_price(symbol)` + `get_alpaca_bars(symbol, days=2)` **per symbol**. Filters at `|gap| ≥ 2%`. **No volume filter.** Returns list of `{symbol, prev_close, premarket_price, gap_pct, direction, scanned_at}`. Writes to `data/premarket_gaps.json` via `_save_gaps()`.
- **`analyze_gaps_with_ai()`** at line 91: pipes the gaps through 4 AI models for catalyst/setup analysis.
- **Finviz-based scanner** at line 468+: writes to DB table `premarket_scan` (sacred — read-only this phase).
- **Launchd job**: `com.trademinds.premarket.plist` exists at `~/Library/LaunchAgents/`. Shell entry `premarket-scan.sh` is wired (6:00 AM MST / before 7:30 AM MST open). Internal AI brain consumer at `engine/ai_brain.py:898` calls `/api/premarket-gaps` with 5-min cache.

**Implication:** the new `engine/momentum/premarket.py` from the plan is a parallel, faster, batched implementation — **not a replacement**. The two coexist by design:

| Aspect | Legacy `premarket_scanner.py` | New `engine/momentum/premarket.py` |
|---|---|---|
| Universe | `config.get_effective_watchlist()` (~15) | `get_active_universe()` (~hundreds) |
| Data source | per-symbol yfinance + Finviz | one batched Alpaca snapshots call |
| Filter | `|gap| ≥ 2%`, no volume | `|gap| ≥ 3% AND vol ≥ 50K` |
| Cadence | Cron at 6AM MST (once) | UI polling 60s (continuous) |
| Consumer | AI brain via `/api/premarket-gaps` | UI tile via `/api/momentum/premarket` |
| Output write | `data/premarket_gaps.json` + DB | none — read-only |

These don't compete. **Recommend: keep both. Add new endpoint alongside.**

## Race tile mount point (for Phase 6 UI insertion)

| Asset | Line |
|---|---|
| End of Phase 2 Race CSS block | `34218` (`</style>`) |
| Start of Race markup | `34221` (`<div id="section-race">`) |
| Inner race-section container | `34222–34229` |
| End of Race markup | `34230` (`</div>`) outside section-race |
| End of Phase 2 Race poller `<script>` | `34328` (`// === end Phase 2: Race tile poller ===`) |
| `<!-- === end Phase 2: Race tile === -->` | `34330` |
| Next block | `34335+` — Phase 4 detail enrichment `<style>` |

**Recommended Phase 6 insertion**: append premarket-tile markup as a SIBLING inside the same `#section-race` container (between current `</div>` of `.race-section` at line ~34229 and the closing `</div>` of `#section-race` at line ~34230). This puts pre-market in the same tab as Race, which matches the plan.

CSS additions: append a new `<style>` block immediately after line `34330` (after the Phase 2 Race end-marker), guarded by Phase 6 anchors. Don't try to merge into the Phase 2 race CSS block — it has its own private LCARS palette scoped to `.race-section`.

JS additions: append a new `<script>` block immediately after the CSS block, also Phase 6 anchored.

## Alpaca snapshot shape (verified in market_data.py / volume_baselines.py / volume_scanner.py)

Snapshots come back as a dict per symbol with the following keys (camelCase as Alpaca returns; codebase has fallback to snake_case):

- `prevDailyBar.c` — previous day close (float)
- `prevDailyBar.v` — previous day volume
- `latestTrade.p` — latest trade price (works for pre-market data)
- `latestQuote.bp` / `latestQuote.ap` — bid/ask
- `minuteBar.v` — current minute bar volume
- `dailyBar.o` / `dailyBar.c` / `dailyBar.v` — today's bar so far

For pre-market: `prevDailyBar.c` for prev_close, `latestTrade.p` for premarket_price (Alpaca SIP feed includes pre-market trades), and pre-market volume needs special handling — Alpaca `minuteBar.v` is current minute only, not cumulative pre-market volume. **For cumulative pre-market volume, would need to sum `multi-bar` query** OR use `dailyBar.v` (which on pre-market is the cumulative pre-market volume so far).

This is a design wrinkle the plan didn't surface — need to verify which field gives "cumulative pre-market volume" before relying on it for the filter.

## Phase 6 revised design (proposed)

**Engine module path:** `engine/momentum/premarket.py` (new — keep parallel to race.py / detail.py)

**Imports (corrected):**
```python
from engine.market_data import get_bulk_snapshots  # not engine.providers.alpaca_provider
from engine.universe import get_active_universe    # not engine.momentum.universe
from engine.momentum.race import _market_status_now  # reuse existing window helper
# Flags: skip for v1 (engine.momentum.flags doesn't exist) — leave as TODO
```

**Universe choice:** use `get_active_universe()` to match Phase 2/3 scope. This is the $5B+ market cap, $100M+ dollar volume universe (currently includes ETFs since 2026-05-07).

**Volume field decision:** propose `dailyBar.v` for "cumulative session volume so far" — empirical test needed to confirm Alpaca returns pre-market volume here during the 4–9:30 ET window. If it returns 0 during pre-market hours, fall back to `prevDailyBar.v` ratio as a proxy or drop the volume filter for v1.

**Output shape:**
```python
{
  "ts": "<iso utc>",
  "window_state": "PRE" | "OPEN" | "AFTER" | "CLOSED",
  "hits": [{
    "rank": 1,
    "ticker": "XYZ",
    "gap_pct": 5.42,
    "prev_close": 100.0,
    "premarket_price": 105.42,
    "premarket_volume": 142000,
    "flags": [],          # empty until engine.momentum.flags is built (separate ticket)
    "direction": "UP",
    "market_status": "PRE"
  }]
}
```

**Endpoint:** `GET /api/momentum/premarket?limit=30&force=false` at `dashboard/app.py` (new). Coexists with the legacy `/api/premarket-gaps`.

**UI tile insertion:** as described above, sibling inside `#section-race`.

## Decisions for the Admiral

### Q1 — Engine path: parallel module or extend legacy?

**Recommend: parallel module** (`engine/momentum/premarket.py`). Justifications:
- Matches the architecture of `race.py` and `detail.py` (single-purpose, batched-Alpaca, polling-friendly).
- Doesn't risk breaking the AI brain pipeline that consumes `/api/premarket-gaps`.
- Legacy uses watchlist (15 symbols) — would not satisfy the plan's "same universe as Phase 2" requirement.

Alternative: refactor `premarket_scanner.py` to support both universes via flag. **Costs:** higher blast radius (touches AI brain consumer); rejected.

### Q2 — `engine.momentum.flags.get_flags_bulk()` doesn't exist

Plan's UI shows earnings / squeeze / lowfloat flags. Three options:

- **A.** Skip flags entirely for Phase 6 v1. Empty `flags: []` array. Add `engine/momentum/flags.py` as a follow-up ticket. **Recommended for speed.**
- **B.** Build a minimal `flags.py` inline as part of Phase 6: just an earnings flag from `data/earnings_cache.json`. ~30 lines, useful, scoped.
- **C.** Block Phase 6 until a separate flags-module ticket ships.

**Recommend A** for speed; B if you want flags visible at first ship.

### Q3 — Pre-market volume filter — verify Alpaca field semantics

Plan filters at `premarket_volume ≥ 50K`. The exact Alpaca snapshot field that returns "cumulative pre-market session volume" needs empirical verification. Two paths:

- **A.** Ship Phase 6 with `dailyBar.v` as the proxy. If it shows 0 during pre-market hours, revise. Risk: low — worst case the filter is too tight and we see fewer hits than expected.
- **B.** Drop the volume filter for Phase 6 v1. Rank by gap % alone. Add volume back after empirical Alpaca check.

**Recommend A** with logged fallback to "no filter" if `dailyBar.v` is zero.

### Q4 — Carry-over: Race desktop nav STILL missing

Same as Phase 4-static.0 finding. Phase 6 will add a third panel inside `#section-race`, but if `#section-race` is only reachable via the mobile-nav entry at line 12796, **desktop users won't see any of this work**. The ~3-line fix is the same as in Phase 4-static.0.

Strongly recommend bundling that 3-line nav addition into Phase 6.0 or 6.3. Otherwise we're shipping a third tile in a tab desktop users can't reach.

### Q5 — Service restart will be required for Phase 6

Same as Phase 4-static: new Python module + new endpoint → requires service restart for Python imports to load. Confirmed by the plan itself (Phase 6.5 + 6.6 both note this). Will be flagged at closure for Admiral action.

## Halt point

Discovery complete. Halting before Phase 6.1 (engine) until Admiral confirms:

1. **Q1** — proceed with parallel `engine/momentum/premarket.py` module (recommend YES)
2. **Q2** — flags handling: skip / minimal earnings flag / block (recommend SKIP for v1)
3. **Q3** — volume filter: use `dailyBar.v` with empirical fallback (recommend YES)
4. **Q4** — bundle the Race desktop nav fix into Phase 6 (recommend YES)
5. Acknowledge Q5 (restart required at closure)

If Admiral defaults all five to recommended, Phase 6.1 through 6.6 proceed as 4 engine/endpoint/UI/JS commits + 1 closure commit. Plus optional ~3-line nav commit (Q4).
