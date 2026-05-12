# Phase 4 Discovery — 2026-05-10 (Scotty / Detail panel)

**Log:** `/tmp/scotty_phase4_discovery_20260510_1938.log`

## Phase 1/2 endpoints live
- `/api/momentum/heartbeat` → 200
- `/api/momentum/recent_signals` → 200
- `/api/momentum/race` → 200 ✅ (Phase 2)
- `/api/momentum/scanner` → **404** — Phase 3 was never executed in this session

## Module inventory

```
engine/momentum/
├── __init__.py      (0 B, Phase 1)
├── bridge.py        (Phase 1)
└── race.py          (Phase 2)
```

**Missing (directive expects them):**
- `engine/momentum/flags.py` — Phase 3 (directive's `compute_detail()` imports `get_flags_bulk` from it)
- `engine/momentum/scanner.py` — Phase 3

## Alpaca multi-timeframe bars

**Already exists, battle-tested:** `engine/market_data.py::get_alpaca_bars(symbols, timeframe="1Day", days=30)`

```python
def get_alpaca_bars(symbols, timeframe: str = "1Day", days: int = 30) -> "pd.DataFrame | dict":
    # Single symbol → DataFrame. List → dict {symbol: DataFrame}.
    # Calls /v2/stocks/bars?timeframe=...&start=...&limit=...&feed=iex&sort=asc
```

- 8+ existing callers (ghost_trader, squeeze_scanner, chekov_autotrade, regime_ma, daily_enrichment, alpha_signals, etc.)
- Accepts any Alpaca timeframe string: "1Min", "5Min", "15Min", "1Hour", "1Day"
- IEX feed, 15s timeout, returns empty DataFrame on failure
- Returns columns Open/High/Low/Close/Volume with DatetimeIndex — pandas-native, drop-in for yfinance

## Chart library — *already on the page*

Two libraries already loaded in `dashboard/static/index.html`:
- **`lightweight-charts@4.2.2`** (TradingView) — CDN-included at line 13898, used by the existing `openTickerDetail` modal
- **TradingView Advanced Chart Widget** — embedded at line 25985, used for full-screen chart

Frontend React tree has `recharts ^2.15.0` in `dashboard/frontend/package.json`, but that tree remains unmounted (same as Phase 2 finding).

**Conclusion:** no need for SVG sparklines or new chart dep. Reuse `lightweight-charts` already loaded.

## 🎯 EXISTING `openTickerDetail()` modal — comprehensive, already wired

**Location:** `dashboard/static/index.html:13079` — `function openTickerDetail(symbol)`. Modal element id `#posDetailModal`.

**Already provides:**
- Lightweight-Charts multi-timeframe chart (1D / 5D / 1M / 3M / 1Y) with indicator toggles (SMA20/50/200, EMA9)
- Live price + percent change (from `/api/price/{symbol}`)
- RSI(14), Volume Ratio, 30d Range (from `/api/market/mtf/{symbol}`)
- **Crew Consensus** (from `/api/consensus`) — entries per agent + consensus action + vote pct
- **Chekov's Convergence** section
- **🖖 Kirk's Recommendation** section
- External links: Yahoo Finance, Finviz
- Action buttons: "Debate in War Room", "Full Chart" (TradingView advanced)
- Modal overlay class `pos-modal-overlay` (open/close via `.open` class — see CSS line 1076-1077)

**Already wired from many surfaces:** lines 3601, 3941, 4747, 4799 all call `openTickerDetail(symbol)`. Phase 2's `raceOnRowClick` falls through to it too via the graceful-fallback chain.

## Per-ticker signal filter capability

- `signals` table: `symbol` column present ✅, but no symbol index (only `idx_signals_player_ts` on player+time and `idx_signals_status` on status)
- Sample: `SELECT COUNT(*) FROM signals WHERE symbol='AAPL'` returns 3,097 rows — full-table-scan but a single per-click query is fine
- Last 24h: only **3 signals total** (Saturday/Sunday quiet)

## Fundamentals tables in `trader.db`

| Table | Schema |
|-------|--------|
| `stock_fundamentals` | `symbol TEXT PK, data TEXT, smart_score INT, grade TEXT, updated_at TEXT` (`data` is a JSON blob) |
| `earnings_universe` | `id, ticker, added_date, created_at` |
| `earnings_impact` | `id, symbol, report_date, expected_eps, actual_eps, beat_miss, price_reaction_1d` |

**No `polygon_fundamentals` table** (directive expected it). **No `earnings_calendar` table** (directive expected it).

## URL-state + modal pattern in static dashboard

- `URLSearchParams` used at line 19548 (some existing function) — pattern is in-house
- Existing modal pattern: `.pos-modal-overlay.open` (lines 1076-1077) — that's literally the modal `openTickerDetail` uses

## Endpoint count baseline

619 (Phase 1+2). Phase 4 expects +1 → 620.

## 🚨 Three blockers for Admiral — see `data/scotty_questions_phase4_20260510.md`

The directive's `compute_detail()` imports a non-existent module, the UI it builds duplicates a richer existing modal, and the fundamentals/earnings tables it references don't exist. Same "structural divergence" pattern as Phase 2.

Cannot proceed to Phase 4.1 without resolution.
