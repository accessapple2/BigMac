# Total Portfolio Reader

**Module:** `engine/total_portfolio.py`
**Phase:** **HM-AM Phase 1 SHIPPED 2026-05-07** — data layer only.
**Phases 2-4 (deferred):** consumer integration into Kirk advisory, Advisory Team, dalio-metals strategy realign. Each becomes its own ticket when ready.
**Captain mental-model:** "metals are an extension of the total portfolio." Schwab + Dilithium Reserve + Alpaca paper now reachable through one API.

## What it does

Aggregates three previously-siloed portfolio surfaces into a unified read-only view:

| Source | Accessor | Path |
|---|---|---|
| Schwab + TradeStation (real-money) | `_load_schwab()` | `data/real_holdings.json` (last_updated 2026-05-07) |
| Dilithium Reserve (physical metals) | `_load_metals()` | `metals_ledger` SQLite table aggregated by metal; spot prices from yfinance (GC=F, SI=F) |
| Alpaca paper | `_load_alpaca_paper()` | `engine.alpaca_bridge.AlpacaBridge.status()` + `.positions()` |

Returns a single `TotalPortfolio` dict with `positions`, `cash_by_account`, `total_value`, `total_cash`, `total_invested`, `last_updated`, `sources_loaded`, `sources_failed`.

## Per-source resilience

Each loader is wrapped in its own try/except. A single broken source does NOT silently degrade the unified view. Failures are recorded in `sources_failed` (string: `"<source>: <ExceptionType>: <message>"`); callers can inspect.

Examples:
- Schwab JSON missing → `"schwab: FileNotFoundError: ..."`
- Alpaca API auth fail → `"alpaca_paper: RuntimeError: Alpaca not connected: <reason>"`
- yfinance offline (metals spot) → metals positions still load with `market_value=None`

The two CS sources (Schwab, Alpaca) load independently; metals uses yfinance with a None-fallback for spot.

## Cache

30-second TTL, process-local. Matches the `engine/universe.py` precedent. Pass `force_refresh=True` to bypass.

## Data shape

```python
class Position(TypedDict, total=False):
    symbol: str
    account: str        # "schwab" | "tradestation" | "metals" | "alpaca_paper"
    qty: float
    avg_cost: Optional[float]
    market_value: Optional[float]
    asset_type: str     # "equity" | "etf" | "metal" | "option"
    notes: str

class TotalPortfolio(TypedDict, total=False):
    positions: list[Position]
    cash_by_account: dict[str, float]
    total_value: float
    total_cash: float
    total_invested: float
    last_updated: str
    sources_loaded: list[str]
    sources_failed: list[str]
```

## Public API

```python
from engine.total_portfolio import get_total_portfolio, get_portfolio_summary

tp = get_total_portfolio()                  # full unified view
summary = get_portfolio_summary()           # lightweight (no positions list)
fresh = get_total_portfolio(force_refresh=True)  # bypass 30s cache
```

## Standalone smoke

```
venv/bin/python3 engine/total_portfolio.py
```

Outputs JSON summary with totals + by-source position counts + sources_loaded/failed. First Phase 1 smoke output (2026-05-07):

```json
{
  "total_value": 138371.20,
  "total_cash": 104308.93,
  "total_invested": 34062.27,
  "position_count": 22,
  "by_source": {"schwab": 11, "metals": 2, "alpaca_paper": 9},
  "cash_by_account": {
    "schwab": 8393.71,
    "tradestation": 1353.43,
    "metals": 0.0,
    "alpaca_paper": 94561.79
  },
  "sources_loaded": ["schwab", "metals", "alpaca_paper"],
  "sources_failed": []
}
```

## Schema dependencies

- `data/real_holdings.json` — Schwab + TradeStation account data, populated by `engine/sync_schwab_to_real_holdings.py` from CSV imports (HM-AT-β pipeline ships from `~/autonomous-trader/inbox/`).
- `metals_ledger` table — physical metal purchases, schema in `docs/SCHEMA.md`. 7 rows as of 2026-05-07 (1 oz gold, 65 oz silver).
- Alpaca paper account — live broker state via `AlpacaBridge`.

## Phase 2-4 deferred work

Each becomes its own backlog ticket when prioritized:

| Phase | Scope |
|---|---|
| **Phase 2** | Kirk advisory integration — `engine/kirk_advisory.py` reads `get_total_portfolio()` instead of just `_load_real_holdings()`. Kirk sees the metals + Alpaca paper book alongside Schwab. |
| **Phase 3** | Advisory Team integration — Team prompts include the unified portfolio context (currently they see Schwab via Kirk only). |
| **Phase 4** | `dalio-metals` strategy realign — currently `dalio-metals` has its own metals view; consolidate to read from `total_portfolio` so the player_id's metals "holdings" match physical reality + Captain's actions. |

## Cross-references

- HM-AM (parent): `docs/XO_BACKLOG.md`
- `docs/SCHEMA.md` — `metals_ledger` schema
- HM-AT-β — Schwab CSV pipeline that feeds `real_holdings.json`
- HM-AU — Kirk advisory source routing audit (relevant for Phase 2 integration)
- Existing dashboard mentions `total_portfolio` as a local variable name — NOT the same module; no name collision concern at module level.
