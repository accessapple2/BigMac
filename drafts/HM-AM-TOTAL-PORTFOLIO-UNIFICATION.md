HM-AM — Total Portfolio Unification (Real-World Net Worth)

Purpose: Single source of truth for Admiral's real-world 
net worth across all REAL brokers + physical assets. 
EXCLUDES Alpaca paper (paper-only research surface). This 
has been deferred for weeks per memory; queued behind 
Chrome audit work. Now time to break ground.

Mode: Architecture-first → backend producer → API → 
      dashboard surface
Effort: ~3-4 sessions total (architecture + backend + UI)
Output: Aggregate live net-worth widget + per-broker breakdown

══════════════════════════════════════════════════════════
SCOPE
══════════════════════════════════════════════════════════

INCLUDED (real assets):
  - Schwab — primary real account (162 holdings as of 
    today, $9,634 metals via ledger, plus equity positions)
  - Webull — real monitor-only (~$6.6K per memory)
  - IBKR — active, $0 currently per memory
  - Physical metals — Dilithium Reserve (1 oz gold + 
    65 oz silver = $9,634.10 today)

EXCLUDED:
  - Alpaca paper account ($99,885 equity) — RESEARCH ONLY
  - Any test/sandbox account
  - Plutus paper bench accounts

DATA SOURCES (verify in Phase 1):
  - Schwab: import_schwab_csv pipeline → real_holdings.json
  - Webull: needs investigation (CSV import? API?)
  - IBKR: needs investigation (CSV import? API?)
  - Metals: metals_ledger SQL (truth source per today's 
    Round 3 Item 3 fix)

══════════════════════════════════════════════════════════
PHASE 1 — ARCHITECTURE AUDIT
══════════════════════════════════════════════════════════

# 1.1 — Inventory existing data sources
ls -la ~/autonomous-trader/data/*holdings*.json 2>/dev/null
ls -la ~/autonomous-trader/data/*portfolio*.json 2>/dev/null
ls -la ~/autonomous-trader/inbox/ 2>/dev/null

# 1.2 — Existing endpoints that touch real money
grep -rn "real_holdings\|real_portfolio\|webull\|ibkr\|schwab" dashboard/app.py engine/ 2>/dev/null | head -30

# 1.3 — Schema check
sqlite3 ~/autonomous-trader/data/trader.db ".schema" | grep -iE "schwab\|webull\|ibkr\|holdings\|portfolio" 2>&1 | head -20

# 1.4 — Current Kirk Advisory state (real-world consumer)
grep -rn "kirk_advisory\|kirk_alerts\|real_holdings" engine/ 2>/dev/null | head -20

══════════════════════════════════════════════════════════
PHASE 2 — DATA INTEGRATION PLAN
══════════════════════════════════════════════════════════

For each non-Schwab source, decide:

WEBULL
  Question: How does data currently enter the system?
  Options:
    (a) Manual CSV export same pattern as Schwab → inbox/ → 
        watcher → parser → real_holdings.json (Webull section)
    (b) API integration if Webull provides one
    (c) Static JSON the Admiral updates manually
  Recommend: (a) — mirror Schwab pattern, lowest risk

IBKR  
  Question: Current state?
  Per memory: "active, $0 currently" — may not need 
  immediate pipeline if account is empty.
  Recommend: Stub the integration — empty broker placeholder 
  in aggregator, document the CSV import path for future 
  activation. Don't build until there's actual data.

METALS
  Already wired post-Round 3 Item 3 (metals_ledger SQL).
  Just read from same source.

SCHWAB
  Pipeline live and proven. Reuse import_schwab_csv chain.

══════════════════════════════════════════════════════════
PHASE 3 — UNIFICATION SCHEMA
══════════════════════════════════════════════════════════

Proposed table: real_total_net_worth

CREATE TABLE IF NOT EXISTS real_total_net_worth_snapshots (
    snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    schwab_cash REAL,
    schwab_positions_value REAL,
    schwab_total REAL,
    webull_cash REAL,
    webull_positions_value REAL,
    webull_total REAL,
    ibkr_cash REAL,
    ibkr_positions_value REAL,
    ibkr_total REAL,
    metals_physical_value REAL,
    grand_total REAL,
    snapshot_source TEXT  -- 'producer' or 'manual'
);

Producer cron: every 60s during market hours, daily after 
hours. Computes grand_total = sum of all 4 sources.

Endpoint: GET /api/real-portfolio/total
Returns:
{
  "grand_total": 53210.47,
  "as_of": "2026-05-18T16:30:00Z",
  "breakdown": {
    "schwab": {"total": 27106.16, "cash": 0, "positions": 27106.16},
    "webull": {"total": 6600.00, "cash": 100, "positions": 6500.00},
    "ibkr": {"total": 0, "cash": 0, "positions": 0},
    "metals": {"total": 9634.10, "gold": 4568, "silver": 5066.10}
  },
  "last_updated_per_source": {
    "schwab": "2026-05-18T13:17:11Z",
    "webull": "2026-05-17T16:00:00Z",
    "ibkr": "2026-05-15T12:00:00Z",
    "metals": "2026-05-18T19:30:00Z"
  }
}

══════════════════════════════════════════════════════════
PHASE 4 — DASHBOARD SURFACE
══════════════════════════════════════════════════════════

Per memory: "Dashboard surface deferred."

Decision: build the API in Phase 3, defer the UI panel until 
API is soaked 7 days. Initial UI is bare-bones JSON viewer 
in Starfleet tab. Polished panel comes in HM-AM-V2.

Initial UI scope (HM-AM-V1):
  - Add new section "Total Net Worth" in Starfleet tab
  - Single big number: grand_total
  - 4-row breakdown: Schwab / Webull / IBKR / Metals
  - "Last updated" timestamp per source
  - NO charts in V1 (defer)
  - NO time-series in V1 (defer until snapshots accumulate)

══════════════════════════════════════════════════════════
PHASE 5 — DEPENDENCIES + UNBLOCKERS
══════════════════════════════════════════════════════════

HM-AM blocks downstream work:
  - HM-AN (Morpheus port 9000 reframe) depends on HM-AM 
    per memory
  - Future "real account vs paper account" comparison 
    dashboards
  - Tax alpha tracking (cost-basis aggregation)

HM-AM is blocked by:
  - Webull data ingestion path decision (Admiral input 
    needed in Phase 2)
  - IBKR scope decision (active integration vs stub)

Admiral checkpoint required at:
  - End of Phase 2 (data integration path decisions)
  - End of Phase 3 (schema approval before producer build)
  - End of Phase 4 (UI placement approval)

══════════════════════════════════════════════════════════
EXECUTION SCHEDULE
══════════════════════════════════════════════════════════

Session A (first fire):
  - Phases 1+2 complete
  - Admiral brief on Webull + IBKR ingestion paths
  - No code shipped

Session B:
  - Phase 3 schema + producer
  - Ship producer + endpoint
  - Soak 24h

Session C:
  - Phase 4 V1 UI in Starfleet tab
  - Browser smoke
  - Ship

Session D (after 7-day soak):
  - HM-AM-V2 polished UI + charts

══════════════════════════════════════════════════════════
GUARDRAILS
══════════════════════════════════════════════════════════

- This is REAL money tracking — no margin for error
- NEVER auto-trade on real brokers (Schwab/Webull/IBKR)
- Alpaca paper account NEVER included in real net worth 
  aggregate (paper ≠ real, sacred boundary)
- Schwab CSV import path is canonical and proven — DO NOT 
  refactor it as part of this work
- Sacred DB rule applies: never DELETE from real_holdings, 
  metals_ledger, or schwab_holdings
- Manual entry path required for offline brokers — Admiral 
  may update Webull holdings manually some weeks
- If a source returns null/error, real_total_net_worth uses 
  LAST KNOWN GOOD value, never zero (zero would corrupt 
  time series)
- Frontend Ship Rule applies to UI work
- ANY producer crash logs to NTFY ollietrades-admin 
  (real-money tracking failure is a P0 alert)

END HM-AM-TOTAL-PORTFOLIO-UNIFICATION
