# Investigation 6 — HM-AM (Total Portfolio Unification) — RETROSPECTIVE

**Filed:** 2026-05-10 by Scotty (loose-ends sweep)
**Status:** All four phases shipped 2026-05-07. Retrospective only.
**Linked:** `engine/total_portfolio.py`, `docs/TOTAL_PORTFOLIO.md`, `docs/XO_BACKLOG.md` HM-AM section.

## What the directive asked for

> Produce `hm_am_scope.md` covering: data sources, drift problem, proposed unified schema, migration plan in 3–5 phases, risk assessment, multi-week effort.

## Why this is now retrospective

Per `docs/XO_BACKLOG.md`: **"ALL PHASES SHIPPED 2026-05-07."**

| Phase | Commit | What landed |
|---|---|---|
| 1 — Data layer | `4f0bcff` | `engine/total_portfolio.py` — `get_total_portfolio()` + `get_portfolio_summary()`, 30s TTL cache, per-source resilience flags |
| 2 — Kirk integration | `d338605` | Kirk advisory switched `_load_real_holdings()` → unified portfolio |
| 3 — Advisory Team prompts | `d6c9647` | Advisory Team prompt context expanded |
| 4 — dalio-metals realignment | `52d7298` | dalio-metals prompts use unified portfolio |

First smoke (per `docs/OPS_LOG.md` 2026-05-07 entry):
```
total_value:    $138,371.20
total_cash:     $104,308.93
total_invested: $34,062.27
positions:      22 (11 Schwab + 2 metals + 9 Alpaca paper)
sources_loaded: ["schwab", "metals", "alpaca_paper"]
sources_failed: []
```

## Data sources unified (delivered)

- **Schwab** real cash account → via `data/real_holdings.json` (watcher-driven CSV import)
- **Dilithium Reserve** (physical metals) → via `metals_ledger` table
- **Alpaca paper** → via `AlpacaBridge.status()` (was `.account()`; smoke-test surface fixed during Phase 1)
- Per-source resilience: failures surface via `sources_failed` list rather than crashing the module.

## What's NOT in HM-AM (intentional, per scope)

- **Webull** — being wound down (CLAUDE.md "Broker Accounts" section). Read-only display only, not in unified API.
- **IBKR** — monitor-only; out of scope.
- **Internal AI fleet `positions` table** — that's the research book, NOT the real-money portfolio. Two-Book Bridge Policy keeps them separate by design (`CLAUDE.md` "Architecture: Two-Book Bridge Policy").

## Residual / followup ideas (NOT tickets, just observations)

1. **Dashboard surfacing** — `engine/total_portfolio.py` is consumed by Kirk + Advisory Team + dalio-metals. Whether the dashboard's existing per-source panels should be replaced by a single unified panel is a UX call, not a data-layer one. Defer until Captain says yes.
2. **Daily reconciliation report** (HM-I-β Item 5) — replaces the ε canary; surfaces internal-vs-Alpaca drift via NTFY when thresholds exceeded. Mentioned in CLAUDE.md "Open followups (HM-I-β, deferred to future sessions)." Independent of HM-AM but related.
3. **Webull → Starfleet label rename** (per CLAUDE.md "Pending TODOs") — keeps internal id `section-webull`; touches dashboard `dashboard/static/index.html` only. Low priority; ~30 min if done in isolation.

## Risk assessment of NOT doing further work

- **None.** The unified API works, three consumers wired, smoke passes. The "drift problem" the directive references was real before 2026-05-07; it's now solved.

## Recommendation

Close Investigation 6 as **retrospective — done.** No new design work. If the dashboard surfacing comes up, file as a separate HM-AM-β ticket (already named in `docs/SCOTTY_AUDIT_2.md` Section H as a hygiene pass).
