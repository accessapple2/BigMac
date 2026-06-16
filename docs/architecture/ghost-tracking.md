# ghost-tracking.md

> Relocated from CLAUDE.md by HM-PRIME Part C (move, not delete).

## Ghost Tracking Architecture (two systems, established HM-BC 2026-05-11)

**OllieTrades has TWO ghost-tracking systems with orthogonal concerns.** They
share a SQL table name (`ghost_trades`) but live in different DBs with
different schemas.

| | `engine/ghost_scoring.py` | `engine/ghost_trades.py` |
|---|---|---|
| **Purpose** | Signal-center agent win-rate scorecard | OllieTrades fleet decision log + missed-opportunity stats |
| **Storage DB** | `data/ghost_trades.db` (own DB) | `data/trader.db` (shared, sacred) |
| **Reads from** | `signal-center/signals.db::trade_signals` (BUY signals, conf ≥ 70) | n/a — writer-only |
| **Written by** | Own pipeline: `capture_new_signals()` + `check_outcomes()` | `ai_brain.py::log_ghost_trade` (HOLD>0.6) + `ghost_advisor.py` (BUY/SELL) |
| **Outcome resolver** | Live Alpaca bars + signal-center `signal_outcomes` | Computed at SELL time vs `ghost_portfolio.avg_cost` |
| **Agent universe** | signal-center agents (etf_regime_trader, danelfin_ai, chekov, navigator, …) | OllieTrades fleet (ollie_super_trades, trailing_stop, kirk_advisory_log, …) |
| **Dashboard endpoints** | `/api/ghost/scorecard`, `/api/ghost/trades`, `/api/ghost/refresh` | `/api/ghost-trades`, `/api/ghost-trades/stats` |

### Naming discipline
- `ghost_scoring.py` (renamed from `ghost_trader.py` in HM-BC.2) = the
  signal-center pipeline.
- `ghost_trades.py` (the HM-AZ/HM-BB module) = the trader.db decision log.
- "Ghost trades" user-facing term can mean either — API path is the
  disambiguator. `/api/ghost/*` = scoring. `/api/ghost-trades*` = decision log.

### Do not consolidate
Two modules export non-overlapping functions, read different DBs, serve
different agent universes. A previous attempt to treat one as stale (HM-BB
closure note misread the singular file as obsolete) would have silently
broken the live `section-ghost-scorecard` panel. **Future cleanup must verify
both UI panels still render before touching either module.**
