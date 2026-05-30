# TRACKING-AWARE AGGREGATOR — SITE MAP (Phase 1, read-only)

Maps every realized-PnL / win-rate / scorecard rollup over `trades` for the dalio/tracking-pollution + contaminated-flag-adoption fix. **No code changed.** Built from 4 parallel read-only sweeps.

## The headline (reshapes the fix)
- **`alpaca_order_id IS NOT NULL` is the WHOLE fix.** Tracking-route players (dalio et al.) are log-only → their rows are **100% NULL-aoid** (dalio: 39/39 rows, all −255.08 of pollution). The aoid boundary (first real fill 2026-05-14) **inherently excludes all tracking pollution AND all pre-boundary garbage** in one predicate. Changes A (tracking-aware) and B (alpaca-boundary) **are the same one-line filter.**
- **No `ai_players.route_mode` column exists** (it's derived in paper_trader.py) → there's nothing to JOIN on anyway; the aoid boundary is the only practical tracking filter.
- **`trades_clean` view: 0 readers** (cosmetic). **`known_contaminated`: 0 rollup readers** (only setup_db.py DDL). Both dead — confirmed across all 4 sweeps.
- **NO shared chokepoint.** ~30 inline copies of `action='SELL' AND realized_pnl IS NOT NULL` across ~22 files. **Zero** filter on aoid. → a one-line fix is impossible; fixing in place leaves drift-prone copies.

## STRUCTURAL VERDICT: scattered → needs a shared helper
The only existing reused rollup is `agent_ratings.calculate_rating` (covers 5 endpoints). Everything else hand-rolls its own SQL with its own `_conn()`. **Recommendation:** new canonical module `engine/trades_filter.py` exposing a `CLEAN_TRADES_WHERE` constant (`alpaca_order_id IS NOT NULL`) + `fleet_realized_pnl(conn, player=None, season=None, since_days=None)` helper, adopted at every NEEDS-FIX site — so the boundary lives in ONE place, not 30.

═══════════════════════════════════════════════════════════════════════════════
## NEEDS-FIX SITES (rollups that currently include tracking/pre-boundary pollution)
═══════════════════════════════════════════════════════════════════════════════
Legend: filter shown is the CURRENT WHERE; all lack `alpaca_order_id IS NOT NULL`. 🔴=live-decision/go-to-money, 🟠=scorecard/report, 🟡=LLM-prompt-context.

### A. brain_context / fleet_cache → LLM prompt context (🟡)
| site | computes | current filter | feeds | 
|---|---|---|---|
| brain_context.py:401 `get_fleet_recent_trades` | fleet 7d WR% | `executed_at>-7d` ONLY | prompt fleet block |
| brain_context.py:442 `get_strategy_leaderboard` | per-strategy WR+SUM(pnl) | date + `realized_pnl IS NOT NULL` | prompt strategy block |
| brain_context.py:481 `get_hot_agents` | per-agent SUM(pnl)+WR | date + `realized_pnl IS NOT NULL` | prompt top/cold (dalio −255 lands here) |
| brain_context.py:521 `get_danger_tickers` | per-symbol loss SUM | date + `realized_pnl<0` | prompt danger block |
| fleet_cache.py:71 `_refresh_cache` (×4 aggs) | caches the 4 above | inline, no aoid | `get_fleet_context` → **every agent's prompt** |
| brain_context.py:255 `_source_backtest_performance` | single-player/symbol WR | player+symbol+SELL | that agent's own prompt | *(borderline: single-player, low pri)*

### B. agent_ratings → Fleet Report Card (the ONE shared helper, 5 endpoints) (🟠)
| site | computes | current filter | feeds |
|---|---|---|---|
| **agent_ratings.py:81 `calculate_rating`** | per-agent WR/PnL/PF/A–E | player+SELL+`realized_pnl IS NOT NULL`+season | `/api/ratings`,`/ratings/*`,`/fleet-report-card`, main.py:2587 → **Fleet Report Card** (fix once → 5 endpoints) |
| agent_ratings.py:307 `detect_cold_agents` | per-agent recent-5 WR | player+SELL+`realized_pnl IS NOT NULL` (NO season) | `/api/ratings/cold`, lineup bench (separate inline query — also needs fix) |

### C. dashboard inline (app.py) → on-screen panels (🟠)
| site | computes | current filter | feeds panel |
|---|---|---|---|
| app.py:2764/2794/2814/3010 leaderboard cluster | per-player WR/PF/season-PnL | SELL+`realized_pnl IS NOT NULL`(+season) | Arena Leaderboard + Capital sidebar |
| app.py:3653 `cockpit_snapshot` dispatcher_mix_24h | per-provider 24h PnL+W/L | `executed_at>-24h` ONLY ⚠️ no pnl filter → pulls BUY rows | Command Bridge "Dispatcher Mix" |
| app.py:6081 `arena_analytics` | per-player WR/PF/streak (FIFO price-match) | `player_id` only | Performance Analytics view (apply aoid to fetch SQL) |
| app.py:9131 `get_performance` | fleet WR/total_pnl/PF | SELL(+season)(+fleet_only) | STATS panel |
| app.py:9417 `get_performance_by_model` | per-player WR/PnL (regex from reasoning) | player(+season) | Model Leaderboard (aoid also kills the fragile regex fallback) |
| app.py:21002 `agent_scoreboard` | per-agent WR/total_pnl/PF | `realized_pnl IS NOT NULL` ONLY ⚠️ | P12 Accuracy Scoreboard (dalio lands directly) |
| app.py:21048 `agent_affinity` | per-agent×ticker WR+PnL | `realized_pnl IS NOT NULL` ONLY ⚠️ | P13 Affinity Matrix |
| app.py:12418 `fleet_report_card_alias` 7d_summary | fleet 7d PnL+W/L | `executed_at>-7d` ONLY ⚠️ no pnl filter | Fleet Report Card 7d layer |
| app.py:9448 `get_equity_curve` | fleet cumulative PnL | SELL(+season) | **⚠️ NO in-page consumer (orphan)** — revive-or-retire decision |

### D. reports / NTFY / commander decisions (engine) (🔴 several)
| site | computes | current filter | feeds |
|---|---|---|---|
| 🔴 ollie_commander.py:141 `_get_agent_wr_pts` | 30d per-agent WR → commander pts | player+date+`realized_pnl IS NOT NULL` | **OllieScore commander decision** (W_AGENT_WR=0.20, live gate) |
| 🔴 ollie_commander.py:344 approved-trade stats | approved-decision WR | decision=APPROVE+`realized_pnl IS NOT NULL` | commander decision-quality report |
| 🔴 proving_ground.py:176/196 `_pull_*_trades` | 3 Sniper agents WR/Sharpe/maxDD | player IN(SNIPER)+exit+`realized_pnl IS NOT NULL` | **`ship_kill_evaluator` SHIP/KILL go-to-REAL-MONEY gate** + dashboard + NTFY |
| 🟠 cost_tracker.py:290/330/365 (×3) | per-player SUM/ROI/WR+A–F grade | is_active+SELL+`realized_pnl IS NOT NULL` | model ROI/efficiency panels (**the realized-PnL scorecard**) |
| 🟠 war_room.py:597 `post_super_agent_pipeline_take` | 24h fleet WR+PnL | `is_human=0`+`executed_at>-24h` (no aoid) | War Room/NTFY hot take (is_human=0 intends to keep alpaca-mirror; aoid aligns) |
| 🟠 strategy_breakdown.py:24 | per-(tf,player) WR/PF/grade | date+`realized_pnl IS NOT NULL` | strategy scorecard |
| 🟠 regime_analyzer.py:27 | per-(regime,player) WR/PnL → recs | date+`realized_pnl IS NOT NULL` | regime report + **allocation recommendations** |
| 🟠 season_manager.py:84 (season finalize) | per-player season WR | player+season ONLY ⚠️ **no pnl filter at all** | `season_history` scorecard (worst polluter) |
| 🟠 oddsmaker.py:28 `get_odds` | per-symbol/dir historical WR+avg | `realized_pnl IS NOT NULL`+!=0 (broadens → all-fleet) | OddsMaker odds on **live signals** (broadest fallback = whole fleet) |

### E. indirect / upstream + allocation (🔴/🟠)
| site | computes | current filter | feeds |
|---|---|---|---|
| 🔴 adaptive_strategy.py:140 `weekly_agent_review` | per-agent 30d WR → bench/sizing | SELL+`realized_pnl IS NOT NULL`+date | **agent_allocation sizing + auto-bench <30% WR** |
| trade_outcomes.py:200 `auto_record_closed_trades` | backfills `trade_outcomes` from trades | SELL+`realized_pnl IS NOT NULL` | upstream of `get_player_stats`/`get_strategy_stats` (fix here protects 2 readers) |

### F. additional inline aggregators to CONFIRM in design phase (flagged by sweep, scope TBD)
finmem_memory.py:156-192 (×4), trade_log.py:100, providers/base.py:1175, crew_scanner.py:3798, dayblade.py:1177, super_backtest_v2.py:178 — several may be backtest/training (out of live scope); confirm live-vs-backtest before including.

═══════════════════════════════════════════════════════════════════════════════
## NOT-FIX (do NOT touch — legitimate tracking display or out-of-scope)
═══════════════════════════════════════════════════════════════════════════════
- **metals_tracker.py / metals_commentary.py** — INTENTIONALLY show dalio/metals PnL (unrealized, from `metals_ledger`/`positions`, NOT `trades.realized_pnl`). This is their job. **Excluding here would be the regression.**
- **eod_scorecard.py** — forecast-accuracy scorecard (`forecast_scorecards`), not trade PnL. **signal_scorecard.py** — indicator WR. **adaptive_strategy.update_trust_scores** — `signals.outcome_pct`. All different tables, out of scope.
- **morning_briefing / telegram_alerts** — unrealized/portfolio-value, not realized trade rollups.
- **self_improvement.py:40** — single-player same-day; already excludes dalio explicitly (L178).
- **cto_advisor._gather_recent_trades** + various `/api/trades/*`, `/api/winners-losers` (unrealized) — per-row lists/counts, not rollups.

═══════════════════════════════════════════════════════════════════════════════
## DESIGN QUESTIONS THIS MAP RAISES (for review before Phase 2)
═══════════════════════════════════════════════════════════════════════════════
1. **A+B are one filter** — `alpaca_order_id IS NOT NULL` does both (excludes tracking AND pre-boundary garbage). Confirm we treat it as one change, not two.
2. **Shared helper vs ~30 inline edits** — strongly recommend the new `engine/trades_filter.py` helper (one boundary definition) over 30 in-place edits. Confirm the approach.
3. **proving_ground is the highest-stakes before/after** — its 3 Sniper agents have a pre-2026-05-14 trial month (NULL-aoid) feeding the SHIP/KILL real-money gate. The boundary DROPS that month. Intended cleanup or unwanted data loss? This needs explicit Phase-3 before/after + your judgment.
4. **Two orphan/bug findings (separate from the fix):** `get_equity_curve` has no consumer (revive/retire?); `benchmark.py` writes snapshots to `autonomous_trader.db` but reads `data/trader.db` (DB-constant inconsistency).
5. **season_manager + cockpit dispatcher_mix + fleet-report-card-7d don't even gate `realized_pnl IS NOT NULL`** → they pull BUY rows too; the boundary fix also tightens these.

*Phase 2 (design + change shape) and Phase 3 (before/after on every number) follow on your review of this map.*
