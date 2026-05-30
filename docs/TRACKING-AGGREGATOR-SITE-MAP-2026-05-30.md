# TRACKING-AWARE AGGREGATOR — SITE MAP (Phase 1, read-only)

Maps every realized-PnL / win-rate / scorecard rollup over `trades` for the dalio/tracking-pollution + contaminated-flag-adoption fix. **No code changed.** Built from 4 parallel read-only sweeps.

## ⚠️ CORRECTED BOUNDARY (2026-05-30) — `alpaca_order_id IS NOT NULL` RETRACTED as the global filter
**An earlier draft of this map concluded "aoid IS NOT NULL is the whole fix." That is WRONG and retracted.** The
proving-ground provenance dig proved why: **aoid conflates "not real Alpaca" with "dirty."** Post-2026-05-14, **72% of
clean fleet performance is `execution_type='simulated'`** (143 trades / 11 agents / +$124.84, vs only 56 alpaca_paper
trades / 2 agents) — legitimate paper-sim evaluation, NOT garbage. `alpaca_order_id IS NOT NULL` would **erase clean
post-boundary performance for 11 of 13 agents**, gutting the leaderboard/ratings/scorecard. The contamination is
**pre-boundary mispricing garbage** (impossible prices: MU $533, TSLA $4 — gemini-2.5-pro alone carries +$225K of it),
not "non-Alpaca."

### The correct filter = TWO predicates (`CLEAN_TRADES_WHERE`)
1. **`executed_at >= '2026-05-14 07:37:44'`** — date floor; drops the pre-S5 mispricing garbage (affects ALL agents). Post-05-14 garbage spot-check = **empty** (the floor cleanly bounds it).
2. **`player_id NOT IN (<TRACKING_PLAYERS>)`** — drops manual-SQL tracking pollution incl. dalio's **post**-boundary ONDS row that the date floor alone misses.

**TRACKING_PLAYERS = `('dalio-metals','enterprise-computer','schwab')`** — derived from `engine/paper_trader.py`
`_EXECUTION_PORTFOLIO_BY_PLAYER` → portfolios with `execution_mode='tracking'` OR `type='physical'` (Enterprise
Computer + Schwab). **Only `dalio-metals` actually has trades rows today** (18, −255.08); the other two are 0-row
belt-and-braces/future-proofing. (No `route_mode` DB column exists — this set IS the code-precise mirror.)

**VBC proof (compose-correctly):** dalio → **0.0** (both predicates) · clean sim qwen3-8b-flash +$57.24/aoid=0 **kept** ·
fleet **$237,423 (polluted) → $270.35 (clean)** · post-05-14 garbage spot-check empty.

### proving_ground EXCEPTION — predicate-1 ONLY (date floor, NO tracking-exclusion)
The Sniper agents (deepseek-7b-grok4, ollama-plutus, neo-matrix) are **sim-evaluation** agents — deepseek & plutus are
100% `simulated`/0-aoid post-boundary BY DESIGN (evaluated in sim before shipping to real money). They are NOT tracking
players. proving_ground uses **`executed_at >= '2026-05-14'` only** — drops the confirmed-contaminated pre-boundary month
(WR-holds-post-boundary: deepseek 79→100%, neo-matrix 25→94.6%, plutus 84.7→100%; 59 clean post-trades suffice the gate),
keeps the clean sim it needs. `SIM_EVAL_WHERE` = predicate-1 only.

- **`trades_clean` view: 0 readers** (cosmetic — and now also wrong-definition: it uses aoid). **`known_contaminated`: 0 rollup readers** (only setup_db.py DDL). Both dead.
- **NO shared chokepoint.** ~30 inline copies of `action='SELL' AND realized_pnl IS NOT NULL` across ~22 files. **Zero** apply the date floor or tracking exclusion. → fixing in place leaves drift-prone copies; a shared helper is required.

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

═══════════════════════════════════════════════════════════════════════════════
## PHASE 2 — THE DESIGN (proposed, not built)
═══════════════════════════════════════════════════════════════════════════════
### New module: `engine/trades_filter.py` (one place the boundary lives)
```python
# Tracking-route players: portfolio execution_mode='tracking' or type='physical'
# (mirror of engine/paper_trader.py _EXECUTION_PORTFOLIO_BY_PLAYER → Enterprise Computer/Schwab).
# Their `trades` rows are log-only / manual-SQL pollution, never real performance.
TRACKING_PLAYERS = ("dalio-metals", "enterprise-computer", "schwab")
GARBAGE_FLOOR = "2026-05-14 07:37:44"   # first real Alpaca fill; pre = pre-S5 mispricing garbage

# Fleet/dashboard/scorecard rollups → drop garbage AND tracking pollution:
CLEAN_TRADES_WHERE = (
    f"executed_at >= '{GARBAGE_FLOOR}' "
    f"AND player_id NOT IN ({','.join(repr(p) for p in TRACKING_PLAYERS)})"
)
# Sim-evaluation gates (proving_ground) → drop garbage ONLY, keep clean sim:
SIM_EVAL_WHERE = f"executed_at >= '{GARBAGE_FLOOR}'"

def fleet_realized_pnl(conn, player=None, season=None, since_days=None, sim_eval=False) -> dict:
    """Per-player (or single-player) {wins, losses, total_pnl, trade_count, win_rate}
    over CLEAN trades. sim_eval=True uses SIM_EVAL_WHERE (proving_ground)."""
    where = SIM_EVAL_WHERE if sim_eval else CLEAN_TRADES_WHERE
    # ... action IN ('SELL','COVER') AND realized_pnl IS NOT NULL AND {where} [+player/season/since] ...
```
**Why a helper, not 30 inline edits:** the boundary is replicated ~30× today with zero consistency; one constant means
the next new rollup is clean by construction and the floor/tracking-set changes in ONE place.

### Adoption — the ~22 NEEDS-FIX sites route through it
- **5 endpoints collapse via `agent_ratings.calculate_rating`** (fix its WHERE once).
- **brain_context (4) + fleet_cache (4 cached aggs)** → adopt `CLEAN_TRADES_WHERE`.
- **dashboard app.py (9 inline)** → adopt (leaderboard cluster, cockpit dispatcher-mix, analytics, performance, by-model, scoreboard, affinity, report-card-7d; equity-curve = revive/retire first).
- **commander/reports (11)**: ollie_commander (2, live gate), cost_tracker (3, scorecard), war_room, strategy_breakdown, regime_analyzer, season_manager, oddsmaker → adopt.
- **indirect (2)**: adaptive weekly_review + trade_outcomes backfill → adopt at the upstream SELECT.
- **proving_ground.py:176/196** → adopt `SIM_EVAL_WHERE` (predicate-1 only).
- **NOT touched**: metals_tracker/commentary (legit tracking display), eod_scorecard/signal_scorecard (different tables), unrealized/list endpoints.

### Two orphan/bug findings to resolve alongside (not part of the filter)
- `get_equity_curve` (app.py:9448) — no in-page consumer → revive or retire (decide before adopting).
- `benchmark.py` — writes snapshots to `autonomous_trader.db` but reads `data/trader.db` (DB-constant inconsistency) — fix or document.

═══════════════════════════════════════════════════════════════════════════════
## PHASE 3 — BEFORE/AFTER (to run on the helper, before ship)
═══════════════════════════════════════════════════════════════════════════════
Predicted (from VBC): **dalio → 0** · **fleet realized $237,423 → ~$270** (delta = pre-05-14 garbage + dalio, NOT clean sim) ·
**11 sim agents' clean post-05-14 sim PRESERVED** (the thing aoid would've broken) · **proving_ground keeps deepseek+plutus+neo-matrix
clean post-05-14 sim, drops the contaminated month**. NOTE: ollie-auto/neo-matrix **do shift** (they carry pre-05-14 garbage too:
ollie-auto +98.79 pre dropped, neo-matrix +26.91 pre dropped) — correct garbage-removal, not data loss; their post-05-14 data is unchanged.
Phase 3 will tabulate every affected site's number before/after on your review of this design.
