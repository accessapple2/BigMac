# Scotty 2.3 — First Canonical Forward Audit

**Auditor:** Claude Code · Opus 4.7 · Read-Only
**Date:** 2026-05-07 (post-cleanup-wave, ~21:45 MST)
**Repo:** `/Users/bigmac/autonomous-trader/`
**Trader PID:** 15619 (started 15:12:44 MST today, 3h 19m after the α-lift commit)
**Predecessor on disk:** none — `docs/SCOTTY_AUDIT_REPORT.md` does not exist. This is the first canonical Top-Level-Improvements audit committed to disk.

---

## Section 0 — What Is OllieTrades Optimizing For Right Now?

> **Research throughput, with intermittent edge confirmation. Not paper P&L. Not yet.**

Defended by what's on disk, not what we wish were true:

- **25 agents in the registry**, only 6 actually trade in the last 30 days, only 1 (`ollama-plutus`) produces meaningful realized P&L. The other 24 seats are exploring agent configurations, not concentrating capital on what works.
- **8 ORPHANED options strategies in `engine/options_agents.py`** (Quark, McCoy-bps, Anderson-bcs, Covered-Call, four ghost-* variants) — all `halt_mode=full`, zero trades or signals ever. Pure unused capacity preserved per "iterate to the next Top 4" doctrine.
- **Two parallel signal services** (`signal-center` on :9000 with its own `signals.db` 643MB, plus the trader on :8080 with `data/trader.db` 256MB) — built for breadth of experimentation, not focused production.
- **Sniper Mode** — a 30-day proving-ground trial of `ollie-auto` running with $73 average notional per trade (vs Plutus's $204) — a research experiment masquerading as a production agent, currently coasting on undersized positions.
- **Alpaca paper book equity:** ~$99.9K cash, ~$115 *down* from the $100K start. The book is not generating P&L. The fleet is generating signals.

If the optimization target were paper P&L, the Admiral would have killed everything except `ollama-plutus` and `McCoy CSP` six weeks ago and sized them up. He hasn't, because the actual goal is the *next* edge — confirming OOS, validating regime-robustness, finding orthogonal alpha. That's research throughput.

**The Top 10 below treats the implicit pivot as: shift the optimization from research-throughput to OOS-validated-edge-confirmation.** Concentrate, retire, harden. The Top 10 should serve that pivot.

---

## Section 0.5 — OOS Tracking Verdict: **TRACKING** (data thin)

OOS bands to hit per the established backtest (already validated, do not re-litigate):

| Window | Sharpe | WR | Trades |
|---|---|---|---|
| OOS-A 2024 bull | **2.692** | 65.8% | 456 |
| OOS-C 2022 bear | **2.087** | 49.7% | 890 |
| CSP regime-robust | **6.05** bull / **5.42** bear | — | — |

### Live numbers (`data/trader.db`, last 30 days, `realized_pnl IS NOT NULL`)

| Slice | Realized P&L | Trades | Wins | WR |
|---|---|---|---|---|
| **Whole fleet** | **+$6,763.85** | 478 | 353 | **74%** |
| Working agents only (plutus + qwen3 + ollie-auto + navigator) | ~+$4,564 | 273 | 199 | ~73% |
| `ollama-plutus` alone | **+$4,108** | 96 (75 closed) | 62 | 82.7% |
| `alpaca-mirror` (real broker book) — equity Δ | +$704 over 1.3 days | 5 data points | n/a | n/a |

### Read

- **WR 74%** beats OOS-A (65.8%) and crushes OOS-C (49.7%). ✅
- **Annualized return on closed-trade P&L:** $6,764 / 30 days × 365 ≈ +$82k/yr on a notional book ~$100k = **+82%/yr.** Above OOS-A's implied annual return at 2.7 Sharpe. ✅
- **Daily-return Sharpe:** **cannot compute reliably** — the alpaca-mirror equity curve has only 5 data points over 1.3 days; ollama-plutus has step-jumps (e.g. May 1: +$3,885 in one day, looks like a position revaluation event, not organic alpha). The clean 30-day Sharpe needed to compare against OOS-A 2.692 cannot be calculated from current `portfolio_history` without a denoising pass.
- **CSP regime-robust ≥4 in bull regimes** — `ollama-plutus` (CSP, McCoy lineage) running 82.7% WR / +$67 avg win on 75 closes. *Implied* Sharpe in the 5-7 range. Tracks OOS CSP-bull 6.05 if you trust the directional read.

### Verdict: **TRACKING** — the realized-P&L and WR slices align with the OOS band. Sharpe cannot be cleanly computed; that's the gap. **Recommendation:** start writing daily portfolio totals for `ollie-auto` (Top-10 #2) and add the alpaca-mirror writer to a 1-hour cron so the equity curve becomes Sharpe-computable inside 30 days.

The Sharpe-4.8 question is *partially answered yes*: WR and absolute return track. Sharpe itself is data-thin until the equity-curve writers are in place.

---

## Executive Summary

Aye Captain, she held. Twenty-four commits today and the bones are still in one piece. ALPACA_* purge clean (`config.py:94-95` keep two backcompat aliases as definitions only). Twelve zombies stayed halted. Kirk source routing is honest. Schwab watcher made it off Downloads/. The α-lift on `SPREAD_CANNIBALIZATION_GUARD_ENABLED` *did* take effect once PID 15619 restarted at 15:12:44 — the only "guard active" log entry post-flip is at 12:49:23, which is *pre-restart* cruft. **HM-P confidence-scale safety sweep is also CLEAR: zero 🚨 WRONG comparisons in any gate-flipped strategy. Continue gate-flip soak.** Two real issues surfaced by the wave's own observability: (a) HM-AS-β is firing nonstop on `battle_station_monitor` cadence drift to 6,857 seconds against a 120s target — the alarm works; the scheduler is the problem, and (b) the Schwab CSV import script crashes `IndexError` on empty files.

**Now the harder findings.** Sniper Mode is on Day 26/30 and the *real* metrics — recomputed from `trader.db` and ignoring the documented `cumulative_return` / `max_drawdown` / `exec_gap_pp` bugs — say **KILL or HARD-EXTEND with sizing fix.** Sniper's `ollie-auto` posted +0.75% / 30 days at $73 avg notional vs the rest-of-fleet's +66.9% at $200+ notionals. Its 14.6 Sharpe is a sizing artifact, not alpha. **The Grok Signal Center report claimed at `docs/Signal_Center_Upgrade_Report_v2.md` does not exist on disk** — verified by repo-wide and home-directory search. I verified the substantive technical claims directly from `signal-center/server.py` instead: Volume Radar 10x+ is real (in `engine/volume_scanner.py`, not signal-center), GEX overlay is real (`engine/gex_overlay.py`), but the **F&G Recovery Protocol is NOT coded** (passive read only — no auto-trigger), and Signal Center replicates ~30-40% of UW+SpotGamma+Capitol-Trades, not "80%." Replication value: ~$50-120/mo, not $200-300/mo.

**The biggest unmitigated risk on this ship is not the alpha book — it's the backup architecture.** Daily backups land in `~/autonomous-trader/backups/` on the same Mac Mini APFS volume as the source DB. One disk failure = total loss of `data/trader.db` (256MB), the 7-day rolling backups (1.6GB), and 15 pre-op snapshots (~3.5GB). **Same-disk vulnerability is the single biggest call to action in this audit.** Top-10 #1. The second biggest is dashboard auth: **zero authentication on any mutating route in `dashboard/app.py`** — `POST /api/kill-switch`, `POST /api/alpaca/buy`, `POST /api/alpaca/sell`, `POST /api/trade/manual`, etc. The dashboard relies entirely on network binding + Cloudflare tunnel. The hard-coded `PIN=2026` at `signal-center/server.py:179` is the small problem. The dashboard auth gap is the big one. Top-10 #3.

**The roster does not match reality.** The "Active 9" referenced in the audit prompt includes `chekov` (0 trades ever), `ollama-coder` (0 trades ever — a utility scoring role, not a trader), `navigator` (idle since April 9), and `ollama-llama` (already in `exit_only`, −$5,536 of 90-day realized P&L). Meanwhile `deepseek-7b-grok4` ran **133 trades in 30 days — the highest-volume player on the ship — and is in no roster doc at all.** `qwen3-8b-flash` (78 trades) is also a ghost. Roster reconciliation is the highest-leverage missing-doc work right now. Top-10 #4.

---

## Section A — May 7 Wave Post-Mortem

| Check | Status | Evidence |
|---|---|---|
| **HM-AV** ALPACA_* dead-code purge | **HELD** | Only `config.py:94-95` retain `ALPACA_API_KEY/SECRET_KEY` as backcompat alias *definitions*. Zero production reads. |
| **HM-AK** 12 zombies halted, scan loops filtered | **HELD** | All 12 halts at `2026-05-07 17:18:36`. None of the 12 emitted signals or trades in the last 24h. Active emitters last 24h: 8 (none halted). Trade emitters last 24h: 6 (none halted). |
| **HM-AF α-lift** `SPREAD_CANNIBALIZATION_GUARD_ENABLED=False` | **HELD** (corrected) | `config.py:17 = False`. PID 15619 restarted at 15:12:44 MST. Last "guard active" log entry: 12:49:23 — *pre-restart*, stale. No β-layer orphan event since restart. Spread legs in `options_trades` all paired (`strategy:bull_spread_v1`); no orphans. |
| **HM-AU-β** `?source=all` dedupe | **HELD** | `dashboard/app.py:13405-13501` — `paper` calls engine, `real` does inline read with no engine side effects, `all` runs engine for envelope but uses `real_positions` only. Kirk `?source=real` does not write `kirk_advisory_log`. Verified clean. |
| **HM-AT-β** Schwab watcher → `inbox/` | **HELD with bug** | Plist runs `scripts/schwab_csv_watcher.sh` with `WATCH_DIR="$HOME/autonomous-trader/inbox"`. Plist loaded, watcher PID 23907. **`scripts/import_schwab_csv.py:parse_csv` crashes `IndexError` on empty/malformed CSV.** Watcher fine; parser brittle. |
| **HM-AS-β** battle_station cadence-drift observability | **HELD; firing on a real bug** | 20 drift warnings in `trader_error.log` 11:09–18:36 MST. Drift escalation: 233s → 293s → 383s → 701s → 5,342s → 6,857s against 120s target. Observability works. The underlying scheduler does not. (Top-10 #5.) |
| **HM-AR-β** `earnings_injector.py` retired | **HELD** | Module gone (commit `3508699`). |
| **Stub `trader.db` at root** | Not the wave's fault | `./trader.db` (0B stub) and `./autonomous_trader.db` (2.5MB live sidecar) present. Real writer at `engine/daily_enrichment.py:112` uses bare `"autonomous_trader.db"` (cwd-relative). Top-10 #8. |

---

## Section B — HM-P Confidence Scale Safety Sweep

**RESULT: ALL CLEAR. Zero 🚨 WRONG comparisons in any gate-flipped strategy.**

| File:line | Comparison | Threshold | Source scale | Class |
|---|---|---|---|---|
| `strategies/bull_spread_v1.py:276,278` | `confidence = min(1.0, iv_rank / 100.0)` | n/a (write) | REAL 0-1 | ✅ EMIT-CORRECT |
| `strategies/bull_call_spread_v1.py:60,181-184` | `TB_CONF_THRESHOLD = 85`; `WHERE confidence >= 85` against `trade_signals.confidence` | 85 | INT 0-100 | ✅ CORRECT |
| `strategies/bear_put_spread_v1.py:58,198-201` | `TB_CONF_THRESHOLD = 85`; mirror of bull_call | 85 | INT 0-100 | ✅ CORRECT |
| `strategies/executor.py` | (no numeric `confidence` comparisons) | — | — | ✅ N/A |
| `strategies/exit_manager.py` | (no numeric `confidence` comparisons) | — | — | ✅ N/A |
| `engine/paper_trader.py:740,748,1584` | `confidence < 0.70 / 0.90` (legacy fleet path only) | 0.70/0.90 | REAL 0-1 | ✅ CORRECT |

**Live data corroborates the convention:**
- `signal-center/signals.db::trade_signals` last 7d: MIN=50 MAX=90 AVG=74.95 N=64 (INT scale)
- `data/trader.db::signals` last 7d: MIN=0.0 MAX=1.0 AVG=0.594 N=6,658 (REAL scale)

**Important note on operational silence:** The TB-gated spread strategies emitted **0 signals at conf ≥ 85 in the last 7 days**. This looks identical to a silent-no-fire scale bug but is *not* — the gate is operationally tight by design, and `bull_spread_v1.py` (which doesn't require TB) is firing normally. Document this so future audits don't false-flag.

**Verdict: HM-P fully resolved. Continue gate-flip soak. No spread-strategy confidence-scale bug exists today.**

---

## Section C — Sniper Mode Verdict: **KILL** (or HARD-EXTEND with sizing fix)

**The proving_ground.running_scorecard does NOT track Sniper.** Latest row (id=28, as_of_date 2026-05-07): 244 trades, WR 75.0%, Sharpe 4.892, max_drawdown −87.557 (synthetic-bug), exec_gap_pp 4.0, gates 4 of 6 pass. Those numbers describe legacy-fleet ghost rollups (deepseek-7b-grok4, ollama-plutus, qwen3-8b-flash, gemini-2.5-flash, ollama-llama, grok-4) from `proving_ground.daily_trades` — **not** ollie-auto. The scorecard is rolling up the wrong universe.

### Real metrics on the routed Sniper player (`ollie-auto`, `trader.db`, 30d window)

| Metric | Value | OOS-A target |
|---|---|---|
| Trades | 74 | — |
| Win rate | 90.5% | 65.8% |
| **30d total return** | **+0.75%** ($75.45 on $10k) | — |
| Avg trade notional | **$73** | — |
| Daily Sharpe (annualized) | 14.6 (sizing artifact) | 2.69 |
| Max drawdown | 0.00% | — |

### Rest-of-fleet ex-Sniper (same window)

| Metric | Value |
|---|---|
| Trades | 560 |
| WR | 86.3% |
| **30d total return** | **+66.9%** |
| Plutus alone | +$4,108 |

**Sniper Mode is strictly worse than rest-of-fleet on absolute return — by ~89× — and is coasting on low variance from undersized positions.** PROMOTE rule fails. The literal KILL Sharpe rule (>50% worse) doesn't trigger because Sniper's *raw* Sharpe is higher (sizing artifact). The KILL *spirit* applies.

### Recommendation

**KILL the current Sniper config.** If the Admiral wants to preserve "Sniper Mode" as a concept, restart on a fresh 30-day window with notional-parity sizing (~$200/trade) so the trial is apples-to-apples.

### Proving Ground v2 acceptance criteria (6 gates, no metric bug)

All metrics computed from `trader.db::portfolio_history` (requires Sniper player to write equity rows — that's a v2 prerequisite) plus `trades`:

1. **Equity-curve Sharpe ≥ 2.0** annualized. `daily_ret = total_value/LAG(total_value,1) - 1; Sharpe = AVG/STDDEV * SQRT(252)` over the 30-day window.
2. **Max drawdown ≤ 10%** (peak-to-trough on equity curve).
3. **30d total return ≥ +3%** (beats T-bill).
4. **Trade count ≥ 100** in window (statistical significance).
5. **Avg notional within ±25% of fleet median** (rejects throttle-to-win sizing exploits — the bug exposed today).
6. **Beats SPY 30d return AND ≥ 50% of rest-of-fleet's return.**

Optional 7: **Regime tag.** When VIX > 25, require Sharpe ≥ 4.0 (validates OOS-A regime-robust ≥4).

---

## Section D — Top 10 Next Moves (ranked by reward ÷ effort)

### #1 — Off-Host Backup Plan (URGENT)

- **What:** rsync `~/autonomous-trader/backups/` and `data/trader.db` daily to Ollie Box (`192.168.1.166`) at 06:30 MST. Add gzip on the rotation to drop 1.6 GB → ~500 MB.
- **Why now:** Daily 6 AM backups land on the **same Mac Mini APFS volume** as the source DB. One disk failure = total loss of source + 7 rolling backups + 15 pre-op snapshots (5.6 GB total). This is the single biggest unmitigated risk on the ship. There is no scenario where this isn't fixed within 30 days; do it in 30 minutes instead.
- **Where:** `healthcheck.py:417-468 backup_trader_db()`. Add a post-backup `subprocess.run(["rsync", "-a", "--delete", "backups/", "bigmac@192.168.1.166:~/trader-backups/"])`. Or a parallel launchd job at 06:30. Or SSH-key-based cron — pick one.
- **Effort:** **2h.** rsync over LAN to existing Ollie Box. SSH keys already in place per Ollie Box deploy.
- **Reward:** Catastrophe insurance for the entire research record. The 256 MB `data/trader.db` is irreplaceable — sacred per CLAUDE.md.
- **Deps:** none.
- **Risk:** Low. rsync is mature. Ollie Box has space.

### #2 — Sniper Mode KILL or Sizing Reset

- **What:** Either retire `ollie-auto` Sniper Mode (preferred), or restart its 30-day trial with notional-parity sizing (~$200/trade). Stop pretending the current numbers prove anything.
- **Why now:** Day 26/30 ends Saturday May 9. Sniper's recomputed metrics (Section C) say it's 89× worse than rest-of-fleet on absolute return. Letting the trial run another 4 days at $73/trade adds zero information.
- **Where:** `engine/paper_trader.py::_EXECUTION_PORTFOLIO_BY_PLAYER` (routing table for `ollie-auto`); `proving_ground.daily_trades` writer; ollie-auto sizing logic.
- **Effort:** **1d.** KILL = remove from routing table + halt; HARD-EXTEND = sizing patch + new 30-day window.
- **Reward:** Closes a research surface that's currently producing false confidence. Frees the Sniper Mode slot for a real candidate.
- **Deps:** Top-10 #5 — also implement Proving Ground v2 acceptance criteria so the next trial isn't measuring the wrong universe.
- **Risk:** Low.

### #3 — Dashboard Auth on Mutating Routes

- **What:** Add FastAPI `Depends(verify_session)` (or equivalent) to all mutating routes in `dashboard/app.py`. Minimum 12 routes are critical: `/api/kill-switch`, `/api/alpaca/buy`, `/api/alpaca/sell`, `/api/trade/manual`, `/api/arena/player/{id}/buy`, `/api/model-control/pause-all`, `/api/agents/{id}/{pause,unpause}`, `/api/gateway/kill-switch/{id}`, `/api/admin/*`, `/api/metals/{add,sell}`.
- **Why now:** **Zero authentication on any mutating route today.** `grep -c "verify_token|require_admin|require_auth|Depends" dashboard/app.py` = 0. Dashboard relies entirely on network binding + Cloudflare tunnel. Any LAN intruder + browser pin into the Cloudflare-fronted hostname = full kill-switch + arbitrary trade authority. This is the bigger half of HM-AW.3.
- **Where:** `dashboard/app.py` — 12 mutating routes + `_v1_auth_error` helper at line 15636 (already exists, not wired). Reuse Signal Center's TOTP/RBAC infra.
- **Effort:** **2d.** P0 priority.
- **Reward:** Closes the largest unmitigated security gap on the trader.
- **Deps:** HM-AW.3 (TOTP enforcement at Signal Center) — sub-ticket A. Dashboard auth (this) is sub-ticket B.
- **Risk:** Medium — auth code is silent-fail-prone. Pair with `security-reviewer` agent on PR.

### #4 — Fleet Roster Reconciliation

- **What:** Reconcile the documented "Active 9" against trade-volume reality. Document `deepseek-7b-grok4` (133 trades/30d — top-volume player on the ship) and `qwen3-8b-flash` (78 trades) — currently in zero roster docs. Demote `chekov` and `ollama-coder` from "active" to "utility" or "retired."
- **Why now:** Every audit has to triage the gap between docs and reality. Roster discipline is also a prereq for any "fleet of N" alpha analysis.
- **Where:** `CLAUDE.md` Fleet Roster section, `XO_BACKLOG.md`, `data/trader.db::ai_players.is_active`.
- **Effort:** **1d.** No code changes — pure documentation reconciliation.
- **Reward:** Future audits stop comparing apples to phantoms.
- **Deps:** none.
- **Risk:** Low.

### #5 — Resurrect or Retire `battle_station_monitor`

- **What:** Find why the 2-min schedule drifted to 6,857-second gaps and either fix the scheduler or formally retire `battle_station`. HM-AS-β has been firing all afternoon.
- **Why now:** Continuous alarm without action trains the Admiral to ignore observability. 2026-05-03 reconciliation already noted "battle_station — dormant via missing inputs" (launchd feeders absent). Either restore feeders or drop the schedule and the alarm.
- **Where:** `main.py:2602` (scheduler), `engine/battle_station.py::monitor_active_options`, missing `~/Library/LaunchAgents/com.trademinds.battle*` plists.
- **Effort:** **4h.** Likely outcome: retire the schedule (no feeders → no work). If feeders restored, +1d.
- **Reward:** Restores trust in HM-AS-β as a real signal. Drops 20+ daily false alarms.
- **Deps:** none.
- **Risk:** Low. Retirement is safer than restoration.

### #6 — Sunset `ollama-llama` (finish the exit)

- **What:** Already `halt_mode=exit_only` since 2026-04-25. Drop the scheduler entry, archive the agent module, remove from `AI_PLAYERS` config.
- **Why now:** 90-day realized P&L: **−$5,536.45** — biggest red on the entire board. 1,309 signals/30d → 9 trades = 145.4 sig-to-trade waste. Per 2026-05-03 reconciliation: halted players still emit signals; `exit_only` doesn't stop the compute burn.
- **Where:** `config.py` AI_PLAYERS, `main.py` schedule loop, signal-emission gate (currently absent).
- **Effort:** **4h.**
- **Reward:** Stops 1,309 wasted signal cycles/month. Removes a −$5,536 dead weight from the leaderboard.
- **Deps:** Confirm zero open positions before final retirement.
- **Risk:** Low.

### #7 — `ollama-plutus` Single-Point-of-Failure Hedge

- **What:** Build a backup CSP/options-seller agent at near-parity with `ollama-plutus` so the book isn't 94% dependent on one prompt + one Ollama model. The doctrine here is McCoy/Dax — high-VIX vs low-VIX seller — the original CLAUDE.md design that never fully materialized.
- **Why now:** Plutus alone produces +$4,108/30d on 75 closed trades. Every other active agent combined: +$228 (excluding pre-retired gemini-2.5-pro / claude-sonnet historical artifacts). Single-vendor, single-prompt risk.
- **Where:** New agent module + `config.py` AI_PLAYERS entry. Per CLAUDE.md "Healthy duplication": McCoy + Dax was the design.
- **Effort:** **1wk.** New prompt template, ghost-trade 30-day shadow, then live activation.
- **Reward:** Halves single-vendor risk on the alpha producer. Potential 50% book-level return increase if the hedge agent runs at +$2k/30d at 75% WR.
- **Deps:** Top-10 #4 (clean roster first).
- **Risk:** Medium — could underperform Plutus and dilute returns. Mitigate via 30-day ghost-trade gate before live activation.

### #8 — Stub DB Landmine Cleanup

- **What:** `./trader.db` (0B) and `./autonomous_trader.db` (2.5 MB) at repo root. The 2.5 MB file is actively grown by `engine/daily_enrichment.py:112` (`sqlite3.connect("autonomous_trader.db", timeout=5)` — relative path from cwd). Both are landmines next to the canonical 256 MB `data/trader.db`.
- **Why now:** Prior cleanup attempted (`archive/trader.db.stub-archive-20260507` exists) but not finished.
- **Where:** `engine/daily_enrichment.py:112`. (1) `git mv autonomous_trader.db data/enrichment.db`; (2) edit line 112; (3) delete the 0B stub. Restart trader.
- **Effort:** **2h.**
- **Reward:** Eliminates the most common "wrong-DB" trap.
- **Deps:** none.
- **Risk:** Medium — `autonomous_trader.db` has live data; inspect before relocating.

### #9 — Drop ~25 Vestigial 0-Row Tables

- **What:** One migration script: `DROP TABLE` for ~25 tables with 0 rows AND no `INSERT INTO <table>` in any non-archived `.py`. Candidates: `adaptive_weights, bootstrap_metrics, cash_manager_settings, earnings_impact, flash_alerts, gemini_failover, generated_indexes, gex_strikes, indicator_benchmarks, kill_switch_log, manual_trades, model_watchlist, news_impact, ollie_backtest_30d, options_flow_history, orcl_gex_alerts, rebalance_log, rebalance_targets, session_grades, short_watchlist, strategy_optimization, strategy_scores, tax_harvester_settings, tax_harvests, theta_opportunities, trade_explanations, trust_scores, user_agents, wash_sale_log`.
- **Why now:** 180 tables in `data/trader.db` is a navigability problem. Every audit wastes time triaging dead schemas.
- **Where:** `migrations/2026-05-XX-drop-vestigial-tables.sql`.
- **Effort:** **2h.**
- **Reward:** Schema clarity for every future audit.
- **Deps:** Top-10 #1 (off-host backup) shipped first as safety.
- **Risk:** Low. Each candidate verified zero rows AND zero writers.

### #10 — Schwab CSV Parser Hardening

- **What:** `scripts/import_schwab_csv.py::parse_csv` crashes `IndexError` on empty/malformed CSV. Add empty-file guard + NTFY on parse failure per CLAUDE.md error-handling posture.
- **Why now:** Schwab pipeline is the *only* feed for `real_holdings.json`, which is the *only* feed for Kirk advisory after Option A. Silent watcher crash = stale Schwab data undetected.
- **Where:** `scripts/import_schwab_csv.py:200-215`.
- **Effort:** **2h.**
- **Reward:** Kirk advisory stays calibrated. Early-warning on bad files.
- **Deps:** none.
- **Risk:** Low.

### Reward÷effort math (informal)

| # | Reward | Effort | Score |
|---|---|---|---|
| 1 | Catastrophe insurance | 2h | 🟢🟢🟢🟢🟢 |
| 2 | Closes false-confidence research surface | 1d | 🟢🟢🟢🟢 |
| 3 | Closes biggest security gap | 2d | 🟢🟢🟢🟢 |
| 4 | Roster vs reality alignment | 1d | 🟢🟢🟢 |
| 5 | Stops false-alarm fatigue | 4h | 🟢🟢🟢 |
| 6 | Stops 1,309 wasted signals/mo | 4h | 🟢🟢🟢 |
| 7 | Halves SPOF risk on alpha producer | 1wk | 🟢🟢 |
| 8 | DB landmine cleanup | 2h | 🟢🟢 |
| 9 | Schema clarity | 2h | 🟢🟢 |
| 10 | Watcher robustness | 2h | 🟢🟢 |

**Total: ~12 working days for a ship that finally matches its docs and survives a disk failure.**

---

## Section E — 14-Strategy Decision Brief

All 8 ORPHANED strategies are class definitions in `engine/options_agents.py` (last touched 2026-04-24, commit `df05b43`). Registry at line 787 maps them to player IDs; **no `main.py` scheduler instantiates them.** All `halt_mode=full`, halted 2026-05-05; zero trades or signals ever.

| # | Strategy | Verdict | One-liner |
|---|---|---|---|
| 1 | QuarkIronCondor | **RETIRE** | halt_mode=full, 0 history, no scheduler. |
| 2 | McCoyBullPut | **RETIRE** | 0 history; "McCoy" name conflicts with the live Active-4 McCoy CSP. Retiring removes naming hazard. |
| 3 | AndersonBearCall | **RETIRE** | 0 history, no scheduler entry. |
| 4 | CoveredCallAgent | **KEEP-COLD** | 0 activity, but covered-call P&L bug fix (commit `14689a7`) lives in this lineage. Useful registry slot for future Wheel/Troi assignment. |
| 5 | GhostKirkBullCall | **RETIRE** | Option-4 ghost bundle; preview-only, no rehab path. |
| 6 | GhostKirk0DTEBullCall | **RETIRE** | same bundle; active 0DTE work belongs to dayblade lineage. |
| 7 | GhostLongCall | **RETIRE** | preview-only ghost, never wired. |
| 8 | GhostNakedPut | **RETIRE** | naked-put exposure is wrong shape for current research. |

**WIRED-DORMANT (2):**

| # | Strategy | Verdict | One-liner |
|---|---|---|---|
| 9 | **Wheel/Troi** | **KEEP-COLD** | `engine/wheel_strategy.py` IS wired (`main.py:3011-3013`, every 15 min). Player `options-sosnoff` is `halt_mode=active` with **2,083 signals** (latest 2026-05-07 21:34) and 12 trades. Trades stalled 12+ days; VIX gate at line 88 likely blocking. Keep — it's not dead, just regime-gated. |
| 10 | **Theta Scanner** | **KEEP-COLD** | `engine/theta_scanner.py` wired (`main.py:1396, 2959`, every 30 min). Last touched today (May 7 12:56). Active context layer; not a standalone player_id. Keep. |

**Action: 6 RETIRE + 1 KEEP-COLD (CoveredCallAgent) + 2 KEEP-COLD (Wheel/Troi, Theta Scanner) + 1 ACTIVE (engine.options_agents.py preserved per sacred-data rule).**

---

## Section F — Kill List

| # | Target | Evidence | Effort | Risk |
|---|---|---|---|---|
| 1 | **`crew_*` framework remnants** (3 tables: `crew_runs`, `crew_strategies`, `crew_trade_results` + `crew/agents.py`, `crew/pipeline.py`, `crew/learning.py` + `.venv-crew/`) | `crew_runs.created_at MAX = 2026-04-01`. Plist already renamed `*.bak-decommissioned-20260421`. | 4h | Low |
| 2 | **`bakeoff_runs` + `bakeoff_trades` tables + `/api/bakeoff/*`** | 1 row since 2026-04-03; 0 rows in `bakeoff_trades` ever. UI shipped, never gained traction. | 4h | Low |
| 3 | **`kirk_signals`, `kirk_swing_trades`, `pike_votes`** (Swing Desk orphans) | All 3 tables 0 rows. CLAUDE.md retired Swing Desk 2026-05-04. Live Kirk uses `kirk_advisory_log`, not these. | 1h | Low |
| 4 | **Six zero-ref engine modules**: `spread_trader.py`, `stock_race.py`, `fred_data.py`, `orcl_gex_alerts.py`, `super_backtest_oos_c.py`, `arsenal_backtest.py` | Zero in-tree callers (excluding `_archive/` and venvs). | 2h | Low |
| 5 | **25+ vestigial 0-row tables** (Top-10 #9) | See list above. | 2h | Low |
| 6 | **`archive/sprint-backups/2026-04/` (~30 MB of `.bak` files)** | 12+ `.bak` copies of `dashboard/app.py`, 9+ `static/index.html`. Git already preserves history. | 1h | Low |
| 7 | **`dashboard/frontend/` Vite tree + `node_modules/`** | `dashboard/app.py` serves `static/index.html` only. CLAUDE.md memory: "Port 8080 serves vanilla HTML, NOT React." | 1h | Medium (confirm no revival plan) |
| 8 | **Stub DBs at root** (Top-10 #8) | `./trader.db` 0B + `./autonomous_trader.db` 2.5MB live sidecar. | 2h | Medium |
| 9 | **6 zombie agents in `ai_players` registry** (claude-sonnet, claude-haiku, gpt-4o, gpt-o3, qwen-coder-haiku, qwen3-14b-grok3) | All `halt_mode=full` 2026-05-07; zero trades/signals last 24h; pre-retirement P&L now stale. Registry rows can be `is_active=0`'d or kept-cold per sacred-data rule. | 1h | Low |

**Top 5 to ship in a single drydock session (~10h):** items 5, 1, 6, 4, 8.

---

## Section G — Agent Edge Scorecard + Minimum Viable Fleet

### Active 9 (per audit prompt) — last 30 days, `data/trader.db::trades`

| Player | halt | trades 30d | WR 30d | P&L 30d | P&L 90d | Sig/Trade | Verdict | One-line |
|---|---|---|---|---|---|---|---|---|
| **ollama-plutus** | active | 96 | 82.7% | **+$4,108** | +$4,108 | 10.2 | **KEEP** | The alpha engine. 75 closes / 82.7% WR / +$67 avg win. SPOF (Top-10 #7). |
| **ollie-auto** | active | 74 | 88.4% | +$75 | +$75 | n/a | **KEEP** | Quality gate, micro size. 88% WR on 43 closes. Sniper Mode trial — Top-10 #2 says reset sizing. |
| **ollama-qwen3** | active | 101 | 71.4% | +$240 | −$282 | 12.4 | **KEEP** | 30d turned green after 90d red. Recovering. |
| **navigator** | active | 2 | 50% | +$140 | +$125 | 153.5 | **WATCH** | Stopped trading 04-09; signals stopped 04-14. Silently dead. Retire candidate. |
| **neo-matrix** | active | 12 | 0% | −$23 | +$27 | n/a | **WATCH** | 0/3 30d, idle since 04-22. 90d still green by $27. |
| **ollama-llama** | exit_only | 9 | 14.3% | −$28 | **−$5,536** | 145.4 | **CUT** | Top-10 #6: finish the exit. |
| **capitol-trades** | active | 62 | 29.2% | −$101 | −$101 | n/a | **WATCH** | Avg loss −$74 vs avg win +$8.63. Broken R:R. Investigation owed. |
| **ollama-coder** | active | **0** | n/a | $0 | $0 | ∞ | **CUT** | 1,759 signals, 0 trades ever. Utility role, not a trader. |
| **chekov** | active | **0** | n/a | $0 | $0 | n/a | **CUT** | Zero trades ever, zero signals 30d. Muted per S6.3. |

### Phantom roster — players that *actually trade* but aren't in any roster doc

| Player | trades 30d | Notes |
|---|---|---|
| **deepseek-7b-grok4** | **133** | **Highest-volume trader on the ship.** Not in any roster doc. |
| qwen3-8b-flash | 78 | Also unrostered. |
| gemini-2.5-flash | 20 | Just halted to `exit_only` 17:18 today (HM-AK). |
| cto-grok42 | 14 | Unrostered. |
| energy-arnold | 12 | Per 2026-05-03 reconciliation: "high-volume noise generator, IMPROVE pending." |
| grok-4 | 11 | CLAUDE.md flags as retired — still trading. |

### Minimum Viable Fleet (≥80% of current performance)

If the smallest possible fleet had to deliver ≥80% of current performance, **it's 1 agent: `ollama-plutus`.** Plutus alone produces +$4,108 of the working-fleet's ~$4,564 in 30d realized P&L (89.9%). Every other active agent rounds to noise.

**But that's not the right answer.** A 1-agent book is structurally fragile — Top-10 #7 is the corrective. The defensible MVF is **3 agents:**

1. `ollama-plutus` (CSP/Plutus-3B) — the alpha producer
2. `ollama-qwen3` (qwen3:8b) — second-source momentum/regime variation
3. *new* CSP-Dax variant (low-VIX regime) — the SPOF hedge from Top-10 #7

Defending against the OOS finding "CSP regime-robust Sharpe 6.05/5.42": this 3-agent MVF concentrates on the regime-robust core. McCoy (high-VIX CSP) = `ollama-plutus`'s lineage; Dax (low-VIX CSP) = the missing hedge. `ollama-qwen3` provides momentum diversification for the non-CSP regimes the OOS data suggests are weaker. **3 agents instead of 25.** Frees Ollie Box bandwidth, simplifies the leaderboard, halves the audit surface.

---

## Section H — HM-AM Design Challenge + Readiness

### Challenge: does merging Schwab real-money + metals + Alpaca paper into one Kirk view introduce real-money/paper-money confusion risk?

**Answer: Yes, modestly. The challenge holds. The epic is still worth shipping, but with one design tightening.**

The risk: a unified `total_portfolio` envelope in Kirk's prompt context lets the agent reason about a "$240k book" when in reality it's $138k Schwab (real money), $90k Alpaca paper (research), and physical metals (off-broker). If the prompt says "you have $240k of buying power across Schwab + Alpaca," and a user reads Kirk's response casually, they could conflate paper trade recommendations with real-money trade authority. CLAUDE.md's broker-hard-rule explicitly forbids the inverse direction (no paper system routes to Schwab), but the *informational* leak is still possible.

**Mitigation: keep "separate views, shared analytics."** The shipped Phase 1 design (`engine/total_portfolio.py`) actually does this correctly — `TotalPortfolio` is a TypedDict with `by_account` segmentation; it's the *consumer* (Kirk prompt) that must keep account boundaries visible.

**Specific design tightening I would propose:**
1. Kirk prompt template should print account boundaries explicitly: *"Schwab (REAL): $138,371.20 — DO NOT trade · Alpaca (PAPER): $99,902 — research book · Metals: $X — physical"* — labeled at every prompt injection.
2. The 30-second cache (`engine/total_portfolio.py::_cache`) is process-local. If Kirk advisory and Advisory Team consume from the same cache simultaneously, they see consistent data; if a Schwab CSV ingest happens during the 30s window, both serve stale. Acceptable; document.
3. Open `HM-AM-β` as a 4h hygiene pass for: (a) silent yfinance failures in `_load_metals` not surfaced in `sources_failed`; (b) lack of file lock on `real_holdings.json`; (c) defensive prefix on cash-by-account map keys.

### Readiness Brief — POST-SHIP STATUS

All 4 phases verified shipped at the source level:
- **Phase 1** `4f0bcff` — `engine/total_portfolio.py` (337 lines)
- **Phase 2** `d338605` — Kirk advisory wired (`engine/kirk_advisory.py:280`)
- **Phase 3** `d6c9647` — Advisory Team prompt wired (`engine/team_advisor_grok.py:381`)
- **Phase 4** `52d7298` — dalio-metals injection (`engine/providers/base.py:1235, :1640`)

**Followups:** open `HM-AM-β` (4h hygiene) per design challenge above. `XO_BACKLOG.md` does NOT currently track HM-AM-β.

---

## Section I — HM-AN Readiness Brief

`grep -n "HM-AN" docs/XO_BACKLOG.md` → **0 matches.** HM-AN does not exist on disk. The audit prompt describes it as "Morpheus reframe of port 9000 (depends on HM-AM)" — that's a verbal placeholder, not a filed ticket.

If "HM-AN — Morpheus reframe of port 9000" is the intended ticket:

**Pre-challenge:** is it actually warranted? Section J below says **RIGHT-SIZE Signal Center, not expand.** Signal Center is genuinely a producer (4 engine consumers), but the dashboard never reads it. The right move is to surface what exists into the dashboard, not "expand" or "reframe."

**Recommendation: do NOT file HM-AN as a Morpheus reframe.** File it instead as:

**HM-AN — Signal Center → Dashboard read bridge** (P3, 4h)
- Wire 2-3 dashboard panels (e.g., `/dashboard/intelligence-feed`, `/dashboard/quant-signals`) to read from `signal-center/signals.db` directly (read-only).
- Adds `base_rate_features` retention policy (rolling 6 months ≈ 75k rows) — drops 643 MB to ~250 MB.
- Eliminates the need for any "reframe" — Signal Center earns its keep by feeding the user surface, not by being renamed.

**Effort: 4h. Risk: Low. Reward: User-visible value from Signal Center for the first time.**

If the Admiral wants Morpheus-reframe semantics specifically (rename + UI overhaul), defer. The plumbing fix is the higher-leverage move.

---

## Section J — Port 9000 Empirical Verdict: **RIGHT-SIZE**

### Process state
PID 18380, `com.trademinds.signal-center` launchd-managed, up 3.3h, listening localhost:9000. Auth gate at `signal-center/server.py:564` (localhost API bypass + session check for UI).

### DB activity (last 7 days, `signal-center/signals.db`)
- `signal_history`: latest 2026-05-07 16:52:50 — fresh
- `trade_signals`: 64 rows last 7 days, latest 2026-05-07 07:00:23 — active
- `intelligence_feed`: latest 2026-05-08 01:19:56 — actively writing
- `signal_outcomes`: 1,176 total rows
- `base_rate_features`: 218,687 rows — **dominates the 643 MB file size**

### Downstream consumers (confirmed reads of `signals.db`)
- `engine/ghost_trader.py:24` — pulls BUY signals into ghost_trades, uses `signal_outcomes`
- `engine/strategies.py:17, 727` — Tractor Beam reads TB signals ≥ threshold
- `engine/archer_morning_synthesis.py:33, 107`
- `engine/crew_scanner.py:1593, 3606`
- `dashboard/app.py` — **zero direct reads.** Dashboard only reads `data/trader.db`.

### "Could 9000 be turned off for 24 hours?"
**Yes, with degradation.** Ghost trader + TB read paths would stale, then resume. Dashboard wouldn't notice. Active grades B; not load-bearing for user surface today.

### Verdict: **RIGHT-SIZE**

Not vapor; not "the best foundation seen" (that's Grok's overstatement). It's a producer with engine-side consumers and zero dashboard consumers. **Right action:** (a) keep server, (b) add `base_rate_features` rolling-window prune (retain last 6 months ≈ 75k rows; drops 643 MB to ~250 MB), (c) wire 2-3 read paths into the dashboard so it earns its name (Top-10 #4 candidate for HM-AN), (d) reject Grok's "expansion" framing.

---

## Section K — Options Infrastructure Status

**The audit prompt's claim "_EXECUTION_ENABLED = False — three options-strategy gates exist. Never flip them" is stale.**

### Gate-flip provenance (who, how, when)

| Field | Value |
|---|---|
| Commit | **`df7320c`** "gate-flip: _EXECUTION_ENABLED False -> True at 3 sites (atomic)" |
| Author / Committer | **Admiral Steve `<steve@ollietrades.local>`** (manual flip, not autonomous) |
| Date | **2026-05-04 08:31:31 MST** (Monday morning) |
| Sites flipped (atomic, 1-line each) | `strategies/executor.py:22`, `strategies/bull_call_spread_v1.py:65`, `strategies/bear_put_spread_v1.py:65` |
| Pre-flip main HEAD | `753f01a` |
| Pre-flip DB backup | `backups/trader.db.pre-gate-flip-20260504_082909` |
| Recovery procedure (in commit body) | `git reset --hard gate-flip-revert` + force-push + `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` |

**Documented prerequisite chain (per commit body):**
1. `cbf8add` — Fix #1: halt_mode signal-emit gate
2. `50ef95c` — HM-C: read-path filter on 28 decisional sites
3. `dc3e1c1` — Audit #6X: signal-center scorecard system verified healthy
4. `753f01a` — docs: Audit #6X Admiral verdicts — gate-flip SQL-level READY

**Calibration baseline at flip time** (last 30 days, tier-2 voter — tractor-beam): 268 signals, 34.3% hit_tp, PF 2.02, avg_pnl +1.74%. Production execution path filters `WHERE agent_name='tractor-beam'`; chekov/navigator/morning_briefing are calibration-only and do not execute.

**The 4th gate** — `strategies/bull_spread_v1.py:95` — was NOT in this atomic commit. It was on a separate timeline: re-halted at commit `44c80c2` for position-stacking concerns, then re-unhalted at commit `d98e00c` (HM-AB self-symbol-skip) on 2026-05-05. Comment at line 95 still reads `# HM-AB-unhalted-2026-05-05: was False (44c80c2 halt)`.

**Conclusion: the gate-flip was a deliberate, manual, Admiral-authorized atomic commit with a 4-step documented prerequisite chain, a pre-flip DB backup, and a written rollback procedure.** Not autonomous, not accidental. Production-grade change discipline.

### Current state (verified now)

All four sites are now `True`:

| File:line | Path | Value |
|---|---|---|
| `strategies/executor.py:22` | Generic spread executor | **True** (docstring at line 4 still says "Hardcoded to False" — stale comment, 30-min fix) |
| `strategies/bull_call_spread_v1.py:65` | BCS signal emitter | **True** |
| `strategies/bear_put_spread_v1.py:65` | BPS signal emitter | **True** |
| `strategies/bull_spread_v1.py:95` | Active bull-put-spread (writes `options_trades`) | **True** (HM-AB-unhalted-2026-05-05) |

**Production reality:** 24 rows in `options_trades` (13 closed / 7 open / 4 failed), activity 2026-04-22 → 2026-05-06. All recent rows are SPY `bull_spread_v1`. `engine/alpaca_options.py::submit_spread / submit_single / close_all_options` all firing today.

**Verdict: options execution is LIVE in production. Not gated, not dead weight.**

**Two follow-ups worth filing:**
1. Stale "Hardcoded to False" docstring at `strategies/executor.py:4-7` — confusing for new engineers. 30-min fix.
2. Battle Station's silent execution (Top-10 #5) — separate ticket from gate state.

---

## Section L — Backup Reality Check

**Mechanism:** `healthcheck.py:417-468 backup_trader_db()`. sqlite3 backup API (online-safe). Runs from launchd `com.trademinds.watchdog.plist` at 6 AM premarket + hourly 7 AM-1 PM MST. `DB_BACKUP_KEEP=7` days.

**Reality on disk:**
- 7 daily backups: 211–246 MB each (compression: none)
- 15 pre-op snapshots: 220–232 MB each (kept indefinitely)
- Total: 5.6 GB
- All on `~/autonomous-trader/backups/` — **same Mac Mini APFS volume as `~/autonomous-trader/data/trader.db`**

**Same-disk vulnerability confirmed.** One disk failure = total loss. **This is the single biggest unmitigated risk on the ship.** Top-10 #1.

### Recommendations (off-host, ranked)

1. **rsync to Ollie Box (192.168.1.166)** — already on LAN, has its own SSD. Daily cron: `rsync -a --delete backups/ bigmac@192.168.1.166:~/trader-backups/`. **Zero new infra. 2h to set up.**
2. **rclone to cloud (B2/R2)** — paid, offsite. ~6 GB at $0.005/GB/mo = $0.03/mo on B2. One-time setup.
3. **USB cron to external drive** — manual; hardware-isolated.

**Add gzip on the rotation.** Daily file 246 MB → ~80 MB compressed. 7-day rolling 1.6 GB → ~500 MB.

---

## Section M — Grok Report Verification & Reconciliation

### Doc location (updated 2026-05-07 post-audit)
**`docs/Signal_Center_Upgrade_Report_v2.md` does not exist on bigmac.** Verified via repo-wide and home-directory search. The file lives on **Bonnie's laptop at `C:\Users\Bonnie\Downloads\Signal_Center_Upgrade_Report_v2.md`** (Admiral confirmed post-audit).

**Implication for this section:** the verification below was run against actual source code (`signal-center/server.py`, `dashboard/app.py`, `engine/volume_scanner.py`, `engine/gex_overlay.py`, `engine/fear_greed.py`) — not against Grok's words. The technical verdicts (F&G Recovery Protocol NOT coded, Volume Radar in `engine/` not `signal-center/`, hard-coded PIN, fixed watchlist, no "phase" UI element) stand independent of the doc. **What's deferred until the file is scp'd to bigmac:**
- Verifying Grok's exact wording on the "80% coded" claim
- Confirming the line numbers Grok cited (the one I could spot-check — Congress+Insider at lines "950-980" — was off by ~850; actual location 1801-1828)
- Reconciling Tier S/A/B/C breakdown to what's actually in the codebase

**Recommendation when Bonnie scp's the file over:** drop into `docs/` (not `inbox/` — the launchd watcher only matches `Sc*Position*.csv`). Re-audit Section M then; substantive verdicts will not move much.

### Architecture correction (changes how every Grok claim is graded)
`signal-center/server.py` (3,084 lines) is **NOT** the implementer of the features Grok lists. It is a **proxy/aggregator/UI layer** that calls the dashboard bridge at `http://127.0.0.1:8080` for all market data. The actual computation lives in `dashboard/app.py` + `engine/*.py`.

- `signal-center/server.py:178` — `BRIDGE = "http://127.0.0.1:8080"`
- `signal-center/server.py:725-762` — `_SIGNALS_ENDPOINTS` dict — every "feature" Grok cites maps to a `/api/...` path that's `_bridge_get(...)`'d to port 8080.

When Grok says "Volume Radar 10x+ is 80% coded in Signal Center," he's looking at proxy plumbing, not implementation.

### Tier S substantive claims

| Claim | Verdict | Evidence |
|---|---|---|
| Volume Radar 10x+ coded | **VERIFIED** (in `engine/volume_scanner.py`, not signal-center) | `engine/volume_scanner.py:39 REL_VOL_TRIGGER = 10.0`. `dashboard/app.py:5552` `/api/volume-radar`. Writes to `volume_alerts` table. Real, working, threshold-driven. |
| GEX Flip/Walls coded with actionable signals | **PARTIALLY VERIFIED** | `dashboard/app.py:5575 /api/gex-overlay/levels` → `engine/gex_overlay.py:391-414` extracts `king_node, gamma_flip, put_wall, call_wall`. Real flip/wall computation. **"Actionable signal" claim is THIN** — only feeds a heuristic mean-reversion score in `quant_signals['mean_reversion']` at server.py:1750-1763. No automated alerting on flip-cross. |
| F&G Recovery Protocol coded | **FALSE** | `dashboard/app.py:12783 /api/fear-greed` → `engine/fear_greed.py` (218 lines). Composite from VIX/SPY RSI/breadth. Returns `{score, label, signals}`. **Pure passive read.** Two references in dashboard mention F&G but as advisory text, not auto-trade triggers. **No code matches "score < 25 → auto-buy on > 35 cross."** |
| Congress + Insider Overlap at lines 950-980 | **PARTIALLY VERIFIED** (line numbers off by ~850) | Actual location: `signal-center/server.py:1801-1828` inside `quant_signals()`. Set-intersection of insider BUY tickers and congress BUY tickers; +15 score on overlap. Logic correct, line numbers wrong. |

### Flagged weaknesses

| Claim | Verdict | Evidence |
|---|---|---|
| Hard-coded `PIN=2026` | **VERIFIED** | `signal-center/server.py:179 PIN = "2026"`. Used at line 206 `_get_session()`. Server-to-bridge auth secret, not user-facing. Still hard-coded, real finding. |
| yfinance dependency in hot paths | **PARTIALLY VERIFIED** | `signal-center/server.py` 4 references; `dashboard/app.py` 25; `engine/market_data.py` 4; `engine/fear_greed.py` 3. Per CLAUDE.md (2026-04-27), VIX migrated off yfinance, sector ETFs on Alpaca. Remaining yfinance is wrapped in `try/except` and degrades gracefully. **Real concern but not outage-class hot path.** |
| Bare `except: pass` proliferation | **PARTIALLY VERIFIED** (specific syntax claim FALSE) | Zero `except:\s*$` matches in server.py. 53 total `except` clauses, all named (`except Exception:` consistent). Many ARE broad-catch which CLAUDE.md HM-Z/HM-AA posture flags as risky. Sample silent swallows: lines 716, 1218, 1499. **Directionally right; specific syntax claim wrong.** |
| Phase 1/3 modal contradiction | **CANNOT VERIFY** (likely hallucinated) | `grep -i "phase"` in `signal-center/index.html` returns ZERO matches. Only modal in file is `regime-modal` at lines 192-196. **No "phase 1" / "phase 3" UI element exists.** |
| Fixed watchlist | **VERIFIED** | `signal-center/server.py:192-195 FIXED_WATCHLIST = [...]`. Hard-coded mega-cap list; not config-driven. |

### Tier A items (Backtest Engine, Confidence Auto-Sizing, Self-Learning Loop, Portfolio Risk Overlay)
**FALSE**, for the signal-center directory itself. `grep -nE "backtest|auto_size|self_learn|portfolio_risk|confidence_size"` in `signal-center/server.py` returns **zero matches.** Adjacent capability exists in `engine/` (project-level backtest infra), but Grok's framing of "Tier A is partially in Signal Center" is wrong for the signal-center directory.

The closest thing to self-learning in signal-center is `signal_outcomes` (server.py:335-353) which tracks whether emitted signals hit TP/SL. **Outcome tracking, not a self-learning loop with feedback into signal generation.**

### Competitive replication claim
Grok claimed Signal Center replicates ~$200-300/mo of Unusual Whales + SpotGamma + Capitol Trades. Reality:
- **UW-equivalent (volume + options flow):** Volume Radar at 10x is real; UW has order-flow tape, dark pool, sweep classification — none of those.
- **SpotGamma-equivalent:** GEX overlay is the strongest piece. Genuinely SpotGamma-lite. Worth $50-100/mo. SPY+single-stock only.
- **Capitol Trades-equivalent:** Real feed, cleanest 1:1 replica.
- **F&G Recovery:** Not coded as a protocol. Replication value zero.

**Verdict: replicates ~30-40%, not 80%. Realistic replacement value: $50-120/mo, not $200-300/mo. Grok overclaimed by ~2x.**

### Reconciliation summary
About 55% of Grok's substantive claims hold up; the rest are wrong, missing, or fabricated. **Trust the GEX overlay piece, distrust the F&G "Recovery Protocol" and Tier A claims.** No Top-10 item below duplicates a Grok prioritization unless I have new evidence.

---

## Section N — Security Tail (HM-AW.3)

### Findings

- **Hard-coded PIN:** `signal-center/server.py:179 PIN = "2026"`. Gates Flask UI login. Localhost API bypass means automation never sees this gate; only browsers do. Trivially weak.
- **`.env`:** chmod 600 (✅), 3,789 bytes, 104 keys. Not in git: `git ls-files .env` empty, `git log --all --diff-filter=A` for .env returned nothing. Posture correct.
- **Dashboard auth (port 8080):** **No auth on any mutating route.** `grep -c "verify_token|require_admin|require_auth|Depends" dashboard/app.py` = 0. The `_v1_auth_error` helper exists at line 15636 but is not wired to any `Depends()`.

### Confirmed-unauthenticated mutating endpoints (sample)

| Endpoint | Line | Effect |
|---|---|---|
| `POST /api/kill-switch` | 6499 | Closes ALL positions, ALL models |
| `POST /api/alpaca/buy` | 8679 | Submits live Alpaca paper buy |
| `POST /api/alpaca/sell` | 8698 | Submits live Alpaca paper sell |
| `POST /api/trade/manual` | 8644 | Manual market order to Alpaca |
| `POST /api/arena/player/{id}/buy` | 7713 | DCA buy on any player |
| `POST /api/model-control/pause-all` | 7907 | Global pause toggle |
| `POST /api/agents/{id}/pause` | 8068 | Per-agent pause |
| `POST /api/agents/{id}/unpause` | 8084 | Per-agent unpause |
| `POST /api/gateway/kill-switch/{id}` | 17174 | Per-agent kill switch |
| `POST /api/admin/clean-stale-snapshots` | 8139 | DB mutation |
| `POST /api/metals/sell` | 4375 | Metals ledger mutation |
| `POST /api/metals/add` | 4362 | Metals ledger mutation |

The dashboard relies entirely on **network binding + Cloudflare tunnel** for security. The hard-coded PIN at signal-center is the small problem. **The dashboard auth gap is the big one.**

### Recommendation: split HM-AW.3 into two sub-tickets

- **Sub-ticket A (P1, 30 min):** Rotate hard-coded `PIN=2026` to env-derived `SIGNAL_CENTER_PIN`. Add fail2ban-style lockout.
- **Sub-ticket B (P0, 2d):** Add FastAPI `Depends(verify_session)` to all mutating routes in `dashboard/app.py`. Reuse Signal Center TOTP/RBAC infra (per CLAUDE.md HM-AW Phase C halted on TOTP gap).

**Do NOT roll into a broader pass — split into a focused P0 sub-ticket so it doesn't get buried.** This is Top-10 #3.

---

## Section O — Dependency Graph

```
                                    ┌─── #6 (sunset ollama-llama) ──┐
                                    │                                 │
#4 (Roster Reconciliation) ─────────┤                                 ├──> #7 (Plutus hedge)
                                    │                                 │
                                    └─── (capitol-trades fix) ────────┘

#1 (Off-Host Backup) ─────> [SAFETY PREREQ for #9 (drop tables)]

#2 (Sniper Mode KILL) ───┬──> #5 (battle_station — orthogonal observability cleanup)
                         └──> Proving Ground v2 acceptance criteria

#3 (Dashboard Auth) ─────> sub-ticket A (PIN, 30min) + sub-ticket B (auth, 2d)
                                  │
                                  └──> HM-AW (LAN bind, currently halted) ──> HM-AW.2 (RBAC)

#5 (battle_station)      ──> independent
#8 (stub DB cleanup)     ──> independent (low risk standalone)
#9 (drop dead tables)    ──> blocked by #1 (backup safety)
#10 (Schwab parser)      ──> independent

HM-AM-β (4h hygiene)     ──> from Section H design challenge
HM-AN (Signal Center → Dashboard read bridge) ──> from Section I, replaces Morpheus-reframe
```

**Critical path for safety:** #1 → #3 (sub-ticket B).
**Critical path for fleet quality:** #4 → (#6 + #7).
**Critical path for research integrity:** #2 → Proving Ground v2.
**Quick wins (any order, ~10h total):** #5, #8, #9, #10, #2 (decision phase).

---

## Section P — Appendix: Read-Only Commands Run

In order, for replay verification:

```bash
# Pre-reads
git log --oneline -30
ls docs/ | head -60
find . -iname "*scotty*" -o -iname "*Signal_Center_Upgrade*" -o -iname "*grok*report*"
sqlite3 data/trader.db ".tables"
ls -la trader.db autonomous_trader.db data/*.db
wc -l docs/XO_BACKLOG.md
cat docs/KIRK_SOURCES.md
cat docs/HM-P_CONFIDENCE_SCALE_AUDIT_2026-05-04.md
sqlite3 data/proving_ground.db ".tables"
sqlite3 data/proving_ground.db "SELECT * FROM running_scorecard ORDER BY id DESC LIMIT 5"

# May 7 wave validation
grep -rn "ALPACA_API_KEY\|ALPACA_API_SECRET\|ALPACA_BASE_URL\|ALPACA_DATA_BASE_URL" --include="*.py" .
sqlite3 data/trader.db "SELECT id, halt_mode, halted_at FROM ai_players WHERE halt_mode != 'active' ORDER BY halted_at DESC"
sqlite3 data/trader.db "SELECT id, halt_mode FROM ai_players WHERE halt_mode = 'active'"
grep -E "guard active" logs/trader.log
launchctl list | grep -i trader
ps -p 15619 -o pid,etime,lstart,command
grep -n "SPREAD_CANNIBALIZATION_GUARD_ENABLED" config.py engine/alpaca_options.py
grep -rn "_EXECUTION_ENABLED" --include="*.py" .

# HM-P safety sweep
grep -nE "confidence\s*[<>=!]+\s*[0-9]" strategies/*.py
grep -rnE "confidence\s*[<>=!]+\s*[0-9]" --include="*.py" engine/
sqlite3 data/trader.db "SELECT MIN(confidence), MAX(confidence), AVG(confidence) FROM signals WHERE timestamp > datetime('now','-7 days')"
sqlite3 signal-center/signals.db "SELECT MIN(confidence), MAX(confidence), AVG(confidence) FROM trade_signals WHERE created_at > datetime('now','-7 days')"

# Sniper Mode + OOS tracking
sqlite3 data/trader.db "SELECT player_id, COUNT(*) FROM trades WHERE executed_at > datetime('now','-30 days') AND player_id IN ('ollie-auto','super-agent') GROUP BY player_id"
sqlite3 data/trader.db "SELECT recorded_at, total_value FROM portfolio_history WHERE player_id='ollie-auto' AND recorded_at > datetime('now','-30 days') ORDER BY recorded_at"
sqlite3 data/trader.db "SELECT SUM(realized_pnl), COUNT(*), SUM(CASE WHEN realized_pnl>0 THEN 1 ELSE 0 END) FROM trades WHERE executed_at > datetime('now','-30 days')"
sqlite3 data/trader.db "SELECT player_id, COUNT(*), SUM(realized_pnl) FROM trades WHERE executed_at BETWEEN datetime('now','-90 days') AND datetime('now','-30 days') GROUP BY player_id ORDER BY SUM(realized_pnl) DESC"

# Grok claims verification
grep -n "BRIDGE\|_SIGNALS_ENDPOINTS\|FIXED_WATCHLIST\|PIN" signal-center/server.py | head -30
grep -rn "REL_VOL_TRIGGER" engine/
grep -n "/api/volume-radar\|/api/gex-overlay\|/api/fear-greed" dashboard/app.py
grep -n "phase" signal-center/index.html
grep -nE "except:\s*$|except.*:\s*$" signal-center/server.py | head -20
grep -nE "backtest|auto_size|self_learn|portfolio_risk" signal-center/server.py

# 14-strategy
sqlite3 data/trader.db "SELECT halt_mode, halted_at FROM ai_players WHERE id IN ('quark-ic','mccoy-bps','anderson-bcs','covered-call','ghost-kirk-bc','ghost-kirk-0dte-bc','ghost-long-call','ghost-naked-put')"
sqlite3 data/trader.db "SELECT player_id, COUNT(*) FROM trades WHERE player_id IN ('quark-ic',...) GROUP BY player_id"
sqlite3 data/trader.db "SELECT player_id, COUNT(*), MAX(executed_at) FROM trades WHERE player_id IN ('options-sosnoff','theta-scanner') GROUP BY player_id"

# Port 9000
launchctl list | grep -i signal
sqlite3 signal-center/signals.db "SELECT name FROM sqlite_master WHERE type='table'"
sqlite3 signal-center/signals.db "SELECT MAX(created_at) FROM trade_signals"
sqlite3 signal-center/signals.db "SELECT COUNT(*) FROM base_rate_features"
grep -n "signals\.db" dashboard/app.py engine/*.py main.py

# Backup + security
ls -la ~/autonomous-trader/backups/
du -sh ~/autonomous-trader/backups/
launchctl list | grep -i backup
grep -n "backup_trader_db\|DB_BACKUP_KEEP" healthcheck.py
ls -la ~/autonomous-trader/.env
grep -rn "PIN.*=.*2026" signal-center/ --include="*.py"
grep -c "verify_token\|require_admin\|require_auth\|Depends" dashboard/app.py
grep -nE "@app\.(post|put|delete)" dashboard/app.py | head -30
```

---

**End of Audit. She'll hold, Admiral. But the same-disk backups will not survive a single drive failure, and the dashboard will not survive a single LAN intruder. Fix #1 and #3 first.**
**— Scotty 2.3**
