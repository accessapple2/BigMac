# OllieTrades / bigmac — project memory
(Last updated 2026-07-04. Paste or attach this at the start of new chats.)

## Where the code lives — stop chasing folders
- Bot runs on **bigmac** (Mac, 192.168.1.248). SSH alias configured: `ssh bigmac` (user `bigmac`, via `C:\Users\Bonnie\.ssh\config`).
- Main bot: `~/autonomous-trader` on bigmac (engine/, lib/, scripts/, strategies/, dashboard/, signal-center/, swingdesk/). ~21 GB total; code itself is small.
- Also on bigmac: `~/ollietrades` (4.1 MB, the web UI) and `~/paper-trader` (157 MB).
- Other host: **g1** = 192.168.1.166, user `ollie`.

## Local copy on this PC (Bonnie's Windows machine)
- `C:\Users\Bonnie\Documents\bigmac-bot\` — extracted from `bot-code.tar.gz` pulled 2026-07-03.
- Patched files live in `C:\Users\Bonnie\Documents\bigmac-bot\patched\`; dashboard files in `...\bigmac-bot\dashboard\`.
- **To work on the code in Cowork: connect the folder `C:\Users\Bonnie\Documents\bigmac-bot`.**

## Workflow rules
- Claude cannot SSH to bigmac directly. User runs copy-paste PowerShell commands (`ssh` / `scp`).
- PowerShell mangles nested quotes — never give inline `python3 -c "..."` one-liners; write a runner script, scp it over, execute it.
- Deploy pattern: scp to `<file>.new` on bigmac → `python3 -m py_compile` → backup `.bak_<reason>_<date>` → `mv` into place.
- Restart: `ssh bigmac "cd autonomous-trader && ./restart.sh"` — dashboard runs inside main.py (port 8080, .venv runtime, logs/trader.log + trader_error.log). Health check in restart.sh polls too early; API responding = healthy.
- Gotcha (bit us twice): re-running the PULL scp (bigmac→local) overwrites local patches — always grep for the HM- marker in the local file before deploying, and state scp direction explicitly.
- Test envs on bigmac: `venv/` = full bot deps (use for backtests); `.venv-deps/` = pre-commit hook env (dotenv, scipy, pyotp added 2026-07-03; test_auth still needs fastapi). `.venv/` = live runtime.

## Recent state (July 2026)
- 2026-07-03 commits on exec-pipeline: 95d0055 (backtest realism + events_bus swing 30s→3600s), 7ca21c9 (scanner ghost-fix), 69d04c9 (raw-mode counters), 303b9d9 (Scotty's race.py/market_data.py holiday/bounds hardening), 7c82202 (db-lock retry in breadth_scanner + sector_heatmap — busy_timeout alone lost rows to a >30s midnight writer, errors clustered 00:04-00:18; root-cause writer NOT yet identified). First four pushed; verify 7c82202 pushed.
- FLEET SWEEP running since 20:25 (nohup on bigmac, survives everything): fleet_realism_sweep.py, 22 agents, honest guarded+raw, checkpoints to reports/fleet_realism_sweep_20260703_202558.json after each agent. Ranked table at end of logs/fleet_sweep.log.
- SWEEP COMPLETE 2026-07-03 21:19 (22 agents, reports/fleet_realism_sweep_20260703_202558.json). Top honest-guarded: gemini-2.5-pro +29.74% (VETO: 5 tr, 62% spam, paid seat, one March week) · mlx-qwen3 +25.15% (14 tr, 85.7% WR, 8.7% spam) · energy-arnold +21.39% · ollama-qwen3 +20.06% (1.6% spam, cleanest fleet discipline) · ollama-llama +18.18% · McCoy +13.74% (22 tr, biggest sample) · qwen3-8b-flash +13.73% (8/8 wins, 0.0% spam). Bottom: capitol-trades -3.92% LAST while ACTIVE; Spock/sulu/dalio ~0. dayblade-0dte +17.76% is luck on 28.6% WR + 86.4% spam — stays halted.
- REOPENING PLAN: Batch 1 = ollama-qwen3 (exit_only→active flip), mlx-qwen3 (verify model on olliemax first — orphan-seat suspect, silent since 05-07), energy-arnold (advisory→scan roster). Batch 2 queue: qwen3-8b-flash, options-sosnoff, ollama-llama, ollama-coder. Cut criteria for the 4-week window: ≥20 trades, guarded honest >0%, spam <30%. PREREQ: fix the 21 rule-surface conflicts first. Admiral decision pending: capitol-trades demotion.
- Bootstrap fix (Scotty, 49902bc): nightly bootstrap_intelligence crash was the midnight lock-storm root cause AND meant bootstrap_metrics was empty forever; fix deployed GATED OFF (BOOTSTRAP_METRICS_LIVE_ENABLED=False) — flipping it populates the adaptive engine's table for the first time ever = deliberate decision, not tonight. Tickets: guardian_sweep 125-145s chronic writes; holly_nightly cron doc-vs-reality gap.
- AGENT-RULES-REVIEW.md in bigmac-bot folder: 21 inconsistencies (options ban vs iron-condor mandates, 3-way position-cap conflict, Sulu dual identity, stale 8% stop table, no stop coverage for exit_only). Fix rule surfaces BEFORE reopening; reopen in batches of 3 per its Section 8 checklist.
- Operating rules (Admiral, 2026-07-03): KEEP ALL DATA #1 — archive never delete; LET LONG RUNS COMPLETE (days/weeks — that's where compounding happens); an agent training run is in progress; review all agent rules before reopening anyone.
- Fleet signal reality: only ollama-plutus emits through today; Spock is halt=full (admin halt, not breakage); Troi/Trip/Geordi/Uhura/Worf benched to ADVISORY_CREW 2026-05-29 by design.
- Two agents work this repo: this Cowork session (Windows/SSH relay) + "Scotty" (Claude Code natively on bigmac). Coordinate via this file; check git log before assuming file state.
- Counter fix VERIFIED 2026-07-03 rerun: reentry_blocked=117 = exact trade delta 1603→1486; raw friction $18,804 (raw sim churns ~$6M notional on $7k account to net +$498 — huge churn, hairline edge); guarded friction $100. Headline returns unchanged vs pre-fix run (regression-clean).
- Realism A/B (ollama-plutus): 90d raw +35.08%→+7.12% honest, guarded +10.09%→+8.66% (21 trades); 180d raw +33.48%→+5.48%, guarded +15.31%→+13.74% (22 trades). Old headline was ~28pts inflated; guarded honest is the number to trust. 180d added only 67 signals vs 90d — signal history recent-heavy, sample thin (~21 trades → ±21pt CI on win rate).
- Pre-existing test failures (NOT ours): war_room instrumentation/budget errors, quality_gate_hold, universe_filter, ollama_cancel, conviction/trail flag tests, bbkc dedupe. Only `test_crew_init_guard.py` imports backtester/events_bus — passes.
- SCANNER GHOST-FIX deployed + VERIFIED 2026-07-03 (`bak_pre_ghostfix_20260703`): /api/scanner/convergence in dashboard/app.py — mover_watchlist join mirrors HM-MOVERS-STALE-FIX guards (refreshed_at<24h, |pct_change|<=50; BNY +1261% was a documented pre-split ghost row from 2026-05-21), datetime() normalization on strategy_signals/volume_alerts (T-format same-day leak), per-row stale_plan flag (entry >3% off price), meta.market_open from engine/market_calendar. UI static/index.html: STALE PLAN badge + CLOSED·STALE panel badge. Post-restart check: market_open:false, ghost tiers empty on closed day. Monday open = real-world test. Dashboard commit pending (note: app.py had earlier uncommitted edits from prior sessions).

## HM-AGENT-RULES-CONSOLIDATION — 2026-07-04 (Scotty, exec-pipeline, all pushed)
Admiral decisions from AGENT-RULES-REVIEW-2026-07-03.md Section 7/8, implemented in 6 small
commits (acd62d1, 2787efa, 9b3767f, f9e3a4c, a384667, 9d3e097) + 1 doc commit (00c4978) + 1
backtester commit (c1dd786) — each backed up `.bak_pre_*` (gitignored, local only), each
py_compile'd, each through the pre-commit hook (19 tests) before committing.
- **Canonical numbers baked in everywhere:** max positions 8→5 normal / 8→3 bear
  (trading_rules.txt, config.MAX_POSITIONS, risk_manager NORMAL/BEAR_MAX_POSITIONS +
  MAX_POSITIONS_PER_MODEL, backtester already matched); cash floor 15%→20% normal / →35% bear
  (config.MIN_CASH_RESERVE_PCT; risk_manager UNIVERSAL/BEAR_MIN_CASH_PCT + backtester already
  matched); position cap 40%→30% (base.py prompt Rule 2 + CONVICTION SCORING, config.MAX_POSITION_PCT
  already 0.30 = the reference); options cap 20%→10% (base.py Rule 3 + Troi persona, config
  already 0.10); stops = engine/stops.py 12/15/18% tiers as the one true source (trading_rules.txt
  + base.py Rule 9 rewritten to describe it); deepseek-7b-grok4 MAX_TRADES_PER_DAY 8→3 (matches its
  own MODEL_GUARDRAILS + persona, was internally contradicting itself); RiskManager.__init__
  take_profit_tiers now reads config.TAKE_PROFIT_TIERS directly instead of a hardcoded dup that
  started at +10% instead of +5%.
- **Live bug found and fixed, not just a doc conflict:** risk_manager.py had its OWN
  `get_stop_loss_pct` staticmethod (0.08 tight stop below 0.70 conviction — the exact regression
  the canonical stops.py 12% floor was built to kill, documented: hurt kimi PF 9.89→4.02) sitting
  alongside the module-level canonical import of the same name. paper_trader.py:1111 called
  `_rm.get_stop_loss_pct(confidence)` on an instance — Python resolved that to the STALE class
  method regardless of what risk_manager.py imports at module scope. Removed the stale method;
  paper_trader.py now imports engine.stops.get_stop_loss_pct directly.
- **Sulu retired to Iron Condor King, DayBlade persona archived in-place** (base.py, commented
  block above the live entry, not deleted — CLAUDE.md Archive Convention has the record). Also
  fixed the shared CREW_ROSTER line every agent's prompt sees. NOT done: ~15 other files still
  say "dayblade-sulu"/DayBlade-era assumptions (main.py EOD sweep, paper_trader sizing/circuit-
  breaker exemptions, crew_scanner, super_backtest_v4, weekend_backtest) — ticketed in
  XO_BACKLOG, not touched (Sulu is exit_only, nothing live-executing on it right now).
- **exit_only stop coverage generalized** (guardian_sweep.py): was hardcoded to guardian-of-forever
  only; audit found 5 exit_only seats holding 23 open positions total, 15 of them (ollama-qwen3 x1,
  navigator x5 options, ollie-auto x8, gemini-2.5-flash x1 short) uncovered. Now queries every
  exit_only agent with a position fresh each run. VERIFIED LIVE post-restart: navigator's 5 expired
  options (all negative DTE) got picked up and an auto-close was attempted, correctly blocked by
  the weekend market-closed gate — will fill Monday.
- **Tier-1 roster swept** (main.py): removed dayblade-sulu/super-agent/deepseek-7b-grok4/
  ollama-coder (all halt_mode='full' or exit_only, never actually scanned) — kept mlx-qwen3
  (Batch-1 reopen candidate, verified its model_id `ministral-3:3b` IS installed on olliemax,
  so the "orphan seat" suspicion from the silence-since-05-07 theory doesn't hold; its halt_mode=
  'full' is just because it's benched, not broken).
- **Docstring fixes:** main.py scan-interval docstring said market=3min/power-hour=90s;
  SCAN_INTERVAL_MARKET/POWER_HOUR have both been 300s since the v3 2026-03-23 cost cut — fixed
  docstring + 2 stale inline comments to match. halt_gate.py claimed `is_halted` "remains for
  backwards-compat" — HM-B (2026-05-04, 9256890) already dropped that column entirely; verified
  live via PRAGMA table_info before editing; fixed.
- **Document-only (no code, per Admiral instruction):** item 9 (ADVISORY_CREW kill-gated bridge
  voters silently out of the WR vote) and item 21 (TRADE_DESK_BYPASS_GATES=True) both recorded
  in XO_BACKLOG.md as accepted consequences.
- **Tickets filed (XO_BACKLOG.md), not fixed:** item 11 (3-way divergent model-id sources), item
  12 (cto-grok42 dead model devstral-small-2), item 13 (naming dedup — two "Jadzia Dax"s,
  Scotty-vs-Dax, Chekov-vs-Ensign-Ro, stale roster counts), item 18 (9 paused personas vs real
  CREW_MANIFEST mandates — qwen3-8b-flash persona check flagged as a Batch-1 prereq), Sulu
  DayBlade-label sweep (see above).
- **Backtester MIN_HOLD_DAYS → live parity:** was a per-model dict (5-10 trading days, unrelated
  to anything live). Live paper_trader._check_min_hold is 24h universal / 72h(3d) for SWING
  timeframe. The v1 `signals` table this backtester reads has no per-signal timeframe column at
  all (ASSUMED_TIMEFRAME="swing" fallback already established by the 07-03 realism patch) — so
  every signal here was already being treated as swing; replaced the dict with a flat
  MIN_HOLD_DAYS=3 (3 trading days = this daily-bar backtester's honest analog to 72h). Smoke-tested
  clean (ollama-plutus/30d: 1105 signals, 21 trades).
- **Fleet sweep RELAUNCHED + COMPLETE under the new canonical rules** — launched 2026-07-04 ~07:32,
  finished ~08:26. 22 agents, `reports/fleet_realism_sweep_20260704_073227.json`,
  `logs/fleet_sweep_20260704_rerun.log`. The 2026-07-03 baseline JSON/log are UNTOUCHED (never
  overwrite, per KEEP ALL DATA). **Old→new comparison (guarded/honest):** ollama-qwen3 +20.06%→
  **+26.07%** (16 tr, now #1 overall) · options-sosnoff +11.78%→**+25.23%** (14→21 tr) ·
  **McCoy (ollama-plutus) +13.74%→+21.67%** (22→37 trades, #8→#3) · mlx-qwen3 +25.15%→+19.76%
  (14→20 tr) · **Spock (deepseek-7b-grok4) +0.56%→+5.00%** (14→26 trades, DESPITE
  MAX_TRADES_PER_DAY dropping 8→3) · Geordi (ollama-local) -0.06%→+0.94% · capitol-trades still
  last, -3.92%→-3.51%. Broad pattern: almost every agent shows MORE trades post-fix, not fewer —
  `MIN_HOLD_DAYS` (old 5-10 day per-model values → flat 3-day live parity) is the dominant driver,
  not the trade-count caps: shorter mandatory holds freed capital for more round-trips, and that
  proved more profitable across the board under guarded rules. This is the real answer to "did the
  rule consolidation change anything" — yes, materially, and net positive so far on this single
  backtest pass (thin samples throughout, same ±20pt CI caveat as everything else this week).

## HM-EXEC-PIPELINE-MERGE-2026-07-04 (Scotty, exec-pipeline → main)
Directive: merge exec-pipeline into main, verify the stop-loss fix live. Done, pushed, verified.
- **Merge commit `fbd25c6`** (`git merge --no-ff exec-pipeline`, two parents `ff8b5f0` main-tip +
  `c1dd786` exec-pipeline-tip — the full HM-AGENT-RULES-CONSOLIDATION range 49902bc..c1dd786, 99
  files changed, clean merge, zero conflicts). Pushed to `origin/main`.
- **Anomaly — stashed the other session's WIP to unblock checkout:** `git checkout main` was
  blocked by 8 uncommitted tracked-file changes on exec-pipeline that weren't mine (bridge-v2.html,
  manifest.json, agent_scoreboard.json, HANDOFF.md, daily_ledger.csv, canonical_gex.py,
  signal-center/{index.html,server.py} — this Cowork session's in-progress work). Stashed
  (reversible, not discarded) rather than block the directive; restored via `git stash pop
  stash@{0}` by exact message match after the merge was done, back to identical working-tree state.
  A SEPARATE, older, not-mine stash (`51650ad test(stops): HM-CONVICTION-TIER-BOUNDARY-CALIBRATION
  Opt 1 — mid-tier 0.15 -> 0.13`) was already sitting there and was left untouched — don't pop it
  without knowing whose it is / what it's for.
- **Anomaly — test gate wasn't literally green, flagged before pushing, Admiral approved proceed:**
  `.venv-deps/bin/pytest tests/ -q` (the literal directive command) hard-fails at collection —
  `test_auth.py` can't import (`ModuleNotFoundError: fastapi`). Confirmed via `git blame` this
  predates BOTH branches (same commit `53b9113` on both `main` and `exec-pipeline` pre-merge tips)
  — not a merge regression, just `.venv-deps` never had fastapi installed (matches the existing
  "test_auth still needs fastapi" note in Workflow rules above). With that one file excluded:
  **475 passed, 12 failed, 15 errors** — every single failure/error matches the already-documented
  pre-existing "NOT ours" list exactly (war_room instrumentation/budget, quality_gate_hold,
  universe_filter, ollama_cancel, conviction/trail flag tests, bbkc dedupe). Asked before pushing
  given main-push is the consequential step this gate protects; Admiral chose proceed.
- **Restart + verify on main, 2026-07-04 08:25** (`zsh scripts/trader_restart.sh`): single trader
  PID bound :8080, healthz `{"ok":true}`, zero database-is-locked since restart, bootstrap gate
  still holding (`BOOTSTRAP_METRICS_LIVE_ENABLED=False — skipping`), guardian startup sweep fired
  at 08:25:41 (30s post-boot, as designed).
- **Stop-loss fix verified live on main:** `RiskManager` instance/class has NO `get_stop_loss_pct`
  attribute at all anymore (confirmed via `hasattr`); `engine.stops.get_stop_loss_pct` resolves
  0.50→0.12 / 0.80→0.15 / 0.95→0.18; `paper_trader.py` confirmed calling the canonical
  `engine.stops` import directly, zero remaining `_rm.get_stop_loss_pct(` call sites in live code
  (the only string match left is my own explanatory comment documenting what used to be there).
- **Restart done + verified 2026-07-04 07:34** (`zsh scripts/trader_restart.sh`): single trader
  PID bound :8080, healthz `{"ok":true}`, HM-CN Phase 2 routing clean, zero tracebacks/CRITICAL/
  database-is-locked since restart. Bootstrap gate confirmed holding live
  (`[BOOTSTRAP] gated: BOOTSTRAP_METRICS_LIVE_ENABLED=False — skipping`) — no accidental first-
  ever population happened. Guardian startup sweep fired and correctly processed the newly-
  covered agents (see above).
