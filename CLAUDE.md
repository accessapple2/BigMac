# OllieTrades — Claude Code Session Guide
# READ THIS FIRST. EVERY SESSION.

## Sacred Rules (Never Break)
- NEVER delete, drop, or truncate trader.db or arena.db
- NEVER delete backtest history — INSERT only, never DELETE
- ALWAYS use venv/bin/python3
- Dashboard always serves from dashboard/static/index.html
- str_replace only — never rewrite whole files
- $1.50/day cost ceiling (currently $0.00)
- Paper trading ONLY

## Current State — Season 6 "Sniper Mode"
- Season: 6 | Start: 2026-04-10 | End: 2026-07-10
- Dashboard: bridge.ollietrades.com (port 8080)
- Signal Center: localhost:9000 (bound to 127.0.0.1)
- Login: Sniff (case sensitive) | Display: Admiral
- Broker: Alpaca Paper ~$99,612 equity
- Daily cost: $0.00 (all local Ollama)
- Last commit: 08cc0eb — 76 files

## Active Fleet (8 Free Agents — $0/day)
- ollie-auto (Ollie)     → Fleet Commander, OllieScore >= 2.0
- navigator (Chekov)     → Scanner, top free performer +$1,037
- ollama-llama (Uhura)   → Earnings plays, 71% WR best live
- ollama-plutus (McCoy)  → Finance expert, VIX > 22 ONLY
- ollama-qwen3 (Dax)     → Swing trades 2-5 days
- ollama-coder (Data)    → 11-signal composite scoring
- neo-matrix (Neo)       → High conviction, alpha >= 0.6 only
- capitol-trades         → Congress copycat, free signal

## Models on bigmac (ollama list)
- qwen3.5:9b    → Dax, Data, Neo, Ollie, Chekov (primary)
- 0xroyce/plutus → McCoy (finance specialist, 394 books)
- llama3.1      → Uhura (fast earnings decisions)
- mistral:7b    → Emergency fallback only

## Backtest Standard (MANDATORY — no exceptions)
ALL backtests import scripts/backtest_baseline.py.
Never run equity-only simulations again.

Every backtest must include:
- Real Ollama decisions per agent (not simulated scoring)
- VIX regime gating (McCoy ONLY trades when VIX > 22)
- Fear & Greed historical context (CNN Money API, cached)
- Agent-specific strategies from crew_specialization.py
- Regime-aware position sizing (CRISIS=0.25x, BULL=1.25x)
- Agent personas in every Ollama prompt
- Shared cache per (agent, sym, date) — not per version

Run backtests:
  venv/bin/python3 scripts/ollie_backtest_v6.py --days 5   # verify
  venv/bin/python3 scripts/ollie_backtest_v6.py --days 60  # full run

Monitor: tail -f /tmp/backtest_v6_t2.log (60-day Tier 2 running)

## Agent Personas (in every backtest prompt)
- Chekov:  scanner, BUY only if 3+ signals agree
- McCoy:   crisis doctor, BUY only if VIX > 22, GLD/TLT/defensive
- Dax:     patient swing trader, hold 3-7 days, don't cut early
- Data:    rules machine, BUY only if composite score > 0.6
- Neo:     high conviction only, BUY only if confidence >= 80

## What Was Fixed Today (commit 08cc0eb)
1. Ollie regime mismatch — NEUTRAL now scores 1.5 (was 1.0)
   TRENDING_UP matches correctly. Exact dict lookup, no hacks.
2. GEX now feeds into Ollie as 5th factor (10% weight)
   Reads gex_levels table, +0.4 if score>0.6 + bullish bias
3. Sulu iron condors now route to alpaca_options.py for real
   Alpaca paper execution. Falls back to paper_trader on error.
4. backtest_baseline.py — shared signal stack for ALL backtests
5. Ollama Tier 1+2 wired — real model decisions, VIX, F&G,
   personas, agent strategies, regime-aware sizing

## What's Still in the Queue
1. Holly A+ → Scout→Critic gate (pipeline exists, no trigger)
2. Options strategies in backtest (CSP, covered calls, condors)
3. 60-day Tier 2 results (running, PID 66453)

## Best Historical Results (Know These)
- Iron condor:    +249.6%, 82.1% WR (best ever)
- Covered calls:  Sharpe 3.145, 85.7% WR
- V3b fixed:      +16.30%, Sharpe 1.003, 61.5% WR, 87 trades
- Sniper TT:      Sharpe 1.136, 83.3% WR, 18 trades
- v6 Ollama 5-day: Sharpe 15.04 (best Sharpe, real Ollama)
- S6 beat SPY:    +2.39% alpha in 60-day bear market

## Key File Locations
- Main app:         dashboard/app.py
- Dashboard:        dashboard/static/index.html
- Crew scanner:     engine/crew_scanner.py
- Crew spec:        engine/crew_specialization.py
- Ollie commander:  engine/ollie_commander.py
- Wheel strategy:   engine/wheel_strategy.py
- Alpaca options:   engine/alpaca_options.py
- GEX engine:       engine/gex_engine.py
- Backtest standard: scripts/backtest_baseline.py
- Backtest runner:  scripts/ollie_backtest_v6.py
- Databases:        data/trader.db | data/arena.db (SACRED)
- Cache:            data/backtest_cache/
- Logs:             /tmp/trademinds.log | /tmp/backtest_v6_t2.log

## Server Commands
# Start fresh:
cd ~/autonomous-trader && source venv/bin/activate
nohup python main.py > /tmp/trademinds.log 2>&1 &

# Restart:
pkill -f "main.py" && sleep 2
nohup venv/bin/python3 main.py > /tmp/trademinds.log 2>&1 &

# Health check:
curl -s http://localhost:8080/api/health | head -c 100

# Check backtest progress:
tail -20 /tmp/backtest_v6_t2.log

## Weekly Habit (Every Sunday)
cd ~/autonomous-trader
git add -A
git commit -m "Weekly snapshot $(date +%Y-%m-%d)"
git push
