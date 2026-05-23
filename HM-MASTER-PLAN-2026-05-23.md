# HM-MASTER-PLAN-2026-05-23
# Captain's Standing Orders — XO Priority Stack
# Generated: 2026-05-23 06:43 AZ by XO
# Rules: ship in order unless Captain re-prioritizes, git commit each phase,
# restart after any main.py/engine change, NEVER rm .db files,
# browser smoke required for all frontend JS changes,
# daemon lifecycle rule applies to all new background workers,
# diagnostics first theories second, simplest solution first.

---

## WAVE 1 — HOUSEKEEPING (fast, high value, low risk)
Target: ship today

### W1-A — CLAUDE.md Polygon line update
Effort: 5 min
- Find "approved in principle" Polygon line in CLAUDE.md
- Replace with: "ACTIVE — $29/mo Stocks + Options Starter bundle. Wire as PRIMARY
  for candles + options; Alpaca + yfinance as fallbacks."
- Git commit: docs(claude): update Polygon status to active
- No restart needed

### W1-B — Riker launchd cron audit
Effort: 10 min
- Scheduler is now healthy (RCA closed 2026-05-23)
- Riker fires correctly on its own via schedule.every(10).minutes
- launchd cron at ~/Library/LaunchAgents/com.ollietrades.riker-cron.plist
  is now redundant but harmless
- Decision: keep as belt-and-suspenders (fires every 10 min = double-fire,
  not harmful) OR unload with:
  launchctl unload ~/Library/LaunchAgents/com.ollietrades.riker-cron.plist
- Captain's call — document decision in CLAUDE.md

### W1-C — HM-MORPHEUS content wiring (port 9000 tab data)
Effort: 1-2h
- Phase 7 shipped the structure; sections render but content is sparse
- Wire real data into each section:
  - 🟥 Red Alert → pull from ntfy ollietrades-admin last 24h alerts
  - 🔮 The Matrix → daily_snapshot latest row
  - 🧠 Intelligence → Riker synthesis latest
  - 🔭 Oracle → Kirk advisory latest recommendation
  - ⚓ Fleet Status → /api/cockpit/snapshot summary
  - 📜 Ship's Log → execution_log last 20 entries
- Browser smoke required before commit
- Git commit: feat(morpheus): wire real data into all 6 sections

---

## WAVE 2 — FLEET HEALTH (medium effort, high operational value)
Target: this weekend

### W2-A — deepseek-7b-grok4 thesis review
Effort: 30 min analysis + possible retirement
- Stop cap now in place ($150 max loss per trade)
- But: 96 wins / 20 losses with -$477 net = fundamentally broken risk/reward
- Pull trade breakdown by strategy type:
  SELECT strategy_id, COUNT(*), SUM(realized_pnl), AVG(realized_pnl)
  FROM trades WHERE player_id='deepseek-7b-grok4'
  GROUP BY strategy_id ORDER BY SUM(realized_pnl)
- If no redeemable strategy bucket: halt_mode='full' + document in CLAUDE.md
- If one bucket profitable: isolate to that strategy only

### W2-B — Bull_call_spread regime gate
Effort: 1-2h
- Backtest: 13% win rate in bear/tariff regime
- Active fleet still running bull_call_spread_v1 signals
- Add regime gate: only fire bull_call_spread signals when regime=BULL_CROSS
  AND VIX < 18 AND SPY above 200MA
- This is a config change not a rewrite — find regime check in
  run_bull_call_spread_signals() and add the gate
- Git commit: feat(spreads): regime gate for bull_call_spread_v1
- Restart + verify gate fires correctly in logs

### W2-C — Navigator options position review
Effort: 30 min
- LITE strike at $1,101 with qty 0.5 — likely deep ITM or stale
- MRAM, COHR, MNST positions flagged in backtest audit
- Pull current navigator positions + thesis for each
- Close any where thesis is stale or strike is unreachable

### W2-D — dalio-metals routing audit
Effort: 30 min
- dalio-metals had rogue ONDS equity short (now cleared)
- Verify route_mode='tracking' is enforced — no new equity positions possible
- Check if any other equity positions exist under dalio-metals
  SELECT * FROM positions WHERE player_id='dalio-metals'
- If any found: archive + delete same pattern as ONDS cleanup

---

## WAVE 3 — INFRASTRUCTURE (larger effort, long-term stability)
Target: next week

### W3-A — HM-BM Model Bakeoff (Ollie Max)
Effort: 4-6h
- Due: ~June 15 2026
- Ollie Max now has RTX 5080 + CUDA 13.2 — full GPU muscle available
- Models to evaluate: qwen3:14b vs qwen3:8b, deepseek-r1:14b,
  ministral-3:3b vs gemma3:4b, any new SOTA releases by June
- Bakeoff framework: HM-BM.md already scoped
- Run on Ollie Max (192.168.1.168), report to bigmac
- Update OLLIE_URL routing based on results
- Retire underperformers from fleet

### W3-B — HM-STOCK-PRICE-PROVENANCE full close
Effort: 1-2h
- Phase 6 added observability (source= in SANITY-WARN logs)
- Root cause of $533 MU phantom was db_cache poisoned by original writeback bug
- Now that entry_price backfill is done (613 rows fixed), the cache poison
  source is gone
- Verify: run 24h of live trading, check for any new SANITY-WARN fires
- If clean: close ticket. If new fires: trace source= field and fix upstream

### W3-C — HM-TRADE-DESK-AUTOPILOT Phase 3 (polish)
Effort: 3-4h
- Phase 1 + 2 shipped. Phase 3 = polish layer:
  - Cascade cancel: cancel parent → children auto-cancel (already in Phase 2,
    verify working end-to-end on Monday open)
  - Post-fill SL/TP edit UI (cancel children + resubmit flow)
  - Trailing stop variant (replace fixed SL with trailing %)
  - localStorage persistence of Captain's preferred defaults
- Browser smoke required
- Git commit: feat(autopilot): phase 3 polish

### W3-D — Congress copy strategy audit
Effort: 1h
- congress_copy showed 0 trades in backtest (no data)
- Capitol Trades agent is active and copying trades
- Verify: is congress_copy a separate strategy or the same agent?
- If separate + dormant: wire it or retire it
- If same as Capitol Trades: document and close

---

## WAVE 4 — CAPTAIN'S PORTFOLIO (real money, high priority)
Target: ongoing / Monday open

### W4-A — MU re-entry watch
- MU ~$757, exited at ~$102 during tariff bear
- Strong HBM/AI tailwind thesis intact
- Watch for pullback to $680-700 range as re-entry
- Add to Kirk watchlist with thesis note

### W4-B — COST earnings (Thu May 28)
- Dashboard flagged COST reporting Thursday
- Captain holds COST (real portfolio)
- Review position size before earnings
- Troi says: take profits / tighten stops in extreme greed environment

### W4-C — Real holdings.json sync
- Kirk shows $28,459 cash, real positions stale (85d ago trades showing)
- Run Schwab CSV export → scp to bigmac inbox → watcher imports
- Verify real_holdings.json reflects current Schwab state before Monday

---

## WAVE 5 — LONG GAME (lower urgency, high strategic value)
Target: June 2026

### W5-A — HM-AN Morpheus Phase 2
- Port 9000 full content wiring (W1-C above)
- Then: Morpheus as primary intelligence aggregator
- Daily briefing auto-push to ntfy at 6 AM AZ

### W5-B — Iron Condor fleet expansion
- Backtest: iron_condor = #1 strategy (+249% in tariff bear)
- Current: IC Squadron in shadow/queued
- Ship IC Squadron out of shadow: verify entry signals, size limits,
  risk gates, then flip to active
- Expected impact: largest single alpha improvement available

### W5-C — Spread King blend implementation
- Backtest Blend E: Spread King = +95.75% (iron_condor heavy)
- Current fleet allocation doesn't match this blend
- Build allocation rebalancer: shift capital toward IC + CSP + bull_put_spread
- Retire or reduce: momentum, long_call, mean_reversion allocations

### W5-D — HM-AM dashboard phase
- Total portfolio unification (engine) shipped
- Dashboard view deferred per Captain
- When ready: add Real Portfolio tab showing unified $37,914 view
  with Schwab + Metals + Webull + IBKR breakdown

---

## STANDING GUARDRAILS (every wave, every phase)
- NEVER rm trader.db, arena.db, tractor.db or any .db — archive/rename only
- NEVER rm -rf ~/ollietrades or ~/autonomous-trader
- Backup DB before any schema change
- Frontend JS: browser smoke before declaring shipped
- Daemon lifecycle: bind at module-level, never lazy
- Diagnostics first, theories second
- Simplest solution first — pivot after 2 failures
- git commit each phase separately
- launchctl kickstart after any main.py change
- Verify: pgrep -af main.py + lsof -ti :8080 after restart
- Real brokers (Schwab/Webull/IBKR) = MONITOR ONLY, never auto-trade

