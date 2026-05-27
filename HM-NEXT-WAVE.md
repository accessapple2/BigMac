# HM-NEXT-WAVE — Captain's Priority Stack
# Generated: 2026-05-23 by XO
# Rules: ship in order, git commit each phase, no stopping between phases,
# restart trader after any main.py/paper_trader.py/engine change,
# NEVER rm any .db file — archive/rename only,
# NEVER touch Webull/Schwab/IBKR auto-trade — paper only,
# all DB changes require backup first,
# browser smoke test required for any frontend JS change before declaring shipped.

---

## PHASE 1 — HM-ALPACA-BRIDGE-LIMIT-FIX (PREREQ for Autopilot)
Priority: HIGH — blocks HM-TRADE-DESK-AUTOPILOT
Effort: ~2h

Scope:
- alpaca_bridge.py buy() signature: add time_in_force="day" param (already partially present, verify canonical)
- Add limit_price support: if limit_price > 0, submit LimitOrderRequest instead of MarketOrderRequest
- Add extended_hours flag: if True, time_in_force="day" (limit only, Alpaca requirement)
- Verify existing callers pass kwargs cleanly (no positional arg breakage)
- Test: paper BUY AAPL limit @ $1.00 (should queue, not fill) — confirm order appears in Alpaca pending
- Git commit: HM-ALPACA-BRIDGE-LIMIT-FIX
- Restart trader, verify PID + lsof :8080

---

## PHASE 2 — HM-TRADE-DESK-AUTOPILOT Phase 1 (Bracket — whole-share market/limit BUYs)
Priority: HIGH — Captain's manual orders flying naked
Effort: ~4-5h
Depends: PHASE 1 complete

Scope:
- dashboard/static/index.html Trade Desk form: add autopilot row under qty/notional
  - Stop loss checkbox (default ON) + pct input (default 8%)
  - Take profit checkbox (default ON) + pct input (default 15%)
  - Reset button → tdResetAutopilotDefaults()
  - Validation: sl_pct 0.5–50, tp_pct 0.5–200
  - Disable autopilot UI for SELL entries and STOP/STOP_LIMIT order types
- dashboard/app.py /api/alpaca/buy: accept attach_sl_pct, attach_tp_pct from request body, pass to alpaca.buy()
- alpaca_bridge.py buy(): if both pcts set + whole-share + not extended-hours → BracketOrderRequest
  - stop_loss price = fill_price * (1 - sl_pct/100)
  - take_profit price = fill_price * (1 + tp_pct/100)
  - both legs GTC
- Pending Orders table: render parent + SL pill + TP pill as single grouped row
- DB: add sl_order_id, tp_order_id cols to trades table (ALTER TABLE, no migration needed for paper)
- Reject autopilot for fractional/notional with clean UI error message
- Kill switch: autopilot child submissions honor is_halted gate
- Browser smoke test REQUIRED before declaring shipped (manual hover/click full UI flow)
- Git commit: HM-TRADE-DESK-AUTOPILOT-PHASE1
- Restart trader, verify

---

## PHASE 3 — HM-TRADE-DESK-AUTOPILOT Phase 2 (Fractional/Notional path)
Priority: MEDIUM-HIGH — primary Captain flow
Effort: ~4-5h
Depends: PHASE 2 complete

Scope:
- engine/trade_desk_autopilot.py (new module):
  - attach_children_after_fill(parent_order_id, side, sl_pct, tp_pct, agent_id)
  - Called from _poll_fill after fill confirmed
  - Submits SELL STOP @ fill_price*(1-sl_pct/100), SELL LIMIT @ fill_price*(1+tp_pct/100), qty=filled_qty, GTC
  - Writes sl_order_id + tp_order_id back to trades row
  - OCO daemon: 30s poll loop, module-level bind (per daemon lifecycle rule)
    - For each parent with both children open: if one child terminal → cancel sibling
    - NTFY ollietrades-admin on OCO trigger
  - Daemon crash → NTFY ollietrades-admin alert
- Cancel cascade: dashboard/app.py cancel endpoint — if canceling parent with live children, cancel children first
- Guard: filled_qty not requested_qty (partial fill safe)
- Guard: no double-attach if poll runs twice (idempotency check on sl_order_id IS NULL before attaching)
- Browser smoke test REQUIRED
- Git commit: HM-TRADE-DESK-AUTOPILOT-PHASE2
- Restart trader, verify OCO daemon fires within 30s of startup (check log)

---

## PHASE 4 — HM-TRADES-ENTRY-BACKFILL
Priority: MEDIUM — 88 rows with NULL entry_price from pre-fix era
Effort: ~1h

Scope:
- Find all trades rows WHERE entry_price IS NULL AND asset_type='stock'
- For each: attempt to backfill from positions table avg_price where player_id+symbol match near opened_at
- For rows with no positions match: set entry_price = exit_price (neutral, marks as unresolvable)
- Log each backfill action to backups/entry_backfill_YYYYMMDD.log
- Backup trades table before any UPDATE: cp data/trader.db backups/trader.db.pre-entry-backfill-YYYYMMDD
- Verify: SELECT COUNT(*) FROM trades WHERE entry_price IS NULL AND asset_type='stock' → should be 0
- Git commit: HM-TRADES-ENTRY-BACKFILL
- No restart needed (data fix only)

---

## PHASE 5 — HM-AM — Total Portfolio Unification (non-dashboard phase)
Priority: MEDIUM
Effort: ~2-3h

Scope:
- engine/total_portfolio.py (new module):
  - read_total_portfolio() → merges:
    - data/real_holdings.json (Schwab primary)
    - Webull positions (~$6.6K, monitor only)
    - IBKR positions ($0, monitor only)
    - engine/metals_ledger → Dilithium Reserve
  - EXCLUDES Alpaca paper (~$99,885) — paper stays separate
  - Returns unified dict: {accounts, total_real_value, by_account, by_symbol, metals_pct}
- Wire into Kirk advisory: engine/kirk_advisory.py reads total_portfolio() for position context
- Wire into Advisory Team: engine/team_advisor_grok.py passes total portfolio summary in prompt
- NTFY ollietrades-admin on portfolio sync errors
- No dashboard phase yet (deferred per Captain)
- Git commit: HM-AM-PORTFOLIO-UNIFICATION
- Restart trader, verify Kirk sees real holdings in next advisory cycle

---

## PHASE 6 — HM-STOCK-PRICE-PROVENANCE-AUDIT
Priority: MEDIUM — root cause of $533 MU phantom (get_stock_price() returning wrong price)
Effort: ~1-2h

Scope:
- Trace get_stock_price() call chain for MU on 2026-05-01
- Identify which data source returned $533 (Polygon? yfinance? cached stale value?)
- Add source tagging to get_stock_price() return: {'price': x, 'source': 'polygon|yfinance|cache|alpaca'}
- Log source at every SELL time alongside TRADE-PRICE-SANITY-WARN
- If stale cache is culprit: add TTL expiry (suggest 60s for intraday)
- Git commit: HM-STOCK-PRICE-PROVENANCE-AUDIT
- No restart needed unless cache TTL added to live path

---

## PHASE 7 — HM-AN — Morpheus/Port 9000 Reframe
Priority: LOW-MEDIUM
Effort: ~5h
Depends: HM-AM complete

Scope:
- New dashboard tab: tab-matrix as default landing for port 9000
- Sections: Red Alert / Matrix / Intelligence / Oracle / Fleet / Ship's Log
- Activate daily_snapshot + execution_log empty tables
- Morpheus = Operator of port 9000 (the Matrix); neo-matrix stays as fleet agent
- Browser smoke test REQUIRED
- Git commit: HM-AN-MORPHEUS-REFRAME
- Restart signal-center server, verify port 9000 health

---

## STANDING GUARDRAILS (apply to every phase)
- NEVER rm trader.db, arena.db, tractor.db or any .db — archive/rename only
- NEVER rm -rf ~/ollietrades or ~/autonomous-trader
- Backup DB before any schema change: cp data/trader.db backups/trader.db.pre-HM-XXX-YYYYMMDD
- Frontend JS changes: browser smoke test before declaring shipped (HM-BJ lesson)
- Daemon lifecycle: bind at module-level, never lazy (HM-EQ lesson)
- Diagnostics first, theories second
- Simplest solution first — if something fails twice, pivot
- git commit each phase separately with HM ticket in message
- launchctl kickstart -k gui/$(id -u)/com.trademinds.trader after any main.py change
- Verify post-restart: pgrep -af main.py (new PID) + lsof -ti :8080 (port bound)

