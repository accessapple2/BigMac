# Relay — 2026-09-13. Fire targets from live fleet · stop coverage (report) · Season 8 config row

RULE #1 held: no trade/signal/decision/rating/prompt row deleted or rewritten. The only
live DB write was one new `season_config` row + one new `settings.season_8_name` key,
backup-first (`data/backups/trader_2026-09-13_0927_pre-s8-season-config.db`, integrity ok).

## 1. Fire Agent panel + Smart Money — `e75f735`
**Wrong before:** the Research tab's Fire Agent select was a hardcoded roster. "Lt. Jadzia
Dax · qwen3:8b" is live `ollama-qwen3`, halt_mode=full, `ministral-3:3b`. "Lt. Cmdr. Data ·
neo-matrix" conflated two full-halt seats (`neo-matrix` "Neo"; `ollama-coder` "Lt. Cmdr.
Data", `qwen2.5-coder:7b`). Every BUY on the tab (Signal Consensus, Smart Money, TI
watchlist, Volume Radar) fired that select. Smart Money listed halted buyers as consensus.
`/api/paper-trader/manual-trade` never checked halt_mode (`buy()` does, further down).

**Fix:** `engine/fire_targets.py` (mirrors `buy()`'s halt/human/audition/bench gates) →
`GET /api/fleet/fire-targets`; manual-trade refuses a non-eligible BUY before price fetch;
`/api/smart-money` stamps live halt_mode + `active_buyers`; v2 select built from the
endpoint with real `model_id`, `fireCandidate` refuses off-list agents, halted-only Smart
Money rows get no BUY; Classic alert-card BUY renders unavailable unless live-eligible.

**Live (PID 92478):** targets = Counselor Troi · qwen3:8b, Dr. McCoy · plutus-v1, Lt. Cmdr.
Worf · qwen3:8b; 23 halted model seats excluded (incl. Dax, Data); Smart Money INTC shows
0 active buyers. Served `/static/bridge-v2.html` select has no hardcoded options; served
Classic carries the alert-card gate. Not live-exercised: a POST to manual-trade (order path)
— covered by the unit test, which asserts `buy()` is never reached. Browser render not
checked (pages are login-gated; APIs are loopback-open).

## 2. Stop coverage — report only, no orders placed or modified
- KMI (18 @ 33.825, now 30.86, −8.77%) and TQQQ (0.31 @ 81.228, now 70.98, −12.62%) are held
  only by `alpaca-mirror` (Alpaca PAPER broker mirror, halt_mode=full), same for AG/AVB/BLK.
  None are in Schwab `real_holdings.json`.
- Hard-stop sweep `crew_scanner._check_hard_stops()`: −8% flat, iterates every position
  holder (so includes the mirror), but `paper_trader.sell()` returns early for players that
  aren't auto-tradeable — `is_auto_tradeable('alpaca-mirror')` = False. No HARD STOP line for
  KMI/TQQQ in trader.log since 09-09. **Nominally covered, cannot act.**
- Guardian sweep: flat −12%, `exit_only` agents only; the mirror is `full`. **Not covered.**
- Broker: 9 open Alpaca orders, all market-type, none on these 5 symbols; last 50 orders
  contain no stop/stop_limit type at all; zero `trades` rows carry `stop_loss_order_id`.
  **No stop order exists on either position.**
- "Stops in place on AG/AVB/BLK": the 09-11 text is the `cto_briefings` portfolio advisory
  (Spock's, which Riker's synthesis ingests): "Stop-loss at $16.00 / $180.00 / $1000.00" —
  recommended levels, not confirmations. Riker's synthesis isn't persisted. **Not
  evidenced**: no stop orders at the broker, no stop order ids on file.

## 3. Season 8 config row — `bbd8d71`
- `rotate_season()`/`start_season()` now write `season_config` + `season_N_name` inside the
  rotation (INSERT OR IGNORE, never rewrites; active_agents = halt_mode='active' ids after
  unhalt, same rule as S7's hand-written row). `ensure_season_config()` backfills from
  `settings.season_N_start`, idempotent, refuses to invent a start date.
- Backfill run via committed code: S8 row = "Season 8", start 2026-09-11, 8 active agents.
  S6/S7 rows byte-identical to backup; settings: +`season_8_name`, nothing changed.
- Live: `/api/season` config populated → panel reads "Active: 8 agents".
- Still open (not in scope): S7 `end_date` is NULL; rotation doesn't set the ending season's
  end_date.

## Tests
`test_bridge_consistency.py` 10 passed + 2 strict xfails (items 1, 2 still open);
`test_season_config_autowrite.py` 6 + existing rotation-scope tests pass; full suite
failure set identical to baseline.
