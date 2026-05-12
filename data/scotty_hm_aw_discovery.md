# HM-AW Discovery — 2026-05-11

## Confirmed
- Gate line: `if now.hour != 22: return` at `main.py:2134`
- `execute_convergence_trades` only called inside the gated function (line 2148, wrapped in try/except inside the 22:00 block)
- `run_chekov_stoploss` defined at `main.py:2155`, closes at line 2161 (trailing `except`)
- Schedule line `schedule.every(10).minutes.do(run_chekov_stoploss)` at `main.py:3074`
- HM-AW anchors absent (`grep` returned no matches — idempotency confirmed)
- `execute_convergence_trades` signature: `engine/chekov_autotrade.py:347 — def execute_convergence_trades(signals: list = None):`

## Insertion plan
- **Phase 1:** insert new function `run_chekov_intraday_convergence` between line 2161 (end of `run_chekov_stoploss`) and line 2164 (`_premarket_gaps_done = False` module-level flag). One blank line padding above and below preserved.
- **Phase 2:** insert new schedule entry immediately after line 3074 (`schedule.every(10).minutes.do(run_chekov_stoploss)`).

## Restart impact
- Yes — new function + new schedule entry require trader restart to take effect
- Captain will issue `launchctl kickstart -k gui/$(id -u)/com.trademinds.trader` after commit
- Scotty does NOT restart the service.

## Scope guards
- main.py only
- No DB writes
- No push
- No service restart
- No Vite tree
