# Broker Posture — 2026-04-20

**Established by Admiral. Immutable until explicitly changed.**

## Current Rule

NO auto-trading on real brokers. Alpaca paper is the ONLY account where agents may execute. All real brokers are MONITOR ONLY.

If you find code that auto-trades on a real broker — STOP and report for Admiral decision. Do not auto-fix either way.

## Status

| Broker | Auto-trade? | Monitor? | Credentials | Notes |
|--------|-------------|----------|-------------|-------|
| Alpaca paper | ✅ agents execute here | yes | APCA_API_KEY_ID/SECRET | paper=True enforced |
| Schwab | ❌ no code | ✅ yes | none in .env | Steve's new real account; 4 AMZN logged manually |
| Webull | ❌ blocked at code level | ✅ yes | WEBULL_APP_KEY/SECRET (live, not rotated) | buy()/sell() raise PermissionError |
| IBKR | ❌ stub inert | ✅ yes | none | IBKRAdapter.submit_order() returns {"status":"inactive"} |
| TradeStation | ❌ stub inert | ✅ yes | none | TradeStationAdapter.submit_order() returns {"status":"inactive"} |

## Latent Risks (acknowledged, not fixed)

- Webull live API keys in `.env` (`WEBULL_APP_KEY/SECRET/ACCOUNT_ID`) — real write-capable credentials. `broker/webull_broker.py` would accept them if guards were removed. Admiral decision 2026-04-20: keep as-is, no rotation.
- `APCA_API_BASE_URL` env var not set → defaults to paper endpoint. If flipped to live Alpaca URL, agents would trade real money. No mitigation beyond process discipline.
- `_archive/nvda_strike.py` — Webull order script, archived 2026-04-20. Placeholder credentials only. No execution path.

## Promotion Path (real broker → auto-trade)

To promote any broker to auto-trade:
1. Admiral confirms in writing (update this file)
2. Remove PermissionError guards (webull_broker.py) or activate stubs (IBKR/TradeStation adapters)
3. Paper/sandbox first if available
4. Single agent, limited position size test
5. 30-day OOS Sharpe ≥ backtest baseline per CLAUDE.md promotion gate
6. Update this doc + memory file

## Memory Note

`steve-webull` player_id in `positions` table is written by BOTH Webull broker sync AND Alpaca portfolio sync (last-writer-wins). Do not confuse positions in this table with Steve's actual real-money account. See `memory/project_steve_webull_alpaca.md`.
