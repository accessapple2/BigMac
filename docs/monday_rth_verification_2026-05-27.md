# Monday RTH Verification — 2026-05-27

## Pre-market (06:00-06:30 AZ)
- [ ] Trader PID alive: `ps aux | grep main.py`
- [ ] Port 8080 bound: `lsof -ti :8080`
- [ ] Dashboard loads at http://192.168.1.248:8080
- [ ] Ollie Max (192.168.1.168) Ollama responding: `curl http://192.168.1.168:11434/api/tags`
- [ ] Check logs/trader.log for overnight errors: `grep -i "error\|exception\|CRITICAL" logs/trader.log | tail -20`

## Market open (06:30 AZ)
- [ ] HM-EVENTS-BUS-CONSUMER fires: `grep "EVENTS-BUS-CONSUMER" logs/trader.log | tail -5`
- [ ] [SELF-CLOSE] or [CONSUMER-DISPATCH] tags appearing in logs
- [ ] Chekov stop/target check fires (10-min cadence): `grep "run_chekov_stoploss" logs/trader.log | tail -3`
- [ ] deepseek-7b-grok4 (Spock) trading again — check leaderboard for first trade
- [ ] navigator (Chekov) — confirm PRICE-SANITY-REJECT fires if bad price detected

## SQUEEZE ARC verification (first scanner run ~07:00 AZ)
- [ ] BBKC-COMPRESSION scanner fired: `grep "BBKC" logs/trader.log | tail -5`
- [ ] RS-RANK scanner fired: `grep "RS_RANK" logs/trader.log | tail -5`
- [ ] MINERVINI scanner fired: `grep "MINERVINI" logs/trader.log | tail -5`
- [ ] PRE-BREAKOUT-COMPOSITE fired
- [ ] MOVERS-RS-OVERLAY fired
- [ ] Check TrendSpider DSL ref: docs/TrendSpider-Scanners-47-Reference.md

## Options universe (first options scan)
- [ ] 7 options agents hitting 345-symbol universe (not 2-6 hardcoded)
- [ ] QuarkIronCondor anchor=SPY confirmed
- [ ] AndersonBearCall anchor=SPY confirmed

## EOD checks (13:00 AZ close)
- [ ] signals_v2 pending count: `sqlite3 data/trader.db "SELECT status, COUNT(*) FROM signals_v2 GROUP BY status;"`
- [ ] No new ghost positions: `sqlite3 data/trader.db "SELECT player_id, symbol, qty, avg_price FROM positions WHERE qty < 0 AND avg_price < 10;"`
- [ ] Navigator positions still clean: `sqlite3 data/trader.db "SELECT symbol, qty, avg_price FROM positions WHERE player_id='navigator';"`
- [ ] Spock trade count > 0: `sqlite3 data/trader.db "SELECT COUNT(*) FROM trades WHERE player_id='deepseek-7b-grok4' AND DATE(executed_at)=DATE('now');"`
