# Monday RTH Verification — 2026-05-27

## 06:25 AZ — Pre-Open (5 min before open)
- [ ] Confirm PID 88538 alive: `ps aux | grep main.py | grep -v grep`
- [ ] Confirm kill switch ARMED: `curl -s http://localhost:8080/api/status | python3 -c "import sys,json;d=json.load(sys.stdin);print('KILL SWITCH:', d.get('is_halted'))"`
- [ ] Confirm PRAX queue still loaded: `cat data/pending_manual_closes.json`
- [ ] Check PRAX pre-market price (Webull/TradingView)
- [ ] Confirm Ollie Max alive: `curl -s http://192.168.1.168:11434/api/tags | python3 -c "import sys,json;print('Ollie Max OK:', len(json.load(sys.stdin)['models']), 'models')"`

## 06:30 AZ — Market Open
- [ ] PRAX auto-close fires within first 1-min tick
- [ ] Watch logs: `tail -f logs/trader.log | grep PRAX`
- [ ] Confirm Alpaca paper fill: check Alpaca paper dashboard for PRAX sell order

## 06:35 AZ — Post-Close Verify
- [ ] pending_manual_closes.json = []: `cat data/pending_manual_closes.json`
- [ ] PRAX position = 0: `sqlite3 data/trader.db "SELECT * FROM positions WHERE symbol='PRAX';"`
- [ ] Log entry present: `grep "MANUAL-CLOSE.*PRAX" logs/trader.log`

## General RTH Health Check
- [ ] Dashboard live: http://localhost:8080 hard reload
- [ ] Fleet P&L loading on Bridge
- [ ] War Room heartbeat green (< 5m ago)
- [ ] Regime showing (not stale)
- [ ] No red errors in: `tail -50 logs/trader.log | grep -i error`

## If PRAX auto-close FAILS
1. Check log: `grep "pending_manual" logs/trader.log | tail -10`
2. Manual fallback via Trade Desk: PRAX / SELL / Market
3. Verify fill in Alpaca paper dashboard

---

## Additional verification — broader RTH coverage

### Market open (06:30 AZ) — feature-level
- [ ] HM-EVENTS-BUS-CONSUMER fires: `grep "EVENTS-BUS-CONSUMER" logs/trader.log | tail -5`
- [ ] [SELF-CLOSE] or [CONSUMER-DISPATCH] tags appearing in logs
- [ ] Chekov stop/target check fires (10-min cadence): `grep "run_chekov_stoploss" logs/trader.log | tail -3`
- [ ] deepseek-7b-grok4 (Spock) trading again — check leaderboard for first trade
- [ ] navigator — confirm PRICE-SANITY-REJECT fires if bad price detected

### SQUEEZE ARC verification (first scanner run ~07:00 AZ)
- [ ] BBKC-COMPRESSION scanner fired: `grep "BBKC" logs/trader.log | tail -5`
- [ ] RS-RANK scanner fired: `grep "RS_RANK" logs/trader.log | tail -5`
- [ ] MINERVINI scanner fired: `grep "MINERVINI" logs/trader.log | tail -5`
- [ ] PRE-BREAKOUT-COMPOSITE fired
- [ ] MOVERS-RS-OVERLAY fired
- [ ] Check TrendSpider DSL ref: docs/TrendSpider-Scanners-47-Reference.md

### Options universe (first options scan)
- [ ] 7 options agents hitting 345-symbol universe (not 2-6 hardcoded)
- [ ] QuarkIronCondor anchor=SPY confirmed
- [ ] AndersonBearCall anchor=SPY confirmed

### EOD checks (13:00 AZ close)
- [ ] signals_v2 pending count: `sqlite3 data/trader.db "SELECT status, COUNT(*) FROM signals_v2 GROUP BY status;"`
- [ ] gate_reject_log volume (HM-GATE-REJECT-TELEMETRY-V1): `sqlite3 data/trader.db "SELECT gate_name, COUNT(*) FROM gate_reject_log GROUP BY gate_name ORDER BY 2 DESC;"`
- [ ] No new ghost positions: `sqlite3 data/trader.db "SELECT player_id, symbol, qty, avg_price FROM positions WHERE qty < 0 AND avg_price < 10;"`
- [ ] Navigator positions still clean: `sqlite3 data/trader.db "SELECT symbol, qty, avg_price FROM positions WHERE player_id='navigator';"`
- [ ] Spock trade count > 0: `sqlite3 data/trader.db "SELECT COUNT(*) FROM trades WHERE player_id='deepseek-7b-grok4' AND DATE(executed_at)=DATE('now');"`
- [ ] signal_history fresh writes (HM-SIGNAL-CENTER-REFRESH): `sqlite3 signal-center/signals.db "SELECT MAX(timestamp), COUNT(*) FROM signal_history WHERE timestamp >= '2026-05-27';"`
- [ ] daily_snapshot 5/27 row landed (HM-DAILY-SNAPSHOT-REFRESH fires 13:00-14:00 AZ): `sqlite3 signal-center/signals.db "SELECT * FROM daily_snapshot WHERE date='2026-05-27';"`
