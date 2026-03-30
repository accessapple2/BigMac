# Port 8000 — Old PaperTrader Dashboard

**Disabled:** 2026-03-23

## What it was
- Separate project at `/Users/bigmac/paper-trader/`
- Served by: `uvicorn main:app --host 127.0.0.1 --port 8000`
- Template: `/Users/bigmac/paper-trader/static/index.html` (backed up here as `papertrader_8000_template.html`)
- LaunchAgent: `~/Library/LaunchAgents/com.papertrader.server.plist` (KeepAlive=true, RunAtLoad=true)
- Log: `/Users/bigmac/paper-trader/server.log`

## Why disabled
Everything now runs through port 8080 (autonomous-trader/main.py).
Port 8000 was redundant and the ngrok tunnel points to 8080.

## How to re-enable
```bash
launchctl load ~/Library/LaunchAgents/com.papertrader.server.plist
```

## DayBlade trades
All DayBlade 0DTE trades are in `autonomous-trader/data/trader.db` (trades table).
Query: `SELECT * FROM trades WHERE player_id='dayblade-0dte' ORDER BY executed_at DESC;`
Total trades: ~50+ option trades (BUY_CALL, BUY_PUT, SELL)
