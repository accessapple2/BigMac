# USS TradeMinds — Claude Code Instructions

## CRITICAL: Python Environment

**ALWAYS use `venv/bin/python3` — NEVER `.venv` or bare `python3`.**

There are two virtualenvs in this repo:
- `venv/`  — the correct one. Has fastapi, scrapling, yfinance, rich, all deps.
- `.venv/` — Homebrew Python 3.14, missing fastapi. Dashboard cannot start.

The launchd plist (`~/Library/LaunchAgents/com.trademinds.trader.plist`) already
uses the absolute path `/Users/bigmac/autonomous-trader/venv/bin/python3`.
Manual restarts must match.

## Restart Command

```bash
cd ~/autonomous-trader && pkill -9 -f "main\.py" 2>/dev/null; lsof -ti :8080 | xargs kill -9 2>/dev/null; sleep 3; venv/bin/python3 main.py >> scanner.log 2>&1 &
```

Or use the wrapper script:
```bash
~/autonomous-trader/restart.sh
```

## Logs

- `scanner.log` — main stdout/stderr (nohup)
- `logs/trader.log` — launchd stdout
- `logs/trader_error.log` — launchd stderr

## Key Facts

- Dashboard: http://127.0.0.1:8080
- Port 8080 must be free before restart (`lsof -ti :8080 | xargs kill -9`)
- launchd KeepAlive=true — killing main.py causes automatic launchd restart
  using the plist's correct venv/bin/python3. Let launchd restart unless
  actively debugging.
