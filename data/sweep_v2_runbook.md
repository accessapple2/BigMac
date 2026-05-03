# Model Sweep v2 — Operations Runbook
_Generated 2026-04-20 15:18 AZ_

## Identity
- **PID**: 27842
- **Log**: `~/autonomous-trader/logs/sweep_v2_20260420_151349.log`
- **Internal log**: `/tmp/model_sweep_v2.log`
- **DB**: writes to `backtest_runs`, `backtest_results`, `backtest_history` — no live tables

## Status Check
```bash
ps -p $(cat ~/autonomous-trader/data/sweep_v2.pid) -o pid,etime,state,%cpu
tail -20 $(ls -t ~/autonomous-trader/logs/sweep_v2_*.log | head -1)
sqlite3 ~/autonomous-trader/data/trader.db \
  "SELECT run_name, status, created_at FROM backtest_runs WHERE run_name LIKE 'sweep_v2%' ORDER BY created_at DESC LIMIT 5;"
```

## Kill Commands
```bash
# Pause sweep only (trader continues unaffected):
touch ~/autonomous-trader/SWEEP_KILL_SWITCH

# Abort sweep entirely (trader continues):
kill $(cat ~/autonomous-trader/data/sweep_v2.pid)

# Fleet halt — pauses BOTH sweep and trader:
touch ~/autonomous-trader/KILL_SWITCH
```

## Resume After Pause
```bash
# Clear the pause (after market close, 13:15+ AZ):
rm ~/autonomous-trader/SWEEP_KILL_SWITCH
# Sweep detects cleared switch within 60s and resumes automatically
```

## Time Guard
- Auto-pauses at **06:25 AZ** by creating `SWEEP_KILL_SWITCH`
- Designed to protect market-hours Ollie GPU capacity
- If sweep is incomplete: clear switch after market close (13:15+ AZ) to resume

## Schedule
- Window tonight: 15:16 AZ → 06:25 AZ tomorrow (~15h)
- 22 runs total, 12 tickers, 53 weekly decision points per ticker
- ~13,992 LLM calls total via Ollie GPU only (192.168.1.166:11434)

## Results
- Final report: `docs/MODEL_SWEEP_V2_RESULTS_2026-04-20.md`
- ntfy notification to `ollietrades-admin` on completion or halt
- **DO NOT AUTO-APPLY** any model changes — Admiral reviews results first
