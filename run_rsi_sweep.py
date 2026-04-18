"""RSI sweep backtest for 12 tickers using Holodeck engine."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from engine.holodeck import Holodeck
from datetime import datetime

TICKERS = ['AMD', 'SPY', 'AAPL', 'TSLA', 'QQQ', 'META', 'AMZN', 'DELL', 'ORCL', 'NOW', 'KMI', 'WMB']
DAYS = 365

holodeck = Holodeck()
all_best = []

for ticker in TICKERS:
    print(f"Running RSI sweep for {ticker}...", flush=True)
    try:
        result = holodeck.run_rsi_sweep(
            ticker,
            days=DAYS,
            rsi_windows=[10, 14, 20, 25, 30],
            entry_thresholds=[20, 25, 30, 35],
            exit_thresholds=[65, 70, 75, 80],
        )
        best = result['best']
        print(f"  {ticker}: best_return={best['total_return']}%, win_rate={best['win_rate']}%, sharpe={best['sharpe']}, max_dd={best['max_drawdown']}%, entry={best['entry']}, exit={best['exit']}, window={best['window']}", flush=True)
        all_best.append({
            'ticker': ticker,
            'total_return': best['total_return'],
            'win_rate': best['win_rate'],
            'sharpe': best['sharpe'],
            'max_drawdown': best['max_drawdown'],
            'buy_rsi': best['entry'],
            'sell_rsi': best['exit'],
            'window': best['window'],
            'combos': result['combos_tested'],
        })
    except Exception as e:
        print(f"  ERROR for {ticker}: {e}", flush=True)
        all_best.append({
            'ticker': ticker,
            'total_return': None,
            'win_rate': None,
            'sharpe': None,
            'max_drawdown': None,
            'buy_rsi': None,
            'sell_rsi': None,
            'window': None,
            'combos': 0,
            'error': str(e),
        })

# Sort by best return descending (None last)
all_best.sort(key=lambda x: x['total_return'] if x['total_return'] is not None else float('-inf'), reverse=True)

# Write results
timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
lines = []
lines.append(f"# RSI Sweep Backtest Results")
lines.append(f"")
lines.append(f"**Generated:** {timestamp}")
lines.append(f"**Period:** {DAYS} days")
lines.append(f"**Combos per ticker:** 80 (windows: [10,14,20,25,30] × entry: [20,25,30,35] × exit: [65,70,75,80])")
lines.append(f"**Strategy:** RSI Mean Reversion — buy when RSI crosses above entry threshold (oversold), sell when RSI crosses below exit threshold (overbought)")
lines.append(f"")
lines.append(f"| Ticker | Best Return | Win Rate | Sharpe | Max Drawdown | Buy RSI | Sell RSI | Window |")
lines.append(f"|--------|-------------|----------|--------|--------------|---------|----------|--------|")

for r in all_best:
    if r.get('error'):
        lines.append(f"| {r['ticker']} | ERROR | — | — | — | — | — | — |")
    else:
        ret = f"{r['total_return']:+.2f}%" if r['total_return'] is not None else "—"
        wr = f"{r['win_rate']:.1f}%" if r['win_rate'] is not None else "—"
        sh = f"{r['sharpe']:.3f}" if r['sharpe'] is not None else "—"
        dd = f"{r['max_drawdown']:.2f}%" if r['max_drawdown'] is not None else "—"
        lines.append(f"| {r['ticker']} | {ret} | {wr} | {sh} | {dd} | {r['buy_rsi']} | {r['sell_rsi']} | {r['window']} |")

lines.append(f"")
lines.append(f"## Notes")
lines.append(f"- Initial cash per backtest: $7,000")
lines.append(f"- Fees: 0.1% per trade")
lines.append(f"- Data source: Yahoo Finance via VectorBT")
lines.append(f"- Entry signal: RSI **crossed above** buy threshold (ascending from oversold)")
lines.append(f"- Exit signal: RSI **crossed below** sell threshold (descending from overbought)")

output = "\n".join(lines) + "\n"
out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "backtest_results.md")
with open(out_path, "w") as f:
    f.write(output)

print(f"\nResults saved to {out_path}")
print(f"\nSummary (sorted by return):")
for r in all_best:
    if not r.get('error'):
        print(f"  {r['ticker']:6s}  {r['total_return']:+7.2f}%  sharpe={r['sharpe']:.3f}  window={r['window']}  buy={r['buy_rsi']}  sell={r['sell_rsi']}")
