# RSI Sweep Backtest Results

**Generated:** 2026-03-26 21:03:05
**Period:** 365 days
**Combos per ticker:** 80 (windows: [10,14,20,25,30] × entry: [20,25,30,35] × exit: [65,70,75,80])
**Strategy:** RSI Mean Reversion — buy when RSI crosses above entry threshold (oversold), sell when RSI crosses below exit threshold (overbought)

| Ticker | Best Return | Win Rate | Sharpe | Max Drawdown | Buy RSI | Sell RSI | Window |
|--------|-------------|----------|--------|--------------|---------|----------|--------|
| AMD | +84.64% | 100.0% | 2.086 | 13.76% | 35 | 70 | 20 |
| DELL | +57.87% | 83.3% | 1.758 | 14.01% | 35 | 75 | 10 |
| WMB | +41.68% | 100.0% | 2.962 | 7.73% | 35 | 75 | 25 |
| TSLA | +36.52% | 100.0% | 1.382 | 18.11% | 30 | 80 | 14 |
| META | +32.20% | 100.0% | 1.453 | 18.81% | 35 | 65 | 14 |
| AAPL | +31.08% | 66.7% | 2.001 | 10.73% | 35 | 80 | 14 |
| KMI | +28.79% | 100.0% | 2.695 | 5.42% | 30 | 75 | 20 |
| AMZN | +25.25% | 100.0% | 1.641 | 7.79% | 35 | 70 | 14 |
| ORCL | +9.46% | 50.0% | 0.482 | 23.92% | 30 | 65 | 14 |
| QQQ | +2.28% | 100.0% | 1.292 | 1.73% | 20 | 65 | 10 |
| SPY | +0.97% | 100.0% | 0.341 | 2.82% | 30 | 75 | 14 |
| NOW | +0.00% | 0.0% | 0.000 | 0.00% | 20 | 65 | 30 |

## Notes
- Initial cash per backtest: $7,000
- Fees: 0.1% per trade
- Data source: Yahoo Finance via VectorBT
- Entry signal: RSI **crossed above** buy threshold (ascending from oversold)
- Exit signal: RSI **crossed below** sell threshold (descending from overbought)
