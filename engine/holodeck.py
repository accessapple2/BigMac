"""Holodeck — VectorBT-powered fast backtesting engine for TradeMinds.

Runs parameter sweeps across RSI, MACD, Bollinger, and SMA strategies
in seconds using vectorized computation.
"""
import vectorbt as vbt
import pandas as pd
import numpy as np
import json
from datetime import datetime, timedelta


def _s(v):
    """Convert NaN/Inf to 0 for JSON serialization."""
    f = float(v)
    if np.isnan(f) or np.isinf(f):
        return 0.0
    return f


def _stat(stats, key, decimals=2):
    """Safely extract and round a stat value."""
    v = stats.get(key, 0)
    if v is None:
        return 0.0
    return round(_s(v), decimals)


class Holodeck:
    """VectorBT-powered fast backtesting engine for TradeMinds"""

    def run_rsi_sweep(self, symbol, days=180, rsi_windows=None, entry_thresholds=None, exit_thresholds=None, cash=7000, fees=0.001):
        """Sweep RSI parameters — test hundreds of combos in seconds"""
        if rsi_windows is None:
            rsi_windows = [10, 14, 20, 25, 30]
        if entry_thresholds is None:
            entry_thresholds = [20, 25, 30, 35]
        if exit_thresholds is None:
            exit_thresholds = [65, 70, 75, 80]

        start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        data = vbt.YFData.download(symbol, start=start).get("Close")

        results = []
        for window in rsi_windows:
            rsi = vbt.RSI.run(data, window=window)
            for entry_th in entry_thresholds:
                for exit_th in exit_thresholds:
                    entries = rsi.rsi_crossed_above(entry_th)
                    exits = rsi.rsi_crossed_below(exit_th)
                    pf = vbt.Portfolio.from_signals(data, entries, exits, freq='1D', fees=fees, init_cash=cash)
                    stats = pf.stats()
                    results.append({
                        'window': window,
                        'entry': entry_th,
                        'exit': exit_th,
                        'total_return': _stat(stats, 'Total Return [%]'),
                        'win_rate': _stat(stats, 'Win Rate [%]'),
                        'max_drawdown': _stat(stats, 'Max Drawdown [%]'),
                        'sharpe': _stat(stats, 'Sharpe Ratio', 3),
                        'profit_factor': _stat(stats, 'Profit Factor'),
                        'num_trades': int(_s(stats.get('Total Trades', 0))),
                        'final_value': round(_s(pf.final_value()), 2),
                    })

        results.sort(key=lambda x: x['total_return'], reverse=True)
        return {
            'symbol': symbol, 'days': days, 'combos_tested': len(results),
            'best': results[0] if results else None,
            'worst': results[-1] if results else None,
            'top_10': results[:10], 'all_results': results
        }

    def run_bollinger_sweep(self, symbol, days=180, windows=None, std_devs=None, cash=7000, fees=0.001):
        """Sweep Bollinger Band parameters"""
        if windows is None:
            windows = [15, 20, 25, 30]
        if std_devs is None:
            std_devs = [1.5, 2.0, 2.5, 3.0]

        start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        data = vbt.YFData.download(symbol, start=start).get("Close")

        results = []
        for window in windows:
            for std in std_devs:
                bb = vbt.BBANDS.run(data, window=window, alpha=std)
                entries = data < bb.lower
                exits = data > bb.middle
                pf = vbt.Portfolio.from_signals(data, entries, exits, freq='1D', fees=fees, init_cash=cash)
                stats = pf.stats()
                results.append({
                    'window': window, 'std_dev': std,
                    'total_return': _stat(stats, 'Total Return [%]'),
                    'win_rate': _stat(stats, 'Win Rate [%]'),
                    'max_drawdown': _stat(stats, 'Max Drawdown [%]'),
                    'sharpe': _stat(stats, 'Sharpe Ratio', 3),
                    'num_trades': int(_s(stats.get('Total Trades', 0))),
                    'final_value': round(_s(pf.final_value()), 2),
                })

        results.sort(key=lambda x: x['total_return'], reverse=True)
        return {
            'symbol': symbol, 'days': days, 'combos_tested': len(results),
            'best': results[0] if results else None,
            'top_10': results[:10], 'all_results': results
        }

    def run_macd_sweep(self, symbol, days=180, fast_periods=None, slow_periods=None, signal_periods=None, cash=7000, fees=0.001):
        """Sweep MACD parameters"""
        if fast_periods is None:
            fast_periods = [8, 10, 12, 14]
        if slow_periods is None:
            slow_periods = [20, 24, 26, 30]
        if signal_periods is None:
            signal_periods = [7, 9, 11]

        start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        data = vbt.YFData.download(symbol, start=start).get("Close")

        results = []
        for fast in fast_periods:
            for slow in slow_periods:
                if fast >= slow:
                    continue
                for sig in signal_periods:
                    macd = vbt.MACD.run(data, fast_window=fast, slow_window=slow, signal_window=sig)
                    entries = macd.macd_crossed_above(macd.signal)
                    exits = macd.macd_crossed_below(macd.signal)
                    pf = vbt.Portfolio.from_signals(data, entries, exits, freq='1D', fees=fees, init_cash=cash)
                    stats = pf.stats()
                    results.append({
                        'fast': fast, 'slow': slow, 'signal': sig,
                        'total_return': _stat(stats, 'Total Return [%]'),
                        'win_rate': _stat(stats, 'Win Rate [%]'),
                        'max_drawdown': _stat(stats, 'Max Drawdown [%]'),
                        'sharpe': _stat(stats, 'Sharpe Ratio', 3),
                        'num_trades': int(_s(stats.get('Total Trades', 0))),
                        'final_value': round(_s(pf.final_value()), 2),
                    })

        results.sort(key=lambda x: x['total_return'], reverse=True)
        return {
            'symbol': symbol, 'days': days, 'combos_tested': len(results),
            'best': results[0] if results else None,
            'top_10': results[:10], 'all_results': results
        }

    def run_custom_strategy(self, symbol, days=180, strategy_type='sma_cross', params=None, cash=7000, fees=0.001):
        """Run a single strategy with specific params — returns equity curve data for charting"""
        if params is None:
            params = {}
        start = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        data = vbt.YFData.download(symbol, start=start).get("Close")

        if strategy_type == 'sma_cross':
            fast = params.get('fast', 10)
            slow = params.get('slow', 50)
            fast_ma = vbt.MA.run(data, window=fast)
            slow_ma = vbt.MA.run(data, window=slow)
            entries = fast_ma.ma_crossed_above(slow_ma.ma)
            exits = fast_ma.ma_crossed_below(slow_ma.ma)
        elif strategy_type == 'rsi':
            window = params.get('window', 14)
            entry = params.get('entry', 30)
            exit_th = params.get('exit', 70)
            rsi = vbt.RSI.run(data, window=window)
            entries = rsi.rsi_crossed_above(entry)
            exits = rsi.rsi_crossed_below(exit_th)
        elif strategy_type == 'bollinger':
            window = params.get('window', 20)
            std = params.get('std', 2.0)
            bb = vbt.BBANDS.run(data, window=window, alpha=std)
            entries = data < bb.lower
            exits = data > bb.middle
        else:
            return {'error': f'Unknown strategy: {strategy_type}'}

        pf = vbt.Portfolio.from_signals(data, entries, exits, freq='1D', fees=fees, init_cash=cash)
        stats = pf.stats()

        # Equity curve for charting
        equity = pf.value()
        equity_data = [
            {'date': str(d.date()), 'value': round(_s(v), 2)}
            for d, v in zip(equity.index, equity.values)
        ]

        # Trade list
        trades = pf.trades.records_readable
        trade_list = []
        if len(trades) > 0:
            for _, t in trades.iterrows():
                trade_list.append({
                    'entry_date': str(t.get('Entry Timestamp', '')).split(' ')[0],
                    'exit_date': str(t.get('Exit Timestamp', '')).split(' ')[0],
                    'entry_price': round(_s(t.get('Entry Price', 0)), 2),
                    'exit_price': round(_s(t.get('Exit Price', 0)), 2),
                    'pnl': round(_s(t.get('PnL', 0)), 2),
                    'return_pct': round(_s(t.get('Return', 0)) * 100, 2),
                    'direction': 'LONG',
                })

        return {
            'symbol': symbol, 'strategy': strategy_type, 'params': params, 'days': days,
            'total_return': _stat(stats, 'Total Return [%]'),
            'win_rate': _stat(stats, 'Win Rate [%]'),
            'max_drawdown': _stat(stats, 'Max Drawdown [%]'),
            'sharpe': _stat(stats, 'Sharpe Ratio', 3),
            'profit_factor': _stat(stats, 'Profit Factor'),
            'num_trades': int(_s(stats.get('Total Trades', 0))),
            'final_value': round(_s(pf.final_value()), 2),
            'equity_curve': equity_data,
            'trades': trade_list[:50],
        }


holodeck = Holodeck()
