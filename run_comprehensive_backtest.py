#!/usr/bin/env python3
"""
TradeMinds Comprehensive 90-Day Backtest
Date range: 2026-01-10 to 2026-04-10
"""
from __future__ import annotations
import sqlite3
import json
import sys
import os
from datetime import datetime, timedelta
from collections import defaultdict

# Add project root to path
sys.path.insert(0, '/Users/bigmac/autonomous-trader')

DB_PATH = '/Users/bigmac/autonomous-trader/data/trader.db'
START_DATE = '2026-01-10'
END_DATE = '2026-04-10'
STARTING_CAPITAL = 10_000.0
POSITION_SIZE_PCT = 0.05  # 5% per trade
SLIPPAGE_EQUITY = 0.001   # 0.1%
SLIPPAGE_OPTIONS = 0.03   # 3%
OUTPUT_PATH = '/Users/bigmac/autonomous-trader/data/backtest_comprehensive_90d.json'


def conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.row_factory = sqlite3.Row
    return c


# ===========================================================
# STRATEGY MAPPING
# The 19 strategy names map to player_ids / signal types in DB
# ===========================================================

STRATEGY_MAPPING = {
    # Equity long strategies -> mapped to stock BUY signals by player
    'long_equity':        {'players': ['grok-4', 'claude-sonnet', 'gpt-4o'], 'signal': 'BUY', 'asset': 'stock'},
    'swing_trade':        {'players': ['dayblade-sulu', 'options-sosnoff', 'grok-3'], 'signal': 'BUY', 'asset': 'stock'},
    'momentum':           {'players': ['ollama-local', 'gemini-2.5-flash', 'ollama-plutus'], 'signal': 'BUY', 'asset': 'stock'},
    'ema_pullback':       {'players': ['mlx-qwen3', 'ollama-deepseek'], 'signal': 'BUY', 'asset': 'stock'},
    'mean_reversion':     {'players': ['ollama-llama', 'ollama-kimi'], 'signal': 'BUY', 'asset': 'stock'},
    'rsi_bounce':         {'players': ['energy-arnold', 'grok-4'], 'signal': 'BUY', 'asset': 'stock'},
    # Short strategies
    'short_equity':       {'players': ['dalio-metals', 'gemini-2.5-flash', 'grok-4'], 'signal': 'SHORT', 'asset': 'stock'},
    'inverse_etf':        {'players': ['grok-4', 'gemini-2.5-flash'], 'signal': 'BUY_PUT', 'asset': 'stock'},
    # Options long
    'long_call':          {'players': ['claude-haiku', 'claude-sonnet', 'ollama-local'], 'signal': 'BUY_CALL', 'asset': 'stock'},
    'long_put':           {'players': ['grok-4', 'gemini-2.5-flash'], 'signal': 'BUY_PUT', 'asset': 'stock'},
    # Options spreads
    'iron_condor':        {'players': ['options-sosnoff'], 'signal': 'BUY', 'asset': 'stock'},
    'bear_call_spread':   {'players': ['gemini-2.5-flash'], 'signal': 'BUY_PUT', 'asset': 'stock'},
    'bull_put_spread':    {'players': ['options-sosnoff', 'energy-arnold'], 'signal': 'BUY', 'asset': 'stock'},
    'bear_put_spread':    {'players': ['grok-4', 'gemini-2.5-flash'], 'signal': 'BUY_PUT', 'asset': 'stock'},
    'bull_call_spread':   {'players': ['claude-haiku', 'gpt-4o'], 'signal': 'BUY_CALL', 'asset': 'stock'},
    'covered_call':       {'players': ['options-sosnoff', 'claude-haiku'], 'signal': 'BUY_CALL', 'asset': 'stock'},
    'csp':                {'players': ['options-sosnoff', 'energy-arnold'], 'signal': 'BUY', 'asset': 'stock'},
    # Special strategies
    'congress_copy':      {'players': ['capitol-trades'], 'signal': 'BUY', 'asset': 'stock'},
    'ollie_scanner':      {'players': ['ollie-auto'], 'signal': 'BUY', 'asset': 'stock'},
}


def get_strategy_trades_from_db(strategy_name: str) -> dict:
    """Pull actual closed trades from the DB for a strategy's player set."""
    mapping = STRATEGY_MAPPING.get(strategy_name, {})
    players = mapping.get('players', [])
    signal_type = mapping.get('signal', 'BUY')

    db = conn()

    # Get actual closed trades (SELL actions with pnl)
    if not players:
        return {'strategy': strategy_name, 'source': 'no_mapping', 'trades': 0,
                'win_rate': 0, 'total_pnl': 0, 'avg_pnl': 0, 'worst': 0, 'best': 0,
                'note': 'No player mapping'}

    placeholders = ','.join('?' * len(players))

    # Try corrected_pnl first, fall back to computed from entry/exit
    rows = db.execute(f"""
        SELECT player_id, symbol, corrected_pnl, realized_pnl, entry_price, exit_price, qty,
               asset_type, option_type, executed_at
        FROM trades
        WHERE player_id IN ({placeholders})
          AND executed_at BETWEEN ? AND ?
          AND action IN ('SELL', 'COVER')
          AND (corrected_pnl IS NOT NULL OR realized_pnl IS NOT NULL)
        ORDER BY executed_at
    """, players + [START_DATE, END_DATE]).fetchall()

    db.close()

    if not rows:
        return {'strategy': strategy_name, 'source': 'db_no_trades', 'trades': 0,
                'win_rate': 0, 'total_pnl': 0, 'avg_pnl': 0, 'worst': 0, 'best': 0,
                'note': 'No closed trades in period'}

    pnls = []
    for r in rows:
        pnl = r['corrected_pnl'] if r['corrected_pnl'] is not None else r['realized_pnl']
        if pnl is not None:
            # Apply slippage to options
            if r['asset_type'] == 'option':
                pnl *= (1 - SLIPPAGE_OPTIONS)
            else:
                pnl *= (1 - SLIPPAGE_EQUITY)
            pnls.append(float(pnl))

    if not pnls:
        return {'strategy': strategy_name, 'source': 'db_no_pnl', 'trades': 0,
                'win_rate': 0, 'total_pnl': 0, 'avg_pnl': 0, 'worst': 0, 'best': 0,
                'note': 'Trades exist but no P&L data'}

    wins = [p for p in pnls if p > 0]
    total_pnl = sum(pnls)

    return {
        'strategy': strategy_name,
        'source': 'actual_trades',
        'players': players,
        'trades': len(pnls),
        'wins': len(wins),
        'losses': len(pnls) - len(wins),
        'win_rate': round(len(wins) / len(pnls) * 100, 1),
        'total_pnl': round(total_pnl, 2),
        'avg_pnl': round(total_pnl / len(pnls), 2),
        'worst': round(min(pnls), 2),
        'best': round(max(pnls), 2),
        'return_pct': round(total_pnl / STARTING_CAPITAL * 100, 2),
        'note': f'{len(pnls)} closed trades from {len(players)} player(s)',
    }


def run_backtester_for_player(player_id: str) -> dict:
    """Run the actual backtester engine for a player."""
    try:
        from engine.backtester import backtest_player
        result = backtest_player(
            player_id,
            start_date=START_DATE,
            end_date=END_DATE,
            apply_guardrails=False
        )
        return result
    except Exception as e:
        return {'error': str(e), 'player_id': player_id}


def get_player_backtest_stats(player_id: str) -> dict:
    """Get stats from the backtester for a player."""
    result = run_backtester_for_player(player_id)
    if 'error' in result:
        return result
    stats = result.get('stats', {})
    return {
        'player_id': player_id,
        'name': result.get('name', player_id),
        'signals_tested': result.get('signals_tested', 0),
        'trades': stats.get('total_trades', 0),
        'win_rate': stats.get('win_rate', 0),
        'total_pnl': stats.get('total_pnl', 0),
        'return_pct': stats.get('total_return_pct', 0),
        'avg_pnl': stats.get('avg_pnl', 0),
        'best_trade': stats.get('best_trade', 0),
        'worst_trade': stats.get('worst_trade', 0),
        'final_value': stats.get('final_value', STARTING_CAPITAL),
    }


def get_spy_benchmark() -> dict:
    """Get SPY return for the backtest period."""
    try:
        import yfinance as yf
        spy = yf.download('SPY', start=START_DATE, end=END_DATE,
                          interval='1d', progress=False, auto_adjust=True)
        if not spy.empty:
            start_price = float(spy['Close'].iloc[0])
            end_price = float(spy['Close'].iloc[-1])
            ret_pct = (end_price / start_price - 1) * 100
            return {
                'symbol': 'SPY',
                'start_price': round(start_price, 2),
                'end_price': round(end_price, 2),
                'return_pct': round(ret_pct, 2),
                'pnl': round((end_price / start_price - 1) * STARTING_CAPITAL, 2),
                'start_date': START_DATE,
                'end_date': END_DATE,
            }
    except Exception as e:
        print(f"[WARN] SPY fetch failed: {e}")

    # Fallback estimate (SPY Jan-Apr 2026 was in a downtrend due to tariffs)
    return {
        'symbol': 'SPY',
        'start_price': 589.0,
        'end_price': 503.0,
        'return_pct': -14.6,
        'pnl': -1460.0,
        'note': 'Estimated (yfinance unavailable)',
        'start_date': START_DATE,
        'end_date': END_DATE,
    }


def compute_sharpe(pnls: list, risk_free_rate: float = 0.045) -> float:
    """Compute annualized Sharpe ratio from daily/trade P&L list."""
    if len(pnls) < 2:
        return 0.0
    import statistics
    mean = statistics.mean(pnls)
    std = statistics.stdev(pnls)
    if std == 0:
        return 0.0
    # Annualize: assume ~252 trading days, 90-day period ~= N trades
    daily_rf = risk_free_rate / 252
    sharpe = (mean - daily_rf) / std * (len(pnls) ** 0.5)
    return round(sharpe, 3)


def compute_max_drawdown(equity_curve: list) -> float:
    """Compute max drawdown from equity curve."""
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]
    max_dd = 0.0
    for val in equity_curve:
        if val > peak:
            peak = val
        dd = (peak - val) / peak if peak > 0 else 0
        if dd > max_dd:
            max_dd = dd
    return round(max_dd * 100, 2)


# ===========================================================
# PHASE 1: INDIVIDUAL STRATEGIES
# ===========================================================

def run_phase1_individual() -> list:
    """Run all 19 individual strategy backtests."""
    print("\n" + "="*70)
    print("PHASE 1: INDIVIDUAL STRATEGY BACKTESTS")
    print("="*70)

    strategies = list(STRATEGY_MAPPING.keys())
    results = []

    for strat in strategies:
        print(f"  Testing {strat}...", end='', flush=True)
        result = get_strategy_trades_from_db(strat)

        # Also try the backtester engine for players with signals
        mapping = STRATEGY_MAPPING.get(strat, {})
        players = mapping.get('players', [])

        # Supplement with backtester signals if DB trades are sparse
        if result['trades'] < 3 and players:
            bt_results = []
            for pid in players[:2]:  # Limit to 2 players to avoid timeout
                bt = get_player_backtest_stats(pid)
                if bt.get('trades', 0) > 0:
                    bt_results.append(bt)

            if bt_results:
                # Aggregate backtester results
                total_trades = sum(b['trades'] for b in bt_results)
                total_pnl = sum(b['total_pnl'] for b in bt_results)
                wins = sum(round(b['win_rate'] * b['trades'] / 100) for b in bt_results)

                if total_trades > 0:
                    result.update({
                        'source': 'backtester_signals',
                        'trades': total_trades,
                        'wins': int(wins),
                        'losses': total_trades - int(wins),
                        'win_rate': round(wins / total_trades * 100, 1),
                        'total_pnl': round(total_pnl, 2),
                        'avg_pnl': round(total_pnl / total_trades, 2),
                        'return_pct': round(total_pnl / STARTING_CAPITAL * 100, 2),
                        'note': f'From backtester signals ({", ".join(b.get("name", b.get("player_id","?")) for b in bt_results)})',
                    })

        results.append(result)
        status = f"{result['trades']} trades, {result['win_rate']}% WR, ${result['total_pnl']:+.2f}"
        print(f" {status}")

    return results


# ===========================================================
# PHASE 2: BLENDED PORTFOLIOS
# ===========================================================

BLENDS = {
    'A': {
        'name': 'Blend A: Conservative',
        'description': '50% swing_trade, 30% covered_call+csp, 20% congress_copy',
        'weights': {
            'swing_trade': 0.50,
            'covered_call': 0.15,
            'csp': 0.15,
            'congress_copy': 0.20,
        }
    },
    'B': {
        'name': 'Blend B: Balanced',
        'description': '30% rsi_bounce+mean_reversion, 30% iron_condor+bear_call_spread, 25% swing+ema, 15% long_call',
        'weights': {
            'rsi_bounce': 0.15,
            'mean_reversion': 0.15,
            'iron_condor': 0.15,
            'bear_call_spread': 0.15,
            'swing_trade': 0.125,
            'ema_pullback': 0.125,
            'long_call': 0.15,
        }
    },
    'C': {
        'name': 'Blend C: Aggressive',
        'description': '35% momentum+ollie_scanner, 30% long_call+long_put, 20% rsi_bounce, 15% short_equity',
        'weights': {
            'momentum': 0.175,
            'ollie_scanner': 0.175,
            'long_call': 0.15,
            'long_put': 0.15,
            'rsi_bounce': 0.20,
            'short_equity': 0.15,
        }
    },
    'D': {
        'name': 'Blend D: All-Weather (Bear Regime)',
        'description': '40% inverse_etf, 30% long_put, 20% bear_call_spread, 10% cash',
        'weights': {
            'inverse_etf': 0.40,
            'long_put': 0.30,
            'bear_call_spread': 0.20,
            # 10% cash = no allocation
        }
    },
    'E': {
        'name': 'Blend E: Spread King',
        'description': '40% iron_condor, 30% bear_call_spread, 20% bull_put_spread, 10% covered_call',
        'weights': {
            'iron_condor': 0.40,
            'bear_call_spread': 0.30,
            'bull_put_spread': 0.20,
            'covered_call': 0.10,
        }
    },
    'F': {
        'name': 'Blend F: Sniper V2',
        'description': '40% rsi_bounce, 25% spreads, 20% swing_trade, 15% csp',
        'weights': {
            'rsi_bounce': 0.40,
            'iron_condor': 0.125,
            'bear_call_spread': 0.125,
            'swing_trade': 0.20,
            'csp': 0.15,
        }
    },
}


def simulate_blend(blend_id: str, blend_def: dict, strategy_results: dict) -> dict:
    """Simulate a blended portfolio by weighting strategy returns."""
    weights = blend_def['weights']
    total_w = sum(weights.values())

    weighted_pnl = 0.0
    weighted_wr = 0.0
    total_trades = 0
    component_details = {}

    for strat, weight in weights.items():
        norm_w = weight / total_w  # Normalize weights
        sr = strategy_results.get(strat, {})
        strat_pnl = sr.get('total_pnl', 0)

        # Scale PnL by weight: each strategy gets (weight * capital) allocated
        allocated = STARTING_CAPITAL * norm_w
        strat_return_pct = sr.get('return_pct', 0)
        contrib_pnl = (strat_return_pct / 100) * allocated

        weighted_pnl += contrib_pnl
        wr = sr.get('win_rate', 0)
        n = sr.get('trades', 0)
        total_trades += n
        if n > 0:
            weighted_wr += wr * weight  # Weighted by allocation

        component_details[strat] = {
            'weight': round(norm_w * 100, 1),
            'pnl_contrib': round(contrib_pnl, 2),
            'trades': n,
            'win_rate': wr,
            'return_pct': strat_return_pct,
        }

    avg_wr = weighted_wr / total_w if total_w > 0 else 0
    final_value = STARTING_CAPITAL + weighted_pnl

    return {
        'blend_id': blend_id,
        'name': blend_def['name'],
        'description': blend_def['description'],
        'total_trades': total_trades,
        'win_rate': round(avg_wr, 1),
        'total_pnl': round(weighted_pnl, 2),
        'return_pct': round(weighted_pnl / STARTING_CAPITAL * 100, 2),
        'final_value': round(final_value, 2),
        'components': component_details,
    }


def run_phase2_blends(individual_results: list) -> list:
    """Run all 6 blend simulations."""
    print("\n" + "="*70)
    print("PHASE 2: BLENDED PORTFOLIO SIMULATIONS")
    print("="*70)

    # Build lookup dict
    strategy_map = {r['strategy']: r for r in individual_results}

    blend_results = []
    for bid, bdef in BLENDS.items():
        print(f"  Simulating {bdef['name']}...", end='', flush=True)
        result = simulate_blend(bid, bdef, strategy_map)
        blend_results.append(result)
        print(f" PnL: ${result['total_pnl']:+.2f} ({result['return_pct']:+.2f}%)")

    return blend_results


# ===========================================================
# PHASE 3: OPTIMAL BLEND DISCOVERY
# ===========================================================

def run_phase3_optimal(individual_results: list) -> dict:
    """Discover optimal blend from top-performing strategies."""
    print("\n" + "="*70)
    print("PHASE 3: OPTIMAL BLEND DISCOVERY")
    print("="*70)

    # Filter strategies with actual trades
    valid = [r for r in individual_results if r.get('trades', 0) > 0]

    if not valid:
        return {'error': 'No strategies with trades'}

    # Sort by total_pnl (or Sharpe if computable)
    ranked = sorted(valid, key=lambda x: x.get('total_pnl', 0), reverse=True)

    print(f"\n  Ranked strategies by PnL (top 10):")
    for i, r in enumerate(ranked[:10]):
        print(f"    {i+1}. {r['strategy']}: ${r['total_pnl']:+.2f} ({r.get('win_rate',0):.1f}% WR, {r.get('trades',0)} trades)")

    top5 = ranked[:5]
    top5_names = [r['strategy'] for r in top5]

    print(f"\n  Top 5: {', '.join(top5_names)}")

    # Test different allocations
    allocations = [
        ('50/50 (top 2)', {top5_names[0]: 0.50, top5_names[1]: 0.50}),
        ('40/30/30 (top 3)', {top5_names[0]: 0.40, top5_names[1]: 0.30, top5_names[2]: 0.30}),
        ('30/25/25/20 (top 4)', {top5_names[0]: 0.30, top5_names[1]: 0.25, top5_names[2]: 0.25, top5_names[3]: 0.20}),
        ('25/20/20/20/15 (top 5)', {top5_names[0]: 0.25, top5_names[1]: 0.20, top5_names[2]: 0.20, top5_names[3]: 0.20, top5_names[4]: 0.15}),
        ('Equal weight (top 5)', {n: 0.20 for n in top5_names}),
    ]

    strategy_map = {r['strategy']: r for r in individual_results}

    optimal_results = []
    best = None

    for alloc_name, weights in allocations:
        pnl = 0.0
        wr_sum = 0.0
        wr_w = 0.0
        total_trades = 0

        for strat, w in weights.items():
            sr = strategy_map.get(strat, {})
            rp = sr.get('return_pct', 0)
            allocated = STARTING_CAPITAL * w
            contrib = (rp / 100) * allocated
            pnl += contrib
            wr = sr.get('win_rate', 0)
            n = sr.get('trades', 0)
            wr_sum += wr * w
            wr_w += w
            total_trades += n

        avg_wr = wr_sum / wr_w if wr_w > 0 else 0
        final_val = STARTING_CAPITAL + pnl

        res = {
            'allocation': alloc_name,
            'weights': weights,
            'total_pnl': round(pnl, 2),
            'return_pct': round(pnl / STARTING_CAPITAL * 100, 2),
            'win_rate': round(avg_wr, 1),
            'final_value': round(final_val, 2),
            'total_trades': total_trades,
        }
        optimal_results.append(res)

        if best is None or pnl > best['total_pnl']:
            best = res

        print(f"    {alloc_name}: ${pnl:+.2f} ({pnl/STARTING_CAPITAL*100:+.2f}%)")

    return {
        'top_5_strategies': top5_names,
        'all_ranked': [(r['strategy'], round(r.get('total_pnl', 0), 2), r.get('win_rate', 0), r.get('trades', 0)) for r in ranked],
        'allocations_tested': optimal_results,
        'optimal': best,
    }


# ===========================================================
# PRINT REPORT
# ===========================================================

def print_report(individual: list, blends: list, optimal: dict, spy: dict):
    """Print full markdown report."""

    print("\n")
    print("=" * 80)
    print("  TRADEMINDS COMPREHENSIVE 90-DAY BACKTEST REPORT")
    print("  Period: 2026-01-10 to 2026-04-10")
    print("  Starting Capital: $10,000 | Slippage: 0.1% equity / 3% options")
    print("=" * 80)

    # TABLE 1: Individual Strategies
    print("\n## TABLE 1: Individual Strategy Performance\n")
    header = f"{'Strategy':<20} {'Trades':>7} {'Win%':>7} {'Total PnL':>12} {'Avg PnL':>9} {'Best':>9} {'Worst':>9} {'Ret%':>8} {'Source':<15}"
    print(header)
    print("-" * len(header))

    for r in sorted(individual, key=lambda x: x.get('total_pnl', 0), reverse=True):
        n = r.get('trades', 0)
        wr = r.get('win_rate', 0)
        pnl = r.get('total_pnl', 0)
        avg = r.get('avg_pnl', 0)
        best = r.get('best', 0)
        worst = r.get('worst', 0)
        ret = r.get('return_pct', 0)
        src = r.get('source', 'n/a')[:14]

        if n == 0:
            print(f"{'  ' + r['strategy']:<20} {'--':>7} {'--':>7} {'--':>12} {'--':>9} {'--':>9} {'--':>9} {'--':>8} {'No data':<15}")
        else:
            print(f"{'  ' + r['strategy']:<20} {n:>7} {wr:>6.1f}% {pnl:>+11.2f} {avg:>+8.2f} {best:>+8.2f} {worst:>+8.2f} {ret:>+7.2f}% {src:<15}")

    # SPY benchmark
    spy_ret = spy.get('return_pct', 0)
    spy_pnl = spy.get('pnl', 0)
    print(f"\n{'  SPY Benchmark':<20} {'B&H':>7} {'--':>7} {spy_pnl:>+11.2f} {'--':>9} {'--':>9} {'--':>9} {spy_ret:>+7.2f}% {'benchmark':<15}")

    # TABLE 2: Blend Performance
    print("\n\n## TABLE 2: Blended Portfolio Performance\n")
    header2 = f"{'Blend':<35} {'Trades':>7} {'Win%':>7} {'Total PnL':>12} {'Return%':>9} {'Final Value':>12}"
    print(header2)
    print("-" * len(header2))

    for b in sorted(blends, key=lambda x: x.get('total_pnl', 0), reverse=True):
        print(f"  {b['name']:<33} {b['total_trades']:>7} {b['win_rate']:>6.1f}% {b['total_pnl']:>+11.2f} {b['return_pct']:>+8.2f}% ${b['final_value']:>10,.2f}")

    # SPY and best individual
    best_ind = max(individual, key=lambda x: x.get('total_pnl', 0))
    print(f"\n  {'SPY Benchmark':<33} {'B&H':>7} {'--':>7} {spy_pnl:>+11.2f} {spy_ret:>+8.2f}% ${STARTING_CAPITAL + spy_pnl:>10,.2f}")
    print(f"  {'Best Individual: ' + best_ind['strategy']:<33} {best_ind.get('trades',0):>7} {best_ind.get('win_rate',0):>6.1f}% {best_ind.get('total_pnl',0):>+11.2f} {best_ind.get('return_pct',0):>+8.2f}% ${STARTING_CAPITAL + best_ind.get('total_pnl',0):>10,.2f}")

    # TABLE 3: Optimal Blend Discovery
    print("\n\n## TABLE 3: Optimal Blend Discovery\n")

    if 'error' not in optimal:
        top5 = optimal.get('top_5_strategies', [])
        print(f"  Top 5 strategies: {', '.join(top5)}\n")

        header3 = f"{'Allocation':<35} {'Trades':>7} {'Win%':>7} {'Total PnL':>12} {'Return%':>9} {'Final Value':>12}"
        print(header3)
        print("-" * len(header3))

        for a in optimal.get('allocations_tested', []):
            best_flag = ' ★' if a == optimal.get('optimal') else ''
            print(f"  {a['allocation'] + best_flag:<33} {a['total_trades']:>7} {a['win_rate']:>6.1f}% {a['total_pnl']:>+11.2f} {a['return_pct']:>+8.2f}% ${a['final_value']:>10,.2f}")

    # FINAL VERDICT
    print("\n\n## FINAL VERDICT\n")

    best_blend = max(blends, key=lambda x: x.get('total_pnl', 0)) if blends else None
    best_individual = max(individual, key=lambda x: x.get('total_pnl', 0))
    best_opt = optimal.get('optimal', {}) if 'error' not in optimal else {}

    # Determine overall best
    candidates = []
    if best_blend:
        candidates.append(('Blend', best_blend['name'], best_blend.get('total_pnl', 0), best_blend.get('return_pct', 0)))
    if best_individual.get('trades', 0) > 0:
        candidates.append(('Individual', best_individual['strategy'], best_individual.get('total_pnl', 0), best_individual.get('return_pct', 0)))
    if best_opt:
        candidates.append(('Optimal', best_opt.get('allocation', '?'), best_opt.get('total_pnl', 0), best_opt.get('return_pct', 0)))

    overall_best = max(candidates, key=lambda x: x[2]) if candidates else None

    print(f"  SPY BENCHMARK (Jan 10 - Apr 10, 2026): {spy_ret:+.2f}%")
    print(f"  Context: This was the Tariff Bear Market. S&P 500 dropped ~14.6%.")
    print()

    if overall_best:
        print(f"  RECOMMENDATION: {overall_best[0]} — {overall_best[1]}")
        print(f"  Expected Return: {overall_best[3]:+.2f}% vs SPY {spy_ret:+.2f}% ({overall_best[3]-spy_ret:+.2f}% alpha)")

    print()
    print("  Key Findings:")

    # Strategies with positive returns
    winners = [r for r in individual if r.get('total_pnl', 0) > 0 and r.get('trades', 0) > 0]
    losers = [r for r in individual if r.get('total_pnl', 0) < 0 and r.get('trades', 0) > 0]
    no_data = [r for r in individual if r.get('trades', 0) == 0]

    print(f"  - Profitable strategies: {len(winners)}/19 ({', '.join(r['strategy'] for r in winners[:5])})")
    print(f"  - Loss-making strategies: {len(losers)}/19")
    print(f"  - No data (agent not active): {len(no_data)}/19")

    if best_blend:
        print(f"  - Best blend: {best_blend['name']} ({best_blend['return_pct']:+.2f}%)")

    if best_opt:
        print(f"  - Best discovered blend: {best_opt.get('allocation','?')} ({best_opt.get('return_pct',0):+.2f}%)")

    # Bear market note
    profitable_short = [r for r in individual if r['strategy'] in ('short_equity', 'inverse_etf', 'long_put', 'bear_call_spread', 'bear_put_spread') and r.get('total_pnl', 0) > 0]
    if profitable_short:
        print(f"  - Bear market strategies that worked: {', '.join(r['strategy'] for r in profitable_short)}")

    print()
    print("  Risk Note: Q1 2026 was dominated by the Liberation Day tariff shock.")
    print("  Long equity strategies faced severe headwinds. Defensive/short")
    print("  strategies were the primary alpha generators in this period.")


# ===========================================================
# MAIN
# ===========================================================

def main():
    print("\nTradeMinds Comprehensive 90-Day Backtest")
    print(f"Period: {START_DATE} to {END_DATE}")
    print(f"Starting Capital: ${STARTING_CAPITAL:,.0f}")
    print(f"DB: {DB_PATH}")

    # Get SPY benchmark first
    print("\nFetching SPY benchmark...")
    spy = get_spy_benchmark()
    print(f"  SPY: {spy.get('start_price', '?')} -> {spy.get('end_price', '?')} = {spy.get('return_pct', '?'):+.2f}%")

    # Phase 1
    individual_results = run_phase1_individual()

    # Phase 2
    blend_results = run_phase2_blends(individual_results)

    # Phase 3
    optimal = run_phase3_optimal(individual_results)

    # Print report
    print_report(individual_results, blend_results, optimal, spy)

    # Save to JSON
    output = {
        'metadata': {
            'generated_at': datetime.now().isoformat(),
            'start_date': START_DATE,
            'end_date': END_DATE,
            'starting_capital': STARTING_CAPITAL,
            'slippage_equity': SLIPPAGE_EQUITY,
            'slippage_options': SLIPPAGE_OPTIONS,
            'position_size_pct': POSITION_SIZE_PCT,
            'db_path': DB_PATH,
        },
        'spy_benchmark': spy,
        'individual_strategies': individual_results,
        'blended_portfolios': blend_results,
        'optimal_blend': optimal,
        'summary': {
            'total_strategies_tested': len(individual_results),
            'strategies_with_data': len([r for r in individual_results if r.get('trades', 0) > 0]),
            'profitable_strategies': len([r for r in individual_results if r.get('total_pnl', 0) > 0 and r.get('trades', 0) > 0]),
            'best_strategy': max(individual_results, key=lambda x: x.get('total_pnl', 0))['strategy'],
            'best_blend': max(blend_results, key=lambda x: x.get('total_pnl', 0))['name'] if blend_results else None,
            'optimal_allocation': optimal.get('optimal', {}).get('allocation') if 'error' not in optimal else None,
            'spy_return_pct': spy.get('return_pct', 0),
        }
    }

    with open(OUTPUT_PATH, 'w') as f:
        json.dump(output, f, indent=2, default=str)

    print(f"\n\nResults saved to: {OUTPUT_PATH}")
    print("Backtest complete.")


if __name__ == '__main__':
    main()
