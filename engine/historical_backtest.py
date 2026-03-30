"""
Historical Backtesting Engine for TradeMinds AI Arena.

Replays a trading day using intraday 5-minute candles, feeding each AI model
only data available at each point in time (no future data leakage).
Tracks simulated trades, P&L, and computes performance metrics.
"""

from __future__ import annotations

import json
import os
import time
import math
import sqlite3
import threading
from datetime import datetime, timedelta
from dataclasses import dataclass, field, asdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional

import pandas as pd
from rich.console import Console

from config import WATCH_STOCKS, STARTING_CASH, POSITION_SIZE_PCT, MAX_POSITIONS

console = Console()

DB_PATH = "data/trader.db"
CACHE_DIR = "data/backtest_cache"

# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class BacktestTrade:
    timestamp: str
    symbol: str
    action: str          # BUY / SELL
    price: float
    qty: float
    confidence: float
    reasoning: str
    pnl: float = 0.0     # filled on SELL

@dataclass
class BacktestPosition:
    symbol: str
    qty: float
    avg_price: float
    opened_at: str

@dataclass
class ModelResult:
    player_id: str
    display_name: str
    trades: list = field(default_factory=list)
    equity_curve: list = field(default_factory=list)  # [{time, value}]
    final_value: float = 0.0
    total_return_pct: float = 0.0
    win_rate: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    num_trades: int = 0
    best_trade_pct: float = 0.0
    worst_trade_pct: float = 0.0

    def to_dict(self):
        return {
            "player_id": self.player_id,
            "display_name": self.display_name,
            "trades": [asdict(t) if isinstance(t, BacktestTrade) else t for t in self.trades],
            "equity_curve": self.equity_curve,
            "final_value": self.final_value,
            "total_return_pct": self.total_return_pct,
            "win_rate": self.win_rate,
            "sharpe_ratio": self.sharpe_ratio,
            "max_drawdown": self.max_drawdown,
            "num_trades": self.num_trades,
            "best_trade_pct": self.best_trade_pct,
            "worst_trade_pct": self.worst_trade_pct,
        }


# ---------------------------------------------------------------------------
# Yahoo Finance historical intraday data
# ---------------------------------------------------------------------------

def _yahoo_chart_historical(symbol: str, date_str: str) -> list:
    """Fetch 5-minute candles for a specific past date from Yahoo Finance."""
    import requests

    dt = datetime.strptime(date_str, "%Y-%m-%d")
    period1 = int(dt.timestamp())
    period2 = int((dt + timedelta(days=1)).timestamp())

    url = f"https://query2.finance.yahoo.com/v8/finance/chart/{symbol}"
    params = {
        "period1": period1,
        "period2": period2,
        "interval": "5m",
        "includePrePost": "false",
    }
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    try:
        resp = requests.get(url, params=params, headers=headers, timeout=15)
        if resp.status_code == 429:
            time.sleep(2)
            resp = requests.get(url, params=params, headers=headers, timeout=15)
        data = resp.json()
        result = data.get("chart", {}).get("result", [])
        if not result:
            return []

        chart = result[0]
        timestamps = chart.get("timestamp", [])
        quotes = chart.get("indicators", {}).get("quote", [{}])[0]
        opens = quotes.get("open", [])
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])
        volumes = quotes.get("volume", [])

        candles = []
        for i, ts in enumerate(timestamps):
            if i >= len(closes) or closes[i] is None:
                continue
            candles.append({
                "time": datetime.fromtimestamp(ts).isoformat(),
                "timestamp": ts,
                "open": round(float(opens[i] or closes[i]), 2),
                "high": round(float(highs[i] or closes[i]), 2),
                "low": round(float(lows[i] or closes[i]), 2),
                "close": round(float(closes[i]), 2),
                "volume": int(volumes[i] or 0),
            })
        return candles
    except Exception as e:
        console.log(f"[red]Backtest fetch error {symbol} {date_str}: {e}")
        return []


def download_day_data(date_str: str, symbols: list = None) -> dict:
    """Download and cache all intraday data for a trading day.
    Returns {symbol: [candles]}."""
    symbols = symbols or WATCH_STOCKS
    os.makedirs(CACHE_DIR, exist_ok=True)
    cache_file = os.path.join(CACHE_DIR, f"{date_str}.json")

    # Check cache
    if os.path.exists(cache_file):
        with open(cache_file, "r") as f:
            cached = json.load(f)
        # Only use cache if it has all requested symbols
        if all(s in cached for s in symbols):
            console.log(f"[green]Backtest cache hit for {date_str}")
            return cached

    console.log(f"[cyan]Downloading intraday data for {date_str}...")
    all_data = {}

    def _fetch(sym):
        candles = _yahoo_chart_historical(sym, date_str)
        time.sleep(0.4)  # Rate limit
        return sym, candles

    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = {pool.submit(_fetch, s): s for s in symbols}
        for future in as_completed(futures):
            sym, candles = future.result()
            all_data[sym] = candles
            if candles:
                console.log(f"  [dim]{sym}: {len(candles)} candles")
            else:
                console.log(f"  [yellow]{sym}: no data")

    # Save cache
    with open(cache_file, "w") as f:
        json.dump(all_data, f)

    console.log(f"[green]Cached {len(all_data)} symbols for {date_str}")
    return all_data


# ---------------------------------------------------------------------------
# Technical indicator computation from candle history
# ---------------------------------------------------------------------------

def _compute_indicators_at(candles_so_far: list) -> dict:
    """Compute RSI, MACD, SMA from candle history up to current point."""
    if len(candles_so_far) < 14:
        return {}

    closes = pd.Series([c["close"] for c in candles_so_far])
    volumes = pd.Series([c["volume"] for c in candles_so_far])

    # RSI (14-period)
    delta = closes.diff()
    gain = delta.where(delta > 0, 0.0).rolling(14).mean()
    loss = (-delta.where(delta < 0, 0.0)).rolling(14).mean()
    rs = gain.iloc[-1] / loss.iloc[-1] if loss.iloc[-1] > 0 else 100
    rsi = round(100 - (100 / (1 + rs)), 2)

    # MACD (12, 26, 9)
    ema12 = closes.ewm(span=12, adjust=False).mean()
    ema26 = closes.ewm(span=26, adjust=False).mean()
    macd_line = ema12 - ema26
    signal_line = macd_line.ewm(span=9, adjust=False).mean()
    histogram = macd_line - signal_line

    # SMA
    sma_20 = round(float(closes.rolling(20).mean().iloc[-1]), 2) if len(closes) >= 20 else None
    sma_50 = round(float(closes.rolling(50).mean().iloc[-1]), 2) if len(closes) >= 50 else None

    # Volume ratio
    avg_vol = volumes.rolling(20).mean().iloc[-1] if len(volumes) >= 20 else volumes.mean()
    vol_ratio = round(float(volumes.iloc[-1] / avg_vol), 2) if avg_vol > 0 else 1.0

    return {
        "rsi": rsi,
        "macd": round(float(macd_line.iloc[-1]), 4),
        "macd_signal": round(float(signal_line.iloc[-1]), 4),
        "macd_histogram": round(float(histogram.iloc[-1]), 4),
        "sma_20": sma_20,
        "sma_50": sma_50,
        "volume_ratio": vol_ratio,
    }


# ---------------------------------------------------------------------------
# Simplified backtest portfolio (in-memory, no DB writes)
# ---------------------------------------------------------------------------

class BacktestPortfolio:
    """Simulated portfolio for one AI model during backtest."""

    def __init__(self, starting_cash: float = STARTING_CASH):
        self.starting_cash = starting_cash
        self.cash = starting_cash
        self.positions: dict[str, BacktestPosition] = {}
        self.trades: list[BacktestTrade] = []
        self.equity_curve: list[dict] = []

    def total_value(self, prices: dict) -> float:
        positions_val = sum(
            pos.qty * prices.get(sym, pos.avg_price)
            for sym, pos in self.positions.items()
        )
        return self.cash + positions_val

    def snapshot(self, timestamp: str, prices: dict):
        self.equity_curve.append({
            "time": timestamp,
            "value": round(self.total_value(prices), 2),
        })

    def buy(self, symbol: str, price: float, timestamp: str,
            confidence: float, reasoning: str) -> Optional[BacktestTrade]:
        if len(self.positions) >= MAX_POSITIONS:
            return None
        if symbol in self.positions:
            return None  # No double-buy

        alloc = self.cash * POSITION_SIZE_PCT
        qty = round(alloc / price, 4)
        cost = qty * price
        if cost > self.cash or qty <= 0:
            return None

        self.cash -= cost
        self.positions[symbol] = BacktestPosition(
            symbol=symbol, qty=qty, avg_price=price, opened_at=timestamp
        )
        trade = BacktestTrade(
            timestamp=timestamp, symbol=symbol, action="BUY",
            price=price, qty=qty, confidence=confidence, reasoning=reasoning,
        )
        self.trades.append(trade)
        return trade

    def sell(self, symbol: str, price: float, timestamp: str,
             confidence: float, reasoning: str) -> Optional[BacktestTrade]:
        pos = self.positions.get(symbol)
        if not pos:
            return None

        proceeds = pos.qty * price
        pnl = proceeds - (pos.qty * pos.avg_price)
        self.cash += proceeds
        del self.positions[symbol]

        trade = BacktestTrade(
            timestamp=timestamp, symbol=symbol, action="SELL",
            price=price, qty=pos.qty, confidence=confidence,
            reasoning=reasoning, pnl=round(pnl, 2),
        )
        self.trades.append(trade)
        return trade

    def close_all(self, prices: dict, timestamp: str):
        """Force-close all positions at end of day."""
        for sym in list(self.positions.keys()):
            price = prices.get(sym, self.positions[sym].avg_price)
            self.sell(sym, price, timestamp, 0.0, "End-of-day close")


# ---------------------------------------------------------------------------
# Backtest simulation
# ---------------------------------------------------------------------------

def _build_backtest_prompt(symbol: str, price: float, change_pct: float,
                           indicators: dict, portfolio: BacktestPortfolio,
                           candles_so_far: list) -> str:
    """Build a simplified prompt for backtest (no live news/fundamentals)."""
    already_holds = symbol in portfolio.positions

    # Recent price action summary
    recent = candles_so_far[-5:] if len(candles_so_far) >= 5 else candles_so_far
    price_action = " | ".join(
        f"{c['time'][-8:]}: O={c['open']} H={c['high']} L={c['low']} C={c['close']}"
        for c in recent
    )

    # Portfolio context
    cash = round(portfolio.cash, 2)
    total_val = round(portfolio.total_value({symbol: price}), 2)
    positions_str = ", ".join(
        f"{s} ({p.qty} @ ${p.avg_price})" for s, p in portfolio.positions.items()
    ) or "None"

    # Indicators
    ind_str = ""
    if indicators:
        rsi = indicators.get("rsi", "N/A")
        macd = indicators.get("macd", "N/A")
        macd_sig = indicators.get("macd_signal", "N/A")
        sma_20 = indicators.get("sma_20", "N/A")
        vol_r = indicators.get("volume_ratio", "N/A")
        ind_str = f"""
Technical Indicators:
- RSI(14): {rsi}
- MACD: {macd} | Signal: {macd_sig} | Histogram: {indicators.get('macd_histogram', 'N/A')}
- SMA(20): {sma_20}
- Volume Ratio: {vol_r}"""

    hold_warning = ""
    if already_holds:
        hold_warning = f"\n⚠ You ALREADY HOLD {symbol}. Choose SELL or HOLD only."

    return f"""You are a stock trading AI. Analyze {symbol} and decide: BUY, SELL, or HOLD.
This is a single-day backtest simulation. You start with ${STARTING_CASH} cash.

Current: {symbol} @ ${price:.2f} ({change_pct:+.2f}% from open)
{hold_warning}

Recent 5-Minute Candles:
{price_action}
{ind_str}

Portfolio: Cash=${cash} | Total=${total_val}
Positions: {positions_str}

Rules:
- Position size is 10% of cash per trade
- Maximum {MAX_POSITIONS} positions
- Only BUY if strong conviction (confidence >= 0.65)
- SELL to take profits or cut losses

Respond EXACTLY in this format:
Decision: BUY or SELL or HOLD
Confidence: 0.XX
Reasoning: Brief 1-2 sentence explanation"""


# Decision interval: analyze every N candles (not every single one)
ANALYSIS_INTERVAL = 6  # Every 30 minutes (6 x 5min candles)


def run_single_day_backtest(
    date_str: str,
    model_ids: list[str],
    progress_callback=None,
) -> dict[str, ModelResult]:
    """Run backtest for one day across selected models.

    Args:
        date_str: Date like "2026-03-10"
        model_ids: List of player_ids to test
        progress_callback: Optional fn(pct, msg) for progress updates

    Returns: {player_id: ModelResult}
    """
    from engine.providers.base import AIProvider

    # Step 1: Download / load cached data
    if progress_callback:
        progress_callback(5, "Downloading market data...")
    day_data = download_day_data(date_str)

    if not any(day_data.get(s) for s in WATCH_STOCKS):
        raise ValueError(f"No market data available for {date_str}. Was it a trading day?")

    # Step 2: Initialize providers
    if progress_callback:
        progress_callback(10, "Initializing AI models...")
    providers = _load_providers(model_ids)

    if not providers:
        raise ValueError("No valid providers found for selected models")

    # Step 3: Build unified timeline of candle timestamps
    all_times = set()
    for sym, candles in day_data.items():
        for c in candles:
            all_times.add(c["time"])
    timeline = sorted(all_times)

    if not timeline:
        raise ValueError(f"No candle data for {date_str}")

    # Step 4: Simulate
    results = {}
    total_models = len(providers)

    for idx, (player_id, provider) in enumerate(providers.items()):
        if progress_callback:
            pct = 15 + int(80 * idx / total_models)
            progress_callback(pct, f"Running {provider.display_name}...")

        result = _simulate_model(
            player_id=player_id,
            provider=provider,
            day_data=day_data,
            timeline=timeline,
            date_str=date_str,
        )
        results[player_id] = result
        console.log(f"[green]{provider.display_name}: {result.total_return_pct:+.2f}% | {result.num_trades} trades")

    if progress_callback:
        progress_callback(100, "Complete")

    return results


def _simulate_model(
    player_id: str,
    provider,
    day_data: dict,
    timeline: list[str],
    date_str: str,
) -> ModelResult:
    """Run one model through the trading day."""
    portfolio = BacktestPortfolio()

    # Track candles seen per symbol
    candles_seen: dict[str, list] = {s: [] for s in WATCH_STOCKS}

    for step, timestamp in enumerate(timeline):
        # Update candles seen for each symbol
        current_prices = {}
        for sym in WATCH_STOCKS:
            for c in day_data.get(sym, []):
                if c["time"] == timestamp and c not in candles_seen[sym]:
                    candles_seen[sym].append(c)
            # Current price = latest close we've seen
            if candles_seen[sym]:
                current_prices[sym] = candles_seen[sym][-1]["close"]

        # Only analyze at intervals (not every 5 min)
        if step % ANALYSIS_INTERVAL != 0 or step == 0:
            portfolio.snapshot(timestamp, current_prices)
            continue

        # Analyze each symbol
        for sym in WATCH_STOCKS:
            seen = candles_seen[sym]
            if len(seen) < 3:
                continue

            price = seen[-1]["close"]
            open_price = seen[0]["open"]
            change_pct = round((price - open_price) / open_price * 100, 2) if open_price > 0 else 0

            # Compute indicators from what we've seen
            indicators = _compute_indicators_at(seen)

            # Build prompt and get decision
            prompt = _build_backtest_prompt(sym, price, change_pct, indicators, portfolio, seen)

            try:
                response = provider.call_model(prompt)
                decision = _parse_backtest_decision(response, sym)
            except Exception as e:
                console.log(f"[dim]  {player_id} error on {sym}: {e}")
                continue

            if not decision:
                continue

            action, confidence, reasoning = decision

            if confidence < 0.65 and action != "SELL":
                continue

            if action == "BUY":
                portfolio.buy(sym, price, timestamp, confidence, reasoning)
            elif action == "SELL":
                portfolio.sell(sym, price, timestamp, confidence, reasoning)

        portfolio.snapshot(timestamp, current_prices)

    # End of day: close all remaining positions
    final_prices = {}
    for sym in WATCH_STOCKS:
        if candles_seen[sym]:
            final_prices[sym] = candles_seen[sym][-1]["close"]
    portfolio.close_all(final_prices, timeline[-1] if timeline else date_str)
    portfolio.snapshot(timeline[-1] if timeline else date_str, final_prices)

    # Compute metrics
    return _compute_metrics(player_id, provider.display_name, portfolio)


def _parse_backtest_decision(text: str, symbol: str):
    """Parse model response. Returns (action, confidence, reasoning) or None."""
    import re
    if not text:
        return None

    text_upper = text.upper()

    # Extract action
    action = "HOLD"
    action_match = re.search(r"DECISION:\s*(BUY|SELL|HOLD)", text_upper)
    if action_match:
        action = action_match.group(1)
    elif "BUY" in text_upper[:100] and "SELL" not in text_upper[:100]:
        action = "BUY"
    elif "SELL" in text_upper[:100]:
        action = "SELL"

    if action == "HOLD":
        return None

    # Extract confidence
    confidence = 0.5
    conf_match = re.search(r"CONFIDENCE:\s*(0\.\d+|1\.0)", text, re.IGNORECASE)
    if conf_match:
        confidence = float(conf_match.group(1))

    # Extract reasoning
    reasoning = ""
    reason_match = re.search(r"REASONING:\s*(.+)", text, re.IGNORECASE | re.DOTALL)
    if reason_match:
        reasoning = reason_match.group(1).strip()[:200]

    return action, confidence, reasoning


def _compute_metrics(player_id: str, display_name: str, portfolio: BacktestPortfolio) -> ModelResult:
    """Calculate performance metrics from completed backtest."""
    trades = portfolio.trades
    equity = portfolio.equity_curve

    final_value = equity[-1]["value"] if equity else portfolio.starting_cash
    total_return = round((final_value - portfolio.starting_cash) / portfolio.starting_cash * 100, 2)

    # Win rate from SELL trades
    sells = [t for t in trades if t.action == "SELL"]
    wins = sum(1 for t in sells if t.pnl > 0)
    win_rate = round(wins / len(sells) * 100, 1) if sells else 0

    # Best/worst trade
    trade_pcts = []
    for t in sells:
        # Find matching buy
        buy_price = None
        for bt in trades:
            if bt.action == "BUY" and bt.symbol == t.symbol:
                buy_price = bt.price
        if buy_price and buy_price > 0:
            pct = round((t.price - buy_price) / buy_price * 100, 2)
            trade_pcts.append(pct)

    best_trade = max(trade_pcts) if trade_pcts else 0
    worst_trade = min(trade_pcts) if trade_pcts else 0

    # Sharpe ratio (annualized from intraday returns)
    if len(equity) >= 2:
        values = [e["value"] for e in equity]
        returns = [(values[i] - values[i - 1]) / values[i - 1]
                   for i in range(1, len(values)) if values[i - 1] > 0]
        if returns:
            mean_ret = sum(returns) / len(returns)
            std_ret = (sum((r - mean_ret) ** 2 for r in returns) / len(returns)) ** 0.5
            # Annualize: ~78 5-min periods per day, ~252 trading days
            sharpe = round(mean_ret / std_ret * math.sqrt(252 * 78), 2) if std_ret > 0 else 0
        else:
            sharpe = 0
    else:
        sharpe = 0

    # Max drawdown
    if equity:
        values = [e["value"] for e in equity]
        peak = values[0]
        max_dd = 0
        for v in values:
            if v > peak:
                peak = v
            dd = (peak - v) / peak if peak > 0 else 0
            if dd > max_dd:
                max_dd = dd
        max_dd = round(max_dd * 100, 2)
    else:
        max_dd = 0

    return ModelResult(
        player_id=player_id,
        display_name=display_name,
        trades=[asdict(t) for t in trades],
        equity_curve=equity,
        final_value=round(final_value, 2),
        total_return_pct=total_return,
        win_rate=win_rate,
        sharpe_ratio=sharpe,
        max_drawdown=max_dd,
        num_trades=len(trades),
        best_trade_pct=best_trade,
        worst_trade_pct=worst_trade,
    )


# ---------------------------------------------------------------------------
# Provider loader (reuses existing provider infrastructure)
# ---------------------------------------------------------------------------

def _load_providers(model_ids: list[str]) -> dict:
    """Load AI providers for selected model IDs."""
    from config import (
        OLLAMA_URL, OPENAI_API_KEY,
        GEMINI_API_KEY, GROK_API_KEY, AI_PLAYERS,
    )

    providers = {}
    player_map = {p["id"]: p for p in AI_PLAYERS}

    for mid in model_ids:
        info = player_map.get(mid)
        if not info:
            continue

        try:
            if info["provider"] == "ollama":
                from engine.providers.ollama_provider import OllamaProvider
                providers[mid] = OllamaProvider(
                    player_id=mid, model=info["model"],
                    url=OLLAMA_URL, timeout=60,
                )
            elif info["provider"] == "openai" and OPENAI_API_KEY:
                from engine.providers.openai_provider import OpenAIProvider
                providers[mid] = OpenAIProvider(
                    api_key=OPENAI_API_KEY, player_id=mid,
                    model=info["model"], display_name=info["name"],
                )
            elif info["provider"] == "google" and GEMINI_API_KEY:
                from engine.providers.gemini_provider import GeminiProvider
                providers[mid] = GeminiProvider(
                    api_key=GEMINI_API_KEY, player_id=mid,
                    model=info["model"], display_name=info["name"],
                )
            elif info["provider"] == "xai" and GROK_API_KEY:
                from engine.providers.grok_provider import GrokProvider
                providers[mid] = GrokProvider(
                    api_key=GROK_API_KEY, player_id=mid,
                    model=info["model"], display_name=info["name"],
                )
        except Exception as e:
            console.log(f"[yellow]Skipping {mid}: {e}")

    return providers


# ---------------------------------------------------------------------------
# Multi-day backtest
# ---------------------------------------------------------------------------

def run_multi_day_backtest(
    start_date: str,
    end_date: str,
    model_ids: list[str],
    progress_callback=None,
) -> dict:
    """Run backtest across a date range, aggregating results.

    Returns:
        {
            "days": [{date, results: {player_id: ModelResult}}],
            "cumulative": {player_id: {total_return, win_rate, sharpe, ...}},
        }
    """
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")

    # Generate trading day dates (weekdays only)
    dates = []
    current = start
    while current <= end:
        if current.weekday() < 5:  # Monday-Friday
            dates.append(current.strftime("%Y-%m-%d"))
        current += timedelta(days=1)

    if not dates:
        raise ValueError("No trading days in selected range")

    days_results = []
    cumulative: dict[str, dict] = {}

    for i, date_str in enumerate(dates):
        if progress_callback:
            pct = int(100 * i / len(dates))
            progress_callback(pct, f"Day {i + 1}/{len(dates)}: {date_str}")

        try:
            day_results = run_single_day_backtest(date_str, model_ids)
            days_results.append({
                "date": date_str,
                "results": {pid: r.to_dict() for pid, r in day_results.items()},
            })

            # Accumulate
            for pid, result in day_results.items():
                if pid not in cumulative:
                    cumulative[pid] = {
                        "player_id": pid,
                        "display_name": result.display_name,
                        "days_tested": 0,
                        "total_trades": 0,
                        "total_wins": 0,
                        "total_sells": 0,
                        "daily_returns": [],
                        "all_trade_pcts": [],
                        "max_drawdowns": [],
                    }
                c = cumulative[pid]
                c["days_tested"] += 1
                c["total_trades"] += result.num_trades
                c["daily_returns"].append(result.total_return_pct)
                c["max_drawdowns"].append(result.max_drawdown)

                # Count wins from trades
                sells = [t for t in result.trades if t.get("action") == "SELL"]
                wins = sum(1 for t in sells if t.get("pnl", 0) > 0)
                c["total_wins"] += wins
                c["total_sells"] += len(sells)

        except Exception as e:
            console.log(f"[yellow]Skipping {date_str}: {e}")

    # Finalize cumulative stats
    for pid, c in cumulative.items():
        returns = c["daily_returns"]
        c["avg_daily_return"] = round(sum(returns) / len(returns), 2) if returns else 0
        c["cumulative_return"] = round(sum(returns), 2)
        c["win_rate"] = round(c["total_wins"] / c["total_sells"] * 100, 1) if c["total_sells"] > 0 else 0
        c["avg_max_drawdown"] = round(sum(c["max_drawdowns"]) / len(c["max_drawdowns"]), 2) if c["max_drawdowns"] else 0

        # Sharpe from daily returns
        if len(returns) >= 2:
            mean_r = sum(returns) / len(returns)
            std_r = (sum((r - mean_r) ** 2 for r in returns) / len(returns)) ** 0.5
            c["sharpe_ratio"] = round(mean_r / std_r * math.sqrt(252), 2) if std_r > 0 else 0
        else:
            c["sharpe_ratio"] = 0

        # Clean up intermediate arrays
        del c["daily_returns"]
        del c["all_trade_pcts"]
        del c["max_drawdowns"]
        del c["total_wins"]
        del c["total_sells"]

    if progress_callback:
        progress_callback(100, "Complete")

    return {
        "days": days_results,
        "cumulative": cumulative,
    }


# ---------------------------------------------------------------------------
# Database persistence for backtest results
# ---------------------------------------------------------------------------

def _bt_conn():
    c = sqlite3.connect(DB_PATH, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def ensure_backtest_tables():
    """Create backtest tables if they don't exist."""
    conn = _bt_conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS backtest_runs (
        id INTEGER PRIMARY KEY,
        run_type TEXT NOT NULL DEFAULT 'single',
        start_date TEXT NOT NULL,
        end_date TEXT NOT NULL,
        model_ids TEXT NOT NULL,
        status TEXT DEFAULT 'running',
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        completed_at TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS backtest_results (
        id INTEGER PRIMARY KEY,
        run_id INTEGER NOT NULL REFERENCES backtest_runs(id),
        player_id TEXT NOT NULL,
        display_name TEXT,
        test_date TEXT NOT NULL,
        final_value REAL,
        total_return_pct REAL,
        win_rate REAL,
        sharpe_ratio REAL,
        max_drawdown REAL,
        num_trades INTEGER,
        best_trade_pct REAL,
        worst_trade_pct REAL,
        trades_json TEXT,
        equity_json TEXT
    )""")
    conn.commit()
    conn.close()


def save_backtest_run(
    run_type: str,
    start_date: str,
    end_date: str,
    model_ids: list[str],
    results: dict,
) -> int:
    """Save a completed backtest run to database. Returns run_id."""
    ensure_backtest_tables()
    conn = _bt_conn()

    cur = conn.execute(
        "INSERT INTO backtest_runs (run_type, start_date, end_date, model_ids, status, completed_at) "
        "VALUES (?, ?, ?, ?, 'complete', CURRENT_TIMESTAMP)",
        (run_type, start_date, end_date, json.dumps(model_ids)),
    )
    run_id = cur.lastrowid

    # For single-day results
    if isinstance(results, dict) and "days" not in results:
        for pid, result in results.items():
            r = result if isinstance(result, dict) else result.to_dict()
            conn.execute(
                "INSERT INTO backtest_results (run_id, player_id, display_name, test_date, "
                "final_value, total_return_pct, win_rate, sharpe_ratio, max_drawdown, "
                "num_trades, best_trade_pct, worst_trade_pct, trades_json, equity_json) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (run_id, pid, r.get("display_name", pid), start_date,
                 r.get("final_value", 0), r.get("total_return_pct", 0),
                 r.get("win_rate", 0), r.get("sharpe_ratio", 0),
                 r.get("max_drawdown", 0), r.get("num_trades", 0),
                 r.get("best_trade_pct", 0), r.get("worst_trade_pct", 0),
                 json.dumps(r.get("trades", [])), json.dumps(r.get("equity_curve", []))),
            )
    else:
        # Multi-day: save each day's results
        for day in results.get("days", []):
            date = day["date"]
            for pid, r in day["results"].items():
                conn.execute(
                    "INSERT INTO backtest_results (run_id, player_id, display_name, test_date, "
                    "final_value, total_return_pct, win_rate, sharpe_ratio, max_drawdown, "
                    "num_trades, best_trade_pct, worst_trade_pct, trades_json, equity_json) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (run_id, pid, r.get("display_name", pid), date,
                     r.get("final_value", 0), r.get("total_return_pct", 0),
                     r.get("win_rate", 0), r.get("sharpe_ratio", 0),
                     r.get("max_drawdown", 0), r.get("num_trades", 0),
                     r.get("best_trade_pct", 0), r.get("worst_trade_pct", 0),
                     json.dumps(r.get("trades", [])), json.dumps(r.get("equity_curve", []))),
                )

    conn.commit()
    conn.close()
    return run_id


def get_backtest_runs(limit: int = 20) -> list[dict]:
    """Get recent backtest runs."""
    ensure_backtest_tables()
    conn = _bt_conn()
    rows = conn.execute(
        "SELECT * FROM backtest_runs ORDER BY created_at DESC LIMIT ?", (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_backtest_run_results(run_id: int) -> list[dict]:
    """Get all results for a specific backtest run."""
    ensure_backtest_tables()
    conn = _bt_conn()
    rows = conn.execute(
        "SELECT * FROM backtest_results WHERE run_id=? ORDER BY player_id, test_date",
        (run_id,),
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        d = dict(r)
        d["trades"] = json.loads(d.pop("trades_json", "[]"))
        d["equity_curve"] = json.loads(d.pop("equity_json", "[]"))
        results.append(d)
    return results


def get_model_rankings() -> list[dict]:
    """Aggregate all backtest results and rank models by performance."""
    ensure_backtest_tables()
    conn = _bt_conn()
    rows = conn.execute("""
        SELECT
            player_id,
            display_name,
            COUNT(*) as days_tested,
            ROUND(AVG(total_return_pct), 2) as avg_return,
            ROUND(SUM(total_return_pct), 2) as cumulative_return,
            ROUND(AVG(win_rate), 1) as avg_win_rate,
            ROUND(AVG(sharpe_ratio), 2) as avg_sharpe,
            ROUND(AVG(max_drawdown), 2) as avg_max_drawdown,
            SUM(num_trades) as total_trades,
            ROUND(MAX(best_trade_pct), 2) as best_ever_trade,
            ROUND(MIN(worst_trade_pct), 2) as worst_ever_trade
        FROM backtest_results
        GROUP BY player_id
        ORDER BY avg_return DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]
