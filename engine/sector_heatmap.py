"""Sector Heatmap — real-time sector ETF performance treemap.

Uses Yahoo Finance (free) to get sector ETF data.
"""
from __future__ import annotations
import time
import threading
from rich.console import Console

console = Console()

_cache = {"data": None, "ts": 0}
_lock = threading.Lock()
_TTL = 300  # 5 minutes

SECTORS = {
    "XLK": {"name": "Technology", "tickers": ["AAPL", "MSFT", "NVDA", "AVGO", "ORCL"], "weight": 30},
    "XLV": {"name": "Healthcare", "tickers": ["UNH", "JNJ", "LLY", "ABBV", "MRK"], "weight": 12},
    "XLF": {"name": "Financials", "tickers": ["BRK-B", "JPM", "V", "MA", "BAC"], "weight": 13},
    "XLE": {"name": "Energy", "tickers": ["XOM", "CVX", "COP", "SLB", "EOG"], "weight": 4},
    "XLY": {"name": "Consumer Disc", "tickers": ["AMZN", "TSLA", "HD", "MCD", "NKE"], "weight": 10},
    "XLP": {"name": "Consumer Stpl", "tickers": ["PG", "KO", "PEP", "COST", "WMT"], "weight": 6},
    "XLI": {"name": "Industrials", "tickers": ["CAT", "UNP", "RTX", "HON", "DE"], "weight": 9},
    "XLB": {"name": "Materials", "tickers": ["LIN", "APD", "SHW", "FCX", "NEM"], "weight": 2},
    "XLU": {"name": "Utilities", "tickers": ["NEE", "DUK", "SO", "D", "AEP"], "weight": 2},
    "XLRE": {"name": "Real Estate", "tickers": ["PLD", "AMT", "EQIX", "SPG", "O"], "weight": 2},
    "XLC": {"name": "Communication", "tickers": ["META", "GOOGL", "NFLX", "DIS", "CMCSA"], "weight": 10},
}


def get_sector_heatmap() -> dict:
    """Get sector ETF performance for heatmap visualization."""
    with _lock:
        if _cache["data"] and time.time() - _cache["ts"] < _TTL:
            return _cache["data"]

    import yfinance as yf

    etf_tickers = list(SECTORS.keys()) + ["SPY"]
    results = []
    spy_change = 0

    try:
        data = yf.download(etf_tickers, period="5d", progress=False, group_by="ticker")

        for etf, info in SECTORS.items():
            try:
                etf_data = data[etf] if etf in data.columns.get_level_values(0) else None
                if etf_data is None or etf_data.empty:
                    continue
                current = float(etf_data["Close"].iloc[-1])
                prev = float(etf_data["Close"].iloc[-2])
                five_day = float(etf_data["Close"].iloc[0])
                change_1d = ((current - prev) / prev) * 100
                change_5d = ((current - five_day) / five_day) * 100

                results.append({
                    "etf": etf,
                    "name": info["name"],
                    "price": round(current, 2),
                    "change_1d": round(change_1d, 2),
                    "change_5d": round(change_5d, 2),
                    "weight": info["weight"],
                    "top_stocks": info["tickers"],
                })
            except Exception:
                continue

        try:
            spy_data = data["SPY"]
            spy_change = round(
                ((float(spy_data["Close"].iloc[-1]) - float(spy_data["Close"].iloc[-2]))
                 / float(spy_data["Close"].iloc[-2])) * 100, 2
            )
        except Exception:
            pass
    except Exception as e:
        console.log(f"[red]Sector heatmap error: {e}")

    results.sort(key=lambda x: x["change_1d"], reverse=True)

    result = {
        "sectors": results,
        "spy_change": spy_change,
        "leaders": [r for r in results if r["change_1d"] > spy_change],
        "laggards": [r for r in results if r["change_1d"] < spy_change],
        "above_spy": sum(1 for r in results if r["change_1d"] > spy_change),
        "total": len(results),
    }

    with _lock:
        _cache["data"] = result
        _cache["ts"] = time.time()

    return result
