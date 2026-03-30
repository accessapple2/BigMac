"""Market Breadth — internal market health beyond just SPY."""
from __future__ import annotations
import time
import threading
from rich.console import Console

console = Console()

_cache = {"data": None, "ts": 0}
_lock = threading.Lock()
_TTL = 600  # 10 minutes


def get_market_breadth() -> dict:
    """Market breadth indicators using free data."""
    with _lock:
        if _cache["data"] and time.time() - _cache["ts"] < _TTL:
            return _cache["data"]

    import yfinance as yf

    tickers = [
        "SPY", "QQQ", "IWM", "DIA",
        "XLK", "XLV", "XLF", "XLE", "XLY", "XLP",
        "XLI", "XLB", "XLU", "XLRE", "XLC",
        "TLT", "GLD",
    ]

    try:
        data = yf.download(tickers, period="30d", progress=False, group_by="ticker")
    except Exception:
        return {"error": "Failed to download market data"}

    breadth = {}
    sector_etfs = ["XLK", "XLV", "XLF", "XLE", "XLY", "XLP", "XLI", "XLB", "XLU", "XLRE", "XLC"]

    # Sectors above 20-day SMA
    above_sma = 0
    sector_details = []
    for etf in sector_etfs:
        try:
            close = data[etf]["Close"]
            sma20 = close.rolling(20).mean()
            curr = float(close.iloc[-1])
            sma_val = float(sma20.iloc[-1])
            above = curr > sma_val
            if above:
                above_sma += 1
            change_1d = round(((curr / float(close.iloc[-2])) - 1) * 100, 2)
            sector_details.append({"etf": etf, "above_sma": above, "change_1d": change_1d})
        except Exception:
            pass

    breadth["sectors_above_20sma"] = above_sma
    breadth["sectors_total"] = len(sector_etfs)
    breadth["breadth_pct"] = round(above_sma / len(sector_etfs) * 100)
    breadth["sector_details"] = sector_details

    # Risk-on vs risk-off
    try:
        spy_ret = round(((float(data["SPY"]["Close"].iloc[-1]) / float(data["SPY"]["Close"].iloc[-5])) - 1) * 100, 2)
        tlt_ret = round(((float(data["TLT"]["Close"].iloc[-1]) / float(data["TLT"]["Close"].iloc[-5])) - 1) * 100, 2)
        gld_ret = round(((float(data["GLD"]["Close"].iloc[-1]) / float(data["GLD"]["Close"].iloc[-5])) - 1) * 100, 2)
        breadth["risk_appetite"] = {
            "spy_5d": spy_ret, "bonds_5d": tlt_ret, "gold_5d": gld_ret,
            "signal": "RISK_ON" if spy_ret > tlt_ret and spy_ret > gld_ret else "RISK_OFF",
        }
    except Exception:
        pass

    # Small caps vs large caps
    try:
        iwm_ret = round(((float(data["IWM"]["Close"].iloc[-1]) / float(data["IWM"]["Close"].iloc[-5])) - 1) * 100, 2)
        breadth["small_vs_large"] = {
            "iwm_5d": iwm_ret, "spy_5d": spy_ret,
            "signal": "SMALL_LEADING" if iwm_ret > spy_ret else "LARGE_LEADING",
        }
    except Exception:
        pass

    with _lock:
        _cache["data"] = breadth
        _cache["ts"] = time.time()

    return breadth
