"""Polygon.io Data Provider — real-time snapshots, historical bars, options flow.

Activates only when POLYGON_API_KEY is set in config.py or environment.
Provides:
  - Real-time ticker snapshots (faster than Yahoo for scanner)
  - Historical minute/day bars (for backtesting and charts)
  - Options flow data (for Counselor Troi's sentiment analysis)
  - Dividends and splits (for position tracking)

Usage:
    from engine.providers.polygon_provider import PolygonData
    poly = PolygonData()
    if poly.is_active():
        snapshot = poly.get_snapshot("NVDA")
        bars = poly.get_bars("NVDA", timespan="day", limit=30)
        flow = poly.get_options_flow("NVDA")
"""
from __future__ import annotations
import time
import requests
from datetime import datetime, timedelta
from rich.console import Console

console = Console()

_BASE = "https://api.polygon.io"
_cache = {}
_CACHE_TTL = {
    "snapshot": 15,       # 15 seconds for real-time
    "bars": 300,          # 5 minutes for bars
    "options_flow": 60,   # 1 minute for options
    "dividends": 86400,   # 24 hours for corporate actions
    "splits": 86400,
}


class PolygonData:
    """Polygon.io API client for TradeMinds."""

    def __init__(self):
        from config import POLYGON_API_KEY
        self.api_key = POLYGON_API_KEY
        self._session = requests.Session()
        self._session.params = {"apiKey": self.api_key}
        self._session.headers.update({"User-Agent": "TradeMinds/1.0"})

    def is_active(self) -> bool:
        """Check if Polygon API key is configured."""
        return bool(self.api_key and len(self.api_key) > 5)

    def _get(self, path: str, params: dict = None, cache_key: str = None, ttl: int = 60) -> dict | None:
        """Make a cached GET request to Polygon API."""
        if not self.is_active():
            return None

        if cache_key and cache_key in _cache:
            cached_ts, cached_data = _cache[cache_key]
            if time.time() - cached_ts < ttl:
                return cached_data

        try:
            r = self._session.get(f"{_BASE}{path}", params=params, timeout=10)
            r.raise_for_status()
            data = r.json()

            if cache_key:
                _cache[cache_key] = (time.time(), data)

            return data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403:
                console.log("[yellow]Polygon: API key invalid or insufficient permissions")
            elif e.response.status_code == 429:
                console.log("[yellow]Polygon: Rate limit exceeded")
            return None
        except Exception as e:
            console.log(f"[yellow]Polygon request error: {e}")
            return None

    # --- Real-time Snapshots ---

    def get_snapshot(self, symbol: str) -> dict | None:
        """Get real-time snapshot for a ticker.

        Returns: {price, change, change_pct, volume, vwap, high, low, open, prev_close, timestamp}
        """
        data = self._get(
            f"/v2/snapshot/locale/us/markets/stocks/tickers/{symbol.upper()}",
            cache_key=f"snap:{symbol}",
            ttl=_CACHE_TTL["snapshot"],
        )
        if not data or data.get("status") != "OK":
            return None

        ticker = data.get("ticker", {})
        day = ticker.get("day", {})
        prev = ticker.get("prevDay", {})
        last = ticker.get("lastTrade", {}) or ticker.get("min", {})

        price = last.get("p") or day.get("c", 0)
        prev_close = prev.get("c", 0)
        change = price - prev_close if prev_close else 0

        return {
            "symbol": symbol.upper(),
            "price": price,
            "change": round(change, 2),
            "change_pct": round(change / prev_close * 100, 2) if prev_close else 0,
            "volume": day.get("v", 0),
            "vwap": day.get("vw", 0),
            "high": day.get("h", 0),
            "low": day.get("l", 0),
            "open": day.get("o", 0),
            "prev_close": prev_close,
            "timestamp": datetime.now().isoformat(),
            "source": "polygon",
        }

    def get_snapshots_bulk(self, symbols: list) -> dict:
        """Get snapshots for multiple tickers at once.

        Returns: {symbol: snapshot_dict}
        """
        data = self._get(
            "/v2/snapshot/locale/us/markets/stocks/tickers",
            params={"tickers": ",".join(s.upper() for s in symbols)},
            cache_key=f"snaps:{'_'.join(sorted(symbols))}",
            ttl=_CACHE_TTL["snapshot"],
        )
        if not data or not data.get("tickers"):
            return {}

        results = {}
        for t in data["tickers"]:
            sym = t.get("ticker", "")
            day = t.get("day", {})
            prev = t.get("prevDay", {})
            last = t.get("lastTrade", {}) or {}
            price = last.get("p") or day.get("c", 0)
            prev_close = prev.get("c", 0)
            change = price - prev_close if prev_close else 0
            results[sym] = {
                "symbol": sym,
                "price": price,
                "change_pct": round(change / prev_close * 100, 2) if prev_close else 0,
                "volume": day.get("v", 0),
                "high": day.get("h", 0),
                "low": day.get("l", 0),
                "source": "polygon",
            }
        return results

    # --- Historical Bars ---

    def get_bars(self, symbol: str, timespan: str = "day", multiplier: int = 1,
                 limit: int = 30, from_date: str = None, to_date: str = None) -> list:
        """Get historical OHLCV bars.

        Args:
            symbol: Ticker symbol
            timespan: "minute", "hour", "day", "week", "month"
            multiplier: Bar size multiplier (e.g. 5 for 5-minute bars)
            limit: Number of bars to return
            from_date: Start date (YYYY-MM-DD), defaults to `limit` days ago
            to_date: End date (YYYY-MM-DD), defaults to today

        Returns: [{time, open, high, low, close, volume, vwap}, ...]
        """
        if not to_date:
            to_date = datetime.now().strftime("%Y-%m-%d")
        if not from_date:
            days_back = limit * (1 if timespan == "day" else 7 if timespan == "week" else 30 if timespan == "month" else 1)
            from_date = (datetime.now() - timedelta(days=days_back + 5)).strftime("%Y-%m-%d")

        data = self._get(
            f"/v2/aggs/ticker/{symbol.upper()}/range/{multiplier}/{timespan}/{from_date}/{to_date}",
            params={"limit": limit, "sort": "asc"},
            cache_key=f"bars:{symbol}:{timespan}:{multiplier}:{limit}",
            ttl=_CACHE_TTL["bars"],
        )
        if not data or not data.get("results"):
            return []

        return [
            {
                "time": datetime.fromtimestamp(bar["t"] / 1000).strftime("%Y-%m-%d %H:%M"),
                "open": bar.get("o", 0),
                "high": bar.get("h", 0),
                "low": bar.get("l", 0),
                "close": bar.get("c", 0),
                "volume": bar.get("v", 0),
                "vwap": bar.get("vw", 0),
            }
            for bar in data["results"]
        ]

    # --- Options Flow (for Counselor Troi) ---

    def get_options_flow(self, symbol: str, limit: int = 50) -> list:
        """Get recent options contracts activity for a symbol.

        Returns: [{strike, expiry, type, volume, open_interest, implied_volatility, ...}]
        Feeds into Counselor Troi's sentiment analysis.
        """
        data = self._get(
            f"/v3/snapshot/options/{symbol.upper()}",
            params={"limit": limit, "order": "desc", "sort": "volume"},
            cache_key=f"opts:{symbol}",
            ttl=_CACHE_TTL["options_flow"],
        )
        if not data or not data.get("results"):
            return []

        contracts = []
        for c in data["results"]:
            details = c.get("details", {})
            greeks = c.get("greeks", {})
            day = c.get("day", {})
            contracts.append({
                "strike": details.get("strike_price", 0),
                "expiry": details.get("expiration_date", ""),
                "type": details.get("contract_type", "").lower(),  # "call" or "put"
                "volume": day.get("volume", 0),
                "open_interest": c.get("open_interest", 0),
                "implied_volatility": c.get("implied_volatility", 0),
                "delta": greeks.get("delta", 0),
                "gamma": greeks.get("gamma", 0),
                "theta": greeks.get("theta", 0),
                "vega": greeks.get("vega", 0),
                "last_price": day.get("close", 0),
                "change_pct": day.get("change_percent", 0),
            })

        return contracts

    def get_options_sentiment(self, symbol: str) -> dict:
        """Analyze options flow for sentiment signal (for Troi).

        Returns: {signal, put_call_ratio, total_call_vol, total_put_vol, max_pain, unusual_activity}
        """
        contracts = self.get_options_flow(symbol, limit=100)
        if not contracts:
            return {"signal": "NO_DATA", "symbol": symbol}

        call_vol = sum(c["volume"] for c in contracts if c["type"] == "call")
        put_vol = sum(c["volume"] for c in contracts if c["type"] == "put")
        total_vol = call_vol + put_vol

        pc_ratio = put_vol / call_vol if call_vol > 0 else 999

        # Unusual activity: contracts with volume > 5x open interest
        unusual = [
            c for c in contracts
            if c["open_interest"] > 0 and c["volume"] > c["open_interest"] * 5
        ]

        # Sentiment signal
        if pc_ratio > 1.5:
            signal = "EXTREME_FEAR"
        elif pc_ratio > 1.0:
            signal = "FEAR"
        elif pc_ratio > 0.7:
            signal = "NEUTRAL"
        elif pc_ratio > 0.4:
            signal = "GREED"
        else:
            signal = "EXTREME_GREED"

        return {
            "symbol": symbol,
            "signal": signal,
            "put_call_ratio": round(pc_ratio, 2),
            "total_call_vol": call_vol,
            "total_put_vol": put_vol,
            "total_vol": total_vol,
            "unusual_activity": len(unusual),
            "unusual_contracts": unusual[:5],
            "timestamp": datetime.now().isoformat(),
        }

    # --- Corporate Actions ---

    def get_dividends(self, symbol: str, limit: int = 10) -> list:
        """Get recent dividends for a symbol."""
        data = self._get(
            f"/v3/reference/dividends",
            params={"ticker": symbol.upper(), "limit": limit, "order": "desc", "sort": "pay_date"},
            cache_key=f"divs:{symbol}",
            ttl=_CACHE_TTL["dividends"],
        )
        if not data or not data.get("results"):
            return []

        return [
            {
                "ex_date": d.get("ex_dividend_date", ""),
                "pay_date": d.get("pay_date", ""),
                "amount": d.get("cash_amount", 0),
                "frequency": d.get("frequency", 0),
                "type": d.get("dividend_type", ""),
            }
            for d in data["results"]
        ]

    def get_splits(self, symbol: str, limit: int = 5) -> list:
        """Get recent stock splits."""
        data = self._get(
            f"/v3/reference/splits",
            params={"ticker": symbol.upper(), "limit": limit, "order": "desc"},
            cache_key=f"splits:{symbol}",
            ttl=_CACHE_TTL["splits"],
        )
        if not data or not data.get("results"):
            return []

        return [
            {
                "execution_date": s.get("execution_date", ""),
                "split_from": s.get("split_from", 1),
                "split_to": s.get("split_to", 1),
            }
            for s in data["results"]
        ]


def build_troi_context(symbol: str) -> str:
    """Build options flow context for Counselor Troi's analysis.

    Returns empty string if Polygon is not active.
    """
    try:
        poly = PolygonData()
        if not poly.is_active():
            return ""

        sentiment = poly.get_options_sentiment(symbol)
        if sentiment.get("signal") == "NO_DATA":
            return ""

        ctx = (
            f"OPTIONS FLOW ({symbol}): "
            f"Signal={sentiment['signal']}, "
            f"P/C Ratio={sentiment['put_call_ratio']}, "
            f"Call Vol={sentiment['total_call_vol']:,}, "
            f"Put Vol={sentiment['total_put_vol']:,}"
        )
        if sentiment.get("unusual_activity", 0) > 0:
            ctx += f", {sentiment['unusual_activity']} unusual contracts detected"

        return ctx
    except Exception:
        return ""
