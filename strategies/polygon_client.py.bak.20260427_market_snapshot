"""
Polygon.io client for real IV + option chain data.

Replaces the mock_data source when POLYGON_API_KEY is configured.
Read-only market data calls — never places orders (that's alpaca's job).
"""
from __future__ import annotations
import time
from dataclasses import dataclass
from datetime import date, timedelta
from typing import Optional

import urllib.request
import urllib.parse
import json

from .polygon_config import _get_api_key
from .mock_data import SpreadQuote, OptionLeg


POLYGON_BASE = "https://api.polygon.io"
REQUEST_TIMEOUT = 10  # seconds

# ---------------------------------------------------------------------------
# Rate limiter — safe for Options Starter plan (~1.5 req/s sustained)
# ---------------------------------------------------------------------------
_last_api_call: float = 0.0
RATE_LIMIT_DELAY: float = 1.2   # seconds between calls (conservative for Options Starter)


def _rate_limited_get(path: str, params: dict | None = None) -> Optional[dict]:
    """Throttled wrapper: enforces RATE_LIMIT_DELAY between successive calls."""
    global _last_api_call
    now = time.time()
    wait = RATE_LIMIT_DELAY - (now - _last_api_call)
    if wait > 0:
        time.sleep(wait)
    result = _get(path, params)
    _last_api_call = time.time()
    return result


def _get(path: str, params: dict | None = None) -> Optional[dict]:
    """Make a GET request to Polygon. Returns None on failure."""
    key = _get_api_key()
    if not key:
        return None

    params = dict(params or {})
    params["apiKey"] = key
    url = f"{POLYGON_BASE}{path}?{urllib.parse.urlencode(params)}"

    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=REQUEST_TIMEOUT) as resp:
            if resp.status != 200:
                print(f"[polygon] {path} returned HTTP {resp.status}")
                return None
            return json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        print(f"[polygon] {path} HTTPError: {e.code} {e.reason}")
        return None
    except Exception as e:
        print(f"[polygon] {path} error: {type(e).__name__}: {e}")
        return None


def fetch_daily_bars(ticker: str, days: int = 30) -> list[dict]:
    """
    Fetch daily OHLCV bars for the given ticker from Polygon.
    Returns list of dicts with keys: date, open, high, low, close, volume.
    Most recent bar last (ascending date order).

    Uses _get directly (no rate-limit delay) — suitable for batch calls
    (e.g. 11 ETFs for breadth). Polygon Starter is 15-min delayed; fine
    for EOD calculations like RSI, momentum, and breadth.
    """
    from datetime import datetime, timezone as _tz
    end = date.today()
    start = end - timedelta(days=days + 10)  # +10 buffer for weekends/holidays
    path = f"/v2/aggs/ticker/{ticker}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    data = _get(path, {"adjusted": "true", "sort": "asc", "limit": 500})
    if not data or "results" not in data:
        return []
    bars = []
    for r in data["results"]:
        ts_ms = r.get("t", 0)
        d = datetime.fromtimestamp(ts_ms / 1000, tz=_tz.utc).date()
        bars.append({
            "date": d.isoformat(),
            "open": r.get("o"),
            "high": r.get("h"),
            "low": r.get("l"),
            "close": r.get("c"),
            "volume": r.get("v"),
        })
    return bars[-days:] if len(bars) > days else bars


def fetch_ticker_snapshots(tickers: list[str]) -> dict[str, dict]:
    """
    Fetch current-day and previous-day close for multiple tickers in one call.
    Returns {ticker: {"close": float, "prev_close": float}}.
    Uses /v2/snapshot/locale/us/markets/stocks/tickers (bulk endpoint).
    Suitable for breadth calculations — 1 call instead of N calls.
    """
    if not tickers:
        return {}
    params = {"tickers": ",".join(tickers)}
    data = _get("/v2/snapshot/locale/us/markets/stocks/tickers", params)
    if not data:
        return {}
    result: dict[str, dict] = {}
    for item in data.get("tickers", []):
        ticker = item.get("ticker", "")
        if not ticker:
            continue
        day = item.get("day") or {}
        prev = item.get("prevDay") or {}
        close = day.get("c") or item.get("lastTrade", {}).get("p")
        prev_close = prev.get("c")
        result[ticker] = {
            "close": float(close) if close is not None else None,
            "prev_close": float(prev_close) if prev_close is not None else None,
        }
    return result


def fetch_spot_price(ticker: str) -> Optional[float]:
    """
    Get recent spot price for underlying.
    Uses /v2/aggs/ticker/{ticker}/prev (Options Starter covers this).
    Returns previous trading day's close price — sufficient for strike
    selection on 3-14 DTE spreads.
    """
    data = _rate_limited_get(f"/v2/aggs/ticker/{ticker}/prev", {"adjusted": "true"})
    if not data or data.get("status") != "OK":
        return None
    results = data.get("results") or []
    if not results:
        return None
    try:
        return float(results[0]["c"])  # close price
    except (KeyError, ValueError, TypeError, IndexError):
        return None


@dataclass
class OptionContractSummary:
    ticker: str          # e.g. "O:SPY250425C00700000"
    strike: float
    expiration: str       # YYYY-MM-DD
    option_type: str      # "call" | "put"
    iv: Optional[float]   # Implied volatility (decimal, e.g. 0.185)
    bid: Optional[float]
    ask: Optional[float]
    mid: Optional[float]


def fetch_option_snapshot(
    underlying: str, target_dte: int,
) -> list[OptionContractSummary]:
    """
    Get option chain snapshot for underlying, filtered by target expiration.
    Now handles weekends/holidays by searching a ±5 day window and picking closest expiry.
    """
    target_date = (date.today() + timedelta(days=target_dte)).isoformat()

    # Search a window around target DTE to catch nearest trading expiration
    params = {
        "expiration_date.gte": (date.today() + timedelta(days=target_dte - 5)).isoformat(),
        "expiration_date.lte": (date.today() + timedelta(days=target_dte + 5)).isoformat(),
        "limit": 250,
    }

    data = _get(f"/v3/snapshot/options/{underlying}", params)
    if not data or "results" not in data:
        return []

    # Pass 1: find closest expiry to target across all returned results
    target_dt = date.fromisoformat(target_date)
    closest_expiry = None
    min_diff = float('inf')
    for item in data.get("results", []):
        expiry = (item.get("details") or {}).get("expiration_date", "")
        if not expiry:
            continue
        diff = abs((date.fromisoformat(expiry) - target_dt).days)
        if diff < min_diff:
            min_diff = diff
            closest_expiry = expiry

    if not closest_expiry:
        return []

    # Pass 2: collect contracts from the closest expiry only
    contracts = []
    for item in data.get("results", []):
        try:
            details = item.get("details", {})
            if details.get("expiration_date") != closest_expiry:
                continue
            quote = item.get("last_quote", {})
            iv_raw = item.get("implied_volatility")
            bid = quote.get("bid")
            ask = quote.get("ask")
            mid = (bid + ask) / 2.0 if (bid and ask) else None

            contracts.append(OptionContractSummary(
                ticker=details.get("ticker", ""),
                strike=float(details.get("strike_price", 0)),
                expiration=closest_expiry,
                option_type=details.get("contract_type", "").lower(),
                iv=float(iv_raw) if iv_raw is not None else None,
                bid=float(bid) if bid else None,
                ask=float(ask) if ask else None,
                mid=mid,
            ))
        except (KeyError, ValueError, TypeError):
            continue

    return contracts


def fetch_atm_iv(underlying: str, target_dte: int = 30) -> Optional[float]:
    """
    Get ATM implied volatility for the underlying at a given DTE.
    Used for IV rank computation — this is the real IV, not realized vol.
    """
    spot = fetch_spot_price(underlying)
    if spot is None:
        return None

    contracts = fetch_option_snapshot(underlying, target_dte)
    if not contracts:
        return None

    # Find the ATM call (closest strike to spot, call side)
    calls = [c for c in contracts if c.option_type == "call" and c.iv is not None]
    if not calls:
        return None

    closest = min(calls, key=lambda c: abs(c.strike - spot))
    return closest.iv


def build_spread_quote(
    ticker: str, structure: str, dte_target: int, width: float
) -> Optional[SpreadQuote]:
    """
    Build a real SpreadQuote from Polygon data.
    Returns None if chain is unavailable or quote can't be constructed.
    """
    spot = fetch_spot_price(ticker)
    if spot is None:
        return None

    contracts = fetch_option_snapshot(ticker, dte_target)
    if not contracts:
        return None

    import math

    if structure == "bull_call_spread":
        calls = [c for c in contracts if c.option_type == "call"]
        long_target = float(math.floor(spot))
        short_target = long_target + width
        long_leg_contract = min(calls, key=lambda c: abs(c.strike - long_target), default=None)
        short_leg_contract = min(calls, key=lambda c: abs(c.strike - short_target), default=None)
        if not (long_leg_contract and short_leg_contract):
            return None
        if long_leg_contract.strike >= short_leg_contract.strike:
            return None  # strike selection collapsed
        if not (long_leg_contract.mid and short_leg_contract.mid):
            return None

        net_debit = long_leg_contract.mid - short_leg_contract.mid
        actual_width = short_leg_contract.strike - long_leg_contract.strike
        if net_debit >= actual_width:
            print(f"[polygon] rejecting {ticker}: debit ${net_debit:.2f} >= width ${actual_width}")
            return None

        return SpreadQuote(
            ticker=ticker, structure=structure,
            long_leg=OptionLeg("buy", "call", long_leg_contract.strike,
                               long_leg_contract.expiration, long_leg_contract.mid),
            short_leg=OptionLeg("sell", "call", short_leg_contract.strike,
                                short_leg_contract.expiration, short_leg_contract.mid),
            net_debit=net_debit, net_credit=0.0,
            max_profit=(actual_width - net_debit) * 100.0,
            max_loss=net_debit * 100.0,
            width=actual_width, dte=dte_target,
        )

    elif structure == "bull_put_spread":
        puts = [c for c in contracts if c.option_type == "put"]
        short_target = float(math.floor(spot))
        long_target = short_target - width
        short_leg_contract = min(puts, key=lambda c: abs(c.strike - short_target), default=None)
        long_leg_contract = min(puts, key=lambda c: abs(c.strike - long_target), default=None)
        if not (long_leg_contract and short_leg_contract):
            return None
        if long_leg_contract.strike >= short_leg_contract.strike:
            return None
        if not (long_leg_contract.mid and short_leg_contract.mid):
            return None

        net_credit = short_leg_contract.mid - long_leg_contract.mid
        actual_width = short_leg_contract.strike - long_leg_contract.strike
        if net_credit >= actual_width:
            print(f"[polygon] rejecting {ticker}: credit ${net_credit:.2f} >= width ${actual_width}")
            return None

        return SpreadQuote(
            ticker=ticker, structure=structure,
            short_leg=OptionLeg("sell", "put", short_leg_contract.strike,
                                short_leg_contract.expiration, short_leg_contract.mid),
            long_leg=OptionLeg("buy", "put", long_leg_contract.strike,
                               long_leg_contract.expiration, long_leg_contract.mid),
            net_debit=0.0, net_credit=net_credit,
            max_profit=net_credit * 100.0,
            max_loss=(actual_width - net_credit) * 100.0,
            width=actual_width, dte=dte_target,
        )

    return None
