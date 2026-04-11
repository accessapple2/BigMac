"""Premium Tracker — live SPY 0DTE options chain.

Primary: Alpaca paper options API.
Fallback: yfinance.

Fetches chain every 30s during market hours.
Tracks premium direction: net put vs call premium.
"""
from __future__ import annotations

import logging
import os
import threading
import time
from datetime import datetime, date
from typing import Optional

import requests

logger = logging.getLogger("premium_tracker")

ALPACA_DATA_BASE = "https://data.alpaca.markets"
STRIKE_RANGE = 5.0          # show strikes within $5 of spot
LARGE_TRADE = 100           # contracts — classify as "large"
CACHE_TTL = 30              # seconds

_cache: dict = {"chain": None, "flow": None, "ts": 0.0}
_lock = threading.Lock()


# ---------------------------------------------------------------------------
# Auth
# ---------------------------------------------------------------------------

def _headers() -> dict:
    key = os.getenv("ALPACA_API_KEY") or os.getenv("ALPACA_KEY", "")
    secret = os.getenv("ALPACA_SECRET_KEY") or os.getenv("ALPACA_API_SECRET", "")
    return {"APCA-API-KEY-ID": key, "APCA-API-SECRET-KEY": secret}


# ---------------------------------------------------------------------------
# Spot price
# ---------------------------------------------------------------------------

def _spy_spot() -> float:
    try:
        r = requests.get(
            f"{ALPACA_DATA_BASE}/v2/stocks/SPY/quotes/latest",
            headers=_headers(), timeout=5,
        )
        if r.ok:
            q = r.json().get("quote", {})
            mid = (q.get("ap", 0) + q.get("bp", 0)) / 2
            return mid or q.get("ap", 0)
    except Exception:
        pass
    try:
        import yfinance as yf
        return yf.Ticker("SPY").fast_info.last_price or 0
    except Exception:
        return 0


# ---------------------------------------------------------------------------
# Chain fetch
# ---------------------------------------------------------------------------

def fetch_chain() -> list:
    """Fetch SPY 0DTE chain. Returns list of contract dicts, sorted by strike."""
    spot = _spy_spot()
    if not spot:
        return []

    exp = date.today().strftime("%Y-%m-%d")

    try:
        r = requests.get(
            f"{ALPACA_DATA_BASE}/v1beta1/options/snapshots/SPY",
            headers=_headers(),
            params={
                "expiration_date": exp,
                "strike_price_gte": spot - STRIKE_RANGE,
                "strike_price_lte": spot + STRIKE_RANGE,
                "limit": 200,
                "feed": "indicative",
            },
            timeout=10,
        )
        if r.ok:
            return _parse_alpaca(r.json().get("snapshots", {}), spot)
    except Exception as e:
        logger.debug("Alpaca chain fetch error: %s", e)

    return _fallback_yf(spot, exp)


def _parse_alpaca(snapshots: dict, spot: float) -> list:
    contracts = []
    for sym, snap in snapshots.items():
        try:
            greeks = snap.get("greeks", {})
            quote = snap.get("latestQuote", {})
            trade = snap.get("latestTrade", {})
            details = snap.get("details", {})
            daily = snap.get("dailyBar", {})
            prev = snap.get("prevDailyBar", {})

            opt_type = details.get("type", "").lower()
            strike = float(details.get("strike_price", 0))
            bid = float(quote.get("bp", 0) or 0)
            ask = float(quote.get("ap", 0) or 0)
            last = float(trade.get("p", 0) or 0)
            volume = int(daily.get("v", 0) or 0)
            oi = int(snap.get("openInterest", 0) or 0)
            iv_raw = float(greeks.get("iv", 0) or 0)
            iv = round(iv_raw * 100, 1) if iv_raw < 5 else round(iv_raw, 1)
            delta = float(greeks.get("delta", 0) or 0)
            gamma = float(greeks.get("gamma", 0) or 0)
            prev_close = float(prev.get("c", 0) or 0)
            change_pct = round((last - prev_close) / prev_close * 100, 1) if prev_close and last else 0.0

            contracts.append({
                "symbol": sym,
                "type": opt_type,
                "strike": strike,
                "bid": round(bid, 2),
                "ask": round(ask, 2),
                "last": round(last, 2),
                "volume": volume,
                "oi": oi,
                "iv": iv,
                "delta": round(delta, 3),
                "gamma": round(gamma, 4),
                "change_pct": change_pct,
                "spot": round(spot, 2),
                "unusual_volume": volume > 2 * max(oi, 100),
            })
        except Exception:
            continue
    return sorted(contracts, key=lambda x: (x["strike"], x["type"]))


def _fallback_yf(spot: float, exp: str) -> list:
    try:
        import yfinance as yf
        t = yf.Ticker("SPY")
        avail = t.options or []
        if exp not in avail:
            exp = avail[0] if avail else exp
        chain = t.option_chain(exp)
        result = []
        for opt_type, df in [("put", chain.puts), ("call", chain.calls)]:
            for _, row in df.iterrows():
                strike = float(row.get("strike", 0))
                if not (spot - STRIKE_RANGE <= strike <= spot + STRIKE_RANGE):
                    continue
                bid = float(row.get("bid", 0) or 0)
                ask = float(row.get("ask", 0) or 0)
                last = float(row.get("lastPrice", 0) or 0)
                volume = int(row.get("volume", 0) or 0)
                oi = int(row.get("openInterest", 0) or 0)
                iv = float(row.get("impliedVolatility", 0) or 0) * 100
                result.append({
                    "symbol": f"SPY{'P' if opt_type=='put' else 'C'}{strike:.0f}",
                    "type": opt_type,
                    "strike": strike,
                    "bid": round(bid, 2),
                    "ask": round(ask, 2),
                    "last": round(last, 2),
                    "volume": volume,
                    "oi": oi,
                    "iv": round(iv, 1),
                    "delta": -0.35 if opt_type == "put" else 0.35,
                    "gamma": 0.03,
                    "change_pct": 0.0,
                    "spot": round(spot, 2),
                    "unusual_volume": volume > 2 * max(oi, 100),
                })
        return sorted(result, key=lambda x: (x["strike"], x["type"]))
    except Exception as e:
        logger.warning("yfinance chain fallback error: %s", e)
        return []


# ---------------------------------------------------------------------------
# Premium flow
# ---------------------------------------------------------------------------

def compute_flow(chain: list) -> dict:
    """Net premium direction: bearish if put premium > call premium."""
    put_p = call_p = 0.0
    big_puts: list = []
    big_calls: list = []

    for c in chain:
        mid = (c["bid"] + c["ask"]) / 2 if c["ask"] > 0 else c["last"]
        vol = c["volume"]
        dollar_val = mid * vol * 100

        if c["type"] == "put":
            put_p += dollar_val
            if vol >= LARGE_TRADE:
                big_puts.append({"strike": c["strike"], "volume": vol,
                                  "premium_usd": round(dollar_val), "mid": round(mid, 2)})
        else:
            call_p += dollar_val
            if vol >= LARGE_TRADE:
                big_calls.append({"strike": c["strike"], "volume": vol,
                                   "premium_usd": round(dollar_val), "mid": round(mid, 2)})

    total = put_p + call_p or 1
    net = put_p - call_p

    return {
        "put_premium_usd": round(put_p),
        "call_premium_usd": round(call_p),
        "net_usd": round(net),
        "put_pct": round(put_p / total * 100, 1),
        "call_pct": round(call_p / total * 100, 1),
        "direction": "BEARISH" if net > 0 else "BULLISH",
        "conviction": round(abs(net) / total * 100, 1),
        "large_put_buys": sorted(big_puts, key=lambda x: -x["premium_usd"])[:5],
        "large_call_buys": sorted(big_calls, key=lambda x: -x["premium_usd"])[:5],
        "updated_at": datetime.now().isoformat(),
    }


# ---------------------------------------------------------------------------
# Cached accessors
# ---------------------------------------------------------------------------

def get_chain() -> list:
    with _lock:
        if _cache["chain"] is not None and time.time() - _cache["ts"] < CACHE_TTL:
            return _cache["chain"]

    chain = fetch_chain()
    flow = compute_flow(chain)
    with _lock:
        _cache["chain"] = chain
        _cache["flow"] = flow
        _cache["ts"] = time.time()
    return chain


def get_flow() -> dict:
    get_chain()   # ensures flow is populated
    with _lock:
        return _cache["flow"] or {}
