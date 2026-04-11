"""options_chain.py — yfinance options chain integration.

No API key required. All data via yf.Ticker(symbol).options / .option_chain().

Public API:
  get_expirations(symbol)              → list[str] of expiry dates
  get_chain(symbol, expiration, ...)   → dict with calls + puts DataFrames
  get_0dte_chain(symbol)               → chain for today or nearest expiry
  find_best_plays(symbol, direction, budget) → top 3 calls + top 3 puts
  get_max_pain(symbol, expiration)     → float (strike price)
  get_put_call_ratio(symbol, expiration) → float

Cache: 2-minute TTL per symbol.
All functions return None on yfinance failure (graceful degradation).
"""
from __future__ import annotations

import logging
import threading
import time
from datetime import date, datetime
from typing import Any

logger = logging.getLogger(__name__)

# ── 0DTE universe (only symbols with liquid 0DTE chains) ──────────────────────
ZDTE_SYMBOLS = {"SPY", "QQQ", "NVDA", "TSLA", "AMD", "META"}

# ── Cache ─────────────────────────────────────────────────────────────────────
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 120  # 2 minutes


def _cache_get(key: str) -> Any | None:
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < _CACHE_TTL:
            return entry["data"]
    return None


def _cache_set(key: str, data: Any) -> None:
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe_float(val, default=0.0) -> float:
    try:
        f = float(val)
        return f if f == f else default  # NaN check
    except (TypeError, ValueError):
        return default


def _row_to_dict(row) -> dict:
    """Convert a DataFrame row to a plain dict with safe numeric types."""
    return {
        "strike":    _safe_float(row.get("strike")),
        "bid":       _safe_float(row.get("bid")),
        "ask":       _safe_float(row.get("ask")),
        "last":      _safe_float(row.get("lastPrice")),
        "volume":    int(_safe_float(row.get("volume"))),
        "oi":        int(_safe_float(row.get("openInterest"))),
        "iv":        round(_safe_float(row.get("impliedVolatility")) * 100, 1),
        "in_the_money": bool(row.get("inTheMoney", False)),
        "contract":  str(row.get("contractSymbol", "")),
    }


def _get_spot_price(symbol: str) -> float | None:
    """Current last price for ATM calculations."""
    try:
        import yfinance as yf
        t = yf.Ticker(symbol)
        hist = t.history(period="1d", interval="1m")
        if hist.empty:
            return None
        return float(hist["Close"].iloc[-1])
    except Exception:
        return None


# ── Core functions ─────────────────────────────────────────────────────────────

def get_expirations(symbol: str) -> list[str]:
    """Return list of available option expiration dates."""
    key = f"exp:{symbol}"
    cached = _cache_get(key)
    if cached is not None:
        return cached
    try:
        import yfinance as yf
        exps = list(yf.Ticker(symbol).options)
        _cache_set(key, exps)
        return exps
    except Exception as e:
        logger.warning(f"options_chain.get_expirations({symbol}): {e}")
        return []


def get_chain(symbol: str, expiration: str, option_type: str = "both") -> dict | None:
    """Fetch options chain for a specific expiration.

    Returns dict with keys:
      calls, puts — each a list[dict] with strike/bid/ask/last/volume/oi/iv
      expiration, symbol, spot
    """
    key = f"chain:{symbol}:{expiration}:{option_type}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    try:
        import yfinance as yf
        ticker = yf.Ticker(symbol)
        raw = ticker.option_chain(expiration)
        spot = _get_spot_price(symbol)

        result: dict[str, Any] = {
            "symbol":     symbol,
            "expiration": expiration,
            "spot":       spot,
            "calls":      [],
            "puts":       [],
        }

        if option_type in ("both", "calls"):
            calls_df = raw.calls
            result["calls"] = [
                _row_to_dict(calls_df.iloc[i])
                for i in range(len(calls_df))
            ]

        if option_type in ("both", "puts"):
            puts_df = raw.puts
            result["puts"] = [
                _row_to_dict(puts_df.iloc[i])
                for i in range(len(puts_df))
            ]

        _cache_set(key, result)
        return result

    except Exception as e:
        logger.warning(f"options_chain.get_chain({symbol}, {expiration}): {e}")
        return None


def get_0dte_chain(symbol: str) -> dict | None:
    """Return the chain for today's expiration, or nearest upcoming expiry."""
    key = f"0dte:{symbol}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    exps = get_expirations(symbol)
    if not exps:
        return None

    today_str = date.today().isoformat()
    # Prefer exact today, then first future date
    target = None
    for exp in exps:
        if exp >= today_str:
            target = exp
            break

    if not target:
        target = exps[-1]

    chain = get_chain(symbol, target)
    if chain:
        chain["is_0dte"] = (target == today_str)
        _cache_set(key, chain)
    return chain


def get_max_pain(symbol: str, expiration: str) -> float | None:
    """Calculate max pain: strike where total OI dollar loss is minimized.

    For each potential expiry strike S:
      pain = sum over all call strikes K: max(0, S-K)*call_OI(K)
            + sum over all put strikes K: max(0, K-S)*put_OI(K)
    Max pain = S that minimizes total pain.
    """
    key = f"maxpain:{symbol}:{expiration}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    chain = get_chain(symbol, expiration)
    if not chain:
        return None

    calls = chain.get("calls", [])
    puts  = chain.get("puts", [])

    if not calls and not puts:
        return None

    all_strikes = sorted(set(
        [c["strike"] for c in calls if c["strike"] > 0] +
        [p["strike"] for p in puts  if p["strike"] > 0]
    ))

    if len(all_strikes) < 2:
        return None

    min_pain = None
    max_pain_strike = None

    for s in all_strikes:
        call_pain = sum(
            max(0.0, s - c["strike"]) * c["oi"]
            for c in calls if c["strike"] > 0
        )
        put_pain = sum(
            max(0.0, p["strike"] - s) * p["oi"]
            for p in puts if p["strike"] > 0
        )
        total = call_pain + put_pain
        if min_pain is None or total < min_pain:
            min_pain = total
            max_pain_strike = s

    _cache_set(key, max_pain_strike)
    return max_pain_strike


def get_put_call_ratio(symbol: str, expiration: str) -> float | None:
    """Total put OI / total call OI for a given expiry."""
    key = f"pcr:{symbol}:{expiration}"
    cached = _cache_get(key)
    if cached is not None:
        return cached

    chain = get_chain(symbol, expiration)
    if not chain:
        return None

    total_call_oi = sum(c["oi"] for c in chain.get("calls", []))
    total_put_oi  = sum(p["oi"] for p in chain.get("puts",  []))

    if total_call_oi == 0:
        return None

    ratio = round(total_put_oi / total_call_oi, 3)
    _cache_set(key, ratio)
    return ratio


def find_best_plays(
    symbol: str,
    direction: str = "bullish",
    budget: float = 200.0,
) -> dict | None:
    """Rank 0DTE options by liquidity + proximity to ATM + budget fit.

    Returns dict with:
      top_calls: list of up to 3 ranked call dicts (each with score, rr_est)
      top_puts:  list of up to 3 ranked put dicts
      expiration, spot, direction
    """
    chain = get_0dte_chain(symbol)
    if not chain:
        return None

    spot  = chain.get("spot") or 0.0
    expiration = chain.get("expiration", "")

    def _score(opt: dict, is_call: bool) -> float:
        strike = opt["strike"]
        if spot > 0:
            moneyness = abs(strike - spot) / spot
        else:
            moneyness = 1.0
        vol  = opt["volume"]
        oi   = opt["oi"]
        cost = (opt["ask"] + opt["bid"]) / 2 * 100  # one contract

        # Skip if too expensive or no liquidity
        if cost > budget or vol < 10 or oi < 50:
            return -1.0

        # Prefer near ATM (within 2%)
        atm_bonus = max(0.0, 1.0 - moneyness / 0.02)
        # Liquidity score (log-scaled)
        import math
        liq = math.log1p(vol) + math.log1p(oi) * 0.5
        # Directional bonus
        dir_bonus = 0.0
        if is_call and strike > spot:
            dir_bonus = 0.3 if direction == "bullish" else -0.3
        elif not is_call and strike < spot:
            dir_bonus = 0.3 if direction == "bearish" else -0.3

        return atm_bonus + liq * 0.1 + dir_bonus

    def _rr(opt: dict) -> str:
        mid = (opt["ask"] + opt["bid"]) / 2
        if mid <= 0:
            return "N/A"
        # Rough R:R: if it goes ITM by 1%, gain ~$100/contract; risk = premium
        cost = mid * 100
        if cost <= 0:
            return "N/A"
        gain_est = 100.0  # rough $1 move → $100/contract
        return f"{gain_est / cost:.1f}:1"

    calls = sorted(
        [c for c in chain.get("calls", []) if _score(c, True) > 0],
        key=lambda c: _score(c, True), reverse=True
    )[:3]

    puts = sorted(
        [p for p in chain.get("puts", []) if _score(p, False) > 0],
        key=lambda p: _score(p, False), reverse=True
    )[:3]

    for c in calls:
        c["rr_est"] = _rr(c)
        c["score"]  = round(_score(c, True), 3)

    for p in puts:
        p["rr_est"] = _rr(p)
        p["score"]  = round(_score(p, False), 3)

    return {
        "symbol":     symbol,
        "expiration": expiration,
        "spot":       spot,
        "direction":  direction,
        "top_calls":  calls,
        "top_puts":   puts,
    }


# ── Unusual activity detection ────────────────────────────────────────────────

def check_unusual_activity(symbol: str) -> list[dict]:
    """Return strikes where volume > 10x open interest (unusual flow)."""
    chain = get_0dte_chain(symbol)
    if not chain:
        return []

    unusual = []
    for opt_type, contracts in [("call", chain.get("calls", [])), ("put", chain.get("puts", []))]:
        for c in contracts:
            vol = c["volume"]
            oi  = c["oi"]
            if oi > 0 and vol >= 10 * oi and vol >= 100:
                unusual.append({
                    "symbol":     symbol,
                    "type":       opt_type,
                    "strike":     c["strike"],
                    "volume":     vol,
                    "oi":         oi,
                    "vol_oi_ratio": round(vol / oi, 1),
                    "iv":         c["iv"],
                    "contract":   c["contract"],
                    "expiration": chain.get("expiration", ""),
                })

    return unusual


# ── Formatted summary for brain_context ──────────────────────────────────────

def get_options_summary(symbol: str) -> str | None:
    """Build a formatted options intelligence block for AI prompt injection."""
    if symbol not in ZDTE_SYMBOLS:
        return None

    chain = get_0dte_chain(symbol)
    if not chain:
        return None

    expiration = chain.get("expiration", "?")
    spot       = chain.get("spot")
    calls      = chain.get("calls", [])
    puts       = chain.get("puts", [])

    # Sort by volume descending
    top_calls = sorted([c for c in calls if c["volume"] > 0], key=lambda x: x["volume"], reverse=True)[:2]
    top_puts  = sorted([p for p in puts  if p["volume"] > 0], key=lambda x: x["volume"], reverse=True)[:2]

    lines = [f"OPTIONS INTELLIGENCE — {symbol} 0DTE ({expiration}):"]

    if spot:
        lines.append(f"  Spot: ${spot:.2f}")

    if top_calls:
        call_str = " | ".join(
            f"{int(c['strike'])}C vol={c['volume']:,} OI={c['oi']:,} IV={c['iv']:.0f}%"
            for c in top_calls
        )
        lines.append(f"  Top calls: {call_str}")

    if top_puts:
        put_str = " | ".join(
            f"{int(p['strike'])}P vol={p['volume']:,} OI={p['oi']:,} IV={p['iv']:.0f}%"
            for p in top_puts
        )
        lines.append(f"  Top puts: {put_str}")

    # Put/call ratio
    pcr = get_put_call_ratio(symbol, expiration)
    if pcr is not None:
        skew = "bearish skew" if pcr > 1.2 else "bullish skew" if pcr < 0.8 else "neutral"
        lines.append(f"  Put/Call OI ratio: {pcr:.2f} ({skew})")

    # Max pain
    mp = get_max_pain(symbol, expiration)
    if mp:
        lines.append(f"  Max pain: ${mp:.0f}")

    # Highest volume strike across all
    all_contracts = [(c, "call") for c in calls] + [(p, "put") for p in puts]
    if all_contracts:
        top = max(all_contracts, key=lambda x: x[0]["volume"])
        top_c, top_type = top
        if top_c["volume"] > 0:
            lines.append(
                f"  Highest volume strike: {int(top_c['strike'])}{top_type[0].upper()} "
                f"vol={top_c['volume']:,}"
            )

    if len(lines) <= 1:
        return None

    return "\n".join(lines)
