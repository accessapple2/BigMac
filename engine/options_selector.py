"""Select optimal options expiry and strike for arena AI players."""
from __future__ import annotations
import json
import logging
import math
import os
import time
from datetime import datetime, timedelta
from rich.console import Console

console = Console()
logger = logging.getLogger("options_selector")

_earnings_mem_cache: dict = {}
_EARNINGS_CACHE_TTL  = 21_600   # 6 hours
_EARNINGS_CACHE_FILE = os.path.join(os.path.dirname(__file__), "..", "data", "earnings_cache.json")


def _next_earnings_date(symbol: str):
    """Return next earnings date for symbol as datetime.date, or None (fail open)."""
    now = time.time()
    if symbol in _earnings_mem_cache:
        ts, dt = _earnings_mem_cache[symbol]
        if now - ts < _EARNINGS_CACHE_TTL:
            return dt
    # Fast path: file-based cache (data/earnings_cache.json)
    try:
        with open(_EARNINGS_CACHE_FILE) as _f:
            _file_data = json.load(_f)
        _entry = _file_data.get("data", {}).get(symbol)
        if _entry and _entry.get("date"):
            dt = datetime.strptime(_entry["date"], "%Y-%m-%d").date()
            _earnings_mem_cache[symbol] = (now, dt)
            return dt
    except Exception:
        pass
    # yfinance fallback for cache misses
    try:
        import yfinance as _yf
        _tk = _yf.Ticker(symbol)
        _cal = _tk.calendar
        if _cal is not None:
            _earn_dt = None
            if hasattr(_cal, "get"):
                _earn_dt = _cal.get("Earnings Date")
            elif hasattr(_cal, "columns") and "Earnings Date" in _cal.columns:
                _earn_dt = _cal["Earnings Date"].iloc[0] if not _cal.empty else None
            if _earn_dt is not None:
                if hasattr(_earn_dt, "__len__") and not hasattr(_earn_dt, "date"):
                    _earn_dt = _earn_dt[0] if len(_earn_dt) > 0 else None
                if _earn_dt is not None and hasattr(_earn_dt, "date"):
                    _earn_dt = _earn_dt.date()
                if isinstance(_earn_dt, datetime):
                    _earn_dt = _earn_dt.date()
                if _earn_dt is not None:
                    _earnings_mem_cache[symbol] = (now, _earn_dt)
                    return _earn_dt
    except Exception as _e:
        logger.debug(f"earnings lookup failed for {symbol}: {_e}")
    return None  # fail open

_BSM_RISK_FREE = 0.045   # hardcoded; move to config when Polygon activated
_BSM_CEILING   = 1.5     # reject if market premium > 1.5× BSM fair value


def _bsm_call(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes call price (stdlib math.erf only)."""
    if T <= 0 or sigma <= 0:
        return max(S - K, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    N = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return S * N(d1) - K * math.exp(-r * T) * N(d2)


def _bsm_put(S: float, K: float, T: float, r: float, sigma: float) -> float:
    """Black-Scholes put price (stdlib math.erf only)."""
    if T <= 0 or sigma <= 0:
        return max(K - S, 0.0)
    d1 = (math.log(S / K) + (r + 0.5 * sigma * sigma) * T) / (sigma * math.sqrt(T))
    d2 = d1 - sigma * math.sqrt(T)
    N = lambda x: 0.5 * (1 + math.erf(x / math.sqrt(2)))
    return K * math.exp(-r * T) * N(-d2) - S * N(-d1)


def select_option(symbol: str, option_type: str, target_dte: int = 30,
                  min_dte: int = 7) -> dict | None:
    """Pick the best expiry + ATM strike for a symbol.

    Returns {"expiry_date": "YYYY-MM-DD", "strike_price": float} or None.
    """
    try:
        from engine.market_data import yahoo_options_chain
        chain = yahoo_options_chain(symbol)
        if not chain:
            return None

        # Get available expiry dates (Unix timestamps)
        expiry_timestamps = chain.get("expirationDates", [])
        if not expiry_timestamps:
            return None

        # Current price from the chain quote
        quote = chain.get("quote", {})
        current_price = quote.get("regularMarketPrice", 0)
        if not current_price:
            return None

        # Convert timestamps to dates and find nearest to target_dte
        today = datetime.now().date()
        target_date = today + timedelta(days=target_dte)
        min_date = today + timedelta(days=min_dte)

        best_expiry = None
        best_diff = float("inf")
        best_ts = None

        for ts in expiry_timestamps:
            exp_date = datetime.utcfromtimestamp(ts).date()
            if exp_date < min_date:
                continue
            diff = abs((exp_date - target_date).days)
            if diff < best_diff:
                best_diff = diff
                best_expiry = exp_date
                best_ts = ts

        if not best_expiry:
            # Fallback: just pick the first expiry that's >= min_dte
            for ts in sorted(expiry_timestamps):
                exp_date = datetime.utcfromtimestamp(ts).date()
                if exp_date >= min_date:
                    best_expiry = exp_date
                    best_ts = ts
                    break

        if not best_expiry:
            return None

        # Now fetch the chain for that specific expiry to get strikes
        from engine.market_data import yahoo_options_chain_for_date
        dated_chain = yahoo_options_chain_for_date(symbol, best_ts)

        if dated_chain:
            options_list = dated_chain.get("calls" if option_type == "call" else "puts", [])
        else:
            # Fallback: use the default chain's options
            options_data = chain.get("options", [])
            if not options_data:
                return {"expiry_date": best_expiry.isoformat(), "strike_price": round(current_price, 2)}
            first_exp = options_data[0]
            options_list = first_exp.get("calls" if option_type == "call" else "puts", [])

        if not options_list:
            return {"expiry_date": best_expiry.isoformat(), "strike_price": round(current_price, 2)}

        # Find ATM or slightly ITM strike (prefer higher delta / less OTM risk)
        # For calls: ATM or 1 strike ITM (strike <= current_price)
        # For puts:  ATM or 1 strike ITM (strike >= current_price)
        prefer_itm = getattr(config, "OPTIONS_PREFER_ITM", True) if "config" in dir() else True
        try:
            import config as _cfg
            prefer_itm = getattr(_cfg, "OPTIONS_PREFER_ITM", True)
        except Exception:
            pass

        # Target delta 0.30-0.50 (ATM to slightly ITM)
        # For calls: strike slightly below current_price = higher delta
        # For puts:  strike slightly above current_price = higher delta
        # We use strike proximity as a delta proxy (ATM ≈ 0.50 delta)
        best_strike = None
        best_score = float("inf")
        option_premium = None
        best_bid_ask_ok = False
        best_iv = 0.0

        for opt in options_list:
            strike = opt.get("strike", 0)
            ask = opt.get("ask", 0)
            bid = opt.get("bid", 0)
            iv = opt.get("impliedVolatility", 0)
            delta = opt.get("delta", None)

            # Bid/ask spread filter: skip if spread > 10% of mid
            if ask > 0 and bid > 0:
                mid = (ask + bid) / 2
                spread_pct = (ask - bid) / mid if mid > 0 else 1.0
                if spread_pct > 0.10:
                    continue  # spread too wide
                option_premium_candidate = round(mid, 2)
                bid_ask_ok = True
            else:
                option_premium_candidate = opt.get("lastPrice", 0)
                bid_ask_ok = False

            # Delta targeting: prefer 0.30-0.50 delta
            # If delta not available, use strike proximity as proxy
            if delta is not None:
                delta_val = abs(float(delta))
                # Score: 0 = perfect (0.40 delta), penalty for outside 0.30-0.50
                if 0.30 <= delta_val <= 0.50:
                    delta_score = abs(delta_val - 0.40)
                else:
                    delta_score = abs(delta_val - 0.40) + 0.5  # penalty for out-of-range
            else:
                # Proxy: ATM = best (strike closest to current_price)
                diff = abs(strike - current_price)
                delta_score = diff / current_price if current_price > 0 else diff

                if prefer_itm:
                    if option_type == "call" and strike > current_price * 1.02:
                        delta_score += 0.5
                    elif option_type == "put" and strike < current_price * 0.98:
                        delta_score += 0.5

            # Prefer strikes with good bid/ask
            score = delta_score - (0.1 if bid_ask_ok else 0)

            if score < best_score:
                best_score = score
                best_strike = strike
                option_premium = option_premium_candidate if option_premium_candidate > 0 else None
                best_bid_ask_ok = bid_ask_ok
                best_iv = iv if iv and iv > 0.01 else 0.0

        if not best_strike:
            best_strike = round(current_price, 2)

        # BSM ceiling: reject if market premium > 1.5× theoretical fair value
        if option_premium and option_premium > 0 and best_strike and current_price > 0:
            _dte   = max((best_expiry - today).days, 0)
            _T     = _dte / 365.0
            _sigma = best_iv if best_iv > 0.01 else 0.45
            _fair  = (_bsm_call(current_price, best_strike, _T, _BSM_RISK_FREE, _sigma)
                      if option_type == "call" else
                      _bsm_put(current_price, best_strike, _T, _BSM_RISK_FREE, _sigma))
            if _fair > 0 and option_premium > _BSM_CEILING * _fair:
                logger.info(
                    f"Options ceiling: {symbol} {best_strike}{option_type[0].upper()} "
                    f"{_dte}DTE — premium ${option_premium:.2f} > "
                    f"{_BSM_CEILING}x fair ${_fair:.2f}, skipping"
                )
                console.log(
                    f"[red]Options ceiling: {symbol} ${best_strike}{option_type[0].upper()} "
                    f"{_dte}DTE — prem ${option_premium:.2f} > "
                    f"{_BSM_CEILING}x BSM ${_fair:.2f}[/red]"
                )
                return None

        # Earnings blackout: block if earnings within 3d of today OR within ±5d of expiry
        earnings_warning = False
        _earn_dt = _next_earnings_date(symbol)
        if _earn_dt is not None:
            _days_to_earn        = (_earn_dt - today).days
            _days_earn_to_expiry = abs((_earn_dt - best_expiry).days)
            if 0 <= _days_to_earn <= 3 or _days_earn_to_expiry <= 5:
                logger.info(
                    f"Earnings blackout: {symbol} earnings in {_days_to_earn}d "
                    f"(expiry {(best_expiry - today).days}d away), skipping option"
                )
                console.log(
                    f"[red]Earnings blackout: {symbol} earnings in {_days_to_earn}d "
                    f"— expiry {(best_expiry - today).days}d — skipping[/red]"
                )
                return None
            if 0 <= _days_to_earn <= 7:
                earnings_warning = True
                console.log(
                    f"[yellow]⚠ EARNINGS WARNING: {symbol} reports in {_days_to_earn}d "
                    f"— option sizing at risk[/yellow]"
                )

        result = {
            "expiry_date": best_expiry.isoformat(),
            "strike_price": round(best_strike, 2),
        }
        if option_premium and option_premium > 0:
            result["premium"] = round(option_premium, 2)
        if earnings_warning:
            result["earnings_warning"] = True

        dte = (best_expiry - today).days
        console.log(f"[dim]Options: {symbol} {option_type.upper()} ${best_strike} exp {best_expiry} ({dte}d){' ⚠EARN' if earnings_warning else ''}[/dim]")
        return result

    except Exception as e:
        console.log(f"[red]Options selector error for {symbol}: {e}")
        return None
