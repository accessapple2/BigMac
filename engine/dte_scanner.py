"""0DTE Options Scanner — finds cheap and premium 0DTE opportunities."""
from __future__ import annotations

import threading
import time
from datetime import datetime, date
from rich.console import Console

console = Console()

DAYBLADE_TICKERS_EXPANDED = [
    "SPY", "QQQ", "TSLA", "NVDA", "IWM", "ORCL", "HIMS", "AMZN", "MSFT", "AAPL",
    "MU", "AMD", "META", "SLV", "NIO", "INTC", "PLTR", "IBIT", "RIVN", "SOFI",
    "GOOGL", "NFLX", "MSTR", "AVGO", "BABA",
]

_cache: dict = {"data": None, "ts": 0}
_cache_lock = threading.Lock()
_CACHE_TTL = 30  # 30 seconds


def _find_0dte_expiry(expiration_dates: list[int]) -> int | None:
    """Find today's expiry timestamp from a list of Unix timestamps. Returns None if no 0DTE."""
    today = date.today()
    for ts in expiration_dates:
        exp_date = datetime.utcfromtimestamp(ts).date()
        if exp_date == today:
            return ts
    return None


def _get_gex_magnets(symbol: str) -> list[float]:
    """Try to get GEX magnet strikes for proximity scoring. Returns empty list on failure."""
    try:
        from engine.gex_scanner import get_gex_magnets
        magnets = get_gex_magnets(symbol)
        return [m.get("strike", 0) for m in magnets if m.get("strike")]
    except Exception:
        return []


def _near_gex_magnet(strike: float, spot: float, magnet_strikes: list[float]) -> bool:
    """Check if strike is within 0.5% of any GEX magnet level."""
    if not magnet_strikes or spot <= 0:
        return False
    threshold = spot * 0.005  # 0.5%
    for magnet in magnet_strikes:
        if abs(strike - magnet) <= threshold:
            return True
    return False


def _compute_score(volume: int, iv: float, distance_pct: float, near_gex: bool) -> float:
    """Composite score = (volume / 1000) * (IV * 100) * (1 / (1 + distance_pct)), doubled if near GEX."""
    if volume <= 0 or iv <= 0:
        return 0.0
    score = (volume / 1000.0) * (iv * 100.0) * (1.0 / (1.0 + distance_pct))
    if near_gex:
        score *= 2.0
    return round(score, 4)


def _process_option_leg(opt: dict, spot: float, symbol: str, opt_type: str,
                        magnet_strikes: list[float]) -> dict | None:
    """Process a single option contract and return scored entry or None."""
    try:
        bid = opt.get("bid", 0) or 0
        ask = opt.get("ask", 0) or 0
        strike = opt.get("strike", 0) or 0
        volume = opt.get("volume", 0) or 0
        oi = opt.get("openInterest", 0) or 0
        iv = opt.get("impliedVolatility", 0) or 0
        last = opt.get("lastPrice", 0) or 0

        if ask <= 0 or bid < 0 or strike <= 0 or spot <= 0:
            return None

        mid = (bid + ask) / 2.0
        spread = ask - bid
        distance_pct = abs(strike - spot) / spot
        near_gex = _near_gex_magnet(strike, spot, magnet_strikes)
        score = _compute_score(volume, iv, distance_pct, near_gex)

        return {
            "symbol": symbol,
            "type": opt_type,
            "strike": strike,
            "bid": bid,
            "ask": ask,
            "mid": round(mid, 2),
            "spread": round(spread, 2),
            "last": last,
            "volume": volume,
            "oi": oi,
            "iv": round(iv, 4),
            "distance_pct": round(distance_pct, 4),
            "near_gex": near_gex,
            "score": score,
        }
    except Exception:
        return None


def scan_0dte_opportunities() -> dict:
    """Scan all tickers for 0DTE options. Returns {"cheap": [...], "premium": [...], "all_scored": [...], "scan_timestamp": ...}"""
    # Check cache
    with _cache_lock:
        if _cache["data"] and time.time() - _cache["ts"] < _CACHE_TTL:
            return _cache["data"]

    from engine.market_data import yahoo_options_chain, yahoo_options_chain_for_date

    cheap = []      # mid < $1.00, volume > 100, spread < $0.15
    premium = []    # $1.00 <= mid <= $5.00, OI > 500
    all_scored = []

    for symbol in DAYBLADE_TICKERS_EXPANDED:
        try:
            chain_data = yahoo_options_chain(symbol)
            if not chain_data:
                continue

            # Get spot price
            quote = chain_data.get("quote", {})
            spot = quote.get("regularMarketPrice", 0)
            if not spot:
                continue

            # Find 0DTE expiry
            exp_dates = chain_data.get("expirationDates", [])
            dte_ts = _find_0dte_expiry(exp_dates)
            if dte_ts is None:
                continue

            # Fetch the specific 0DTE chain
            options_data = yahoo_options_chain_for_date(symbol, dte_ts)
            if not options_data:
                continue

            # Get GEX magnets (best-effort)
            magnet_strikes = _get_gex_magnets(symbol)

            # Process calls and puts
            for opt_type, key in [("call", "calls"), ("put", "puts")]:
                for opt in options_data.get(key, []):
                    entry = _process_option_leg(opt, spot, symbol, opt_type, magnet_strikes)
                    if entry is None or entry["score"] <= 0:
                        continue

                    mid = entry["mid"]
                    spread = entry["spread"]
                    volume = entry["volume"]
                    oi = entry["oi"]

                    # Cheap tier: lottery tickets
                    if mid < 1.00 and volume > 100 and spread < 0.15:
                        cheap.append(entry)

                    # Premium tier: high OI near key levels
                    if 1.00 <= mid <= 5.00 and oi > 500:
                        premium.append(entry)

                    # All scored (any option with a positive score)
                    all_scored.append(entry)

        except Exception as e:
            console.print(f"[dim]0DTE scan skip {symbol}: {e}[/dim]")
            continue

    # Sort all lists by composite score descending
    cheap.sort(key=lambda x: x["score"], reverse=True)
    premium.sort(key=lambda x: x["score"], reverse=True)
    all_scored.sort(key=lambda x: x["score"], reverse=True)

    # Cap lists to keep payloads reasonable
    cheap = cheap[:25]
    premium = premium[:25]
    all_scored = all_scored[:50]

    result = {
        "cheap": cheap,
        "premium": premium,
        "all_scored": all_scored,
        "scan_timestamp": datetime.now().isoformat(),
        "tickers_scanned": len(DAYBLADE_TICKERS_EXPANDED),
    }

    # Update cache
    with _cache_lock:
        _cache["data"] = result
        _cache["ts"] = time.time()

    console.print(
        f"[green]0DTE scan complete:[/green] {len(cheap)} cheap, "
        f"{len(premium)} premium, {len(all_scored)} total scored"
    )

    return result


def get_top_scored(n: int = 3) -> list:
    """Return top N scored opportunities for DayBlade to trade."""
    data = scan_0dte_opportunities()
    return data.get("all_scored", [])[:n]
