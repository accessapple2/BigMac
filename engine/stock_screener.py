"""Stock Screener — filter WATCH_STOCKS by fundamentals with caching."""
from __future__ import annotations
import json
import time
from datetime import datetime
from pathlib import Path
from rich.console import Console

import config

console = Console()
DATA_FILE = Path("data/stock_fundamentals.json")
CACHE_TTL = 3600  # 1 hour


def screen_stocks(
    min_pe: float = None,
    max_pe: float = None,
    min_short_float: float = None,
    max_short_float: float = None,
    min_rel_volume: float = None,
    consensus: str = None,
    has_insider_buying: bool = None,
    earnings_within_days: int = None,
) -> list:
    """Screen WATCH_STOCKS by fundamental criteria. All parameters are optional.

    Args:
        min_pe: Minimum trailing P/E ratio.
        max_pe: Maximum trailing P/E ratio.
        min_short_float: Minimum short interest as % of float.
        max_short_float: Maximum short interest as % of float.
        min_rel_volume: Minimum relative volume (current vs 10-day avg).
        consensus: Analyst consensus filter ("buy", "hold", "sell", "strongBuy").
        has_insider_buying: If True, only stocks with recent insider purchases.
        earnings_within_days: Only stocks with earnings within N days.

    Returns:
        List of dicts with symbol, fundamentals, and match details.
    """
    fundamentals = _load_cached_fundamentals()
    if not fundamentals:
        fundamentals = refresh_fundamentals()

    results = []
    for symbol, data in fundamentals.items():
        if not data or "error" in data:
            continue

        info = data.get("info", {})
        match_reasons = []

        # P/E filter
        pe = info.get("trailingPE")
        if pe is not None:
            if min_pe is not None and pe < min_pe:
                continue
            if max_pe is not None and pe > max_pe:
                continue
            if min_pe is not None or max_pe is not None:
                match_reasons.append(f"P/E: {pe:.1f}")

        # Short float filter
        short_pct = info.get("shortPercentOfFloat")
        if short_pct is not None:
            short_pct_val = short_pct * 100 if short_pct < 1 else short_pct
            if min_short_float is not None and short_pct_val < min_short_float:
                continue
            if max_short_float is not None and short_pct_val > max_short_float:
                continue
            if min_short_float is not None or max_short_float is not None:
                match_reasons.append(f"Short%: {short_pct_val:.1f}%")

        # Relative volume filter
        avg_vol = info.get("averageVolume10days") or info.get("averageVolume")
        curr_vol = info.get("regularMarketVolume") or info.get("volume")
        if avg_vol and curr_vol and avg_vol > 0:
            rel_vol = curr_vol / avg_vol
            if min_rel_volume is not None and rel_vol < min_rel_volume:
                continue
            if min_rel_volume is not None:
                match_reasons.append(f"RelVol: {rel_vol:.2f}x")

        # Consensus filter
        rec_key = info.get("recommendationKey", "")
        if consensus is not None:
            if rec_key.lower() != consensus.lower():
                continue
            match_reasons.append(f"Consensus: {rec_key}")

        # Insider buying filter
        if has_insider_buying is True:
            insider_purchases = data.get("insider_purchases", [])
            if not insider_purchases:
                continue
            match_reasons.append(f"Insider buys: {len(insider_purchases)}")

        # Earnings proximity filter
        if earnings_within_days is not None:
            earnings_date = info.get("earningsDate")
            if earnings_date:
                # earningsDate can be a list of timestamps
                if isinstance(earnings_date, (list, tuple)) and earnings_date:
                    next_earn = earnings_date[0]
                else:
                    next_earn = earnings_date
                try:
                    if isinstance(next_earn, (int, float)):
                        earn_dt = datetime.fromtimestamp(next_earn)
                    else:
                        earn_dt = datetime.fromisoformat(str(next_earn))
                    days_away = (earn_dt - datetime.now()).days
                    if days_away < 0 or days_away > earnings_within_days:
                        continue
                    match_reasons.append(f"Earnings in {days_away}d")
                except (ValueError, TypeError, OSError):
                    continue
            else:
                continue

        # Build result entry
        results.append({
            "symbol": symbol,
            "price": info.get("regularMarketPrice") or info.get("currentPrice"),
            "pe_trailing": info.get("trailingPE"),
            "pe_forward": info.get("forwardPE"),
            "market_cap": info.get("marketCap"),
            "short_pct_float": info.get("shortPercentOfFloat"),
            "recommendation": rec_key,
            "sector": info.get("sector", "Unknown"),
            "industry": info.get("industry", "Unknown"),
            "match_reasons": match_reasons,
            "updated": data.get("fetched_at", ""),
        })

    # Sort by number of match reasons (more criteria matched = more relevant)
    results.sort(key=lambda x: len(x["match_reasons"]), reverse=True)
    return results


def refresh_fundamentals() -> dict:
    """Force refresh fundamentals for all WATCH_STOCKS via yfinance.

    Returns dict keyed by symbol with info and insider data.
    """
    try:
        import yfinance as yf
    except ImportError:
        return {"error": "yfinance not installed"}

    fundamentals = {}
    for symbol in config.WATCH_STOCKS:
        try:
            ticker = yf.Ticker(symbol)
            info = ticker.info or {}

            # Fetch insider purchases
            insider_purchases = []
            try:
                purchases = ticker.insider_purchases
                if purchases is not None and not purchases.empty:
                    insider_purchases = purchases.head(10).to_dict("records")
            except Exception:
                pass

            fundamentals[symbol] = {
                "info": info,
                "insider_purchases": insider_purchases,
                "fetched_at": datetime.now().isoformat(),
            }
        except Exception as e:
            console.log(f"[red]Fundamentals fetch error for {symbol}: {e}")
            fundamentals[symbol] = {"error": str(e), "fetched_at": datetime.now().isoformat()}

    # Save to cache
    _save_fundamentals(fundamentals)
    return fundamentals


# ── Cache Persistence ────────────────────────────────────────────────

def _load_cached_fundamentals() -> dict:
    """Load fundamentals from JSON cache if within TTL."""
    try:
        if not DATA_FILE.exists():
            return {}
        data = json.loads(DATA_FILE.read_text())
        # Check TTL on first entry
        first_entry = next(iter(data.values()), None) if data else None
        if first_entry:
            fetched_at = first_entry.get("fetched_at", "")
            try:
                fetched_dt = datetime.fromisoformat(fetched_at)
                age_seconds = (datetime.now() - fetched_dt).total_seconds()
                if age_seconds > CACHE_TTL:
                    return {}  # Cache expired
            except (ValueError, TypeError):
                return {}
        return data
    except Exception as e:
        console.log(f"[red]Error loading fundamentals cache: {e}")
        return {}


def _save_fundamentals(fundamentals: dict):
    """Save fundamentals to JSON cache."""
    try:
        DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
        # Serialize — yfinance info can contain non-serializable types
        clean = {}
        for sym, data in fundamentals.items():
            clean[sym] = _make_serializable(data)
        DATA_FILE.write_text(json.dumps(clean, indent=2, default=str))
    except Exception as e:
        console.log(f"[red]Error saving fundamentals cache: {e}")


def _make_serializable(obj):
    """Recursively convert non-serializable types."""
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [_make_serializable(v) for v in obj]
    elif isinstance(obj, (int, float, str, bool, type(None))):
        return obj
    else:
        return str(obj)
