"""Volatility Surface Scanner — map IV across strikes to find mispriced options for 0DTE."""
from __future__ import annotations
import threading
import time
import math
from datetime import datetime
from engine.market_data import yahoo_options_chain
from rich.console import Console

console = Console()

_vol_cache: dict[str, dict] = {}
_vol_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def scan_vol_surface(symbol: str) -> dict | None:
    """Map implied volatility across strikes for the nearest expiry.

    Returns {symbol, expiry, spot, strikes: [{strike, call_iv, put_iv, call_vol, put_vol,
             moneyness, iv_rank}], skew, atm_iv, mean_iv, mispriced: [...]}.
    """
    now = time.time()
    with _vol_lock:
        cached = _vol_cache.get(symbol)
        if cached and (now - cached.get("_ts", 0)) < _CACHE_TTL:
            return {k: v for k, v in cached.items() if k != "_ts"}

    try:
        chain_data = yahoo_options_chain(symbol)
        if not chain_data:
            return None

        # Get spot price from quote
        quote = chain_data.get("quote", {})
        spot = quote.get("regularMarketPrice")
        if not spot:
            return None
        spot = float(spot)

        # Get available expiration dates
        exps = chain_data.get("expirationDates", [])
        if not exps:
            return None

        # Use nearest expiry
        options = chain_data.get("options", [])
        if not options:
            return None

        calls = options[0].get("calls", [])
        puts = options[0].get("puts", [])
        target_exp = datetime.fromtimestamp(exps[0]).strftime("%Y-%m-%d") if exps else ""

        if not calls and not puts:
            return None

        # Build strike-level data
        strikes_data = []
        all_call_ivs = []
        all_put_ivs = []

        # Merge calls and puts by strike — filter to +/- 10% of spot
        lower = spot * 0.90
        upper = spot * 1.10

        call_by_strike = {c["strike"]: c for c in calls if lower <= c["strike"] <= upper}
        put_by_strike = {p["strike"]: p for p in puts if lower <= p["strike"] <= upper}
        all_strikes = sorted(set(list(call_by_strike.keys()) + list(put_by_strike.keys())))

        for strike in all_strikes:
            call_row = call_by_strike.get(strike)
            put_row = put_by_strike.get(strike)

            call_iv = None
            call_vol = 0
            put_iv = None
            put_vol = 0

            if call_row:
                iv = call_row.get("impliedVolatility")
                if iv is not None and iv > 0 and not _is_nan(iv):
                    call_iv = round(float(iv) * 100, 2)
                    all_call_ivs.append(call_iv)
                call_vol = int(call_row.get("volume", 0) or 0)

            if put_row:
                iv = put_row.get("impliedVolatility")
                if iv is not None and iv > 0 and not _is_nan(iv):
                    put_iv = round(float(iv) * 100, 2)
                    all_put_ivs.append(put_iv)
                put_vol = int(put_row.get("volume", 0) or 0)

            moneyness = round((strike / spot - 1) * 100, 2)

            strikes_data.append({
                "strike": strike,
                "call_iv": call_iv,
                "put_iv": put_iv,
                "call_volume": call_vol,
                "put_volume": put_vol,
                "moneyness": moneyness,
            })

        if not strikes_data:
            return None

        # ATM IV (closest to spot)
        atm = min(strikes_data, key=lambda s: abs(s["strike"] - spot))
        atm_iv = atm.get("call_iv") or atm.get("put_iv") or 0

        # IV skew: put IV at -3%+ vs call IV at +3%+
        otm_puts = [s for s in strikes_data if s["moneyness"] <= -3 and s["put_iv"]]
        otm_calls = [s for s in strikes_data if s["moneyness"] >= 3 and s["call_iv"]]
        put_wing_iv = sum(s["put_iv"] for s in otm_puts) / len(otm_puts) if otm_puts else 0
        call_wing_iv = sum(s["call_iv"] for s in otm_calls) / len(otm_calls) if otm_calls else 0
        skew = round(put_wing_iv - call_wing_iv, 2) if put_wing_iv and call_wing_iv else 0

        # Find mispriced options: IV significantly below mean
        all_ivs = [iv for iv in all_call_ivs + all_put_ivs if iv > 0]
        mean_iv = sum(all_ivs) / len(all_ivs) if all_ivs else 0
        std_iv = (sum((iv - mean_iv) ** 2 for iv in all_ivs) / len(all_ivs)) ** 0.5 if len(all_ivs) > 1 else 0

        mispriced = []
        for s in strikes_data:
            for iv_key, opt_type in [("call_iv", "call"), ("put_iv", "put")]:
                iv = s.get(iv_key)
                if iv and mean_iv > 0 and std_iv > 0:
                    z_score = (iv - mean_iv) / std_iv
                    if z_score < -1.5:
                        mispriced.append({
                            "strike": s["strike"],
                            "type": opt_type,
                            "iv": iv,
                            "mean_iv": round(mean_iv, 2),
                            "z_score": round(z_score, 2),
                            "moneyness": s["moneyness"],
                        })

        # Rank each strike's IV
        for s in strikes_data:
            for iv_key in ["call_iv", "put_iv"]:
                iv = s.get(iv_key)
                if iv and all_ivs:
                    rank = sum(1 for x in all_ivs if x <= iv) / len(all_ivs) * 100
                    s[f"{iv_key}_rank"] = round(rank, 1)

        result = {
            "symbol": symbol,
            "expiry": target_exp,
            "spot": round(spot, 2),
            "strikes": strikes_data,
            "atm_iv": round(atm_iv, 2),
            "skew": skew,
            "mean_iv": round(mean_iv, 2),
            "mispriced": mispriced,
            "updated": datetime.now().isoformat(),
        }

        with _vol_lock:
            _vol_cache[symbol] = {**result, "_ts": now}

        return result

    except Exception as e:
        console.log(f"[red]Vol surface error for {symbol}: {e}")
        return None


def _is_nan(val) -> bool:
    try:
        return math.isnan(val)
    except (TypeError, ValueError):
        return val != val


def get_all_vol_surfaces() -> list:
    """Get vol surfaces for DayBlade tickers."""
    from engine.dayblade import DAYBLADE_TICKERS
    results = []
    for sym in DAYBLADE_TICKERS:
        surface = scan_vol_surface(sym)
        if surface:
            results.append(surface)
    return results
