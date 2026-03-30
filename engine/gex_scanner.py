"""GEX (Gamma Exposure) Scanner — scrape CBOE delayed options data and compute GEX per strike.

Formula: GEX_per_strike = spot_price * gamma * open_interest * 100 * spot_price * 0.01
  - Calls contribute positive GEX (dealer long gamma → sell into rallies, buy dips = "pin")
  - Puts contribute negative GEX (dealer short gamma → amplify moves)

Top 3 absolute-GEX strikes are "magnets" — key support/resistance.
"""
from __future__ import annotations
import time
import threading
import requests
from datetime import datetime
from rich.console import Console

console = Console()

GEX_TICKERS = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL"]
CBOE_URL = "https://cdn.cboe.com/api/global/delayed_quotes/options/{ticker}.json"

# In-memory cache: {ticker: {data: ..., ts: float}}
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()
CACHE_TTL = 900  # 15 minutes


def _fetch_cboe_chain(ticker: str) -> dict | None:
    """Fetch delayed options chain from CBOE CDN. Returns raw JSON or None."""
    try:
        url = CBOE_URL.format(ticker=ticker.upper())
        resp = requests.get(url, timeout=15, headers={
            "User-Agent": "Mozilla/5.0 (TradeMinds/1.0)",
            "Accept": "application/json",
        })
        if resp.status_code != 200:
            console.log(f"[red]GEX: CBOE returned {resp.status_code} for {ticker}")
            return None
        return resp.json()
    except Exception as e:
        console.log(f"[red]GEX fetch error for {ticker}: {e}")
        return None


def _parse_gex(raw: dict, ticker: str) -> dict | None:
    """Parse CBOE JSON and compute GEX per strike.

    Returns {ticker, spot, strikes: [{strike, call_gex, put_gex, net_gex, call_oi, put_oi, call_gamma, put_gamma}],
             magnets: [{strike, net_gex, type}], total_gex, updated}.
    """
    try:
        data = raw.get("data", {})
        spot = None

        # Try to extract spot price
        if "current_price" in data:
            spot = float(data["current_price"])
        elif "close" in data:
            spot = float(data["close"])

        options = data.get("options", [])
        if not options:
            return None

        # If spot wasn't found in the top-level data, try getting it from the
        # underlying quote or fall back to the ATM region mid-price.
        if spot is None or spot <= 0:
            try:
                from engine.market_data import get_stock_price
                price_data = get_stock_price(ticker)
                if "error" not in price_data:
                    spot = price_data["price"]
            except Exception:
                pass

        if spot is None or spot <= 0:
            return None

        # Aggregate by strike
        strikes_map: dict[float, dict] = {}

        import re

        for opt in options:
            option_sym = opt.get("option", "")
            gamma = opt.get("gamma")
            oi = opt.get("open_interest")

            if gamma is None or oi is None:
                continue

            try:
                gamma = float(gamma)
                oi = int(float(oi))
            except (ValueError, TypeError):
                continue

            if oi <= 0 or gamma <= 0:
                continue

            # Parse strike and call/put from OCC symbol: e.g. SPY260310C00570000
            # Format: TICKER + YYMMDD + C/P + 8-digit strike (price * 1000)
            strike = opt.get("strike")
            is_call = True
            occ_match = re.search(r'([CP])(\d{8})$', option_sym)
            if occ_match:
                is_call = occ_match.group(1) == 'C'
                if strike is None:
                    strike = int(occ_match.group(2)) / 1000
            elif strike is None:
                continue

            try:
                strike = float(strike)
            except (ValueError, TypeError):
                continue

            # GEX = spot * gamma * OI * 100 * spot * 0.01
            gex = spot * gamma * oi * 100 * spot * 0.01

            if strike not in strikes_map:
                strikes_map[strike] = {
                    "strike": strike,
                    "call_gex": 0.0, "put_gex": 0.0,
                    "call_oi": 0, "put_oi": 0,
                    "call_gamma": 0.0, "put_gamma": 0.0,
                }

            if is_call:
                strikes_map[strike]["call_gex"] += gex
                strikes_map[strike]["call_oi"] += oi
                strikes_map[strike]["call_gamma"] += gamma
            else:
                strikes_map[strike]["put_gex"] -= gex  # Puts are negative GEX
                strikes_map[strike]["put_oi"] += oi
                strikes_map[strike]["put_gamma"] += gamma

        if not strikes_map:
            return None

        # Compute net GEX and build list
        strikes_list = []
        for s_data in strikes_map.values():
            s_data["net_gex"] = round(s_data["call_gex"] + s_data["put_gex"], 2)
            s_data["call_gex"] = round(s_data["call_gex"], 2)
            s_data["put_gex"] = round(s_data["put_gex"], 2)
            strikes_list.append(s_data)

        # Sort by strike
        strikes_list.sort(key=lambda x: x["strike"])

        # Filter to strikes within ~10% of spot for relevance
        lower = spot * 0.90
        upper = spot * 1.10
        relevant = [s for s in strikes_list if lower <= s["strike"] <= upper]
        if not relevant:
            relevant = strikes_list

        # Top 3 primary magnets by absolute net GEX
        sorted_by_gex = sorted(relevant, key=lambda x: abs(x["net_gex"]), reverse=True)
        magnets = sorted_by_gex[:3]
        magnet_data = []
        for m in magnets:
            gex_type = "call_wall" if m["net_gex"] > 0 else "put_wall"
            magnet_data.append({
                "strike": m["strike"],
                "net_gex": m["net_gex"],
                "type": gex_type,
                "tier": "primary",
            })

        # Secondary GEX levels (next 3 after primary)
        secondary = sorted_by_gex[3:6]
        secondary_data = []
        for m in secondary:
            gex_type = "call_wall" if m["net_gex"] > 0 else "put_wall"
            secondary_data.append({
                "strike": m["strike"],
                "net_gex": m["net_gex"],
                "type": gex_type,
                "tier": "secondary",
            })

        total_gex = sum(s["net_gex"] for s in relevant)

        return {
            "ticker": ticker,
            "spot": spot,
            "strikes": relevant,
            "magnets": magnet_data,
            "secondary_levels": secondary_data,
            "total_gex": round(total_gex, 2),
            "updated": datetime.now().isoformat(),
        }

    except Exception as e:
        console.log(f"[red]GEX parse error for {ticker}: {e}")
        return None


# ── Public API ────────────────────────────────────────────────────────

def get_gex(ticker: str, force: bool = False) -> dict | None:
    """Get GEX data for a ticker, using cache if fresh enough."""
    ticker = ticker.upper()
    now = time.time()

    with _cache_lock:
        cached = _cache.get(ticker)
        if cached and not force and (now - cached["ts"]) < CACHE_TTL:
            return cached["data"]

    # Fetch fresh
    raw = _fetch_cboe_chain(ticker)
    if raw is None:
        return None

    parsed = _parse_gex(raw, ticker)
    if parsed is None:
        return None

    with _cache_lock:
        _cache[ticker] = {"data": parsed, "ts": now}

    return parsed


def get_all_gex(force: bool = False) -> list:
    """Get GEX data for all GEX tickers."""
    results = []
    for ticker in GEX_TICKERS:
        gex = get_gex(ticker, force=force)
        if gex:
            results.append(gex)
    return results


def get_gex_magnets(ticker: str) -> list:
    """Get just the top 3 magnet strikes for a ticker."""
    gex = get_gex(ticker)
    if gex:
        return gex.get("magnets", [])
    return []


def build_gex_prompt_section(symbol: str) -> str:
    """Build a text block for injection into DayBlade's prompt."""
    gex = get_gex(symbol)
    if not gex or not gex.get("magnets"):
        return ""

    lines = [f"=== GEX LEVELS for {symbol} (Gamma Exposure) ==="]
    lines.append(f"Spot: ${gex['spot']:.2f} | Total Net GEX: {gex['total_gex']:+,.0f}")
    lines.append("")

    lines.append("PRIMARY LEVELS:")
    for i, m in enumerate(gex["magnets"], 1):
        label = "CALL WALL (support/pin)" if m["type"] == "call_wall" else "PUT WALL (resistance/accelerator)"
        lines.append(f"  Magnet #{i}: ${m['strike']:.2f} — {label} (GEX: {m['net_gex']:+,.0f})")

    secondary = gex.get("secondary_levels", [])
    if secondary:
        lines.append("SECONDARY LEVELS:")
        for i, m in enumerate(secondary, 4):
            label = "call wall" if m["type"] == "call_wall" else "put wall"
            lines.append(f"  Level #{i}: ${m['strike']:.2f} — {label} (GEX: {m['net_gex']:+,.0f})")

    lines.append("")
    lines.append("GEX interpretation:")
    lines.append("- Positive GEX (call walls) = dealers sell into rallies, buy dips → price tends to PIN near these levels")
    lines.append("- Negative GEX (put walls) = dealers amplify moves → expect momentum/volatility near these levels")
    lines.append("- Trade WITH the gamma: fade moves toward call walls, ride momentum through put walls")

    return "\n".join(lines)


def refresh_gex_cache():
    """Called by scheduler to pre-warm the cache during market hours."""
    from engine.risk_manager import RiskManager
    if not RiskManager.is_market_hours():
        return
    console.log("[cyan]GEX: Refreshing cache...")
    results = get_all_gex(force=True)
    console.log(f"[cyan]GEX: Refreshed {len(results)} tickers")
    for r in results:
        magnets = r.get("magnets", [])
        if magnets:
            mag_str = ", ".join(f"${m['strike']:.0f}" for m in magnets)
            console.log(f"[dim]  {r['ticker']}: magnets at {mag_str}[/dim]")
