"""Options Greeks Dashboard — live delta, theta, gamma, vega for every options position."""
from __future__ import annotations
import sqlite3
from datetime import datetime, timedelta
from engine.market_data import _is_yf_limited, _set_yf_limited
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_options_greeks(prices: dict) -> list:
    """Get live Greeks for all open options positions across all players.

    Uses yfinance options chain to find matching contracts and extract Greeks.
    Returns list of position dicts enriched with delta, gamma, theta, vega, theta_decay_daily.
    """
    conn = _conn()
    positions = conn.execute(
        "SELECT pos.player_id, p.display_name, pos.symbol, pos.qty, pos.avg_price, "
        "pos.option_type, pos.strike_price, pos.expiry_date, pos.opened_at "
        "FROM positions pos JOIN ai_players p ON pos.player_id = p.id "
        "WHERE pos.asset_type='option'"
    ).fetchall()
    conn.close()

    if not positions:
        return []

    # Cache options chains per symbol
    chains: dict = {}
    result = []

    for pos in positions:
        sym = pos["symbol"]
        opt_type = pos["option_type"] or "call"
        entry_price = pos["avg_price"]
        qty = pos["qty"]
        current_price = prices.get(sym, {}).get("price", entry_price)

        greeks = _fetch_greeks_for_position(sym, opt_type, current_price, pos["strike_price"], chains)

        # Theta decay per day in dollars
        theta = greeks.get("theta", 0) or 0
        theta_decay_daily = abs(theta) * qty * 100  # per contract (100 shares)

        # Days until expiry
        dte = 0
        if pos["expiry_date"]:
            try:
                exp = datetime.strptime(pos["expiry_date"], "%Y-%m-%d")
                dte = max(0, (exp - datetime.now()).days)
            except Exception:
                pass

        # Time value remaining
        intrinsic = 0
        if opt_type == "call":
            intrinsic = max(0, current_price - (pos["strike_price"] or current_price))
        else:
            intrinsic = max(0, (pos["strike_price"] or current_price) - current_price)
        time_value = max(0, current_price - intrinsic)

        result.append({
            "player_id": pos["player_id"],
            "display_name": pos["display_name"],
            "symbol": sym,
            "option_type": opt_type,
            "strike_price": pos["strike_price"],
            "expiry_date": pos["expiry_date"],
            "qty": qty,
            "avg_price": entry_price,
            "current_price": round(current_price, 2),
            "delta": greeks.get("delta"),
            "gamma": greeks.get("gamma"),
            "theta": greeks.get("theta"),
            "vega": greeks.get("vega"),
            "iv": greeks.get("iv"),
            "theta_decay_daily": round(theta_decay_daily, 2),
            "dte": dte,
            "time_value": round(time_value, 2),
            "unrealized_pnl": round((current_price - entry_price) * qty, 2),
            "unrealized_pnl_pct": round((current_price / entry_price - 1) * 100, 2) if entry_price > 0 else 0,
        })

    return result


def _fetch_greeks_for_position(symbol: str, option_type: str, spot: float,
                                strike: float | None, chains: dict) -> dict:
    """Try to get Greeks from yfinance options chain for the closest matching contract."""
    if _is_yf_limited():
        return {}
    try:
        if symbol not in chains:
            ticker = yf.Ticker(symbol)
            exps = ticker.options
            if not exps:
                return {}
            # Use nearest expiry
            chains[symbol] = {"ticker": ticker, "exps": exps}

        cached = chains[symbol]
        ticker = cached["ticker"]
        exps = cached["exps"]

        # Pick today's or nearest expiry
        today_str = datetime.now().strftime("%Y-%m-%d")
        target_exp = exps[0]
        for exp in exps:
            if exp >= today_str:
                target_exp = exp
                break

        chain = ticker.option_chain(target_exp)
        opts = chain.calls if option_type == "call" else chain.puts

        if opts.empty:
            return {}

        # Find closest strike to the position's strike (or ATM if no strike)
        target_strike = strike or spot
        opts = opts.copy()
        opts["strike_diff"] = abs(opts["strike"] - target_strike)
        closest = opts.loc[opts["strike_diff"].idxmin()]

        return {
            "delta": _safe_float(closest.get("delta")),
            "gamma": _safe_float(closest.get("gamma")),
            "theta": _safe_float(closest.get("theta")),
            "vega": _safe_float(closest.get("vega")),
            "iv": _safe_float(closest.get("impliedVolatility")),
        }
    except Exception as e:
        err = str(e)
        if "Too Many Requests" in err or "Rate" in err:
            _set_yf_limited()
        return {}


def _safe_float(val) -> float | None:
    if val is None:
        return None
    try:
        import pandas as pd
        if pd.isna(val):
            return None
        return round(float(val), 4)
    except (ValueError, TypeError):
        return None


def get_total_theta_burn() -> dict:
    """Calculate total theta burn across all options positions."""
    from engine.market_data import get_stock_price
    conn = _conn()
    symbols = conn.execute(
        "SELECT DISTINCT symbol FROM positions WHERE asset_type='option'"
    ).fetchall()
    conn.close()

    prices = {}
    for s in symbols:
        data = get_stock_price(s["symbol"])
        if "error" not in data:
            prices[s["symbol"]] = data

    greeks = get_options_greeks(prices)

    total_theta = sum(g.get("theta_decay_daily", 0) for g in greeks)
    by_player: dict[str, float] = {}
    for g in greeks:
        pid = g["player_id"]
        by_player[pid] = by_player.get(pid, 0) + g.get("theta_decay_daily", 0)

    return {
        "total_theta_daily": round(total_theta, 2),
        "by_player": {k: round(v, 2) for k, v in by_player.items()},
        "positions": len(greeks),
    }
