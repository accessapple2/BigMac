"""Dalio Metals — Physical precious metals tracker with smart stacking advisor.

Tracks gold/silver holdings as a leaderboard-competing player.
Uses Yahoo Finance for live spot prices (GC=F for gold, SI=F for silver).
Provides buy/hold/reduce/sell signals based on macro regime, RSI, and
gold/silver ratio analysis.
"""
from __future__ import annotations
import os
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)
PLAYER_ID = "enterprise-computer"

# Metal symbols on Yahoo Finance
METAL_SYMBOLS = {
    "GOLD": "GC=F",
    "SILVER": "SI=F",
    "PLATINUM": "PL=F",
    "PALLADIUM": "PA=F",
}

# Reverse mapping: DB symbols (futures tickers) → spot price keys
SYMBOL_TO_SPOT = {v: k for k, v in METAL_SYMBOLS.items()}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def get_spot_prices(fresh: bool = False) -> dict:
    """Fetch live spot prices for gold, silver, platinum.

    Args:
        fresh: If True, bypass the price cache to get live data (use for API calls).
    """
    from engine.market_data import get_stock_price, _price_cache
    if fresh:
        for symbol in METAL_SYMBOLS.values():
            _price_cache.pop(symbol, None)
    prices = {}
    for metal, symbol in METAL_SYMBOLS.items():
        try:
            data = get_stock_price(symbol)
            if data and "price" in data:
                prices[metal] = {
                    "price": data["price"],
                    "change_pct": data.get("change_pct", 0),
                    "high": data.get("high", data["price"]),
                    "low": data.get("low", data["price"]),
                }
        except Exception:
            pass
    # Gold/Silver ratio
    if "GOLD" in prices and "SILVER" in prices and prices["SILVER"]["price"] > 0:
        prices["GSR"] = round(prices["GOLD"]["price"] / prices["SILVER"]["price"], 1)
    return prices


def get_holdings() -> list:
    """Get current metal holdings."""
    conn = _conn()
    rows = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, opened_at FROM positions "
        "WHERE player_id=? ORDER BY symbol", (PLAYER_ID,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_portfolio() -> dict:
    """Get full portfolio with live valuations."""
    holdings = get_holdings()
    prices = get_spot_prices()
    conn = _conn()
    player = conn.execute("SELECT cash FROM ai_players WHERE id=?", (PLAYER_ID,)).fetchone()
    conn.close()
    cash = player["cash"] if player else 0

    positions = []
    metals_value = 0
    total_cost = 0

    for h in holdings:
        sym = h["symbol"]
        qty = h["qty"]
        avg = h["avg_price"]
        cost = qty * avg
        total_cost += cost

        # Map position symbol (GC=F/SI=F) to spot price key (GOLD/SILVER)
        spot_key = SYMBOL_TO_SPOT.get(sym, sym)
        spot_data = prices.get(spot_key, {})
        current = spot_data.get("price", avg)
        change = spot_data.get("change_pct", 0)
        mkt_val = qty * current
        pnl = mkt_val - cost
        pnl_pct = ((current / avg) - 1) * 100 if avg > 0 else 0
        metals_value += mkt_val

        positions.append({
            "symbol": sym,
            "qty": qty,
            "avg_price": round(avg, 2),
            "current_price": round(current, 2),
            "market_value": round(mkt_val, 2),
            "cost_basis": round(cost, 2),
            "unrealized_pnl": round(pnl, 2),
            "unrealized_pnl_pct": round(pnl_pct, 2),
            "day_change_pct": round(change, 2),
            "unit": "oz",
            "asset_type": "metal",
        })

    # Return based on metals cost vs metals value (no cash — physical holdings only)
    total_unrealized = metals_value - total_cost
    return_pct = round(total_unrealized / total_cost * 100, 2) if total_cost > 0 else 0

    return {
        "player_id": PLAYER_ID,
        "cash": 0,
        "positions": positions,
        "total_value": round(metals_value, 2),
        "total_cost_basis": round(total_cost, 2),
        "total_unrealized_pnl": round(total_unrealized, 2),
        "return_pct": return_pct,
        "spot_prices": prices,
    }


def get_stacking_signal() -> dict:
    """Smart stacking advisor — buy/hold/reduce/sell signals for gold and silver.

    Factors:
    1. Gold/Silver Ratio (GSR): >80 = silver undervalued (buy silver), <65 = gold undervalued (buy gold)
    2. VIX regime: >25 = accumulate (fear = metals rally), <15 = reduce (complacency)
    3. DXY (dollar): strong dollar = headwind, weak dollar = tailwind
    4. RSI of gold/silver: oversold (<30) = buy, overbought (>70) = trim
    """
    prices = get_spot_prices()
    signals = {}

    # VIX
    vix_price = 20
    try:
        from engine.vix_monitor import get_vix_status
        vix = get_vix_status()
        if vix and vix.get("price"):
            vix_price = vix["price"]
    except Exception:
        pass

    # Gold/Silver Ratio
    gsr = prices.get("GSR", 75)

    # Base signals
    for metal in ["GOLD", "SILVER"]:
        spot = prices.get(metal, {})
        price = spot.get("price", 0)
        change = spot.get("change_pct", 0)

        signal = "HOLD"
        conviction = 5
        reasons = []

        # VIX regime
        if vix_price >= 30:
            signal = "BUY"
            conviction += 2
            reasons.append(f"VIX at {vix_price:.0f} — extreme fear favors metals")
        elif vix_price >= 25:
            conviction += 1
            reasons.append(f"VIX elevated at {vix_price:.0f} — mild tailwind")
        elif vix_price < 15:
            signal = "REDUCE"
            conviction -= 1
            reasons.append(f"VIX low at {vix_price:.0f} — complacency, metals may drift")

        # GSR signals
        if metal == "SILVER" and gsr > 80:
            signal = "BUY"
            conviction += 2
            reasons.append(f"GSR at {gsr} — silver historically undervalued vs gold")
        elif metal == "SILVER" and gsr > 75:
            conviction += 1
            reasons.append(f"GSR at {gsr} — silver relatively cheap")
        elif metal == "GOLD" and gsr < 65:
            signal = "BUY"
            conviction += 1
            reasons.append(f"GSR at {gsr} — gold relatively undervalued")

        # Momentum
        if change <= -2:
            conviction += 1
            reasons.append(f"Down {change:.1f}% today — accumulation zone but watch the knife")
        elif change >= 3:
            if signal != "BUY":
                signal = "HOLD"
            reasons.append(f"Up {change:.1f}% today — strong but extended")

        # Falling knife guard: if price is actively falling, don't say BUY —
        # a falling knife in the accumulation zone is still a falling knife
        if change < 0 and signal == "BUY":
            signal = "WAIT"
            conviction = max(conviction - 1, 1)
            reasons.append(f"Price falling ({change:+.1f}%) — wait for stabilization before buying")

        conviction = max(1, min(10, conviction))

        signals[metal] = {
            "signal": signal,
            "conviction": conviction,
            "price": price,
            "change_pct": change,
            "reasons": reasons,
        }

    return {
        "signals": signals,
        "gsr": gsr,
        "vix": vix_price,
        "timestamp": datetime.now().isoformat(),
    }


def add_metal(symbol: str, qty: float, price: float) -> dict:
    """Add physical metal to inventory."""
    symbol = symbol.upper()
    if symbol not in METAL_SYMBOLS:
        return {"error": f"Unknown metal: {symbol}. Use: {', '.join(METAL_SYMBOLS.keys())}"}

    conn = _conn()
    existing = conn.execute(
        "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=?",
        (PLAYER_ID, symbol)
    ).fetchone()

    if existing:
        old_qty = existing["qty"]
        old_avg = existing["avg_price"]
        new_qty = old_qty + qty
        new_avg = ((old_qty * old_avg) + (qty * price)) / new_qty
        conn.execute(
            "UPDATE positions SET qty=?, avg_price=? WHERE player_id=? AND symbol=?",
            (round(new_qty, 4), round(new_avg, 2), PLAYER_ID, symbol)
        )
    else:
        conn.execute(
            "INSERT INTO positions (player_id, symbol, qty, avg_price, asset_type) "
            "VALUES (?, ?, ?, ?, 'metal')",
            (PLAYER_ID, symbol, qty, price)
        )

    # Deduct from cash
    cost = qty * price
    conn.execute(
        "UPDATE ai_players SET cash = cash - ? WHERE id=?",
        (round(cost, 2), PLAYER_ID)
    )
    conn.commit()
    conn.close()
    console.log(f"[bold yellow]Dalio Metals: Added {qty} oz {symbol} @ ${price:.2f}")
    return {"ok": True, "symbol": symbol, "qty": qty, "price": price}


def remove_metal(symbol: str, qty: float, price: float) -> dict:
    """Remove/sell physical metal from inventory."""
    symbol = symbol.upper()
    conn = _conn()
    existing = conn.execute(
        "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=?",
        (PLAYER_ID, symbol)
    ).fetchone()

    if not existing or existing["qty"] < qty:
        conn.close()
        return {"error": f"Not enough {symbol}. Have: {existing['qty'] if existing else 0} oz"}

    proceeds = qty * price
    new_qty = existing["qty"] - qty

    if new_qty <= 0:
        conn.execute("DELETE FROM positions WHERE player_id=? AND symbol=?", (PLAYER_ID, symbol))
    else:
        conn.execute(
            "UPDATE positions SET qty=? WHERE player_id=? AND symbol=?",
            (round(new_qty, 4), PLAYER_ID, symbol)
        )

    conn.execute(
        "UPDATE ai_players SET cash = cash + ? WHERE id=?",
        (round(proceeds, 2), PLAYER_ID)
    )
    conn.commit()
    conn.close()
    return {"ok": True, "symbol": symbol, "qty_sold": qty, "price": price, "proceeds": round(proceeds, 2)}


def set_cost_basis(symbol: str, cost_per_oz: float) -> dict:
    """Update the cost basis for a metal position."""
    symbol = symbol.upper()
    if symbol not in METAL_SYMBOLS:
        return {"error": f"Unknown metal: {symbol}. Use: {', '.join(METAL_SYMBOLS.keys())}"}
    conn = _conn()
    existing = conn.execute(
        "SELECT qty FROM positions WHERE player_id=? AND symbol=?", (PLAYER_ID, symbol)
    ).fetchone()
    if not existing:
        conn.close()
        return {"error": f"No {symbol} position found"}
    conn.execute(
        "UPDATE positions SET avg_price=? WHERE player_id=? AND symbol=?",
        (round(cost_per_oz, 2), PLAYER_ID, symbol)
    )
    conn.commit()
    conn.close()
    console.log(f"[yellow]Dalio Metals: {symbol} cost basis set to ${cost_per_oz:.2f}/oz")
    return {"ok": True, "symbol": symbol, "cost_basis_per_oz": round(cost_per_oz, 2)}
