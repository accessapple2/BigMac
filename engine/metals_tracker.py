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

# Map positions table symbols → metals_ledger metal name (lowercase)
POSITION_SYM_TO_LEDGER = {
    "GC=F": "gold", "GOLD": "gold",
    "SI=F": "silver", "SILVER": "silver",
    "PL=F": "platinum", "PA=F": "palladium",
}

# Map metals_ledger metal name → spot price key
LEDGER_TO_SPOT = {
    "gold": "GOLD", "silver": "SILVER",
    "platinum": "PLATINUM", "palladium": "PALLADIUM",
}


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
    """Get current metal holdings from metals_ledger (authoritative source)."""
    conn = _conn()
    rows = conn.execute(
        "SELECT metal, SUM(qty_oz) as qty, SUM(total_cost) as total_cost "
        "FROM metals_ledger GROUP BY metal"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        metal = r["metal"].lower()
        spot_key = LEDGER_TO_SPOT.get(metal, metal.upper())
        symbol = METAL_SYMBOLS.get(spot_key)
        if not symbol:
            continue
        qty = r["qty"] or 0.0
        tc = r["total_cost"] or 0.0
        avg_price = tc / qty if qty > 0 else 0.0
        result.append({
            "symbol": symbol,
            "qty": round(qty, 4),
            "avg_price": round(avg_price, 2),
            "asset_type": "metal",
            "opened_at": None,
        })
    return sorted(result, key=lambda x: x["symbol"])


def _get_ledger_costs() -> dict:
    """Get aggregated cost data from metals_ledger. Returns {metal: {total_cost, qty_oz}}."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT metal, SUM(total_cost) as total_cost, SUM(qty_oz) as qty_oz "
            "FROM metals_ledger GROUP BY metal"
        ).fetchall()
        conn.close()
        return {r["metal"]: {"total_cost": r["total_cost"], "qty_oz": r["qty_oz"]} for r in rows}
    except Exception:
        return {}


def get_portfolio() -> dict:
    """Get full portfolio with live valuations."""
    holdings = get_holdings()
    prices = get_spot_prices()

    # Prefer metals_ledger for real cost basis when available
    ledger_costs = _get_ledger_costs()

    positions = []
    metals_value = 0
    total_cost = 0

    for h in holdings:
        sym = h["symbol"]
        qty = h["qty"]
        avg = h["avg_price"]

        # Use ledger cost if available, otherwise fall back to positions.avg_price
        ledger_metal = POSITION_SYM_TO_LEDGER.get(sym)
        if ledger_metal and ledger_metal in ledger_costs:
            lc = ledger_costs[ledger_metal]
            cost = lc["total_cost"]
            avg = cost / lc["qty_oz"] if lc["qty_oz"] > 0 else avg
        else:
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


def get_dilithium_portfolio() -> dict:
    """Get Dilithium Reserve portfolio from metals_ledger with live spot prices.

    Returns structured data per metal (gold/silver) with real cost basis,
    live spot prices, P&L, and the full purchase history.
    """
    conn = _conn()
    agg_rows = conn.execute(
        "SELECT metal, SUM(qty_oz) as qty_oz, SUM(total_cost) as total_cost "
        "FROM metals_ledger GROUP BY metal"
    ).fetchall()
    purchase_rows = conn.execute(
        "SELECT id, purchase_date, metal, qty_oz, total_cost, cost_per_oz, source, notes "
        "FROM metals_ledger ORDER BY purchase_date ASC"
    ).fetchall()
    conn.close()

    prices = get_spot_prices()
    result: dict = {}
    total_invested = 0.0
    total_value = 0.0

    for r in agg_rows:
        metal = r["metal"].lower()
        qty = r["qty_oz"]
        tc = r["total_cost"]
        avg_cost = tc / qty if qty > 0 else 0.0

        spot_key = LEDGER_TO_SPOT.get(metal, metal.upper())
        spot_data = prices.get(spot_key, {})
        spot_price = spot_data.get("price", 0.0)
        current_value = qty * spot_price
        pnl = current_value - tc
        pnl_pct = (pnl / tc * 100) if tc > 0 else 0.0

        result[metal] = {
            "qty_oz": round(qty, 4),
            "total_cost": round(tc, 2),
            "avg_cost_per_oz": round(avg_cost, 2),
            "spot_price": round(spot_price, 2),
            "current_value": round(current_value, 2),
            "pnl": round(pnl, 2),
            "pnl_pct": round(pnl_pct, 2),
        }
        total_invested += tc
        total_value += current_value

    total_pnl = total_value - total_invested
    total_pnl_pct = (total_pnl / total_invested * 100) if total_invested > 0 else 0.0

    return {
        **result,
        "total_invested": round(total_invested, 2),
        "total_value": round(total_value, 2),
        "total_pnl": round(total_pnl, 2),
        "total_pnl_pct": round(total_pnl_pct, 2),
        "spot_prices": prices,
        "purchases": [dict(p) for p in purchase_rows],
    }


def add_ledger_purchase(data: dict) -> dict:
    """Add a new physical metal purchase to metals_ledger and return updated portfolio."""
    metal = (data.get("metal") or "").lower()
    if metal not in ("gold", "silver", "platinum", "palladium"):
        return {"error": f"Invalid metal '{metal}'. Use: gold, silver, platinum, palladium"}
    try:
        qty_oz = float(data.get("qty_oz", 0))
        total_cost = float(data.get("total_cost", 0))
    except (ValueError, TypeError):
        return {"error": "qty_oz and total_cost must be numbers"}
    if qty_oz <= 0 or total_cost <= 0:
        return {"error": "qty_oz and total_cost must be positive"}
    cost_per_oz = round(total_cost / qty_oz, 2)
    purchase_date = data.get("purchase_date") or datetime.now().strftime("%Y-%m-%d")
    source = data.get("source") or ""
    notes = data.get("notes")

    conn = _conn()
    conn.execute(
        "INSERT INTO metals_ledger (purchase_date, metal, qty_oz, total_cost, cost_per_oz, source, notes) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (purchase_date, metal, round(qty_oz, 4), round(total_cost, 2), cost_per_oz, source, notes)
    )
    conn.commit()
    conn.close()
    console.log(f"[bold yellow]Dilithium Ledger: Added {qty_oz} oz {metal} @ ${cost_per_oz:.2f}/oz from {source}")
    return get_dilithium_portfolio()


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
