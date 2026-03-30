"""Smart Risk Levels — calculate entry, stop, targets, swing exit for open positions."""
from __future__ import annotations
import sqlite3
from engine.market_data import _is_yf_limited, _set_yf_limited
import pandas as pd
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def get_risk_levels(prices: dict = None) -> list:
    """Calculate risk levels for all open positions across all players.

    For each position:
    - entry: avg_price
    - stop_loss: -12% from entry (hard stop)
    - target_1: 2:1 R/R (risk = entry - stop, target = entry + 2*risk)
    - target_2: 3:1 R/R (entry + 3*risk)
    - swing_exit: 10-day low
    - current_price: live price
    - r_multiple: current gain expressed as R
    """
    conn = _conn()
    positions = conn.execute("""
        SELECT p.player_id, a.display_name, p.symbol, p.qty, p.avg_price,
               p.asset_type, p.option_type
        FROM positions p
        JOIN ai_players a ON p.player_id = a.id
        WHERE p.qty > 0
    """).fetchall()
    conn.close()

    results = []
    # Cache 10-day lows per symbol
    swing_lows = {}

    for pos in positions:
        sym = pos["symbol"]
        entry = pos["avg_price"]
        asset_type = pos["asset_type"] or "stock"

        # Get current price
        current_price = entry
        if prices and sym in prices:
            current_price = prices[sym].get("price", entry)

        # Calculate risk (12% stop for stocks, premium for options)
        if asset_type == "option":
            risk = entry  # Max loss = premium paid
            stop_loss = 0.0
        else:
            stop_loss = round(entry * 0.88, 2)  # -12%
            risk = entry - stop_loss

        # Targets based on R/R
        target_1 = round(entry + risk * 2, 2) if risk > 0 else entry * 1.24
        target_2 = round(entry + risk * 3, 2) if risk > 0 else entry * 1.36

        # 10-day low for swing exit
        if sym not in swing_lows:
            if _is_yf_limited():
                swing_lows[sym] = None
            else:
                try:
                    ticker = yf.Ticker(sym)
                    hist = ticker.history(period="15d", interval="1d")
                    if len(hist) >= 10:
                        swing_lows[sym] = round(float(hist["Low"].tail(10).min()), 2)
                    elif not hist.empty:
                        swing_lows[sym] = round(float(hist["Low"].min()), 2)
                    else:
                        swing_lows[sym] = None
                except Exception as e:
                    err = str(e)
                    if "Too Many Requests" in err or "Rate" in err:
                        _set_yf_limited()
                    swing_lows[sym] = None

        swing_exit = swing_lows.get(sym)

        # R-multiple: how many R's of profit/loss
        if risk > 0:
            r_multiple = round((current_price - entry) / risk, 2)
        else:
            r_multiple = 0.0

        # Distance to each level as %
        pnl_pct = round((current_price / entry - 1) * 100, 2) if entry > 0 else 0

        results.append({
            "player_id": pos["player_id"],
            "display_name": pos["display_name"],
            "symbol": sym,
            "qty": pos["qty"],
            "asset_type": asset_type,
            "option_type": pos["option_type"],
            "entry": round(entry, 2),
            "current_price": round(current_price, 2),
            "pnl_pct": pnl_pct,
            "stop_loss": stop_loss,
            "target_1": round(target_1, 2),
            "target_2": round(target_2, 2),
            "swing_exit": swing_exit,
            "risk_per_share": round(risk, 2),
            "r_multiple": r_multiple,
            "levels": [
                {"label": "Stop Loss", "price": stop_loss, "color": "#f85149"},
                {"label": "Entry", "price": round(entry, 2), "color": "#8b949e"},
                {"label": "Target 1 (2R)", "price": round(target_1, 2), "color": "#3fb950"},
                {"label": "Target 2 (3R)", "price": round(target_2, 2), "color": "#58a6ff"},
                {"label": "Swing Exit", "price": swing_exit, "color": "#f0883e"},
            ],
        })

    return results


def get_levels_for_symbol(symbol: str, prices: dict = None) -> list:
    """Get risk levels for a specific symbol (all players holding it)."""
    all_levels = get_risk_levels(prices)
    return [l for l in all_levels if l["symbol"] == symbol.upper()]
