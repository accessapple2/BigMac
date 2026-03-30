"""Smart Risk Levels -- auto-calculate stop-loss, profit targets, trailing stops for every signal."""
from __future__ import annotations
import json
import sqlite3
from datetime import datetime
from rich.console import Console

import config

console = Console()
DB = "data/trader.db"


def calculate_risk_levels(symbol: str, entry_price: float, side: str = "BUY") -> dict:
    """Calculate stop-loss and profit targets based on ATR.
    ATR is calculated from recent daily candles via Yahoo.
    """
    from engine.market_data import _yahoo_chart

    # Get 20 days of daily data for ATR calculation
    chart = _yahoo_chart(symbol, interval="1d", range_="1mo")
    if not chart:
        # Fallback: use 2% of price as ATR estimate
        atr = entry_price * 0.02
    else:
        indicators = chart.get("indicators", {})
        quotes = indicators.get("quote", [{}])[0]
        highs = quotes.get("high", [])
        lows = quotes.get("low", [])
        closes = quotes.get("close", [])

        if len(highs) >= 14 and len(lows) >= 14 and len(closes) >= 14:
            # Calculate ATR(14)
            trs = []
            for i in range(1, min(len(highs), 21)):
                if highs[i] and lows[i] and closes[i - 1]:
                    tr = max(
                        highs[i] - lows[i],
                        abs(highs[i] - closes[i - 1]),
                        abs(lows[i] - closes[i - 1]),
                    )
                    trs.append(tr)
            atr = sum(trs[-14:]) / min(len(trs), 14) if trs else entry_price * 0.02
        else:
            atr = entry_price * 0.02

    atr = round(atr, 2)

    if side.upper() in ("BUY", "LONG"):
        stop_loss = round(entry_price - (atr * 1.5), 2)
        risk = entry_price - stop_loss
        target_1 = round(entry_price + (risk * 2), 2)  # 2:1 R/R
        target_2 = round(entry_price + (risk * 3), 2)  # 3:1 R/R
        trailing_stop = round(entry_price - atr, 2)
    else:
        stop_loss = round(entry_price + (atr * 1.5), 2)
        risk = stop_loss - entry_price
        target_1 = round(entry_price - (risk * 2), 2)
        target_2 = round(entry_price - (risk * 3), 2)
        trailing_stop = round(entry_price + atr, 2)

    return {
        "symbol": symbol,
        "entry_price": entry_price,
        "side": side.upper(),
        "atr": atr,
        "stop_loss": stop_loss,
        "target_1": target_1,
        "target_2": target_2,
        "trailing_stop": trailing_stop,
        "risk_per_share": round(risk, 2),
        "reward_risk_t1": 2.0,
        "reward_risk_t2": 3.0,
    }


def get_recent_signals_with_risk(limit: int = 20) -> list:
    """Get recent BUY signals with auto-calculated risk levels."""
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            """
            SELECT s.player_id, s.symbol, s.signal, s.confidence, s.reasoning, s.created_at,
                   p.display_name
            FROM signals s
            JOIN ai_players p ON p.id = s.player_id
            WHERE s.signal IN ('BUY', 'STRONG_BUY', 'SELL', 'STRONG_SELL')
            ORDER BY s.created_at DESC LIMIT ?
        """,
            (limit,),
        ).fetchall()
        conn.close()
    except Exception:
        return []

    results = []
    for r in rows:
        sig = dict(r)
        side = "BUY" if "BUY" in sig.get("signal", "") else "SELL"
        # Get current price for risk calc
        from engine.market_data import get_stock_price

        price_data = get_stock_price(sig["symbol"])
        if "error" not in price_data:
            risk = calculate_risk_levels(sig["symbol"], price_data["price"], side)
            sig["risk_levels"] = risk
            sig["current_price"] = price_data["price"]
        results.append(sig)
    return results
