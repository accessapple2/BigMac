"""Options flow detection — track when AI options trades align with unusual volume."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from engine.market_data import yahoo_options_chain
from rich.console import Console

console = Console()
DB = "data/trader.db"


def get_options_volume(symbol: str) -> dict | None:
    """Get options volume data for a symbol using Yahoo direct HTTP.

    Returns {symbol, total_call_vol, total_put_vol, put_call_ratio,
             unusual_activity: bool, flow_direction: str}.
    """
    try:
        chain_data = yahoo_options_chain(symbol)
        if not chain_data:
            return None

        options = chain_data.get("options", [])
        if not options:
            return None

        calls = options[0].get("calls", [])
        puts = options[0].get("puts", [])

        total_call_vol = sum(c.get("volume", 0) or 0 for c in calls)
        total_put_vol = sum(p.get("volume", 0) or 0 for p in puts)
        total_call_oi = sum(c.get("openInterest", 0) or 0 for c in calls)
        total_put_oi = sum(p.get("openInterest", 0) or 0 for p in puts)

        total_vol = total_call_vol + total_put_vol
        pc_ratio = total_put_vol / total_call_vol if total_call_vol > 0 else 0

        # Unusual activity: volume > 2x open interest on any side
        unusual = False
        if total_call_oi > 0 and total_call_vol > total_call_oi * 2:
            unusual = True
        if total_put_oi > 0 and total_put_vol > total_put_oi * 2:
            unusual = True

        # Flow direction
        if pc_ratio < 0.5:
            flow = "bullish"
        elif pc_ratio > 1.5:
            flow = "bearish"
        else:
            flow = "neutral"

        return {
            "symbol": symbol,
            "total_call_vol": total_call_vol,
            "total_put_vol": total_put_vol,
            "total_call_oi": total_call_oi,
            "total_put_oi": total_put_oi,
            "put_call_ratio": round(pc_ratio, 2),
            "unusual_activity": unusual,
            "flow_direction": flow,
            "total_volume": total_vol,
        }
    except Exception as e:
        console.log(f"[red]Options flow error for {symbol}: {e}")
        return None


def check_ai_alignment(player_id: str, symbol: str, ai_action: str) -> dict | None:
    """Check if an AI's options trade aligns with unusual options flow."""
    flow = get_options_volume(symbol)
    if not flow:
        return None

    ai_direction = ""
    if ai_action in ("BUY_CALL",):
        ai_direction = "bullish"
    elif ai_action in ("BUY_PUT",):
        ai_direction = "bearish"
    else:
        return None

    aligned = ai_direction == flow["flow_direction"]
    unusual_aligned = flow["unusual_activity"] and aligned

    return {
        "symbol": symbol,
        "player_id": player_id,
        "ai_direction": ai_direction,
        "flow_direction": flow["flow_direction"],
        "aligned": aligned,
        "unusual_aligned": unusual_aligned,
        "put_call_ratio": flow["put_call_ratio"],
        "total_volume": flow["total_volume"],
        "unusual_activity": flow["unusual_activity"],
    }


def get_flow_summary(symbols: list) -> list:
    """Get options flow for multiple symbols."""
    results = []
    for sym in symbols:
        flow = get_options_volume(sym)
        if flow:
            results.append(flow)
    return results


def get_recent_ai_options_alignment() -> list:
    """Check alignment for all recent AI options trades (last 24h)."""
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    trades = conn.execute(
        "SELECT player_id, symbol, action FROM trades "
        "WHERE action IN ('BUY_CALL', 'BUY_PUT') "
        "AND executed_at >= datetime('now', '-24 hours') "
        "ORDER BY executed_at DESC"
    ).fetchall()
    conn.close()

    results = []
    seen = set()
    for t in trades:
        key = f"{t['player_id']}:{t['symbol']}:{t['action']}"
        if key in seen:
            continue
        seen.add(key)
        alignment = check_ai_alignment(t["player_id"], t["symbol"], t["action"])
        if alignment:
            results.append(alignment)
    return results
