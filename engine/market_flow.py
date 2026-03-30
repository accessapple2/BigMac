"""Market Flow — daily directional lean indicator from aggregate options premium flow.

Calculates net call vs put premium across SPY, QQQ, and top watchlist stocks
using Yahoo options chain data. Produces a BULL/BEAR lean with conviction score
that gets injected into every AI model's scan prompt.

Refreshes every 15 minutes during market hours. History stored in DB for
Strategy Lab backtesting.
"""
from __future__ import annotations
import sqlite3
import time
import threading
from datetime import datetime
from engine.market_data import yahoo_options_chain
from rich.console import Console

console = Console()
DB = "data/trader.db"

# In-memory cache
_flow_cache = None  # latest flow lean result
_flow_cache_ts = 0
_CACHE_TTL = 300  # 5 min — scheduler runs every 15 min, but prompt reads may be more frequent
_flow_lock = threading.Lock()

# Symbols to aggregate flow from (SPY + QQQ + top 10 watchlist)
FLOW_SYMBOLS = ["SPY", "QQQ", "NVDA", "TSLA", "AAPL", "AMD", "META", "MSFT", "GOOGL", "AMZN", "AVGO", "PLTR"]


def _ensure_table():
    """Create flow_lean_history table if it doesn't exist."""
    conn = sqlite3.connect(DB, check_same_thread=False)
    conn.execute("""CREATE TABLE IF NOT EXISTS flow_lean_history (
        id INTEGER PRIMARY KEY,
        lean TEXT NOT NULL,
        conviction REAL NOT NULL,
        net_flow REAL NOT NULL,
        total_call_premium REAL NOT NULL,
        total_put_premium REAL NOT NULL,
        fresh_cb_call REAL NOT NULL DEFAULT 0,
        fresh_cb_put REAL NOT NULL DEFAULT 0,
        symbols_scanned INTEGER NOT NULL,
        details TEXT,
        recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


def _calc_symbol_flow(symbol: str) -> dict | None:
    """Calculate call/put premium and fresh capital buying for a single symbol.

    Premium = sum(volume * lastPrice * 100) for each strike.
    Fresh CB = premium from contracts where volume > openInterest (new positions).
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

        call_premium = 0.0
        put_premium = 0.0
        fresh_call = 0.0
        fresh_put = 0.0
        call_oi_total = 0
        put_oi_total = 0
        call_vol_total = 0
        put_vol_total = 0

        for c in calls:
            vol = c.get("volume", 0) or 0
            price = c.get("lastPrice", 0) or 0
            oi = c.get("openInterest", 0) or 0
            prem = vol * price * 100  # each contract = 100 shares
            call_premium += prem
            call_vol_total += vol
            call_oi_total += oi
            # Fresh capital: volume exceeding OI means new positions opened today
            if vol > oi and oi > 0:
                excess = vol - oi
                fresh_call += excess * price * 100

        for p in puts:
            vol = p.get("volume", 0) or 0
            price = p.get("lastPrice", 0) or 0
            oi = p.get("openInterest", 0) or 0
            prem = vol * price * 100
            put_premium += prem
            put_vol_total += vol
            put_oi_total += oi
            if vol > oi and oi > 0:
                excess = vol - oi
                fresh_put += excess * price * 100

        return {
            "symbol": symbol,
            "call_premium": call_premium,
            "put_premium": put_premium,
            "fresh_call": fresh_call,
            "fresh_put": fresh_put,
            "call_vol": call_vol_total,
            "put_vol": put_vol_total,
            "call_oi": call_oi_total,
            "put_oi": put_oi_total,
        }
    except Exception as e:
        console.log(f"[red]Flow calc error for {symbol}: {e}")
        return None


def calculate_flow_lean() -> dict | None:
    """Calculate aggregate flow lean across all FLOW_SYMBOLS.

    Returns:
        {lean: "BULL"|"BEAR", conviction: 0-100, net_flow: float,
         total_call_premium: float, total_put_premium: float,
         fresh_cb_call: float, fresh_cb_put: float, fresh_cb_net: float,
         symbols_scanned: int, per_symbol: list, guidance: str,
         recorded_at: str}
    """
    global _flow_cache, _flow_cache_ts

    total_call = 0.0
    total_put = 0.0
    total_fresh_call = 0.0
    total_fresh_put = 0.0
    per_symbol = []
    scanned = 0

    for sym in FLOW_SYMBOLS:
        result = _calc_symbol_flow(sym)
        if result:
            total_call += result["call_premium"]
            total_put += result["put_premium"]
            total_fresh_call += result["fresh_call"]
            total_fresh_put += result["fresh_put"]
            per_symbol.append(result)
            scanned += 1
        # Small delay to avoid hammering Yahoo
        time.sleep(0.3)

    if scanned == 0:
        return None

    net_flow = total_call - total_put
    fresh_cb_net = total_fresh_call - total_fresh_put
    lean = "BULL" if net_flow >= 0 else "BEAR"

    # Conviction: based on magnitude of net flow relative to total volume
    total_premium = total_call + total_put
    if total_premium > 0:
        # Raw ratio: how skewed the flow is (0 = balanced, 1 = all one side)
        skew = abs(net_flow) / total_premium
        # Scale to 0-100. A 20%+ skew is very strong conviction
        conviction = min(100.0, round(skew * 500, 1))  # 20% skew = 100 conviction
    else:
        conviction = 0.0

    # Format net flow in millions
    net_flow_m = net_flow / 1_000_000
    fresh_cb_m = fresh_cb_net / 1_000_000

    # Generate guidance text for AI prompt
    if lean == "BEAR":
        if conviction >= 70:
            guidance = (
                f"Smart money is aggressively buying puts today. "
                f"Be very cautious with new longs. Consider tighter stops or waiting for a better entry. "
                f"Fresh capital is flowing into protection."
            )
        elif conviction >= 40:
            guidance = (
                f"Put premium outweighs calls — moderate bearish positioning. "
                f"Be selective with new longs. Favor high-conviction setups only."
            )
        else:
            guidance = (
                f"Slightly more put premium than calls — mild caution. "
                f"Normal trading, but keep stops tight."
            )
    else:  # BULL
        if conviction >= 70:
            guidance = (
                f"Smart money is aggressively buying calls today. "
                f"Lean into long setups with confidence. Momentum is your friend."
            )
        elif conviction >= 40:
            guidance = (
                f"Call premium outweighs puts — moderate bullish positioning. "
                f"Good environment for new longs with catalysts."
            )
        else:
            guidance = (
                f"Slightly more call premium than puts — mild bullish tilt. "
                f"Normal trading conditions."
            )

    now = datetime.now().isoformat()
    result = {
        "lean": lean,
        "conviction": conviction,
        "net_flow": round(net_flow, 2),
        "net_flow_m": round(net_flow_m, 2),
        "total_call_premium": round(total_call, 2),
        "total_put_premium": round(total_put, 2),
        "fresh_cb_call": round(total_fresh_call, 2),
        "fresh_cb_put": round(total_fresh_put, 2),
        "fresh_cb_net": round(fresh_cb_net, 2),
        "fresh_cb_m": round(fresh_cb_m, 2),
        "symbols_scanned": scanned,
        "per_symbol": per_symbol,
        "guidance": guidance,
        "recorded_at": now,
    }

    with _flow_lock:
        _flow_cache = result
        _flow_cache_ts = time.time()

    return result


def save_flow_lean(data: dict):
    """Persist flow lean snapshot to DB for backtesting."""
    _ensure_table()
    try:
        import json
        details = json.dumps(data.get("per_symbol", []), default=str)
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.execute(
            "INSERT INTO flow_lean_history "
            "(lean, conviction, net_flow, total_call_premium, total_put_premium, "
            "fresh_cb_call, fresh_cb_put, symbols_scanned, details) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                data["lean"], data["conviction"], data["net_flow"],
                data["total_call_premium"], data["total_put_premium"],
                data["fresh_cb_call"], data["fresh_cb_put"],
                data["symbols_scanned"], details,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]Flow lean DB save error: {e}")


def refresh_flow_lean() -> dict | None:
    """Full refresh: calculate + save + return. Called by scheduler."""
    data = calculate_flow_lean()
    if data:
        save_flow_lean(data)
        arrow = "\u2193" if data["lean"] == "BEAR" else "\u2191"
        console.log(
            f"[{'red' if data['lean'] == 'BEAR' else 'green'}]"
            f"Flow Lean: {data['lean']} {arrow} | "
            f"Net: ${data['net_flow_m']:+.1f}M | "
            f"Conv: {data['conviction']:.0f} | "
            f"Fresh CB: ${data['fresh_cb_m']:+.1f}M | "
            f"Scanned: {data['symbols_scanned']}/{len(FLOW_SYMBOLS)}"
        )
    return data


def get_flow_lean() -> dict | None:
    """Get current flow lean (from cache if fresh, otherwise return stale)."""
    with _flow_lock:
        return _flow_cache


def get_flow_lean_history(limit: int = 50) -> list:
    """Get recent flow lean history for dashboard sparkline / Strategy Lab."""
    _ensure_table()
    try:
        conn = sqlite3.connect(DB, check_same_thread=False)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT lean, conviction, net_flow, total_call_premium, total_put_premium, "
            "fresh_cb_call, fresh_cb_put, symbols_scanned, recorded_at "
            "FROM flow_lean_history ORDER BY recorded_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in reversed(rows)]
    except Exception:
        return []


def build_flow_lean_prompt_section() -> str:
    """Build prompt section for AI model injection.

    Returns a formatted string like:
    === MARKET DIRECTIONAL LEAN ===
    MARKET DIRECTIONAL LEAN: BEAR (-$44.3M net flow).
    Smart money is selling today. Be cautious with new longs.
    Fresh Capital Buying: -$12.1M net (more new put positions than calls).
    Conviction: 72/100.
    """
    data = get_flow_lean()
    if not data:
        return ""

    lean = data["lean"]
    net_m = data["net_flow_m"]
    fresh_m = data["fresh_cb_m"]
    conv = data["conviction"]
    guidance = data["guidance"]

    lines = [
        f"=== MARKET DIRECTIONAL LEAN ===",
        f"MARKET DIRECTIONAL LEAN: {lean} (${net_m:+.1f}M net options flow).",
        f"{guidance}",
        f"Fresh Capital Buying: ${fresh_m:+.1f}M net ({'more new put positions' if fresh_m < 0 else 'more new call positions'}).",
        f"Conviction: {conv:.0f}/100.",
    ]
    return "\n".join(lines)
