"""Pair Trade Detection — find same-sector opposing signals and track as single strategy."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def detect_pair_opportunities() -> list:
    """Scan recent signals for same-sector opposing views.

    If AI is bullish on stock A and bearish on stock B in same sector,
    that's a pair trade opportunity (long A / short B via puts).

    Returns list of {long_symbol, short_symbol, sector, long_signal, short_signal,
                     player_id, display_name, confidence}.
    """
    from engine.sector_tracker import SECTOR_MAP

    conn = _conn()
    # Get latest signals per player per symbol (last 10 min)
    signals = conn.execute("""
        SELECT s.player_id, p.display_name, s.symbol, s.signal, s.confidence, s.created_at
        FROM signals s JOIN ai_players p ON s.player_id = p.id
        WHERE s.created_at >= datetime('now', '-10 minutes')
        AND s.player_id != 'dayblade-0dte'
        ORDER BY s.created_at DESC
    """).fetchall()
    conn.close()

    # Group by player
    by_player: dict[str, list] = {}
    for s in signals:
        pid = s["player_id"]
        if pid not in by_player:
            by_player[pid] = []
        # Only keep latest per symbol
        if not any(x["symbol"] == s["symbol"] for x in by_player[pid]):
            by_player[pid].append(dict(s))

    pairs = []
    for pid, sigs in by_player.items():
        # Find bullish and bearish signals
        bullish = [s for s in sigs if s["signal"] in ("BUY", "BUY_CALL")]
        bearish = [s for s in sigs if s["signal"] == "BUY_PUT"]

        for bull in bullish:
            bull_sector = SECTOR_MAP.get(bull["symbol"], "Unknown")
            for bear in bearish:
                bear_sector = SECTOR_MAP.get(bear["symbol"], "Unknown")
                if bull_sector == bear_sector and bull_sector != "Unknown":
                    avg_conf = ((bull["confidence"] or 0) + (bear["confidence"] or 0)) / 2
                    pairs.append({
                        "long_symbol": bull["symbol"],
                        "short_symbol": bear["symbol"],
                        "sector": bull_sector,
                        "long_signal": bull["signal"],
                        "short_signal": bear["signal"],
                        "player_id": pid,
                        "display_name": bull["display_name"],
                        "confidence": round(avg_conf, 2),
                        "detected_at": datetime.now().isoformat(),
                    })

    # Also save detected pairs
    for pair in pairs:
        _save_pair(pair)

    return pairs


def _save_pair(pair: dict):
    """Save detected pair trade to DB."""
    import json
    conn = _conn()
    conn.execute(
        "INSERT INTO pair_trades (long_symbol, short_symbol, sector, player_id, confidence, details) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (pair["long_symbol"], pair["short_symbol"], pair["sector"],
         pair["player_id"], pair["confidence"], json.dumps(pair))
    )
    conn.commit()
    conn.close()


def get_pair_trades(limit: int = 20) -> list:
    """Get recent pair trade detections."""
    conn = _conn()
    rows = conn.execute("""
        SELECT pt.*, p.display_name
        FROM pair_trades pt JOIN ai_players p ON pt.player_id = p.id
        ORDER BY pt.detected_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()

    result = []
    for r in rows:
        result.append({
            "long_symbol": r["long_symbol"],
            "short_symbol": r["short_symbol"],
            "sector": r["sector"],
            "player_id": r["player_id"],
            "display_name": r["display_name"],
            "confidence": r["confidence"],
            "detected_at": r["detected_at"],
        })
    return result


def get_pair_pnl(prices: dict) -> list:
    """Calculate combined P&L for active pair trades.

    For each pair: long P&L + short P&L (put position) = combined.
    """
    conn = _conn()
    pairs = conn.execute("""
        SELECT long_symbol, short_symbol, player_id, detected_at
        FROM pair_trades
        WHERE detected_at >= datetime('now', '-7 days')
        ORDER BY detected_at DESC LIMIT 10
    """).fetchall()

    result = []
    for pair in pairs:
        long_sym = pair["long_symbol"]
        short_sym = pair["short_symbol"]
        pid = pair["player_id"]

        # Check if player has positions in both
        long_pos = conn.execute(
            "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND asset_type='stock'",
            (pid, long_sym)
        ).fetchone()
        short_pos = conn.execute(
            "SELECT qty, avg_price FROM positions WHERE player_id=? AND symbol=? AND option_type='put'",
            (pid, short_sym)
        ).fetchone()

        long_pnl = 0
        short_pnl = 0

        if long_pos and long_sym in prices:
            cur = prices[long_sym].get("price", long_pos["avg_price"])
            long_pnl = (cur - long_pos["avg_price"]) * long_pos["qty"]

        if short_pos and short_sym in prices:
            cur = prices[short_sym].get("price", short_pos["avg_price"])
            short_pnl = (cur - short_pos["avg_price"]) * short_pos["qty"]

        result.append({
            "long_symbol": long_sym,
            "short_symbol": short_sym,
            "player_id": pid,
            "long_pnl": round(long_pnl, 2),
            "short_pnl": round(short_pnl, 2),
            "combined_pnl": round(long_pnl + short_pnl, 2),
            "detected_at": pair["detected_at"],
        })

    conn.close()
    return result
