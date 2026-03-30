"""Smart Money Detector — flags when 3+ AI models independently BUY the same stock in one scan."""
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


def check_smart_money_signals() -> list:
    """Check if 3+ models bought the same stock in the last scan cycle (last 5 min).

    Returns list of {symbol, buyers: [{player_id, display_name, price, confidence}], count}.
    """
    conn = _conn()
    # Look at BUY trades in the last 5 minutes
    rows = conn.execute("""
        SELECT t.symbol, t.player_id, p.display_name, t.price, t.confidence, t.executed_at
        FROM trades t JOIN ai_players p ON t.player_id = p.id
        WHERE t.action IN ('BUY', 'BUY_CALL')
        AND t.executed_at >= datetime('now', '-5 minutes')
        AND t.player_id != 'dayblade-0dte'
        ORDER BY t.symbol, t.executed_at DESC
    """).fetchall()
    conn.close()

    # Group by symbol
    by_symbol: dict[str, list] = {}
    for r in rows:
        sym = r["symbol"]
        if sym not in by_symbol:
            by_symbol[sym] = []
        # Avoid duplicate player entries
        if not any(b["player_id"] == r["player_id"] for b in by_symbol[sym]):
            by_symbol[sym].append({
                "player_id": r["player_id"],
                "display_name": r["display_name"],
                "price": r["price"],
                "confidence": r["confidence"],
            })

    signals = []
    for sym, buyers in by_symbol.items():
        if len(buyers) >= 3:
            signals.append({
                "symbol": sym,
                "buyers": buyers,
                "count": len(buyers),
                "detected_at": datetime.now().isoformat(),
            })

    return signals


def get_recent_smart_money(limit: int = 20) -> list:
    """Get recent smart money signals from the DB."""
    conn = _conn()
    rows = conn.execute("""
        SELECT symbol, buyers, detected_at
        FROM smart_money_signals
        ORDER BY detected_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    import json
    return [{"symbol": r["symbol"], "buyers": json.loads(r["buyers"]), "detected_at": r["detected_at"]} for r in rows]


def save_smart_money_signal(signal: dict):
    """Save a smart money signal to DB."""
    import json
    conn = _conn()
    conn.execute(
        "INSERT INTO smart_money_signals (symbol, buyers, detected_at) VALUES (?, ?, ?)",
        (signal["symbol"], json.dumps(signal["buyers"]), signal["detected_at"])
    )
    conn.commit()
    conn.close()


def detect_and_alert():
    """Run detection and send Telegram alert if smart money signal found."""
    signals = check_smart_money_signals()
    for sig in signals:
        save_smart_money_signal(sig)
        console.log(f"[bold gold1]SMART MONEY SIGNAL: {sig['symbol']} — {sig['count']} models bought!")

        # Telegram alert
        try:
            from engine.telegram_alerts import send_alert
            names = ", ".join(b["display_name"] for b in sig["buyers"])
            avg_conf = sum(b["confidence"] or 0 for b in sig["buyers"]) / len(sig["buyers"])
            msg = (
                f"🏆 <b>SMART MONEY SIGNAL</b>\n"
                f"<b>{sig['symbol']}</b> — {sig['count']} AI models bought independently!\n"
                f"Models: {names}\n"
                f"Avg Confidence: {avg_conf:.0%}"
            )
            send_alert(msg)
        except Exception as e:
            console.log(f"[red]Smart money alert error: {e}")

    return signals
