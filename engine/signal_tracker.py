"""Signal Tracker — multi-day BUY signal continuation tracking."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

TARGET_PCT = 10.0   # +10% from entry → hit_target
STOP_PCT = -12.0    # -12% from entry → expired


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def record_signal(player_id: str, display_name: str, symbol: str,
                  entry_price: float, confidence: float, reasoning: str):
    """Record a high-confidence BUY signal for multi-day tracking.

    Only records if confidence > 0.65 and no duplicate active signal exists
    for the same model+symbol.
    """
    if confidence < 0.65:
        return None

    conn = _conn()

    # Check for existing active signal from same model+symbol
    existing = conn.execute(
        "SELECT id FROM watchlist_signals WHERE player_id=? AND symbol=? AND status='active'",
        (player_id, symbol)
    ).fetchone()
    if existing:
        conn.close()
        return None

    # Check if another model already has an active signal → CONFIRMED
    other = conn.execute(
        "SELECT player_id FROM watchlist_signals WHERE symbol=? AND status='active' AND player_id!=?",
        (symbol, player_id)
    ).fetchone()
    confirmed = other is not None

    conn.execute(
        "INSERT INTO watchlist_signals "
        "(player_id, display_name, symbol, entry_price, confidence, reasoning, status, confirmed) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (player_id, display_name, symbol, entry_price, confidence, reasoning,
         "active", 1 if confirmed else 0)
    )

    # If this confirms an existing signal, mark the original as confirmed too
    if confirmed:
        conn.execute(
            "UPDATE watchlist_signals SET confirmed=1 WHERE symbol=? AND status='active'",
            (symbol,)
        )
        console.log(f"[bold yellow]SIGNAL CONFIRMED: {player_id} confirms {symbol} "
                     f"(also signaled by {other['player_id']})")

    conn.commit()
    conn.close()

    # Check for re-entry opportunity
    reentry = flag_reentry(player_id, display_name, symbol, entry_price, confidence, reasoning)
    is_reentry = reentry is not None

    status = "RE-ENTRY" if is_reentry else ("CONFIRMED" if confirmed else "NEW")
    console.log(f"[cyan]Signal Tracker: {status} {player_id} BUY {symbol} @ ${entry_price:.2f} "
                f"(conf={confidence:.0%})")
    if is_reentry:
        console.log(f"[bold magenta]RE-ENTRY: {player_id} re-entering {symbol} "
                     f"(prev exit ${reentry['exit_price']:.2f}, pullback {reentry['pullback_pct']:+.1f}%)")
    return {"status": status, "confirmed": confirmed, "reentry": reentry}


def check_active_signals(prices: dict):
    """Check all active signals against current prices. Called each scan cycle.

    Marks hit_target (+10%) or expired (-12%) and sends Telegram alerts.
    """
    conn = _conn()
    active = conn.execute(
        "SELECT id, player_id, display_name, symbol, entry_price, confidence, confirmed, signal_date "
        "FROM watchlist_signals WHERE status='active'"
    ).fetchall()

    hits = []
    expires = []

    for sig in active:
        symbol = sig["symbol"]
        price_data = prices.get(symbol)
        if not price_data:
            continue
        current_price = price_data.get("price", 0)
        if current_price <= 0:
            continue

        entry = sig["entry_price"]
        pnl_pct = ((current_price - entry) / entry) * 100

        if pnl_pct >= TARGET_PCT:
            conn.execute(
                "UPDATE watchlist_signals SET status='hit_target', current_price=?, pnl_pct=?, "
                "resolved_at=CURRENT_TIMESTAMP WHERE id=?",
                (current_price, round(pnl_pct, 2), sig["id"])
            )
            hits.append(sig)
            console.log(f"[bold green]SIGNAL HIT TARGET: {sig['display_name']} {symbol} "
                        f"+{pnl_pct:.1f}% (entry ${entry:.2f} → ${current_price:.2f})")
        elif pnl_pct <= STOP_PCT:
            conn.execute(
                "UPDATE watchlist_signals SET status='expired', current_price=?, pnl_pct=?, "
                "resolved_at=CURRENT_TIMESTAMP WHERE id=?",
                (current_price, round(pnl_pct, 2), sig["id"])
            )
            expires.append(sig)
            console.log(f"[red]SIGNAL EXPIRED: {sig['display_name']} {symbol} "
                        f"{pnl_pct:.1f}% (entry ${entry:.2f} → ${current_price:.2f})")
        else:
            # Update current price/pnl for dashboard display
            conn.execute(
                "UPDATE watchlist_signals SET current_price=?, pnl_pct=? WHERE id=?",
                (current_price, round(pnl_pct, 2), sig["id"])
            )

    conn.commit()
    conn.close()

    # Telegram alerts for hits
    if hits:
        try:
            from engine.telegram_alerts import send_alert
            for sig in hits:
                symbol = sig["symbol"]
                current = prices[symbol]["price"]
                pnl_pct = ((current - sig["entry_price"]) / sig["entry_price"]) * 100
                badge = " 🏅 CONSENSUS" if sig["confirmed"] else ""
                msg = (
                    f"🎯 <b>SIGNAL HIT TARGET</b>{badge}\n"
                    f"Model: {sig['display_name']}\n"
                    f"Symbol: <b>{symbol}</b>\n"
                    f"Entry: ${sig['entry_price']:.2f} → ${current:.2f} (+{pnl_pct:.1f}%)\n"
                    f"Signal date: {sig['signal_date']}"
                )
                send_alert(msg)
        except Exception:
            pass

    return {"hits": len(hits), "expires": len(expires)}


def get_active_signals() -> list:
    """Get all active signals for dashboard, sorted by P&L."""
    conn = _conn()
    rows = conn.execute(
        "SELECT id, player_id, display_name, symbol, entry_price, confidence, "
        "reasoning, status, confirmed, signal_date, current_price, pnl_pct "
        "FROM watchlist_signals WHERE status='active' ORDER BY pnl_pct DESC"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_all_signals(limit: int = 100) -> list:
    """Get all signals (active + resolved) for dashboard."""
    conn = _conn()
    rows = conn.execute(
        "SELECT id, player_id, display_name, symbol, entry_price, confidence, "
        "reasoning, status, confirmed, signal_date, current_price, pnl_pct, resolved_at "
        "FROM watchlist_signals ORDER BY signal_date DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_consensus_signals() -> list:
    """Get symbols with multiple active model signals (consensus)."""
    conn = _conn()
    rows = conn.execute("""
        SELECT symbol, COUNT(DISTINCT player_id) as model_count,
               GROUP_CONCAT(DISTINCT display_name) as models,
               AVG(confidence) as avg_confidence,
               MIN(entry_price) as earliest_entry,
               MAX(entry_price) as latest_entry
        FROM watchlist_signals WHERE status='active'
        GROUP BY symbol HAVING model_count >= 2
        ORDER BY model_count DESC, avg_confidence DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_watching(player_id: str, symbol: str, exit_price: float):
    """Move active signals to 'watching' when the position is closed.

    Called from sell() and sell_partial() (full close).
    """
    conn = _conn()
    updated = conn.execute(
        "UPDATE watchlist_signals SET status='watching', exit_price=?, "
        "resolved_at=CURRENT_TIMESTAMP WHERE player_id=? AND symbol=? AND status='active'",
        (exit_price, player_id, symbol)
    ).rowcount
    conn.commit()
    conn.close()
    if updated:
        console.log(f"[yellow]Signal Tracker: {player_id} {symbol} → WATCHING (exited @ ${exit_price:.2f})")


def check_reentry_opportunities(prices: dict):
    """Check 'watching' signals for re-entry opportunities.

    If a new BUY signal appears within 14 days of exit, flag as RE_ENTRY.
    Called each scan cycle alongside check_active_signals.
    """
    conn = _conn()
    watching = conn.execute(
        "SELECT DISTINCT w.id, w.player_id, w.display_name, w.symbol, w.entry_price, "
        "w.exit_price, w.resolved_at "
        "FROM watchlist_signals w "
        "WHERE w.status='watching' "
        "AND w.resolved_at >= datetime('now', '-14 days')"
    ).fetchall()

    reentries = []
    for w in watching:
        symbol = w["symbol"]
        price_data = prices.get(symbol)
        if not price_data:
            continue
        current = price_data.get("price", 0)
        if current <= 0:
            continue
        # Update current price on watching signals for dashboard
        conn.execute(
            "UPDATE watchlist_signals SET current_price=?, pnl_pct=? WHERE id=?",
            (current, round(((current - w["entry_price"]) / w["entry_price"]) * 100, 2), w["id"])
        )

    conn.commit()
    conn.close()


def flag_reentry(player_id: str, display_name: str, symbol: str,
                 new_price: float, confidence: float, reasoning: str) -> dict | None:
    """Check if this BUY signal qualifies as a RE-ENTRY opportunity.

    Called from record_signal when a new BUY signal comes in.
    Returns re-entry context if found, None otherwise.
    """
    conn = _conn()
    # Find watching signals for this symbol from ANY model (within 14 days)
    watching = conn.execute(
        "SELECT player_id, display_name, symbol, entry_price, exit_price, resolved_at "
        "FROM watchlist_signals WHERE symbol=? AND status='watching' "
        "AND resolved_at >= datetime('now', '-14 days') "
        "ORDER BY resolved_at DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    conn.close()

    if not watching:
        return None

    return {
        "symbol": symbol,
        "prev_player": watching["display_name"],
        "prev_entry": watching["entry_price"],
        "exit_price": watching["exit_price"],
        "current_price": new_price,
        "pullback_pct": round(((new_price - watching["exit_price"]) / watching["exit_price"]) * 100, 2),
        "days_since_exit": watching["resolved_at"],
    }


def get_reentry_opportunities() -> list:
    """Get 'Second Chance' stocks — sold but now have fresh buy signals."""
    conn = _conn()
    rows = conn.execute("""
        SELECT w.id, w.player_id, w.display_name, w.symbol,
               w.entry_price as orig_entry, w.exit_price,
               w.resolved_at as exit_date, w.current_price,
               w.pnl_pct,
               r.player_id as reentry_player, r.display_name as reentry_model,
               r.entry_price as reentry_price, r.confidence as reentry_confidence,
               r.signal_date as reentry_date
        FROM watchlist_signals w
        LEFT JOIN watchlist_signals r ON r.symbol = w.symbol
            AND r.status = 'active'
            AND r.signal_date > w.resolved_at
        WHERE w.status = 'watching'
        AND w.resolved_at >= datetime('now', '-14 days')
        ORDER BY w.resolved_at DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_reentry_prompt_section(player_id: str, symbol: str) -> str:
    """Build prompt section for AI if this stock was previously held and exited."""
    conn = _conn()
    # Check if this model (or any model) recently exited this symbol
    watching = conn.execute(
        "SELECT player_id, display_name, entry_price, exit_price, resolved_at "
        "FROM watchlist_signals WHERE symbol=? AND status='watching' "
        "AND resolved_at >= datetime('now', '-14 days') "
        "ORDER BY resolved_at DESC LIMIT 1",
        (symbol,)
    ).fetchone()
    conn.close()

    if not watching:
        return ""

    who = "You" if watching["player_id"] == player_id else watching["display_name"]
    return (
        f"\n=== RE-ENTRY OPPORTUNITY: {symbol} ===\n"
        f"{who} previously held {symbol} and exited at ${watching['exit_price']:.2f} "
        f"(original entry ${watching['entry_price']:.2f}). "
        f"Exit date: {watching['resolved_at'][:10]}. "
        f"Consider re-entry if the thesis remains intact and price has pulled back to a favorable level.\n"
    )


def get_reentry_leaderboard() -> list:
    """Track re-entry success rate per model."""
    conn = _conn()
    # A successful re-entry: model had a watching signal, then issued a new BUY
    # that later hit target
    rows = conn.execute("""
        SELECT r.player_id, r.display_name,
               COUNT(*) as total_reentries,
               SUM(CASE WHEN r.status='hit_target' THEN 1 ELSE 0 END) as hits,
               SUM(CASE WHEN r.status='expired' THEN 1 ELSE 0 END) as misses,
               SUM(CASE WHEN r.status='active' THEN 1 ELSE 0 END) as active,
               AVG(CASE WHEN r.status!='active' THEN r.pnl_pct ELSE NULL END) as avg_pnl
        FROM watchlist_signals r
        WHERE r.symbol IN (
            SELECT DISTINCT symbol FROM watchlist_signals
            WHERE status='watching' AND resolved_at >= datetime('now', '-30 days')
        )
        AND r.signal_date > (
            SELECT MAX(w2.resolved_at) FROM watchlist_signals w2
            WHERE w2.symbol = r.symbol AND w2.status='watching'
        )
        AND r.status IN ('active', 'hit_target', 'expired')
        GROUP BY r.player_id
        ORDER BY hits DESC
    """).fetchall()
    conn.close()

    result = []
    for r in rows:
        resolved = r["hits"] + r["misses"]
        hit_rate = round(r["hits"] / resolved * 100, 1) if resolved > 0 else 0
        result.append({
            "player_id": r["player_id"],
            "display_name": r["display_name"],
            "total_reentries": r["total_reentries"],
            "hits": r["hits"],
            "misses": r["misses"],
            "active": r["active"],
            "hit_rate": hit_rate,
            "avg_pnl": round(r["avg_pnl"], 2) if r["avg_pnl"] else 0,
        })
    return result


def get_model_leaderboard() -> list:
    """Best Signals leaderboard — which model has highest hit rate."""
    conn = _conn()
    rows = conn.execute("""
        SELECT player_id, display_name,
               COUNT(*) as total_signals,
               SUM(CASE WHEN status='hit_target' THEN 1 ELSE 0 END) as hits,
               SUM(CASE WHEN status='expired' THEN 1 ELSE 0 END) as misses,
               SUM(CASE WHEN status='active' THEN 1 ELSE 0 END) as active,
               AVG(CASE WHEN status!='active' THEN pnl_pct ELSE NULL END) as avg_pnl,
               AVG(confidence) as avg_confidence
        FROM watchlist_signals
        GROUP BY player_id
        ORDER BY hits DESC, avg_pnl DESC
    """).fetchall()
    conn.close()

    result = []
    for r in rows:
        resolved = r["hits"] + r["misses"]
        hit_rate = round(r["hits"] / resolved * 100, 1) if resolved > 0 else 0
        result.append({
            "player_id": r["player_id"],
            "display_name": r["display_name"],
            "total_signals": r["total_signals"],
            "hits": r["hits"],
            "misses": r["misses"],
            "active": r["active"],
            "hit_rate": hit_rate,
            "avg_pnl": round(r["avg_pnl"], 2) if r["avg_pnl"] else 0,
            "avg_confidence": round(r["avg_confidence"], 2) if r["avg_confidence"] else 0,
        })
    return result
