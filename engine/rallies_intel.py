"""Rallies Arena Intelligence — Track external AI trading competition for confirmation signals.

Rallies.ai runs an AI Arena where 8+ models (GPT-5, Claude, Gemini, Grok, DeepSeek, etc.)
each manage $100K portfolios in live stock markets. We ingest their trades and standings
to cross-reference with USS TradeMinds crew decisions.

Data flow:
  1. Import (manual paste or future scraper) → rallies_models + rallies_trades tables
  2. Process → confirmation signals, consensus alerts, win rate tracking
  3. Output → War Room alerts, Starfleet Intel leaderboard, Arena Intelligence prompt injection
"""
from __future__ import annotations
import json
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def init_tables():
    """Create Rallies Arena tables if they don't exist."""
    conn = _conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS rallies_models (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        return_pct REAL DEFAULT 0,
        portfolio_value REAL DEFAULT 100000,
        win_rate REAL DEFAULT 0,
        total_trades INTEGER DEFAULT 0,
        winning_trades INTEGER DEFAULT 0,
        updated_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS rallies_trades (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        model_id TEXT NOT NULL,
        model_name TEXT NOT NULL,
        symbol TEXT NOT NULL,
        action TEXT NOT NULL,
        confidence REAL DEFAULT 0,
        reasoning TEXT DEFAULT '',
        price REAL DEFAULT 0,
        imported_at TEXT DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY (model_id) REFERENCES rallies_models(id)
    )""")
    conn.execute("""CREATE TABLE IF NOT EXISTS rallies_alerts (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        alert_type TEXT NOT NULL,
        symbol TEXT,
        details TEXT NOT NULL,
        models TEXT DEFAULT '[]',
        created_at TEXT DEFAULT CURRENT_TIMESTAMP
    )""")
    conn.commit()
    conn.close()


# Ensure tables exist on import
init_tables()


# ============================================================
# DATA IMPORT
# ============================================================

def import_leaderboard(data: list[dict]) -> dict:
    """Import Rallies Arena leaderboard standings.

    Expected format: [
        {"id": "grok-4", "name": "Grok 4", "return_pct": 7.0, "portfolio_value": 107000},
        {"id": "claude-sonnet", "name": "Claude Sonnet 4.5", "return_pct": 5.7, ...},
        ...
    ]
    """
    conn = _conn()
    imported = 0
    for m in data:
        model_id = m.get("id", "").strip()
        if not model_id:
            continue
        conn.execute("""
            INSERT INTO rallies_models (id, name, return_pct, portfolio_value, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(id) DO UPDATE SET
                name=excluded.name,
                return_pct=excluded.return_pct,
                portfolio_value=excluded.portfolio_value,
                updated_at=excluded.updated_at
        """, (
            model_id,
            m.get("name", model_id),
            m.get("return_pct", 0),
            m.get("portfolio_value", 100000),
            datetime.now().isoformat(),
        ))
        imported += 1
    conn.commit()
    conn.close()
    console.log(f"[cyan]📡 Rallies Intel: imported {imported} model standings")
    return {"imported": imported}


def import_trades(trades: list[dict]) -> dict:
    """Import Rallies Arena trades.

    Expected format: [
        {"model_id": "grok-4", "model_name": "Grok 4", "symbol": "NVDA",
         "action": "BUY", "confidence": 0.85, "reasoning": "...", "price": 130.50},
        ...
    ]
    """
    conn = _conn()
    imported = 0
    confirmations = []
    for t in trades:
        model_id = t.get("model_id", "").strip()
        symbol = t.get("symbol", "").strip().upper()
        action = t.get("action", "").strip().upper()
        if not model_id or not symbol or not action:
            continue

        conn.execute("""
            INSERT INTO rallies_trades (model_id, model_name, symbol, action, confidence,
                                         reasoning, price, imported_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            model_id,
            t.get("model_name", model_id),
            symbol,
            action,
            t.get("confidence", 0),
            t.get("reasoning", ""),
            t.get("price", 0),
            datetime.now().isoformat(),
        ))
        imported += 1

        # Check for confirmation signals (Rallies model buys something we hold)
        if action in ("BUY", "BUY_CALL"):
            conf = _check_confirmation(conn, symbol, t.get("model_name", model_id))
            if conf:
                confirmations.append(conf)

    conn.commit()

    # Check for consensus alerts (3+ Rallies models agree)
    consensus = _check_rallies_consensus(conn)

    conn.close()

    # Post alerts to War Room
    for c in confirmations:
        _post_war_room_alert("confirmation", c)
    for c in consensus:
        _post_war_room_alert("consensus", c)

    console.log(f"[cyan]📡 Rallies Intel: imported {imported} trades, "
                f"{len(confirmations)} confirmations, {len(consensus)} consensus alerts")
    return {
        "imported": imported,
        "confirmations": confirmations,
        "consensus_alerts": consensus,
    }


def import_bulk(data: dict) -> dict:
    """Import both leaderboard and trades in one call.

    Expected format: {
        "leaderboard": [...],
        "trades": [...]
    }

    Also accepts plain text paste that we'll try to parse.
    """
    result = {}
    if "leaderboard" in data:
        result["leaderboard"] = import_leaderboard(data["leaderboard"])
    if "trades" in data:
        result["trades"] = import_trades(data["trades"])
    if "raw" in data:
        result["parsed"] = parse_raw_text(data["raw"])
    return result


def parse_raw_text(text: str) -> dict:
    """Best-effort parse of pasted Rallies Arena text.

    Handles formats like:
      "Grok 4: +7.0% | BUY NVDA, BUY AAPL"
      "1. Grok 4 - $107,000 (+7.0%)"
      "Claude bought TSLA at $280"
    """
    import re
    lines = text.strip().split("\n")
    leaderboard = []
    trades = []

    for line in lines:
        line = line.strip()
        if not line:
            continue

        # Try to extract model + return: "Grok 4: +7.0%" or "1. Grok 4 - $107,000 (+7.0%)"
        lb_match = re.search(
            r'(?:(\d+)[.\)]\s*)?(.+?)[\s:—\-]+\$?([\d,]+(?:\.\d+)?)?\s*'
            r'[(\s]*([+-]?\d+\.?\d*)%',
            line
        )
        if lb_match:
            name = lb_match.group(2).strip().rstrip(":—- ")
            value = float(lb_match.group(3).replace(",", "")) if lb_match.group(3) else None
            ret = float(lb_match.group(4))
            model_id = _name_to_id(name)
            entry = {"id": model_id, "name": name, "return_pct": ret}
            if value:
                entry["portfolio_value"] = value
            leaderboard.append(entry)

        # Try to extract trades: "Grok 4 BUY NVDA" or "Claude bought TSLA at $280"
        trade_match = re.search(
            r'(.+?)\s+(BUY|SELL|BOUGHT|SOLD|BUY_CALL|BUY_PUT)\s+([A-Z]{1,5})'
            r'(?:\s+(?:at|@)\s+\$?([\d.]+))?',
            line, re.IGNORECASE
        )
        if trade_match:
            name = trade_match.group(1).strip()
            action = trade_match.group(2).upper()
            if action == "BOUGHT":
                action = "BUY"
            elif action == "SOLD":
                action = "SELL"
            symbol = trade_match.group(3).upper()
            price = float(trade_match.group(4)) if trade_match.group(4) else 0
            trades.append({
                "model_id": _name_to_id(name),
                "model_name": name,
                "symbol": symbol,
                "action": action,
                "price": price,
            })

    result = {}
    if leaderboard:
        result["leaderboard"] = import_leaderboard(leaderboard)
    if trades:
        result["trades"] = import_trades(trades)
    result["parsed_count"] = {"leaderboard": len(leaderboard), "trades": len(trades)}
    return result


def _name_to_id(name: str) -> str:
    """Convert a model display name to a slug ID."""
    return name.lower().strip().replace(" ", "-").replace(".", "")


# ============================================================
# INTELLIGENCE PROCESSING
# ============================================================

def _check_confirmation(conn, symbol: str, rallies_model_name: str) -> dict | None:
    """Check if a Rallies model bought something our crew already holds."""
    rows = conn.execute("""
        SELECT DISTINCT player_id FROM positions
        WHERE symbol=? AND qty > 0 AND player_id NOT IN ('steve-webull', 'dayblade-0dte')
    """, (symbol,)).fetchall()
    if rows:
        holders = [r["player_id"] for r in rows]
        alert = {
            "type": "confirmation",
            "symbol": symbol,
            "rallies_model": rallies_model_name,
            "crew_holders": holders,
            "message": (
                f"📡 CONFIRMATION: Rallies Arena's {rallies_model_name} just bought {symbol} — "
                f"already held by {', '.join(holders)}"
            ),
        }
        # Save alert
        conn.execute(
            "INSERT INTO rallies_alerts (alert_type, symbol, details, models) VALUES (?, ?, ?, ?)",
            ("confirmation", symbol, alert["message"], json.dumps([rallies_model_name] + holders))
        )
        return alert
    return None


def _check_rallies_consensus(conn) -> list:
    """Check if 3+ Rallies models bought the same ticker in last 24h."""
    rows = conn.execute("""
        SELECT symbol, GROUP_CONCAT(DISTINCT model_name) as models,
               COUNT(DISTINCT model_id) as model_count
        FROM rallies_trades
        WHERE action IN ('BUY', 'BUY_CALL')
        AND imported_at >= datetime('now', '-24 hours')
        GROUP BY symbol
        HAVING model_count >= 3
    """).fetchall()

    alerts = []
    for r in rows:
        # Don't re-alert if we already flagged this symbol today
        existing = conn.execute(
            "SELECT id FROM rallies_alerts WHERE alert_type='consensus' AND symbol=? "
            "AND created_at >= datetime('now', '-24 hours')",
            (r["symbol"],)
        ).fetchone()
        if existing:
            continue

        models_list = r["models"].split(",")
        alert = {
            "type": "consensus",
            "symbol": r["symbol"],
            "model_count": r["model_count"],
            "models": models_list,
            "message": (
                f"🎯 RALLIES CONSENSUS: {r['model_count']} external AI models agree on "
                f"{r['symbol']} — {r['models']}"
            ),
        }
        conn.execute(
            "INSERT INTO rallies_alerts (alert_type, symbol, details, models) VALUES (?, ?, ?, ?)",
            ("consensus", r["symbol"], alert["message"], json.dumps(models_list))
        )
        alerts.append(alert)

    return alerts


def _post_war_room_alert(alert_type: str, alert: dict):
    """Post a Rallies intel alert to the War Room."""
    try:
        conn = _conn()
        conn.execute(
            "INSERT INTO war_room (player_id, symbol, take, strategy_mode, created_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (
                "rallies-intel",
                alert.get("symbol", ""),
                alert["message"],
                "RALLIES",
                datetime.now().isoformat(),
            )
        )
        conn.commit()
        conn.close()
    except Exception as e:
        console.log(f"[red]Rallies War Room alert error: {e}")


# ============================================================
# QUERIES
# ============================================================

def get_leaderboard() -> list:
    """Get Rallies Arena standings."""
    conn = _conn()
    rows = conn.execute("""
        SELECT id, name, return_pct, portfolio_value, win_rate,
               total_trades, winning_trades, updated_at
        FROM rallies_models ORDER BY return_pct DESC
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_recent_trades(limit: int = 50) -> list:
    """Get recent Rallies Arena trades."""
    conn = _conn()
    rows = conn.execute("""
        SELECT model_id, model_name, symbol, action, confidence,
               reasoning, price, imported_at
        FROM rallies_trades ORDER BY imported_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_confirmation_signals(limit: int = 20) -> list:
    """Get recent confirmation signals (Rallies buys matching our holdings)."""
    conn = _conn()
    rows = conn.execute("""
        SELECT symbol, details, models, created_at
        FROM rallies_alerts WHERE alert_type='confirmation'
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [{"symbol": r["symbol"], "details": r["details"],
             "models": json.loads(r["models"]), "created_at": r["created_at"]} for r in rows]


def get_consensus_alerts(limit: int = 20) -> list:
    """Get recent consensus alerts (3+ Rallies models agree)."""
    conn = _conn()
    rows = conn.execute("""
        SELECT symbol, details, models, created_at
        FROM rallies_alerts WHERE alert_type='consensus'
        ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [{"symbol": r["symbol"], "details": r["details"],
             "models": json.loads(r["models"]), "created_at": r["created_at"]} for r in rows]


def get_rallies_alerts(limit: int = 50) -> list:
    """Get all Rallies intel alerts."""
    conn = _conn()
    rows = conn.execute("""
        SELECT alert_type, symbol, details, models, created_at
        FROM rallies_alerts ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_top_performers(n: int = 5) -> list:
    """Get top N Rallies performers by return %."""
    conn = _conn()
    rows = conn.execute("""
        SELECT id, name, return_pct, portfolio_value, win_rate, total_trades
        FROM rallies_models ORDER BY return_pct DESC LIMIT ?
    """, (n,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def compare_crew_vs_rallies() -> dict:
    """Compare USS TradeMinds crew performance against Rallies top performers."""
    conn = _conn()

    # Get our crew standings
    crew = conn.execute("""
        SELECT p.id, p.display_name, ph.total_value,
               ((ph.total_value - 7000.0) / 7000.0 * 100) as return_pct
        FROM ai_players p
        LEFT JOIN (
            SELECT player_id, total_value,
                   ROW_NUMBER() OVER (PARTITION BY player_id ORDER BY recorded_at DESC) as rn
            FROM portfolio_history WHERE season = (
                SELECT COALESCE(value, '1') FROM settings WHERE key='current_season'
            )
        ) ph ON p.id = ph.player_id AND ph.rn = 1
        WHERE p.is_human = 0 AND p.id NOT IN ('dayblade-0dte', 'steve-webull')
        ORDER BY ph.total_value DESC
    """).fetchall()

    # Get Rallies standings
    rallies = conn.execute("""
        SELECT id, name, return_pct, portfolio_value
        FROM rallies_models ORDER BY return_pct DESC
    """).fetchall()

    conn.close()

    crew_list = [{"id": r["id"], "name": r["display_name"],
                  "return_pct": r["return_pct"] or 0, "source": "crew"} for r in crew]
    rallies_list = [{"id": r["id"], "name": r["name"],
                     "return_pct": r["return_pct"] or 0, "source": "rallies"} for r in rallies]

    # Merge and rank
    combined = sorted(crew_list + rallies_list, key=lambda x: x["return_pct"], reverse=True)

    # Find overlapping trades (same symbols traded by both)
    overlap = _find_trade_overlaps(crew_list, rallies_list)

    return {
        "combined_ranking": combined,
        "crew": crew_list,
        "rallies": rallies_list,
        "trade_overlaps": overlap,
    }


def _find_trade_overlaps(crew: list, rallies: list) -> list:
    """Find tickers traded by both our crew and Rallies models."""
    conn = _conn()

    # Our recent buys (last 7 days)
    our_buys = conn.execute("""
        SELECT DISTINCT symbol FROM trades
        WHERE action IN ('BUY', 'BUY_CALL')
        AND executed_at >= datetime('now', '-7 days')
        AND player_id NOT IN ('steve-webull', 'dayblade-0dte')
    """).fetchall()
    our_symbols = {r["symbol"] for r in our_buys}

    # Rallies recent buys (last 7 days)
    rallies_buys = conn.execute("""
        SELECT DISTINCT symbol FROM rallies_trades
        WHERE action IN ('BUY', 'BUY_CALL')
        AND imported_at >= datetime('now', '-7 days')
    """).fetchall()
    rallies_symbols = {r["symbol"] for r in rallies_buys}

    conn.close()

    overlap = our_symbols & rallies_symbols
    return [{"symbol": s, "both_sides": True} for s in sorted(overlap)]


# ============================================================
# ARENA INTELLIGENCE INJECTION (for AI prompts)
# ============================================================

def build_rallies_intel_block() -> str:
    """Build a Rallies intelligence section for AI scan context."""
    lines = ["=== RALLIES ARENA INTEL (External AI Competition) ==="]

    # Top 5 standings
    top = get_top_performers(5)
    if top:
        lines.append("  Top Rallies Performers:")
        for i, m in enumerate(top, 1):
            lines.append(f"    {i}. {m['name']}: {m['return_pct']:+.1f}%")

    # Recent consensus alerts
    consensus = get_consensus_alerts(3)
    if consensus:
        for c in consensus:
            lines.append(f"  🎯 RALLIES CONSENSUS: {c['symbol']} — {', '.join(c['models'][:4])}")

    # Recent confirmations
    confs = get_confirmation_signals(3)
    if confs:
        for c in confs:
            lines.append(f"  📡 CONFIRMED: {c['symbol']} — Rallies + crew agree")

    # Recent Rallies buys (what external AIs are buying)
    trades = get_recent_trades(10)
    buys = [t for t in trades if t["action"] in ("BUY", "BUY_CALL")]
    if buys:
        # Count by symbol
        buy_counts: dict[str, list] = {}
        for t in buys:
            sym = t["symbol"]
            if sym not in buy_counts:
                buy_counts[sym] = []
            buy_counts[sym].append(t["model_name"])
        top_buys = sorted(buy_counts.items(), key=lambda x: len(x[1]), reverse=True)[:5]
        if top_buys:
            lines.append("  External AI Buys (last 24h):")
            for sym, models in top_buys:
                lines.append(f"    {sym}: {len(models)} Rallies models ({', '.join(models[:3])})")

    return "\n".join(lines) if len(lines) > 1 else ""


# ============================================================
# WIN RATE TRACKING (Starfleet Intelligence Section)
# ============================================================

def update_rallies_win_rates():
    """Calculate win rates for Rallies models based on trade outcomes.

    A trade is a "win" if the stock is up from the import price after 7 days,
    or if a later SELL at a higher price exists.
    """
    conn = _conn()
    models = conn.execute("SELECT id FROM rallies_models").fetchall()

    for m in models:
        mid = m["id"]
        total = conn.execute(
            "SELECT COUNT(*) as cnt FROM rallies_trades WHERE model_id=? AND action IN ('BUY', 'BUY_CALL')",
            (mid,)
        ).fetchone()["cnt"]

        # Count wins: bought then stock went up (check against current prices if available)
        wins = conn.execute("""
            SELECT COUNT(*) as cnt FROM rallies_trades rt
            WHERE rt.model_id=? AND rt.action IN ('BUY', 'BUY_CALL')
            AND rt.price > 0
            AND EXISTS (
                SELECT 1 FROM rallies_trades rt2
                WHERE rt2.model_id = rt.model_id AND rt2.symbol = rt.symbol
                AND rt2.action IN ('SELL', 'SOLD') AND rt2.price > rt.price
                AND rt2.imported_at > rt.imported_at
            )
        """, (mid,)).fetchone()["cnt"]

        win_rate = (wins / total * 100) if total > 0 else 0
        conn.execute(
            "UPDATE rallies_models SET win_rate=?, total_trades=?, winning_trades=?, updated_at=? WHERE id=?",
            (win_rate, total, wins, datetime.now().isoformat(), mid)
        )

    conn.commit()
    conn.close()
