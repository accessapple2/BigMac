"""Reference Data Engine — imports external arena data as learning benchmarks.

Supports importing trade data from Rallies.ai (or any external arena) via:
  1. CSV upload (/api/reference/import-csv)
  2. JSON upload (/api/reference/import)
  3. Manual paste via UI

Model name mapping: external names → our crew IDs.
Comparison functions for learning system integration.
"""
from __future__ import annotations
import csv
import io
import json
import sqlite3
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Map external model names to our player IDs
MODEL_MAP = {
    # Rallies.ai names → our crew
    "gpt-4": "gpt-4o",
    "gpt-4o": "gpt-4o",
    "gpt-o3": "gpt-o3",
    "claude": "claude-sonnet",
    "codex": "claude-sonnet",
    "codex-prime": "claude-sonnet",
    "codex-scout": "claude-haiku",
    "claude-sonnet": "claude-sonnet",
    "claude-haiku": "claude-haiku",
    "gemini": "gemini-2.5-flash",
    "gemini-flash": "gemini-2.5-flash",
    "gemini-pro": "gemini-2.5-pro",
    "grok": "grok-4",
    "grok-4": "grok-4",
    "grok-3": "grok-3",
    "llama": "ollama-llama",
    "qwen": "ollama-qwen3",
    "gemma": "ollama-local",
    # Display name mapping
    "Spock": "grok-4",
    "Worf": "gemini-2.5-flash",
    "Arnold": "energy-arnold",
    "Sosnoff": "options-sosnoff",
}

# Reverse map for display
CREW_NAMES = {
    "grok-4": "Spock (Grok 4)",
    "gemini-2.5-flash": "Worf (Gemini Flash)",
    "gpt-4o": "GPT-4o",
    "gpt-o3": "GPT-o3",
    "claude-sonnet": "Codex Prime",
    "claude-haiku": "Codex Scout",
    "ollama-local": "Gemma3 4B",
    "energy-arnold": "Arnold Energy",
    "options-sosnoff": "Sosnoff Options",
}


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def map_model_name(external_name: str) -> str | None:
    """Map an external model name to our player_id."""
    if not external_name:
        return None
    name_lower = external_name.lower().strip()
    for key, value in MODEL_MAP.items():
        if key.lower() == name_lower:
            return value
    # Fuzzy match
    for key, value in MODEL_MAP.items():
        if key.lower() in name_lower or name_lower in key.lower():
            return value
    return None


def import_trades(trades: list, source: str = "rallies.ai") -> dict:
    """Import a list of trade dicts into reference_trades. Returns summary."""
    conn = _conn()
    imported = 0
    skipped = 0

    for t in trades:
        model = t.get("model_name") or t.get("model") or t.get("player") or ""
        symbol = (t.get("symbol") or t.get("ticker") or "").upper()
        action = (t.get("action") or t.get("signal") or "").upper()

        if not symbol or not action:
            skipped += 1
            continue

        # Check duplicate (same source + model + symbol + traded_at)
        traded_at = t.get("traded_at") or t.get("timestamp") or t.get("date") or ""
        existing = conn.execute(
            "SELECT 1 FROM reference_trades WHERE source=? AND model_name=? AND symbol=? AND traded_at=?",
            (source, model, symbol, traded_at)
        ).fetchone()
        if existing:
            skipped += 1
            continue

        conn.execute("""
            INSERT INTO reference_trades
            (source, model_name, symbol, action, price, qty, reasoning,
             confidence, outcome, pnl, pnl_pct, regime, traded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            source, model, symbol, action,
            t.get("price"), t.get("qty"),
            t.get("reasoning") or t.get("thesis") or "",
            t.get("confidence"), t.get("outcome"),
            t.get("pnl"), t.get("pnl_pct"),
            t.get("regime"), traded_at,
        ))
        imported += 1

    conn.commit()
    conn.close()
    console.log(f"[green]Reference import: {imported} trades imported, {skipped} skipped from {source}")
    return {"imported": imported, "skipped": skipped, "source": source}


def import_csv(csv_text: str, source: str = "rallies.ai") -> dict:
    """Import trades from CSV text. Auto-detects column mapping."""
    reader = csv.DictReader(io.StringIO(csv_text))
    trades = []
    for row in reader:
        trades.append({
            "model_name": row.get("model_name") or row.get("model") or row.get("player") or "",
            "symbol": row.get("symbol") or row.get("ticker") or "",
            "action": row.get("action") or row.get("signal") or "",
            "price": _safe_float(row.get("price")),
            "qty": _safe_float(row.get("qty") or row.get("quantity")),
            "reasoning": row.get("reasoning") or row.get("thesis") or "",
            "confidence": _safe_float(row.get("confidence")),
            "outcome": row.get("outcome") or row.get("result") or "",
            "pnl": _safe_float(row.get("pnl") or row.get("profit")),
            "pnl_pct": _safe_float(row.get("pnl_pct") or row.get("return_pct")),
            "regime": row.get("regime") or "",
            "traded_at": row.get("traded_at") or row.get("timestamp") or row.get("date") or "",
        })
    return import_trades(trades, source)


def get_reference_stats() -> dict:
    """Get summary stats of reference data."""
    conn = _conn()
    total = conn.execute("SELECT COUNT(*) as cnt FROM reference_trades").fetchone()["cnt"]
    by_source = conn.execute(
        "SELECT source, COUNT(*) as cnt FROM reference_trades GROUP BY source"
    ).fetchall()
    by_model = conn.execute(
        "SELECT model_name, COUNT(*) as cnt, AVG(pnl_pct) as avg_return "
        "FROM reference_trades GROUP BY model_name ORDER BY cnt DESC"
    ).fetchall()
    recent = conn.execute(
        "SELECT * FROM reference_trades ORDER BY imported_at DESC LIMIT 10"
    ).fetchall()
    conn.close()

    return {
        "total_trades": total,
        "by_source": [dict(r) for r in by_source],
        "by_model": [dict(r) for r in by_model],
        "recent": [dict(r) for r in recent],
    }


def compare_models(our_player_id: str) -> dict:
    """Compare our model's performance against reference data for the same LLM."""
    conn = _conn()

    # Find matching reference model names
    ref_names = []
    for ext_name, pid in MODEL_MAP.items():
        if pid == our_player_id:
            ref_names.append(ext_name)

    if not ref_names:
        conn.close()
        return {"match": False, "reason": f"No reference mapping for {our_player_id}"}

    # Our performance
    our_trades = conn.execute("""
        SELECT COUNT(*) as total,
               SUM(CASE WHEN action LIKE 'BUY%' THEN 1 ELSE 0 END) as buys,
               SUM(CASE WHEN action = 'SELL' THEN 1 ELSE 0 END) as sells
        FROM trades WHERE player_id = ?
    """, (our_player_id,)).fetchone()

    our_lessons = conn.execute("""
        SELECT grade, COUNT(*) as cnt FROM daily_lessons
        WHERE player_id = ? GROUP BY grade
    """, (our_player_id,)).fetchall()

    # Reference performance (match any mapped name)
    placeholders = ",".join("?" * len(ref_names))
    ref_trades = conn.execute(f"""
        SELECT COUNT(*) as total,
               AVG(pnl_pct) as avg_return,
               SUM(CASE WHEN pnl > 0 THEN 1 ELSE 0 END) as wins,
               SUM(CASE WHEN pnl <= 0 THEN 1 ELSE 0 END) as losses
        FROM reference_trades WHERE LOWER(model_name) IN ({placeholders})
    """, [n.lower() for n in ref_names]).fetchone()

    # Shared mistakes (same symbol + loss in both)
    ref_losers = conn.execute(f"""
        SELECT symbol, COUNT(*) as cnt FROM reference_trades
        WHERE LOWER(model_name) IN ({placeholders}) AND pnl < 0
        GROUP BY symbol ORDER BY cnt DESC LIMIT 10
    """, [n.lower() for n in ref_names]).fetchall()

    our_losers = conn.execute("""
        SELECT symbol, COUNT(*) as cnt FROM daily_lessons
        WHERE player_id = ? AND grade IN ('D', 'F')
        GROUP BY symbol ORDER BY cnt DESC LIMIT 10
    """, (our_player_id,)).fetchall()

    conn.close()

    ref_loser_syms = {r["symbol"] for r in ref_losers}
    our_loser_syms = {r["symbol"] for r in our_losers}
    shared_losers = ref_loser_syms & our_loser_syms
    our_only_losers = our_loser_syms - ref_loser_syms

    return {
        "match": True,
        "our_player_id": our_player_id,
        "our_name": CREW_NAMES.get(our_player_id, our_player_id),
        "ref_names": ref_names,
        "our_stats": dict(our_trades) if our_trades else {},
        "our_grades": {r["grade"]: r["cnt"] for r in our_lessons},
        "ref_stats": dict(ref_trades) if ref_trades else {},
        "shared_losers": list(shared_losers),
        "our_only_losers": list(our_only_losers),
        "pattern_overlap": len(shared_losers),
        "our_unique_issues": len(our_only_losers),
    }


def get_reference_strategies(limit: int = 10) -> list:
    """Get top-performing reference trades as strategy inspiration."""
    conn = _conn()
    rows = conn.execute("""
        SELECT model_name, symbol, action, reasoning, pnl_pct, traded_at
        FROM reference_trades
        WHERE pnl_pct > 0
        ORDER BY pnl_pct DESC LIMIT ?
    """, (limit,)).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_reference_for_learning(player_id: str, symbol: str = None) -> str:
    """Get reference context string for the learning engine to inject."""
    conn = _conn()

    ref_names = [k for k, v in MODEL_MAP.items() if v == player_id]
    if not ref_names:
        conn.close()
        return ""

    placeholders = ",".join("?" * len(ref_names))

    if symbol:
        ref = conn.execute(f"""
            SELECT model_name, action, pnl_pct, reasoning FROM reference_trades
            WHERE LOWER(model_name) IN ({placeholders}) AND symbol = ?
            ORDER BY traded_at DESC LIMIT 3
        """, [n.lower() for n in ref_names] + [symbol]).fetchall()
    else:
        ref = conn.execute(f"""
            SELECT model_name, symbol, action, pnl_pct, reasoning FROM reference_trades
            WHERE LOWER(model_name) IN ({placeholders})
            ORDER BY traded_at DESC LIMIT 5
        """, [n.lower() for n in ref_names]).fetchall()

    conn.close()

    if not ref:
        return ""

    ctx = "\n=== REFERENCE DATA (external arena) ===\n"
    for r in ref:
        pnl = r["pnl_pct"] or 0
        ctx += f"  {r['model_name']} {r.get('symbol','')} {r['action']}: {pnl:+.1f}%"
        if r["reasoning"]:
            ctx += f" — {r['reasoning'][:100]}"
        ctx += "\n"
    ctx += "=== END REFERENCE ===\n"
    return ctx


def _safe_float(val):
    if val is None or val == "":
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None
