"""AI4Trade.ai Signal Importer — pulls AI trading signals as reference data.

Fetches signals from ai4trade.ai API and stores in reference_trades table.
Crypto-focused (BNB, ETH, SOL) but reasoning patterns (entries, exits,
risk management, regime thinking) are valuable across markets.

Schedule: Sundays 8 PM MST (before Weekly Tuning at 9 PM).
Also callable manually via POST /api/reference/import-ai4trade.
"""
from __future__ import annotations
import os
import sqlite3
import requests
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"
API_BASE = "https://ai4trade.ai/api"
SOURCE = "ai4trade.ai"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def _get_token():
    return os.getenv("AI4TRADE_TOKEN")


def fetch_signals(limit: int = 100) -> list:
    """Fetch signals from ai4trade.ai API."""
    token = _get_token()
    if not token:
        console.log("[yellow]AI4Trade: No API token configured (AI4TRADE_TOKEN)")
        return []

    try:
        r = requests.get(
            f"{API_BASE}/signals/feed",
            params={"limit": limit},
            headers={"Authorization": f"Bearer {token}"},
            timeout=30,
        )
        if r.status_code == 401:
            console.log("[red]AI4Trade: Authentication failed — check AI4TRADE_TOKEN")
            return []
        if not r.ok:
            console.log(f"[red]AI4Trade: API returned {r.status_code}")
            return []

        data = r.json()
        return data.get("signals", data if isinstance(data, list) else [])
    except Exception as e:
        console.log(f"[red]AI4Trade fetch error: {e}")
        return []


def import_signals(limit: int = 100) -> dict:
    """Fetch and import signals into reference_trades. Returns summary."""
    signals = fetch_signals(limit)
    if not signals:
        return {"imported": 0, "skipped": 0, "error": "No signals fetched"}

    conn = _conn()
    imported = 0
    skipped = 0

    for sig in signals:
        agent = sig.get("agent_name") or ""
        symbol = (sig.get("symbol") or "").upper()
        side = (sig.get("side") or "").upper()
        content = sig.get("content") or ""
        entry_price = sig.get("entry_price")
        exit_price = sig.get("exit_price")
        qty = sig.get("quantity")
        pnl = sig.get("pnl")
        signal_type = sig.get("signal_type") or ""
        market = sig.get("market") or "crypto"
        signal_id = sig.get("signal_id") or sig.get("id")

        # Map side to action
        action = "BUY" if side == "BUY" else "SELL" if side == "SELL" else "HOLD"

        # Use executed_at or created_at as timestamp
        traded_at = sig.get("executed_at") or sig.get("created_at") or ""

        # Skip signals without meaningful data
        if not agent and not symbol and not content:
            skipped += 1
            continue

        # For strategy signals, symbol may be in the symbols array
        if not symbol and sig.get("symbols"):
            symbol = sig["symbols"][0].upper() if sig["symbols"] else ""

        # Deduplicate by signal_id
        if signal_id:
            existing = conn.execute(
                "SELECT 1 FROM reference_trades WHERE source=? AND reasoning LIKE ?",
                (SOURCE, f"%signal_id:{signal_id}%")
            ).fetchone()
            if existing:
                skipped += 1
                continue

        # Build reasoning with metadata
        reasoning_parts = [content]
        if signal_type:
            reasoning_parts.append(f"[type:{signal_type}]")
        if market:
            reasoning_parts.append(f"[market:{market}]")
        if signal_id:
            reasoning_parts.append(f"[signal_id:{signal_id}]")
        if exit_price:
            reasoning_parts.append(f"[exit:${exit_price}]")

        # Calculate P&L if we have entry and exit
        if entry_price and exit_price and not pnl:
            if action == "BUY":
                pnl = (exit_price - entry_price) * (qty or 1)
            elif action == "SELL":
                pnl = (entry_price - exit_price) * (qty or 1)

        pnl_pct = None
        if pnl and entry_price and qty:
            cost = entry_price * qty
            if cost > 0:
                pnl_pct = round(pnl / cost * 100, 2)

        conn.execute("""
            INSERT INTO reference_trades
            (source, model_name, symbol, action, price, qty, reasoning,
             confidence, pnl, pnl_pct, traded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            SOURCE, agent, symbol, action,
            entry_price, qty,
            " ".join(reasoning_parts)[:2000],
            None,  # confidence not in API
            pnl, pnl_pct, traded_at,
        ))
        imported += 1

    # Also import discussions (signals with replies)
    disc_imported = 0
    for sig in signals:
        if sig.get("reply_count", 0) > 0 and sig.get("signal_id"):
            try:
                replies = _fetch_replies(sig["signal_id"])
                for reply in replies:
                    conn.execute("""
                        INSERT INTO reference_discussions
                        (source, model_name, reply_text, sentiment)
                        VALUES (?, ?, ?, ?)
                    """, (
                        SOURCE,
                        reply.get("agent_name") or "unknown",
                        (reply.get("content") or "")[:2000],
                        "neutral",
                    ))
                    disc_imported += 1
            except Exception:
                pass

    conn.commit()
    conn.close()

    console.log(
        f"[green]AI4Trade import: {imported} signals, {disc_imported} discussions "
        f"({skipped} skipped) from {SOURCE}"
    )

    return {
        "source": SOURCE,
        "imported": imported,
        "discussions": disc_imported,
        "skipped": skipped,
        "total_fetched": len(signals),
    }


def _fetch_replies(signal_id: int) -> list:
    """Fetch replies/discussions for a signal."""
    token = _get_token()
    if not token:
        return []
    try:
        r = requests.get(
            f"{API_BASE}/signals/{signal_id}/replies",
            headers={"Authorization": f"Bearer {token}"},
            timeout=15,
        )
        if r.ok:
            data = r.json()
            return data.get("replies", data if isinstance(data, list) else [])
    except Exception:
        pass
    return []


if __name__ == "__main__":
    from dotenv import load_dotenv
    load_dotenv(override=True)
    result = import_signals(50)
    print(f"Result: {result}")
