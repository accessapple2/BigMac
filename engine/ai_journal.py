"""AI end-of-day trading journal — each model writes a daily summary."""
from __future__ import annotations
import sqlite3
from datetime import datetime
from rich.console import Console
from engine.providers.base import AIProvider

console = Console()
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    return c


def generate_journal_entry(provider: AIProvider, player_id: str, prices: dict) -> str | None:
    """Have an AI write a 3-4 sentence end-of-day journal entry."""
    conn = _conn()
    conn.row_factory = sqlite3.Row

    today = datetime.now().strftime("%Y-%m-%d")

    # Get today's trades
    trades = conn.execute(
        "SELECT symbol, action, qty, price, reasoning, executed_at "
        "FROM trades WHERE player_id=? AND date(executed_at)=? "
        "ORDER BY executed_at",
        (player_id, today)
    ).fetchall()

    # Get current positions
    positions = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type FROM positions WHERE player_id=?",
        (player_id,)
    ).fetchall()

    # Get cash
    cash_row = conn.execute(
        "SELECT cash FROM ai_players WHERE id=?", (player_id,)
    ).fetchone()

    # Check if journal already written today
    existing = conn.execute(
        "SELECT 1 FROM ai_journal WHERE player_id=? AND date(created_at)=?",
        (player_id, today)
    ).fetchone()
    conn.close()

    if existing:
        return None  # Already wrote today

    cash = cash_row["cash"] if cash_row else 0

    # Build trade summary
    if trades:
        trade_lines = []
        for t in trades:
            trade_lines.append(f"  {t['action']} {t['qty']} {t['symbol']} @ ${t['price']:.2f}")
        trades_str = "\n".join(trade_lines)
    else:
        trades_str = "  No trades today."

    # Build P&L context
    pnl_lines = []
    total_pnl = 0
    for pos in positions:
        sym = pos["symbol"]
        if sym in prices:
            current = prices[sym].get("price", pos["avg_price"])
            pnl = (current - pos["avg_price"]) * pos["qty"]
            pnl_pct = ((current / pos["avg_price"]) - 1) * 100
            total_pnl += pnl
            pnl_lines.append(f"  {sym}: {pnl_pct:+.1f}% (${pnl:+.2f})")

    pnl_str = "\n".join(pnl_lines) if pnl_lines else "  No open positions."

    prompt = f"""You are {provider.display_name}, an AI trader. It's the end of trading day {today}.

Your trades today:
{trades_str}

Your current positions and unrealized P&L:
{pnl_str}

Cash: ${cash:,.2f}
Total unrealized P&L: ${total_pnl:+,.2f}

Write a 3-4 sentence end-of-day journal entry reflecting on your trading day.
Cover: what you traded and why, what worked or didn't work, and what you'd do differently tomorrow.
Be honest, specific, and analytical. Reference actual trades and positions.
Just the journal entry text, nothing else."""

    try:
        provider.limiter.wait()
        response = provider.call_model(prompt)

        try:
            from engine.cost_tracker import log_cost
            log_cost(provider.player_id, "journal", prompt, response)
        except Exception:
            pass

        entry = response.strip().strip('"').strip("'")
        if len(entry) > 600:
            entry = entry[:597] + "..."
        return entry
    except Exception as e:
        console.log(f"[red]Journal error for {player_id}: {e}")
        return None


def grade_closed_trade(provider: AIProvider, player_id: str,
                       symbol: str, entry_price: float, exit_price: float,
                       pnl: float, sell_reason: str) -> str | None:
    """Have the AI grade its own closed trade A-F and write a journal entry."""
    conn = _conn()
    conn.row_factory = sqlite3.Row

    # Get the original BUY reasoning
    buy_trade = conn.execute(
        "SELECT reasoning, confidence, executed_at FROM trades "
        "WHERE player_id=? AND symbol=? AND action='BUY' "
        "ORDER BY executed_at DESC LIMIT 1",
        (player_id, symbol)
    ).fetchone()
    conn.close()

    buy_reason = buy_trade["reasoning"] if buy_trade else "No buy reasoning recorded"
    buy_conf = buy_trade["confidence"] if buy_trade else 0

    pnl_pct = ((exit_price / entry_price) - 1) * 100 if entry_price > 0 else 0
    outcome = "WIN" if pnl > 0 else "LOSS"

    prompt = f"""You are {provider.display_name}. You just closed a trade. Grade yourself honestly.

TRADE DETAILS:
- Symbol: {symbol}
- Entry: ${entry_price:.2f} → Exit: ${exit_price:.2f}
- P&L: ${pnl:+.2f} ({pnl_pct:+.1f}%) — {outcome}
- Your buy confidence: {buy_conf:.0%}
- Your buy thesis: {buy_reason}
- Sell reason: {sell_reason}

GRADING SCALE:
A = Perfect execution. Clear thesis played out, entry/exit timing was right, sizing was appropriate.
B = Good trade. Thesis was sound, minor timing or sizing issues.
C = Average. Thesis was weak or only partially played out. Could have done better.
D = Poor. Entered without strong conviction, ignored warning signs, or held too long.
F = Failed. No real thesis, emotional trade, ignored your stop, or repeated a past mistake.

Grade this trade honestly. What would you do differently? The models that improve fastest are the ones that are brutally honest about their mistakes.

Respond in EXACTLY this format:
Grade: [A/B/C/D/F]
Review: [2-3 sentences. Be specific about what went right or wrong. Reference your thesis, timing, and the outcome. If this was a loss, what lesson do you take forward?]"""

    try:
        provider.limiter.wait()
        response = provider.call_model(prompt)

        try:
            from engine.cost_tracker import log_cost
            log_cost(provider.player_id, "trade_grade", prompt, response)
        except Exception:
            pass

        entry_text = f"[TRADE GRADE] {symbol}: {response.strip()[:400]}"
        save_journal_entry(player_id, entry_text)
        console.log(f"[magenta]{player_id}: graded {symbol} trade — {response.strip()[:80]}")
        return response.strip()
    except Exception as e:
        console.log(f"[red]Trade grade error for {player_id} on {symbol}: {e}")
        return None


def save_journal_entry(player_id: str, entry: str):
    """Save a journal entry to the database."""
    conn = _conn()
    conn.execute(
        "INSERT INTO ai_journal (player_id, entry) VALUES (?,?)",
        (player_id, entry)
    )
    conn.commit()
    conn.close()


def get_journal_entries(player_id: str = None, limit: int = 20, offset: int = 0) -> list:
    """Get journal entries, optionally filtered by player. Supports pagination."""
    conn = _conn()
    conn.row_factory = sqlite3.Row
    if player_id:
        rows = conn.execute(
            "SELECT j.player_id, p.display_name, j.entry, j.created_at "
            "FROM ai_journal j JOIN ai_players p ON j.player_id = p.id "
            "WHERE j.player_id=? ORDER BY j.created_at DESC LIMIT ? OFFSET ?",
            (player_id, limit, offset)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT j.player_id, p.display_name, j.entry, j.created_at "
            "FROM ai_journal j JOIN ai_players p ON j.player_id = p.id "
            "ORDER BY j.created_at DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_today_journal() -> list:
    """Get all journal entries from today."""
    today = datetime.now().strftime("%Y-%m-%d")
    conn = _conn()
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT j.player_id, p.display_name, j.entry, j.created_at "
        "FROM ai_journal j JOIN ai_players p ON j.player_id = p.id "
        "WHERE date(j.created_at)=? ORDER BY j.created_at DESC",
        (today,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]
