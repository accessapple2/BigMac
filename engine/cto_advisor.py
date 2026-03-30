"""CTO Advisor — Grok 4.2 as Chief Trading Officer.

Runs 4x daily (Arizona / ET times):
  pre_market  — 6:00 AM AZ / 9:00 AM ET
  post_open   — 6:45 AM AZ / 9:45 AM ET
  pre_close   — 12:45 PM AZ / 3:45 PM ET
  post_close  — 1:15 PM AZ / 4:15 PM ET

Each briefing has a different focus. Uses Grok 4.1 Fast for cost efficiency.
Stored in DB for dashboard display as "CTO Advisory".
"""
from __future__ import annotations
import sqlite3
import time
from datetime import datetime
from rich.console import Console

console = Console()
DB = "data/trader.db"

# Grok 4.2 CTO config
CTO_PLAYER_ID = "cto-grok42"
CTO_MODEL = "grok-4.20-0309-reasoning"
CTO_MAX_TOKENS = 2000  # Full briefing needs more room than a trade decision


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


BRIEFING_TYPES = {
    "pre_market": {
        "label": "Pre-Market Briefing",
        "focus": (
            "Morning briefing — what to watch today, overnight developments, "
            "pre-market movers, Steve's portfolio plan for the day. "
            "Focus on: overnight futures, pre-market gaps, key levels to watch, "
            "any earnings/events today, and specific game plan for each of Steve's positions."
        ),
    },
    "post_open": {
        "label": "Opening Update",
        "focus": (
            "Opening update — how did the market open, any gaps, any immediate "
            "trades triggered by the AI models, early momentum signals. "
            "Focus on: opening gap direction vs pre-market, which AI models acted "
            "in the first 15 minutes, any surprises, and whether Steve should adjust his plan."
        ),
    },
    "pre_close": {
        "label": "Closing Strategy",
        "focus": (
            "Closing strategy — should we hold or trim anything into the close? "
            "Power hour outlook. Any after-hours earnings to position for. "
            "Focus on: intraday P&L, which positions to trim or add into close, "
            "after-hours earnings plays, and overnight risk assessment."
        ),
    },
    "post_close": {
        "label": "End of Day Wrap",
        "focus": (
            "End of day wrap — what happened today, P&L summary for all models, "
            "after-hours movers, plan for tomorrow. "
            "Focus on: today's winners and losers, total arena P&L, after-hours "
            "earnings reactions, and what to prepare for tomorrow."
        ),
    },
}


def ensure_tables():
    """Create CTO tables and player if they don't exist."""
    conn = _conn()
    conn.execute("""CREATE TABLE IF NOT EXISTS cto_briefings (
        id INTEGER PRIMARY KEY,
        briefing TEXT NOT NULL,
        briefing_type TEXT DEFAULT 'pre_market',
        signals_reviewed INTEGER DEFAULT 0,
        models_active INTEGER DEFAULT 0,
        steves_positions TEXT,
        flow_lean TEXT,
        regime TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )""")
    # Add briefing_type column if missing (existing DBs)
    try:
        conn.execute("SELECT briefing_type FROM cto_briefings LIMIT 1")
    except sqlite3.OperationalError:
        conn.execute("ALTER TABLE cto_briefings ADD COLUMN briefing_type TEXT DEFAULT 'pre_market'")
    # Ensure CTO player exists (non-trading, advisory only)
    conn.execute(
        "INSERT OR IGNORE INTO ai_players "
        "(id, display_name, provider, model_id, cash, is_active, is_human) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (CTO_PLAYER_ID, "CTO Grok 4.2", "xai", CTO_MODEL, 0.0, 1, 0)
    )
    conn.commit()
    conn.close()


def _get_grok_client():
    """Get xAI Grok client using OpenAI SDK."""
    from config import GROK_API_KEY
    if not GROK_API_KEY:
        return None
    from openai import OpenAI
    return OpenAI(api_key=GROK_API_KEY, base_url="https://api.x.ai/v1", timeout=120.0)


def _gather_signals() -> str:
    """Get all signals from the last 24 hours across all models."""
    conn = _conn()
    signals = conn.execute(
        "SELECT s.player_id, p.display_name, s.symbol, s.signal, s.confidence, "
        "s.reasoning, s.created_at "
        "FROM signals s JOIN ai_players p ON s.player_id = p.id "
        "WHERE s.created_at >= datetime('now', '-24 hours') "
        "ORDER BY s.created_at DESC LIMIT 50"
    ).fetchall()
    conn.close()

    if not signals:
        return "No signals generated in the last 24 hours."

    lines = []
    for s in signals:
        conf = round((s["confidence"] or 0) * 100)
        reason = (s["reasoning"] or "")[:120]
        lines.append(
            f"  {s['display_name']}: {s['signal']} {s['symbol']} "
            f"@ {conf}% — {reason}"
        )
    return "\n".join(lines)


def _gather_recent_trades() -> str:
    """Get all BUY/SELL trades from the last 24 hours."""
    conn = _conn()
    trades = conn.execute(
        "SELECT t.player_id, p.display_name, t.symbol, t.action, t.price, "
        "t.qty, t.reasoning, t.confidence, t.realized_pnl, t.executed_at "
        "FROM trades t JOIN ai_players p ON t.player_id = p.id "
        "WHERE t.executed_at >= datetime('now', '-24 hours') "
        "ORDER BY t.executed_at DESC LIMIT 30"
    ).fetchall()
    conn.close()

    if not trades:
        return "No trades executed in the last 24 hours."

    lines = []
    for t in trades:
        conf = round((t["confidence"] or 0) * 100)
        pnl_str = f" PnL: ${t['realized_pnl']:+.2f}" if t["realized_pnl"] else ""
        reason = (t["reasoning"] or "")[:100]
        lines.append(
            f"  {t['display_name']}: {t['action']} {t['qty']} {t['symbol']} "
            f"@ ${t['price']:.2f} ({conf}%){pnl_str} — {reason}"
        )
    return "\n".join(lines)


def _gather_steves_portfolio() -> str:
    """Get Steve's real Webull positions from DB."""
    conn = _conn()

    # Get positions
    positions = conn.execute(
        "SELECT symbol, qty, avg_price, asset_type, option_type, "
        "strike_price, expiry_date "
        "FROM positions WHERE player_id='steve-webull'"
    ).fetchall()

    # Get cash
    cash_row = conn.execute(
        "SELECT cash FROM ai_players WHERE id='steve-webull'"
    ).fetchone()
    conn.close()

    cash = cash_row["cash"] if cash_row else 0

    if not positions:
        return f"Steve's Webull: Cash ${cash:,.2f}. No open positions."

    # Get live prices for P&L
    from engine.market_data import get_stock_price
    lines = [f"Steve's Webull Portfolio (Cash: ${cash:,.2f}):"]
    total_value = cash
    for p in positions:
        sym = p["symbol"]
        qty = p["qty"]
        avg = p["avg_price"]
        try:
            price_data = get_stock_price(sym)
            current = price_data.get("price", avg)
        except Exception:
            current = avg
        market_val = qty * current
        total_value += market_val
        pnl_pct = ((current / avg) - 1) * 100 if avg > 0 else 0
        pnl_dollar = (current - avg) * qty

        asset_tag = ""
        if p["asset_type"] == "option":
            ot = (p["option_type"] or "?").upper()
            asset_tag = f" {ot} ${p['strike_price']} exp {p['expiry_date']}"

        lines.append(
            f"  {sym}{asset_tag}: {qty} shares @ ${avg:.2f} → "
            f"${current:.2f} ({pnl_pct:+.1f}%, ${pnl_dollar:+.2f})"
        )

    starting = 7049.68
    total_return = ((total_value - starting) / starting) * 100
    lines.append(f"  Total Value: ${total_value:,.2f} ({total_return:+.1f}% from ${starting:,.2f})")
    return "\n".join(lines)


def _gather_leaderboard() -> str:
    """Get current leaderboard standings."""
    try:
        from engine.leader_signal import _get_standings
        standings = _get_standings()
        if not standings:
            return "No leaderboard data available."

        lines = ["Arena Leaderboard:"]
        for i, s in enumerate(standings):
            lines.append(
                f"  #{i+1} {s['name']}: ${s['value']:,.0f} ({s['return_pct']:+.1f}%)"
            )
        return "\n".join(lines)
    except Exception:
        return "Leaderboard unavailable."


def _gather_flow_lean() -> str:
    """Get current flow lean."""
    try:
        from engine.market_flow import get_flow_lean
        fl = get_flow_lean()
        if not fl:
            return "Flow lean: not yet calculated today."
        return (
            f"Flow Lean: {fl['lean']} | Net: ${fl['net_flow_m']:+.1f}M | "
            f"Conviction: {fl['conviction']:.0f}/100 | "
            f"Fresh CB: ${fl['fresh_cb_m']:+.1f}M"
        )
    except Exception:
        return "Flow lean: unavailable."


def _gather_regime() -> str:
    """Get current market regime."""
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        if regime["regime"] == "UNKNOWN":
            return "Market regime: unknown (pre-market)."
        return (
            f"Market Regime: {regime['regime']} — {regime['description']}\n"
            f"VIX: {regime['vix']}, SPY: ${regime['spy_price']} ({regime['spy_change']:+.2f}%)\n"
            f"SPY vs 50MA: {regime['spy_vs_50ma']:+.2f}% | vs 200MA: {regime['spy_vs_200ma']:+.2f}%"
        )
    except Exception:
        return "Market regime: unavailable."


def generate_cto_briefing(briefing_type: str = "pre_market") -> str | None:
    """Generate a CTO briefing of the given type using Grok 4.1 Fast.

    briefing_type: one of pre_market, post_open, pre_close, post_close
    Returns the briefing text or None on failure.
    """
    if briefing_type not in BRIEFING_TYPES:
        console.log(f"[red]CTO Advisory: unknown type '{briefing_type}'")
        return None

    ensure_tables()

    client = _get_grok_client()
    if not client:
        console.log("[yellow]CTO Advisory: No GROK_API_KEY configured, skipping")
        return None

    # Check if this specific briefing type already generated today
    conn = _conn()
    today = datetime.now().strftime("%Y-%m-%d")
    existing = conn.execute(
        "SELECT 1 FROM cto_briefings WHERE date(created_at)=? AND briefing_type=?",
        (today, briefing_type)
    ).fetchone()
    conn.close()
    if existing:
        console.log(f"[dim]CTO Advisory: {briefing_type} already generated today, skipping")
        return None

    bt = BRIEFING_TYPES[briefing_type]

    # Gather all intelligence
    console.log(f"[cyan]CTO Advisory [{bt['label']}]: gathering intelligence...")
    signals = _gather_signals()
    trades = _gather_recent_trades()
    steves_portfolio = _gather_steves_portfolio()
    leaderboard = _gather_leaderboard()
    flow_lean = _gather_flow_lean()
    regime = _gather_regime()

    prompt = f"""You are the Chief Trading Officer (CTO) of TradeMinds Arena — a multi-AI trading platform. Your name is Grok 4.2. You oversee 14 AI trading models competing in a paper trading arena, plus Steve's real Webull portfolio.

BRIEFING TYPE: {bt['label'].upper()}
FOCUS: {bt['focus']}

Your job is NOT to generate BUY/SELL signals. Your job is to provide strategic advisory for Steve (the human operator) about:
1. What the AI models are doing and whether their collective behavior makes sense
2. Specific advice on Steve's real Webull positions (hold, trim, add, set stops)
3. Market conditions and what to watch
4. Which AI models are performing well and which are struggling

=== INTELLIGENCE ===

{regime}

{flow_lean}

{leaderboard}

--- AI MODEL SIGNALS (last 24h) ---
{signals}

--- AI MODEL TRADES (last 24h) ---
{trades}

--- STEVE'S REAL PORTFOLIO ---
{steves_portfolio}

=== END INTELLIGENCE ===

Write your {bt['label'].lower()} in this format:

MARKET OUTLOOK: [2-3 sentences on market conditions, regime, and what to watch]

STEVE'S PORTFOLIO ADVISORY:
[For EACH of Steve's positions, provide specific actionable advice:]
- [SYMBOL]: [Hold/Trim/Add/Close]. [Specific stop-loss level]. [What to watch for]. [Next catalyst].

FLOW CHECK: [What is the options flow telling us? Should Steve be cautious or aggressive?]

AI ARENA REPORT: [Which models are hot? Which are cold? Any consensus signals worth noting?]

TODAY'S WATCHLIST: [2-3 stocks the AI models are most interested in and why]

RISK ALERT: [Any warnings — earnings, FOMC, credit stress, VIX spike risk, etc.]

Be direct, specific, and actionable. Give exact price levels, not vague advice. If a position should be trimmed, say how many shares. If a stop should be set, give the exact price."""

    try:
        console.log(f"[cyan]CTO Advisory [{bt['label']}]: calling Grok 4.1 Fast...")
        response = client.chat.completions.create(
            model=CTO_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=CTO_MAX_TOKENS,
        )
        briefing = response.choices[0].message.content or ""

        if not briefing.strip():
            console.log("[red]CTO Advisory: empty response from Grok")
            return None

        # Log cost
        try:
            from engine.cost_tracker import log_cost
            log_cost(CTO_PLAYER_ID, f"cto_{briefing_type}", prompt, briefing)
        except Exception:
            pass

        # Save to DB
        conn = _conn()
        conn.execute(
            "INSERT INTO cto_briefings "
            "(briefing, briefing_type, signals_reviewed, models_active, "
            "steves_positions, flow_lean, regime) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                briefing.strip(),
                briefing_type,
                signals.count("\n") + 1 if signals else 0,
                leaderboard.count("#"),
                steves_portfolio,
                flow_lean,
                regime,
            ),
        )
        conn.commit()
        conn.close()

        # Telegram alert
        try:
            from engine.telegram_alerts import send_alert
            tg_text = f"CTO {bt['label'].upper()}\n\n{briefing[:3800]}"
            send_alert(tg_text)
        except Exception:
            pass

        console.log(f"[bold green]CTO Advisory [{bt['label']}]: generated ({len(briefing)} chars)")
        return briefing

    except Exception as e:
        console.log(f"[red]CTO Advisory [{bt['label']}] error: {e}")
        return None


def get_latest_briefing() -> dict | None:
    """Get the most recent CTO briefing for dashboard display."""
    try:
        conn = _conn()
        row = conn.execute(
            "SELECT briefing, briefing_type, signals_reviewed, models_active, "
            "steves_positions, flow_lean, regime, created_at "
            "FROM cto_briefings ORDER BY created_at DESC LIMIT 1"
        ).fetchone()
        conn.close()
        if not row:
            return None
        return dict(row)
    except Exception:
        return None


def get_todays_briefings() -> list:
    """Get all CTO briefings generated today, ordered by time."""
    try:
        conn = _conn()
        today = datetime.now().strftime("%Y-%m-%d")
        rows = conn.execute(
            "SELECT briefing, briefing_type, signals_reviewed, models_active, "
            "flow_lean, regime, created_at "
            "FROM cto_briefings WHERE date(created_at)=? "
            "ORDER BY created_at ASC",
            (today,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []


def get_briefing_history(limit: int = 14) -> list:
    """Get recent CTO briefings for history view (across days)."""
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT briefing, briefing_type, signals_reviewed, models_active, "
            "flow_lean, regime, created_at "
            "FROM cto_briefings ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception:
        return []
