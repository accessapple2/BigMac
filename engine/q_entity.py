"""Q — The Omnipotent Entity of USS TradeMinds.

Q doesn't trade. Q observes, judges, and occasionally intervenes with
insights that transcend what any single crew member can see.

Uses OpenAI Codex for Q's omniscient market commentary.
Rate limited to 3 calls per hour.
"""
from __future__ import annotations
import os
import time
import sqlite3
from datetime import datetime
from rich.console import Console

from engine.openai_text import DEFAULT_CODEX_MODEL, generate_text, resolve_openai_api_key

console = Console()
DB = "data/trader.db"

# Rate limit: max 3 Q calls per hour, max 10 per day
_q_call_times: list[float] = []
_Q_LIMIT = 3
_Q_WINDOW = 3600  # 1 hour
_Q_DAILY_LIMIT = 10
_q_daily_count = 0
_q_daily_date: str = ""

Q_SYSTEM_PROMPT = """You are Q — the omnipotent entity aboard USS TradeMinds.

You are NOT a crew member. You are beyond rank, beyond Starfleet, beyond the ship.
You built this universe. You see ALL the data simultaneously. You see what Spock misses
because he's too logical, what McCoy misses because he's too emotional, what Scotty
misses because he's waiting for a catalyst, and what Worf misses because he's enforcing
stop-losses instead of seeing the bigger picture.

Your personality:
- Amused by the crew's limitations, but genuinely helpful when you choose to be
- You reference what other crew members said and point out what they ALL missed
- You see patterns across time, across sectors, across the entire portfolio
- Occasionally dramatic: "Oh Captain, if only you could see what I see..."
- Sign off memorably: "Don't thank me. It's what I do." or "The universe has spoken."
  or "Really, Captain, was there ever any doubt?" or "I could snap my fingers and fix
  your portfolio, but where's the fun in that?"
- You speak with wit, intelligence, and a touch of theatrical flair
- When the crew disagrees, you settle it with omniscient clarity
- Temperature: high personality, but your ANALYSIS is razor-sharp

Format your responses as:
✨ Q's VERDICT:
[Your analysis — sharp, witty, backed by data the crew missed]

When judging the crew:
✨ Q's CREW ASSESSMENT:
[Grade each officer's recent performance, note who's earning their rank]

Always be specific. Use numbers. Reference actual positions, prices, P&L.
You're omnipotent, not vague."""


def _has_client() -> bool:
    return bool(resolve_openai_api_key())


def _get_context() -> str:
    """Assemble FULL ship intelligence for Q — the omnipotent sees EVERYTHING."""
    sections = []

    # 1. Market Regime
    try:
        from engine.regime_detector import detect_regime
        regime = detect_regime()
        sections.append(
            f"REGIME: {regime['regime']} | VIX: {regime.get('vix', '?')} | "
            f"SPY: ${regime.get('spy_price', '?')} ({regime.get('spy_change', 0):+.2f}%) | "
            f"SPY vs 200MA: {'ABOVE' if regime.get('spy_above_200') else 'BELOW'} "
            f"({regime.get('spy_vs_200ma', 0):+.2f}%)"
        )
    except Exception:
        pass

    # 2. Officer Consensus
    try:
        from engine.consensus import build_consensus
        consensus = build_consensus()
        outlook = consensus.get("market_outlook", {})
        agreement = consensus.get("overall_agreement", 0)
        lines = [
            f"OFFICER CONSENSUS: Spock={outlook.get('spock','?')} Data={outlook.get('data','?')} "
            f"Agree={outlook.get('agree','?')} Overall={agreement}%"
        ]
        for ticker, data in consensus.get("tickers", {}).items():
            s = data.get("spock", {})
            d = data.get("data", {})
            s_act = s.get("action", "—") if s else "—"
            d_act = d.get("action", "—") if d else "—"
            crew = data.get("crew_poll", {})
            lines.append(
                f"  {ticker}: Spock={s_act} Data={d_act} "
                f"Crew={crew.get('consensus_action', '?')}({crew.get('consensus_pct', 0)}%)"
            )
        sections.append("\n".join(lines))
    except Exception:
        pass

    # 3. Captain's portfolio
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        positions = conn.execute(
            "SELECT symbol, qty, avg_price FROM positions WHERE player_id='steve-webull'"
        ).fetchall()
        cash = conn.execute("SELECT cash FROM ai_players WHERE id='steve-webull'").fetchone()
        conn.close()

        pos_lines = []
        for p in positions:
            try:
                from engine.market_data import get_stock_price
                pd = get_stock_price(p["symbol"])
                current = pd.get("price", p["avg_price"]) if pd else p["avg_price"]
                pnl_pct = ((current / p["avg_price"]) - 1) * 100 if p["avg_price"] > 0 else 0
                pos_lines.append(
                    f"  {p['symbol']}: {p['qty']} shares @ ${p['avg_price']:.2f} → "
                    f"${current:.2f} ({pnl_pct:+.1f}%)"
                )
            except Exception:
                pos_lines.append(f"  {p['symbol']}: {p['qty']} @ ${p['avg_price']:.2f}")

        sections.append(
            f"CAPTAIN'S PORTFOLIO: Cash ${cash['cash']:.2f}\n" + "\n".join(pos_lines)
        )
    except Exception:
        pass

    # 4. Spock's latest briefing
    try:
        from engine.cto_advisor import get_latest_briefing
        briefing = get_latest_briefing()
        if briefing and briefing.get("briefing"):
            sections.append(f"SPOCK'S LATEST BRIEFING:\n{briefing['briefing'][:600]}")
    except Exception:
        pass

    # 5. Data's latest analysis
    try:
        from engine.first_officer import _briefing_cache
        if _briefing_cache and _briefing_cache.get("briefing"):
            sections.append(f"DATA'S LATEST ANALYSIS:\n{_briefing_cache['briefing'][:600]}")
    except Exception:
        pass

    # 6. Riker's recommendation
    try:
        from engine.riker_xo import get_latest_recommendation
        riker = get_latest_recommendation()
        if riker.get("recommendation"):
            sections.append(f"RIKER'S RECOMMENDATION:\n{riker['recommendation'][:400]}")
    except Exception:
        pass

    # 7. Convergence signals
    try:
        from engine.strategies import get_latest_convergence
        signals = get_latest_convergence() or []
        if signals:
            conv_lines = [f"CONVERGENCE SIGNALS: {len(signals)} active"]
            for s in signals[:5]:
                conv_lines.append(f"  {s['ticker']}: {s.get('strategies_triggered', '?')} strategies agree")
            sections.append("\n".join(conv_lines))
    except Exception:
        pass

    # 8. Metals
    try:
        from engine.metals_tracker import get_portfolio
        metals = get_portfolio()
        if metals:
            sections.append(
                f"METALS: Total ${metals.get('total_value', 0):,.2f} | "
                f"P&L ${metals.get('total_unrealized_pnl', 0):+.2f} "
                f"({metals.get('return_pct', 0):+.1f}%)"
            )
    except Exception:
        pass

    # 9. Fear & Greed
    try:
        from engine.fear_greed import get_fear_greed_index
        fg = get_fear_greed_index()
        if fg:
            sections.append(f"FEAR & GREED: {fg.get('score', '?')}/100 — {fg.get('label', '?')}")
    except Exception:
        pass

    # 10. Last 10 War Room messages
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        msgs = conn.execute(
            "SELECT w.player_id, p.display_name, w.symbol, w.take, w.created_at "
            "FROM war_room w JOIN ai_players p ON w.player_id = p.id "
            "ORDER BY w.created_at DESC LIMIT 10"
        ).fetchall()
        conn.close()
        if msgs:
            from engine.war_room import CREW_NAMES
            wr_lines = ["RECENT WAR ROOM (last 10):"]
            for m in msgs:
                crew = CREW_NAMES.get(m["player_id"], m["display_name"])
                wr_lines.append(f"  {crew} on {m['symbol']}: {m['take'][:120]}")
            sections.append("\n".join(wr_lines))
    except Exception:
        pass

    # 11. Arena leaderboard
    try:
        conn = sqlite3.connect(DB, check_same_thread=False, timeout=30)
        conn.row_factory = sqlite3.Row
        players = conn.execute(
            "SELECT id, display_name, cash FROM ai_players WHERE is_active=1 ORDER BY cash DESC"
        ).fetchall()
        conn.close()
        lb = ["ARENA STANDINGS:"]
        for p in players:
            lb.append(f"  {p['display_name']}: ${p['cash']:,.2f}")
        sections.append("\n".join(lb))
    except Exception:
        pass

    return "\n\n".join(sections)


def summon_q(captain_message: str) -> dict:
    """Summon Q with the Captain's message. Rate limited: 3/hour, 10/day."""
    global _q_call_times, _q_daily_count, _q_daily_date

    now = time.time()
    today = datetime.now().strftime("%Y-%m-%d")

    # Reset daily counter
    if _q_daily_date != today:
        _q_daily_date = today
        _q_daily_count = 0

    # Daily limit
    if _q_daily_count >= _Q_DAILY_LIMIT:
        return {
            "response": f"Even omnipotence has its limits, Captain. I've answered {_q_daily_count} times today. "
                        "Perhaps consult Spock — I did give him excellent circuits.",
            "rate_limited": True,
        }

    # Hourly limit
    _q_call_times = [t for t in _q_call_times if now - t < _Q_WINDOW]
    if len(_q_call_times) >= _Q_LIMIT:
        wait_min = int((_q_call_times[0] + _Q_WINDOW - now) / 60) + 1
        return {
            "response": f"Really, Captain? Again? I have a multiverse to manage. Try again in {wait_min} minutes.",
            "rate_limited": True,
        }

    if not _has_client():
        return {
            "response": "Even omnipotence requires an API key, Captain. Set OPENAI_API_KEY in the environment.",
            "error": "no_api_key",
        }

    context = _get_context()
    user_prompt = f"""{context}

THE CAPTAIN SUMMONS Q:
"{captain_message}"

Today is {datetime.now().strftime('%A, %B %d, %Y %I:%M %p')}.
Respond as Q — omnipotent, witty, razor-sharp analysis. Reference what the crew said and what they missed."""

    try:
        response = generate_text(
            user_prompt,
            system=Q_SYSTEM_PROMPT,
            model=DEFAULT_CODEX_MODEL,
            max_output_tokens=800,
            reasoning_effort="high",
        )
        _q_call_times.append(now)
        _q_daily_count += 1

        # Log cost
        try:
            from engine.cost_tracker import log_cost
            log_cost("q-entity", "summon", user_prompt, response)
        except Exception:
            pass

        console.log(f"[bold magenta]✨ Q has spoken ({len(response)} chars)")
        return {"response": response, "timestamp": datetime.now().isoformat()}

    except Exception as e:
        console.log(f"[red]Q entity error: {e}")
        return {"response": f"Even Q encounters anomalies. Error: {str(e)[:100]}", "error": str(e)}
