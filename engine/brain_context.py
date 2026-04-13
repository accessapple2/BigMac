"""brain_context.py — Comprehensive intelligence aggregator for AI scan prompts.

Aggregates intelligence from sources that complement base.py's build_prompt():
  - Fear & Greed index
  - Red Alert condition (GO / CAUTION / STAND DOWN)
  - Congress trades (symbol-specific)
  - Signal scorecard (recent scored signals for this symbol)
  - Fleet consensus (what other models decided recently for this symbol)
  - Backtest performance (this model's historical accuracy for this symbol)

build_full_context(player_id, symbol) → formatted text block for prompt injection
build_full_context_raw(player_id, symbol) → dict of raw data (for API endpoint)

Cache: 5-minute TTL per (player_id, symbol) pair.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any

# ── Cache ────────────────────────────────────────────────────────────────────
_cache: dict[tuple, dict] = {}
_cache_lock = threading.Lock()
_CACHE_TTL = 300  # 5 minutes


def _get_cached(player_id: str, symbol: str) -> dict | None:
    key = (player_id, symbol)
    with _cache_lock:
        entry = _cache.get(key)
        if entry and time.time() - entry["ts"] < _CACHE_TTL:
            return {**entry["data"], "cached": True}
    return None


def _set_cached(player_id: str, symbol: str, data: dict) -> None:
    key = (player_id, symbol)
    with _cache_lock:
        _cache[key] = {"data": data, "ts": time.time()}


def invalidate_cache() -> None:
    """Call between scan cycles to force fresh data on next call."""
    with _cache_lock:
        _cache.clear()


# ── Source functions ─────────────────────────────────────────────────────────

def _source_fear_greed() -> dict[str, Any] | None:
    """Fear & Greed composite score (0–100)."""
    try:
        from engine.fear_greed import get_fear_greed_index
        data = get_fear_greed_index()
        score = data.get("score")
        label = data.get("label", "Unknown")
        if score is None:
            return None
        if score < 25:
            context = "Extreme Fear — contrarian BUY signal for quality names."
        elif score < 45:
            context = "Fear zone — be selective, market is nervous."
        elif score < 55:
            context = "Neutral — no directional edge from sentiment."
        elif score < 75:
            context = "Greed zone — momentum names working, watch for blow-offs."
        else:
            context = "Extreme Greed — elevated risk of reversal. Be cautious."
        return {
            "label": "Fear & Greed",
            "score": round(score),
            "sentiment_label": label,
            "text": f"Market Sentiment: {label} ({score:.0f}/100). {context}",
        }
    except Exception:
        return None


def _source_red_alert() -> dict[str, Any] | None:
    """Current market condition from Red Alert monitor."""
    try:
        from engine.red_alert import get_current_condition
        cond = get_current_condition()
        condition = cond.get("condition", "UNKNOWN")
        score = cond.get("score", 0)
        alerts = cond.get("active_alerts", [])
        alert_str = ", ".join(alerts[:3]) if alerts else "none"
        action_map = {
            "GO": "All systems nominal. Normal position sizing.",
            "CAUTION": "Elevated risk. Reduce position sizes 30%. Avoid new options.",
            "STAND DOWN": "DANGER. Do NOT open new positions. Hold cash only.",
        }
        action = action_map.get(condition, "Monitor closely.")
        return {
            "label": "Red Alert",
            "condition": condition,
            "score": round(score, 3),
            "active_alerts": alerts,
            "text": (
                f"Combat Condition: {condition} (score: {score:.2f}). "
                f"Active alerts: {alert_str}. {action}"
            ),
        }
    except Exception:
        return None


def _source_congress_trades(symbol: str) -> dict[str, Any] | None:
    """Recent congressional stock trades for this symbol."""
    try:
        from engine.congress_scraper import get_congress_trades_for_ticker
        trades = get_congress_trades_for_ticker(symbol)
        if not trades:
            return None
        recent = trades[:5]
        lines = []
        buys = 0
        sells = 0
        for t in recent:
            name = t.get("politician") or t.get("name", "Unknown")
            action = str(t.get("transaction") or t.get("type", "?")).lower()
            amount = t.get("amount", "?")
            date_str = t.get("date") or t.get("traded", "?")
            lines.append(f"  {name}: {action} {amount} on {date_str}")
            if "buy" in action or "purchase" in action:
                buys += 1
            else:
                sells += 1
        if buys > sells:
            signal = "BULLISH — Congress buying."
        elif sells > buys:
            signal = "BEARISH — Congress selling."
        else:
            signal = "Mixed congressional signals."
        return {
            "label": "Congress Trades",
            "buys": buys,
            "sells": sells,
            "trades": [dict(t) for t in recent],
            "text": (
                f"Congressional activity in {symbol} ({buys} buys / {sells} sells — {signal}):\n"
                + "\n".join(lines)
            ),
        }
    except Exception:
        return None


def _source_signal_scorecard(symbol: str) -> dict[str, Any] | None:
    """Recent scored signals for this symbol from the signal scorecard."""
    try:
        from engine.signal_scorecard import get_scorecard
        signals = get_scorecard(limit=200)
        # Filter for this symbol with resolved outcomes
        sym_signals = [
            s for s in signals
            if s.get("symbol") == symbol and s.get("outcome_pct") is not None
        ]
        if len(sym_signals) < 2:
            return None
        recent = sym_signals[:10]
        wins = sum(1 for s in recent if (s.get("outcome_pct") or 0) > 0)
        losses = len(recent) - wins
        avg_pct = sum(s.get("outcome_pct", 0) for s in recent) / len(recent)
        lines = []
        for s in recent[:4]:
            ind = s.get("indicator", "?")
            outcome = s.get("outcome_pct", 0)
            tag = "WIN" if outcome > 0 else "LOSS"
            lines.append(f"  {ind}: {outcome:+.1f}% [{tag}]")
        if avg_pct > 1:
            verdict = "Signals historically profitable here."
        elif avg_pct < -1:
            verdict = "Signals historically lose money here — higher bar needed."
        else:
            verdict = "Signals roughly break-even here — rely on other factors."
        return {
            "label": "Signal Scorecard",
            "wins": wins,
            "losses": losses,
            "avg_outcome_pct": round(avg_pct, 2),
            "recent_signals": recent[:4],
            "text": (
                f"Historical signals for {symbol}: {wins}W/{losses}L, "
                f"avg outcome {avg_pct:+.1f}%.\n"
                + "\n".join(lines)
                + f"\n  {verdict}"
            ),
        }
    except Exception:
        return None


def _source_fleet_consensus(player_id: str, symbol: str) -> dict[str, Any] | None:
    """What other AI models decided for this symbol in the last 4 hours."""
    try:
        db = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT player_id, action, confidence
            FROM trades
            WHERE symbol = ? AND player_id != ?
              AND action IN ('BUY','BUY_CALL','BUY_PUT','SHORT','HOLD')
              AND executed_at >= datetime('now', '-4 hours')
            ORDER BY executed_at DESC
            LIMIT 30
        """, (symbol, player_id)).fetchall()
        db.close()

        if not rows:
            return None

        action_counts: dict[str, int] = {}
        for r in rows:
            a = r["action"]
            action_counts[a] = action_counts.get(a, 0) + 1

        total = len(rows)
        buy_n = action_counts.get("BUY", 0) + action_counts.get("BUY_CALL", 0)
        hold_n = action_counts.get("HOLD", 0)
        bear_n = action_counts.get("BUY_PUT", 0) + action_counts.get("SHORT", 0)

        bull_pct = round(buy_n / total * 100)
        hold_pct = round(hold_n / total * 100)
        bear_pct = round(bear_n / total * 100)
        top = max(bull_pct, hold_pct, bear_pct)

        if bull_pct >= 60:
            consensus = f"BULLISH ({bull_pct}% of fleet buying)"
        elif bear_pct >= 60:
            consensus = f"BEARISH ({bear_pct}% of fleet shorting/puts)"
        elif hold_pct >= 60:
            consensus = f"HOLD ({hold_pct}% of fleet passing)"
        else:
            consensus = f"MIXED (bull {bull_pct}% / hold {hold_pct}% / bear {bear_pct}%)"

        strength = "Strong alignment — high-confidence signal." if top >= 70 else "Divided fleet — use your own edge."
        return {
            "label": "Fleet Consensus",
            "total_models": total,
            "bull_pct": bull_pct,
            "hold_pct": hold_pct,
            "bear_pct": bear_pct,
            "text": (
                f"Fleet verdict on {symbol} ({total} models): {consensus}. {strength}"
            ),
        }
    except Exception:
        return None


def _source_backtest_performance(player_id: str, symbol: str) -> dict[str, Any] | None:
    """This model's historical closed-trade accuracy for this specific symbol."""
    try:
        db = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        db.row_factory = sqlite3.Row
        rows = db.execute("""
            SELECT realized_pnl
            FROM trades
            WHERE player_id = ? AND symbol = ? AND action = 'SELL'
              AND realized_pnl IS NOT NULL
            ORDER BY executed_at DESC
            LIMIT 20
        """, (player_id, symbol)).fetchall()
        db.close()

        if len(rows) < 2:
            return None

        wins = sum(1 for r in rows if (r["realized_pnl"] or 0) > 0)
        losses = len(rows) - wins
        total_pnl = sum(r["realized_pnl"] or 0 for r in rows)
        win_rate = round(wins / len(rows) * 100)

        if win_rate >= 60:
            verdict = "Keep doing what works here."
        elif win_rate < 40:
            verdict = "You tend to lose on this stock — raise conviction bar."
        else:
            verdict = "Even track record — size normally."

        return {
            "label": "Your History",
            "wins": wins,
            "losses": losses,
            "win_rate_pct": win_rate,
            "total_pnl": round(total_pnl, 2),
            "text": (
                f"Your history in {symbol}: {wins}W/{losses}L ({win_rate}% win rate), "
                f"total P&L ${total_pnl:+.2f}. {verdict}"
            ),
        }
    except Exception:
        return None


# ── FinMem layered memory ─────────────────────────────────────────────────────

def _source_layered_memory(player_id: str) -> dict[str, Any] | None:
    """3-layer FinMem memory for this agent (working/short-term/long-term)."""
    try:
        from engine.finmem_memory import build_layered_memory
        text = build_layered_memory(player_id)
        if not text.strip():
            return None
        return {"label": "Agent Memory", "text": text}
    except Exception:
        return None


def _source_news_sentiment(symbol: str) -> dict[str, Any] | None:
    """FinGPT-style news sentiment for this symbol (mistral:7b, 15-min cache)."""
    try:
        from engine.fingpt_sentiment import get_sentiment
        return get_sentiment(symbol)
    except Exception:
        return None


def _source_debate_intel() -> dict[str, Any] | None:
    """Latest TradingAgents debate result (cached from Bridge Vote morning run)."""
    try:
        from engine.debate_engine import get_latest_ta_debate
        d = get_latest_ta_debate()
        if not d:
            return None
        bull = d.get("bull_case", "")[:200]
        bear = d.get("bear_case", "")[:200]
        reasoning = d.get("reasoning", "")[:200]
        text = (
            f"TradingAgents Debate ({d['symbol']} {d['date']}): "
            f"Consensus {d['consensus']}. "
            f"Bull: {bull} | Bear: {bear} | Decision: {reasoning}"
        )
        return {"label": "Debate Intel", "consensus": d["consensus"], "text": text}
    except Exception:
        return None


def _source_bootstrap_intelligence(player_id: str, symbol: str) -> dict[str, Any] | None:
    """Historical win-rate intelligence from bootstrap_metrics table."""
    try:
        from engine.bootstrap_intelligence import get_agent_intelligence
        text = get_agent_intelligence(player_id, symbol)
        if not text or not text.strip():
            return None
        return {"label": "Bootstrap Intel", "text": text}
    except Exception:
        return None


def _source_options_chain(symbol: str) -> dict[str, Any] | None:
    """0DTE options intelligence: top strikes, P/C ratio, max pain.
    Only runs for ZDTE_SYMBOLS (SPY, QQQ, NVDA, TSLA, AMD, META).
    Posts OPTIONS_FLOW to port 9000 if unusual activity detected.
    """
    try:
        from engine.options_chain import (
            ZDTE_SYMBOLS, get_options_summary, check_unusual_activity
        )
        if symbol not in ZDTE_SYMBOLS:
            return None

        text = get_options_summary(symbol)
        if not text:
            return None

        # Check for unusual activity and fire-and-forget to port 9000
        try:
            unusual = check_unusual_activity(symbol)
            if unusual:
                from engine.signal_poster import post_to_9000
                for u in unusual[:3]:  # cap at 3 events
                    post_to_9000("OPTIONS_FLOW", {
                        "symbol":       u["symbol"],
                        "type":         u["type"],
                        "strike":       u["strike"],
                        "volume":       u["volume"],
                        "oi":           u["oi"],
                        "vol_oi_ratio": u["vol_oi_ratio"],
                        "expiration":   u["expiration"],
                        "message": (
                            f"Unusual options flow: {u['symbol']} "
                            f"{int(u['strike'])}{u['type'][0].upper()} "
                            f"vol={u['volume']:,} ({u['vol_oi_ratio']}x OI)"
                        ),
                    })
        except Exception:
            pass

        return {"label": "Options Chain", "text": text}
    except Exception:
        return None


# ── Fleet-Wide Learning ───────────────────────────────────────────────────────

def get_fleet_recent_trades(limit: int = 20) -> str:
    """Get recent trades from ALL agents for fleet-wide learning."""
    try:
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT p.display_name, t.action, t.symbol, t.price,
                   t.realized_pnl, t.executed_at
            FROM trades t
            JOIN ai_players p ON t.player_id = p.id
            WHERE t.executed_at > datetime('now', '-7 days')
            ORDER BY t.executed_at DESC
            LIMIT ?
        """, (limit,))
        trades = cursor.fetchall()
        conn.close()
    except Exception:
        return "No recent fleet trades."

    if not trades:
        return "No recent fleet trades."

    lines = ["FLEET ACTIVITY (learn from your crewmates):"]
    wins = 0
    total = 0
    for name, action, symbol, price, pnl, ts in trades:
        if pnl is not None:
            total += 1
            result = "WIN" if pnl > 0 else "LOSS"
            if pnl > 0:
                wins += 1
            lines.append(f"  - {name} {action} {symbol} @ ${price:.2f} → {result} ${pnl:+.2f}")
        else:
            lines.append(f"  - {name} {action} {symbol} @ ${price:.2f} → OPEN")

    if total > 0:
        lines.append(f"Fleet win rate: {wins/total*100:.0f}% ({wins}/{total})")

    return "\n".join(lines)


def get_strategy_leaderboard(days: int = 30) -> str:
    """Rank strategies by performance across ALL agents (uses timeframe column)."""
    try:
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                COALESCE(timeframe, 'unknown') as strategy,
                COUNT(*) as trades,
                SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(realized_pnl) as total_pnl
            FROM trades
            WHERE executed_at > datetime('now', ? || ' days')
              AND realized_pnl IS NOT NULL
            GROUP BY strategy
            HAVING trades >= 3
            ORDER BY total_pnl DESC
        """, (f"-{days}",))
        results = cursor.fetchall()
        conn.close()
    except Exception:
        return "No strategy data yet."

    if not results:
        return "No strategy data yet."

    lines = [f"TOP STRATEGIES (last {days} days):"]
    for i, (strat, trades, wins, pnl) in enumerate(results[:5], 1):
        wr = wins / trades * 100 if trades > 0 else 0
        lines.append(f"  {i}. {strat}: {wr:.0f}% WR, ${pnl:+.2f} ({trades} trades)")

    if len(results) > 5:
        worst = results[-1]
        wr = worst[2] / worst[1] * 100 if worst[1] > 0 else 0
        lines.append(f"  WORST: {worst[0]} {wr:.0f}% WR, ${worst[3]:+.2f}")

    return "\n".join(lines)


def get_hot_agents(days: int = 7) -> str:
    """Find best and worst performing agents this week."""
    try:
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                p.display_name,
                COUNT(*) as trades,
                SUM(CASE WHEN t.realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                SUM(t.realized_pnl) as total_pnl
            FROM trades t
            JOIN ai_players p ON t.player_id = p.id
            WHERE t.executed_at > datetime('now', ? || ' days')
              AND t.realized_pnl IS NOT NULL
            GROUP BY t.player_id
            HAVING trades >= 2
            ORDER BY total_pnl DESC
        """, (f"-{days}",))
        results = cursor.fetchall()
        conn.close()
    except Exception:
        return "No agent performance data yet."

    if not results:
        return "No agent performance data yet."

    lines = [f"TOP PERFORMERS (last {days} days):"]
    for i, (name, trades, wins, pnl) in enumerate(results[:3], 1):
        wr = wins / trades * 100 if trades > 0 else 0
        lines.append(f"  {i}. {name}: ${pnl:+.2f} ({trades} trades, {wr:.0f}% WR)")

    if len(results) > 3 and results[-1][3] < 0:
        cold = results[-1]
        wr = cold[2] / cold[1] * 100 if cold[1] > 0 else 0
        lines.append(f"  COLD: {cold[0]} ${cold[3]:+.2f} (avoid recent picks)")

    return "\n".join(lines)


def get_danger_tickers(days: int = 14, min_losses: int = 2) -> str:
    """Find tickers where the fleet keeps losing money."""
    try:
        conn = sqlite3.connect("data/trader.db", check_same_thread=False, timeout=5)
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                symbol,
                COUNT(*) as losses,
                SUM(realized_pnl) as total_loss
            FROM trades
            WHERE executed_at > datetime('now', ? || ' days')
              AND realized_pnl < 0
            GROUP BY symbol
            HAVING losses >= ?
            ORDER BY total_loss ASC
            LIMIT 5
        """, (f"-{days}", min_losses))
        results = cursor.fetchall()
        conn.close()
    except Exception:
        return ""

    if not results:
        return ""

    lines = ["DANGER TICKERS (fleet lost money — avoid unless high conviction):"]
    for symbol, losses, total_loss in results:
        lines.append(f"  - {symbol}: {losses} losses, ${total_loss:.2f} total")

    return "\n".join(lines)


def _source_fleet_intelligence() -> dict[str, Any] | None:
    """Aggregate fleet-wide learning via FleetCache (<1ms read, 5-min background refresh)."""
    try:
        # Fast-path: use pre-computed cached context (no DB hit at scan time)
        from engine.fleet_cache import get_fleet_context as _get_cached
        cached = _get_cached()
        if cached and cached.strip():
            return {"label": "Fleet Intelligence", "text": cached}

        # Fallback: compute on-demand if cache is empty
        fleet_trades   = get_fleet_recent_trades(20)
        strategy_board = get_strategy_leaderboard(30)
        hot_agents     = get_hot_agents(7)
        danger         = get_danger_tickers(14)

        parts = [fleet_trades, strategy_board, hot_agents]
        if danger:
            parts.append(danger)
        parts.append(
            "LESSON: Follow what's working. Avoid what's failing. "
            "The fleet's experience is YOUR experience. 62,000+ trades of wisdom."
        )

        text = "\n\n".join(p for p in parts if p and p.strip())
        if not text.strip():
            return None

        return {
            "label": "Fleet Intelligence",
            "text": text,
        }
    except Exception:
        return None


# ── Main entry points ────────────────────────────────────────────────────────

_SOURCE_KEYS = (
    "fear_greed",
    "red_alert",
    "congress_trades",
    "signal_scorecard",
    "fleet_consensus",
    "backtest",
    "layered_memory",
    "news_sentiment",
    "debate_intel",
    "bootstrap_intel",
    "options_chain",
    "fleet_intelligence",
)


def build_full_context_raw(player_id: str, symbol: str) -> dict[str, Any]:
    """Return dict of raw intelligence data (for the /api/brain-context endpoint)."""
    cached = _get_cached(player_id, symbol)
    if cached:
        return cached

    source_fns = [
        ("fear_greed",         lambda: _source_fear_greed()),
        ("red_alert",          lambda: _source_red_alert()),
        ("congress_trades",    lambda: _source_congress_trades(symbol)),
        ("signal_scorecard",   lambda: _source_signal_scorecard(symbol)),
        ("fleet_consensus",    lambda: _source_fleet_consensus(player_id, symbol)),
        ("backtest",           lambda: _source_backtest_performance(player_id, symbol)),
        ("layered_memory",     lambda: _source_layered_memory(player_id)),
        ("news_sentiment",     lambda: _source_news_sentiment(symbol)),
        ("debate_intel",       lambda: _source_debate_intel()),
        ("bootstrap_intel",    lambda: _source_bootstrap_intelligence(player_id, symbol)),
        ("options_chain",      lambda: _source_options_chain(symbol)),
        ("fleet_intelligence", lambda: _source_fleet_intelligence()),
    ]

    result: dict[str, Any] = {
        "player_id": player_id,
        "symbol": symbol,
        "sources": [],
        "cached": False,
        "ts": time.time(),
    }

    for key, fn in source_fns:
        try:
            data = fn()
            result[key] = data
            if data:
                result["sources"].append(data.get("label", key))
        except Exception:
            result[key] = None

    _set_cached(player_id, symbol, result)
    return result


def build_full_context(player_id: str, symbol: str) -> str:
    """Return formatted text block for injection into AI scan prompts.

    Complements the existing trade_memory_block and competitive_block in
    base.py. This adds: fear/greed, red alert, congress trades, signal
    history, fleet consensus, and per-symbol backtest performance.
    Returns empty string if no sources produce data (graceful degradation).
    """
    data = build_full_context_raw(player_id, symbol)
    lines: list[str] = []

    for key in _SOURCE_KEYS:
        entry = data.get(key)
        if entry and isinstance(entry, dict):
            text = entry.get("text", "").strip()
            if text:
                label = entry.get("label", key)
                lines.append(f"[{label}] {text}")

    if not lines:
        return ""

    return (
        "\n=== BRAIN CONTEXT ===\n"
        + "\n".join(lines)
        + "\n=== END BRAIN CONTEXT ===\n"
    )
