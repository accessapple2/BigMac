"""Adaptive Learning Engine — real-time confidence adjustments and prompt injection.

Loop 3: Continuous learning. Before each trade executes, the system looks up
that model's adjustments and modifies trade parameters. Before each scan,
learning context is injected into the model's prompt.

All learning data is ADDITIVE — never delete rows.
"""
from __future__ import annotations
import sqlite3
import json
import logging
from datetime import datetime, date

from rich.console import Console

console = Console()
logger = logging.getLogger("learning")
DB = "data/trader.db"


def _conn():
    c = sqlite3.connect(DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.row_factory = sqlite3.Row
    return c


def apply_learning(player_id: str, trade_signal: dict) -> dict | None:
    """Apply learned adjustments to a trade signal before execution.

    Returns modified trade_signal, or None if trade should be blocked.
    """
    conn = _conn()

    # Load active adjustments
    adjustments = conn.execute("""
        SELECT adjustment_type, new_value, reason
        FROM model_adjustments
        WHERE player_id = ? AND effective_date <= date('now')
        ORDER BY created_at DESC
    """, (player_id,)).fetchall()

    # Recent losses by ticker
    recent_losses = conn.execute("""
        SELECT symbol, COUNT(*) as loss_count
        FROM daily_lessons
        WHERE player_id = ? AND grade IN ('D', 'F')
        AND date >= date('now', '-14 days')
        GROUP BY symbol
    """, (player_id,)).fetchall()

    conn.close()

    # Build adjustment map (most recent per type)
    adj_map = {}
    for adj in adjustments:
        atype = adj["adjustment_type"]
        if atype not in adj_map:
            adj_map[atype] = adj

    modified = dict(trade_signal)
    blocked_reasons = []

    # 1. Confidence modifier
    if "confidence_modifier" in adj_map:
        try:
            modifier = float(adj_map["confidence_modifier"]["new_value"])
            original = modified.get("confidence", 0.5)
            modified["confidence"] = round(original * modifier, 3)
            modified["confidence_adjusted"] = True
            modified["confidence_original"] = original
        except (ValueError, TypeError):
            pass

    # 2. Regime filter
    if "regime_filter" in adj_map:
        regime_rule = adj_map["regime_filter"]["new_value"]
        current_regime = _get_current_regime()
        action = modified.get("action", "").upper()

        if regime_rule == "BEAR_ONLY_SELL" and current_regime == "BEAR" and action == "BUY":
            blocked_reasons.append(f"Learning blocked BUY in BEAR regime (rule: {regime_rule})")
        elif regime_rule == "BULL_ONLY_BUY" and current_regime == "BULL" and action == "SELL":
            blocked_reasons.append(f"Learning blocked SELL in BULL regime (rule: {regime_rule})")

    # 3. Position size limit
    if "position_size" in adj_map:
        try:
            max_pct = float(adj_map["position_size"]["new_value"])
            trade_value = modified.get("qty", 0) * modified.get("price", 0)
            capital = 7000
            if capital > 0 and trade_value / capital > max_pct:
                allowed_value = capital * max_pct
                modified["qty"] = round(allowed_value / max(modified.get("price", 1), 0.01), 4)
                modified["qty_adjusted"] = True
        except (ValueError, TypeError):
            pass

    # 4. Stop-loss override
    if "stop_loss" in adj_map:
        try:
            modified["stop_loss_pct"] = float(adj_map["stop_loss"]["new_value"])
            modified["stop_loss_adjusted"] = True
        except (ValueError, TypeError):
            pass

    # 5. Ticker blacklist from recent losses
    loss_symbols = {row["symbol"]: row["loss_count"] for row in recent_losses}
    sym = modified.get("symbol")
    if sym in loss_symbols and loss_symbols[sym] >= 3:
        if modified.get("confidence", 0) < 0.9:
            blocked_reasons.append(
                f"Learning blocked {sym}: {loss_symbols[sym]} losses in 14 days, "
                f"conf {modified.get('confidence', 0)} < 0.9 threshold"
            )

    # 6. Ghost promotion override
    if "ghost_promotion_override" in adj_map:
        if adj_map["ghost_promotion_override"]["new_value"] == "disabled":
            modified["ghost_promotion_disabled"] = True
            if "ghost promotion" in modified.get("reasoning", "").lower():
                blocked_reasons.append("Learning disabled ghost promotion for this model")

    # 7. Cooldown
    if "cooldown" in adj_map:
        try:
            cooldown_str = adj_map["cooldown"]["new_value"].replace("min", "")
            cooldown_minutes = int(cooldown_str)
            last_trade = _get_last_trade_time(player_id)
            if last_trade:
                elapsed = (datetime.now() - last_trade).total_seconds() / 60
                if elapsed < cooldown_minutes:
                    blocked_reasons.append(
                        f"Learning cooldown: {cooldown_minutes - elapsed:.0f}min remaining"
                    )
        except (ValueError, TypeError):
            pass

    if blocked_reasons:
        _log_learning_block(player_id, modified, blocked_reasons)
        return None

    return modified


def get_learning_context(player_id: str) -> str:
    """Get learning context to inject into a model's scan prompt.

    Returns a string appended to the model's system prompt,
    giving it awareness of its own learning history.
    """
    conn = _conn()

    # Recent lessons (last 7 days)
    lessons = conn.execute("""
        SELECT date, symbol, grade, lesson, recommendation
        FROM daily_lessons
        WHERE player_id = ? AND date >= date('now', '-7 days')
        ORDER BY date DESC LIMIT 10
    """, (player_id,)).fetchall()

    # Current score
    score = conn.execute("""
        SELECT overall_score, regime_alignment, confidence_calibration
        FROM model_scores
        WHERE player_id = ? ORDER BY date DESC LIMIT 1
    """, (player_id,)).fetchone()

    # Active adjustments
    adjustments = conn.execute("""
        SELECT adjustment_type, new_value, reason
        FROM model_adjustments
        WHERE player_id = ? AND effective_date <= date('now')
        ORDER BY created_at DESC LIMIT 5
    """, (player_id,)).fetchall()

    # Ticker warnings
    bad_tickers = conn.execute("""
        SELECT symbol, COUNT(*) as losses
        FROM daily_lessons
        WHERE player_id = ? AND grade IN ('D', 'F')
        AND date >= date('now', '-14 days')
        GROUP BY symbol HAVING losses >= 2
    """, (player_id,)).fetchall()

    conn.close()

    # No learning data yet — return empty
    if not lessons and not score and not adjustments and not bad_tickers:
        return ""

    ctx = "\n\n=== LEARNING CONTEXT (auto-generated from your performance) ===\n"

    if score:
        ctx += f"Your current performance score: {score['overall_score']}/100\n"
        ctx += f"Regime alignment: {score['regime_alignment']}/100\n"
        ctx += f"Confidence calibration: {score['confidence_calibration']}/100\n"

    if lessons:
        ctx += "\nRecent lessons from your trades:\n"
        for l in lessons[:5]:
            ctx += f"  [{l['date']}] {l['symbol']} grade={l['grade']}: {l['lesson']}\n"

    if adjustments:
        ctx += "\nActive adjustments (YOU MUST FOLLOW THESE):\n"
        for a in adjustments:
            ctx += f"  - {a['adjustment_type']}: {a['new_value']} ({a['reason']})\n"

    if bad_tickers:
        ctx += "\nTICKER WARNINGS (recent losses — approach with extreme caution):\n"
        for t in bad_tickers:
            ctx += f"  - {t['symbol']}: {t['losses']} losses in 14 days — DO NOT trade unless confidence > 90%\n"

    ctx += "=== END LEARNING CONTEXT ===\n"
    return ctx


def get_model_profile(player_id: str) -> dict:
    """Get full learning profile for a model (for UI)."""
    conn = _conn()

    score = conn.execute("""
        SELECT * FROM model_scores
        WHERE player_id = ? ORDER BY date DESC LIMIT 1
    """, (player_id,)).fetchone()

    prev_score = conn.execute("""
        SELECT overall_score FROM model_scores
        WHERE player_id = ? ORDER BY date DESC LIMIT 1 OFFSET 1
    """, (player_id,)).fetchone()

    adjustments = conn.execute("""
        SELECT * FROM model_adjustments
        WHERE player_id = ? ORDER BY created_at DESC LIMIT 10
    """, (player_id,)).fetchall()

    lessons = conn.execute("""
        SELECT * FROM daily_lessons
        WHERE player_id = ? ORDER BY date DESC LIMIT 14
    """, (player_id,)).fetchall()

    blocked = conn.execute("""
        SELECT COUNT(*) as cnt FROM daily_lessons
        WHERE player_id = ? AND recommendation LIKE '%blocked%'
        AND date >= date('now', '-7 days')
    """, (player_id,)).fetchone()

    conn.close()

    current_score = dict(score) if score else None
    trend = None
    if current_score and prev_score:
        diff = current_score["overall_score"] - prev_score["overall_score"]
        trend = "improving" if diff > 0 else "declining" if diff < 0 else "steady"

    # Status based on score
    s = current_score["overall_score"] if current_score else 50
    status = "PROMOTED" if s >= 75 else "STEADY" if s >= 40 else "PROBATION" if s >= 20 else "DEMOTED"

    return {
        "player_id": player_id,
        "score": current_score,
        "trend": trend,
        "status": status,
        "adjustments": [dict(a) for a in adjustments],
        "lessons": [dict(l) for l in lessons],
        "blocked_count": blocked["cnt"] if blocked else 0,
    }


def get_fleet_summary() -> dict:
    """Get fleet-wide learning summary (for UI)."""
    conn = _conn()

    # Latest scores per model
    scores = conn.execute("""
        SELECT m.player_id, p.display_name, m.overall_score, m.date
        FROM model_scores m
        JOIN ai_players p ON m.player_id = p.id
        WHERE m.id IN (SELECT MAX(id) FROM model_scores GROUP BY player_id)
        ORDER BY m.overall_score DESC
    """).fetchall()

    # Total blocks this week
    blocks = conn.execute("""
        SELECT COUNT(*) as cnt FROM daily_lessons
        WHERE recommendation LIKE '%blocked%'
        AND date >= date('now', '-7 days')
    """).fetchone()

    # Recent adjustments
    recent_adj = conn.execute("""
        SELECT ma.player_id, p.display_name, ma.adjustment_type,
               ma.old_value, ma.new_value, ma.reason, ma.created_at
        FROM model_adjustments ma
        JOIN ai_players p ON ma.player_id = p.id
        ORDER BY ma.created_at DESC LIMIT 20
    """).fetchall()

    # Models on probation
    probation = [dict(s) for s in scores if s["overall_score"] and s["overall_score"] < 40]

    conn.close()

    return {
        "scores": [dict(s) for s in scores],
        "blocked_this_week": blocks["cnt"] if blocks else 0,
        "recent_adjustments": [dict(a) for a in recent_adj],
        "probation_models": probation,
        "total_models_scored": len(scores),
    }


def get_learning_log(limit: int = 50) -> list:
    """Chronological feed of all learning events."""
    conn = _conn()
    events = []

    # Lessons
    for row in conn.execute("""
        SELECT 'lesson' as type, player_id, date as ts,
               symbol || ' ' || grade || ': ' || lesson as detail
        FROM daily_lessons ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall():
        events.append(dict(row))

    # Adjustments
    for row in conn.execute("""
        SELECT 'adjustment' as type, player_id, created_at as ts,
               adjustment_type || ' ' || COALESCE(old_value,'?') || ' -> ' || new_value || ' (' || reason || ')' as detail
        FROM model_adjustments ORDER BY created_at DESC LIMIT ?
    """, (limit,)).fetchall():
        events.append(dict(row))

    conn.close()

    events.sort(key=lambda e: e.get("ts", ""), reverse=True)
    return events[:limit]


# ─── Internal helpers ────────────────────────────────────────────

def _get_current_regime():
    import requests
    try:
        r = requests.get("http://127.0.0.1:8080/api/regime", timeout=5)
        return r.json().get("regime", "UNKNOWN")
    except Exception:
        return "UNKNOWN"


def _get_last_trade_time(player_id):
    conn = _conn()
    row = conn.execute(
        "SELECT executed_at FROM trades WHERE player_id = ? ORDER BY executed_at DESC LIMIT 1",
        (player_id,)
    ).fetchone()
    conn.close()
    if row and row["executed_at"]:
        try:
            return datetime.fromisoformat(row["executed_at"])
        except (ValueError, TypeError):
            pass
    return None


def _log_learning_block(player_id, trade, reasons):
    """Log when learning blocks a trade."""
    console.log(
        f"[bold yellow]LEARNING BLOCK: {player_id} "
        f"{trade.get('action')} {trade.get('symbol')}: "
        f"{'; '.join(reasons)}"
    )
    logger.info(
        f"[LEARNING BLOCK] {player_id} {trade.get('action')} "
        f"{trade.get('symbol')}: {'; '.join(reasons)}"
    )
