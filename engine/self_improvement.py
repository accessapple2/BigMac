"""
USS TradeMinds — Self-Improvement Loop (engine/self_improvement.py)

Daily at 2:30 PM AZ (4:30 PM ET) on weekdays, each active agent reflects on
today's trading performance and generates 3 concrete rules for tomorrow
using qwen3.5:9b. Lessons are stored in agent_memory (LAYER: LESSON) and
surfaced automatically in finmem_memory's Layer 3 the next day.

Sacred rules:
  - INSERT ONLY to agent_memory — NEVER drop or truncate trade data
  - Runs once per day via gate check (idempotent)
"""
from __future__ import annotations

import logging
import sqlite3
import threading
from datetime import datetime

logger = logging.getLogger(__name__)

_DB_PATH   = "data/trader.db"
_OLLAMA    = "http://localhost:11434"
_run_lock  = threading.Lock()
_last_date = ""


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=10)
    c.row_factory = sqlite3.Row
    c.execute("PRAGMA busy_timeout=10000")
    return c


def _today_summary(player_id: str, display_name: str) -> dict:
    today = datetime.now().strftime("%Y-%m-%d")
    db = _conn()

    trades = db.execute("""
        SELECT symbol, action, price, realized_pnl, confidence, executed_at
        FROM trades
        WHERE player_id = ? AND date(executed_at) = ?
        ORDER BY executed_at
    """, (player_id, today)).fetchall()

    closed = [t for t in trades if t["action"] == "SELL" and t["realized_pnl"] is not None]
    wins   = [t for t in closed if (t["realized_pnl"] or 0) > 0]
    total_pnl = sum(t["realized_pnl"] or 0 for t in closed)

    regime_row = db.execute(
        "SELECT regime FROM regime_history WHERE date = ? LIMIT 1", (today,)
    ).fetchone()
    regime = regime_row["regime"] if regime_row else "UNKNOWN"
    db.close()

    return {
        "player_id":     player_id,
        "display_name":  display_name,
        "today":         today,
        "regime":        regime,
        "total_trades":  len(trades),
        "closed_trades": len(closed),
        "wins":          len(wins),
        "losses":        len(closed) - len(wins),
        "total_pnl":     round(total_pnl, 2),
        "trade_log":     [
            {"symbol": t["symbol"], "action": t["action"],
             "pnl": round(t["realized_pnl"] or 0, 2),
             "conf": round((t["confidence"] or 0) * 100)}
            for t in closed[:10]
        ],
    }


def _generate_lesson(s: dict) -> str | None:
    import requests

    if s["closed_trades"] == 0:
        if s["total_trades"] == 0:
            return None
        return (
            f"No closed trades today in {s['regime']} regime. "
            "Lesson: Hold until conviction ≥70%. Watch open positions at open tomorrow."
        )

    trade_str = "\n".join(
        f"  {t['action']} {t['symbol']} P&L ${t['pnl']:+.2f} conf {t['conf']}%"
        for t in s["trade_log"]
    )

    prompt = (
        f"You are {s['display_name']}, an AI trading agent on USS TradeMinds.\n"
        f"Today ({s['today']}) you made {s['closed_trades']} closed trades "
        f"in a {s['regime']} market: {s['wins']}W/{s['losses']}L, "
        f"total P&L ${s['total_pnl']:+.2f}.\n\n"
        f"Trades:\n{trade_str}\n\n"
        f"Generate EXACTLY 3 specific, actionable rules for tomorrow. "
        f"Name symbols, regimes, or conditions. Be blunt.\n"
        f"GOOD: 'Do not buy INTC in BEAR regime — lost 3× this week.'\n"
        f"BAD:  'Be more careful with trades.'\n\n"
        f"1.\n2.\n3.\n\nRules:"
    )

    try:
        resp = requests.post(
            f"{_OLLAMA}/api/generate",
            json={
                "model": "qwen3.5:9b",
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.5, "num_predict": 200},
            },
            timeout=45,
        )
        resp.raise_for_status()
        raw = resp.json().get("response", "").strip()
        # Strip qwen3 think tags
        if "<think>" in raw and "</think>" in raw:
            raw = raw[raw.rfind("</think>") + 8:].strip()
        return raw[:700] if raw else None
    except Exception as exc:
        logger.warning("[SELF-IMPROVE] LLM error for %s: %s", s["player_id"], exc)
        return None


def _store_lesson(player_id: str, lesson: str, score: float) -> None:
    """INSERT only — sacred data rule."""
    try:
        db = _conn()
        db.execute(
            "INSERT INTO agent_memory (player_id, memory_layer, summary, score) "
            "VALUES (?, 'LESSON', ?, ?)",
            (player_id, lesson[:1000], score),
        )
        db.commit()
        db.close()
    except Exception as exc:
        logger.warning("[SELF-IMPROVE] store error: %s", exc)


def run_daily_reflection() -> None:
    """
    Scheduled gate: fires once per day at 2:30 PM AZ (4:30 PM ET).
    Called by main.py every 5 minutes — gate ensures it runs once.
    """
    global _last_date

    try:
        import pytz
        az = pytz.timezone("US/Arizona")
        now = datetime.now(az)
    except ImportError:
        now = datetime.now()

    if now.weekday() >= 5:
        return

    # Gate window: 2:30–2:40 PM AZ (4:30–4:40 PM ET)
    h = now.hour + now.minute / 60.0
    if not (14.5 <= h < 14.67):
        return

    today = now.strftime("%Y-%m-%d")
    with _run_lock:
        if _last_date == today:
            return
        _last_date = today

    logger.info("[SELF-IMPROVE] Daily reflection starting for %s", today)

    try:
        db = _conn()
        players = db.execute("""
            SELECT id, display_name FROM ai_players
            WHERE is_active = 1
              AND id NOT IN ('steve-webull', 'dayblade-0dte', 'dalio-metals')
        """).fetchall()
        db.close()
    except Exception as exc:
        logger.error("[SELF-IMPROVE] DB read failed: %s", exc)
        return

    for p in players:
        try:
            summary = _today_summary(p["id"], p["display_name"])
            if summary["total_trades"] == 0:
                continue
            lesson = _generate_lesson(summary)
            if not lesson:
                continue
            n = summary["closed_trades"] or 1
            win_rate  = summary["wins"] / n
            pnl_score = min(abs(summary["total_pnl"]) / 500.0, 1.0)
            score     = round(win_rate * 0.6 + pnl_score * 0.4, 3)
            _store_lesson(p["id"], lesson, score)
            logger.info("[SELF-IMPROVE] %s: lesson stored (score=%.3f)", p["id"], score)
        except Exception as exc:
            logger.warning("[SELF-IMPROVE] %s: %s", p["id"], exc)
