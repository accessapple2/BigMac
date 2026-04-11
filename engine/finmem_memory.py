"""
USS TradeMinds — FinMem-Inspired Layered Memory (engine/finmem_memory.py)

3-layer memory system per AI agent. Surfaced in brain_context prompts.

  Layer 1 — WORKING MEMORY  (last 2h):   live positions + today's activity
  Layer 2 — SHORT-TERM      (7 days):    closed trades, win/loss streaks, what worked
  Layer 3 — LONG-TERM       (all time):  lifetime stats, per-symbol edge, weekday bias,
                                          stored lessons from self_improvement loop

Entries ranked by RECENCY × IMPORTANCE (abs P&L magnitude).
Returns formatted text block for injection into AI scan prompts.
"""
from __future__ import annotations

import logging
import sqlite3
import threading
import time

logger = logging.getLogger(__name__)

_DB_PATH = "data/trader.db"
_CACHE_TTL = 300  # 5 minutes (one scan cycle)
_cache: dict[str, dict] = {}
_cache_lock = threading.Lock()


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=5)
    c.row_factory = sqlite3.Row
    return c


# ── Layer 1: Working Memory ──────────────────────────────────────────────────

def _get_layer1(player_id: str) -> str:
    try:
        db = _conn()
        positions = db.execute("""
            SELECT symbol, qty, avg_price, asset_type
            FROM positions
            WHERE player_id = ? AND qty > 0
            ORDER BY opened_at DESC LIMIT 8
        """, (player_id,)).fetchall()

        recent = db.execute("""
            SELECT symbol, action, price, realized_pnl, executed_at
            FROM trades
            WHERE player_id = ?
              AND executed_at >= datetime('now', '-2 hours')
            ORDER BY executed_at DESC LIMIT 6
        """, (player_id,)).fetchall()
        db.close()

        lines = []
        if positions:
            pos_strs = []
            for p in positions:
                label = f"{p['symbol']} ×{p['qty']:.0f} @${p['avg_price']:.2f}"
                if p["asset_type"] != "stock":
                    label += f" ({p['asset_type']})"
                pos_strs.append(label)
            lines.append("Open: " + " | ".join(pos_strs))
        else:
            lines.append("Open positions: None (all cash)")

        if recent:
            t_strs = []
            for t in recent:
                pnl = f" P&L ${t['realized_pnl']:+.2f}" if t["realized_pnl"] is not None else ""
                t_strs.append(f"{t['action']} {t['symbol']}{pnl}")
            lines.append("Last 2h: " + " → ".join(t_strs))

        return "\n".join(lines)
    except Exception as exc:
        logger.debug("finmem layer1 error: %s", exc)
        return ""


# ── Layer 2: Short-Term Memory ───────────────────────────────────────────────

def _get_layer2(player_id: str) -> str:
    try:
        db = _conn()
        trades = db.execute("""
            SELECT symbol, action, price, realized_pnl, sources, executed_at
            FROM trades
            WHERE player_id = ?
              AND executed_at >= datetime('now', '-7 days')
              AND action = 'SELL'
              AND realized_pnl IS NOT NULL
            ORDER BY abs(COALESCE(realized_pnl, 0)) DESC, executed_at DESC
            LIMIT 20
        """, (player_id,)).fetchall()
        db.close()

        if not trades:
            return ""

        wins   = [t for t in trades if (t["realized_pnl"] or 0) > 0]
        losses = [t for t in trades if (t["realized_pnl"] or 0) <= 0]
        total  = len(trades)
        win_rate  = round(len(wins) / total * 100)
        total_pnl = sum(t["realized_pnl"] or 0 for t in trades)

        # Current streak (from most-recent trade backward)
        sorted_trades = sorted(trades, key=lambda t: t["executed_at"], reverse=True)
        streak = 0
        streak_type: str | None = None
        for t in sorted_trades:
            is_win = (t["realized_pnl"] or 0) > 0
            if streak_type is None:
                streak_type = "W" if is_win else "L"
                streak = 1
            elif (streak_type == "W") == is_win:
                streak += 1
            else:
                break
        streak_str = f"{streak}× {streak_type}" if streak > 1 else "no streak"

        best  = max(trades, key=lambda t: t["realized_pnl"] or 0)
        worst = min(trades, key=lambda t: t["realized_pnl"] or 0)

        lines = [
            f"7-day: {total} closed | {win_rate}% WR | P&L ${total_pnl:+.2f} | {streak_str}",
            f"Best: {best['symbol']} ${best['realized_pnl']:+.2f} | "
            f"Worst: {worst['symbol']} ${worst['realized_pnl']:+.2f}",
        ]

        # Sources that appear most in winning trades
        winning_sources: dict[str, int] = {}
        for t in wins:
            for src in (t["sources"] or "").split(","):
                src = src.strip()
                if src:
                    winning_sources[src] = winning_sources.get(src, 0) + 1
        if winning_sources:
            top = sorted(winning_sources.items(), key=lambda x: -x[1])[:3]
            lines.append("What worked: " + ", ".join(f"{s} ({n}×)" for s, n in top))

        return "\n".join(lines)
    except Exception as exc:
        logger.debug("finmem layer2 error: %s", exc)
        return ""


# ── Layer 3: Long-Term Memory ────────────────────────────────────────────────

def _get_layer3(player_id: str) -> str:
    try:
        db = _conn()

        totals = db.execute("""
            SELECT COUNT(*) as n,
                   SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(COALESCE(realized_pnl, 0)) as total_pnl,
                   SUM(CASE WHEN realized_pnl > 0 THEN realized_pnl ELSE 0 END) as gross_win,
                   ABS(SUM(CASE WHEN realized_pnl <= 0 THEN realized_pnl ELSE 0 END)) as gross_loss
            FROM trades
            WHERE player_id = ? AND action = 'SELL' AND realized_pnl IS NOT NULL
        """, (player_id,)).fetchone()

        if not totals or (totals["n"] or 0) < 5:
            db.close()
            return ""

        n        = totals["n"] or 0
        win_rate = round((totals["wins"] or 0) / n * 100)
        total_pnl = totals["total_pnl"] or 0
        gross_win = totals["gross_win"] or 0.01
        gross_loss = totals["gross_loss"] or 0.01
        pf = round(gross_win / gross_loss, 2)

        # Per-symbol edge (≥3 trades)
        sym_rows = db.execute("""
            SELECT symbol, COUNT(*) as n,
                   SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(COALESCE(realized_pnl, 0)) as pnl
            FROM trades
            WHERE player_id = ? AND action = 'SELL' AND realized_pnl IS NOT NULL
            GROUP BY symbol HAVING COUNT(*) >= 3
            ORDER BY COUNT(*) DESC LIMIT 6
        """, (player_id,)).fetchall()

        # Weekday bias (≥2 trades per day)
        wd_rows = db.execute("""
            SELECT strftime('%w', executed_at) as wd,
                   COUNT(*) as n,
                   SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins,
                   SUM(COALESCE(realized_pnl, 0)) as pnl
            FROM trades
            WHERE player_id = ? AND action = 'SELL' AND realized_pnl IS NOT NULL
            GROUP BY wd ORDER BY wd
        """, (player_id,)).fetchall()

        # Stored lessons from self_improvement loop
        lessons = []
        try:
            lessons = db.execute("""
                SELECT summary FROM agent_memory
                WHERE player_id = ? AND memory_layer = 'LESSON'
                ORDER BY created_at DESC LIMIT 3
            """, (player_id,)).fetchall()
        except Exception:
            pass

        db.close()

        WD = {0: "Sun", 1: "Mon", 2: "Tue", 3: "Wed", 4: "Thu", 5: "Fri", 6: "Sat"}
        lines = [
            f"Lifetime: {n} trades | {win_rate}% WR | P&L ${total_pnl:+.2f} | PF {pf}×",
        ]

        if sym_rows:
            parts = []
            for r in sym_rows:
                wr = round((r["wins"] or 0) / max(r["n"] or 1, 1) * 100)
                tag = "✓edge" if wr >= 60 else ("✗avoid" if wr < 35 else "~ok")
                parts.append(f"{r['symbol']} {wr}%W({r['n']}×){tag}")
            lines.append("By symbol: " + " | ".join(parts))

        if wd_rows:
            wd_data = [
                (WD.get(int(r["wd"]), "?"), r["n"] or 0,
                 round((r["wins"] or 0) / max(r["n"] or 1, 1) * 100),
                 r["pnl"] or 0)
                for r in wd_rows if (r["n"] or 0) >= 2
            ]
            if len(wd_data) >= 2:
                best_wd  = max(wd_data, key=lambda x: x[2])
                worst_wd = min(wd_data, key=lambda x: x[2])
                if best_wd[0] != worst_wd[0]:
                    lines.append(
                        f"Day bias: Best {best_wd[0]} ({best_wd[2]}%WR) | "
                        f"Worst {worst_wd[0]} ({worst_wd[2]}%WR)"
                    )

        for lesson in lessons:
            lines.append(f"Lesson: {lesson['summary'][:180]}")

        return "\n".join(lines)
    except Exception as exc:
        logger.debug("finmem layer3 error: %s", exc)
        return ""


# ── Public API ────────────────────────────────────────────────────────────────

def build_layered_memory(player_id: str) -> str:
    """
    Build a formatted 3-layer memory block for this agent.
    Cached per player for 5 minutes (one scan cycle).
    Returns empty string if no meaningful history.
    """
    with _cache_lock:
        entry = _cache.get(player_id)
        if entry and time.time() - entry["ts"] < _CACHE_TTL:
            return entry["text"]

    l1 = _get_layer1(player_id)
    l2 = _get_layer2(player_id)
    l3 = _get_layer3(player_id)

    parts = []
    if l1:
        parts.append(f"[Working Memory]\n{l1}")
    if l2:
        parts.append(f"[Short-Term — 7 days]\n{l2}")
    if l3:
        parts.append(f"[Long-Term — Lifetime]\n{l3}")

    text = "\n\n".join(parts)
    with _cache_lock:
        _cache[player_id] = {"text": text, "ts": time.time()}

    return text
