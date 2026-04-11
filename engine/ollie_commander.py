"""engine/ollie_commander.py — Ollie Fleet Commander (Master Filter)

Ollie is NOT a trader — he is the quality gate.
Every Sniper trade must pass Ollie's scoring before execution.

Scoring factors (combined Ollie Score, threshold >= 2.0):
  1. Signal Center grade from composite_alpha   (0-5)
  2. Composite alpha score                      (0-2 scale → 0-3 pts)
  3. Agent rolling 30-day win rate              (0-2 pts)
  4. Regime alignment (strategy vs VIX regime)  (0-2 pts)

Ollie approves if OllieScore >= 2.0, otherwise NO-GO.
Threshold lowered from 3.0 to 2.0 after v5 backtest showed Ollie blocked 4 winners.
"""
from __future__ import annotations
import sqlite3
import os
from datetime import datetime, timedelta
from typing import Any

# ── Config ────────────────────────────────────────────────────────────────────
TRADER_DB  = "data/trader.db"
ALPHA_DB   = "data/alpha_signals.db"
OLLIE_ID   = "ollie-auto"
THRESHOLD  = 2.0   # minimum Ollie Score to approve (lowered from 3.0 — was blocking winners)

# Score weights
W_GRADE    = 0.30   # Signal Center grade
W_ALPHA    = 0.25   # Composite alpha
W_AGENT_WR = 0.25   # Agent win rate
W_REGIME   = 0.20   # Regime alignment

# Regime × strategy alignment table
# "covered_call" → prefers CAUTIOUS/BEAR, "csp" → any, "rsi_bounce" → BULL
REGIME_ALIGNMENT = {
    "covered_call": {"TRENDING_BULL": 1.0, "BULL": 1.2, "CAUTIOUS": 2.0, "BEAR": 2.0, "CRISIS": 0.0},
    "csp":          {"TRENDING_BULL": 1.5, "BULL": 2.0, "CAUTIOUS": 1.5, "BEAR": 0.5, "CRISIS": 0.0},
    "rsi_bounce":   {"TRENDING_BULL": 2.0, "BULL": 2.0, "CAUTIOUS": 1.0, "BEAR": 0.5, "CRISIS": 0.0},
    "bollinger":    {"TRENDING_BULL": 1.5, "BULL": 1.5, "CAUTIOUS": 1.0, "BEAR": 0.5, "CRISIS": 0.0},
    "default":      {"TRENDING_BULL": 1.5, "BULL": 1.5, "CAUTIOUS": 1.0, "BEAR": 0.5, "CRISIS": 0.0},
}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _conn_trader() -> sqlite3.Connection:
    c = sqlite3.connect(TRADER_DB, check_same_thread=False, timeout=30)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _conn_alpha() -> sqlite3.Connection:
    path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ALPHA_DB)
    c = sqlite3.connect(path, check_same_thread=False, timeout=10)
    c.row_factory = sqlite3.Row
    return c


def ensure_tables() -> None:
    """Create ollie_decisions table in trader.db."""
    c = _conn_trader()
    c.execute("""CREATE TABLE IF NOT EXISTS ollie_decisions (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        decided_at  TEXT    NOT NULL DEFAULT (datetime('now')),
        player_id   TEXT    NOT NULL,
        symbol      TEXT    NOT NULL,
        decision    TEXT    NOT NULL,   -- 'APPROVE' | 'REJECT'
        ollie_score REAL    NOT NULL,
        grade_pts   REAL,
        alpha_pts   REAL,
        agent_wr_pts REAL,
        regime_pts  REAL,
        reason      TEXT,
        market_regime TEXT,
        agent_conf  REAL
    )""")
    c.commit()
    c.close()


# ── Scoring helpers ───────────────────────────────────────────────────────────

def _grade_to_pts(composite_score: float) -> float:
    """Convert composite alpha score to 0-5 grade points."""
    if composite_score >= 1.5:   return 5.0   # A+
    if composite_score >= 1.0:   return 4.0   # A
    if composite_score >= 0.5:   return 3.0   # B
    if composite_score >= 0.0:   return 2.0   # C
    return 1.0                                 # D / negative


def _alpha_to_pts(composite_score: float) -> float:
    """Convert composite alpha to 0-3 alpha points (used separately from grade)."""
    # Scale: [-2, +2] → [0, 3]
    clamped = max(-2.0, min(2.0, composite_score))
    return round((clamped + 2.0) / 4.0 * 3.0, 3)


def _get_composite_score(symbol: str) -> float:
    """Return most recent composite_score for symbol from alpha_signals.db."""
    try:
        ac = _conn_alpha()
        row = ac.execute(
            "SELECT composite_score FROM composite_alpha WHERE symbol=? ORDER BY created_at DESC LIMIT 1",
            (symbol,)
        ).fetchone()
        ac.close()
        return float(row[0]) if row and row[0] is not None else 0.0
    except Exception:
        return 0.0


def _get_agent_wr_pts(player_id: str) -> float:
    """Rolling 30-day win rate → 0-2 pts."""
    try:
        c = _conn_trader()
        cutoff = (datetime.utcnow() - timedelta(days=30)).isoformat()
        row = c.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN realized_pnl > 0 THEN 1 ELSE 0 END) as wins
               FROM trades
               WHERE player_id=? AND executed_at >= ? AND realized_pnl IS NOT NULL""",
            (player_id, cutoff)
        ).fetchone()
        c.close()
        total = row["total"] or 0
        wins  = row["wins"] or 0
        if total < 3:
            return 1.0   # not enough data — neutral
        wr = wins / total
        if wr >= 0.70:   return 2.0
        if wr >= 0.55:   return 1.5
        if wr >= 0.40:   return 1.0
        return 0.5
    except Exception:
        return 1.0


def _get_regime_pts(strategy: str | None, regime: str) -> float:
    """Regime alignment → 0-2 pts."""
    key = (strategy or "default").lower()
    table = REGIME_ALIGNMENT.get(key, REGIME_ALIGNMENT["default"])
    for r_key, pts in table.items():
        if r_key.upper() in regime.upper():
            return pts
    return 1.0   # neutral fallback


# ── Main gate function ─────────────────────────────────────────────────────────

def approve_or_reject(
    player_id:  str,
    symbol:     str,
    confidence: float,        # 0-100 LLM confidence
    strategy:   str | None,
    market_ctx: dict[str, Any],
) -> tuple[bool, float, str]:
    """
    Evaluate a trade and return (approved, ollie_score, reason).

    approved    True = GO, False = NO-GO
    ollie_score Weighted score (threshold = 3.0)
    reason      Human-readable explanation
    """
    ensure_tables()

    regime = str(market_ctx.get("regime", market_ctx.get("market_regime", "CAUTIOUS"))).upper()

    # ── Factor 1: Signal Center grade (30%) ──────────────────────────────────
    comp_score  = _get_composite_score(symbol)
    grade_pts   = _grade_to_pts(comp_score)

    # ── Factor 2: Composite alpha (25%) ──────────────────────────────────────
    alpha_pts   = _alpha_to_pts(comp_score)

    # ── Factor 3: Agent 30-day win rate (25%) ────────────────────────────────
    agent_wr_pts = _get_agent_wr_pts(player_id)

    # ── Factor 4: Regime alignment (20%) ─────────────────────────────────────
    regime_pts  = _get_regime_pts(strategy, regime)

    # ── Weighted score ────────────────────────────────────────────────────────
    # Normalise each factor to 0-5 scale first, then weight
    # grade_pts:    0-5 already
    # alpha_pts:    0-3 → ×(5/3)
    # agent_wr_pts: 0-2 → ×(5/2)
    # regime_pts:   0-2 → ×(5/2)
    norm_grade  = grade_pts
    norm_alpha  = alpha_pts  * (5.0 / 3.0)
    norm_wr     = agent_wr_pts * (5.0 / 2.0)
    norm_regime = regime_pts   * (5.0 / 2.0)

    ollie_score = round(
        W_GRADE    * norm_grade  +
        W_ALPHA    * norm_alpha  +
        W_AGENT_WR * norm_wr     +
        W_REGIME   * norm_regime,
        3
    )

    approved = ollie_score >= THRESHOLD

    # Build reason string
    grade_label = "A+" if comp_score >= 1.5 else "A" if comp_score >= 1.0 else "B" if comp_score >= 0.5 else "C" if comp_score >= 0 else "D"
    reason = (
        f"OllieScore={ollie_score:.2f}/5 ({'GO' if approved else 'NO-GO'}) | "
        f"Grade={grade_label}({grade_pts:.1f}) "
        f"Alpha={comp_score:+.2f}({alpha_pts:.1f}) "
        f"AgentWR={agent_wr_pts:.1f} "
        f"Regime={regime_pts:.1f} | "
        f"Regime={regime}"
    )

    # ── Log decision ──────────────────────────────────────────────────────────
    try:
        c = _conn_trader()
        c.execute(
            """INSERT INTO ollie_decisions
               (player_id, symbol, decision, ollie_score,
                grade_pts, alpha_pts, agent_wr_pts, regime_pts,
                reason, market_regime, agent_conf)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (player_id, symbol,
             "APPROVE" if approved else "REJECT",
             ollie_score,
             grade_pts, alpha_pts, agent_wr_pts, regime_pts,
             reason, regime, confidence)
        )
        c.commit()
        c.close()
    except Exception:
        pass

    return approved, ollie_score, reason


# ── Dashboard stats ───────────────────────────────────────────────────────────

def get_ollie_stats(days: int = 30) -> dict[str, Any]:
    """Return Ollie approval/rejection stats for dashboard."""
    try:
        ensure_tables()
        c = _conn_trader()
        cutoff = (datetime.utcnow() - timedelta(days=days)).isoformat()

        totals = c.execute(
            """SELECT decision, COUNT(*) as cnt
               FROM ollie_decisions
               WHERE decided_at >= ?
               GROUP BY decision""",
            (cutoff,)
        ).fetchall()

        approved = rejected = 0
        for row in totals:
            if row["decision"] == "APPROVE":
                approved = row["cnt"]
            else:
                rejected = row["cnt"]

        # Win rate of APPROVED trades (join with actual trades)
        wr_row = c.execute(
            """SELECT COUNT(*) as total,
                      SUM(CASE WHEN t.realized_pnl > 0 THEN 1 ELSE 0 END) as wins
               FROM ollie_decisions od
               JOIN trades t ON od.symbol = t.symbol AND od.player_id = t.player_id
                             AND ABS(strftime('%s', od.decided_at) - strftime('%s', t.executed_at)) < 300
               WHERE od.decision='APPROVE' AND od.decided_at >= ?
                 AND t.realized_pnl IS NOT NULL""",
            (cutoff,)
        ).fetchone()

        c.close()
        total_wr = wr_row["total"] or 0
        wins_wr  = wr_row["wins"] or 0
        filter_wr = wins_wr / total_wr * 100.0 if total_wr > 0 else 0.0

        return {
            "approved":   approved,
            "rejected":   rejected,
            "total":      approved + rejected,
            "filter_wr":  round(filter_wr, 1),
            "threshold":  THRESHOLD,
        }
    except Exception as e:
        return {"approved": 0, "rejected": 0, "total": 0, "filter_wr": 0.0, "error": str(e)}
