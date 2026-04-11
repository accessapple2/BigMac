"""
Agent Performance Rating System — USS TradeMinds
=================================================
Tracks every agent with an A–E grade. Auto-detects when agents go cold.
Recommends lineup changes like a pitching coach.

Closed trade definition (actual schema):
  action='SELL' AND realized_pnl IS NOT NULL
"""

from __future__ import annotations

import logging
import os
import re
import sqlite3
from typing import Any

# Only Season 5 data is clean (pre-S5 had options mispriced as stocks)
_CURRENT_SEASON = 5

# Reject any single trade where |pnl| > 50% of a $7k account
_MAX_SANE_PNL = 3_500.0

# Stock ticker: 1–5 uppercase letters only (no option contract strings)
_STOCK_RE = re.compile(r'^[A-Z]{1,5}$')

logger = logging.getLogger("agent_ratings")

DB_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "trader.db"))

# System/non-trading players excluded from fleet reports
_SKIP_PLAYERS = {
    "dayblade-0dte",        # 0DTE options bot — separate scoring system
    "red-alert",            # system alert agent, not a trader
    "enterprise-computer",  # dilithium reserve, not a trader
    "steve-webull",         # human benchmark, not an AI agent
}


def _conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=5)
    conn.row_factory = sqlite3.Row
    return conn


# ---------------------------------------------------------------------------
# Core rating calculator
# ---------------------------------------------------------------------------

def calculate_rating(player_id: str, period: str = "alltime") -> dict[str, Any]:
    """
    Calculate A–E rating for a single agent.

    period: 'daily' | 'weekly' | 'alltime'
    Returns dict with rating, score, and all stats. Saves to agent_ratings table.
    """
    conn = _conn()

    if period == "daily":
        where = "AND executed_at > datetime('now', '-1 day')"
    elif period == "weekly":
        where = "AND executed_at > datetime('now', '-7 days')"
    else:
        where = ""

    # Fix 1: Season 5 only — pre-S5 had options mispriced as stocks
    rows = conn.execute(f"""
        SELECT realized_pnl, confidence, entry_price, exit_price,
               executed_at, symbol, asset_type
        FROM trades
        WHERE player_id = ?
          AND action = 'SELL'
          AND realized_pnl IS NOT NULL
          AND season = {_CURRENT_SEASON}
          {where}
        ORDER BY executed_at DESC
    """, (player_id,)).fetchall()
    conn.close()

    # Fix 2 & 3: drop suspicious trades and non-stock symbols
    clean_rows = []
    skipped = 0
    for r in rows:
        pnl    = float(r["realized_pnl"])
        sym    = (r["symbol"] or "").strip()
        atype  = (r["asset_type"] or "stock").lower()

        # Fix 2: reject trades where |pnl| > 50% of $7k account
        if abs(pnl) > _MAX_SANE_PNL:
            logger.warning(
                f"SUSPICIOUS: {player_id} trade {sym} pnl=${pnl:.0f} "
                f"exceeds ${_MAX_SANE_PNL:.0f} sanity cap — excluding from rating"
            )
            skipped += 1
            continue

        # Fix 3: stock symbols only — skip options contracts
        if atype in ("option", "options") or not _STOCK_RE.match(sym):
            skipped += 1
            continue

        clean_rows.append(r)

    if skipped:
        logger.info(f"[{player_id}] Excluded {skipped} suspicious/options trades from rating")

    if len(clean_rows) < 2:
        return {
            "player_id": player_id,
            "period": period,
            "rating": "N/A",
            "rating_score": 0.0,
            "reason": f"Not enough clean trades (total={len(rows)}, excluded={skipped})",
            "total_trades": len(clean_rows),
        }

    pnls = [float(r["realized_pnl"]) for r in clean_rows]
    confs = [float(r["confidence"] or 0) for r in clean_rows]

    wins   = [p for p in pnls if p > 0]
    losses = [p for p in pnls if p <= 0]

    win_rate      = len(wins) / len(pnls) * 100
    total_pnl     = sum(pnls)
    avg_win       = sum(wins) / len(wins) if wins else 0.0
    avg_loss      = sum(losses) / len(losses) if losses else 0.0
    gross_wins    = sum(wins)
    gross_losses  = abs(sum(losses))
    profit_factor = gross_wins / gross_losses if gross_losses > 0 else 99.0
    avg_conf      = sum(confs) / len(confs) if confs else 0.0

    # Consecutive losses/wins from most recent
    consec_losses = 0
    for p in pnls:
        if p <= 0:
            consec_losses += 1
        else:
            break

    consec_wins = 0
    for p in pnls:
        if p > 0:
            consec_wins += 1
        else:
            break

    # ── Composite score (0–100) ──────────────────────────────────────────────
    score = 0.0

    # Win rate: 0–40 pts (THE most important metric)
    # 50% WR = 20pts, 60% = 28pts, 70% = 36pts, 80%+ = 40pts
    score += min(40.0, win_rate * 0.5)

    # Profit factor: 0–20 pts
    score += min(20.0, profit_factor * 8.0)

    # Total P&L: 0–15 pts (penalty up to -10 for losses)
    if total_pnl > 0:
        score += min(15.0, total_pnl / 70.0)   # +$1050 = 15 pts
    else:
        score += max(-10.0, total_pnl / 100.0)

    # Avg win/loss ratio: 0–15 pts
    if avg_loss != 0:
        ratio = abs(avg_win / avg_loss)
        score += min(15.0, ratio * 5.0)

    # Trade count bonus (experience): 0–10 pts
    score += min(10.0, len(pnls) * 0.5)

    # Consecutive loss penalty: -5 per streak
    score -= consec_losses * 5.0

    # Win rate bonus tiers
    if win_rate >= 70: score += 5.0   # Elite bonus
    if win_rate >= 80: score += 5.0   # Legendary bonus

    score = max(0.0, min(100.0, score))

    # ── Letter grade ─────────────────────────────────────────────────────────
    if score >= 80:   rating = "A"
    elif score >= 65: rating = "B"
    elif score >= 50: rating = "C"
    elif score >= 35: rating = "D"
    else:             rating = "E"

    result: dict[str, Any] = {
        "player_id":           player_id,
        "period":              period,
        "total_trades":        len(pnls),
        "wins":                len(wins),
        "losses":              len(losses),
        "win_rate":            round(win_rate, 1),
        "total_pnl":           round(total_pnl, 2),
        "avg_win":             round(avg_win, 2),
        "avg_loss":            round(avg_loss, 2),
        "profit_factor":       round(profit_factor, 2),
        "best_trade":          round(max(pnls), 2),
        "worst_trade":         round(min(pnls), 2),
        "consecutive_losses":  consec_losses,
        "consecutive_wins":    consec_wins,
        "avg_confidence":      round(avg_conf, 1),
        "pass_rate":           0.0,   # not computed here (no PASS signal count)
        "volume_accuracy":     0.0,   # reserved for future
        "rating":              rating,
        "rating_score":        round(score, 1),
    }

    # Save snapshot to DB (never overwrite — historical trend tracking)
    try:
        wconn = _conn()
        wconn.execute("""
            INSERT INTO agent_ratings
            (player_id, period, total_trades, wins, losses, win_rate,
             total_pnl, avg_win, avg_loss, profit_factor, best_trade,
             worst_trade, consecutive_losses, consecutive_wins,
             avg_confidence, pass_rate, volume_accuracy, rating, rating_score)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            player_id, period, result["total_trades"], result["wins"],
            result["losses"], result["win_rate"], result["total_pnl"],
            result["avg_win"], result["avg_loss"], result["profit_factor"],
            result["best_trade"], result["worst_trade"],
            result["consecutive_losses"], result["consecutive_wins"],
            result["avg_confidence"], result["pass_rate"],
            result["volume_accuracy"], result["rating"], result["rating_score"],
        ))
        wconn.commit()
        wconn.close()
    except Exception as e:
        logger.warning(f"Failed to save rating for {player_id}: {e}")

    return result


# ---------------------------------------------------------------------------
# Fleet report card
# ---------------------------------------------------------------------------

def _get_active_fleet() -> list[tuple[str, str]]:
    """Return list of (player_id, display_name) for all tradeable active players."""
    conn = _conn()
    rows = conn.execute(
        "SELECT id, display_name FROM ai_players WHERE is_active=1 ORDER BY id"
    ).fetchall()
    conn.close()
    return [(r["id"], r["display_name"]) for r in rows if r["id"] not in _SKIP_PLAYERS]


def fleet_report_card() -> list[dict[str, Any]]:
    """
    Generate alltime ratings for all active agents.
    Returns list sorted by rating_score descending.
    """
    fleet = _get_active_fleet()
    alltime: list[dict[str, Any]] = []

    for pid, dname in fleet:
        r = calculate_rating(pid, "alltime")
        r["display_name"] = dname
        alltime.append(r)

    alltime.sort(key=lambda x: x["rating_score"], reverse=True)
    return alltime


def fleet_report_weekly() -> list[dict[str, Any]]:
    """Weekly ratings for all active agents, sorted by score."""
    fleet = _get_active_fleet()
    weekly: list[dict[str, Any]] = []
    for pid, dname in fleet:
        r = calculate_rating(pid, "weekly")
        r["display_name"] = dname
        weekly.append(r)
    weekly.sort(key=lambda x: x["rating_score"], reverse=True)
    return weekly


# ---------------------------------------------------------------------------
# Cold agent detection
# ---------------------------------------------------------------------------

def detect_cold_agents() -> list[dict[str, Any]]:
    """
    Find agents who are going cold — like a tired pitcher.
    Checks last 5 closed trades per agent.
    """
    conn = _conn()
    fleet = _get_active_fleet()
    cold: list[dict[str, Any]] = []

    for pid, dname in fleet:
        recent = conn.execute("""
            SELECT realized_pnl FROM trades
            WHERE player_id = ? AND action = 'SELL' AND realized_pnl IS NOT NULL
            ORDER BY executed_at DESC LIMIT 5
        """, (pid,)).fetchall()

        if len(recent) < 3:
            continue

        recent_pnl = [float(r["realized_pnl"]) for r in recent]
        recent_wins = sum(1 for p in recent_pnl if p > 0)
        recent_wr   = recent_wins / len(recent_pnl) * 100

        if recent_wr < 25:
            cold.append({
                "player_id":      pid,
                "display_name":   dname,
                "reason":         f"Win rate dropped to {recent_wr:.0f}% (last {len(recent_pnl)} trades)",
                "recommendation": "BENCH — switch to rules-only or reduce size",
            })

        elif all(p < 0 for p in recent_pnl[:3]):
            cold.append({
                "player_id":      pid,
                "display_name":   dname,
                "reason":         "3 consecutive losses",
                "recommendation": "COOLDOWN — 1 day break, then half size",
            })

        # Losses getting bigger
        losses_only = [p for p in recent_pnl if p < 0]
        if len(losses_only) >= 2 and losses_only[0] < losses_only[1] < 0:
            cold.append({
                "player_id":      pid,
                "display_name":   dname,
                "reason":         "Losses escalating — last loss worse than previous",
                "recommendation": "TIGHTEN STOPS — reduce max loss threshold",
            })

    conn.close()
    return cold


# ---------------------------------------------------------------------------
# Lineup advisor
# ---------------------------------------------------------------------------

def lineup_advisor() -> list[dict[str, Any]]:
    """
    Recommend lineup changes based on agent performance.
    Combines fleet_report_card + detect_cold_agents.
    """
    report = fleet_report_card()
    cold   = detect_cold_agents()
    cold_ids = {c["player_id"] for c in cold}

    advice: list[dict[str, Any]] = []

    for agent in report:
        pid    = agent["player_id"]
        rating = agent["rating"]

        if rating == "N/A":
            continue

        if rating in ("D", "E"):
            advice.append({
                "player_id":  pid,
                "display_name": agent.get("display_name", pid),
                "action":     "BENCH",
                "icon":       "🚫",
                "reason":     (f"Rating {rating} ({agent['rating_score']:.0f}/100). "
                               f"Win rate: {agent['win_rate']}%. "
                               f"P&L: ${agent['total_pnl']:.2f}"),
            })
        elif rating == "C" and agent["consecutive_losses"] >= 2:
            advice.append({
                "player_id":  pid,
                "display_name": agent.get("display_name", pid),
                "action":     "REDUCE SIZE",
                "icon":       "⚠️",
                "reason":     (f"Rating {rating} with {agent['consecutive_losses']} "
                               f"consecutive losses. Reduce to half size."),
            })
        elif rating in ("A", "B"):
            advice.append({
                "player_id":  pid,
                "display_name": agent.get("display_name", pid),
                "action":     "FULL THROTTLE",
                "icon":       "🔥",
                "reason":     (f"Rating {rating} ({agent['rating_score']:.0f}/100). "
                               f"Win rate: {agent['win_rate']}%. Keep at full size."),
            })

    for c in cold:
        if c["player_id"] not in {a["player_id"] for a in advice}:
            advice.append({
                "player_id":  c["player_id"],
                "display_name": c.get("display_name", c["player_id"]),
                "action":     "COLD ALERT",
                "icon":       "🧊",
                "reason":     c["reason"] + " — " + c["recommendation"],
            })

    return advice


# ---------------------------------------------------------------------------
# Rating trend (for dashboard ⬆️ ➡️ ⬇️ indicator)
# ---------------------------------------------------------------------------

def get_rating_trend(player_id: str) -> str:
    """
    Compare most recent weekly rating vs previous weekly rating.
    Returns '▲', '▼', or '→'.
    """
    try:
        conn = _conn()
        rows = conn.execute("""
            SELECT rating_score FROM agent_ratings
            WHERE player_id = ? AND period = 'weekly'
            ORDER BY timestamp DESC LIMIT 2
        """, (player_id,)).fetchall()
        conn.close()
        if len(rows) < 2:
            return "→"
        curr = float(rows[0]["rating_score"])
        prev = float(rows[1]["rating_score"])
        if curr > prev + 2:   return "▲"
        if curr < prev - 2:   return "▼"
        return "→"
    except Exception:
        return "→"


# ---------------------------------------------------------------------------
# Bulk recalculation
# ---------------------------------------------------------------------------

def recalculate_all_ratings() -> None:
    """
    Recalculate ratings for the full active fleet using clean Season 5 data.
    Inserts fresh snapshots — historical rows are preserved for trend tracking.
    Safe to call on startup.
    """
    fleet = _get_active_fleet()
    logger.info(f"[RATINGS] Recalculating for {len(fleet)} active agents (Season {_CURRENT_SEASON} only)...")
    for pid, dname in fleet:
        for period in ("alltime", "weekly"):
            try:
                r = calculate_rating(pid, period)
                if r["rating"] != "N/A":
                    logger.info(
                        f"[RATINGS]  {pid:<22} {period:<8} "
                        f"{r['rating']}  {r['rating_score']:.0f}/100  "
                        f"{r['wins']}W/{r['losses']}L  "
                        f"WR={r['win_rate']}%  P&L=${r['total_pnl']:.2f}"
                    )
            except Exception as e:
                logger.warning(f"[RATINGS] Failed {pid}/{period}: {e}")
    logger.info("[RATINGS] Recalculation complete.")
