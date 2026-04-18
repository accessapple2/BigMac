#!/usr/bin/env python3
"""
Riker Fleet Synthesis — XO auto-synthesis every 10 minutes.
Adapted to actual schema:
  signals:   player_id, symbol, signal, confidence, created_at
  trades:    player_id, symbol, action, qty, price, executed_at
  positions: player_id, symbol, qty, avg_price, opened_at
  rikers_log: entry_type, source, title, content, ticker, conviction
"""
from __future__ import annotations

import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [RIKER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_PATH    = Path(__file__).parent.parent / "data" / "trader.db"
NTFY_TOPIC = "ollietrades-admin"

SIGNIFICANT: dict[str, int] = {
    "high_conf_signals": 3,   # 3+ signals with conf >= 80 in 10 min
    "convergence":       3,   # 3+ agents on same symbol
    "new_trades":        2,   # 2+ trades executed in 10 min
}


# ── ntfy ─────────────────────────────────────────────────────────────────────
def _ntfy(title: str, body: str, priority: str = "default") -> None:
    try:
        requests.post(
            f"https://ntfy.sh/{NTFY_TOPIC}",
            data=body.encode("utf-8"),
            headers={"Title": title, "Priority": priority},
            timeout=10,
        )
    except Exception as e:
        log.warning(f"ntfy error: {e}")


# ── Data fetchers ─────────────────────────────────────────────────────────────
def _get_recent_signals(minutes: int) -> list[dict]:
    conn   = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(minutes=minutes)).isoformat()
    rows   = conn.execute("""
        SELECT player_id, symbol, signal, confidence, created_at
        FROM   signals
        WHERE  created_at >= ?
        ORDER  BY confidence DESC
    """, (cutoff,)).fetchall()
    conn.close()
    return [{"agent": r[0], "symbol": r[1], "signal": r[2],
             "confidence": r[3] or 0, "created_at": r[4]} for r in rows]


def _get_recent_trades(minutes: int) -> list[dict]:
    conn   = sqlite3.connect(DB_PATH)
    cutoff = (datetime.now() - timedelta(minutes=minutes)).isoformat()
    rows   = conn.execute("""
        SELECT player_id, symbol, action, qty, price, executed_at
        FROM   trades
        WHERE  executed_at >= ?
        ORDER  BY executed_at DESC
    """, (cutoff,)).fetchall()
    conn.close()
    return [{"agent": r[0], "symbol": r[1], "action": r[2],
             "qty": r[3], "price": r[4], "executed_at": r[5]} for r in rows]


def _get_fleet_positions() -> list[dict]:
    """Count open positions per agent (all rows in positions table)."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute("""
        SELECT player_id, COUNT(*) AS positions,
               SUM(qty * avg_price)  AS approx_value
        FROM   positions
        GROUP  BY player_id
    """).fetchall()
    conn.close()
    return [{"agent": r[0], "positions": r[1], "approx_value": round(r[2] or 0, 2)}
            for r in rows]


# ── Convergence ───────────────────────────────────────────────────────────────
def _find_convergence(signals: list[dict]) -> list[dict]:
    """Find symbols where 2+ agents issued high-confidence signals."""
    by_symbol: dict[str, list[str]] = defaultdict(list)
    for s in signals:
        if (s["confidence"] or 0) >= 70:
            by_symbol[s["symbol"]].append(s["agent"])

    result = [{"symbol": sym, "agents": agents, "count": len(agents)}
              for sym, agents in by_symbol.items() if len(agents) >= 2]
    return sorted(result, key=lambda x: x["count"], reverse=True)


# ── Synthesis ─────────────────────────────────────────────────────────────────
def generate_synthesis(minutes: int = 10) -> dict:
    signals     = _get_recent_signals(minutes)
    trades      = _get_recent_trades(minutes)
    positions   = _get_fleet_positions()
    convergence = _find_convergence(signals)

    high_conf = [s for s in signals if (s["confidence"] or 0) >= 80]

    agent_activity: dict[str, int] = defaultdict(int)
    for s in signals:
        agent_activity[s["agent"]] += 1

    return {
        "timestamp":      datetime.now().isoformat(),
        "period_minutes": minutes,
        "summary": {
            "total_signals":     len(signals),
            "high_conf_signals": len(high_conf),
            "trades_executed":   len(trades),
            "open_positions":    sum(p["positions"] for p in positions),
            "fleet_agents":      len(positions),
        },
        "high_confidence": high_conf[:5],
        "convergence":     convergence[:3],
        "agent_activity":  dict(agent_activity),
        "recent_trades":   trades[:5],
    }


# ── Persist ───────────────────────────────────────────────────────────────────
def _save_synthesis(synthesis: dict) -> None:
    """Save to rikers_log (existing table)."""
    conn = sqlite3.connect(DB_PATH)
    s    = synthesis["summary"]

    # Build short title
    title = (
        f"Synthesis {datetime.now().strftime('%H:%M')} | "
        f"Sigs:{s['total_signals']} HC:{s['high_conf_signals']} "
        f"Trades:{s['trades_executed']}"
    )

    conn.execute("""
        INSERT INTO rikers_log (entry_type, source, title, content, conviction)
        VALUES ('synthesis', 'riker', ?, ?, ?)
    """, (title, json.dumps(synthesis), s["high_conf_signals"] / 10.0))
    conn.commit()
    conn.close()


# ── Alerts ────────────────────────────────────────────────────────────────────
def _check_alerts(synthesis: dict) -> list[str]:
    events = []
    s      = synthesis["summary"]

    if s["high_conf_signals"] >= SIGNIFICANT["high_conf_signals"]:
        events.append(f"{s['high_conf_signals']} high-confidence signals in last {synthesis['period_minutes']}m")

    for conv in synthesis["convergence"]:
        if conv["count"] >= SIGNIFICANT["convergence"]:
            events.append(f"{conv['count']} agents converging on {conv['symbol']}")

    if s["trades_executed"] >= SIGNIFICANT["new_trades"]:
        events.append(f"{s['trades_executed']} new trades executed")

    return events


# ── Main ──────────────────────────────────────────────────────────────────────
def run_synthesis() -> dict:
    synthesis = generate_synthesis(minutes=10)
    s         = synthesis["summary"]

    log.info(
        f"Signals:{s['total_signals']} HC:{s['high_conf_signals']} "
        f"Trades:{s['trades_executed']} Positions:{s['open_positions']}"
    )

    if synthesis["convergence"]:
        for conv in synthesis["convergence"]:
            log.info(f"  Convergence: {conv['symbol']} x{conv['count']} ({', '.join(conv['agents'][:3])})")

    _save_synthesis(synthesis)

    alerts = _check_alerts(synthesis)
    if alerts:
        body = "\n".join(alerts)
        _ntfy("Riker Fleet Alert", body, priority="high")
        log.info(f"Alerted: {len(alerts)} significant events")

    return synthesis


if __name__ == "__main__":
    run_synthesis()
