"""Ship's Computer — Captain's Portfolio Monitor

Runs every 5 min during market hours. Detects:
  STOP_BREACH  — current price ≤ Grok's recommended stop_loss
  BIG_MOVE     — session P&L moved > ±3%
  NEW_ADVICE   — advisor issued SELL or TRIM (not yet seen)

Alerts are stored in ship_computer_alerts table and surfaced via
GET /api/ship-computer/portfolio-alerts.
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime, timedelta

logger = logging.getLogger("portfolio_monitor")

DB = "data/trader.db"
BIG_MOVE_THRESHOLD = 3.0   # %
ALERT_TTL_HOURS = 4        # dedupe window


def _conn() -> sqlite3.Connection:
    c = sqlite3.connect(DB, check_same_thread=False)
    c.execute("PRAGMA journal_mode=WAL")
    c.row_factory = sqlite3.Row
    return c


def _init_db():
    conn = _conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ship_computer_alerts (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            alert_type  TEXT NOT NULL,
            symbol      TEXT NOT NULL,
            source      TEXT NOT NULL DEFAULT 'ship_computer',
            message     TEXT,
            detail      TEXT,
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            seen        INTEGER DEFAULT 0,
            seen_at     TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def _already_fired(conn: sqlite3.Connection, alert_type: str, symbol: str) -> bool:
    """Return True if same alert fired within TTL window."""
    cutoff = (datetime.utcnow() - timedelta(hours=ALERT_TTL_HOURS)).isoformat()
    row = conn.execute(
        "SELECT id FROM ship_computer_alerts "
        "WHERE alert_type=? AND symbol=? AND created_at > ?",
        (alert_type, symbol, cutoff),
    ).fetchone()
    return row is not None


def _save_alert(conn: sqlite3.Connection, alert_type: str, symbol: str, message: str, detail: str = ""):
    conn.execute(
        "INSERT INTO ship_computer_alerts (alert_type, symbol, source, message, detail) VALUES (?,?,?,?,?)",
        (alert_type, symbol, "ship_computer", message, detail),
    )


def check_captains_portfolio() -> list[dict]:
    """Check positions for stop breaches, big moves, and new advice.

    Returns list of new alert dicts (empty if nothing triggered).
    """
    _init_db()
    alerts: list[dict] = []

    conn = _conn()
    try:
        # ── Fetch current positions ──────────────────────────────────────────
        positions = conn.execute(
            "SELECT symbol, qty, avg_price, "
            "  COALESCE(current_price, avg_price) AS current_price "
            "FROM positions "
            "WHERE player_id='steve-webull' AND qty > 0",
        ).fetchall()

        if not positions:
            return []

        # ── Fetch latest non-expired Grok stop levels ────────────────────────
        stop_map: dict[str, float] = {}
        rows = conn.execute(
            "SELECT symbol, stop_loss FROM portfolio_advice "
            "WHERE advisor='grok' AND stop_loss IS NOT NULL "
            "  AND (expires_at IS NULL OR expires_at > datetime('now')) "
            "ORDER BY created_at DESC",
        ).fetchall()
        seen_sym: set[str] = set()
        for r in rows:
            s = r["symbol"]
            if s not in seen_sym:
                seen_sym.add(s)
                stop_map[s] = float(r["stop_loss"])

        # ── Check each position ──────────────────────────────────────────────
        for p in positions:
            sym = p["symbol"]
            avg = float(p["avg_price"] or 0)
            cur = float(p["current_price"] or avg)
            pnl_pct = round((cur - avg) / avg * 100, 2) if avg else 0

            # STOP_BREACH
            if sym in stop_map and cur <= stop_map[sym]:
                if not _already_fired(conn, "STOP_BREACH", sym):
                    msg = f"🖥️ Stop hit: {sym} at ${cur:.2f} (stop ${stop_map[sym]:.2f})"
                    _save_alert(conn, "STOP_BREACH", sym, msg,
                                f"current={cur:.2f} stop={stop_map[sym]:.2f} pnl={pnl_pct:+.1f}%")
                    alerts.append({"type": "STOP_BREACH", "symbol": sym, "message": msg,
                                   "pnl_pct": pnl_pct, "source": "ship_computer"})
                    logger.warning("STOP_BREACH: %s $%.2f ≤ stop $%.2f", sym, cur, stop_map[sym])

            # BIG_MOVE
            elif abs(pnl_pct) >= BIG_MOVE_THRESHOLD:
                alert_type = "BIG_MOVE_UP" if pnl_pct > 0 else "BIG_MOVE_DOWN"
                if not _already_fired(conn, alert_type, sym):
                    direction = "▲" if pnl_pct > 0 else "▼"
                    msg = f"🖥️ Big move: {sym} {direction}{abs(pnl_pct):.1f}% (${cur:.2f})"
                    _save_alert(conn, alert_type, sym, msg,
                                f"current={cur:.2f} avg={avg:.2f} pnl={pnl_pct:+.1f}%")
                    alerts.append({"type": alert_type, "symbol": sym, "message": msg,
                                   "pnl_pct": pnl_pct, "source": "ship_computer"})
                    logger.info("BIG_MOVE %s %+.1f%%", sym, pnl_pct)

        # ── Check for new SELL/TRIM advice ───────────────────────────────────
        new_advice = conn.execute(
            "SELECT symbol, action, advisor, model_used, created_at "
            "FROM portfolio_advice "
            "WHERE action IN ('SELL','TRIM') "
            "  AND (expires_at IS NULL OR expires_at > datetime('now')) "
            "  AND created_at > datetime('now', '-30 minutes')",
        ).fetchall()
        for a in new_advice:
            sym = a["symbol"]
            if not _already_fired(conn, "NEW_ADVICE", sym):
                adv = a["advisor"].upper()
                model = a["model_used"] or adv
                source_badge = {"grok": "🧠", "troi": "💜", "worf": "🛡️"}.get(adv.lower(), "🖥️")
                msg = f"{source_badge} New {adv} advice: {a['action']} {sym}"
                _save_alert(conn, "NEW_ADVICE", sym, msg,
                            f"advisor={adv} action={a['action']} model={model}")
                alerts.append({"type": "NEW_ADVICE", "symbol": sym, "message": msg,
                               "action": a["action"], "source": a["advisor"]})
                logger.info("NEW_ADVICE: %s %s from %s", a["action"], sym, adv)

        conn.commit()
    except Exception as e:
        logger.error("portfolio monitor error: %s", e)
    finally:
        conn.close()

    return alerts


def get_recent_alerts(limit: int = 30) -> list[dict]:
    """Return most recent Ship's Computer portfolio alerts."""
    _init_db()
    try:
        conn = _conn()
        rows = conn.execute(
            "SELECT id, alert_type, symbol, source, message, detail, created_at, seen "
            "FROM ship_computer_alerts "
            "ORDER BY created_at DESC LIMIT ?",
            (limit,),
        ).fetchall()
        conn.close()
        return [dict(r) for r in rows]
    except Exception as e:
        logger.error("get_recent_alerts: %s", e)
        return []


def mark_seen(alert_ids: list[int]):
    """Mark alert IDs as seen."""
    if not alert_ids:
        return
    try:
        conn = _conn()
        now = datetime.utcnow().isoformat()
        conn.execute(
            f"UPDATE ship_computer_alerts SET seen=1, seen_at=? WHERE id IN ({','.join('?' * len(alert_ids))})",
            [now] + alert_ids,
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error("mark_seen: %s", e)
