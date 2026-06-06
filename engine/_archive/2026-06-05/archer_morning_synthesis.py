#!/usr/bin/env python3
"""
Captain Archer — Morning Synthesis Briefing
Runs daily at 6:25 AM AZ (9:25 AM ET, 5 min before market open).
Combines all intelligence sources into one actionable briefing.

Sources:
  - Kirk Advisory (portfolio_advice — trader.db)
  - Lt. Uhura institutional signals (institutional_signals — trader.db)
  - Long Range Sensors whale detections (whale_detections — trader.db)
  - Top scanner signals (trade_signals — signal-center/signals.db)
  - Market regime (dashboard API)

Usage: cd ~/autonomous-trader && venv/bin/python3 engine/archer_morning_synthesis.py
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import datetime
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [ARCHER] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)

DB_TRADER  = Path(__file__).parent.parent / "data" / "trader.db"
DB_SIGNALS = Path(__file__).parent.parent / "signal-center" / "signals.db"
NTFY_ADMIN = "https://ntfy.sh/ollietrades-admin"
NTFY_CREW  = "https://ntfy.sh/ollietrades-crew"


def init_table():
    conn = sqlite3.connect(DB_TRADER)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS archer_briefings (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            briefing   TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_kirk_advisory() -> list[dict]:
    conn = sqlite3.connect(DB_TRADER)
    try:
        rows = conn.execute("""
            SELECT symbol, action, reasoning, model_used
            FROM portfolio_advice
            WHERE date(created_at) = date('now')
            ORDER BY created_at DESC LIMIT 10
        """).fetchall()
        return [{"ticker": r[0],
                 "advice": f"{r[1] or ''}: {(r[2] or '')[:80]}",
                 "model":  r[3]} for r in rows]
    except Exception as e:
        log.warning(f"Kirk advisory fetch failed: {e}")
        return []
    finally:
        conn.close()


def get_uhura_signals() -> list[dict]:
    conn = sqlite3.connect(DB_TRADER)
    try:
        rows = conn.execute("""
            SELECT ticker, signal, reasoning
            FROM institutional_signals
            WHERE scan_date >= date('now', '-1 day')
              AND signal IN ('STRONG_BUY', 'BUY', 'STRONG_SELL', 'SELL')
            ORDER BY created_at DESC LIMIT 10
        """).fetchall()
        return [{"ticker": r[0], "signal": r[1], "reason": r[2]} for r in rows]
    except Exception as e:
        log.warning(f"Uhura signals fetch failed: {e}")
        return []
    finally:
        conn.close()


def get_whale_detections() -> list[dict]:
    conn = sqlite3.connect(DB_TRADER)
    try:
        rows = conn.execute("""
            SELECT symbol, rel_volume, price, alert_type
            FROM whale_detections
            WHERE detected_at > datetime('now', '-24 hours')
            ORDER BY rel_volume DESC LIMIT 5
        """).fetchall()
        return [{"symbol": r[0], "volume": r[1], "price": r[2], "type": r[3]}
                for r in rows]
    except Exception as e:
        log.warning(f"Whale detections fetch failed: {e}")
        return []
    finally:
        conn.close()


def get_scanner_top_signals() -> list[dict]:
    """Pull high-confidence signals from signal-center/signals.db."""
    if not DB_SIGNALS.exists():
        return []
    conn = sqlite3.connect(DB_SIGNALS)
    try:
        rows = conn.execute("""
            SELECT symbol, confidence, agent_name, reasoning
            FROM trade_signals
            WHERE created_at > datetime('now', '-12 hours')
              AND confidence >= 80
            ORDER BY confidence DESC LIMIT 10
        """).fetchall()
        return [{"symbol": r[0], "conf": r[1], "agent": r[2],
                 "reason": (r[3] or "")[:100]}
                for r in rows]
    except Exception as e:
        log.warning(f"Scanner signals fetch failed: {e}")
        return []
    finally:
        conn.close()


def get_market_regime() -> dict:
    for endpoint in ["http://localhost:8080/api/regime",
                     "http://localhost:8080/api/health"]:
        try:
            resp = requests.get(endpoint, timeout=5)
            if resp.status_code == 200:
                data = resp.json()
                return {
                    "regime": data.get("regime", data.get("status", "UNKNOWN")),
                    "vix":    data.get("vix", 0),
                }
        except Exception:
            pass
    return {"regime": "UNKNOWN", "vix": 0}


def build_briefing() -> str:
    now    = datetime.now()
    kirk   = get_kirk_advisory()
    uhura  = get_uhura_signals()
    whales = get_whale_detections()
    sigs   = get_scanner_top_signals()
    regime = get_market_regime()

    lines = [
        "CAPTAIN ARCHER -- MORNING BRIEFING",
        f"{now.strftime('%A, %B %d, %Y')} -- 09:25 ET",
        "",
        f"MARKET REGIME: {regime['regime']}"
        + (f"  VIX: {regime['vix']:.1f}" if regime.get("vix") else ""),
        "",
    ]

    if kirk:
        lines.append("KIRK ADVISORY:")
        for k in kirk[:3]:
            lines.append(f"  {k['ticker']}: {k['advice']}...")
        lines.append("")

    if uhura:
        lines.append("LT. UHURA (Institutional):")
        for u in uhura[:3]:
            lines.append(f"  {u['ticker']}: {u['signal']} — {u['reason']}")
        lines.append("")

    if whales:
        lines.append("LONG RANGE SENSORS (Whales):")
        for w in whales[:3]:
            lines.append(f"  {w['symbol']}: {w['volume']:.0f}x volume @ ${w['price']:.2f} [{w['type']}]")
        lines.append("")

    if sigs:
        lines.append(f"TOP SIGNALS (>=80% conf): {len(sigs)} found")
        for s in sigs[:5]:
            lines.append(f"  {s['symbol']}: {s['conf']}% via {s['agent']}")
        lines.append("")

    # Focus tickers
    focus = set()
    for k in kirk[:2]:
        focus.add(k["ticker"])
    for s in sigs[:3]:
        focus.add(s["symbol"])
    for w in whales[:2]:
        focus.add(w["symbol"])

    lines.append("TODAY'S FOCUS:")
    lines.append(f"  Watch: {', '.join(list(focus)[:8]) or 'none'}")
    lines.append("")
    lines.append("Archer out. Good hunting, Admiral.")

    # Strip any non-latin-1 chars so ntfy Title header doesn't explode
    return "\n".join(lines).encode("ascii", errors="replace").decode("ascii")

    return "\n".join(lines)


def send_briefing() -> str:
    init_table()
    briefing = build_briefing()

    log.info("\n" + briefing)

    # Send to ntfy (both channels)
    for ntfy_url in [NTFY_ADMIN, NTFY_CREW]:
        try:
            requests.post(
                ntfy_url,
                data=briefing.encode("utf-8"),
                headers={"Priority": "default", "Title": "Morning Briefing -- Archer"},
                timeout=10,
            )
        except Exception as e:
            log.warning(f"ntfy failed: {e}")

    # Persist to DB
    conn = sqlite3.connect(DB_TRADER)
    conn.execute("INSERT INTO archer_briefings (briefing) VALUES (?)", (briefing,))
    conn.commit()
    conn.close()

    log.info("Briefing sent and saved")
    return briefing


if __name__ == "__main__":
    send_briefing()
