"""HM-ARCHER-REBUILD — Tiered alert engine. NTFY + Bridge delivery, deduped.

Tiers (Admiral-locked):
  RED    = 5/5 convergence OR new short signal (sell-the-news) OR
           major options flow.
  YELLOW = 3-4/5 convergence OR single-system notable flag OR regime shift.

Dedup: one alert per (tier, symbol, systems) — same combo never re-fires.
Short-signal RED trigger is wired-and-ready but dormant until sell-the-news
emits (earnings season). ADVISORY ONLY — alerts narrate, never execute.
"""
from __future__ import annotations

import hashlib
import logging
import sqlite3
from pathlib import Path

import requests

from engine.archer.convergence import compute_convergence
from engine.archer import intel_sources as src
from engine.archer.brain import alert_narrative

logger = logging.getLogger(__name__)

_ROOT = Path(__file__).resolve().parent.parent.parent
TRADER_DB = _ROOT / "data" / "trader.db"
NTFY = "https://ntfy.sh/ollietrades-admin"


def _ensure_table() -> None:
    conn = sqlite3.connect(TRADER_DB)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS archer_alerts (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            tier       TEXT,
            symbol     TEXT,
            systems    TEXT,
            narrative  TEXT,
            dedup_key  TEXT UNIQUE,
            created_at TEXT DEFAULT (datetime('now'))
        )
        """
    )
    conn.commit()
    conn.close()


def _save_notification(title: str, body: str, severity: str, notif_type: str, icon: str) -> None:
    """Write a dashboard notification (trader.db) so Archer announces it on the
    frontend. 5-min title+body dedup. Never raises."""
    try:
        conn = sqlite3.connect(TRADER_DB, timeout=10)
        try:
            conn.execute(
                """CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT DEFAULT CURRENT_TIMESTAMP,
                    type TEXT, severity TEXT, title TEXT, body TEXT,
                    icon TEXT, agent_id TEXT, acknowledged INTEGER DEFAULT 0)"""
            )
            exists = conn.execute(
                "SELECT id FROM notifications WHERE title=? AND body=? "
                "AND timestamp >= datetime('now','-5 minutes')",
                (title, body),
            ).fetchone()
            if not exists:
                conn.execute(
                    "INSERT INTO notifications (type, severity, title, body, icon) VALUES (?,?,?,?,?)",
                    (notif_type, severity, title, body, icon),
                )
                conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.warning("[Archer/alerts] notify failed: %s: %r", type(e).__name__, e)


def _dedup_key(tier: str, symbol: str, systems: list[str]) -> str:
    raw = f"{tier}:{symbol}:{','.join(sorted(systems))}"
    return hashlib.md5(raw.encode()).hexdigest()[:16]


def _candidates() -> list[dict]:
    """Gather alert candidates from convergence + short signals."""
    cands: list[dict] = []

    for c in compute_convergence():
        if c["tier"] in ("RED", "YELLOW"):
            cands.append({"tier": c["tier"], "symbol": c["symbol"],
                          "systems": c["systems"], "count": c["count"]})

    # Short signals are always RED (wired-ready, dormant until earnings season)
    for s in src.get_short_signals():
        if s.get("symbol"):
            cands.append({"tier": "RED", "symbol": s["symbol"],
                          "systems": ["sell-the-news"], "short": s})

    # Bridge Vote — daily market-direction consensus (REGIME alert, not per-symbol).
    # YELLOW: total_voters>=5 AND agreement>=5/6. RED: that + conf>=80 AND conviction!=LOW.
    bc = src.get_bridge_consensus()
    if bc and (bc.get("total_voters") or 0) >= 5:
        agree = max(bc.get("buy", 0), bc.get("sell", 0), bc.get("hold", 0))
        conv = (bc.get("conviction") or "").upper()
        conf = bc.get("avg_confidence") or 0
        if agree >= 5:
            tier = "RED" if (conf >= 80 and conv != "LOW") else "YELLOW"
            head = next((ln.strip() for ln in (bc.get("briefing") or "").splitlines()
                         if ln.strip()), "")
            cands.append({
                "tier": tier, "symbol": "SPY", "systems": ["bridge-vote"],
                "dedup_key": f"bridge-vote:{bc.get('session_date')}",  # one per daily vote
                "bridge": {"vote": bc.get("consensus_vote"), "agree": agree,
                           "total": bc.get("total_voters"), "conf": conf,
                           "conviction": conv, "session_date": bc.get("session_date"),
                           "headline": head},
            })

    return cands


def run_alert_cycle() -> dict:
    """Check candidates, fire NTFY + persist Bridge-readable rows, deduped."""
    _ensure_table()
    fired = {"red": 0, "yellow": 0, "skipped_dup": 0, "candidates": 0}
    conn = sqlite3.connect(TRADER_DB)

    cands = _candidates()
    fired["candidates"] = len(cands)

    for item in cands:
        key = item.get("dedup_key") or _dedup_key(item["tier"], item["symbol"], item["systems"])
        if conn.execute("SELECT 1 FROM archer_alerts WHERE dedup_key=?", (key,)).fetchone():
            fired["skipped_dup"] += 1
            continue

        narrative = alert_narrative(item) or (
            f"{item['tier']} flag on {item['symbol']} "
            f"({', '.join(item['systems'])})."
        )
        emoji = "🔴" if item["tier"] == "RED" else "🟡"
        title = f"{emoji} Captain Archer — {item['tier']} — {item['symbol']}"
        try:
            # HM-NTFY-IPV6-NOROUTE-SWEEP 2026-07-10: delegates to the
            # hardened engine.alert_channels._send_ntfy() (forces IPv4)
            # instead of a separate requests.post() implementation.
            from engine.alert_channels import _send_ntfy
            _send_ntfy(
                title=title.encode("ascii", "replace").decode("ascii"),
                message=narrative,
                priority="high" if item["tier"] == "RED" else "default",
                topic=NTFY.rstrip("/").rsplit("/", 1)[-1],
            )
        except Exception as e:
            logger.warning("[Archer/alerts] ntfy failed: %s: %r", type(e).__name__, e)

        conn.execute(
            """INSERT OR IGNORE INTO archer_alerts
               (tier, symbol, systems, narrative, dedup_key) VALUES (?,?,?,?,?)""",
            (item["tier"], item["symbol"], ",".join(item["systems"]), narrative, key),
        )
        conn.commit()
        fired["red" if item["tier"] == "RED" else "yellow"] += 1

        # HM-ARCHER-DYNAMIC: bridge-vote alerts announce via Archer (once per session_date,
        # already deduped above). type='bridgevote' → frontend archerAnnounce.
        bridge = item.get("bridge")
        if bridge:
            body = (f"Bridge vote — {bridge.get('agree')} of {bridge.get('total')} "
                    f"{bridge.get('vote')} on {item['symbol']}, conviction {bridge.get('conviction')}.")
            _save_notification(
                title=f"🛞 Bridge Vote — {item['symbol']}", body=body,
                severity="warning" if item["tier"] == "RED" else "info",
                notif_type="bridgevote", icon="🛞",
            )

    conn.close()
    return fired
