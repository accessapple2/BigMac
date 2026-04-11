"""
Bridge Communications — War Room Feed Integration
--------------------------------------------------
Posts Red Alert events to the War Room feed so they appear in the
dashboard alongside AI debate messages. Also posts to Discord if
DISCORD_WEBHOOK_URL is set in the environment.

Called from engine/red_alert._fire_alert() after logging and macOS
notification.
"""
from __future__ import annotations

import json
import os
from typing import Any

# Crew ID for Red Alert messages in the War Room
_ALERT_PLAYER_ID = "red-alert"
_ALERT_DISPLAY   = "Red Alert System"

# Discord webhook from environment (optional)
_DISCORD_URL = os.environ.get("DISCORD_WEBHOOK_URL", "")

# Severity → symbol / color emoji
_SEV_ICONS = {
    "critical": "🚨",
    "warning":  "⚠️",
    "info":     "✅",
}
_SEV_PREFIXES = {
    "critical": "[RED ALERT]",
    "warning":  "[YELLOW ALERT]",
    "info":     "[ALL CLEAR]",
}


def post_to_war_room(
    alert_type: str,
    severity: str,
    title: str,
    message: str,
    data: dict | None = None,
) -> bool:
    """
    Post a Red Alert event to the War Room table using existing save_hot_take().
    Returns True if the message was saved.
    """
    icon   = _SEV_ICONS.get(severity, "•")
    prefix = _SEV_PREFIXES.get(severity, "[ALERT]")
    take   = f"{prefix} {icon} {title}: {message}"

    # Symbol from data or generic
    symbol = "SPY"
    if data:
        symbol = data.get("symbol") or "SPY"

    saved = False
    try:
        from engine.war_room import save_hot_take
        # Ensure red-alert player exists in ai_players (create if missing)
        _ensure_alert_player()
        saved = save_hot_take(_ALERT_PLAYER_ID, symbol, take)
    except Exception as exc:
        try:
            from rich.console import Console
            Console().log(f"[yellow]BridgeComms: war_room save error: {exc}")
        except Exception:
            pass

    # Discord webhook (optional, fire-and-forget)
    if _DISCORD_URL:
        _post_discord(title, message, severity, data)

    return saved


def _ensure_alert_player() -> None:
    """Make sure 'red-alert' exists in ai_players so JOIN queries don't fail."""
    try:
        import sqlite3
        db_path = os.path.expanduser("~/autonomous-trader/data/trader.db")
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        existing = conn.execute(
            "SELECT id FROM ai_players WHERE id = ?", (_ALERT_PLAYER_ID,)
        ).fetchone()
        if not existing:
            # Use INSERT OR IGNORE with all required NOT NULL columns
            conn.execute("""
                INSERT OR IGNORE INTO ai_players
                    (id, display_name, provider, model_id)
                VALUES (?, ?, 'system', 'system')
            """, (_ALERT_PLAYER_ID, _ALERT_DISPLAY))
            conn.commit()
        conn.close()
    except Exception:
        pass


def _post_discord(
    title: str,
    message: str,
    severity: str,
    data: dict | None = None,
) -> None:
    """Post to Discord via webhook (optional)."""
    if not _DISCORD_URL:
        return
    try:
        import urllib.request
        colors = {"critical": 0xFF0000, "warning": 0xFFA500, "info": 0x00FF00}
        payload = {
            "embeds": [{
                "title":       f"{_SEV_ICONS.get(severity, '•')} {title}",
                "description": message,
                "color":       colors.get(severity, 0x808080),
                "footer":      {"text": "USS TradeMinds — Red Alert System"},
            }]
        }
        body = json.dumps(payload).encode()
        req  = urllib.request.Request(
            _DISCORD_URL, data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=5):
            pass
    except Exception:
        pass
