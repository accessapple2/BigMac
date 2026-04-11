"""
engine/alert_channels.py — Phase 3.7 Unified Alert Channels

Dispatches alerts to multiple channels based on severity:
  INFO    → ntfy.sh only
  WARNING → ntfy.sh + browser push (via DB notification)
  RED ALERT → all channels (ntfy, browser push, email)

Rate limit: 1 alert per 5 minutes per alert_type.

Usage:
    from engine.alert_channels import send_alert, AlertLevel
    send_alert("VaR threshold breached", AlertLevel.WARNING, "var_breach")
    send_alert("System offline", AlertLevel.RED_ALERT, "system_down")

CIC commands (handled by handle_cic_command):
    "alerts on" / "alerts off"
    "alert test"
    "set alert email address@example.com"
"""
from __future__ import annotations

import os
import json
import sqlite3
import threading
import time as _time
import logging
import urllib.request
import urllib.error

logger = logging.getLogger(__name__)

# ── Config ─────────────────────────────────────────────────────────────────────

_DB_PATH = os.environ.get(
    "TRADEMINDS_DB",
    os.path.expanduser("~/autonomous-trader/data/trader.db"),
)

NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "trademinds-captain-sv")
NTFY_ADMIN_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "") or NTFY_TOPIC
NTFY_CREW_TOPIC  = os.environ.get("NTFY_CREW_TOPIC", "") or NTFY_TOPIC
NTFY_BASE        = "https://ntfy.sh"

SMTP_HOST  = os.environ.get("SMTP_HOST", "")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER  = os.environ.get("SMTP_USER", "")
SMTP_PASS  = os.environ.get("SMTP_PASS", "")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")

RATE_LIMIT_SECS = 300   # 5 minutes per alert_type
ALERTS_ENABLED_KEY = "alert_channels_enabled"


class AlertLevel:
    INFO      = "info"
    WARNING   = "warning"
    RED_ALERT = "red_alert"


# ── State ──────────────────────────────────────────────────────────────────────

_rate_state: dict[str, float] = {}   # alert_type → last_sent_ts
_state_lock = threading.Lock()
_alerts_enabled = True               # toggled by CIC commands


# ── DB helpers ─────────────────────────────────────────────────────────────────

def _conn():
    c = sqlite3.connect(_DB_PATH, check_same_thread=False, timeout=20)
    c.row_factory = sqlite3.Row
    return c


def _load_state() -> None:
    """Load persistent state (enabled flag, email) from settings table."""
    global _alerts_enabled, ALERT_EMAIL_TO
    try:
        c = _conn()
        rows = c.execute(
            "SELECT key, value FROM settings WHERE key IN (?,?)",
            (ALERTS_ENABLED_KEY, "alert_email_to")
        ).fetchall()
        c.close()
        for r in rows:
            if r["key"] == ALERTS_ENABLED_KEY:
                _alerts_enabled = (r["value"] or "1") != "0"
            elif r["key"] == "alert_email_to" and r["value"]:
                ALERT_EMAIL_TO = r["value"]
    except Exception:
        pass


def _save_setting(key: str, value: str) -> None:
    try:
        c = _conn()
        c.execute(
            "INSERT OR REPLACE INTO settings(key, value) VALUES(?,?)", (key, value)
        )
        c.commit()
        c.close()
    except Exception as e:
        logger.warning("alert_channels: save_setting failed: %s", e)


def _db_notification(title: str, body: str, severity: str) -> None:
    """Insert into notifications table — browser sees this via /api/notifications."""
    try:
        c = _conn()
        c.execute(
            "INSERT INTO notifications(title, body, severity, type, icon, created_at) "
            "VALUES(?,?,?,?,?,datetime('now'))",
            (title, body, severity, "alert_channel", "🔔")
        )
        c.commit()
        c.close()
    except Exception:
        pass


# ── Rate limiter ───────────────────────────────────────────────────────────────

def _rate_ok(alert_type: str) -> bool:
    """True if we haven't sent this alert_type in the last RATE_LIMIT_SECS."""
    with _state_lock:
        last = _rate_state.get(alert_type, 0)
        if _time.time() - last < RATE_LIMIT_SECS:
            return False
        _rate_state[alert_type] = _time.time()
        return True


# ── Channel senders ────────────────────────────────────────────────────────────

def _send_ntfy(title: str, message: str, priority: str = "default", tags: str = "trademinds", topic: str = "") -> bool:
    """Push via ntfy.sh (iPhone / Android / browser). topic overrides NTFY_TOPIC."""
    _topic = topic or NTFY_TOPIC
    if not _topic:
        return False
    try:
        ascii_title = title.encode("ascii", errors="replace").decode("ascii").strip()
        req = urllib.request.Request(
            f"{NTFY_BASE}/{_topic}",
            data=f"{title}\n{message}".encode("utf-8"),
            headers={
                "Title":        ascii_title or "TradeMinds",
                "Priority":     priority,
                "Tags":         tags,
                "Content-Type": "text/plain; charset=utf-8",
            },
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=8) as r:
            logger.info("ntfy sent [%s]: %s", r.status, ascii_title)
        return True
    except Exception as e:
        logger.warning("ntfy failed: %s", e)
        return False


def _send_email(subject: str, body: str, to: str = "") -> bool:
    """Send email via SMTP. Requires SMTP_HOST, SMTP_USER, SMTP_PASS in .env."""
    to_addr = to or ALERT_EMAIL_TO
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, to_addr]):
        return False
    try:
        import smtplib
        from email.mime.text import MIMEText
        msg = MIMEText(body, "plain", "utf-8")
        msg["Subject"] = f"[TradeMinds] {subject}"
        msg["From"]    = SMTP_USER
        msg["To"]      = to_addr
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo()
            s.starttls()
            s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_addr], msg.as_string())
        logger.info("Email sent to %s: %s", to_addr, subject)
        return True
    except Exception as e:
        logger.warning("Email failed: %s", e)
        return False


# ── Public API ─────────────────────────────────────────────────────────────────

def send_alert(
    message: str,
    level: str = AlertLevel.INFO,
    alert_type: str = "general",
    title: str = "",
    bypass_rate_limit: bool = False,
    audience: str = "admin",   # "admin" | "crew" | "all"
) -> dict:
    """
    Send alert to appropriate channels based on level.

    Returns dict with channel results: {ntfy, email, browser}.
    """
    _load_state()
    if not _alerts_enabled:
        return {"skipped": "alerts disabled"}

    if not bypass_rate_limit and not _rate_ok(alert_type):
        return {"skipped": f"rate_limited (cooldown {RATE_LIMIT_SECS}s per type)"}

    if not title:
        prefix = {"info": "ℹ️", "warning": "⚠️", "red_alert": "🚨"}.get(level, "📢")
        title = f"{prefix} TradeMinds {level.replace('_', ' ').title()}"

    ntfy_priority = {
        AlertLevel.INFO:      "default",
        AlertLevel.WARNING:   "high",
        AlertLevel.RED_ALERT: "urgent",
    }.get(level, "default")

    ntfy_tags = {
        AlertLevel.INFO:      "trademinds",
        AlertLevel.WARNING:   "warning,trademinds",
        AlertLevel.RED_ALERT: "rotating_light,trademinds",
    }.get(level, "trademinds")

    # Resolve ntfy topic(s) based on audience
    def _ntfy_topics() -> list[str]:
        if audience == "crew":
            return [t for t in [NTFY_CREW_TOPIC] if t]
        if audience == "all":
            topics = []
            if NTFY_ADMIN_TOPIC: topics.append(NTFY_ADMIN_TOPIC)
            if NTFY_CREW_TOPIC and NTFY_CREW_TOPIC != NTFY_ADMIN_TOPIC:
                topics.append(NTFY_CREW_TOPIC)
            return topics or [NTFY_TOPIC]
        return [t for t in [NTFY_ADMIN_TOPIC] if t]  # default: admin only

    results: dict = {}

    # INFO → ntfy only
    if level == AlertLevel.INFO:
        results["ntfy"] = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "info")

    # WARNING → ntfy + browser notification (DB)
    elif level == AlertLevel.WARNING:
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "warning")
        results["browser"] = True

    # RED ALERT → all channels
    elif level == AlertLevel.RED_ALERT:
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "critical")
        results["browser"] = True
        results["email"]   = _send_email(title, f"{message}\n\nLevel: RED ALERT\nType: {alert_type}")

    logger.info("Alert dispatched [%s/%s]: %s", level, alert_type, message[:80])
    return results


def send_test_alert(channel: str | None = None) -> dict:
    """Send test alert to all (or specific) channels — bypasses rate limit."""
    msg = "Test alert from USS TradeMinds. All systems nominal."
    title = "🧪 TradeMinds Test Alert"
    results = {}
    if channel in (None, "ntfy"):
        results["ntfy"] = _send_ntfy(title, msg, "default", "test,trademinds")
    if channel in (None, "email"):
        results["email"] = _send_email(title, msg)
    if channel in (None, "browser"):
        _db_notification(title, msg, "info")
        results["browser"] = True
    return results


# ── CIC command handler ────────────────────────────────────────────────────────

def handle_cic_command(command: str) -> str | None:
    """
    Parse and execute alert-related CIC commands.
    Returns a response string if handled, None otherwise.
    """
    global _alerts_enabled, ALERT_EMAIL_TO
    cmd = command.strip().lower()

    if cmd in ("alerts on", "enable alerts"):
        _alerts_enabled = True
        _save_setting(ALERTS_ENABLED_KEY, "1")
        return "✅ Alert channels enabled."

    if cmd in ("alerts off", "disable alerts"):
        _alerts_enabled = False
        _save_setting(ALERTS_ENABLED_KEY, "0")
        return "🔕 Alert channels disabled."

    if cmd in ("alert test", "test alert", "test alerts"):
        results = send_test_alert()
        lines = ["🧪 Test alert sent:"]
        for ch, ok in results.items():
            lines.append(f"  {ch}: {'✅' if ok else '❌'}")
        return "\n".join(lines)

    if cmd.startswith("set alert email "):
        email = command.strip()[len("set alert email "):].strip()
        if "@" in email and "." in email:
            ALERT_EMAIL_TO = email
            _save_setting("alert_email_to", email)
            return f"📧 Alert email set to: {email}"
        return "❌ Invalid email address."

    if cmd in ("alert status", "alerts status"):
        status = "enabled" if _alerts_enabled else "disabled"
        email_status = ALERT_EMAIL_TO or "not configured"
        ntfy_status  = (NTFY_TOPIC[:4] + "****") if NTFY_TOPIC else "not configured"
        smtp_status  = "configured" if all([SMTP_HOST, SMTP_USER, SMTP_PASS]) else "not configured"
        return (
            f"📡 Alert Channels ({status}):\n"
            f"  ntfy.sh: {ntfy_status}\n"
            f"  email: {email_status} (SMTP: {smtp_status})\n"
            f"  browser: always active (DB notifications)\n"
            f"  rate limit: {RATE_LIMIT_SECS}s per alert type"
        )

    return None   # Not an alert command


# ── Convenience shortcuts ──────────────────────────────────────────────────────

def alert_info(message: str, alert_type: str = "info") -> None:
    """Fire-and-forget INFO alert in a background thread."""
    threading.Thread(
        target=send_alert, args=(message, AlertLevel.INFO, alert_type), daemon=True
    ).start()


def alert_warning(message: str, alert_type: str = "warning") -> None:
    """Fire-and-forget WARNING alert in a background thread."""
    threading.Thread(
        target=send_alert, args=(message, AlertLevel.WARNING, alert_type), daemon=True
    ).start()


def alert_red(message: str, alert_type: str = "red_alert", title: str = "🚨 RED ALERT") -> None:
    """Fire-and-forget RED ALERT in a background thread."""
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.RED_ALERT,
                "alert_type": alert_type, "title": title},
        daemon=True,
    ).start()


# Load state at module import
_load_state()
