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
import socket as _socket
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

NTFY_TOPIC       = os.environ.get("NTFY_TOPIC", "ollietrades-admin")
NTFY_ADMIN_TOPIC = os.environ.get("NTFY_ADMIN_TOPIC", "") or NTFY_TOPIC
NTFY_CREW_TOPIC  = os.environ.get("NTFY_CREW_TOPIC", "") or NTFY_TOPIC
NTFY_BASE        = "https://ntfy.sh"

SMTP_HOST  = os.environ.get("SMTP_HOST", "")
SMTP_PORT  = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER  = os.environ.get("SMTP_USER", "")
SMTP_PASS  = os.environ.get("SMTP_PASS", "") or os.environ.get("SMTP_APP_PASSWORD", "")
ALERT_EMAIL_TO = os.environ.get("ALERT_EMAIL_TO", "")

RATE_LIMIT_SECS = 300   # 5 minutes per alert_type
ALERTS_ENABLED_KEY = "alert_channels_enabled"

# HM-UHURA-HAILS (2026-06-08) — standardized outbound topics + independent toggles
NTFY_PREMARKET_TOPIC = os.environ.get("NTFY_PREMARKET_TOPIC", "ollie-premarket")
NTFY_SIGNALS_TOPIC   = os.environ.get("NTFY_SIGNALS_TOPIC",   "ollie-signals")
NTFY_CRITICAL_TOPIC  = os.environ.get("NTFY_CRITICAL_TOPIC",  "ollie-critical")

ALERTS_NTFY_ENABLED  = os.environ.get("ALERTS_NTFY_ENABLED",  "True").lower()  in ("1", "true", "yes", "on")
ALERTS_EMAIL_ENABLED = os.environ.get("ALERTS_EMAIL_ENABLED", "False").lower() in ("1", "true", "yes", "on")
ALERTS_EMAIL_TO      = os.environ.get("ALERTS_EMAIL_TO", "") or ALERT_EMAIL_TO


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
        # HM-NTFY-RATE-PERSIST 2026-05-28: restore rate-limit timestamps so NTFY
        # dedup survives restarts (was in-memory only → alerts re-fired post-restart).
        rs = _conn()
        rsrow = rs.execute(
            "SELECT value FROM settings WHERE key=?", ("alert_rate_state",)
        ).fetchone()
        rs.close()
        if rsrow and rsrow["value"]:
            _rate_state.update({k: float(v) for k, v in json.loads(rsrow["value"]).items()})
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


def _db_notification(title: str, body: str, severity: str, source: str = "") -> None:
    """Insert into notifications table — browser sees this via /api/notifications.

    HM-DYNALERTS-HYGIENE 2026-07-07: two fixes in this pass.
    (1) `source` param, additive -- writes `type = source or "alert_channel"`
    so emit-time Contact Classification tiering (Rung 1 card) can tell
    dynamic/user alerts (dyn_*/user_*) apart from generic alert_channel
    traffic, per the notifications.type column the /api/notifications poll
    already returns verbatim.
    (2) Column-name bug found while verifying the INSERT against the live
    schema before this edit (per directive instruction, not an incidental
    find): the prior INSERT wrote `created_at`, a column that has never
    existed on `notifications` (real column: `timestamp`, DEFAULT
    CURRENT_TIMESTAMP) -- every call has been silently failing since this
    function was written (confirmed empirically: zero rows with
    type='alert_channel' exist in the live table). The bare
    `except Exception: pass` swallowed every failure invisibly. Fixed by
    dropping the now-redundant explicit column (the table default already
    stamps it) rather than guessing at a `datetime('now')` value for a
    column that already self-populates.
    """
    try:
        c = _conn()
        c.execute(
            "INSERT INTO notifications(title, body, severity, type, icon) "
            "VALUES(?,?,?,?,?)",
            (title, body, severity, source or "alert_channel", "🔔")
        )
        c.commit()
        c.close()
    except Exception:
        pass


# ── Rate limiter ───────────────────────────────────────────────────────────────

def _rate_ok(alert_type: str, rate_limit_secs: int = RATE_LIMIT_SECS) -> bool:
    """True if we haven't successfully SENT this alert_type in the last
    rate_limit_secs. Read-only check -- does NOT consume the window itself;
    call _mark_rate_limit_sent() after a confirmed successful delivery to
    do that (see HM-ALERT-RATE-ON-FAILURE below for why the split matters).

    HM-U (2026-05-05): rate_limit_secs parameterized. Default preserves the
    module-level RATE_LIMIT_SECS=300 (5 min) for the 12 existing callers.
    HM-U architecture-path callers pass rate_limit_secs=86400 (24h) per
    'first occurrence per error class per day' policy (CLAUDE.md § Error
    Handling Posture).
    """
    with _state_lock:
        last = _rate_state.get(alert_type, 0)
        return _time.time() - last >= rate_limit_secs


def _mark_rate_limit_sent(alert_type: str) -> None:
    """HM-ALERT-RATE-ON-FAILURE (2026-07-07): the window used to be consumed
    inside _rate_ok() at CHECK time, before send_alert() even attempted
    delivery -- so a genuine network failure (e.g. HM-NTFY-IPV6-NOROUTE
    above: 0/20 real sends from a cron-invoked process, 100% silent
    failure) ALSO silently burned the retry budget. The sentinel's own log
    said "ALERT dispatched" every 5-min cron tick, but nothing had actually
    reached a phone, and the next real attempt wouldn't fire for the full
    1800s window either -- an alarm's own failure mode compounding its
    retry mechanism. Call only after send_alert() confirms at least one
    channel actually delivered.
    """
    with _state_lock:
        _rate_state[alert_type] = _time.time()
        _snapshot = dict(_rate_state)
    # HM-NTFY-RATE-PERSIST 2026-05-28: persist to settings so dedup survives restarts
    try:
        _save_setting("alert_rate_state", json.dumps(_snapshot))
    except Exception:
        pass


# ── Channel senders ────────────────────────────────────────────────────────────

# HM-NTFY-IPV6-NOROUTE (2026-07-07): this box has no working IPv6 route to
# ntfy.sh -- confirmed directly (socket.create_connection to ntfy.sh's AAAA
# address raises OSError [Errno 65] No route to host, 100% reproducible,
# both Python interpreters on this box, IPv4 always succeeds). Real-world
# symptom: scripts/hm_ops_sentinel.py's cron invocation (.venv/python3.14)
# got 0/20 successful ntfy sends in logs/hm_ops_sentinel_cron.log -- every
# single alert this whole time silently never reached a phone, while the
# sentinel's own log said "ALERT dispatched" (send_alert() doesn't raise on
# a network failure inside _send_ntfy, it just returns False and logs a
# warning -- exactly the "alarm shares a failure mode with silence" case).
# git_push_health_check.py (venv/python3.9, once-daily cron) has succeeded
# via the same _send_ntfy() code, so the getaddrinfo() address-family
# ordering that picks IPv6 first is context-dependent (interpreter/cron
# invocation specifics), not fully root-caused -- rather than depend on
# ordering being consistent across execution contexts, force IPv4 for the
# duration of this call. IPv6 not working anywhere on this box means the
# forced window carries no real cost to other concurrent socket use.
_ntfy_ipv4_lock = threading.Lock()
_orig_getaddrinfo = _socket.getaddrinfo


def _ipv4_only_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
    return _orig_getaddrinfo(host, port, _socket.AF_INET, type, proto, flags)


def _send_ntfy(title: str, message: str, priority: str = "default", tags: str = "ollietrades", topic: str = "") -> bool:
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
        with _ntfy_ipv4_lock:
            _socket.getaddrinfo = _ipv4_only_getaddrinfo
            try:
                with urllib.request.urlopen(req, timeout=8) as r:
                    logger.info("ntfy sent [%s]: %s", r.status, ascii_title)
            finally:
                _socket.getaddrinfo = _orig_getaddrinfo
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

def push_ntfy(topic: str, title: str, body: str, priority: str = "default", tags=None) -> bool:
    """HM-UHURA-HAILS — single outbound ntfy primitive. Honors ALERTS_NTFY_ENABLED.
    `tags` may be a list/tuple or a comma-string."""
    if not ALERTS_NTFY_ENABLED:
        return False
    _tags = ",".join(tags) if isinstance(tags, (list, tuple)) else (tags or "ollietrades")
    return _send_ntfy(title, body, priority, _tags, topic)


def send_email(subject: str, html_body: str, to: str | None = None) -> bool:
    """HM-UHURA-HAILS — HTML email via Gmail SMTP. Honors ALERTS_EMAIL_ENABLED.
    Never logs the app password."""
    if not ALERTS_EMAIL_ENABLED:
        return False
    to_addr = to or ALERTS_EMAIL_TO
    if not all([SMTP_HOST, SMTP_USER, SMTP_PASS, to_addr]):
        logger.warning("send_email skipped — SMTP not fully configured")
        return False
    try:
        import smtplib, re as _re
        from email.mime.multipart import MIMEMultipart
        from email.mime.text import MIMEText
        msg = MIMEMultipart("alternative")
        msg["Subject"] = f"[TradeMinds] {subject}"
        msg["From"] = SMTP_USER
        msg["To"]   = to_addr
        msg.attach(MIMEText(_re.sub(r"<[^>]+>", "", html_body), "plain", "utf-8"))  # text fallback
        msg.attach(MIMEText(html_body, "html", "utf-8"))
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as s:
            s.ehlo(); s.starttls(); s.login(SMTP_USER, SMTP_PASS)
            s.sendmail(SMTP_USER, [to_addr], msg.as_string())
        logger.info("HTML email sent to %s: %s", to_addr, subject)
        return True
    except Exception as e:
        logger.warning("send_email failed: %s: %r", type(e).__name__, e)
        return False


def send_alert(
    message: str,
    level: str = AlertLevel.INFO,
    alert_type: str = "general",
    title: str = "",
    bypass_rate_limit: bool = False,
    audience: str = "admin",   # "admin" | "crew" | "all"
    rate_limit_secs: int = RATE_LIMIT_SECS,  # HM-U: per-call override; default 300s, HM-U callers pass 86400 (24h)
    source: str = "",  # HM-DYNALERTS-HYGIENE 2026-07-07: emit-time Contact Classification
                        # tiering -- threaded to _db_notification's `type` column so Rung 1's
                        # ACTIONABLE/INFORMATIONAL split works. Appended LAST (not inserted
                        # among existing params) so no positional-arg caller can collide with it.
                        # Additive/optional: every existing caller is unaffected.
) -> dict:
    """
    Send alert to appropriate channels based on level.

    Returns dict with channel results: {ntfy, email, browser}.

    HM-U (2026-05-05): rate_limit_secs parameter added. Default preserves
    5-min behavior for existing callers; HM-U architecture-path callers pass
    86400 (per CLAUDE.md § Error Handling Posture, principle 3).
    """
    _load_state()
    if not _alerts_enabled:
        return {"skipped": "alerts disabled"}

    if not bypass_rate_limit and not _rate_ok(alert_type, rate_limit_secs):
        return {"skipped": f"rate_limited (cooldown {rate_limit_secs}s per type)"}

    if not title:
        prefix = {"info": "ℹ️", "warning": "⚠️", "red_alert": "🚨"}.get(level, "📢")
        title = f"{prefix} TradeMinds {level.replace('_', ' ').title()}"

    ntfy_priority = {
        AlertLevel.INFO:      "default",
        AlertLevel.WARNING:   "high",
        AlertLevel.RED_ALERT: "urgent",
    }.get(level, "default")

    ntfy_tags = {
        AlertLevel.INFO:      "ollietrades",
        AlertLevel.WARNING:   "warning,ollietrades",
        AlertLevel.RED_ALERT: "rotating_light,ollietrades",
    }.get(level, "ollietrades")

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
        _db_notification(title, message, "info", source)

    # WARNING → ntfy + browser notification (DB)
    elif level == AlertLevel.WARNING:
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in _ntfy_topics())
        _db_notification(title, message, "warning", source)
        results["browser"] = True

    # RED ALERT → all channels
    elif level == AlertLevel.RED_ALERT:
        crit_topics = _ntfy_topics() + [NTFY_CRITICAL_TOPIC]   # HM-UHURA-HAILS: keep admin topic AND add critical lane
        results["ntfy"]    = any(_send_ntfy(title, message, ntfy_priority, ntfy_tags, t) for t in crit_topics)
        _db_notification(title, message, "critical", source)
        results["browser"] = True
        results["email"]   = _send_email(title, f"{message}\n\nLevel: RED ALERT\nType: {alert_type}")

    # Only consume the rate-limit window on a confirmed EXTERNAL delivery
    # (see _mark_rate_limit_sent's docstring). Deliberately checks ntfy/email
    # specifically, not results.get("browser") -- that key is hardcoded True
    # unconditionally above (a pre-existing inaccuracy, not touched here) and
    # would make `any(results.values())` always truthy for WARNING/RED_ALERT,
    # silently defeating this fix for exactly the levels the sentinel uses.
    if not bypass_rate_limit and (results.get("ntfy") or results.get("email")):
        _mark_rate_limit_sent(alert_type)

    logger.info("Alert dispatched [%s/%s]: %s", level, alert_type, message[:80])
    return results


def send_test_alert(channel: str | None = None) -> dict:
    """Send test alert to all (or specific) channels — bypasses rate limit."""
    msg = "Test alert from USS TradeMinds. All systems nominal."
    title = "🧪 TradeMinds Test Alert"
    results = {}
    if channel in (None, "ntfy"):
        results["ntfy"] = _send_ntfy(title, msg, "default", "test,ollietrades")
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

def alert_info(message: str, alert_type: str = "info",
               rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget INFO alert in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.INFO,
                "alert_type": alert_type, "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


def alert_warning(message: str, alert_type: str = "warning",
                  rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget WARNING alert in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.WARNING,
                "alert_type": alert_type, "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


def alert_red(message: str, alert_type: str = "red_alert", title: str = "🚨 RED ALERT",
              rate_limit_secs: int = RATE_LIMIT_SECS) -> None:
    """Fire-and-forget RED ALERT in a background thread.

    HM-U: rate_limit_secs forwarded to send_alert; default preserves 5-min.
    """
    threading.Thread(
        target=send_alert,
        kwargs={"message": message, "level": AlertLevel.RED_ALERT,
                "alert_type": alert_type, "title": title,
                "rate_limit_secs": rate_limit_secs},
        daemon=True,
    ).start()


# Load state at module import
_load_state()
